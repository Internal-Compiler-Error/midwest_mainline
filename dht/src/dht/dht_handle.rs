use crate::schema::*;
use crate::token_generator::TokenGenerator;
use crate::types::{MAX_DIST, TXN_ID_PLACEHOLDER, cmp_resp};
use crate::utils::{bail_on_err, unix_timestmap_ms};
use crate::{
    message::{
        Krpc, announce_peer_query::AnnouncePeerQuery, find_node_query::FindNodeQuery, get_peers_query::GetPeersQuery,
        ping_query::PingQuery,
    },
    our_error::{OurError, naur},
    types::{InfoHash, NodeId, NodeInfo, Token, TransactionId},
};
use diesel::insert_into;
use diesel::r2d2::{Pool, PooledConnection};
use diesel::{prelude::*, r2d2::ConnectionManager};
use futures::future::join_all;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::message::find_node_get_peers_response::Builder as ResBuilder;
use rand::prelude::*;
use std::cmp::min;
use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::task::Builder as TskBuilder;
use tracing::{Instrument, error, info, info_span, trace, warn};

use super::{KrpcBroker, router::Router};

// TODO: make these configurable some day
pub const REQ_TIMEOUT: Duration = Duration::from_secs(15);
const ROUNDS_LIMIT: i32 = 8;
const CONCURRENT_REQS: usize = 3;

#[derive(Debug)]
pub struct DhtHandle {
    pub(crate) our_id: NodeId,
    pub(crate) router: Router,

    conn: Pool<ConnectionManager<SqliteConnection>>,

    token_generator: TokenGenerator,
    message_broker: KrpcBroker,
}

impl DhtHandle {
    pub(crate) fn new(
        id: NodeId,
        router: Router,
        message_broker: KrpcBroker,
        swarms: Pool<ConnectionManager<SqliteConnection>>,
    ) -> Self {
        let mut rng = rand::rng();
        let seed: u128 = rng.random();

        Self {
            conn: swarms,
            token_generator: TokenGenerator::new(seed),
            our_id: id,
            router,
            message_broker,
        }
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn run(self: Arc<Self>) {
        let rx = self.message_broker.subscribe_inbound();
        let rx = ReceiverStream::new(rx);
        let mut requests = rx.filter(|(msg, _)| !msg.is_error() && !msg.is_response());

        // respond to messages, as fast as possible
        while let Some((inbound_msg, socket_addr)) = requests.next().await {
            let this = self.clone();
            let _ = TskBuilder::new().name(&*format!("responding to {socket_addr}")).spawn(
                async move {
                    let this = &*this;

                    trace!("Handling request from {socket_addr}");
                    let response = this.generate_response(&inbound_msg, socket_addr);

                    let txn_id = inbound_msg.transaction_id().clone();
                    let node_info =
                        NodeInfo::new(inbound_msg.node_id().expect("non qeuries are filtered"), socket_addr);
                    let _ = this
                        .message_broker
                        .reply(response, &node_info, txn_id, Duration::MAX)
                        .await
                        .inspect_err(|e| error!("{e}"));
                    trace!("response sending for {socket_addr}");
                }
                .instrument(info_span!("handle_requests")),
            );
        }
    }

    /**************************************   SERVER SECTION   *********************************************/

    #[tracing::instrument(skip(self))]
    fn generate_response(&self, request: &Krpc, from: SocketAddrV4) -> Krpc {
        assert!(request.is_query());

        match request {
            Krpc::PingQuery(ping) => self.generate_ping_response(ping, from),
            Krpc::FindNodeQuery(find_node) => self.generate_find_node_response(find_node, from),
            Krpc::AnnouncePeerQuery(announce_peer) => self.generate_announce_peer_response(announce_peer, from),
            Krpc::GetPeersQuery(get_peers) => self.generate_get_peers_response(get_peers, from),
            _ => unreachable!("caught by assert"),
        }
    }

    #[tracing::instrument(skip(self))]
    fn generate_ping_response(&self, ping: &PingQuery, origin: SocketAddrV4) -> Krpc {
        Krpc::new_ping_response(ping.txn_id().clone(), self.our_id.clone())
    }

    #[tracing::instrument(skip(self))]
    fn generate_find_node_response(&self, query: &FindNodeQuery, origin: SocketAddrV4) -> Krpc {
        let table = &self.router;
        let closest_eight: Vec<_> = table.find_closest(query.target_id()).into_iter().collect();

        // if we have an exact match, it will be the first element in the vector
        return if closest_eight[0].id() == query.target_id() {
            let res = ResBuilder::new(query.txn_id().clone(), self.our_id)
                .with_node(closest_eight[0].clone())
                .build();
            Krpc::FindNodeGetPeersResponse(res)
        } else {
            let stupid: Vec<_> = closest_eight.into_iter().collect();
            let res = ResBuilder::new(query.txn_id().clone(), self.our_id)
                .with_nodes(&stupid)
                .build();
            Krpc::FindNodeGetPeersResponse(res)
        };
    }

    fn swarm_peers(&self, info_hash: &InfoHash) -> Vec<SocketAddrV4> {
        // determines when does a peer is considered expired, default is 30 mins
        // TODO: should be configurable in the future.
        fn cutoff() -> i64 {
            let thirty_minutes_ms = 30 * 60 * 1000;
            unix_timestmap_ms() - thirty_minutes_ms
        }

        let mut conn = self.conn.get().expect("failed to get one connection from pool");

        let peers = peer::table
            .inner_join(swarm::table.on(peer::swarm.eq(swarm::info_hash)))
            .filter(swarm::info_hash.eq(&info_hash.0))
            .filter(peer::last_announced.ge(cutoff()))
            .select((peer::ip_addr, peer::port))
            .load::<(String, i32)>(&mut conn)
            .unwrap();
        let peers: Vec<SocketAddrV4> = peers
            .into_iter()
            .map(|(ip, port)| {
                assert!(port >= 0 && port <= u16::MAX.into(), "port should fit inside an u16");

                let ip: Ipv4Addr = ip.parse().expect(&format!(
                    "invalid ip string representation got into the database: {}",
                    ip
                ));
                SocketAddrV4::new(ip, port as u16)
            })
            .collect();
        peers
    }

    #[tracing::instrument(skip(self))]
    fn generate_get_peers_response(&self, query: &GetPeersQuery, origin: SocketAddrV4) -> Krpc {
        let peers = self.swarm_peers(query.info_hash());
        let token_pool = &self.token_generator;

        let token = token_pool.token_for_node(query.querier());
        if !peers.is_empty() {
            let res = ResBuilder::new(query.txn_id().clone(), self.our_id.clone())
                .with_token(token)
                .with_values(&*peers)
                .build();
            Krpc::FindNodeGetPeersResponse(res)
        } else {
            // when we don't have peer info on an info hash, respond with the cloests nodes so the querier can ask them
            let closest_eight: Vec<_> = self.router.find_closest(*query.querier()).into_iter().collect();

            let res = ResBuilder::new(query.txn_id().clone(), self.our_id.clone())
                .with_token(token)
                .with_nodes(&closest_eight)
                .build();
            Krpc::FindNodeGetPeersResponse(res)
        }
    }

    #[tracing::instrument(skip(self))]
    fn generate_announce_peer_response(&self, announce: &AnnouncePeerQuery, origin: SocketAddrV4) -> Krpc {
        // see if the token is valid
        if !self
            .token_generator
            .is_valid_token(announce.querier(), announce.token())
        {
            return Krpc::new_standard_protocol_error(announce.txn_id().clone());
        }

        // generate the correct peer contact according to the implied port argument, the port
        // argument is ignored if the implied port is not 0 and we use the origin port instead
        let peer_contact = {
            if !announce.implied_port() {
                SocketAddrV4::new(*origin.ip(), announce.port())
            } else {
                origin
            }
        };

        let mut conn = self.conn.get().unwrap();
        let _ = Self::add_peers_to_db(announce.info_hash(), peer_contact, &mut conn).inspect_err(|e| warn!("{e}"));

        Krpc::new_announce_peer_response(announce.txn_id().clone(), self.our_id.clone())
    }

    fn add_peers_to_db(
        info_hash: &InfoHash,
        peer_contact: SocketAddrV4,
        conn: &mut PooledConnection<ConnectionManager<SqliteConnection>>,
    ) -> Result<usize, diesel::result::Error> {
        // the ensure that the swarm bit should probably be a separate function
        conn.transaction(|conn| {
            let info_hash = info_hash.as_bytes().to_vec();
            let swarm: Vec<u8> = insert_into(swarm::table)
                .values(swarm::info_hash.eq(&info_hash))
                .on_conflict_do_nothing()
                .returning(swarm::info_hash)
                .get_result(conn)?;

            let now = unix_timestmap_ms();
            insert_into(peer::table)
                .values(
                    // NOTE: this comes in the host native endianness, but it should be fine as long as the db
                    // file is not transferred between computers
                    (
                        peer::ip_addr.eq(peer_contact.ip().to_string()),
                        peer::port.eq(peer_contact.port() as i32),
                        peer::swarm.eq(swarm),
                        peer::last_announced.eq(now),
                    ),
                )
                .on_conflict((peer::ip_addr, peer::port, peer::swarm))
                .do_update()
                .set(peer::last_announced.eq(now))
                .execute(conn)
        })
    }
    /**************************************   CLIENT SECTION   *********************************************/

    // TODO: need a function to send with timeout, unsubscribe and clean up when timeout expires

    #[tracing::instrument(skip(self))]
    pub async fn ping(&self, peer: SocketAddrV4) -> Result<NodeId, OurError> {
        let ping_msg = Krpc::new_ping_query(TXN_ID_PLACEHOLDER, self.our_id);

        let response = self.message_broker.query(ping_msg, &peer, REQ_TIMEOUT).await?;

        return if let Krpc::PingAnnouncePeerResponse(response) = response {
            Ok(*response.target_id())
        } else {
            warn!("Unexpected response to ping: {:?}", response);
            Err(naur!("Unexpected response to ping"))
        };
    }

    /// starting point of trying to find any nodes on the network
    #[tracing::instrument(skip(self))]
    pub async fn find_node(self: Arc<Self>, target: NodeId) -> Vec<NodeInfo> {
        // TODO: While timout out of an individual request in the process of searching is not an
        // error per se, it's still desirable to deliver these information to the caller somehow

        // if we already know the node, then no need for any network requests
        if let Some(node) = (&self).router.find_exact(&target) {
            return vec![node];
        }

        let mut queried: HashSet<NodeInfo> = HashSet::new();

        // find the closest nodes that we know
        let mut closest = self.router.find_closest(target);
        let mut querying = closest.clone();
        for node in closest.iter() {
            queried.insert(*node);
        }

        let mut prev_distance = MAX_DIST;
        let mut round = 0;
        loop {
            info!("round {round} of finding {target:?}");
            if round == ROUNDS_LIMIT {
                info!(
                    "Too many rounds of find node, returning with {} closest nodes",
                    closest.len()
                );
                return closest;
            }
            round += 1;

            let returned_nodes = querying
                .iter()
                .map(|node| node.end_point())
                .map(|ip| self.clone().send_find_nodes_rpc(ip, target))
                .collect::<Vec<_>>();

            let returned_nodes = join_all(returned_nodes).await;

            // filter out the ones that resulted in failure, such as due to time out
            let mut returned_nodes: Vec<_> = returned_nodes
                .into_iter()
                .filter_map(|node| node.ok())
                .flatten() // combine the "branched out" closests nodes together
                .collect();

            // the node we reached to might be dead already, which will return as a timeout
            if returned_nodes.is_empty() {
                info!(
                    "Last round of branching returned nothing, returning with {} closest nodes",
                    closest.len()
                );
                return closest;
            }

            // it's possible that some of the nodes returned are actually the node we're looking for
            // so we check for that and return it if it's the case
            let target_node = returned_nodes.iter().find(|node| node.id() == target);
            if target_node.is_some() {
                return vec![*target_node.unwrap()];
            }

            let next_round_dist = returned_nodes.iter().map(|n| n.id().dist(&target)).min().unwrap();

            closest.append(&mut returned_nodes);
            // TODO: while individually each request has been deduped, it's possible after
            // combining, there are duplicates again, but sorting again seems kinda wasteful
            closest.sort_unstable_by(|lhs, rhs| cmp_resp(&lhs.id(), &rhs.id(), &target));

            querying.clear();
            querying.extend(
                closest
                    .iter()
                    .take_while(|n| {
                        let dist = self.our_id.dist(&n.id());
                        dist <= prev_distance
                    })
                    .take(CONCURRENT_REQS),
            );
            prev_distance = min(prev_distance, next_round_dist);

            // another round we go!
        }
    }

    // attempt to find the target node via a peer on this address
    #[tracing::instrument(skip(self))]
    async fn send_find_nodes_rpc(
        self: Arc<Self>,
        dest: SocketAddrV4,
        target: NodeId,
    ) -> Result<Vec<NodeInfo>, OurError> {
        // construct the message to query our friends
        let query = Krpc::new_find_node_query(TXN_ID_PLACEHOLDER, self.our_id, target);

        // send the message and await for a response
        let response = self.message_broker.query(query, &dest, REQ_TIMEOUT).await?;

        if let Krpc::FindNodeGetPeersResponse(find_node_response) = response {
            // TODO:: does the following actually handle the giant bitstring thing correctly?
            //
            // the nodes come back as one giant byte string, each 26 bytes is a node
            // we split them up and create a vector of them
            let mut nodes: Vec<_> = find_node_response.nodes().clone();

            // some clients will return duplicate nodes, so we remove them
            nodes.sort_unstable();
            nodes.dedup();

            Result::Ok(nodes)
        } else {
            Err(naur!("Did not get a find node response"))
        }
    }

    // TODO: think of a better name
    /// Send a GetPeers query to the endpoint about the info_hash, and add all the peers obtained to the database
    async fn query_and_update(&self, endpoint: SocketAddrV4, info_hash: &InfoHash) {
        // TODO: definitely not using the token, although we might be interested in adding the
        // nodes?
        let (_token, _nodes, peers) = bail_on_err!(self.send_get_peers_rpc(endpoint, *info_hash).await);

        let mut conn = self.conn.get().unwrap();
        for peer in peers {
            let _ = Self::add_peers_to_db(info_hash, peer, &mut conn).inspect_err(|e| error!("{e}"));
        }
    }

    #[tracing::instrument(skip(self))]
    // TODO:
    // 1. should probably include a variant that always does reaches out even if we have entries
    // in the database
    // 2. the design of this API is problematic, instead of taking in an InfoHash, we should take
    //    in an endpoint like `ping`, because the point of get_peers is two fold:
    //    a. to obtain a list of peers if the querier needs them
    //    b. (far more important) obtain the token so the querier can follow up with an
    //       AnnouncePeer query to update *our* database
    // 3. We should have a function that aggregates the peers we found by querying others, but not
    //    this function with its current deisgn
    // May 2025
    pub async fn get_peers(self: Arc<Self>, info_hash: InfoHash) -> Result<Vec<SocketAddrV4>, OurError> {
        let known_peers = self.swarm_peers(&info_hash);
        if !known_peers.is_empty() {
            return Ok(known_peers);
        }

        // TODO: what should be the terminating condition? Stop when any node returns a list of
        // peers or similar to find_node that stops once we stop "improving"? If the latter, what
        // does it mean by to improve?
        //
        // Current design just does *one* round of get_peers message

        // if let Some(node) = (&self).router.find_exact(&resonsible) {
        //     let (token, nodes, peers) = self.send_get_peers_rpc(node.end_point(), info_hash).await?;
        //
        //     return Ok((
        //         token.expect("A node directly responsible for a piece would return a token"),
        //         peers,
        //     ));
        // }

        let resonsible = NodeId(info_hash.0);
        let closest = self.router.find_closest(resonsible);
        let work: Vec<_> = closest
            .into_iter()
            .map(|n| self.query_and_update(n.end_point(), &info_hash))
            .collect();

        join_all(work).await;

        Ok(self.swarm_peers(&info_hash))
    }

    #[tracing::instrument(skip(self))]
    pub async fn announce_peers(
        self: Arc<Self>,
        recipient: SocketAddrV4,
        info_hash: InfoHash,
        port: Option<u16>,
        token: Token,
    ) -> Result<(), OurError> {
        let query = if let Some(port) = port {
            Krpc::new_announce_peer_query(
                TransactionId::from(TXN_ID_PLACEHOLDER),
                info_hash,
                self.our_id.clone(),
                port,
                true,
                token,
            )
        } else {
            Krpc::new_announce_peer_query(
                TransactionId::from(TXN_ID_PLACEHOLDER),
                info_hash,
                self.our_id.clone(),
                // TODO: hmm, this interesting
                // self.socket_address.port(),
                0, // obviously replace this later
                true,
                token,
            )
        };

        let response = self.message_broker.query(query, &recipient, REQ_TIMEOUT).await?;

        return match response {
            Krpc::PingAnnouncePeerResponse(_) => Ok(()),
            Krpc::ErrorResponse(err) => Err(naur!(
                "node responded with an error to our announce peer request {err:?}"
            )),
            _ => Err(naur!("non-compliant response from DHT node")),
        };
    }

    #[tracing::instrument(skip(self))]
    async fn send_get_peers_rpc(
        &self,
        dest: SocketAddrV4,
        info_hash: InfoHash,
    ) -> Result<(Option<Token>, Vec<NodeInfo>, Vec<SocketAddrV4>), OurError> {
        trace!("Asking {:?} for peers", dest);
        // construct the message to query our friends
        let query = Krpc::new_get_peers_query(TXN_ID_PLACEHOLDER, self.our_id.clone(), info_hash);

        // send the message and await for a response
        let response = self.message_broker.query(query, &dest, REQ_TIMEOUT).await?;

        return match response {
            Krpc::ErrorResponse(response) => {
                warn!("Got an error response to get peers: {:?}", response);
                return Err(naur!("Got an error response to get peers"));
            }
            Krpc::FindNodeGetPeersResponse(response) => {
                let token = response.token().cloned();

                let mut nodes = response.nodes().clone();
                nodes.sort_unstable_by_key(|node| node.end_point());
                nodes.dedup();

                let mut values = response.values().clone();
                nodes.sort_unstable_by_key(|node| node.end_point());
                values.dedup();

                Ok((token, nodes, values))
            }
            other => {
                warn!("Unexpected response to get peers: {:?}", other);
                Err(naur!("Unexpected response to get peers"))
            }
        };
    }
}
