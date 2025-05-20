use crate::schema::*;
use crate::token_generator::TokenGenerator;
use crate::utils::unix_timestmap_ms;
use crate::{
    message::{
        announce_peer_query::AnnouncePeerQuery, find_node_query::FindNodeQuery, get_peers_query::GetPeersQuery,
        ping_query::PingQuery, Krpc,
    },
    our_error::{naur, OurError},
    types::{InfoHash, NodeId, NodeInfo, Token, TransactionId},
    utils::ParSpawnAndAwait,
};
use diesel::insert_into;
use diesel::r2d2::Pool;
use diesel::{prelude::*, r2d2::ConnectionManager};

use crate::message::find_node_get_peers_response::Builder as ResBuilder;
use rand::prelude::*;
use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{task::Builder as TskBuilder, time::timeout};
use tracing::{info_span, trace, warn, Instrument};

use super::{router::Router, txn_id_generator::TxnIdGenerator, KrpcBroker};

// TODO: make these configurable some day
pub const REQ_TIMEOUT: Duration = Duration::from_secs(15);
const ROUNDS_LIMIT: i32 = 8;
const CONCURRENT_REQS: usize = 3;

#[derive(Debug)]
pub struct DhtHandle {
    pub(crate) our_id: NodeId,
    pub(crate) router: Router,

    swarms: Pool<ConnectionManager<SqliteConnection>>,

    token_pool: TokenGenerator,
    message_broker: KrpcBroker,

    // parts needed to make requests
    pub(crate) transaction_id_pool: Arc<TxnIdGenerator>,
}

impl DhtHandle {
    pub(crate) fn new(
        id: NodeId,
        router: Router,
        message_broker: KrpcBroker,
        transaction_id_pool: Arc<TxnIdGenerator>,
        swarms: Pool<ConnectionManager<SqliteConnection>>,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let seed: u128 = rng.gen();

        Self {
            swarms,
            token_pool: TokenGenerator::new(seed),
            our_id: id,
            router,
            message_broker,
            transaction_id_pool,
        }
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn run(self: Arc<Self>) {
        let mut rx = self.message_broker.subscribe_inbound();

        // respond to messages, as fast as possible
        while let Some((inbound_msg, socket_addr)) = rx.recv().await {
            let this = self.clone();
            let _ = TskBuilder::new().name(&*format!("responding to {socket_addr}")).spawn(
                async move {
                    let this = &*this;

                    let response = match this.generate_response(&inbound_msg, socket_addr).await {
                        Some(msg) => msg,
                        None => return,
                    };

                    trace!("Handling request from {socket_addr}");

                    this.message_broker.send_msg(response, socket_addr);
                    trace!("response sending for {socket_addr}");
                }
                .instrument(info_span!("handle_requests")),
            );
        }
    }

    /**************************************   SERVER SECTION   *********************************************/

    #[tracing::instrument(skip(self))]
    async fn generate_response(&self, request: &Krpc, from: SocketAddrV4) -> Option<Krpc> {
        let response = match request {
            Krpc::PingQuery(ping) => Some(self.generate_ping_response(ping, from).await),
            Krpc::FindNodeQuery(find_node) => Some(self.generate_find_node_response(find_node, from).await),
            Krpc::AnnouncePeerQuery(announce_peer) => {
                Some(self.generate_announce_peer_response(announce_peer, from).await)
            }
            Krpc::GetPeersQuery(get_peers) => Some(self.generate_get_peers_response(get_peers, from).await),
            _ => {
                assert!(request.is_response());
                None
            }
        };
        response
    }

    #[tracing::instrument(skip(self))]
    async fn generate_ping_response(&self, ping: &PingQuery, origin: SocketAddrV4) -> Krpc {
        Krpc::new_ping_response(ping.txn_id().clone(), self.our_id.clone())
    }

    #[tracing::instrument(skip(self))]
    async fn generate_find_node_response(&self, query: &FindNodeQuery, origin: SocketAddrV4) -> Krpc {
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

        let mut conn = self.swarms.get().expect("failed to get one connection from pool");

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
    async fn generate_get_peers_response(&self, query: &GetPeersQuery, origin: SocketAddrV4) -> Krpc {
        let peers = self.swarm_peers(query.info_hash());
        let token_pool = &self.token_pool;

        let token = token_pool.token_for_node(query.querier());
        if !peers.is_empty() {
            let res = ResBuilder::new(query.txn_id().clone(), self.our_id.clone())
                .with_token(token)
                .with_values(&*peers)
                .build();
            Krpc::FindNodeGetPeersResponse(res)
        } else {
            // when we don't have peer info on an info hash, respond with the cloests nodes so the querier can ask them
            let closest_eight: Vec<_> = self
                .router
                .find_closest(*query.querier()) // TODO: wtf, why are we finding via info_hash
                .into_iter()
                .collect();

            let res = ResBuilder::new(query.txn_id().clone(), self.our_id.clone())
                .with_token(token)
                .with_nodes(&closest_eight)
                .build();
            Krpc::FindNodeGetPeersResponse(res)
        }
    }

    #[tracing::instrument(skip(self))]
    async fn generate_announce_peer_response(&self, announce: &AnnouncePeerQuery, origin: SocketAddrV4) -> Krpc {
        // see if the token is valid
        if !self.token_pool.is_valid_token(announce.querier(), announce.token()) {
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

        let mut conn = self.swarms.get().unwrap();
        conn.transaction(|conn| {
            let info_hash = announce.info_hash().0.to_vec();
            let swarm: Vec<u8> = insert_into(swarm::table)
                .values(swarm::info_hash.eq(&info_hash))
                .on_conflict_do_nothing()
                .returning(swarm::info_hash)
                .get_result(conn)?;

            let now = unix_timestmap_ms();
            insert_into(peer::table)
                .values(
                    // TODO: this comes in the host native endianness, but it should be fine as long as the db
                    // file is not transfered between computers
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
        .expect("transaction for recording new peers failed");

        Krpc::new_announce_peer_response(announce.txn_id().clone(), self.our_id.clone())
    }

    /**************************************   CLIENT SECTION   *********************************************/

    // TODO: need a function to send with timeout, unsubscribe and clean up when timeout expires

    #[tracing::instrument(skip(self))]
    pub async fn ping(&self, peer: SocketAddrV4) -> Result<NodeId, OurError> {
        let txn_id = self.transaction_id_pool.next();
        let ping_msg = Krpc::new_ping_query(TransactionId::from(txn_id), self.our_id);

        let response = self.message_broker.send_and_wait(ping_msg, peer).await?;

        return if let Krpc::PingAnnouncePeerResponse(response) = response {
            Ok(*response.target_id())
        } else {
            warn!("Unexpected response to ping: {:?}", response);
            Err(naur!("Unexpected response to ping"))
        };
    }

    /// starting point of trying to find any nodes on the network
    #[tracing::instrument(skip(self))]
    pub async fn find_node(self: Arc<Self>, target: NodeId) -> Result<NodeInfo, OurError> {
        // if we already know the node, then no need for any network requests
        if let Some(node) = (&self).router.find_exact(&target) {
            return Ok(node);
        }

        let mut queried: HashSet<NodeInfo> = HashSet::new();

        // find the closest nodes that we know
        let mut closest = self.router.find_closest(target);
        for node in closest.iter() {
            queried.insert(*node);
        }

        let mut round = 0;
        loop {
            if round == ROUNDS_LIMIT {
                return Err(naur!("Too many rounds of find node"));
            }
            round += 1;

            let returned_nodes = closest
                .iter()
                .map(|node| node.end_point())
                .map(|ip| self.clone().send_find_nodes_rpc(ip, target))
                .collect::<Vec<_>>();

            let returned_nodes = returned_nodes.par_spawn_and_await().await?;

            // filter out the ones that resulted in failure, such as due to time out
            let returned_nodes: Vec<_> = returned_nodes
                .into_iter()
                .filter_map(|node| node.ok())
                .flatten()
                .collect();

            // it's possible that some of the nodes returned are actually the node we're looking for
            // so we check for that and return it if it's the case
            let target_node = returned_nodes.iter().find(|node| node.id() == target);
            if target_node.is_some() {
                return Result::Ok(*target_node.unwrap());
            }

            // if we don't have the node, then we find the alpha closest nodes and ask them in turn
            let mut sorted_by_distance: Vec<_> = returned_nodes
                .into_iter()
                .map(|node| {
                    let distance = node.id().dist(&self.our_id);
                    (node, distance)
                })
                .collect();
            sorted_by_distance.sort_unstable_by_key(|(_, distance)| distance.clone());

            closest.clear();
            for (node_info, _dist) in sorted_by_distance.iter().take(CONCURRENT_REQS) {
                if !queried.contains(node_info) {
                    closest.push(*node_info);
                }
            }
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
        let txn_id = self.transaction_id_pool.next();
        let query = Krpc::new_find_node_query(TransactionId::from(txn_id), self.our_id, target);

        // send the message and await for a response
        let time_out = REQ_TIMEOUT;
        // TODO: make this configurable or let parent handle timeout, wooo maybe we can configure this
        // based on ip geo location distance
        let response = timeout(time_out, self.message_broker.send_and_wait(query, dest))
            .await
            .inspect_err(|_e| trace!("find_nodes for {:?} timed out", dest))
            ?  // timeout error
            ?; // send_and_wait error

        if let Krpc::FindNodeGetPeersResponse(find_node_response) = response {
            // TODO:: does the following actually handle the giant bitstring thing correctly?
            //
            // the nodes come back as one giant byte string, each 26 bytes is a node
            // we split them up and create a vector of them
            let mut nodes: Vec<_> = find_node_response.nodes().clone();

            // some clients will return duplicate nodes, so we remove them
            nodes.sort_unstable_by_key(|node| node.end_point());
            nodes.dedup();

            Result::Ok(nodes)
        } else {
            Err(naur!("Did not get a find node response"))
        }
    }

    // TODO: API is broken, since we can't guarantee that the peer will exist or we can find them,
    // we should return a list of K closest nodes or the target itself if can be found
    #[tracing::instrument(skip(self))]
    pub async fn get_peers(self: Arc<Self>, info_hash: InfoHash) -> Result<(Token, Vec<SocketAddrV4>), OurError> {
        let resonsible = NodeId(info_hash.0);
        //
        // if we already know the node, then no need for any network requests
        if let Some(node) = (&self).router.find_exact(&resonsible) {
            let (token, _nodes, peers) = self.send_get_peers_rpc(node.end_point(), info_hash).await?;
            return Ok((
                token.expect("A node directly responsible for a piece would return a token"),
                peers,
            ));
        }

        let mut queried: HashSet<NodeInfo> = HashSet::new();

        // find the closest nodes that we know
        let mut closest = self.router.find_closest(resonsible);
        for node in closest.iter() {
            queried.insert(*node);
        }

        let mut round = 0;
        loop {
            if round == ROUNDS_LIMIT {
                return Err(naur!("Too many rounds of find node"));
            }
            round += 1;

            let returned_nodes = closest
                .iter()
                .map(|node| node.end_point())
                .map(|ip| self.clone().send_get_peers_rpc(ip, info_hash))
                .collect::<Vec<_>>();

            let returned_nodes = returned_nodes.par_spawn_and_await().await?;

            // filter out the ones that resulted in failure, such as due to time out
            let responses: Vec<_> = returned_nodes.into_iter().filter_map(|res| res.ok()).collect();

            // if any of them has a list of peers already, we can stop
            let peers_list = responses.iter().find(|(_token, _nodes, peers)| !peers.is_empty());
            if peers_list.is_some() {
                let peers_list = peers_list.unwrap().clone();
                return Ok((
                    peers_list
                        .0
                        .expect("If they have a list of peers, they will provide a token"),
                    peers_list.2,
                ));
            }

            // if we don't have the node, then we find the alpha closest nodes and ask them in turn
            let mut sorted_by_distance: Vec<_> = responses
                .into_iter()
                .flat_map(|(_token, nodes, _peers)| {
                    let our_id = self.our_id;
                    let distances = nodes
                        .into_iter()
                        .map(move |node| (node, node.id().dist_big_unit(&our_id)));
                    distances
                })
                .collect();
            sorted_by_distance.sort_unstable_by_key(|(_, distance)| distance.clone());

            closest.clear();
            for (node_info, _dist) in sorted_by_distance.iter().take(CONCURRENT_REQS) {
                if !queried.contains(node_info) {
                    closest.push(*node_info);
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn announce_peers(
        self: Arc<Self>,
        recipient: SocketAddrV4,
        info_hash: InfoHash,
        port: Option<u16>,
        token: Token,
    ) -> Result<(), OurError> {
        let transaction_id = self.transaction_id_pool.next();

        let query = if let Some(port) = port {
            Krpc::new_announce_peer_query(
                TransactionId::from(transaction_id),
                info_hash,
                self.our_id.clone(),
                port,
                true,
                token,
            )
        } else {
            Krpc::new_announce_peer_query(
                TransactionId::from(transaction_id),
                info_hash,
                self.our_id.clone(),
                // TODO: hmm, this interesting
                // self.socket_address.port(),
                0, // obviously replace this later
                true,
                token,
            )
        };

        let response = self.message_broker.send_and_wait(query, recipient).await?;

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
        self: Arc<Self>,
        dest: SocketAddrV4,
        info_hash: InfoHash,
    ) -> Result<(Option<Token>, Vec<NodeInfo>, Vec<SocketAddrV4>), OurError> {
        trace!("Asking {:?} for peers", dest);
        // construct the message to query our friends
        let transaction_id = self.transaction_id_pool.next();

        let query = Krpc::new_get_peers_query(TransactionId::from(transaction_id), self.our_id.clone(), info_hash);

        // send the message and await for a response
        let response = self
            .message_broker
            .send_and_wait_timeout(query, dest, REQ_TIMEOUT)
            .await?;
        // let response = timeout(time_out, self.message_broker.send_and_wait(query, dest))
        //     .await
        //     .inspect_err(|_e| {
        //         trace!("get_peers for {:?} timed out", dest);
        //     })
        //     ?  // timeout error
        //     ?; // send_and_wait related error

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
