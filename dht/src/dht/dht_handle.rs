use crate::message::error::KrpcError;
use crate::message::ping_announce_peer_response::PingAnnouncePeerResponse;
use crate::schema::*;
use crate::token_generator::TokenGenerator;
use crate::types::{MAX_DIST, cmp_resp};
use crate::utils::unix_timestmap_ms;
use crate::{
    message::{
        KrpcBody, announce_peer_query::AnnouncePeerQuery, find_node_query::FindNodeQuery,
        get_peers_query::GetPeersQuery, ping_query::PingQuery,
    },
    our_error::{OurError, naur},
    types::{InfoHash, NodeId, NodeInfo, Token},
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

/// Outcome of an iterative get_peers lookup (BEP 5).
#[derive(Debug)]
pub struct GetPeersResult {
    /// every peer contact found for the info hash
    pub peers: Vec<SocketAddrV4>,
    /// nodes that issued us a token and will accept an announce_peer from us
    pub announce_candidates: Vec<(NodeInfo, Token)>,
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
                    let response = this.generate_response(&inbound_msg.body, socket_addr);

                    let txn_id = inbound_msg.transaction_id().clone();
                    let node_info =
                        NodeInfo::new(inbound_msg.node_id().expect("non qeuries are filtered"), socket_addr);
                    this.message_broker.reply(response, &node_info, txn_id);
                    trace!("response sending for {socket_addr}");
                }
                .instrument(info_span!("handle_requests")),
            );
        }
    }

    /**************************************   SERVER SECTION   *********************************************/

    #[tracing::instrument(skip(self))]
    fn generate_response(&self, request: &KrpcBody, from: SocketAddrV4) -> KrpcBody {
        assert!(request.is_query());

        match request {
            KrpcBody::PingQuery(ping) => self.generate_ping_response(ping, from),
            KrpcBody::FindNodeQuery(find_node) => self.generate_find_node_response(find_node, from),
            KrpcBody::AnnouncePeerQuery(announce_peer) => self.generate_announce_peer_response(announce_peer, from),
            KrpcBody::GetPeersQuery(get_peers) => self.generate_get_peers_response(get_peers, from),
            _ => unreachable!("caught by assert"),
        }
    }

    #[tracing::instrument(skip(self))]
    fn generate_ping_response(&self, ping: &PingQuery, origin: SocketAddrV4) -> KrpcBody {
        KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(self.our_id.clone()))
    }

    #[tracing::instrument(skip(self))]
    fn generate_find_node_response(&self, query: &FindNodeQuery, origin: SocketAddrV4) -> KrpcBody {
        let table = &self.router;
        let closest_eight: Vec<_> = table.find_closest(query.target_id()).into_iter().collect();

        // if we have an exact match, it will be the first element in the vector
        return if closest_eight.first().is_some_and(|n| n.id() == query.target_id()) {
            let res = ResBuilder::new(self.our_id).with_node(closest_eight[0].clone()).build();
            KrpcBody::FindNodeGetPeersResponse(res)
        } else {
            let stupid: Vec<_> = closest_eight.into_iter().collect();
            let res = ResBuilder::new(self.our_id).with_nodes(&stupid).build();
            KrpcBody::FindNodeGetPeersResponse(res)
        };
    }

    fn swarm_peers(&self, info_hash: &InfoHash) -> Vec<SocketAddrV4> {
        // peers are considered expired 45 minutes after their announce (BEP 5's suggested
        // lifetime); the cap keeps get_peers responses comfortably inside a datagram
        fn cutoff() -> i64 {
            let forty_five_minutes_ms = 45 * 60 * 1000;
            unix_timestmap_ms() - forty_five_minutes_ms
        }

        let mut conn = self.conn.get().expect("failed to get one connection from pool");

        let peers = peer::table
            .inner_join(swarm::table.on(peer::swarm.eq(swarm::info_hash)))
            .filter(swarm::info_hash.eq(&info_hash.0))
            .filter(peer::last_announced.ge(cutoff()))
            .select((peer::ip_addr, peer::port))
            .limit(50)
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
    fn generate_get_peers_response(&self, query: &GetPeersQuery, origin: SocketAddrV4) -> KrpcBody {
        let peers = self.swarm_peers(query.info_hash());
        let token_pool = &self.token_generator;

        let token = token_pool.token_for_ip(origin.ip());
        if !peers.is_empty() {
            let res = ResBuilder::new(self.our_id.clone())
                .with_token(token)
                .with_values(&*peers)
                .build();
            KrpcBody::FindNodeGetPeersResponse(res)
        } else {
            // when we don't have peer info on an info hash, respond with the closest nodes
            // we know *to that info hash* so the querier can iterate towards it
            let target = NodeId(query.info_hash().0);
            let closest_eight: Vec<_> = self.router.find_closest(target).into_iter().collect();

            let res = ResBuilder::new(self.our_id.clone())
                .with_token(token)
                .with_nodes(&closest_eight)
                .build();
            KrpcBody::FindNodeGetPeersResponse(res)
        }
    }

    #[tracing::instrument(skip(self))]
    fn generate_announce_peer_response(&self, announce: &AnnouncePeerQuery, origin: SocketAddrV4) -> KrpcBody {
        // the token must have been issued to this IP address (BEP 5)
        if !self.token_generator.is_valid_token(origin.ip(), announce.token()) {
            return KrpcBody::ErrorResponse(KrpcError::new_protocol());
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

        KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(self.our_id.clone()))
    }

    fn add_peers_to_db(
        info_hash: &InfoHash,
        peer_contact: SocketAddrV4,
        conn: &mut PooledConnection<ConnectionManager<SqliteConnection>>,
    ) -> Result<usize, diesel::result::Error> {
        // the ensure that the swarm bit should probably be a separate function
        conn.transaction(|conn| {
            let info_hash = info_hash.as_bytes().to_vec();
            let _ = insert_into(swarm::table)
                .values(swarm::info_hash.eq(&info_hash))
                .on_conflict_do_nothing()
                .execute(conn)
                .inspect_err(|e| error!("{e}"))?;

            let now = unix_timestmap_ms();
            insert_into(peer::table)
                .values(
                    // NOTE: this comes in the host native endianness, but it should be fine as long as the db
                    // file is not transferred between computers
                    (
                        peer::ip_addr.eq(peer_contact.ip().to_string()),
                        peer::port.eq(peer_contact.port() as i32),
                        peer::swarm.eq(info_hash),
                        peer::last_announced.eq(now),
                    ),
                )
                .on_conflict((peer::ip_addr, peer::port, peer::swarm))
                .do_update()
                .set(peer::last_announced.eq(now))
                .execute(conn)
                .inspect_err(|e| warn!("{e}"))
        })
    }
    /**************************************   CLIENT SECTION   *********************************************/

    // TODO: need a function to send with timeout, unsubscribe and clean up when timeout expires

    #[tracing::instrument(skip(self))]
    pub async fn ping(&self, peer: SocketAddrV4) -> Result<NodeId, OurError> {
        let ping_msg = KrpcBody::PingQuery(PingQuery::new(self.our_id));

        let response = self.message_broker.query(ping_msg, &peer, REQ_TIMEOUT).await?;

        return if let KrpcBody::PingAnnouncePeerResponse(response) = response.body {
            Ok(*response.target_id())
        } else {
            warn!("Unexpected response to ping: {:?}", response);
            Err(naur!("Unexpected response to ping"))
        };
    }

    /// starting point of trying to find any nodes on the network
    #[tracing::instrument(skip(self))]
    pub async fn find_node(&self, target: NodeId) -> Vec<NodeInfo> {
        // TODO: While timing out out on an individual request in the process of searching is not an
        // error per se, it's still desirable to deliver these information to the caller somehow

        // if we already know the node, then no need for any network requests
        if let Some(node) = (&self).router.find_exact(&target) {
            return vec![node];
        }

        // find the closest nodes that we know
        let mut closest = self.router.find_closest(target);
        // ids we've already sent a query to; consulted and updated every round
        let mut queried: HashSet<NodeId> = HashSet::new();
        let mut querying: Vec<NodeInfo> = vec![];

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

            // this round: the unqueried known nodes closest to the target
            querying.clear();
            querying.extend(
                closest
                    .iter()
                    .filter(|n| !queried.contains(&n.id()))
                    .take(CONCURRENT_REQS),
            );
            if querying.is_empty() {
                info!("asked every known node, returning with {} closest nodes", closest.len());
                return closest;
            }
            for node in &querying {
                queried.insert(node.id());
            }

            let round_results = querying
                .iter()
                .map(|node| async move { (node.id(), self.send_find_nodes_rpc(node.end_point(), target).await) })
                .collect::<Vec<_>>();
            let round_results = join_all(round_results).await;

            let mut returned_nodes = vec![];
            for (node_id, result) in round_results {
                match result {
                    Ok(nodes) => returned_nodes.extend(nodes),
                    // timeouts and the like: record the failure so repeated ones get the
                    // node evicted
                    Err(_) => self.router.mark_failed(&node_id),
                }
            }

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
            if let Some(target_node) = returned_nodes.iter().find(|node| node.id() == target) {
                return vec![*target_node];
            }

            closest.append(&mut returned_nodes);
            closest.sort_unstable_by(|l, r| l.id().0.cmp(&r.id().0));
            closest.dedup_by(|l, r| l.id() == r.id());
            closest.sort_unstable_by(|lhs, rhs| cmp_resp(&lhs.id(), &rhs.id(), &target));

            // another round we go!
        }
    }

    // attempt to find the target node via a peer on this address
    #[tracing::instrument(skip(self))]
    async fn send_find_nodes_rpc(&self, dest: SocketAddrV4, target: NodeId) -> Result<Vec<NodeInfo>, OurError> {
        // construct the message to query our friends
        let query = KrpcBody::FindNodeQuery(FindNodeQuery::new(self.our_id, target));

        // send the message and await for a response
        let response = self.message_broker.query(query, &dest, REQ_TIMEOUT).await?;
        let body = response.body;

        if let KrpcBody::FindNodeGetPeersResponse(find_node_response) = body {
            let mut nodes: Vec<_> = find_node_response.nodes().clone();

            // some clients will return duplicate nodes, so we remove them
            nodes.sort_unstable();
            nodes.dedup();

            Result::Ok(nodes)
        } else {
            Err(naur!("Did not get a find node response"))
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_peers(&self, info_hash: InfoHash) -> Result<GetPeersResult, OurError> {
        // peers others announced *to us* are served from the local store immediately
        let known_peers = self.swarm_peers(&info_hash);
        if !known_peers.is_empty() {
            return Ok(GetPeersResult {
                peers: known_peers,
                announce_candidates: vec![],
            });
        }

        // iterative lookup: query the closest-known nodes, follow their `nodes` referrals
        // towards the info hash, and harvest peers and tokens along the way
        let target = NodeId(info_hash.0);
        let mut closest = self.router.find_closest(target);
        let mut queried: HashSet<NodeId> = HashSet::new();
        let mut peers: Vec<SocketAddrV4> = vec![];
        let mut announce_candidates: Vec<(NodeInfo, Token)> = vec![];

        let mut round = 0;
        loop {
            if round == ROUNDS_LIMIT {
                break;
            }
            round += 1;

            let querying: Vec<NodeInfo> = closest
                .iter()
                .filter(|n| !queried.contains(&n.id()))
                .take(CONCURRENT_REQS)
                .cloned()
                .collect();
            if querying.is_empty() {
                break;
            }
            for node in &querying {
                queried.insert(node.id());
            }

            let results = querying
                .iter()
                .map(|node| async move { (*node, self.send_get_peers_rpc(node.end_point(), info_hash).await) })
                .collect::<Vec<_>>();
            let results = join_all(results).await;

            let mut returned_nodes = vec![];
            for (node, result) in results {
                match result {
                    Ok((token, nodes, values)) => {
                        peers.extend(values);
                        if let Some(token) = token {
                            announce_candidates.push((node, token));
                        }
                        returned_nodes.extend(nodes);
                    }
                    Err(_) => self.router.mark_failed(&node.id()),
                }
            }

            closest.append(&mut returned_nodes);
            closest.sort_unstable_by(|l, r| l.id().0.cmp(&r.id().0));
            closest.dedup_by(|l, r| l.id() == r.id());
            closest.sort_unstable_by(|l, r| cmp_resp(&l.id(), &r.id(), &target));
        }

        peers.sort_unstable();
        peers.dedup();

        Ok(GetPeersResult {
            peers,
            announce_candidates,
        })
    }

    #[tracing::instrument(skip(self))]
    pub async fn announce_peers(
        &self,
        recipient: SocketAddrV4,
        info_hash: InfoHash,
        port: Option<u16>,
        token: Token,
    ) -> Result<(), OurError> {
        // 6881 is a default port when implied_port is used
        let query = KrpcBody::AnnouncePeerQuery(AnnouncePeerQuery::new(
            self.our_id.clone(),
            port.is_none(),
            port.unwrap_or(6881),
            info_hash,
            token,
        ));

        let response = self.message_broker.query(query, &recipient, REQ_TIMEOUT).await?;

        return match response.body {
            KrpcBody::PingAnnouncePeerResponse(_) => Ok(()),
            KrpcBody::ErrorResponse(err) => Err(naur!(
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
        let query = KrpcBody::GetPeersQuery(GetPeersQuery::new(self.our_id.clone(), info_hash));

        // send the message and await for a response
        let response = self.message_broker.query(query, &dest, REQ_TIMEOUT).await?;

        return match response.body {
            KrpcBody::ErrorResponse(response) => {
                warn!("Got an error response to get peers: {:?}", response);
                return Err(naur!("Got an error response to get peers"));
            }
            KrpcBody::FindNodeGetPeersResponse(response) => {
                let token = response.token().cloned();

                let mut nodes = response.nodes().clone();
                nodes.sort_unstable_by_key(|node| node.end_point());
                nodes.dedup();

                let mut values = response.values().clone();
                values.sort_unstable();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::SensibleOptions;
    use crate::dht::txn_id_generator::TxnIdGenerator;
    use crate::message::find_node_get_peers_response::Builder as ResBuilder;
    use crate::message::{Krpc, ParseKrpc};
    use diesel::connection::SimpleConnection;
    use std::net::SocketAddr;
    use tokio::net::UdpSocket;

    fn test_pool(ddl: &str) -> Pool<ConnectionManager<SqliteConnection>> {
        let manager = ConnectionManager::<SqliteConnection>::new(":memory:");
        let pool = Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(SensibleOptions))
            .build(manager)
            .unwrap();
        pool.get().unwrap().batch_execute(ddl).unwrap();
        pool
    }

    /// A fake DHT node that answers every get_peers query with the given response body
    async fn fake_dht_node(socket: UdpSocket, body: KrpcBody) {
        let mut buf = [0u8; 1500];
        loop {
            let (n, peer) = socket.recv_from(&mut buf).await.unwrap();
            let Ok(msg) = (&buf[..n]).parse() else { continue };
            let KrpcBody::GetPeersQuery(_) = &msg.body else {
                continue;
            };
            let resp = Krpc::new_with_body(msg.transaction_id().clone(), body.clone());
            let _ = socket.send_to(&resp.encode(), peer).await;
        }
    }

    #[tokio::test]
    async fn get_peers_iterates_referrals_and_captures_tokens() {
        // node B has the goods: a peer contact and a token
        let socket_b = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let SocketAddr::V4(addr_b) = socket_b.local_addr().unwrap() else {
            unreachable!("bound to an ipv4 address");
        };
        let node_b = NodeInfo::new(NodeId([0xBB; 20]), addr_b);
        let token_b = Token::from_bytes(b"tok_b");
        let peer_x: SocketAddrV4 = "10.9.8.7:6881".parse().unwrap();
        let body_b = KrpcBody::FindNodeGetPeersResponse(
            ResBuilder::new(NodeId([0xBB; 20]))
                .with_token(token_b.clone())
                .with_value(peer_x)
                .build(),
        );
        tokio::spawn(fake_dht_node(socket_b, body_b));

        // node A only refers us to B (with its own token)
        let socket_a = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let SocketAddr::V4(addr_a) = socket_a.local_addr().unwrap() else {
            unreachable!("bound to an ipv4 address");
        };
        let token_a = Token::from_bytes(b"tok_a");
        let body_a = KrpcBody::FindNodeGetPeersResponse(
            ResBuilder::new(NodeId([0xAA; 20]))
                .with_token(token_a.clone())
                .with_node(node_b)
                .build(),
        );
        tokio::spawn(fake_dht_node(socket_a, body_a));

        // our handle, with node A as the entire routing table
        let router_pool = test_pool(
            "CREATE TABLE node (
                id BLOB PRIMARY KEY,
                bucket INTEGER NOT NULL,
                last_contacted BIGINT NOT NULL,
                ip_addr TEXT NOT NULL,
                port INTEGER NOT NULL,
                failed_requests INTEGER NOT NULL,
                removed BOOLEAN NOT NULL,
                last_sent BIGINT,
                added BIGINT NOT NULL DEFAULT 0
            )",
        );
        let swarm_pool = test_pool(
            "CREATE TABLE swarm (info_hash BLOB PRIMARY KEY);
             CREATE TABLE peer (
                ip_addr TEXT NOT NULL,
                port INTEGER NOT NULL,
                last_announced BIGINT NOT NULL,
                swarm BLOB NOT NULL,
                PRIMARY KEY (ip_addr, port, swarm)
            )",
        );

        let our_id = NodeId([0x01; 20]);
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let broker = KrpcBroker::new(
            socket,
            router_pool.clone(),
            Arc::new(TxnIdGenerator::new()),
            Ipv4Addr::LOCALHOST,
        );
        broker.run().await.unwrap();
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let router = Router::new(our_id, broker.clone(), router_pool, rx);
        router.add(NodeId([0xAA; 20]), addr_a);
        let handle = DhtHandle::new(our_id, router, broker, swarm_pool);

        let result = handle.get_peers(InfoHash([0xFF; 20])).await.unwrap();

        assert_eq!(result.peers, vec![peer_x], "the referral must be followed to B");
        assert!(
            result
                .announce_candidates
                .iter()
                .any(|(n, t)| n.id() == node_b.id() && *t == token_b),
            "B's token must be captured for a later announce"
        );
        assert!(
            result
                .announce_candidates
                .iter()
                .any(|(n, t)| n.id() == NodeId([0xAA; 20]) && *t == token_a),
            "A's token must be captured too"
        );
    }
}
