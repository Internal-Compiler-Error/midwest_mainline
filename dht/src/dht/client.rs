//! The client half of the DHT: iterative lookups (find_node, get_peers), ping, and
//! announce_peer, using the shared state. Clone it freely, everything it touches is
//! shared.

use futures::future::join_all;
use std::collections::HashSet;
use std::net::SocketAddrV4;
use std::sync::Arc;
use tracing::{info, warn};

use crate::dht::state::{REQ_TIMEOUT, SharedState};
use crate::message::{
    KrpcBody, announce_peer_query::AnnouncePeerQuery, find_node_query::FindNodeQuery, get_peers_query::GetPeersQuery,
    ping_query::PingQuery,
};
use crate::our_error::{OurError, naur};
use crate::types::{InfoHash, NodeId, NodeInfo, Token, cmp_resp};

const ROUNDS_LIMIT: i32 = 8;
const CONCURRENT_REQS: usize = 3;

/// Outcome of an iterative get_peers lookup (BEP 5).
#[derive(Debug)]
pub struct GetPeersResult {
    /// every peer contact found for the info hash
    pub peers: Vec<SocketAddrV4>,
    /// nodes that issued us a token and will accept an announce_peer from us
    pub announce_candidates: Vec<(NodeInfo, Token)>,
}

#[derive(Debug, Clone)]
pub struct DhtClient {
    state: Arc<SharedState>,
}

impl DhtClient {
    pub(crate) fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    pub fn our_id(&self) -> NodeId {
        self.state.our_id
    }

    #[tracing::instrument(skip(self))]
    pub async fn ping(&self, peer: SocketAddrV4) -> Result<NodeId, OurError> {
        let ping_msg = KrpcBody::PingQuery(PingQuery::new(self.state.our_id));

        let response = self.state.rpc_manager.query(ping_msg, &peer, REQ_TIMEOUT).await?;

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
        if let Some(node) = (&self.state.routing_table).find_exact(&target) {
            return vec![node];
        }

        // find the closest nodes that we know
        let mut closest = self.state.routing_table.find_closest(target);
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
                    Err(_) => self.state.routing_table.mark_failed(&node_id),
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
            closest.sort_unstable_by_key(|n| n.id().0);
            closest.dedup_by(|l, r| l.id() == r.id());
            closest.sort_unstable_by(|lhs, rhs| cmp_resp(&lhs.id(), &rhs.id(), &target));

            // another round we go!
        }
    }

    // attempt to find the target node via a peer on this address
    #[tracing::instrument(skip(self))]
    async fn send_find_nodes_rpc(&self, dest: SocketAddrV4, target: NodeId) -> Result<Vec<NodeInfo>, OurError> {
        // construct the message to query our friends
        let query = KrpcBody::FindNodeQuery(FindNodeQuery::new(self.state.our_id, target));

        // send the message and await for a response
        let response = self.state.rpc_manager.query(query, &dest, REQ_TIMEOUT).await?;
        let body = response.body;

        if let KrpcBody::FindNodeGetPeersResponse(find_node_response) = body {
            let mut nodes: Vec<_> = find_node_response.nodes().clone();

            // some clients will return duplicate nodes, so we remove them
            nodes.sort_unstable();
            nodes.dedup();

            Ok(nodes)
        } else {
            warn!("Did not get a find node response, got {:?}", body);
            Err(naur!("Did not get a find node response"))
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_peers(&self, info_hash: InfoHash) -> Result<GetPeersResult, OurError> {
        // peers others announced *to us* are served from the local store immediately
        let known_peers = self.state.swarm_peers(&info_hash);
        if !known_peers.is_empty() {
            return Ok(GetPeersResult {
                peers: known_peers,
                announce_candidates: vec![],
            });
        }

        // iterative lookup: query the closest-known nodes, follow their `nodes` referrals
        // towards the info hash, and harvest peers and tokens along the way
        let target = NodeId(info_hash.0);
        let mut closest = self.state.routing_table.find_closest(target);
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
                    Err(_) => self.state.routing_table.mark_failed(&node.id()),
                }
            }

            closest.append(&mut returned_nodes);
            closest.sort_unstable_by_key(|n| n.id().0);
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
            self.state.our_id,
            port.is_none(),
            port.unwrap_or(6881),
            info_hash,
            token,
        ));

        let response = self.state.rpc_manager.query(query, &recipient, REQ_TIMEOUT).await?;

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
        // construct the message to query our friends
        let query = KrpcBody::GetPeersQuery(GetPeersQuery::new(self.state.our_id, info_hash));

        // send the message and await for a response
        let response = self.state.rpc_manager.query(query, &dest, REQ_TIMEOUT).await?;

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
    use crate::dht::routing_table::RoutingTable;
    use crate::dht::rpc_manager::RpcManager;
    use crate::dht::txn_id_generator::TxnIdGenerator;
    use crate::message::find_node_get_peers_response::Builder as ResBuilder;
    use crate::message::{Krpc, ParseKrpc};
    use crate::test_support::memory_pool;
    use std::net::{Ipv4Addr, SocketAddr};
    use tokio::net::UdpSocket;

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

        // our client, with node A as the entire routing table
        let router_pool = memory_pool();
        let swarm_pool = memory_pool();

        let our_id = NodeId([0x01; 20]);
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let broker = RpcManager::new(socket, router_pool.clone(), Arc::new(TxnIdGenerator::new()));
        broker.run().await.unwrap();
        let routing_table = RoutingTable::new(our_id, broker.clone(), router_pool);
        routing_table.add(NodeId([0xAA; 20]), addr_a);

        let state = Arc::new(SharedState::new(our_id, routing_table, broker, swarm_pool));
        let client = DhtClient::new(state);

        let result = client.get_peers(InfoHash([0xFF; 20])).await.unwrap();

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
