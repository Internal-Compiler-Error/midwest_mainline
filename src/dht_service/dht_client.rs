use crate::{
    dht_service::{transaction_id_pool::TransactionIdPool, MessageBroker},
    domain_knowledge::{InfoHash, NodeId, NodeInfo, PeerContact, Token, TransactionId},
    message::Krpc,
    our_error::{naur, OurError},
    utils::ParSpawnAndAwait,
};
use std::{collections::HashSet, net::SocketAddrV4, sync::Arc, time::Duration};
use tokio::time::timeout;
use tracing::warn;
use tracing::{instrument, trace};

use super::peer_guide::PeerGuide;

#[derive(Debug)]
pub struct DhtHandle {
    pub(crate) our_id: NodeId,
    pub(crate) message_broker: Arc<MessageBroker>,
    pub(crate) routing_table: Arc<PeerGuide>,
    pub(crate) transaction_id_pool: TransactionIdPool,
}

// TODO: make these configurable some day
const REQ_TIMEOUT: u64 = 15;
const ROUNDS_LIMIT: i32 = 8;
const CONCURRENT_REQS: usize = 3;

impl DhtHandle {
    /// A default DHT node when you really don't know anything about DHTs and just want to provide
    /// a port and IP address
    pub(crate) fn new(message_broker: Arc<MessageBroker>, peer_guide: Arc<PeerGuide>, our_id: NodeId) -> Self {
        DhtHandle {
            message_broker,
            our_id,
            routing_table: peer_guide,
            transaction_id_pool: TransactionIdPool::new(),
        }
    }

    // TODO: need a function to send with timeout, unsubscribe and clean up when timeout expires

    /// Send a message out and await for a response.
    ///
    /// It does not alter the routing table, callers must decide what to do with the response.
    pub async fn send_and_wait(&self, message: Krpc, recipient: SocketAddrV4) -> Result<Krpc, OurError> {
        let rx = {
            let rx = self
                .message_broker
                .subscribe_one(message.transaction_id().clone())
                .await;
            self.message_broker.send_msg(message.clone(), recipient);
            rx
        };
        let (response, _addr) = rx.await.unwrap();
        Ok(response)
    }

    pub async fn ping(&self, peer: SocketAddrV4) -> Result<NodeId, OurError> {
        let txn_id = self.transaction_id_pool.next();
        let ping_msg = Krpc::new_ping_query(TransactionId::from(txn_id), self.our_id);

        let response = self.send_and_wait(ping_msg, peer).await?;

        return if let Krpc::PingAnnouncePeerResponse(response) = response {
            Ok(*response.target_id())
        } else {
            warn!("Unexpected response to ping: {:?}", response);
            Err(naur!("Unexpected response to ping"))
        };
    }

    /// starting point of trying to find any nodes on the network
    pub async fn find_node(self: Arc<Self>, target: NodeId) -> Result<NodeInfo, OurError> {
        // if we already know the node, then no need for any network requests
        if let Some(node) = (&self).routing_table.find(target) {
            return Ok(node);
        }

        let mut queried: HashSet<NodeInfo> = HashSet::new();

        // find the closest nodes that we know
        let mut closest = self.routing_table.find_closest(target);
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
                .map(|node| node.contact().0)
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
    async fn send_find_nodes_rpc(
        self: Arc<Self>,
        dest: SocketAddrV4,
        target: NodeId,
    ) -> Result<Vec<NodeInfo>, OurError> {
        // construct the message to query our friends
        let txn_id = self.transaction_id_pool.next();
        let query = Krpc::new_find_node_query(TransactionId::from(txn_id), self.our_id, target);

        // send the message and await for a response
        let time_out = Duration::from_secs(REQ_TIMEOUT);
        // TODO: make this configurable or let parent handle timeout, wooo maybe we can configure this
        // based on ip geo location distance
        let response = timeout(time_out, self.send_and_wait(query, dest))
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
            nodes.sort_unstable_by_key(|node| node.contact().0);
            nodes.dedup();

            Result::Ok(nodes)
        } else {
            Err(naur!("Did not get a find node response"))
        }
    }

    // TODO: API is broken, since we can't guarantee that the peer will exist or we can find them,
    // we should return a list of K closest nodes or the target itself if can be found
    pub async fn get_peers(self: Arc<Self>, info_hash: InfoHash) -> Result<(Token, Vec<PeerContact>), OurError> {
        let resonsible = NodeId(info_hash.0);
        //
        // if we already know the node, then no need for any network requests
        if let Some(node) = (&self).routing_table.find(resonsible) {
            let (token, _nodes, peers) = self.send_get_peers_rpc(node.contact().0, info_hash).await?;
            return Ok((
                token.expect("A node directly responsible for a piece would return a token"),
                peers,
            ));
        }

        let mut queried: HashSet<NodeInfo> = HashSet::new();

        // find the closest nodes that we know
        let mut closest = self.routing_table.find_closest(resonsible);
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
                .map(|node| node.contact().0)
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

        let response = self.send_and_wait(query, recipient).await?;

        return match response {
            Krpc::PingAnnouncePeerResponse(_) => Ok(()),
            Krpc::ErrorResponse(err) => Err(naur!(
                "node responded with an error to our announce peer request {err:?}"
            )),
            _ => Err(naur!("non-compliant response from DHT node")),
        };
    }

    #[instrument(skip(self))]
    async fn send_get_peers_rpc(
        self: Arc<Self>,
        dest: SocketAddrV4,
        info_hash: InfoHash,
    ) -> Result<(Option<Token>, Vec<NodeInfo>, Vec<PeerContact>), OurError> {
        trace!("Asking {:?} for peers", dest);
        // construct the message to query our friends
        let transaction_id = self.transaction_id_pool.next();

        let query = Krpc::new_get_peers_query(TransactionId::from(transaction_id), self.our_id.clone(), info_hash);

        // send the message and await for a response
        let time_out = Duration::from_secs(REQ_TIMEOUT);
        let response = timeout(time_out, self.send_and_wait(query, dest))
            .await
            .inspect_err(|_e| {
                trace!("get_peers for {:?} timed out", dest);
            })
            ?  // timeout error
            ?; // send_and_wait related error

        return match response {
            Krpc::ErrorResponse(response) => {
                warn!("Got an error response to get peers: {:?}", response);
                return Err(naur!("Got an error response to get peers"));
            }
            Krpc::FindNodeGetPeersResponse(response) => {
                let token = response.token().cloned();

                let mut nodes = response.nodes().clone();
                nodes.sort_unstable_by_key(|node| node.contact().0);
                nodes.dedup();

                let mut values = response.values().clone();
                nodes.sort_unstable_by_key(|node| node.contact().0);
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
