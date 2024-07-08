use crate::{
    dht_service::{transaction_id_pool::TransactionIdPool, MessageBroker},
    domain_knowledge::{InfoHash, NodeId, NodeInfo, PeerContact, Token, TransactionId},
    message::Krpc,
    our_error::{naur, OurError},
    utils::ParSpawnAndAwait,
};
use async_recursion::async_recursion;
use num::BigUint;
use std::{
    collections::HashSet,
    net::SocketAddrV4,
    ops::{BitXor, DerefMut},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        oneshot::{self, Sender},
        Mutex,
    },
    task::JoinSet,
    time::timeout,
};
use tracing::{debug, info, instrument, trace, warn};

use super::peer_guide::PeerGuide;

#[derive(Debug)]
pub struct DhtClientV4 {
    pub(crate) our_id: NodeId,
    pub(crate) message_broker: Arc<MessageBroker>,
    pub(crate) routing_table: Arc<PeerGuide>,
    pub(crate) transaction_id_pool: TransactionIdPool,
}

impl DhtClientV4 {
    /// A default DHT node when you really don't know anything about DHTs and just want to provide
    /// a port and IP address
    pub(crate) fn new(message_broker: Arc<MessageBroker>, peer_guide: Arc<PeerGuide>, our_id: NodeId) -> Self {
        DhtClientV4 {
            message_broker,
            our_id,
            routing_table: peer_guide,
            transaction_id_pool: TransactionIdPool::new(),
        }
    }

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

    pub async fn ping(self: Arc<Self>, peer: SocketAddrV4) -> Result<(), OurError> {
        let this = &self;
        let txn_id = self.transaction_id_pool.next();
        let ping_msg = Krpc::new_ping_query(TransactionId::from(txn_id), this.our_id);

        let response = self.send_and_wait(ping_msg, peer).await?;

        return if let Krpc::PingAnnouncePeerResponse(_response) = response {
            Ok(())
        } else {
            warn!("Unexpected response to ping: {:?}", response);
            Err(naur!("Unexpected response to ping"))
        };
    }

    /// starting point of trying to find any nodes on the network
    pub async fn find_node(self: Arc<Self>, target: NodeId) -> Result<NodeInfo, OurError> {
        // if we already know the node, then no need for any network requests
        if let Some(node) = (&self).routing_table.find(target) {
            return Ok(node.contact);
        }

        // find the closest nodes that we know
        let closest = self.routing_table.find_closest(target);

        let returned_nodes = closest
            .into_iter()
            .map(|node| node.contact().0)
            .map(|ip| self.clone().find_nodes_via_peer(ip, target))
            .collect::<Vec<_>>();

        let returned_nodes = returned_nodes.par_spawn_and_await().await?;

        let returned_nodes: Vec<_> = returned_nodes
            .into_iter()
            .filter_map(|node| node.ok())
            .flatten()
            .collect();

        // if they all ended in failure, then we can't find the node
        if returned_nodes.is_empty() {
            return Err(naur!("Could not find node, all nodes requests ended in failure"));
        }

        // it's possible that some of the nodes returned are actually the node we're looking for
        // so we check for that and return it if it's the case
        let target_node = returned_nodes.iter().find(|node| node.id() == target);

        if target_node.is_some() {
            return Ok(target_node.unwrap().clone());
        }

        // if we don't have the node, then we find the alpha closest nodes and ask them in turn
        let mut sorted_by_distance: Vec<_> = returned_nodes
            .into_iter()
            .map(|node| {
                let node_id = BigUint::from_bytes_be(node.id().as_bytes());
                let our_id = BigUint::from_bytes_be(self.our_id.as_bytes());
                let distance = our_id.bitxor(node_id);

                (node, distance)
            })
            .collect();
        sorted_by_distance.sort_unstable_by_key(|(_, distance)| distance.clone());

        // add all the nodes we have visited so far
        let seen_node: Arc<Mutex<HashSet<NodeInfo>>> = Arc::new(Mutex::new(HashSet::new()));
        {
            let mut seen = seen_node.lock().await;
            sorted_by_distance.iter().for_each(|(node, _)| {
                seen.insert(*node);
            });
        }

        let (tx, rx) = oneshot::channel();
        let tx = Arc::new(Mutex::new(Some(tx)));

        let starting_pool: Vec<NodeInfo> = sorted_by_distance.into_iter().take(3).map(|(node, _)| node).collect();

        let dht = self.clone();
        let target = target.clone();
        let mut parallel_find = tokio::spawn(async move {
            let _ = dht
                .recursive_find_from_pool(starting_pool, target.clone(), seen_node, tx)
                .await;
        });

        tokio::select! {
             _ = &mut parallel_find => {
                Err(naur!("Could not find node, all nodes requests ended in failure"))
            },
            Ok(target) = rx => {
                parallel_find.abort();
                Ok(target)
            },
        }
    }

    // attempt to find the target node via a peer on this address
    async fn find_nodes_via_peer(
        self: Arc<Self>,
        peer_addr: SocketAddrV4,
        target: NodeId,
    ) -> Result<Vec<NodeInfo>, OurError> {
        // construct the message to query our friends
        let txn_id = self.transaction_id_pool.next();
        let query = Krpc::new_find_node_query(TransactionId::from(txn_id), self.our_id, target);

        // send the message and await for a response
        let time_out = Duration::from_secs(15);
        let response = timeout(time_out, self.send_and_wait(query, peer_addr)).await??;

        if let Krpc::FindNodeGetPeersResponse(find_node_response) = response {
            // the nodes come back as one giant byte string, each 26 bytes is a node
            // we split them up and create a vector of them
            let mut nodes: Vec<_> = find_node_response.nodes().clone();

            // some clients will return duplicate nodes, so we remove them
            nodes.sort_unstable_by_key(|node| node.contact().0);
            nodes.dedup();

            Ok(nodes)
        } else {
            Err(naur!("Did not get a find node response"))
        }
    }

    pub async fn get_peers(self: Arc<Self>, info_hash: InfoHash) -> Result<(Token, Vec<PeerContact>), OurError> {
        // TODO: verify this
        // the info hash and node is occupy the same address space, nodes are supposed to store
        // info_hashes close to its id
        let target = NodeId(info_hash.0);

        // get all the closest nodes to the info_hash
        let closest_nodes: Vec<_> = self.routing_table.find_closest(target).into_iter().collect();

        let seen = Arc::new(Mutex::new(HashSet::new()));
        let (tx, mut rx) = oneshot::channel();
        let slot = Arc::new(Mutex::new(Some(tx)));

        let mut search = tokio::spawn(async move {
            self.recursive_get_peers_from_pool(closest_nodes, info_hash, seen.clone(), slot.clone())
                .await
        });

        return tokio::select! {
            _ = &mut search => {
                Err(naur!("All branches in get peers failed"))
            }
            Ok(result) = &mut rx => {
                trace!("Received peers from channel, cancelling search");
                search.abort();
                Ok(result)
            }
            else => {
                warn!("are all the tasks done = {:?}", search.is_finished());
                eprintln!("rx = {:?}",rx);
                panic!()
            }
        };
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
    async fn ask_her_for_peers(
        self: Arc<Self>,
        interlocutor: SocketAddrV4,
        info_hash: InfoHash,
    ) -> Result<(Option<Token>, Vec<NodeInfo>, Vec<PeerContact>), OurError> {
        // trace!("Asking {:?} for peers", interlocutor);
        // construct the message to query our friends
        let transaction_id = self.transaction_id_pool.next();

        let query = Krpc::new_get_peers_query(TransactionId::from(transaction_id), self.our_id.clone(), info_hash);

        // send the message and await for a response
        let time_out = Duration::from_secs(15);
        let response = timeout(time_out, self.send_and_wait(query, interlocutor)).await??;

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

    #[async_recursion]
    #[instrument(skip_all)]
    /// Given a pool of potential nodes, ask them concurrently to see if they have the node we're
    /// looking for, the target return is observed via the slot variable, once it has been filled,
    /// the caller should drop the future to cancel all remaining tasks
    async fn recursive_find_from_pool(
        self: Arc<Self>,
        mut starting_pool: Vec<NodeInfo>,
        finding: NodeId,
        seen: Arc<Mutex<HashSet<NodeInfo>>>,
        slot: Arc<Mutex<Option<Sender<NodeInfo>>>>,
    ) -> Result<(), OurError> {
        // filter the pool to only include nodes that we haven't seen yet
        starting_pool = async {
            let seen = seen.lock().await;
            let seen = starting_pool
                .into_iter()
                .filter(|node| !seen.contains(&node))
                .collect::<Vec<_>>();
            info!("len = {}", seen.len());

            seen
        }
        .await;

        // it's ok to assume that this will never get hit for the first time, since the starting
        // pool is always unseen
        if starting_pool.len() == 0 {
            return Err(naur!("Bottomed out"));
        }

        // ask all the nodes for target!
        let parallel_tasks: Vec<_> = starting_pool
            .into_iter()
            .map(|starting_node| {
                let seen = seen.clone();
                let dht = self.clone();
                let slot = slot.clone();
                async move {
                    let returned_nodes = dht
                        .clone()
                        .find_nodes_via_peer(starting_node.contact().0, finding)
                        .await?;

                    // see if we got the node we're looking for
                    return if let Some(node) = returned_nodes.iter().find(|node| node.id() == finding) {
                        // if we did, then we're done
                        let mut slot = slot.lock().await;
                        let slot = slot.deref_mut();
                        // lock the sender and sent the value
                        if let Some(_sender) = slot {
                            let slot = slot.take();
                            slot.expect("some one else should ready have finished sending and killed us")
                                .send(node.clone())
                                .expect("some one else should ready have finished sending and killed us");
                            Ok(())
                        } else {
                            Err(naur!("Cancelled"))
                        }
                    } else {
                        // if we didn't, then we add the nodes we got to the seen list and recurse
                        seen.lock().await.insert(starting_node.clone());

                        dht.recursive_find_from_pool(returned_nodes, finding, seen, slot).await
                    };
                }
            })
            .collect();

        // spawn all the tasks and await them
        let _results = parallel_tasks.par_spawn_and_await().await?;

        // if we ever reach here, that means we haven't been cancelled, which means nothing were
        // found
        Err(naur!("Bottmed out"))
    }

    /// TODO: need to add a hard recursion depth limit
    #[async_recursion]
    #[instrument(skip_all)]
    async fn recursive_get_peers_from_pool(
        self: Arc<Self>,
        mut starting_pool: Vec<NodeInfo>,
        finding: InfoHash,
        seen: Arc<Mutex<HashSet<NodeInfo>>>,
        slot: Arc<Mutex<Option<Sender<(Token, Vec<PeerContact>)>>>>,
    ) -> Result<(), OurError> {
        // filter the pool to only include nodes that we haven't seen yet
        starting_pool = async {
            let seen = seen.lock().await;
            starting_pool
                .into_iter()
                .filter(|node| !seen.contains(&node))
                .collect::<Vec<_>>()
        }
        .await;
        info!("Starting pool size: {:?}", starting_pool.len());

        if starting_pool.len() == 0 {
            debug!("bottomed out");
            return Err(naur!("Bottomed out"));
        }

        let mut requests = JoinSet::new();

        // ask all the nodes for target!
        for node in starting_pool {
            let seen = seen.clone();
            let this = self.clone();
            let slot = slot.clone();

            requests.spawn(async move {
                async move {
                    let (token, closest_nodes, peers) =
                        this.clone().ask_her_for_peers(node.contact().0, finding).await?;

                    if peers.is_empty() {
                        // need to keep asking
                        {
                            let mut seen = seen.lock().await;
                            seen.insert(node);
                        }

                        this.recursive_get_peers_from_pool(closest_nodes, finding.clone(), seen, slot)
                            .await
                    } else {
                        trace!("got success response, {peers:#?}");
                        let mut slot = slot.lock().await;
                        let slot = slot.take();

                        match slot {
                            Some(sender) => {
                                // TODO: revisit this
                                let _ = sender.send((token.expect("success response must have the token"), peers));
                                Ok(())
                            }
                            // this means some other search operation has already found a list of
                            // peers so we should stop here
                            None => Err(naur!("Cancelled")),
                        }
                    }
                }
            });
        }

        // spawn all the tasks and await them
        debug!("spawning {} tasks", requests.len());
        while let Some(_) = requests.join_next().await {}

        Err(naur!("Bottomed out"))
    }
}
