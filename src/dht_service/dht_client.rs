use crate::{
    dht_service::{transaction_id_pool::TransactionIdPool, DhtServiceFailure, MessageDemultiplexer},
    domain_knowledge::{BetterCompactPeerContact, BetterCompactNodeInfo, BetterInfoHash, BetterNodeId, CompactNodeContact, CompactPeerContact, NodeId},
    message::{InfoHash, Krpc},
    routing::RoutingTable,
    utils::ParSpawnAndAwait,
};
use async_recursion::async_recursion;
use either::Either;
use num::BigUint;
use std::{
    collections::HashSet,
    error::Error,
    fmt::{Display, Formatter},
    net::SocketAddrV4,
    ops::{BitXor, DerefMut},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{oneshot, oneshot::Sender, Mutex, RwLock},
    task::JoinError,
    time::timeout,
};
use tracing::{debug, info, instrument, trace, warn};

#[derive(Debug)]
pub struct DhtClientV4 {
    pub(crate) socket: Arc<UdpSocket>,
    pub(crate) our_id: BetterNodeId,
    pub(crate) demultiplexer: Arc<MessageDemultiplexer>,
    pub(crate) routing_table: Arc<RwLock<RoutingTable>>,
    pub(crate) socket_address: SocketAddrV4,
    pub(crate) transaction_id_pool: TransactionIdPool,
}

#[derive(Debug)]
pub enum RecursiveSearchError {
    BottomedOut,
    Cancelled,
    JoinError,
    DhtServiceFailure,
}

impl From<DhtServiceFailure> for RecursiveSearchError {
    fn from(_: DhtServiceFailure) -> Self {
        RecursiveSearchError::DhtServiceFailure
    }
}

impl From<JoinError> for RecursiveSearchError {
    fn from(_: JoinError) -> Self {
        RecursiveSearchError::JoinError
    }
}

impl Display for RecursiveSearchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for RecursiveSearchError {}

impl DhtClientV4 {
    /// A default DHT node when you really don't know anything about DHTs and just want to provide
    /// a port and IP address
    pub(crate) fn new(
        bind_addr: SocketAddrV4,
        socket: Arc<UdpSocket>,
        demultiplexer: Arc<MessageDemultiplexer>,
        routing_table: Arc<RwLock<RoutingTable>>,
        our_id: BetterNodeId,
    ) -> Self {
        DhtClientV4 {
            socket,
            demultiplexer,
            our_id,
            routing_table,
            socket_address: bind_addr,
            transaction_id_pool: TransactionIdPool::new(),
        }
    }

    /// Send a message out and await for a response.
    ///
    /// It does not alter the routing table, callers must decide what to do with the response.
    pub async fn send_message(&self, message: &Krpc, recipient: &SocketAddrV4) -> Result<Krpc, DhtServiceFailure> {
        let (tx, rx) = oneshot::channel();

        if let Ok(bytes) = bendy::serde::to_bytes(message) {
            self.demultiplexer.register(message.transaction_id().clone(), tx).await;
            self.socket.send_to(&bytes, recipient).await?;
        }
        let response = rx.await.unwrap();
        Ok(response)
    }

    pub async fn ping(self: Arc<Self>, recipient: SocketAddrV4) -> Result<(), DhtServiceFailure> {
        let this = &self;
        let transaction_id = self.transaction_id_pool.next();
        let ping_msg = Krpc::new_ping_query(hex::encode(transaction_id.to_be_bytes()), this.our_id.clone());

        let response = self.send_message(&ping_msg, &recipient).await?;

        return if let Krpc::PingAnnouncePeerResponse(response) = response {
            this.routing_table
                .write()
                .await
                .add_new_node(BetterCompactNodeInfo::new(response.target_id().clone(), BetterCompactPeerContact(recipient)));

            Ok(())
        } else {
            warn!("Unexpected response to ping: {:?}", response);
            Err(DhtServiceFailure {
                message: "Unexpected response to ping".to_string(),
            })
        };
    }

    // peers means something special here so you can't use it
    // ask_node_for_nodes just sounds stupid so fuck it, it's her then.
    // Why her and not them? Because I want to piss people off
    async fn ask_her_for_nodes(
        self: Arc<Self>,
        interlocutor: SocketAddrV4,
        target: BetterNodeId,
    ) -> Result<Vec<BetterCompactNodeInfo>, DhtServiceFailure> {
        // construct the message to query our friends
        let transaction_id = self.transaction_id_pool.next();
        let query = Krpc::new_find_node_query(hex::encode(transaction_id.to_be_bytes()), self.our_id.clone(), target);

        // send the message and await for a response
        let time_out = Duration::from_secs(15);
        let response = timeout(time_out, self.send_message(&query, &interlocutor)).await??;

        if let Krpc::FindNodeGetPeersNonCompliantResponse(find_node_response) = response {
            // the nodes come back as one giant byte string, each 26 bytes is a node
            // we split them up and create a vector of them
            let mut nodes: Vec<_> = find_node_response
                .nodes;

            // TODO: fix this
            // some clients will return duplicate nodes, so we remove them
            // nodes.sort_unstable_by_key(|node| {
            //     let ip: SocketAddrV4 = node.into();
            //     ip
            // });
            nodes.dedup();

            Ok(nodes)
        } else {
            Err(DhtServiceFailure {
                message: "Did not get an find node response".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn ask_her_for_peers(
        self: Arc<Self>,
        interlocutor: SocketAddrV4,
        target: BetterNodeId,
    ) -> Result<
        (
            Option<String>,
            Either<Vec<BetterCompactNodeInfo>, Vec<BetterCompactPeerContact>>,
        ),
        DhtServiceFailure,
    > {
        // trace!("Asking {:?} for peers", interlocutor);
        // construct the message to query our friends
        let transaction_id = self.transaction_id_pool.next();
        // TODO: wtf, it expects a token?
        let query = Krpc::new_get_peers_query(hex::encode(transaction_id.to_be_bytes()), self.our_id.clone(), BetterInfoHash("borken!".to_string()));

        // send the message and await for a response
        let time_out = Duration::from_secs(15);
        let response = timeout(time_out, self.send_message(&query, &interlocutor)).await??;
        return match response {
            Krpc::GetPeersDeferredResponse(response) => {
                // make sure we don't get duplicate nodes
                let mut nodes = response.nodes;

                // TODO: define an order for nodes??
                // nodes.sort_unstable_by_key(|node| *node.);
                nodes.dedup();

                trace!(
                    "got a deferred response from {}, returned nodes: {:#?}",
                    interlocutor,
                    &nodes
                );
                Ok((Some(response.token), Either::Left(nodes)))
            }
            Krpc::FindNodeGetPeersNonCompliantResponse(response) => {
                // make sure we don't get duplicate nodes
                let mut nodes = response.nodes;

                // TODO: define an order for nodes??
                // nodes.sort_unstable_by_key(|node| node.into());
                nodes.dedup();
                trace!(
                    "got a deferred response from {} (token missing), returned nodes {:#?}",
                    interlocutor,
                    &nodes
                );

                Ok((None, Either::Left(nodes)))
            }
            Krpc::GetPeersSuccessResponse(response) => {
                let mut values = response.values;
                // TODO: define an order for nodes??
                // values.sort_unstable_by_key(|value| value.into());
                values.dedup();

                trace!("got a success response from {}, values {:#?}", interlocutor, &values);
                Ok((Some(response.token), Either::Right(values)))
            }
            Krpc::ErrorResponse(response) => {
                warn!("Got an error response to get peers: {:?}", response);
                Err(DhtServiceFailure {
                    message: "Got an error response to get peers".to_string(),
                })
            }
            other => {
                warn!("Unexpected response to get peers: {:?}", other);
                Err(DhtServiceFailure {
                    message: "Unexpected response to get peers".to_string(),
                })
            }
        };
    }

    /// starting point of trying to find any nodes on the network
    pub async fn find_node(self: Arc<Self>, target: &BetterNodeId) -> Result<BetterCompactNodeInfo, DhtServiceFailure> {
        // if we already know the node, then no need for any network requests
        if let Some(node) = (&self).routing_table.read().await.find(target) {
            return Ok(node.contact.clone());
        }

        // find the closest nodes that we know
        let closest;
        {
            let table = (&self).routing_table.read().await;
            closest = table.find_closest(target).into_iter().cloned().collect::<Vec<_>>();
        }

        let returned_nodes = closest
            .into_iter()
            .map(|node| {
                // TODO: smelly
                let ip: SocketAddrV4 = node.contact.0;
                ip
            })
            .map(|ip| self.clone().ask_her_for_nodes(ip, *target))
            .collect::<Vec<_>>();

        let returned_nodes = returned_nodes.par_spawn_and_await().await?;

        let returned_nodes: Vec<_> = returned_nodes
            .into_iter()
            .filter(|node| node.is_ok())
            .map(|node| node.unwrap())
            .collect();

        // if they all ended in failure, then we can't find the node
        if returned_nodes.len() == 0 {
            return Err(DhtServiceFailure {
                message: "Could not find node, all nodes requests ended in failure".to_string(),
            });
        }

        // it's possible that some of the nodes returned are actually the node we're looking for
        // so we check for that and return it if it's the case
        let target_node = returned_nodes.into_iter().flatten().find(|node| node.node_id() == target);

        if target_node.is_some() {
            return Ok(target_node.unwrap().clone());
        }

        // if we don't have the node, then we find the alpha closest nodes and ask them in turn
        let mut sorted_by_distance: Vec<_> = returned_nodes
            .into_iter()
            .flatten()
            .map(|node| {
                let node_id = BigUint::from_bytes_be(node.node_id().0.as_bytes());
                let our_id = BigUint::from_bytes_be(self.our_id.0.as_bytes());
                let distance = our_id.bitxor(node_id);

                (node, distance)
            })
            .collect();
        sorted_by_distance.sort_unstable_by_key(|(_, distance)| distance.clone());

        // add all the nodes we have visited so far
        let seen_node: Arc<Mutex<HashSet<BetterCompactNodeInfo>>> = Arc::new(Mutex::new(HashSet::new()));
        {
            let mut seen = seen_node.lock().await;
            sorted_by_distance.iter().for_each(|(node, _)| {
                seen.insert(node.clone());
            });
        }

        let (tx, rx) = oneshot::channel();
        let tx = Arc::new(Mutex::new(Some(tx)));

        let starting_pool: Vec<BetterCompactPeerContact> =
            sorted_by_distance.into_iter().take(3).map(|(node, _)| node).collect();

        let dht = self.clone();
        let target = target.clone();
        let mut parallel_find = tokio::spawn(async move {
            let _ = dht
                .recursive_find_from_pool(starting_pool, target.clone(), seen_node, tx)
                .await;
        });

        tokio::select! {
             _ = &mut parallel_find => {
                Err(DhtServiceFailure {
                    message: "Could not find node, all nodes requests ended in failure".to_string(),
                })
            },
            Ok(target) = rx => {
                parallel_find.abort();
                Ok(target)
            },
        }
    }

    #[async_recursion]
    #[instrument(skip_all)]
    /// Given a pool of potential nodes, ask them concurrently to see if they have the node we're
    /// looking for, the target return is observed via the slot variable, once it has been filled,
    /// the caller should drop the future to cancel all remaining tasks
    async fn recursive_find_from_pool(
        self: Arc<Self>,
        mut starting_pool: Vec<BetterCompactPeerContact>,
        finding: NodeId,
        seen: Arc<Mutex<HashSet<BetterCompactPeerContact>>>,
        slot: Arc<Mutex<Option<Sender<BetterCompactPeerContact>>>>,
    ) -> Result<(), RecursiveSearchError> {
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
            return Err(RecursiveSearchError::BottomedOut);
        }

        // ask all the nodes for target!
        let parallel_tasks: Vec<_> = starting_pool
            .into_iter()
            .map(|starting_node| {
                let seen = seen.clone();
                let dht = self.clone();
                let slot = slot.clone();
                async move {
                    let returned_nodes = dht.clone().ask_her_for_nodes((&starting_node).into(), finding).await?;

                    // add the nodes to our routing table
                    {
                        let mut table = dht.routing_table.write().await;
                        returned_nodes.iter().for_each(|node| {
                            table.add_new_node(node.clone());
                        });
                    }

                    // see if we got the node we're looking for
                    return if let Some(node) = returned_nodes.iter().find(|node| node.node_id() == &finding) {
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
                            Err(RecursiveSearchError::Cancelled)
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
        Err(RecursiveSearchError::BottomedOut)
    }

    #[async_recursion]
    #[instrument(skip_all)]
    async fn recursive_get_peers_from_pool(
        self: Arc<Self>,
        mut starting_pool: Vec<BetterCompactNodeInfo>,
        finding: BetterNodeId,
        seen: Arc<Mutex<HashSet<BetterCompactNodeInfo>>>,
        slot: Arc<Mutex<Option<Sender<(Box<[u8]>, Vec<BetterCompactNodeInfo>)>>>>,
    ) -> Result<(), RecursiveSearchError> {
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
            return Err(RecursiveSearchError::BottomedOut);
        }

        // ask all the nodes for target!
        let parallel_tasks: Vec<_> = starting_pool
            .into_iter()
            .map(|starting_node| {
                let seen = seen.clone();
                let dht = self.clone();
                let slot = slot.clone();
                async move {
                    let (token, returned) = dht.clone().ask_her_for_peers((&starting_node).into(), finding).await?;

                    return match returned {
                        Either::Left(mut deferred) => {
                            trace!("got deferred response, {deferred:#?}");
                            // make sure we don't get duplicate nodes
                            deferred.dedup();
                            {
                                let mut seen = seen.lock().await;
                                seen.insert(starting_node.clone());
                            }

                            dht.recursive_get_peers_from_pool(deferred, finding, seen, slot).await
                        }
                        Either::Right(success) => {
                            trace!("got success response, {success:#?}");
                            let mut slot = slot.lock().await;
                            let slot = slot.take();

                            match slot {
                                Some(sender) => {
                                    let _ =
                                        sender.send((token.expect("success response must have the token"), success));
                                    Ok(())
                                }
                                None => Err(RecursiveSearchError::Cancelled),
                            }
                        }
                    };
                }
            })
            .collect();

        // spawn all the tasks and await them
        debug!("spawning {} tasks", parallel_tasks.len());
        let _results = parallel_tasks.par_spawn_and_await().await?;
        Err(RecursiveSearchError::BottomedOut)
    }

    pub async fn get_peers(
        self: Arc<Self>,
        info_hash: InfoHash,
    ) -> Result<(Box<[u8]>, Vec<CompactPeerContact>), DhtServiceFailure> {
        // get all the closest nodes to the info_hash
        let closest_nodes: Vec<_> = self
            .routing_table
            .read()
            .await
            .find_closest(&info_hash)
            .into_iter()
            .cloned()
            .collect();

        let seen = Arc::new(Mutex::new(HashSet::new()));
        let (tx, mut rx) = oneshot::channel();
        let slot = Arc::new(Mutex::new(Some(tx)));

        let mut search = tokio::spawn(async move {
            self.recursive_get_peers_from_pool(closest_nodes, info_hash.clone(), seen.clone(), slot.clone())
                .await
        });

        return tokio::select! {
            _ = &mut search => {
                Err(DhtServiceFailure {
                    message: "all branches in get peers failed".to_string(),
                })
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
        info_hash: BetterInfoHash,
        port: Option<u16>,
        token: String,
    ) -> Result<(), DhtServiceFailure> {
        let transaction_id = self.transaction_id_pool.next();
        let query = if let Some(port) = port {
            Krpc::new_announce_peer_query(
                hex::encode(transaction_id.to_be_bytes()),
                info_hash,
                self.our_id.clone(),
                port,
                true,
                token,
            )
        } else {
            Krpc::new_announce_peer_query(
                hex::encode(transaction_id.to_be_bytes()),
                info_hash,
                self.our_id.clone(),
                self.socket_address.port(),
                true,
                token,
            )
        };

        let response = self.send_message(&query, &recipient).await?;

        return match response {
            Krpc::PingAnnouncePeerResponse(_) => Ok(()),
            Krpc::ErrorResponse(err) => Err(DhtServiceFailure {
                message: format!("node responded with an error to our announce peer request {err:?}"),
            }),
            _ => Err(DhtServiceFailure {
                message: "non-compliant response from DHT node".to_string(),
            }),
        };
    }
}
