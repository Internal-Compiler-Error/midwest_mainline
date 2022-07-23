use crate::{
    dht_service::{dht_server::DhtServer, transaction_id_pool::TransactionIdPool},
    domain_knowledge::{CompactNodeContact, CompactPeerContact, NodeId},
    message::{InfoHash, Krpc},
    routing::RoutingTable,
    utils::ParSpawnAndAwait,
};
use async_recursion::async_recursion;
use either::Either;
use num::BigUint;
use rand::{Rng, RngCore};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::{Display, Formatter},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    ops::{BitXor, DerefMut},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot, oneshot::Sender, Mutex, RwLock},
    task::{Builder, JoinError, JoinHandle, JoinSet},
    time::{error::Elapsed, timeout},
};

use crate::message::TransactionId;
use tracing::{debug, info, instrument, log::warn, trace};

mod dht_server;
mod transaction_id_pool;

#[derive(Debug)]
struct DhtServiceV4 {
    inner: Arc<DhtServiceInnerV4>,
    helper_tasks: Vec<JoinHandle<()>>,
}

#[derive(Debug)]
pub(crate) struct DhtServiceInnerV4 {
    socket: Arc<UdpSocket>,
    our_id: [u8; 20],
    request_registry: Arc<PacketDemultiplexer>,
    routing_table: Arc<RwLock<RoutingTable>>,
    socket_address: SocketAddrV4,
    transaction_id_pool: TransactionIdPool,
    // helper_tasks: Vec<JoinHandle<()>>,
}

#[derive(Debug)]
pub(crate) struct DhtServiceFailure {
    message: String,
}

#[derive(Debug)]
pub enum RecursiveSearchError {
    BottomedOut,
    Cancelled,
    JoinError,
    DhtServiceFailure,
}

impl From<DhtServiceFailure> for RecursiveSearchError {
    fn from(error: DhtServiceFailure) -> Self {
        RecursiveSearchError::DhtServiceFailure
    }
}

impl From<JoinError> for RecursiveSearchError {
    fn from(error: JoinError) -> Self {
        RecursiveSearchError::JoinError
    }
}

impl Display for RecursiveSearchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for RecursiveSearchError {}

impl Display for DhtServiceFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for DhtServiceFailure {}

#[derive(Debug)]
struct PacketDemultiplexer {
    pending_responses: Mutex<HashMap<TransactionId, Sender<Krpc>>>,
    query_queue: Mutex<mpsc::Sender<(Krpc, SocketAddrV4)>>,
    incoming_messages: Mutex<mpsc::Receiver<(Krpc, SocketAddrV4)>>,
}

impl PacketDemultiplexer {
    pub fn new(
        incoming_queue: mpsc::Receiver<(Krpc, SocketAddrV4)>,
        query_channel: mpsc::Sender<(Krpc, SocketAddrV4)>,
    ) -> Self {
        Self {
            pending_responses: Mutex::new(HashMap::new()),
            query_queue: Mutex::new(query_channel),
            incoming_messages: Mutex::new(incoming_queue),
        }
    }

    #[instrument(skip_all)]
    pub async fn lifetime_loop(&self) {
        let mut incoming_messages = self.incoming_messages.lock().await;
        while let Some((msg, socket_addr)) = incoming_messages.recv().await {
            let id = msg.transaction_id();
            trace!("received message for transaction id {:?}", hex::encode_upper(&*id));

            // see if we have a slot for this transaction id, if we do, that means one of the
            // messages that we expect, otherwise the message is a query we need to handle
            if let Some(sender) = self.pending_responses.lock().await.remove(id) {
                // failing means we're no longer interested, which is ok
                let _ = sender.send(msg);
            } else {
                self.query_queue
                    .lock()
                    .await
                    .send((msg, socket_addr))
                    .await
                    .expect("the server died before the demultiplexer could send the message");
            }
        }
    }

    /// Tell the placer we should expect some messages
    pub async fn register(&self, transaction_id: TransactionId, sending_half: oneshot::Sender<Krpc>) {
        let mut guard = self.pending_responses.lock().await;
        // it's possible that the response never came and we a new request is now using the same
        // transaction id
        let occupied = guard.insert(transaction_id, sending_half);
        // warn!("Transaction ID {transaction_id} already occupied, new sender inserted");
    }
}

impl From<std::io::Error> for DhtServiceFailure {
    fn from(error: std::io::Error) -> Self {
        DhtServiceFailure {
            message: error.to_string(),
        }
    }
}

impl From<tokio::time::error::Elapsed> for DhtServiceFailure {
    fn from(error: tokio::time::error::Elapsed) -> Self {
        DhtServiceFailure {
            message: error.to_string(),
        }
    }
}

impl From<JoinError> for DhtServiceFailure {
    fn from(error: JoinError) -> Self {
        DhtServiceFailure {
            message: error.to_string(),
        }
    }
}

impl DhtServiceInnerV4 {
    /// A default DHT node when you really don't know anything about DHTs and just want to provide
    /// a port and IP address
    async fn new_with_random_id(
        bind_addr: SocketAddrV4,
        external_addr: Ipv4Addr,
    ) -> Result<(Self, Vec<JoinHandle<()>>), DhtServiceFailure> {
        let socket = UdpSocket::bind(&bind_addr).await?;

        let id = random_idv4(&external_addr, rand::thread_rng().gen::<u8>());

        let socket = Arc::new(socket);
        let routing_table = RoutingTable::new(&id);

        let (incoming_packets_tx, incoming_packets_rx) = mpsc::channel(1024);
        let (queries_tx, queries_rx) = mpsc::channel(1024);

        // keep reading from sockets and place them on a queue for another task to place them into
        // the right slot
        let reading_socket = socket.clone();
        let socket_reader = async move {
            let mut buf = [0u8; 1024];
            loop {
                let (amount, socket_addr) = reading_socket.recv_from(&mut buf).await?;
                trace!("packet from {socket_addr}");
                if let Ok(msg) = bendy::serde::from_bytes::<Krpc>(&buf[..amount]) {
                    let socket_addr = {
                        match socket_addr {
                            SocketAddr::V4(addr) => addr,
                            _ => panic!("Expected V4 socket address"),
                        }
                    };

                    incoming_packets_tx.send((msg, socket_addr)).await.unwrap();
                } else {
                    warn!(
                        "Failed to parse message from {socket_addr}, bytes = {:?}",
                        &buf[..amount]
                    );
                }
            }
            Ok::<_, DhtServiceFailure>(())
        };

        let handle1 = Builder::new().name("socket reader").spawn(async move {
            let _ = socket_reader.await;
        });

        let message_registry = PacketDemultiplexer::new(incoming_packets_rx, queries_tx);
        let message_registry = Arc::new(message_registry);

        // place the messages into the right slot

        let message_registry1 = message_registry.clone();
        let message_placing = async move {
            message_registry1.lifetime_loop().await;
            ()
        };

        let handle2 = Builder::new().name("message registry").spawn(message_placing);

        let routing_table = Arc::new(RwLock::new(routing_table));

        let server = Arc::new(DhtServer::new(
            queries_rx,
            socket.clone(),
            id.clone(),
            routing_table.clone(),
        ));
        // let mut server = Arc::new(server);
        let handle3 = Builder::new().name("server").spawn(async move {
            let _ = server.handle_requests().await;
            ()
        });

        Ok((
            DhtServiceInnerV4 {
                socket,
                request_registry: message_registry,
                our_id: id,
                routing_table,
                socket_address: bind_addr,
                transaction_id_pool: TransactionIdPool::new(),
                // helper_tasks: vec![handle1, handle2, handle3],
            },
            vec![handle1, handle2, handle3],
        ))
    }

    // async fn run(&mut self) {
    //     for task in &mut self.helper_tasks {
    //         let _ = task.await;
    //     }
    // }

    /// Send a message out and await for a response.
    ///
    /// It does not alter the routing table, callers must decide what to do with the response.
    async fn send_message(&self, message: &Krpc, recipient: &SocketAddrV4) -> Result<Krpc, DhtServiceFailure> {
        let (tx, rx) = oneshot::channel();

        if let Ok(bytes) = bendy::serde::to_bytes(message) {
            self.request_registry
                .register(message.transaction_id().clone(), tx)
                .await;
            self.socket.send_to(&bytes, recipient).await?;
        }
        let response = rx.await.unwrap();
        Ok(response)
    }

    async fn ping(self: Arc<Self>, recipient: SocketAddrV4) -> Result<(), DhtServiceFailure> {
        let this = &self;
        let transaction_id = self.transaction_id_pool.next();
        let ping_msg = Krpc::new_ping_query(Box::new(transaction_id.to_be_bytes()), this.our_id);

        let response = self.send_message(&ping_msg, &recipient).await?;

        return if let Krpc::PingAnnouncePeerResponse(response) = response {
            this.routing_table
                .write()
                .await
                .add_new_node(CompactNodeContact::from_node_id_and_addr(&response.body.id, &recipient));

            Ok(())
        } else {
            warn!("Unexpected response to ping: {:?}", response);
            Err(DhtServiceFailure {
                message: "Unexpected response to ping".to_string(),
            })
        };
    }

    // you peers means something special here so you can't use it
    // ask_node_for_nodes just sounds stupid so fuck it, it's her then.
    // Why her and not them? Because I want to piss people off
    async fn ask_her_for_nodes(
        self: Arc<Self>,
        interlocutor: SocketAddrV4,
        target: NodeId,
    ) -> Result<Vec<CompactNodeContact>, DhtServiceFailure> {
        // construct the message to query our friends
        let transaction_id = self.transaction_id_pool.next();
        let query = Krpc::new_find_node_query(Box::new(transaction_id.to_be_bytes()), self.our_id, target);

        // send the message and await for a response
        let time_out = Duration::from_secs(15);
        let response = timeout(time_out, self.send_message(&query, &interlocutor)).await??;

        if let Krpc::FindNodeGetPeersNonCompliantResponse(find_node_response) = response {
            // the nodes come back as one giant byte string, each 26 bytes is a node
            // we split them up and create a vector of them
            let mut nodes: Vec<_> = find_node_response
                .body
                .nodes
                .chunks_exact(26)
                .map(|node| CompactNodeContact::new(node.try_into().unwrap()))
                .collect();

            // some clients will return duplicate nodes, so we remove them
            nodes.sort_unstable_by_key(|node| {
                let ip: SocketAddrV4 = node.into();
                ip
            });
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
        target: InfoHash,
    ) -> Result<
        (
            Option<Box<[u8]>>,
            Either<Vec<CompactNodeContact>, Vec<CompactPeerContact>>,
        ),
        DhtServiceFailure,
    > {
        // trace!("Asking {:?} for peers", interlocutor);
        // construct the message to query our friends
        let transaction_id = self.transaction_id_pool.next();
        let query = Krpc::new_get_peers_query(Box::new(transaction_id.to_be_bytes()), self.our_id, target);

        // send the message and await for a response
        let time_out = Duration::from_secs(15);
        let response = timeout(time_out, self.send_message(&query, &interlocutor)).await??;
        return match response {
            Krpc::GetPeersDeferredResponse(response) => {
                // make sure we don't get duplicate nodes
                let mut nodes: Vec<_> = response
                    .body
                    .nodes
                    .chunks_exact(26)
                    .map(|node| CompactNodeContact::new(node.try_into().unwrap()))
                    .collect();

                // todo: define an order for nodes??
                // nodes.sort_unstable_by_key(|node| node.into());
                nodes.dedup();

                trace!(
                    "got a deferred response from {}, returned nodes: {:#?}",
                    interlocutor,
                    &nodes
                );
                Ok((Some(response.body.token), Either::Left(nodes)))
            }
            Krpc::FindNodeGetPeersNonCompliantResponse(response) => {
                // make sure we don't get duplicate nodes
                let mut nodes: Vec<_> = response
                    .body
                    .nodes
                    .chunks_exact(26)
                    .map(|node| CompactNodeContact::new(node.try_into().unwrap()))
                    .collect();

                // todo: define an order for nodes??
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
                let mut values = response.body.values;
                // todo: define an order for nodes??
                // values.sort_unstable_by_key(|value| value.into());
                values.dedup();

                trace!("got a success response from {}, values {:#?}", interlocutor, &values);
                Ok((Some(response.body.token), Either::Right(values)))
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
    async fn find_node(self: Arc<Self>, target: &NodeId) -> Result<CompactNodeContact, DhtServiceFailure> {
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
            .iter()
            .map(|node| {
                let ip: SocketAddrV4 = node.into();
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
        let target_node = returned_nodes.iter().flatten().find(|node| node.node_id() == target);

        if target_node.is_some() {
            return Ok(target_node.unwrap().clone());
        }

        // if we don't have the node, then we find the alpha closest nodes and ask them in turn
        let mut sorted_by_distance: Vec<_> = returned_nodes
            .into_iter()
            .flatten()
            .map(|node| {
                let node_id = BigUint::from_bytes_be(node.node_id());
                let our_id = BigUint::from_bytes_be(&self.our_id);
                let distance = our_id.bitxor(node_id);

                (node, distance)
            })
            .collect();
        sorted_by_distance.sort_unstable_by_key(|(_, distance)| distance.clone());

        // add all the nodes we have visited so far
        let seen_node: Arc<Mutex<HashSet<CompactNodeContact>>> = Arc::new(Mutex::new(HashSet::new()));
        {
            let mut seen = seen_node.lock().await;
            sorted_by_distance.iter().for_each(|(node, _)| {
                seen.insert(node.clone());
            });
        }

        let (tx, rx) = oneshot::channel();
        let tx = Arc::new(Mutex::new(Some(tx)));

        let starting_pool: Vec<CompactNodeContact> =
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
        mut starting_pool: Vec<CompactNodeContact>,
        finding: NodeId,
        seen: Arc<Mutex<HashSet<CompactNodeContact>>>,
        slot: Arc<Mutex<Option<Sender<CompactNodeContact>>>>,
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
                        if let Some(sender) = slot {
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
        let results = parallel_tasks.par_spawn_and_await().await?;

        // if we ever reach here, that means we haven't been cancelled, which means nothing were
        // found
        Err(RecursiveSearchError::BottomedOut)
    }

    #[async_recursion]
    #[instrument(skip_all)]
    async fn recursive_get_peers_from_pool(
        self: Arc<Self>,
        mut starting_pool: Vec<CompactNodeContact>,
        finding: InfoHash,
        seen: Arc<Mutex<HashSet<CompactNodeContact>>>,
        slot: Arc<Mutex<Option<Sender<(Box<[u8]>, Vec<CompactPeerContact>)>>>>,
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
                            info!("got deferred response, {deferred:#?}");
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
        let results = parallel_tasks.par_spawn_and_await().await?;
        Err(RecursiveSearchError::BottomedOut)
    }

    /// obtain all the peers that c
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

    async fn announce_peers(
        self: Arc<Self>,
        recipient: SocketAddrV4,
        info_hash: InfoHash,
        port: Option<u16>,
        token: Box<[u8]>,
    ) -> Result<(), DhtServiceFailure> {
        let transaction_id = self.transaction_id_pool.next();
        let query = if let Some(port) = port {
            Krpc::new_announce_peer_query(
                Box::new(transaction_id.to_be_bytes()),
                info_hash,
                self.our_id,
                port,
                true,
                token,
            )
        } else {
            Krpc::new_announce_peer_query(
                Box::new(transaction_id.to_be_bytes()),
                info_hash,
                self.our_id,
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

// impl Drop for DhtServiceInnerV4 {
//     fn drop(&mut self) {
//         // stop all tasks required to keep ourself alive
//         self.helper_tasks.iter().for_each(|h| h.abort());
//     }
// }

fn random_idv4(external_ip: &Ipv4Addr, rand: u8) -> NodeId {
    let mut rng = rand::thread_rng();
    let r = rand & 0x07;
    let mut id = [0u8; 20];
    let mut ip = external_ip.octets();
    let mask = [0x03, 0x0f, 0x3f, 0xff];

    for (ip, mask) in ip.iter_mut().zip(mask.iter()) {
        *ip &= mask;
    }

    ip[0] |= r << 5;
    let crc = crc32c::crc32c(&ip);

    id[0] = (crc >> 24) as u8;
    id[1] = (crc >> 16) as u8;
    id[2] = (((crc >> 8) & 0xf8) as u8) | (rng.gen::<u8>() & 0x7);

    rng.fill_bytes(&mut id[3..19]);

    id[19] = rand;

    id
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BootstrapError {
    TimedOut,
}

impl From<Elapsed> for BootstrapError {
    fn from(_: Elapsed) -> Self {
        BootstrapError::TimedOut
    }
}

impl DhtServiceV4 {
    #[instrument(skip_all)]
    pub async fn bootstrap_with_random_id(
        bind_addr: SocketAddrV4,
        external_addr: Ipv4Addr,
        known_nodes: Vec<SocketAddrV4>,
    ) -> Result<Self, DhtServiceFailure> {
        let (inner, tasks) = DhtServiceInnerV4::new_with_random_id(bind_addr, external_addr).await?;
        let inner = Arc::new(inner);
        let dht = Self {
            inner: inner.clone(),
            helper_tasks: tasks,
        };

        // ask all the known nodes for ourselves
        let mut join_set = JoinSet::new();

        for contact in known_nodes {
            join_set
                .build_task()
                .name(&*format!("bootstrap with {contact}"))
                .spawn(Self::bootstrap_single(inner.clone(), contact));
        }

        while let Some(_) = join_set.join_next().await {
            // do nothing
        }
        info!("DHT bootstrapped");
        Ok(dht)
    }

    pub async fn run(self) {
        let inner = self.inner.clone();
        for task in self.helper_tasks {
            let _ = task.await;
        }
    }

    #[instrument(skip_all)]
    async fn bootstrap_single(dht: Arc<DhtServiceInnerV4>, contact: SocketAddrV4) -> Result<(), BootstrapError> {
        let our_id = dht.our_id.clone();
        let transaction_id = dht.transaction_id_pool.next();
        let query = Krpc::new_find_node_query(Box::new(transaction_id.to_be_bytes()), our_id, our_id.clone());

        info!("bootstrapping with {contact}");
        let response = timeout(Duration::from_secs(5), async {
            dht.send_message(&query, &contact)
                .await
                .expect("failure to send message")
        })
        .await?;

        if let Krpc::FindNodeGetPeersNonCompliantResponse(response) = response {
            let nodes = response.body.nodes;
            let mut nodes: Vec<_> = nodes
                .chunks_exact(26)
                .map(|x| CompactNodeContact::new(x.try_into().unwrap()))
                .collect();
            {
                // add the bootstrapping node to our routing table
                let mut table = dht.routing_table.write().await;
                table.add_new_node(CompactNodeContact::from_node_id_and_addr(&response.body.id, &contact));
            }

            nodes.dedup();

            for node in &nodes {
                dht.routing_table.write().await.add_new_node(node.clone());
            }

            let _ = dht.clone().find_node(&dht.our_id).await;
            info!("bootstrapping with {contact} complete");
        }
        Ok(())
    }

    async fn find_node(&self, target: &NodeId) -> Result<CompactNodeContact, DhtServiceFailure> {
        let dht = self.inner.clone();
        dht.find_node(target).await
    }

    async fn get_peers(&self, info_hash: &InfoHash) -> Result<(Box<[u8]>, Vec<CompactPeerContact>), DhtServiceFailure> {
        let dht = self.inner.clone();
        dht.get_peers(*info_hash).await
    }

    async fn ping(&self, contact: &SocketAddrV4) -> Result<(), DhtServiceFailure> {
        let dht = self.inner.clone();
        dht.ping(*contact).await
    }

    async fn announce_peer(
        &self,
        info_hash: &InfoHash,
        contact: &SocketAddrV4,
        port: Option<u16>,
        token: Box<[u8]>,
    ) -> Result<(), DhtServiceFailure> {
        let dht = self.inner.clone();
        dht.announce_peers(*contact, *info_hash, port, token).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        dht_service::{DhtServiceInnerV4, DhtServiceV4},
        message::Krpc,
    };
    use opentelemetry::global;
    use rand::RngCore;
    use std::{
        net::SocketAddrV4,
        str::FromStr,
        sync::{Arc, Once},
    };
    use tokio::{
        net::UdpSocket,
        time::{self, timeout},
    };
    use tracing::info;
    use tracing_subscriber::{filter::LevelFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt, Layer};

    static TEST_INIT: Once = Once::new();

    fn set_up_tracing() {
        let _ = color_eyre::install();
        let fmt_layer = fmt::layer()
            .compact()
            .with_line_number(true)
            .with_filter(LevelFilter::DEBUG);

        global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
        let tracer = opentelemetry_jaeger::new_pipeline().install_simple().unwrap();

        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(console_subscriber::spawn())
            .with(telemetry)
            .with(fmt_layer)
            .init();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bootstrap() -> color_eyre::Result<()> {
        TEST_INIT.call_once(set_up_tracing);

        let external_ip = public_ip::addr_v4().await.unwrap();

        let dht = DhtServiceV4::bootstrap_with_random_id(
            SocketAddrV4::from_str("0.0.0.0:51413").unwrap(),
            external_ip,
            vec![
                // dht.tansmissionbt.com
                "87.98.162.88:6881".parse().unwrap(),
                // router.utorrent.com
                "67.215.246.10:6881".parse().unwrap(),
                // router.bittorrent.com, ironically that this almost never responds
                "82.221.103.244:6881".parse().unwrap(),
                // dht.aelitis.com
                "174.129.43.152:6881".parse().unwrap(),
            ],
        );

        let dht = timeout(time::Duration::from_secs(60), dht).await??;
        info!("Now I'm bootstrapped!");
        {
            let table = dht.inner.routing_table.read().await;
            info!(
                "we've found {:?} nodes and recorded in our routing table",
                table.node_count()
            );
        }

        let dht = Arc::new(dht);

        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; 20];
        rng.fill_bytes(&mut bytes);

        let node = dht.find_node(&bytes).await;
        if let Ok(node) = node {
            info!("found node {:?}", node);
        } else {
            info!("I guess we just didn't find anything")
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn find_peers() -> color_eyre::Result<()> {
        TEST_INIT.call_once(set_up_tracing);

        let external_ip = public_ip::addr_v4().await.unwrap();

        let dht = DhtServiceV4::bootstrap_with_random_id(
            SocketAddrV4::from_str("0.0.0.0:51413").unwrap(),
            external_ip,
            vec![
                // dht.tansmissionbt.com
                "87.98.162.88:6881".parse().unwrap(),
                // router.utorrent.com
                // "67.215.246.10:6881".parse().unwrap(),
            ],
        )
        .await;

        // let info_hash = BigUint::from_str_radix("233b78ca585fe0a8c9e8eb4bda03f52e8b6f554b", 16).unwrap();
        // let info_hash = info_hash.to_bytes_be();
        //
        // let (token, peers) = dht.inner.get_peers(info_hash.as_slice().try_into()?).await?;
        // info!("token {token:?}, peers {peers:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn server() -> color_eyre::Result<()> {
        TEST_INIT.call_once(set_up_tracing);

        let external_ip = public_ip::addr_v4().await.unwrap();

        let dht = DhtServiceInnerV4::new_with_random_id(SocketAddrV4::from_str("0.0.0.0:51413")?, external_ip).await?;
        info!("dht created");

        let fake_client = async move {
            info!("starting to bind");
            let fake_peer_socket = UdpSocket::bind(SocketAddrV4::from_str("127.0.0.1:0")?).await?;
            fake_peer_socket
                .connect(SocketAddrV4::from_str("127.0.0.1:51413")?)
                .await?;
            info!("connected to dht");

            for i in 0..5 {
                let ping = Krpc::new_ping_query(Box::new([b'a', b'a' + i]), b"abcdefghij0123456789".clone());
                let serialized = bendy::serde::to_bytes(&ping)?;

                fake_peer_socket.send(&serialized).await?;

                let mut buf = [0u8; 1024];
                let len = fake_peer_socket.recv(&mut buf).await?;

                let msg = bendy::serde::from_bytes::<Krpc>(&buf[..len])?;

                // add some checks to ensure this is the stuff we actually expect in the future
                // but since there is no way to know the id of the dht right now, we can't do that
            }
            Ok::<_, color_eyre::Report>(())
        };
        let client_handle = tokio::spawn(fake_client);

        let _ = client_handle.await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run() -> color_eyre::Result<()> {
        TEST_INIT.call_once(set_up_tracing);
        let external_ip = public_ip::addr_v4().await.unwrap();

        let dht = DhtServiceV4::bootstrap_with_random_id(
            SocketAddrV4::from_str("0.0.0.0:51413").unwrap(),
            external_ip,
            vec![
                // dht.tansmissionbt.com
                "87.98.162.88:6881".parse().unwrap(),
                // router.utorrent.com
                "67.215.246.10:6881".parse().unwrap(),
                // router.bittorrent.com, ironically that this almost never responds
                "82.221.103.244:6881".parse().unwrap(),
            ],
        )
        .await?;

        dht.run().await;

        Ok(())
    }
}
