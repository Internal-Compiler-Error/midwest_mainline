use crate::{
    dht_service::transaction_id_pool::TransactionIdPool,
    domain_knowledge::{CompactNodeContact, NodeId},
    message::{
        announce_peer_query::AnnouncePeerQuery, find_node_query::FindNodeQuery, find_node_response::FindNodeResponse,
        get_peers_deferred_response::GetPeersDeferredResponse, get_peers_query::GetPeersQuery,
        get_peers_success_response::GetPeersSuccessResponse, ping_announce_peer_response::PingAnnouncePeerResponse,
        ping_query::PingQuery, Krpc,
    },
    routing::RoutingTable,
    utils::ParSpawnAndAwait,
};
use async_recursion::async_recursion;
use either::Either;
use futures::future::join_all;
use num_bigint::BigUint;
use rand::{rngs, rngs::SmallRng, Rng, RngCore, SeedableRng};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::{Display, Formatter},
    future::Future,
    mem::forget,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    ops::{BitXor, Deref, DerefMut, Sub},
    pin::Pin,
    sync::Arc,
    task::{
        Context,
        Poll::{self, Ready},
    },
    time::Duration,
};
use tokio::{
    net::{self, UdpSocket},
    runtime::{Handle, Runtime},
    spawn,
    sync::{mpsc, oneshot, oneshot::Sender, Mutex, RwLock},
    task::{self, JoinHandle},
    time::{error::Elapsed, timeout},
};
use tower::Service;
use tracing::{error, event, info, instrument, log::warn, span, trace, Level};

mod transaction_id_pool;

#[derive(Debug)]
struct DhtServiceV4 {
    inner: Arc<DhtServiceInnerV4>,
}

#[derive(Debug)]
struct DhtServiceInnerV4 {
    socket: Arc<UdpSocket>,
    our_id: [u8; 20],
    request_registry: Arc<RequestRegistry>,
    routing_table: RwLock<RoutingTable>,
    socket_address: SocketAddrV4,
    transaction_id_pool: TransactionIdPool,
    helper_tasks: Vec<JoinHandle<()>>,
}

#[derive(Debug)]
struct DhtServiceFailure {
    message: String,
}

#[derive(Debug)]
struct BottomedOut;

impl Display for BottomedOut {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bottomed out")
    }
}

impl Display for DhtServiceFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for DhtServiceFailure {}

#[derive(Debug)]
struct RequestRegistry {
    slots: Mutex<HashMap<u16, oneshot::Sender<Krpc>>>,
    query_queue: Mutex<mpsc::Sender<Krpc>>,
    packet_queue: Mutex<mpsc::Receiver<Krpc>>,
}

impl RequestRegistry {
    pub fn new(incoming_queue: mpsc::Receiver<Krpc>, query_channel: mpsc::Sender<Krpc>) -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
            query_queue: Mutex::new(query_channel),
            packet_queue: Mutex::new(incoming_queue),
        }
    }

    pub async fn lifetime_loop(&self) {
        let mut queue = self.packet_queue.lock().await;
        loop {
            if let Some(msg) = queue.recv().await {
                let id = u16::from_be_bytes(msg.transaction_id().clone());

                // see if we have a slot for this transaction id, if we do, that means one of the
                // messages that we expect, otherwise the message is a query we need to handle

                if let Some(sender) = self.slots.lock().await.remove(&id) {
                    match sender.send(msg) {
                        Err(err) => {
                            info!("Failed to send response to request: {err:?}");
                        }
                        _ => {}
                    }
                } else {
                    self.query_queue.lock().await.send(msg);
                }
            }
        }
    }

    /// Tell the placer we should expect some messages
    pub async fn register(&self, transaction_id: u16, sending_half: oneshot::Sender<Krpc>) {
        let mut guard = self.slots.lock().await;
        // it's possible that the response never came and we a new request is now using the same
        // transaction id
        let occupied = guard.insert(transaction_id, sending_half);
        warn!("Transaction ID {transaction_id} already occupied, new sender inserted");
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

impl From<tokio::task::JoinError> for DhtServiceFailure {
    fn from(error: tokio::task::JoinError) -> Self {
        DhtServiceFailure {
            message: error.to_string(),
        }
    }
}

impl DhtServiceInnerV4 {
    /// A default DHT node when you really don't know anything about DHTs and just want to provide
    /// a port and IP address
    async fn new_with_random_id(address: SocketAddrV4) -> Result<Self, DhtServiceFailure> {
        let socket = UdpSocket::bind(&address).await?;

        // randomly pick our ID
        let mut id = [0u8; 20];
        rand::thread_rng().fill_bytes(&mut id);

        let socket = Arc::new(socket);
        let routing_table = RoutingTable::new(&id);

        let (incoming_tx, incoming_rx) = mpsc::channel(128);
        let (queries_tx, queries_rx) = mpsc::channel(128);

        // todo: eventually we to read the queries
        forget(queries_rx);

        // keep reading from sockets and place them on a queue for another task to place them into
        // the right slot
        let reading_socket = socket.clone();
        let socket_reader = async move {
            let mut buf = [0u8; 1024];
            loop {
                let (amount, _) = reading_socket.recv_from(&mut buf).await?;
                if let Ok(msg) = bendy::serde::from_bytes(&buf[..amount]) {
                    incoming_tx.send(msg).await.unwrap();
                }
            }
            Ok::<_, DhtServiceFailure>(())
        };

        let handle1 = task::Builder::new().name("socket reader").spawn(async move {
            socket_reader.await;
        });

        let message_registry = RequestRegistry::new(incoming_rx, queries_tx);
        let message_registry = Arc::new(message_registry);

        // place the messages into the right slot

        let message_registry1 = message_registry.clone();
        let message_placing = async move {
            message_registry1.lifetime_loop().await;
            ()
        };

        let handle2 = task::Builder::new().name("message registry").spawn(message_placing);

        Ok(DhtServiceInnerV4 {
            socket,
            request_registry: message_registry,
            our_id: id,
            routing_table: RwLock::new(routing_table),
            socket_address: address,
            transaction_id_pool: TransactionIdPool::new(),
            helper_tasks: vec![handle1, handle2],
        })
    }

    /// Send a message out and await for a response.
    ///
    /// It does not alter the routing table, callers must decide what to do with the response.
    async fn send_message(&self, message: &Krpc, recipient: &SocketAddrV4) -> Result<Krpc, DhtServiceFailure> {
        let (tx, rx) = oneshot::channel();

        if let Ok(bytes) = bendy::serde::to_bytes(message) {
            self.request_registry.register(message.id_as_u16(), tx).await;
            self.socket.send_to(&bytes, recipient).await?;
        }
        let response = rx.await.unwrap();
        Ok(response)
    }

    // async fn ping(&self, recipient: &SocketAddrV4) -> Result<(), DhtServiceFailure> {
    //     let transaction_id = self.transaction_id_pool.next();
    //     let ping_msg = Message::new_ping_query(transaction_id.to_be_bytes(), &self.id);
    //
    //     let response = self.send_message(&ping_msg, recipient).await?;
    //
    //     if response.message_type == MessageType::Response {
    //         if let MessageBody::Response(ResponseBody::Ping(ping_response)) = response.body {
    //             self.routing_table.write().await.add_node(ping_response.);
    //         } else {
    //             return Err(DhtServiceFailure {
    //                 message: "received a response that was not a ping response".to_string(),
    //             });
    //         }
    //     } else {
    //         return Err(DhtServiceFailure {
    //             message: "expected a ping response".to_string(),
    //         });
    //     }
    //
    //     Ok(())
    // }

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
        let query = Krpc::new_find_node_query(transaction_id.to_be_bytes(), self.our_id, target);

        // send the message and await for a response
        let time_out = Duration::from_secs(5);
        let response = timeout(time_out, self.send_message(&query, &interlocutor)).await??;

        if let Krpc::FindNodeResponse(find_node_response) = response {
            // the nodes come back as one giant byte string, each 26 bytes is a node
            // we split them up and create a vector of them
            let mut nodes: Vec<_> = find_node_response
                .body
                .nodes
                .windows(26)
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
        let mut target_node = returned_nodes.iter().flatten().find(|node| node.node_id() == target);

        if target_node.is_some() {
            return Ok(target_node.unwrap().clone());
        }

        // if we don't have the node, then we find the alpha closest nodes and ask them in turn
        let mut sorted_by_distance: Vec<_> = returned_nodes
            .iter()
            .flatten()
            .map(|node| {
                let node_id = BigUint::from_bytes_be(node.node_id());
                let our_id = BigUint::from_bytes_be(&self.our_id);
                let distance = our_id.bitxor(node_id);

                (node, distance)
            })
            .collect();
        sorted_by_distance.sort_unstable_by_key(|(_, distance)| distance.clone());

        let recursive_find = sorted_by_distance
            .into_iter()
            .take(3)
            .map(|(node, _)| self.recursive_find(target, node, 32))
            .collect::<Vec<_>>();

        todo!();

        todo!()
    }

    #[async_recursion]
    #[instrument]
    /// Given a pool of potential nodes, ask them concurrently to see if they have the node we're
    /// looking for
    async fn recurse_find_from_pool(
        self: Arc<Self>,
        mut starting_pool: Vec<CompactNodeContact>,
        finding: NodeId,
        seen: Arc<Mutex<HashSet<CompactNodeContact>>>,
        slot: Arc<Mutex<Option<Sender<CompactNodeContact>>>>,
    ) -> Result<(), BottomedOut> {
        // filter the pool to only include nodes that we haven't seen yet
        starting_pool = async {
            let mut seen = seen.lock().await;
            starting_pool
                .into_iter()
                .filter(|node| !seen.contains(&node))
                .collect::<Vec<_>>()
        }
        .await;

        // it's ok to assume that this will never get hit for the first time, since the starting
        // pool is always unseen
        if starting_pool.len() == 0 {
            return Err(BottomedOut);
        }

        // ask all the nodes for target!
        starting_pool.into_iter().map(|node| {
            let seen = seen.clone();
            let dht = self.clone();
            let slot = slot.clone();
            async move {
                let response = dht.ask_her_for_nodes((&node).into(), finding).await?;

                // see if we got the node we're looking for
                if let Some(node) = response.iter().find(|node| node.node_id() == &finding) {
                    // if we did, then we're done
                    let mut slot = slot.lock().await;

                    let huh = slot.deref_mut();

                    if let Some(sender) = huh {
                        let slot = huh.take();
                        slot.unwrap().send(node.clone());
                    }

                    return Ok(());
                } else {
                    seen.lock().await.insert(node.clone());
                }

                seen.lock().await;

                Ok::<_, Box<dyn Error>>(())
            }
        });

        todo!()
    }

    #[async_recursion]
    #[instrument]
    async fn recursive_find(
        &self,
        target: &NodeId,
        asking: &CompactNodeContact,
        recursion_limit: u16,
    ) -> Result<CompactNodeContact, DhtServiceFailure> {
        if recursion_limit == 0 {
            return Err(DhtServiceFailure {
                message: "recursion limit reached, node still not found".to_string(),
            });
        }

        let transaction_id = self.transaction_id_pool.next();
        let query = Krpc::new_find_node_query(transaction_id.to_be_bytes(), self.our_id, target.clone());

        let time_out = Duration::from_secs(15);
        let response = timeout(time_out, self.send_message(&query, &asking.into())).await??;

        if let Krpc::FindNodeResponse(response) = response {
            let nodes = response.body.nodes;
            let mut nodes: Vec<_> = nodes
                .windows(26)
                .map(|x| CompactNodeContact::new(x.try_into().unwrap()))
                .collect();

            nodes.dedup();

            if nodes.is_empty() {
                warn!("out of spec DHT node, responding with zero nodes");
                return Err(DhtServiceFailure {
                    message: "out of spec DHT node, responding with zero nodes".to_string(),
                });
            }

            for node in nodes {
                self.routing_table.write().await.add_new_node(node.clone());
                return if node.node_id() == target {
                    Ok(node)
                } else {
                    self.recursive_find(target, &node, recursion_limit.sub(1)).await
                };
            }
        } else if let Krpc::Error(err) = response {
            info!("error response from DHT node: {:?}", err.error);
            return Err(DhtServiceFailure {
                message: format!("node responded with an error to our find node request {err:?}"),
            });
        } else {
            warn!("non-compliant response from DHT node");
            return Err(DhtServiceFailure {
                message: "non-compliant response from DHT node".to_string(),
            });
        }

        unreachable!()
    }
}

impl Drop for DhtServiceInnerV4 {
    fn drop(&mut self) {
        // stop all tasks required to keep ourself alive
        self.helper_tasks.iter().for_each(|h| h.abort());
    }
}

impl DhtServiceV4 {
    #[instrument]
    pub async fn bootstrap_with_random_id(
        address: SocketAddrV4,
        known_nodes: Vec<SocketAddrV4>,
    ) -> Result<Self, DhtServiceFailure> {
        let inner = DhtServiceInnerV4::new_with_random_id(address).await?;
        let inner = Arc::new(inner);
        let dht = Self { inner: inner.clone() };

        // ask all the known nodes for ourselves
        let mut tasks = Vec::new();

        for contact in known_nodes {
            tasks.push(tokio::spawn(Self::bootstrap_single(inner.clone(), contact)));
        }

        join_all(tasks).await;

        Ok(dht)
    }

    #[instrument]
    async fn bootstrap_single(dht: Arc<DhtServiceInnerV4>, contact: SocketAddrV4) {
        let our_id = dht.our_id.clone();
        let transaction_id = dht.transaction_id_pool.next();
        let query = Krpc::new_find_node_query(transaction_id.to_be_bytes(), our_id, our_id.clone());

        info!("bootstrapping with {contact}");
        let response = timeout(Duration::from_secs(5), async {
            dht.send_message(&query, &contact).await.unwrap()
        })
        .await;
        let response = match response {
            Ok(response) => {
                info!("bootstrapping node {contact} responded back");
                response
            }
            Err(_) => {
                info!("timed out when trying to bootstrap with {contact}");
                return;
            }
        };

        if let Krpc::FindNodeResponse(response) = response {
            let nodes = response.body.nodes;
            let mut nodes: Vec<_> = nodes
                .windows(26)
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

            let mut find_node_tasks = Vec::new();
            for node in nodes {
                let node_ip: SocketAddrV4 = (&node).into();
                trace!("trying to contact {:?}", node_ip);

                let dht = dht.clone();

                let recursive_find_with_timeout = tokio::spawn(async move {
                    if let Ok(node) = timeout(Duration::from_secs(15), dht.recursive_find(&our_id, &node, 64)).await {
                        trace!("found node {:?}", node);
                        return Some(node.unwrap());
                    } else {
                        trace!("failed to contact {:?}, timed out", node_ip);
                        None
                    }
                });
                find_node_tasks.push(recursive_find_with_timeout);
            }

            join_all(find_node_tasks).await;
            info!("finished attempt to bootstrap with {:?}", contact);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dht_service::DhtServiceV4;
    use num_bigint::BigUint;
    use rand::RngCore;
    use std::{
        net::{Ipv4Addr, SocketAddrV4},
        str::FromStr,
    };
    use tokio::{
        net::{self, UdpSocket},
        time::{self, timeout},
    };
    use tracing::{info, subscriber::set_global_default};
    use tracing_subscriber::fmt::format::Format;

    #[tokio::test]
    async fn bootstrap() -> color_eyre::Result<()> {
        color_eyre::install()?;
        // console_subscriber::init();
        let fmt_sub = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .compact()
            .with_line_number(true)
            .finish();
        set_global_default(fmt_sub);

        let dht = DhtServiceV4::bootstrap_with_random_id(
            SocketAddrV4::from_str("0.0.0.0:51413").unwrap(),
            vec![
                // dht.tansmissionbt.com
                "87.98.162.88:6881".parse().unwrap(),
                // router.utorrent.com
                "67.215.246.10:6881".parse().unwrap(),
                // router.bitcomet.com
                "82.221.103.244:6881".parse().unwrap(),
            ],
        );

        let dht = timeout(time::Duration::from_secs(60), dht).await?;
        let dht = dht?;

        info!("Now I'm bootstrapped!");
        let table = dht.inner.routing_table.read().await;
        info!(
            "we've found {:?} nodes and recorded in our routing table",
            table.node_count()
        );

        Ok(())
    }
}
