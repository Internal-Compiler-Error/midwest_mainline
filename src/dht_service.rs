use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::mem::forget;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::task::Poll::Ready;
use std::time::Duration;
use async_recursion::async_recursion;
use futures::future::join_all;
use num_bigint::BigUint;
use rand::{Rng, RngCore, rngs, SeedableRng};
use rand::rngs::SmallRng;
use tokio::{net, spawn, task};
use tower::Service;
use tokio::sync::oneshot;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::net::UdpSocket;
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use tracing::{error, event, info, instrument, Level};
use tracing::log::warn;
use crate::dht_service::transaction_id_pool::TransactionIdPool;
use crate::domain_knowledge::{CompactNodeContact, NodeId};
use crate::message::{Message, MessageBody, MessageType, QueryMethod};
use crate::message::query::QueryBody;
use crate::message::response::ResponseBody;
use crate::routing::{RoutingTable};

mod transaction_id_pool;

struct DhtServiceV4 {
    inner: Arc<DhtServiceInnerV4>,
}

#[derive(Debug)]
struct DhtServiceInnerV4 {
    socket: Arc<UdpSocket>,
    id: [u8; 20],
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

impl Display for DhtServiceFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for DhtServiceFailure {}

#[derive(Debug)]
struct RequestRegistry {
    slots: Mutex<HashMap<u16, oneshot::Sender<Message>>>,
    query_queue: Mutex<mpsc::Sender<Message>>,
    packet_queue: Mutex<mpsc::Receiver<Message>>,
}

impl RequestRegistry {
    pub fn new(incoming_queue: mpsc::Receiver<Message>,
               query_channel: mpsc::Sender<Message>) -> Self {
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
                let id = u16::from_be_bytes(msg.transaction_id.clone());

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
    pub async fn register(&self, transaction_id: u16, sending_half: oneshot::Sender<Message>) {
        let mut guard = self.slots.lock().await;
        assert!(guard.insert(transaction_id, sending_half).is_none())
    }
}

impl From<std::io::Error> for DhtServiceFailure {
    fn from(error: std::io::Error) -> Self {
        DhtServiceFailure {
            message: error.to_string(),
        }
    }
}

impl DhtServiceInnerV4 {
    /// A default DHT node when you really don't know anything about DHTs and just want to provide
    /// a port and IP address
    async fn new_with_random_id(
        address: SocketAddrV4,
    ) -> Result<Self, DhtServiceFailure> {
        let socket = UdpSocket::bind(&address)
            .await?;

        // randomly pick our ID
        let mut id = [0u8; 20];
        rand::thread_rng()
            .fill_bytes(&mut id);

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

        let handle1 = task::Builder::new()
            .name("socket reader")
            .spawn(async move { socket_reader.await; });

        let message_registry = RequestRegistry::new(incoming_rx, queries_tx);
        let message_registry = Arc::new(message_registry);

        // place the messages into the right slot

        let message_registry1 = message_registry.clone();
        let message_placing = async move {
            message_registry1.lifetime_loop().await;
            ()
        };

        let handle2 = task::Builder::new()
            .name("message registry")
            .spawn(message_placing);

        Ok(DhtServiceInnerV4 {
            socket,
            request_registry: message_registry,
            id,
            routing_table: RwLock::new(routing_table),
            socket_address: address,
            transaction_id_pool: TransactionIdPool::new(),
            helper_tasks: vec![handle1, handle2],
        })
    }


    /// Send a message out and await for a response.
    ///
    /// It does not alter the routing table, callers must decide what to do with the response.
    async fn send_message(&self, message: &Message, recipient: &SocketAddrV4) -> Result<Message, DhtServiceFailure> {
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


    async fn find_node(&self, target: &NodeId) -> Result<CompactNodeContact, DhtServiceFailure> {
        // if we already know the node, then no need for any network requests
        if let Some(node) = self.routing_table.read().await.find(target) {
            return Ok(node.contact.clone());
        }


        // find the closest nodes that we know
        let closest;
        {
            let table = self.routing_table.read().await;
            closest = table
                .find_closest(target)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
        }


        for node in &closest {
            // construct the message to query our friends
            let transaction_id = self.transaction_id_pool.next();
            let query = Message::new_find_node_query(
                transaction_id.to_be_bytes(),
                self.id,
                target.clone(),
            );

            // ask them
            let response = self.send_message(&query, &(&node.contact).into()).await?;

            if let MessageBody::Response(res) = response.body {
                if let ResponseBody::FindNode(nodes) = res {
                    let mut nodes: Vec<_> = nodes
                        .nodes
                        .windows(26)
                        .map(|x| CompactNodeContact::new(x.try_into().unwrap()))
                        .collect();

                    nodes.dedup();


                    for node in nodes {
                        self.routing_table.write().await.add_new_node(node.clone());
                        if node.node_id() == target {
                            return Ok(node);
                        } else {
                            self.recursive_find(target, &node).await?;
                        }
                    }
                }
            }
        }


        unreachable!()
    }

    #[async_recursion]
    #[instrument]
    async fn recursive_find(&self, target: &NodeId, asking: &CompactNodeContact) -> Result<CompactNodeContact, DhtServiceFailure> {
        let transaction_id = self.transaction_id_pool.next();
        let query = Message::new_find_node_query(
            transaction_id.to_be_bytes(),
            self.id,
            target.clone(),
        );

        let response = self.send_message(&query, &asking.into()).await?;

        if let MessageBody::Response(res) = &response.body {
            if let ResponseBody::FindNode(nodes) = res {
                let mut nodes: Vec<_> = nodes
                    .nodes
                    .windows(26)
                    .map(|x| CompactNodeContact::new(x.try_into().unwrap()))
                    .collect();

                nodes.dedup();


                for node in nodes {
                    self.routing_table.write().await.add_new_node(node.clone());
                    if node.node_id() == target {
                        return Ok(node);
                    } else {
                        return self.recursive_find(target, &node).await;
                    }
                }
            }
        } else {
            warn!("unexpected response, got {response:?}");
            return Err(DhtServiceFailure {
                message: "expected a find node response".to_string(),
            });
        }
        error!("got {response:?}");
        unreachable!()
    }
}


impl Drop for DhtServiceInnerV4 {
    fn drop(&mut self) {
        // stop all tasks required to keep ourself alive
        self
            .helper_tasks
            .iter()
            .for_each(|h| h.abort());
    }
}

impl DhtServiceV4 {
    #[instrument]
    pub async fn bootstrap_with_random_id(address: SocketAddrV4, known_nodes: Vec<SocketAddrV4>) -> Result<Self, DhtServiceFailure> {
        let inner = DhtServiceInnerV4::new_with_random_id(address).await?;
        let inner = Arc::new(inner);
        let dht = Self {
            inner: inner.clone(),
        };


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
        let our_id = dht.id.clone();
        let transaction_id = dht.transaction_id_pool.next();
        let query = Message::new_find_node_query(
            transaction_id.to_be_bytes(),
            our_id,
            our_id.clone(),
        );


        info!("bootstrapping with {contact}");
        let response = timeout(Duration::from_secs(5), async { dht.send_message(&query, &contact).await.unwrap() }).await;
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


        match response.body {
            MessageBody::Response(res) => {
                match res {
                    ResponseBody::FindNode(res) => {
                        let nodes: Vec<_> = res.nodes.windows(26).map(|x| {
                            CompactNodeContact::new(x.try_into().unwrap())
                        }).collect();

                        {
                            // add the bootstrapping node to our routing table
                            let mut table = dht.routing_table.write().await;
                            table.add_new_node(CompactNodeContact::from_node_id_and_addr(&res.id, &contact));
                        }


                        let mut find_node_tasks = Vec::new();
                        for node in nodes {
                            let node_ip: SocketAddrV4 = (&node).into();
                            info!("trying to contact {:?}", node_ip);

                            let dht = dht.clone();

                            let recursive_find_with_timeout = tokio::spawn(async move {
                                if let Ok(node) = timeout(
                                    Duration::from_secs(15),
                                    dht.recursive_find(&our_id, &node)).await {
                                    info!("found node {:?}", node);
                                    return Some(node.unwrap());
                                } else {
                                    info!("failed to contact {:?}, timed out", node_ip);
                                    None
                                }
                            });
                            find_node_tasks.push(recursive_find_with_timeout);
                        }

                        join_all(find_node_tasks).await;
                        info!("finished attempt to bootstrap with {:?}", contact);
                    }
                    other => {
                        eprintln!("expected an response, got {:?}", other);
                    }
                }
            }
            other => {
                eprintln!("expected an response, got {:?}", other);
            }
        }
    }
}


impl Service<Message> for DhtServiceV4 {
    type Response = Option<Vec<Message>>;
    type Error = DhtServiceFailure;
    type Future = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>>>>;

    // backpressure? what backpressure? backpressure is when the kernel panics!
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ready(Ok(()))
    }


    fn call(&mut self, req: Message) -> Self::Future {
        let dht = self.inner.clone();

        let our_id = dht.id.clone();
        let fut = Box::pin(async move {
            let mut buffer = [0u8; 1024];

            let transaction_id = &req.transaction_id;
            let message_type = &req.message_type;
            //let query_method = req.query_method;
            let query_args = &req.body;


            match query_args {
                MessageBody::Query(query_body) => {
                    match query_body {
                        QueryBody::Ping(args) => {
                            let message = bendy::serde::to_bytes(&req).map_err(|e| {
                                DhtServiceFailure {
                                    message: format!("Failed to serialize message: {}", e),
                                }
                            })?;

                            dht.socket.send(message.as_slice()).await?;
                            let len = dht.socket.recv(&mut buffer).await?;

                            //socket.send(message.as_slice()).await.unwrap();

                            return Ok(None);
                        }
                        QueryBody::FindNode(args) => {
                            unimplemented!();
                        }
                        QueryBody::GetPeers(args) => {
                            unimplemented!();
                        }
                        QueryBody::AnnouncePeer(args) => {
                            unimplemented!();
                        }
                    }
                }
                MessageBody::Response(response) => {
                    match response {
                        ResponseBody::Ping(ping) => {
                            unimplemented!();
                        }
                        ResponseBody::FindNode(_) => {
                            unimplemented!();
                        }
                        ResponseBody::GetPeers(_) => {
                            unimplemented!();
                        }
                        ResponseBody::AnnouncePeer(_) => {
                            unimplemented!();
                        }
                    }
                }
                MessageBody::Error => { unimplemented!() }
            };
        });

        fut
    }
}


#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::str::FromStr;
    use num_bigint::BigUint;
    use rand::RngCore;
    use tokio::{net, time};
    use tokio::net::UdpSocket;
    use tokio::time::timeout;
    use tracing::info;
    use tracing::subscriber::set_global_default;
    use tracing_subscriber::fmt::format::Format;
    use crate::dht_service::DhtServiceV4;

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
            SocketAddrV4::from_str("0.0.0.0:51776").unwrap(),
            vec![
                // dht.tansmissionbt.com
                "87.98.162.88:6881".parse().unwrap(),
                // router.utorrent.com
                "67.215.246.10:6881".parse().unwrap(),
                // router.bitcomet.com
                "82.221.103.244:6881".parse().unwrap(),
            ]);

        let dht = timeout(time::Duration::from_secs(60), dht).await?;
        let dht = dht?;

        info!("Now I'm bootstrapped!");
        let table = dht.inner.routing_table.read().await;
        info!("we've found {:?} nodes and recorded in our routing table", table.node_count());

        Ok(())
    }

    #[tokio::test]
    async fn why() -> color_eyre::Result<()> {
        color_eyre::install()?;

        let socket = UdpSocket::bind("0.0.0.0:51776").await?;
        let addr = SocketAddrV4::new(
            Ipv4Addr::new(34, 206, 39, 153),
            6881,
        );
        socket.send_to(b"hello", addr).await?;

        Ok(())
    }
}
