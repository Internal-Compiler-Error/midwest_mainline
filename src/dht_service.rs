use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::{SocketAddr, SocketAddrV4};
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
use tokio::{net, task};
use tower::Service;
use tokio::sync::oneshot;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::net::UdpSocket;
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{event, info, instrument, Level};
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
    id: BigUint,
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
struct RequestRegistry {
    slots: Mutex<HashMap<u16, oneshot::Sender<Message>>>,
    packet_queue: Mutex<mpsc::Receiver<Message>>,
}

impl RequestRegistry {
    pub fn new(queue: mpsc::Receiver<Message>) -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
            packet_queue: Mutex::new(queue),
        }
    }

    pub async fn lifetime_loop(&self) {
        let mut queue = self.packet_queue.lock().await;
        loop {
            if let Some(msg) = queue.recv().await {
                let id = u16::from_be_bytes(msg.transaction_id.clone());

                let slot: oneshot::Sender<Message>;
                {
                    let mut guard = self.slots.lock().await;
                    slot = guard.remove(&id).expect("read a message without a corresponding slot");
                }

                // sending might will if the receiving half is already gone, which is harmless
                let _ = slot.send(msg);
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
    async fn new(
        id: BigUint,
        address: SocketAddrV4,
    ) -> Result<Self, DhtServiceFailure> {
        let socket = UdpSocket::bind(&address)
            .await?;

        let socket = Arc::new(socket);
        let routing_table = RoutingTable::new(id);

        // gen our id
        let mut id = [0u8; 20];
        let mut rand = rand::thread_rng();
        rand.fill_bytes(&mut id);
        let id = BigUint::from_bytes_be(&id);

        let (tx, rx) = mpsc::channel(128);


        // keep reading from sockets and place them on a queue for another task to place them into
        // the right slot
        let reading_socket = socket.clone();
        let socket_reader = async move {
            let mut buf = [0u8; 1024];
            loop {
                let (amount, _) = reading_socket.recv_from(&mut buf).await?;
                if let Ok(msg) = bendy::serde::from_bytes(&buf[..amount]) {
                    tx.send(msg).await.unwrap();
                }
            }
            Ok::<_, DhtServiceFailure>(())
        };

        let handle1 = task::Builder::new()
            .name("socket reader")
            .spawn(async move { socket_reader.await; });

        let message_registry = RequestRegistry::new(rx);
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


    async fn send_message(&self, message: &Message, precipitant: &SocketAddrV4) -> Result<Message, DhtServiceFailure> {
        let (tx, rx) = oneshot::channel();

        if let Ok(bytes) = bendy::serde::to_bytes(message) {
            self.request_registry.register(message.id_as_u16(), tx).await;
            self.socket.send_to(&bytes, precipitant).await?;
        }
        let response = rx.await.unwrap();
        Ok(response)
    }


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
            let transaction_id = self.transaction_id_pool.get_id();
            let query = Message::new_find_node_query(
                transaction_id.to_be_bytes(),
                self.id.to_bytes_be().as_slice().clone().try_into().unwrap(),
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
        let transaction_id = self.transaction_id_pool.get_id();
        let query = Message::new_find_node_query(
            transaction_id.to_be_bytes(),
            self.id.to_bytes_be().as_slice().clone().try_into().unwrap(),
            target.clone(),
        );

        let response = self.send_message(&query, &asking.into()).await?;

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
                        return self.recursive_find(target, &node).await;
                    }
                }
            }
        }
        unreachable!();
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
    pub async fn bootstrap(id: BigUint, address: SocketAddrV4, known_nodes: Vec<SocketAddrV4>) -> Result<Self, DhtServiceFailure> {
        let inner = DhtServiceInnerV4::new(id, address).await?;
        let dht = Self {
            inner: Arc::new(inner)
        };


        // add the initial nodes
        for contact in &known_nodes {
            let mut our_id = [0u8; 20];
            our_id = dht.inner.id.to_bytes_be().as_slice().try_into().unwrap();

            let transaction_id = dht.inner.transaction_id_pool.get_id();
            let query = Message::new_find_node_query(
                transaction_id.to_be_bytes(),
                dht.inner.id.to_bytes_be().as_slice().clone().try_into().unwrap(),
                our_id.clone(),
            );


            let response = dht.inner.send_message(&query, contact).await.unwrap();

            info!("bootstrapping with {contact}, got back {response:?}");

            match response.body {
                MessageBody::Response(res) => {
                    match res {
                        ResponseBody::FindNode(res) => {
                            let nodes: Vec<_> = res.nodes.windows(26).map(|x| {
                                CompactNodeContact::new(x.try_into().unwrap())
                            }).collect();


                            let mut handles = Vec::new();
                            for node in nodes {
                                info!("trying to contact {:?}", &node);

                                let dht = dht.inner.clone();
                                let handle = tokio::spawn(async move {
                                    if let Ok(node) = timeout(
                                        Duration::from_secs(15),
                                        dht.recursive_find(&our_id, &node)).await {
                                        return Some(node.unwrap());
                                    } else {
                                        None
                                    }
                                });
                                handles.push(handle);
                            }

                            for handle in handles {
                                handle.await;
                            }
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

        Ok(dht)
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

        let our_id = dht.id.to_bytes_be();
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
    async fn bootstrap() {
        // console_subscriber::init();
        let fmt_sub = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .compact()
            .finish();
        set_global_default(fmt_sub);

        let mut rand = rand::thread_rng();
        let mut id = [0u8; 20];
        rand.fill_bytes(&mut id);
        let id = BigUint::from_bytes_be(&id);

        let dht = DhtServiceV4::bootstrap(id,
                                          SocketAddrV4::from_str("0.0.0.0:51776").unwrap(),
                                          vec![
                                              "87.98.162.88:6881".parse().unwrap(),
                                              "82.221.103.244:6881".parse().unwrap(),
                                              "67.215.246.10:6881".parse().unwrap(),
                                          ]);

        if let Ok(Ok(dht)) = timeout(time::Duration::from_secs(60), dht).await {
            info!("Now I'm bootstrapped!");
            let table = dht.inner.routing_table.read().await;
            info!("we've found {:?} nodes and recorded in our routing table", table.node_count());
        } else {
            info!("Either we timed out or internal dht failure");
        }
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
