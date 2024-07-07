use crate::{
    dht_service::dht_server::DhtServer,
    domain_knowledge::{NodeId, TransactionId},
    message::{Krpc, ParseKrpc, ToRawKrpc},
};

use peer_guide::PeerGuide;
use rand::{Rng, RngCore};
use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{
        mpsc,
        oneshot::{self, Sender},
        Mutex as AsyncMutex,
    },
    task::{Builder, JoinError, JoinHandle, JoinSet},
    time::{error::Elapsed, timeout},
};

use dht_client::DhtClientV4;
use tracing::{info, instrument, trace, warn};

pub mod dht_client;
pub mod dht_server;
pub mod peer_guide;
mod transaction_id_pool;

/// The DHT service, it contains pointers to a server and client, it's main role is to run the
/// tasks required to make DHT alive
#[derive(Debug)]
#[allow(dead_code)]
pub struct DhtV4 {
    client: Arc<DhtClientV4>,
    server: Arc<DhtServer>,
    message_broker: Arc<MessageBroker>,
    peer_guide: Arc<PeerGuide>,
    helper_tasks: JoinSet<()>,
}

/// A very unhelpful error type. This will be replaced with a more helpful error type in the future.
#[derive(Debug)]
pub struct DhtServiceFailure {
    message: String,
}

impl Display for DhtServiceFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for DhtServiceFailure {}

/// A message demultiplexer keeps reading Krpc messages from a queue and place them either into the
/// server response queue when we haven't seen this transaction id before, or into a oneshot channel
/// so the client and await the response.
/// TODO: replace this with some proper pubsub
#[derive(Debug)]
pub(crate) struct MessageBroker {
    /// a map to keep track of the responses we await from the client
    pending_responses: Arc<AsyncMutex<HashMap<TransactionId, Sender<(Krpc, SocketAddrV4)>>>>,

    /// all messages belong to the server are put into this queue.
    // query_queue: Arc<AsyncMutex<mpsc::Sender<(Krpc, SocketAddrV4)>>>,

    /// a channel to receive new krpc read from the socket
    // outbound_messages: Arc<AsyncMutex<mpsc::Receiver<(Krpc, SocketAddrV4)>>>,
    socket: Arc<UdpSocket>,

    /// a list of subscribers that wishes to hear all messages inbound
    inbound_subscribers: Arc<Mutex<Vec<mpsc::Sender<(Krpc, SocketAddrV4)>>>>,
}

impl MessageBroker {
    pub fn new(
        // incoming_queue: mpsc::Receiver<(Krpc, SocketAddrV4)>,
        // query_channel: mpsc::Sender<(Krpc, SocketAddrV4)>,
        socket: UdpSocket,
    ) -> Self {
        Self {
            pending_responses: Arc::new(AsyncMutex::new(HashMap::new())),
            // query_queue: Arc::new(AsyncMutex::new(query_channel)),
            // outbound_messages: Arc::new(AsyncMutex::new(incoming_queue)),
            socket: Arc::new(socket),
            inbound_subscribers: Arc::new(Mutex::new(vec![])),
        }
    }

    #[instrument(skip_all)]
    pub async fn run(&self) -> io::Result<JoinHandle<()>> {
        let socket = self.socket.clone();
        let pending_responses = self.pending_responses.clone();
        // let query_queue = self.query_queue.clone();
        let inbound_subscribers = self.inbound_subscribers.clone();

        let socket_reader = async move {
            let mut buf = [0u8; 1500];

            loop {
                let (amount, socket_addr) = socket.recv_from(&mut buf).await.expect("common MTU 1500 exceeded");
                trace!("packet from {socket_addr}");
                if let Ok(msg) = (&buf[..amount]).parse() {
                    let socket_addr = {
                        match socket_addr {
                            SocketAddr::V4(addr) => addr,
                            _ => panic!("Expected V4 socket address"),
                        }
                    };

                    let id = msg.transaction_id();
                    trace!(
                        "received message for transaction id {:?}",
                        hex::encode_upper(id.as_bytes())
                    );

                    // notify those that subscribed for all inbound messages
                    {
                        let subcribers = inbound_subscribers.lock().unwrap();
                        for sub in &*subcribers {
                            // TODO: worry about what to do about disconnected or full queues later
                            let _ = sub.try_send((msg.clone(), socket_addr));
                        }
                    }

                    // see if we have a slot for this transaction id, if we do, that means one of the
                    // messages that we expect, otherwise the message is a query we need to handle
                    if let Some(sender) = pending_responses.lock().await.remove(id) {
                        // failing means we're no longer interested, which is ok
                        let _ = sender.send((msg, socket_addr));
                    }
                    warn!(
                        "Failed to parse message from {socket_addr}, bytes = {:?}",
                        &buf[..amount]
                    );
                }
            }
        };

        // let outbound_messages = self.outbound_messages.clone();
        // let socket = self.socket.clone();
        // let stupid = async move {
        //     let mut outbound_messages = outbound_messages.lock().await;
        //     loop {
        //         let outbound = outbound_messages.recv().await;
        //         if outbound.is_none() {
        //             break;
        //         }
        //
        //         let (msg, peer) = outbound.unwrap();
        //         let _ = socket.send_to(&msg.to_raw_krpc(), peer).await;
        //     }
        // };

        use tokio::task::Builder;
        Builder::new().name("Message broker").spawn(socket_reader)
    }

    /// Tell the broker we should expect some messages
    // TODO: maybe it should return Receiver<Option<(Krpc, SocketAddrV4)>
    pub async fn subscribe_one(&self, transaction_id: TransactionId) -> oneshot::Receiver<(Krpc, SocketAddrV4)> {
        let (tx, rx) = oneshot::channel();

        let mut guard = self.pending_responses.lock().await;
        // it's possible that the response never came and we a new request is now using the same
        // transaction id
        let _ = guard.insert(transaction_id, tx);
        rx
    }

    pub fn send_msg(&self, msg: Krpc, peer: SocketAddrV4) {
        let socket = self.socket.clone();

        // TODO: maybe this is too expensive?
        tokio::spawn(async move {
            let buf = msg.to_raw_krpc();

            let _ = socket.send_to(&buf, peer);
        });
    }

    // TODO: think of a good way to let them unsubscribe later
    pub fn subscribe_inbound(&self) -> mpsc::Receiver<(Krpc, SocketAddrV4)> {
        // TODO: make this configurable
        let (tx, rx) = mpsc::channel(1024);
        let mut subscribers = self.inbound_subscribers.lock().unwrap();
        subscribers.push(tx);
        rx
    }
}

impl From<std::io::Error> for DhtServiceFailure {
    fn from(error: std::io::Error) -> Self {
        DhtServiceFailure {
            message: error.to_string(),
        }
    }
}

impl From<Elapsed> for DhtServiceFailure {
    fn from(error: Elapsed) -> Self {
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

    NodeId(id)
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

impl DhtV4 {
    /// Create a new DHT service the id is generated randomly in according to BEP-42 using the
    /// external IP address of the machine. Note there is no way to verify the external IP address
    /// is correct and it's duty to make sure it's correct.
    #[instrument(skip_all)]
    pub async fn bootstrap_with_random_id(
        bind_addr: SocketAddrV4,
        external_addr: Ipv4Addr,
        known_nodes: Vec<SocketAddrV4>,
    ) -> Result<Self, DhtServiceFailure> {
        let socket = UdpSocket::bind(&bind_addr).await?;
        // let socket = Arc::new(socket);

        // id is randomly generated according to BEP-42
        let our_id = random_idv4(&external_addr, rand::thread_rng().gen::<u8>());

        // the JoinSet keeps all the tasks required to make the DHT node functional
        let mut join_set = JoinSet::new();

        // all udp packets are sent to the channel, and the demultiplexer will route them into either
        // oneshot senders or a queue for severs to handle
        // let (incoming_packets_tx, incoming_packets_rx) = mpsc::channel(1024);

        // all queries that servers should handle are sent on this channel
        // let (queries_tx, queries_rx) = mpsc::channel(1024);

        // TODO: wtf, why is bootstrapping so special
        //
        // keep reading from sockets and place them on a queue for another task to place them into
        // the right slot
        // let reading_socket = socket.clone();
        // let socket_reader = async move {
        //     let mut buf = [0u8; 1500];
        //     loop {
        //         let (amount, socket_addr) = reading_socket
        //             .recv_from(&mut buf)
        //             .await
        //             .expect("common MTU 1500 exceeded");
        //         trace!("packet from {socket_addr}");
        //         if let Ok(msg) = (&buf[..amount]).parse() {
        //             let socket_addr = {
        //                 match socket_addr {
        //                     SocketAddr::V4(addr) => addr,
        //                     _ => panic!("Expected V4 socket address"),
        //                 }
        //             };
        //
        //             incoming_packets_tx.send((msg, socket_addr)).await.unwrap();
        //         } else {
        //             warn!(
        //                 "Failed to parse message from {socket_addr}, bytes = {:?}",
        //                 &buf[..amount]
        //             );
        //         }
        //     }
        // };
        //
        // join_set
        //     .build_task()
        //     .name(&*format!("socket reader for {bind_addr}"))
        //     .spawn(socket_reader.instrument(info_span!("socket reader")))
        //     .unwrap();

        let message_broker = MessageBroker::new(socket);
        let message_broker = Arc::new(message_broker);

        let message_broker_clone = message_broker.clone();
        join_set
            .build_task()
            .name(&*format!("message demultiplexer for {bind_addr}"))
            .spawn(async move {
                message_broker_clone.run().await;
            })
            .unwrap();

        let peer_guide = Arc::new(PeerGuide::new(our_id));

        let client = DhtClientV4::new(
            // bind_addr,
            // socket.clone(),
            message_broker.clone(),
            peer_guide.clone(),
            our_id.clone(),
        );
        let client = Arc::new(client);

        // ask all the known nodes for ourselves
        let mut bootstrap_join_set = JoinSet::new();

        for contact in known_nodes {
            bootstrap_join_set
                .build_task()
                .name(&*format!("bootstrap with {contact}"))
                .spawn(Self::bootstrap_from(client.clone(), contact))
                .unwrap();
        }

        while let Some(_) = bootstrap_join_set.join_next().await {}

        // info!(
        //     "DHT bootstrapped, routing table has {} nodes",
        //     peer_guide.read().await.node_count()
        // );
        drop(bootstrap_join_set);

        // TODO: huh, since when this this make sense?
        //
        // only spawn the server after the bootstrap has completed
        let server = DhtServer::new(our_id.clone(), peer_guide.clone(), message_broker.clone());
        let server = Arc::new(server);

        join_set
            .build_task()
            .name(&*format!("DHT server for {bind_addr}"))
            .spawn(server.clone().run())
            .unwrap();

        let dht = DhtV4 {
            client: client.clone(),
            server: server.clone(),
            message_broker,
            peer_guide: peer_guide.clone(),

            helper_tasks: join_set,
        };

        Ok(dht)
    }

    /// Keep the DHT running so you can't use the clients and servers, usually you put spawn this
    /// and abort the task when desired
    pub async fn run(mut self) {
        while let Some(_) = self.helper_tasks.join_next().await {}
    }

    /// Given a known know, perform one find node to ourself add the response to the routing table
    /// *and* do one additional round of find node to all the returned nodes from the bootstrapping
    /// node.
    ///
    /// This is subject to change in the future.
    #[instrument(skip_all)]
    async fn bootstrap_from(dht: Arc<DhtClientV4>, contact: SocketAddrV4) -> Result<(), BootstrapError> {
        let our_id = dht.our_id.clone();
        let transaction_id = dht.transaction_id_pool.next();
        // TODO: clearly wrong
        // actually might be right, the goal of bootstrapping is to discover peers near ourselves
        let query = Krpc::new_find_node_query(TransactionId::from(transaction_id), our_id.clone(), our_id.clone());

        info!("bootstrapping with {contact}");
        let response = timeout(Duration::from_secs(5), async {
            dht.send_message(&query, contact)
                .await
                .expect("failure to send message")
        })
        .await?;

        if let Krpc::FindNodeGetPeersResponse(response) = response {
            let mut nodes = response.nodes().clone();
            nodes.sort_unstable_by_key(|node| node.contact().0);
            nodes.dedup();

            // {
            //     // add the bootstrapping node to our routing table
            //     let mut table = dht.routing_table.write().await;
            //     table.add_new_node(NodeInfo::new(response.peer_id().clone(), PeerContact(contact)))
            // }

            for node in nodes {
                // dht.routing_table.write().await.add_new_node(node);

                let dht = dht.clone();
                let contact: SocketAddrV4 = node.contact().0;
                let our_id = dht.our_id.clone();
                let transaction_id = dht.transaction_id_pool.next();
                let query = Krpc::new_find_node_query(
                    TransactionId::from(transaction_id),
                    our_id.clone(),
                    node.node_id().clone(),
                );
                Builder::new()
                    .name(&*format!("leave level bootstrap to {}", contact))
                    .spawn(async move {
                        let response = timeout(Duration::from_secs(5), async {
                            dht.send_message(&query, contact)
                                .await
                                .expect("failure to send message")
                        })
                        .await?;

                        if let Krpc::FindNodeGetPeersResponse(response) = response {
                            // let nodes = response.nodes();

                            // for node in nodes {
                            //     // add the leave level responses to our routing table
                            //     let mut table = dht.routing_table.write().await;
                            //     let peer = NodeInfo::new(node.node_id().clone(), PeerContact(contact));
                            //     table.add_new_node(peer);
                            // }
                        }
                        Ok::<_, color_eyre::Report>(())
                    })
                    .unwrap();
            }
            info!("bootstrapping with {contact} succeeded");
        }
        Ok(())
    }

    /// Returns a handle to the client so you can perform queries
    pub fn client(&self) -> Arc<DhtClientV4> {
        self.client.clone()
    }

    /// Returns a handle to the server, currently there is no public API for the server. In the
    /// the sever will support some APIs to allow you to query about its state
    pub fn server(&self) -> Arc<DhtServer> {
        self.server.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::dht_service::DhtV4;
    // use num::{BigUint, Num};
    // use opentelemetry::global;
    // use rand::RngCore;
    use std::{net::SocketAddrV4, str::FromStr, sync::Once};
    // use tokio::time::{self, timeout};
    // use tracing::info;
    // use tracing_subscriber::{filter::LevelFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt, Layer};

    static TEST_INIT: Once = Once::new();

    fn set_up_tracing() {
        let _ = color_eyre::install();
        // let fmt_layer = fmt::layer()
        //     .compact()
        //     .with_line_number(true)
        //     .with_filter(LevelFilter::DEBUG);
        //
        // global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
        // let tracer = opentelemetry_jaeger::new_pipeline().install_simple().unwrap();
        //
        // let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        //
        // tracing_subscriber::registry()
        //     .with(console_subscriber::spawn())
        //     .with(telemetry)
        //     .with(fmt_layer)
        //     .init();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bootstrap() -> color_eyre::Result<()> {
        // TEST_INIT.call_once(set_up_tracing);
        //
        // let external_ip = public_ip::addr_v4().await.unwrap();
        //
        // let dht = DhtV4::bootstrap_with_random_id(
        //     SocketAddrV4::from_str("0.0.0.0:51413").unwrap(),
        //     external_ip,
        //     vec![
        //         // dht.tansmissionbt.com
        //         "87.98.162.88:6881".parse().unwrap(),
        //         // router.utorrent.com
        //         "67.215.246.10:6881".parse().unwrap(),
        //         // router.bittorrent.com, ironically that this almost never responds
        //         "82.221.103.244:8991".parse().unwrap(),
        //         // dht.aelitis.com
        //         "174.129.43.152:6881".parse().unwrap(),
        //     ],
        // );
        //
        // let dht = timeout(time::Duration::from_secs(60), dht).await??;
        // info!("Now I'm bootstrapped!");
        // {
        //     let table = dht.client.routing_table.read().await;
        //     info!(
        //         "we've found {:?} nodes and recorded in our routing table",
        //         table.node_count()
        //     );
        // }
        //
        // let client = dht.client;
        //
        // let mut rng = rand::thread_rng();
        // let mut bytes = [0u8; 20];
        // rng.fill_bytes(&mut bytes);
        //
        // let node = client.find_node(&bytes).await;
        // if let Ok(node) = node {
        //     info!("found node {:?}", node);
        // } else {
        //     info!("I guess we just didn't find anything")
        // }
        //
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn find_peers() -> color_eyre::Result<()> {
        // TEST_INIT.call_once(set_up_tracing);
        //
        // let external_ip = public_ip::addr_v4().await.unwrap();
        //
        // let dht = DhtV4::bootstrap_with_random_id(
        //     SocketAddrV4::from_str("0.0.0.0:51413").unwrap(),
        //     external_ip,
        //     vec![
        //         // dht.tansmissionbt.com
        //         "87.98.162.88:6881".parse().unwrap(),
        //         // router.utorrent.com
        //         "67.215.246.10:6881".parse().unwrap(),
        //         // router.bittorrent.com, ironically that this almost never responds
        //         "82.221.103.244:6881".parse().unwrap(),
        //         // dht.aelitis.com
        //         "174.129.43.152:6881".parse().unwrap(),
        //     ],
        // )
        // .await?;
        //
        // let info_hash = BigUint::from_str_radix("233b78ca585fe0a8c9e8eb4bda03f52e8b6f554b", 16).unwrap();
        // let info_hash = info_hash.to_bytes_be();
        //
        // let client = dht.client();
        // let (token, peers) = client.get_peers(info_hash.as_slice().try_into()?).await?;
        // info!("token {token:?}, peers {peers:?}");
        Ok(())
    }

    // #[tokio::test(flavor = "multi_thread")]
    // async fn server() -> color_eyre::Result<()> {
    //     TEST_INIT.call_once(set_up_tracing);
    //
    //     let external_ip = public_ip::addr_v4().await.unwrap();
    //
    //     let dht = DhtClientV4::new(SocketAddrV4::from_str("0.0.0.0:51413")?, external_ip).await?;
    //     info!("dht created");
    //
    //     let fake_client = async move {
    //         info!("starting to bind");
    //         let fake_peer_socket = UdpSocket::bind(SocketAddrV4::from_str("127.0.0.1:0")?).await?;
    //         fake_peer_socket
    //             .connect(SocketAddrV4::from_str("127.0.0.1:51413")?)
    //             .await?;
    //         info!("connected to dht");
    //
    //         for i in 0..5 {
    //             let ping = Krpc::new_ping_query(Box::new([b'a', b'a' + i]), b"abcdefghij0123456789".clone());
    //             let serialized = bendy::serde::to_bytes(&ping)?;
    //
    //             fake_peer_socket.send(&serialized).await?;
    //
    //             let mut buf = [0u8; 1024];
    //             let len = fake_peer_socket.recv(&mut buf).await?;
    //
    //             let msg = bendy::serde::from_bytes::<Krpc>(&buf[..len])?;
    //
    //             // add some checks to ensure this is the stuff we actually expect in the future
    //             // but since there is no way to know the id of the dht right now, we can't do that
    //         }
    //         Ok::<_, color_eyre::Report>(())
    //     };
    //     let client_handle = tokio::spawn(fake_client);
    //
    //     let _ = client_handle.await?;
    //
    //     Ok(())
    // }

    #[tokio::test(flavor = "multi_thread")]
    async fn run() -> color_eyre::Result<()> {
        TEST_INIT.call_once(set_up_tracing);
        let external_ip = public_ip::addr_v4().await.unwrap();

        let dht = DhtV4::bootstrap_with_random_id(
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
        )
        .await?;

        dht.run().await;

        Ok(())
    }
}
