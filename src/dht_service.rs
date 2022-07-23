use crate::{
    dht_service::dht_server::DhtServer,
    domain_knowledge::{CompactNodeContact, CompactPeerContact, NodeId},
    message::{InfoHash, Krpc},
    routing::RoutingTable,
};

use rand::{Rng, RngCore};
use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot, oneshot::Sender, Mutex, RwLock},
    task::{Builder, JoinError, JoinSet},
    time::{error::Elapsed, timeout},
};

use crate::message::TransactionId;
use dht_client::DhtClientV4;
use tracing::{info, info_span, instrument, log::warn, trace, Instrument};

pub mod dht_client;
mod dht_server;
mod transaction_id_pool;

#[derive(Debug)]
struct DhtV4 {
    client: Arc<DhtClientV4>,
    server: Arc<DhtServer>,
    demultiplexer: Arc<PacketDemultiplexer>,
    routing_table: Arc<RwLock<RoutingTable>>,
    helper_tasks: JoinSet<()>,
}

#[derive(Debug)]
pub(crate) struct DhtServiceFailure {
    message: String,
}

impl Display for DhtServiceFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for DhtServiceFailure {}

#[derive(Debug)]
pub(crate) struct PacketDemultiplexer {
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
    pub async fn run(&self) {
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

impl DhtV4 {
    #[instrument(skip_all)]
    pub async fn bootstrap_with_random_id(
        bind_addr: SocketAddrV4,
        external_addr: Ipv4Addr,
        known_nodes: Vec<SocketAddrV4>,
    ) -> Result<Self, DhtServiceFailure> {
        let socket = UdpSocket::bind(&bind_addr).await?;
        let socket = Arc::new(socket);

        // id is randomly generated according to BEP-42
        let our_id = random_idv4(&external_addr, rand::thread_rng().gen::<u8>());

        // the JoinSet keeps all the tasks required to make the DHT node functional
        let mut join_set = JoinSet::new();

        // all udp packets are sent to the channel, and the demultiplexer will route them into either
        // oneshot senders or a queue for severs to handle
        let (incoming_packets_tx, incoming_packets_rx) = mpsc::channel(1024);

        // all queries that servers should handle are sent on this channel
        let (queries_tx, queries_rx) = mpsc::channel(1024);

        // keep reading from sockets and place them on a queue for another task to place them into
        // the right slot
        let reading_socket = socket.clone();
        let socket_reader = async move {
            let mut buf = [0u8; 1500];
            loop {
                let (amount, socket_addr) = reading_socket
                    .recv_from(&mut buf)
                    .await
                    .expect("common MTU 1500 exceeded");
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
        };

        join_set
            .build_task()
            .name(&*format!("socket reader for {bind_addr}"))
            .spawn(socket_reader.instrument(info_span!("socket reader")));

        let demultiplexer = PacketDemultiplexer::new(incoming_packets_rx, queries_tx);
        let demultiplexer = Arc::new(demultiplexer);

        let demultiplexer_clone = demultiplexer.clone();
        join_set
            .build_task()
            .name(&*format!("message demultiplexer for {bind_addr}"))
            .spawn(async move {
                demultiplexer_clone.run().await;
            });

        let routing_table = Arc::new(RwLock::new(RoutingTable::new(&our_id)));

        let client = DhtClientV4::new(
            bind_addr,
            external_addr,
            socket.clone(),
            demultiplexer.clone(),
            routing_table.clone(),
            our_id,
        );
        let client = Arc::new(client);

        // ask all the known nodes for ourselves
        let mut bootstrap_join_set = JoinSet::new();

        for contact in known_nodes {
            bootstrap_join_set
                .build_task()
                .name(&*format!("bootstrap with {contact}"))
                .spawn(Self::bootstrap_from(client.clone(), contact));
        }

        while let Some(t) = bootstrap_join_set.join_next().await {}

        info!(
            "DHT bootstrapped, routing table has {} nodes",
            routing_table.read().await.node_count()
        );
        drop(bootstrap_join_set);

        // only spawn the server after the bootstrap has completed
        let server = DhtServer::new(queries_rx, socket.clone(), our_id, routing_table.clone());
        let server = Arc::new(server);

        join_set
            .build_task()
            .name(&*format!("DHT server for {bind_addr}"))
            .spawn(server.clone().run());

        let dht = DhtV4 {
            client: client.clone(),
            server: server.clone(),
            demultiplexer,
            routing_table: routing_table.clone(),

            helper_tasks: join_set,
        };

        Ok(dht)
    }

    pub async fn run(mut self) {
        while let Some(_) = self.helper_tasks.join_next().await {}
    }

    #[instrument(skip_all)]
    async fn bootstrap_from(dht: Arc<DhtClientV4>, contact: SocketAddrV4) -> Result<(), BootstrapError> {
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

                let dht = dht.clone();
                let contact: SocketAddrV4 = node.into();
                let our_id = dht.our_id.clone();
                let transaction_id = dht.transaction_id_pool.next();
                let query = Krpc::new_find_node_query(Box::new(transaction_id.to_be_bytes()), our_id, our_id.clone());
                Builder::new()
                    .name(&*format!("leave level bootstrap to {}", contact))
                    .spawn(async move {
                        let response = timeout(Duration::from_secs(5), async {
                            dht.send_message(&query, &contact)
                                .await
                                .expect("failure to send message")
                        })
                        .await?;

                        if let Krpc::FindNodeGetPeersNonCompliantResponse(response) = response {
                            let nodes: Vec<_> = response
                                .body
                                .nodes
                                .chunks_exact(26)
                                .map(|x| CompactNodeContact::new(x.try_into().unwrap()))
                                .collect();
                            {
                                // add the bootstrapping node to our routing table
                                let mut table = dht.routing_table.write().await;
                                table.add_new_node(CompactNodeContact::from_node_id_and_addr(
                                    &response.body.id,
                                    &contact,
                                ));
                            }
                        }
                        Ok::<_, color_eyre::Report>(())
                    });
            }
            info!("bootstrapping with {contact} succeeded");
        }
        Ok(())
    }

    async fn find_node(&self, target: &NodeId) -> Result<CompactNodeContact, DhtServiceFailure> {
        let dht = self.client.clone();
        dht.find_node(target).await
    }

    async fn get_peers(&self, info_hash: &InfoHash) -> Result<(Box<[u8]>, Vec<CompactPeerContact>), DhtServiceFailure> {
        let dht = self.client.clone();
        dht.get_peers(*info_hash).await
    }

    async fn ping(&self, contact: &SocketAddrV4) -> Result<(), DhtServiceFailure> {
        let dht = self.client.clone();
        dht.ping(*contact).await
    }

    async fn announce_peer(
        &self,
        info_hash: &InfoHash,
        contact: &SocketAddrV4,
        port: Option<u16>,
        token: Box<[u8]>,
    ) -> Result<(), DhtServiceFailure> {
        let dht = self.client.clone();
        dht.announce_peers(*contact, *info_hash, port, token).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        dht_service::{dht_client::DhtClientV4, DhtV4},
        message::Krpc,
    };
    use opentelemetry::global;
    use rand::RngCore;
    use std::{
        net::SocketAddrV4,
        str::FromStr,
        sync::{Arc, Once},
    };
    use tokio::time::{self, timeout};
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
        );

        let dht = timeout(time::Duration::from_secs(60), dht).await??;
        info!("Now I'm bootstrapped!");
        {
            let table = dht.client.routing_table.read().await;
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
        .await;

        // let info_hash = BigUint::from_str_radix("233b78ca585fe0a8c9e8eb4bda03f52e8b6f554b", 16).unwrap();
        // let info_hash = info_hash.to_bytes_be();
        //
        // let (token, peers) = dht.inner.get_peers(info_hash.as_slice().try_into()?).await?;
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
