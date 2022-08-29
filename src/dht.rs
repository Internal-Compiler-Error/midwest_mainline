use crate::{dht::dht_server::DhtServer, domain_knowledge::NodeId, routing::RoutingTable};

use rand::{Rng, RngCore};
use std::{
    error::Error,
    fmt::{Display, Formatter},
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, RwLock},
    task::{Builder, JoinError, JoinSet},
    time::error::Elapsed,
};

use crate::dht::message_broker::{MailBoxes, PostalOffice};
use dht_client::DhtClientV4;
use tracing::{info, instrument};

pub mod dht_client;
pub mod dht_server;
pub(crate) mod message_broker;
mod transaction_id_pool;

/// The DHT service, it contains pointers to a server and client, it's main role is to run the
/// tasks required to make DHT alive
#[derive(Debug)]
#[allow(dead_code)]
pub struct DhtV4<M, P>
where
    // D: MessageDemultiplexer + 'static + Send + Sync,
    M: MailBoxes + 'static + Send + Sync,
    P: PostalOffice + 'static + Send + Sync,
{
    client: Arc<DhtClientV4<M, P>>,
    server: Arc<DhtServer>,
    // message_broker: Arc<MessageBroker<D>>,
    routing_table: Arc<RwLock<RoutingTable>>,
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

impl<M, P> DhtV4<M, P>
where
    // D: MessageDemultiplexer + 'static + Send + Sync,
    M: MailBoxes + 'static + Send + Sync,
    P: PostalOffice + 'static + Send + Sync,
    // BootstrapError: From<<P as PostalOffice>::SendAddrError>,
    // RequestError: From<<P as PostalOffice>::SendAddrError>,
{
    /// Create a new DHT service the id is generated randomly in according to BEP-42 using the
    /// external IP address of the machine. Note there is no way to verify the external IP address
    /// is correct and it's duty to make sure it's correct.
    #[instrument(skip_all)]
    pub async fn bootstrap_with_random_id(
        bind_addr: SocketAddrV4,
        external_addr: Ipv4Addr,
        known_nodes: Vec<SocketAddrV4>,
        mail_boxes: M,
        postal_office: P,
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
        let (queries_tx, queries_rx) = mpsc::channel(1024);

        // all outgoing messages are sent on this channel
        // let (outgoing_tx, outgoing_rx) = mpsc::channel(1024);

        //let udp_message_demultiplexer = UdpMessageDemultiplexer::new(socket, outgoing_rx, incoming_packets_tx);

        // let message_broker = MessageBroker::new(incoming_packets_rx, queries_tx.clone(), outgoing_tx, demulitplexer);
        // let message_broker = Arc::new(message_broker);

        let routing_table = Arc::new(RwLock::new(RoutingTable::new(&our_id)));

        // bootstrap the DHT network
        let table = Arc::clone(&routing_table);
        let client = Builder::new()
            .name("Bootstrapping onto DHT")
            .spawn(async move {
                DhtClientV4::bootstrap_from(
                    our_id,
                    table,
                    bind_addr,
                    mail_boxes,
                    postal_office,
                    known_nodes,
                    Duration::from_secs(180),
                )
                .await
            })
            .await
            .expect("Failed to bootstrap DHT")
            .expect("Failed to bootstrap DHT");

        info!(
            "DHT bootstrapped, routing table has {} nodes",
            routing_table.read().await.node_count()
        );
        // drop(bootstrap_join_set);

        // only spawn the server after the bootstrap has completed
        let server = DhtServer::new(queries_rx, queries_tx.clone(), our_id, routing_table.clone());
        let server = Arc::new(server);

        join_set
            .build_task()
            .name(&*format!("DHT server for {bind_addr}"))
            .spawn(server.clone().run());

        let dht = DhtV4 {
            client: client.clone(),
            server: server.clone(),
            // message_broker,
            routing_table: routing_table.clone(),

            helper_tasks: join_set,
        };

        Ok(dht)
    }

    /// Keep the DHT running so you can't use the clients and servers, usually you put spawn this
    /// and abort the task when desired
    pub async fn run(mut self) {
        while let Some(_) = self.helper_tasks.join_next().await {}
    }

    /// Returns a handle to the client so you can perform queries
    pub fn client(&self) -> Arc<DhtClientV4<M, P>> {
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
    use opentelemetry::global;
    use std::sync::Once;

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

    // #[tokio::test(flavor = "multi_thread")]
    // async fn bootstrap() -> color_eyre::Result<()> {
    //     TEST_INIT.call_once(set_up_tracing);
    //
    //     let external_ip = public_ip::addr_v4().await.unwrap();
    //
    //     let dht = DhtV4::bootstrap_with_random_id(
    //         SocketAddrV4::from_str("0.0.0.0:51413").unwrap(),
    //         external_ip,
    //         vec![
    //             // dht.tansmissionbt.com
    //             "87.98.162.88:6881".parse().unwrap(),
    //             // router.utorrent.com
    //             "67.215.246.10:6881".parse().unwrap(),
    //             // router.bittorrent.com, ironically that this almost never responds
    //             "82.221.103.244:8991".parse().unwrap(),
    //             // dht.aelitis.com
    //             "174.129.43.152:6881".parse().unwrap(),
    //         ],
    //     );
    //
    //     let dht = timeout(time::Duration::from_secs(60), dht).await??;
    //     info!("Now I'm bootstrapped!");
    //     {
    //         let table = dht.client.routing_table.read().await;
    //         info!(
    //             "we've found {:?} nodes and recorded in our routing table",
    //             table.node_count()
    //         );
    //     }
    //
    //     let client = dht.client;
    //
    //     let mut rng = rand::thread_rng();
    //     let mut bytes = [0u8; 20];
    //     rng.fill_bytes(&mut bytes);
    //
    //     let node = client.find_node(&bytes).await;
    //     if let Ok(node) = node {
    //         info!("found node {:?}", node);
    //     } else {
    //         info!("I guess we just didn't find anything")
    //     }
    //
    //     Ok(())
    // }

    // #[tokio::test(flavor = "multi_thread")]
    // async fn find_peers() -> color_eyre::Result<()> {
    //     TEST_INIT.call_once(set_up_tracing);
    //
    //     let external_ip = public_ip::addr_v4().await.unwrap();
    //
    //     let dht = DhtV4::bootstrap_with_random_id(
    //         SocketAddrV4::from_str("0.0.0.0:51413").unwrap(),
    //         external_ip,
    //         vec![
    //             // dht.tansmissionbt.com
    //             "87.98.162.88:6881".parse().unwrap(),
    //             // router.utorrent.com
    //             "67.215.246.10:6881".parse().unwrap(),
    //             // router.bittorrent.com, ironically that this almost never responds
    //             "82.221.103.244:6881".parse().unwrap(),
    //             // dht.aelitis.com
    //             "174.129.43.152:6881".parse().unwrap(),
    //         ],
    //     )
    //     .await?;
    //
    //     let info_hash = BigUint::from_str_radix("233b78ca585fe0a8c9e8eb4bda03f52e8b6f554b", 16).unwrap();
    //     let info_hash = info_hash.to_bytes_be();
    //
    //     let client = dht.client();
    //     let (token, peers) = client.get_peers(info_hash.as_slice().try_into()?).await?;
    //     info!("token {token:?}, peers {peers:?}");
    //     Ok(())
    // }

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

    // #[tokio::test(flavor = "multi_thread")]
    // async fn run() -> color_eyre::Result<()> {
    //     TEST_INIT.call_once(set_up_tracing);
    //     let external_ip = public_ip::addr_v4().await.unwrap();
    //
    //     let dht = DhtV4::bootstrap_with_random_id(
    //         SocketAddrV4::from_str("0.0.0.0:51413").unwrap(),
    //         external_ip,
    //         vec![
    //             // dht.tansmissionbt.com
    //             "87.98.162.88:6881".parse().unwrap(),
    //             // router.utorrent.com
    //             "67.215.246.10:6881".parse().unwrap(),
    //             // router.bittorrent.com, ironically that this almost never responds
    //             "82.221.103.244:6881".parse().unwrap(),
    //             // dht.aelitis.com
    //             "174.129.43.152:6881".parse().unwrap(),
    //         ],
    //     )
    //     .await?;
    //
    //     dht.run().await;
    //
    //     Ok(())
    // }
}