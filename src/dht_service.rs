pub mod dht_client;
pub mod dht_server;
pub mod message_broker;
pub mod peer_guide;
mod transaction_id_pool;

use crate::{
    dht_service::dht_server::DhtServer,
    domain_knowledge::{NodeId, TransactionId},
    message::Krpc,
    our_error::OurError,
};

use message_broker::MessageBroker;
use peer_guide::PeerGuide;
use rand::{Rng, RngCore};
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    task::{Builder as TskBuilder, JoinSet},
    time::timeout,
};

use dht_client::DhtHandle;
use tracing::{info, instrument};

/// The DHT service, it contains pointers to a server and client, it's main role is to run the
/// tasks required to make DHT alive
#[derive(Debug)]
#[allow(dead_code)]
pub struct DhtV4 {
    client: Arc<DhtHandle>,
    server: Arc<DhtServer>,
    message_broker: Arc<MessageBroker>,
    peer_guide: Arc<PeerGuide>,
    helper_tasks: JoinSet<()>,
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

impl DhtV4 {
    /// Create a new DHT service the id is generated randomly in according to BEP-42 using the
    /// external IP address of the machine. Note there is no way to verify the external IP address
    /// is correct and it's duty to make sure it's correct.
    /// # Arguments
    /// * `external_addr`: the ip address that can be reachable from the outside, in Ipv4 land, it
    /// means this needs to be a public IP
    #[instrument(skip_all)]
    pub async fn bootstrap_with_random_id(
        bind_addr: SocketAddrV4,
        external_addr: Ipv4Addr,
        known_nodes: Vec<SocketAddrV4>,
    ) -> Result<Self, OurError> {
        let socket = UdpSocket::bind(&bind_addr).await?;

        // id is randomly generated according to BEP-42
        let our_id = random_idv4(&external_addr, rand::thread_rng().gen::<u8>());

        // the JoinSet keeps all the tasks required to make the DHT node functional
        let mut join_set = JoinSet::new();

        let message_broker = MessageBroker::new(socket);
        let message_broker = Arc::new(message_broker);

        let message_broker_clone = message_broker.clone();
        join_set
            .build_task()
            .name(&*format!("message broker for {bind_addr}"))
            .spawn(async move {
                let _ = message_broker_clone.run().await;
            })
            .unwrap();

        let peer_guide = Arc::new(PeerGuide::new(our_id));

        let rx = message_broker.subscribe_inbound();
        let peer_guide_clone = peer_guide.clone();
        join_set
            .build_task()
            .name("Peer guide")
            .spawn(async move { peer_guide_clone.run(rx).await })
            .unwrap();

        let client = DhtHandle::new(message_broker.clone(), peer_guide.clone(), our_id);
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

    /// Keep the DHT running so you can use the clients and servers, usually you put spawn this
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
    async fn bootstrap_from(dht: Arc<DhtHandle>, peer: SocketAddrV4) -> Result<(), OurError> {
        let our_id = dht.our_id.clone();
        let txn_id = dht.transaction_id_pool.next();
        let query = Krpc::new_find_node_query(TransactionId::from(txn_id), our_id, our_id);

        info!("bootstrapping with {peer}");
        let response = timeout(Duration::from_secs(5), async {
            dht.send_and_wait(query, peer).await.expect("failure to send message")
        })
        .await?;

        // TODO: use ensure!
        if let Krpc::FindNodeGetPeersResponse(response) = response {
            let mut nodes = response.nodes().clone();
            nodes.sort_unstable_by_key(|node| node.contact().0);
            nodes.dedup();

            for node in nodes {
                let dht = dht.clone();
                let contact = node.contact().0;
                let our_id = dht.our_id;
                let txn_id = dht.transaction_id_pool.next();
                let find_ourself = Krpc::new_find_node_query(TransactionId::from(txn_id), our_id, node.id());

                TskBuilder::new()
                    .name(&*format!("leave level bootstrap to {}", contact))
                    .spawn(async move {
                        let _response = timeout(Duration::from_secs(5), async {
                            dht.send_and_wait(find_ourself, contact)
                                .await
                                .expect("failure to send message")
                        })
                        .await?;
                        Ok::<_, OurError>(())
                    })
                    .unwrap();
            }
            info!("bootstrapping with {peer} succeeded");
        }
        Ok(())
    }

    /// Returns a handle to the client so you can perform queries
    pub fn client(&self) -> Arc<DhtHandle> {
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
    use crate::{
        dht_service::DhtV4,
        domain_knowledge::{InfoHash, NodeId},
    };
    use num::{BigUint, Num};
    use opentelemetry::global;
    use rand::RngCore;
    use std::{net::SocketAddrV4, str::FromStr, sync::Once};
    use tokio::time::{self, timeout};
    use tracing::info;
    use tracing_subscriber::{filter::LevelFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt, Layer};

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
                "82.221.103.244:8991".parse().unwrap(),
                // dht.aelitis.com
                "174.129.43.152:6881".parse().unwrap(),
            ],
        );

        let dht = timeout(time::Duration::from_secs(60), dht).await??;
        info!("Now I'm bootstrapped!");
        // {
        //     let table = dht.client.routing_table;
        //     info!(
        //         "we've found {:?} nodes and recorded in our routing table",
        //         table.node_count()
        //     );
        // }

        let client = dht.client;

        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; 20];
        rng.fill_bytes(&mut bytes);

        let node = client.find_node(NodeId(bytes)).await;
        println!("{node:?}");
        if let Ok(node) = node {
            println!("found node {:?}", node);
        } else {
            println!("I guess we just didn't find anything")
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
        .await?;

        let info_hash = BigUint::from_str_radix("233b78ca585fe0a8c9e8eb4bda03f52e8b6f554b", 16).unwrap();
        let info_hash = info_hash.to_bytes_be();

        let mut stupid = [0u8; 20];
        stupid.copy_from_slice(&info_hash[0..20]);

        let info_hash = InfoHash(stupid);

        let client = dht.client();
        let (token, peers) = client.get_peers(info_hash).await?;
        info!("token {token:?}, peers {peers:?}");
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
            SocketAddrV4::from_str("0.0.0.0:44444").unwrap(),
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
