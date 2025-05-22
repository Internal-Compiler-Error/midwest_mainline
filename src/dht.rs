pub mod dht_handle;
pub mod krpc_broker;
pub mod router;
mod txn_id_generator;

use crate::{
    dht::dht_handle::DhtHandle,
    our_error::OurError,
    types::{NodeId, NODE_ID_LEN},
    utils::{base64_dec, base64_enc, db_get, db_put},
};
use diesel::{
    connection::SimpleConnection,
    prelude::*,
    r2d2::{self, ConnectionManager, CustomizeConnection, Pool},
    sql_types,
};
use tracing::info;

use krpc_broker::KrpcBroker;
use rand::{Rng, RngCore};
use router::Router;
use std::{
    env,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
};
use tokio::{net::UdpSocket, task::JoinSet};
use txn_id_generator::TxnIdGenerator;

/// The DHT service, it contains pointers to a server and client, it's main role is to run the
/// tasks required to make DHT alive
#[derive(Debug)]
#[allow(dead_code)]
pub struct DhtV4 {
    server: Arc<DhtHandle>,
    message_broker: KrpcBroker,
    router: Router,
    helper_tasks: JoinSet<()>,
}

#[derive(Debug)]
struct SensibleOptions;

impl CustomizeConnection<SqliteConnection, r2d2::Error> for SensibleOptions {
    fn on_acquire(&self, conn: &mut SqliteConnection) -> Result<(), r2d2::Error> {
        // NOTE: very important to set the timeout before any other pragma because they can require
        // taking a lock themselves!
        conn.batch_execute(
            "
            PRAGMA busy_timeout = 5000;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;
            ",
        )
        .map_err(diesel::r2d2::Error::QueryError)?;

        xor_utils::register_impl(conn, |x: *const [u8], y: *const [u8]| {
            // safety: they came from C, not my problem if they're wonky
            let x = unsafe { &*x };
            let y = unsafe { &*y };

            let mut buf = [0u8; NODE_ID_LEN];
            for i in 0..NODE_ID_LEN {
                buf[i] = x[i] ^ y[i]
            }

            buf
        })
        .unwrap();

        Ok(())
    }

    fn on_release(&self, _conn: SqliteConnection) {}
}

define_sql_function! {
    /// In Kademlia, bitwise xor is the distance metric. As we are storing the node id in BLOB, we
    /// can just xor each bytes and return as a BLOB, ordering on BLOB is defined as C `memcmp`,
    /// see <https://sqlite.org/datatype3.html#sort_order>
    fn xor(x: sql_types::Binary, y: sql_types::Binary) -> sql_types::Binary;
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

fn resume_identity(conn: &mut SqliteConnection, public_ip: Ipv4Addr) -> Result<NodeId, diesel::result::Error> {
    conn.transaction(|conn| {
        let prev_ip = db_get("public_ip", conn)?;
        let prev_id = db_get("id", conn)?;

        let Some(prev_ip) = prev_ip else {
            // No previous IP, so store the current one and generate a new ID
            return new_identity(public_ip, conn);
        };

        let prev_ip: Ipv4Addr = prev_ip.parse().unwrap();
        if prev_ip == public_ip {
            // IP matches, reuse ID
            let id_in_base64 =
                prev_id.expect("if the public ip of last session matches this session, id should have been set");
            let id = base64_dec(id_in_base64);
            Ok(NodeId::from_bytes_unchecked(&*id))
        } else {
            // IP changed, generate new ID
            let id = random_idv4(&public_ip, rand::thread_rng().gen::<u8>());
            db_put("id".to_string(), base64_enc(id.as_bytes()), conn)?;
            Ok(id)
        }
    })
}

fn new_identity(public_ip: Ipv4Addr, conn: &mut SqliteConnection) -> Result<NodeId, diesel::result::Error> {
    db_put("public_ip".to_string(), public_ip.to_string(), conn)?;
    let id = random_idv4(&public_ip, rand::thread_rng().gen::<u8>());
    db_put("id".to_string(), base64_enc(id.as_bytes()), conn)?;
    Ok(id)
}

impl DhtV4 {
    /// Create a new DHT service, if the external_addr matches what was used last time, then reuse
    /// the previous identity, otherwise adopt a new id. Note there is no way to verify the external IP address
    /// is correct and it's duty to make sure it's correct.
    #[tracing::instrument(skip_all)]
    pub async fn bootstrap_with_stable_id(
        bind_addr: SocketAddrV4,
        external_addr: Ipv4Addr,
        known_nodes: Vec<SocketAddrV4>,
    ) -> Result<Self, OurError> {
        let socket = UdpSocket::bind(&bind_addr).await?;

        // TODO: make this configurable
        let database_url = env::var("DATABASE_URL").expect("No DATABASE_URL var");
        let manager = ConnectionManager::<SqliteConnection>::new(database_url);
        let db = Pool::builder()
            .test_on_check_out(true)
            .connection_customizer(Box::new(SensibleOptions {}))
            .build(manager)
            .expect("Could not build DB connection pool");

        let our_id = resume_identity(&mut db.get().unwrap(), external_addr)?;

        // the JoinSet keeps all the tasks required to make the DHT node functional
        let mut join_set = JoinSet::new();

        let txn_id_generator = Arc::new(TxnIdGenerator::new());

        let message_broker = KrpcBroker::new(socket, db.clone(), txn_id_generator.clone());

        let message_broker_clone = message_broker.clone();
        join_set
            .build_task()
            .name(&*format!("message broker for {bind_addr}"))
            .spawn(async move {
                let _ = message_broker_clone.run().await;
            })
            .unwrap();

        let router = Router::new(our_id, message_broker.clone(), db.clone());

        let rx = message_broker.subscribe_inbound();
        let inner_router = router.clone();
        join_set
            .build_task()
            .name("Router")
            .spawn(async move { inner_router.run(rx).await })
            .unwrap();

        let server = DhtHandle::new(our_id, router.clone(), message_broker.clone(), db.clone());
        let server = Arc::new(server);

        join_set
            .build_task()
            .name(&*format!("DHT server for {bind_addr}"))
            .spawn(server.clone().run())
            .unwrap();

        let dht = DhtV4 {
            server: server.clone(),
            message_broker,
            router: router.clone(),
            helper_tasks: join_set,
        };

        // ask all the known nodes for ourselves
        let mut bootstrap_join_set = JoinSet::new();

        for contact in known_nodes {
            bootstrap_join_set
                .build_task()
                .name(&*format!("bootstrap with {contact}"))
                .spawn(Self::bootstrap_from(server.clone(), contact))
                .unwrap();
        }

        while let Some(_) = bootstrap_join_set.join_next().await {}
        drop(bootstrap_join_set);

        info!("DHT bootstrapped, routing table has {} nodes", router.node_count());

        Ok(dht)
    }

    /// Keep the DHT running so you can use the clients and servers, usually you put spawn this
    /// and abort the task when desired
    pub async fn run(mut self) {
        while let Some(_) = self.helper_tasks.join_next().await {}
    }

    pub async fn populate_random(&self) {}

    /// Given a known node, perform one find node to ourself add the response to the routing table
    /// *and* do one additional round of find node to all the returned nodes from the bootstrapping
    /// node.
    ///
    /// This is subject to change in the future.
    #[tracing::instrument(skip_all)]
    async fn bootstrap_from(dht: Arc<DhtHandle>, endpoint: SocketAddrV4) -> Result<(), OurError> {
        let our_id = dht.our_id.clone();

        info!("bootstrapping with {endpoint}");

        let node_id = dht.ping(endpoint).await?;
        info!("obtained bootstrap node id {node_id:?}");

        dht.router.add(node_id, endpoint);
        dht.find_node(our_id).await;

        info!("{endpoint} bootstrap success");
        Ok(())
    }

    /// Returns a handle to the server, currently there is no public API for the server. In the
    /// the sever will support some APIs to allow you to query about its state
    pub fn handle(&self) -> Arc<DhtHandle> {
        self.server.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        dht::DhtV4,
        types::InfoHash,
        // types::{InfoHash /* , NodeId */},
    };
    // use opentelemetry::global;
    // use rand::RngCore;
    use std::{net::SocketAddrV4, str::FromStr, sync::Once};
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

        // global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
        // let tracer = opentelemetry_jaeger::new_pipeline().install_simple().unwrap();

        // let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(console_subscriber::spawn())
            // .with(telemetry)
            .with(fmt_layer)
            .init();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bootstrap() -> color_eyre::Result<()> {
        TEST_INIT.call_once(set_up_tracing);

        let external_ip = public_ip::addr_v4().await.unwrap();

        let dht = DhtV4::bootstrap_with_stable_id(
            SocketAddrV4::from_str("0.0.0.0:44444").unwrap(),
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

        // let server = dht.handle();
        // let mut rng = rand::thread_rng();
        // let mut bytes = [0u8; 20];
        // rng.fill_bytes(&mut bytes);
        //
        // let node = server.find_node(NodeId(bytes)).await;
        // if let Ok(node) = node {
        //     println!("found node {:?}", node);
        // } else {
        //     println!("I guess we just didn't find anything")
        // }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn find_peers() -> color_eyre::Result<()> {
        TEST_INIT.call_once(set_up_tracing);

        let external_ip = public_ip::addr_v4().await.unwrap();

        let dht = DhtV4::bootstrap_with_stable_id(
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

        // XXX: get_peers design is problematic, impl has been replaced with todo!(), see the
        // comment on the function itself for more info.
        //
        let info_hash = InfoHash::from_hex_str("233b78ca585fe0a8c9e8eb4bda03f52e8b6f554b");

        let handle = dht.handle();
        let peers = handle.get_peers(info_hash).await?;
        info!("peers {peers:?}");
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

        let dht = DhtV4::bootstrap_with_stable_id(
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
