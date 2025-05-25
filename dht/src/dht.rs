pub mod dht_handle;
pub mod krpc_broker;
pub mod router;
mod txn_id_generator;

use crate::{
    dht::dht_handle::DhtHandle,
    our_error::OurError,
    types::{InfoHash, NODE_ID_LEN, NodeId, NodeInfo},
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
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
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
    addr: SocketAddrV4,
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
    let mut rng = rand::rng();
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
    id[2] = (((crc >> 8) & 0xf8) as u8) | (rng.random::<u8>() & 0x7);

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
            Ok(NodeId::from_bytes(&*id))
        } else {
            // IP changed, generate new ID
            let id = random_idv4(&public_ip, rand::rng().random::<u8>());
            db_put("id".to_string(), base64_enc(id.as_bytes()), conn)?;
            Ok(id)
        }
    })
}

fn new_identity(public_ip: Ipv4Addr, conn: &mut SqliteConnection) -> Result<NodeId, diesel::result::Error> {
    db_put("public_ip".to_string(), public_ip.to_string(), conn)?;
    let id = random_idv4(&public_ip, rand::rng().random::<u8>());
    db_put("id".to_string(), base64_enc(id.as_bytes()), conn)?;
    Ok(id)
}

impl DhtV4 {
    /// Create a new DHT service, if the external_addr matches what was used last time, then reuse
    /// the previous identity, otherwise adopt a new id. Note there is no way to verify the external IP address
    /// is correct and it's duty to make sure it's correct.
    ///
    /// The UdpSocket must be already binded to an ipv4 address
    pub fn with_stable_id(listen_socket: UdpSocket, external_addr: Ipv4Addr) -> Result<Self, OurError> {
        let local_addr = match listen_socket
            .local_addr()
            .expect("listen socket should already be binded per doc")
        {
            SocketAddr::V4(v4) => v4,
            _ => panic!("listen socket must be binded to ipv4"),
        };

        // TODO: make this configurable
        let database_url = env::var("DATABASE_URL").expect("No DATABASE_URL var");
        let manager = ConnectionManager::<SqliteConnection>::new(database_url);
        let db = Pool::builder()
            .test_on_check_out(true)
            .connection_customizer(Box::new(SensibleOptions {}))
            .build(manager)
            .expect("Could not build DB connection pool");

        let our_id = resume_identity(&mut db.get().unwrap(), external_addr)?;

        let message_broker = KrpcBroker::new(listen_socket, db.clone(), Arc::new(TxnIdGenerator::new()).clone());

        let router = Router::new(
            our_id,
            message_broker.clone(),
            db.clone(),
            message_broker.subscribe_inbound(),
        );

        let server = Arc::new(DhtHandle::new(
            our_id,
            router.clone(),
            message_broker.clone(),
            db.clone(),
        ));

        let dht = DhtV4 {
            server: server.clone(),
            message_broker,
            router: router.clone(),
            addr: local_addr,
        };

        Ok(dht)
    }

    pub async fn bootstrap(&self, known_nodes: Vec<SocketAddrV4>) -> Result<(), OurError> {
        let mut bootstrap_join_set = JoinSet::new();

        for contact in known_nodes {
            bootstrap_join_set
                .build_task()
                .name(&*format!("bootstrap with {contact}"))
                .spawn(Self::bootstrap_from(self.handle().clone(), contact))
                .unwrap();
        }

        bootstrap_join_set.join_all().await;

        info!("DHT bootstrapped, routing table has {} nodes", self.node_count());

        Ok(())
    }

    pub fn node_count(&self) -> usize {
        self.router.node_count()
    }

    pub async fn find_node(&self, target: NodeId) -> Vec<NodeInfo> {
        self.server.find_node(target).await
    }

    pub async fn get_peers(&self, info_hash: InfoHash) -> Vec<SocketAddrV4> {
        match self.server.get_peers(info_hash).await {
            Result::Ok(peers) => peers,
            _ => vec![],
        }
    }

    /// Keep the DHT running so you can use the clients and servers, usually you put spawn this
    /// and abort the task when desired
    pub async fn run(&self) {
        let mut join_set = JoinSet::new();

        let krpc_broker = self.message_broker.clone();
        join_set
            .build_task()
            .name(&*format!("message broker for {}", self.addr))
            .spawn(async move {
                let _ = krpc_broker.run().await;
            })
            .unwrap();

        let router = self.router.clone();
        join_set
            .build_task()
            .name("Router")
            .spawn(async move { router.clone().run().await })
            .unwrap();

        join_set
            .build_task()
            .name(&*format!("DHT server"))
            .spawn(self.server.clone().run())
            .unwrap();

        join_set.join_all().await;
    }

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
        // types::{InfoHash /* , NodeId */},
    };
    // use opentelemetry::global;
    // use rand::RngCore;
    use std::{
        net::SocketAddrV4,
        str::FromStr,
        sync::{Arc, Once},
    };
    use tokio::net::UdpSocket;
    use tracing::info;
    use tracing_subscriber::{Layer, filter::LevelFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

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

        let socket = UdpSocket::bind(SocketAddrV4::from_str("0.0.0.0:44444").unwrap())
            .await
            .unwrap();
        let dht = DhtV4::with_stable_id(socket, external_ip).unwrap();

        let dht = Arc::new(dht);
        let dhtt = Arc::clone(&dht);
        let dht_eventloop = tokio::spawn(async move {
            dhtt.run().await;
        });
        dht.bootstrap(vec![
            // dht.tansmissionbt.com
            "87.98.162.88:6881".parse().unwrap(),
            // router.utorrent.com
            "67.215.246.10:6881".parse().unwrap(),
            // router.bittorrent.com, ironically that this almost never responds
            "82.221.103.244:8991".parse().unwrap(),
            // dht.aelitis.com
            "174.129.43.152:6881".parse().unwrap(),
        ])
        .await
        .unwrap();
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

        drop(dht_eventloop);
        Ok(())
    }
}
