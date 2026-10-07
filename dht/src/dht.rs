//! The wired-up DHT node: [`DhtSession`] owns the four moving parts and runs them.
//!
//! - [`RpcManager`] is the message broker: the only owner of the UDP socket. Outbound
//!   queries register their transaction id and await the response on a oneshot; inbound
//!   queries, and answers matched to a pending query of ours, are fanned out to every
//!   subscriber (routing table, server); other answers are dropped.
//! - [`RoutingTable`] is the k-bucket store, in memory and written behind to SQLite so
//!   contacts survive restarts. It also learns passively from every inbound packet.
//! - [`DhtClient`] (handle via [`DhtSession::handle`]) runs iterative lookups.
//! - `DhtServer` answers inbound queries.
//!
//! What the node stores (peers announced to it) either expires after BEP 5's 45 minutes or,
//! with [`Retention::Forever`], is kept so the node doubles as a long-term index; see
//! [`DhtSession::with_retention`].
//!
//! Both halves share one `SharedState`; nothing is owned twice.
//!
//! A session is one address family: its socket's. BEP 32 runs IPv4 and IPv6 as two
//! independent DHTs, so a dual-stack host runs two sessions, one per socket, over the same
//! database (the routing tables are kept apart by family, the announced peers are shared),
//! and pairs them with [`DhtSession::pair_with`] so each can seed and answer for the other.
//! A host with more global addresses can run a node on each (BEP 45,
//! [`DhtSession::on_own_address`]): its own id and routing table, the same store.

pub mod bep42;
pub mod client;
pub mod crawler;
mod external_ip;
pub mod item;
mod query_limit;
pub mod routing_table;
pub mod rpc_manager;
mod scope;
pub(crate) mod server;
pub(crate) mod state;
mod txn_id_generator;

use crate::{
    dht::client::DhtClient,
    dht::server::DhtServer,
    dht::state::SharedState,
    message::KrpcBody,
    our_error::{OurError, naur},
    types::{Family, InfoHash, NodeId, NodeInfo},
    utils::{base64_dec, base64_enc, db_get, db_put},
};
use diesel::{
    connection::SimpleConnection,
    prelude::*,
    r2d2::{self, ConnectionManager, CustomizeConnection, Pool},
    sql_types,
};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::time::Duration;
use tracing::{info, warn};

use rand::RngExt;
use routing_table::RoutingTable;
use rpc_manager::RpcManager;
use scope::Scope;
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
};
use tokio::{net::UdpSocket, task::JoinSet, time::interval};
use txn_id_generator::TxnIdGenerator;

pub use state::{PEER_LIFETIME, Retention, StoredPeer};

pub(crate) const MIGRATIONS: EmbeddedMigrations = embed_migrations!("../migrations");

/// How much longer bootstrap waits for the other routers once one answered: they answer within
/// a round trip or not at all
const BOOTSTRAP_STRAGGLERS: Duration = Duration::from_millis(300);
/// Nodes a bootstrap wants in the table before it's done, and the rounds of lookups it gives
/// up after; the table goes on growing from every lookup afterwards
const BOOTSTRAP_ENOUGH: usize = 32;
const BOOTSTRAP_RETRIES: usize = 3;
/// SQLite connections a node keeps: more than the server answers from the store at once (see
/// `server`), so lookups, the crawler and the table's writer get one too
const DB_CONNECTIONS: u32 = 16;

/// The DHT service, it contains pointers to a server and client, it's main role is to run the
/// tasks required to make DHT alive
#[derive(Debug)]
pub struct DhtSession {
    client: DhtClient,
    server: DhtServer,
    rpc_manager: RpcManager,
    routing_table: RoutingTable,
    state: Arc<SharedState>,
    retention: Retention,
    addr: SocketAddr,
}

#[derive(Debug)]
pub(crate) struct SensibleOptions;

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

        // a panic here would unwind into SQLite, so blobs of other lengths (none we write) are
        // xored as far as both go rather than indexed
        xor_utils::register_impl(conn, |x: *const [u8], y: *const [u8]| {
            // SAFETY: SQLite hands over its two BLOB arguments, valid for the call
            let (x, y) = unsafe { (&*x, &*y) };
            x.iter().zip(y).map(|(a, b)| a ^ b).collect::<Vec<u8>>()
        })
        .map_err(diesel::r2d2::Error::QueryError)
    }

    fn on_release(&self, _conn: SqliteConnection) {}
}

define_sql_function! {
    /// In Kademlia, bitwise xor is the distance metric. As we are storing the node id in BLOB, we
    /// can just xor each bytes and return as a BLOB, ordering on BLOB is defined as C `memcmp`,
    /// see <https://sqlite.org/datatype3.html#sort_order>
    fn xor(x: sql_types::Binary, y: sql_types::Binary) -> sql_types::Binary;
}

/// The node id for `public_ip`: BEP 42's when the address is known; an IPv6 node that doesn't
/// know its address yet gets a random one (an IPv4 one gets 0.0.0.0's, as it always has).
fn mint_id(public_ip: IpAddr) -> NodeId {
    let rand = rand::rng().random::<u8>();
    match public_ip {
        IpAddr::V6(ip) if ip.is_unspecified() => NodeId(rand::rng().random()),
        ip => bep42::mint(ip, rand),
    }
}

/// `misc` row the vote in `external_ip` is stored under
pub(crate) const OBSERVED_IP_KEY: &str = "observed_ip";

/// Reuse last session's identity for this scope if our public IP is unchanged; otherwise
/// mint a new one. BEP 42 binds the id to the IP, so keeping the old id across an IP change
/// would make us present an id other nodes consider invalid — and every stored bucket index
/// was computed against the old id anyway.
fn resume_identity(
    conn: &mut SqliteConnection,
    scope: &Scope,
    public_ip: IpAddr,
) -> Result<NodeId, diesel::result::Error> {
    let ip_key = scope.key("public_ip");
    let id_key = scope.key("id");
    conn.transaction(|conn| {
        let prev_ip = db_get(&ip_key, conn)?.and_then(|ip| ip.parse::<IpAddr>().ok());
        let prev_id = db_get(&id_key, conn)?.and_then(|id| NodeId::try_from_bytes(&base64_dec(id)?));

        if let (Some(prev_ip), Some(prev_id)) = (prev_ip, prev_id)
            && prev_ip == public_ip
        {
            return Ok(prev_id);
        }

        let id = mint_id(public_ip);
        db_put(ip_key.clone(), public_ip.to_string(), conn)?;
        db_put(id_key.clone(), base64_enc(id.as_bytes()), conn)?;
        if prev_id.is_some() {
            // every stored bucket was computed against the previous id; recompute
            recompute_buckets(&id, scope.table, conn)?;
        }
        Ok(id)
    })
}

/// Recompute every node's bucket against a new identity (bucket assignments are derived
/// from xor distance to our own id, so they go stale when the id changes).
fn recompute_buckets(our_id: &NodeId, table: i32, conn: &mut SqliteConnection) -> Result<(), diesel::result::Error> {
    use crate::schema::node::dsl::*;

    let rows: Vec<Vec<u8>> = node.filter(family.eq(table)).select(id).load(conn)?;
    for raw in rows {
        let Some(node_id) = NodeId::try_from_bytes(&raw) else {
            continue;
        };
        let b = routing_table::bucket_index(our_id, &node_id);
        diesel::update(node.filter(family.eq(table)).filter(id.eq(&raw)))
            .set(bucket.eq(b))
            .execute(conn)?;
    }
    Ok(())
}

/// The address other nodes saw this scope's node at last time, if enough of them agreed
fn known_external_ip(conn: &mut SqliteConnection, scope: &Scope) -> Result<Option<IpAddr>, diesel::result::Error> {
    Ok(db_get(&scope.key(OBSERVED_IP_KEY), conn)?
        .and_then(|ip| ip.parse().ok())
        .filter(|ip| Family::of_ip(ip) == scope.family))
}

impl DhtSession {
    /// Create a node whose BEP 42 id is derived from our external address and kept across
    /// starts while that address stays the same. Pass `None` to derive it from what other
    /// nodes reported the address to be last time (see `external_ip`); a caller that knows
    /// better passes the address. A first start with nothing known gets an id for 0.0.0.0
    /// (IPv4) or a random one (IPv6), and the next start fixes that.
    ///
    /// The node is of the socket's address family. An IPv6 socket should be IPv6-only
    /// (`IPV6_V6ONLY`): IPv4 belongs to the other DHT.
    pub fn with_stable_id(
        listen_socket: UdpSocket,
        external_addr: Option<IpAddr>,
        database_url: &str,
    ) -> Result<Self, OurError> {
        Self::open(listen_socket, external_addr, database_url, false)
    }

    /// BEP 45's multiple-address operation: one more node, on a socket bound to one of the
    /// host's global addresses, beside the family's own (`with_stable_id`) or other such nodes
    /// on the same database. It has a node id and a routing table of its own, kept under its
    /// address; announced peers are stored with everyone else's. Pass the address as
    /// `external_addr` where it's the one other nodes see (no NAT), so the id is BEP 42's
    /// from the first start.
    ///
    /// BEP 45 asks that such nodes not sit in one IPv6 /64: other nodes count a /64 as one host.
    pub fn on_own_address(
        listen_socket: UdpSocket,
        external_addr: Option<IpAddr>,
        database_url: &str,
    ) -> Result<Self, OurError> {
        Self::open(listen_socket, external_addr, database_url, true)
    }

    fn open(
        listen_socket: UdpSocket,
        external_addr: Option<IpAddr>,
        database_url: &str,
        own_address: bool,
    ) -> Result<Self, OurError> {
        let local_addr = listen_socket
            .local_addr()
            .expect("listen socket should already be binded per doc");
        let family = Family::of(&local_addr);
        if own_address && local_addr.ip().is_unspecified() {
            return Err(naur!("a node on an address of its own must be bound to that address"));
        }

        let manager = ConnectionManager::<SqliteConnection>::new(database_url);
        let db = Pool::builder()
            .max_size(DB_CONNECTIONS)
            .connection_timeout(Duration::from_secs(5))
            .connection_customizer(Box::new(SensibleOptions {}))
            .build(manager)
            .map_err(|e| naur!("could not open the database {database_url}: {e}"))?;

        let mut conn = db
            .get()
            .map_err(|e| naur!("could not check out a db connection: {e}"))?;
        // a fresh database file has no schema; WAL lets the routing table write while a
        // lookup reads
        conn.batch_execute("PRAGMA journal_mode = WAL")?;
        conn.run_pending_migrations(MIGRATIONS)
            .map_err(|e| naur!("could not migrate the database: {e}"))?;
        match routing_table::purge_shared_ips(&mut conn) {
            Ok(0) => {}
            Ok(n) => info!("dropped {n} routing table nodes sharing an address with another"),
            Err(e) => warn!("couldn't apply one node per address to the routing table: {e}"),
        }
        if let Err(e) = routing_table::fill_bep42(&mut conn) {
            warn!("couldn't work out the routing table's BEP 42 compliance: {e}");
        }
        let scope = match own_address {
            true => Scope::of_address(local_addr.ip(), &mut conn)?,
            false => Scope::primary(family),
        };
        let observed = known_external_ip(&mut conn, &scope)?;
        let external_addr = external_addr.filter(|ip| {
            let ours = Family::of_ip(ip) == family;
            if !ours {
                warn!("ignoring external address {ip} for the {family} node");
            }
            ours
        });
        let external_addr = match external_addr.or(observed) {
            Some(ip) => {
                info!("BEP 42 node id for external address {ip}");
                ip
            }
            None => {
                warn!(
                    "external {family} address not known yet: the node id is not BEP 42 compliant until the next start"
                );
                match family {
                    Family::V4 => Ipv4Addr::UNSPECIFIED.into(),
                    Family::V6 => Ipv6Addr::UNSPECIFIED.into(),
                }
            }
        };
        let our_id = resume_identity(&mut conn, &scope, external_addr)?;

        let rpc_manager =
            RpcManager::new(listen_socket, db.clone(), Arc::new(TxnIdGenerator::new()), observed).with_scope(scope);

        let routing_table = RoutingTable::new(our_id, rpc_manager.clone(), db.clone());

        let state = Arc::new(SharedState::new(
            our_id,
            routing_table.clone(),
            rpc_manager.clone(),
            db.clone(),
        ));

        let dht = DhtSession {
            client: DhtClient::new(state.clone()),
            server: DhtServer::new(state.clone()),
            rpc_manager,
            routing_table,
            state,
            retention: Retention::default(),
            addr: local_addr,
        };

        Ok(dht)
    }

    /// What happens to announced peers once they go stale; [`Retention::Expire`] unless set.
    /// Switching an existing database to `Expire` deletes whatever stale peers it holds.
    pub fn with_retention(mut self, retention: Retention) -> Self {
        self.retention = retention;
        self
    }

    /// BEP 43: a read-only node answers no queries and tells the nodes it asks so, which
    /// leave it out of their routing tables. For a host that can't take inbound UDP (behind
    /// a NAT that won't forward, or on a metered link); lookups and announces work as ever.
    pub fn with_read_only(self, read_only: bool) -> Self {
        self.rpc_manager.set_read_only(read_only);
        self
    }

    /// Makes `self` and `other`, nodes of the two address families on this host, aware of each
    /// other (BEP 32): each answers a `want` for the other's family from the other's table, and
    /// while one has few nodes, the other's lookups ask for its family too and hand it what
    /// comes back. Panics if both are of the same family; a second pairing is ignored.
    pub fn pair_with(&self, other: &DhtSession) {
        assert_ne!(self.family(), other.family(), "a pair is one node per address family");
        let _ = self.state.sibling.set(Arc::downgrade(&other.state));
        let _ = other.state.sibling.set(Arc::downgrade(&self.state));
    }

    /// The address family of this node, its socket's
    pub fn family(&self) -> Family {
        self.state.family
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Bootstraps from those of `known_nodes` in this node's address family; the rest are
    /// skipped. Every one is pinged; once the first answers (and the others had a moment to),
    /// lookups of our own id and two random ones fill the table. Slow or dead routers don't
    /// hold that up.
    pub async fn bootstrap(&self, known_nodes: Vec<SocketAddr>) -> Result<(), OurError> {
        let client = self.handle();
        let mut pings: FuturesUnordered<_> = known_nodes
            .into_iter()
            .filter(|c| Family::of(c) == self.family())
            .map(|contact| {
                let client = client.clone();
                // spawned, so a ping still in flight when the lookup starts adds its node later
                tokio::spawn(async move {
                    info!("bootstrapping with {contact}");
                    let pinged = client.ping(contact).await;
                    if let Err(e) = &pinged {
                        info!("bootstrap node {contact} didn't answer: {e}");
                    }
                    pinged
                })
            })
            .collect();
        let started = tokio::time::Instant::now();
        while let Some(pinged) = pings.next().await {
            if matches!(pinged, Ok(Ok(_))) {
                let _ =
                    tokio::time::timeout(BOOTSTRAP_STRAGGLERS, async { while pings.next().await.is_some() {} }).await;
                break;
            }
        }
        info!("bootstrap routers answered in {:?}", started.elapsed());
        // our own neighbourhood, and two random ones beside it: routers hand out a node or
        // three, and if those are dead one lookup dead-ends where three rarely all do
        let random = || NodeId(rand::rng().random());
        futures::future::join_all([client.our_id(), random(), random()].map(|target| client.find_node(target))).await;
        // still next to nothing: the routers' few referrals were all dead; they hand out
        // others for other targets
        let mut known = self.node_count();
        for _ in 0..BOOTSTRAP_RETRIES {
            if known >= BOOTSTRAP_ENOUGH {
                break;
            }
            futures::future::join_all([random(), random(), random()].map(|target| client.find_node(target))).await;
            // a small network (or a LAN) that's all known already
            let now = self.node_count();
            if now == known {
                break;
            }
            known = now;
        }

        let (compliant, nodes) = self.bep42_compliance();
        info!(
            "{} DHT bootstrapped in {:?}, routing table has {nodes} nodes, {compliant} with BEP 42 ids",
            self.family(),
            started.elapsed(),
        );

        Ok(())
    }

    /// Nodes in this family's routing table
    pub fn node_count(&self) -> usize {
        self.routing_table.node_count()
    }

    /// Nodes in this family's routing table whose ids are BEP 42 compliant, and all of them
    pub fn bep42_compliance(&self) -> (usize, usize) {
        self.routing_table.bep42_compliance()
    }

    pub async fn find_node(&self, target: NodeId) -> Vec<NodeInfo> {
        self.client.find_node(target).await
    }

    pub async fn get_peers(&self, info_hash: InfoHash) -> client::GetPeersResult {
        self.client.get_peers(info_hash).await
    }

    /// Info hashes we hold announced peers for (with [`Retention::Forever`], every one anyone
    /// ever announced to us): up to `limit` of them, in order, from the first past `after`.
    /// A long-term index holds millions, so it's read a page at a time.
    pub fn stored_swarms(&self, after: Option<InfoHash>, limit: usize) -> Vec<InfoHash> {
        self.state.stored_swarms(after, limit)
    }

    /// A BEP 51 crawler on this node, sending `per_second` queries a second once run; see
    /// [`crawler`]. What it finds is counted by [`DhtSession::sampled_count`].
    pub fn crawler(&self, per_second: u32) -> crawler::Crawler {
        crawler::Crawler::new(self.handle(), self.state.clone(), per_second)
    }

    /// Distinct info hashes a crawler on this database has sampled from other nodes (BEP 51)
    pub fn sampled_count(&self) -> usize {
        self.state.sampled_count()
    }

    /// The peers announced to us for `info_hash` that we still hold, stale ones included,
    /// most recently announced first.
    pub fn stored_peers(&self, info_hash: &InfoHash) -> Vec<StoredPeer> {
        self.state.stored_peers(info_hash)
    }

    /// Keep the DHT running so you can use the clients and servers, usually you put spawn this
    /// and abort the task when desired
    pub async fn run(&self) {
        let mut join_set = JoinSet::new();

        let rpc_manager = self.rpc_manager.clone();
        join_set
            .build_task()
            .name(&format!("message broker for {}", self.addr))
            .spawn(async move { rpc_manager.run().await })
            .unwrap();

        let routing_table = self.routing_table.clone();
        let router_inbox = self.rpc_manager.subscribe_inbound();
        join_set
            .build_task()
            .name("RoutingTable")
            .spawn(async move { routing_table.run(router_inbox).await })
            .unwrap();

        // nodes of the other family that answers carry (asked for with `want`) go to the
        // paired node's table
        let state = self.state.clone();
        let mut inbox = self.rpc_manager.subscribe_inbound();
        join_set
            .build_task()
            .name("cross-family seeding")
            .spawn(async move {
                let other = state.family.other();
                while let Some((msg, _)) = inbox.recv().await {
                    let KrpcBody::FindNodeGetPeersResponse(res) = &msg.body else {
                        continue;
                    };
                    let nodes = res.nodes_of(other);
                    if nodes.is_empty() {
                        continue;
                    }
                    let Some(sibling) = state.sibling() else { continue };
                    for node in nodes {
                        sibling.routing_table.vet(node.id(), node.end_point());
                    }
                }
            })
            .unwrap();

        let server = self.server.clone();
        join_set
            .build_task()
            .name("DHT server")
            .spawn(async move { server.run().await })
            .unwrap();

        if self.retention == Retention::Expire {
            let state = self.state.clone();
            join_set
                .build_task()
                .name("peer expiry")
                .spawn(async move {
                    let mut tick = interval(PEER_LIFETIME / 9);
                    loop {
                        tick.tick().await;
                        let state = state.clone();
                        let _ = tokio::task::spawn_blocking(move || {
                            let _ = state
                                .expire_peers()
                                .inspect_err(|e| warn!("couldn't expire peers: {e}"));
                            let _ = state
                                .expire_items()
                                .inspect_err(|e| warn!("couldn't expire BEP 44 items: {e}"));
                        })
                        .await;
                    }
                })
                .unwrap();
        }

        join_set.join_all().await;
    }

    /// Returns a cheap handle to the lookup client; cloning is a single refcount bump
    pub fn handle(&self) -> DhtClient {
        self.client.clone()
    }
}

#[cfg(test)]
mod recompute_tests {
    use super::{OBSERVED_IP_KEY, Scope, known_external_ip, resume_identity};
    use crate::dht::bep42::compliant as bep42_valid;
    use crate::dht::routing_table::bucket_index;
    use crate::schema::node::dsl as node_dsl;
    use crate::test_support::memory_pool;
    use crate::types::{Family, NodeId};
    use crate::utils::{base64_enc, db_put};
    use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl, SqliteConnection};
    use std::net::{IpAddr, Ipv6Addr};

    /// The identity of the primary node of `ip`'s family
    fn resume(conn: &mut SqliteConnection, ip: IpAddr) -> Result<NodeId, diesel::result::Error> {
        resume_identity(conn, &Scope::primary(Family::of_ip(&ip)), ip)
    }

    #[test]
    fn identity_change_recomputes_buckets() {
        let pool = memory_pool();
        let mut conn = pool.get().unwrap();

        // pretend we were 1.2.3.4 with the all-zero id last session
        db_put("public_ip".to_string(), "1.2.3.4".to_string(), &mut conn).unwrap();
        db_put("id".to_string(), base64_enc([0u8; 20]), &mut conn).unwrap();

        // a node whose bucket was computed against that old identity
        let peer_node = NodeId([0xF0; 20]);
        diesel::insert_into(node_dsl::node)
            .values(crate::models::NodeRow {
                id: peer_node.0.to_vec(),
                family: Family::V4.db(),
                bucket: bucket_index(&NodeId([0; 20]), &peer_node),
                last_contacted: 0,
                ip_addr: "10.0.0.1".to_string(),
                ip_group: None,
                port: 6881,
                failed_requests: 0,
                removed: false,
                bep42: None,
            })
            .execute(&mut *conn)
            .unwrap();

        // our IP changed: a new identity must be adopted and buckets recomputed against it
        let new_ip = IpAddr::from([5, 6, 7, 8]);
        let new_id = resume(&mut conn, new_ip).unwrap();
        assert_ne!(new_id, NodeId([0; 20]), "a new identity must be adopted");
        assert!(bep42_valid(&new_id, new_ip));

        let stored: i32 = node_dsl::node
            .filter(node_dsl::id.eq(peer_node.0.to_vec()))
            .select(node_dsl::bucket)
            .first(&mut *conn)
            .unwrap();
        assert_eq!(stored, bucket_index(&new_id, &peer_node));

        // the new address is remembered with the id, so the next start keeps this identity
        let again = resume(&mut conn, new_ip).unwrap();
        assert_eq!(again, new_id);
    }

    #[test]
    fn each_family_keeps_its_own_identity() {
        let pool = memory_pool();
        let mut conn = pool.get().unwrap();
        let v4 = resume(&mut conn, IpAddr::from([5, 6, 7, 8])).unwrap();

        // not knowing our IPv6 address yet: a random id, kept while that stays so
        let unknown = IpAddr::from(Ipv6Addr::UNSPECIFIED);
        let v6 = resume(&mut conn, unknown).unwrap();
        assert_ne!(v4, v6);
        assert_eq!(resume(&mut conn, unknown).unwrap(), v6);

        // learning it: a BEP 42 id, and the IPv4 identity is untouched
        let known: IpAddr = "2001:470:1:2::5".parse().unwrap();
        let v6_known = resume(&mut conn, known).unwrap();
        assert_ne!(v6_known, v6);
        assert!(bep42_valid(&v6_known, known));
        assert_eq!(resume(&mut conn, IpAddr::from([5, 6, 7, 8])).unwrap(), v4);
    }

    #[test]
    fn what_other_nodes_saw_last_time_is_used_when_the_caller_knows_nothing() {
        let pool = memory_pool();
        let mut conn = pool.get().unwrap();
        assert_eq!(known_external_ip(&mut conn, &Scope::primary(Family::V4)).unwrap(), None);

        db_put(OBSERVED_IP_KEY.to_string(), "5.6.7.8".to_string(), &mut conn).unwrap();
        db_put(
            Scope::primary(Family::V6).key(OBSERVED_IP_KEY),
            "2001:470:1:2::5".to_string(),
            &mut conn,
        )
        .unwrap();
        assert_eq!(
            known_external_ip(&mut conn, &Scope::primary(Family::V4)).unwrap(),
            Some(IpAddr::from([5, 6, 7, 8]))
        );
        assert_eq!(
            known_external_ip(&mut conn, &Scope::primary(Family::V6)).unwrap(),
            Some("2001:470:1:2::5".parse().unwrap())
        );
    }
}

#[cfg(test)]
mod migration_tests {
    use super::*;
    use diesel_migrations::MigrationHarness;

    fn scratch_db(name: &str) -> (std::path::PathBuf, String) {
        let dir = std::env::temp_dir().join(format!("midwest-mainline-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("dht.db").to_str().unwrap().to_string();
        (dir, db)
    }

    #[tokio::test]
    async fn a_fresh_database_file_gets_its_schema() {
        let (dir, db) = scratch_db("fresh");
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let dht = DhtSession::with_stable_id(socket, Some(IpAddr::from([1, 2, 3, 4])), &db).unwrap();
        assert_eq!(dht.node_count(), 0, "the node table exists and is empty");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_ipv4_only_routing_table_becomes_the_ipv4_family() {
        let (dir, db) = scratch_db("upgrade");
        let mut conn = SqliteConnection::establish(&db).unwrap();
        // the schema as it was before IPv6
        for _ in 0..3 {
            conn.run_next_migration(MIGRATIONS).unwrap();
        }
        conn.batch_execute(
            "insert into node (id, bucket, last_contacted, ip_addr, port, failed_requests)
             values (x'0101010101010101010101010101010101010101', 7, 1, '8.8.8.8', 6881, 0)",
        )
        .unwrap();
        conn.run_pending_migrations(MIGRATIONS).unwrap();

        use crate::schema::node::dsl::*;
        let row: (i32, i32, Option<String>) = node.select((family, bucket, ip_group)).first(&mut conn).unwrap();
        assert_eq!(row, (4, 7, Some("8.8.8.8".to_string())));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[cfg(test)]
mod ipv6_tests {
    use super::*;
    use crate::test_support::{eventually, node, scratch_dir};

    const V6_LOOPBACK: SocketAddr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
    const V4_LOOPBACK: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    #[tokio::test(flavor = "multi_thread")]
    async fn a_lookup_and_an_announce_over_ipv6() {
        let dir = scratch_dir("v6-lookup");
        let b = node(&dir, "b", V6_LOOPBACK).await;
        let a = node(&dir, "a", V6_LOOPBACK).await;
        let c = node(&dir, "c", V6_LOOPBACK).await;
        assert_eq!(a.session.family(), Family::V6);

        let b_addr = b.session.local_addr();
        a.session.bootstrap(vec![b_addr]).await.unwrap();
        assert_eq!(a.session.node_count(), 1, "A knows B");

        // A looks the hash up at B, gets a token, and announces itself there
        let info_hash = InfoHash([0x42; 20]);
        let found = a.session.get_peers(info_hash).await;
        assert!(found.peers.is_empty());
        let (_, token) = found
            .announce_candidates
            .into_iter()
            .find(|(n, _)| n.end_point() == b_addr)
            .expect("B hands out a token");
        a.session
            .handle()
            .announce_peers(b_addr, info_hash, Some(1234), token, false)
            .await
            .unwrap();

        // C, knowing only B, finds A's announce: an 18-byte IPv6 value
        c.session.bootstrap(vec![b_addr]).await.unwrap();
        let found = c.session.get_peers(info_hash).await;
        assert_eq!(found.peers, vec![SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 1234)]);
        // B learned of both, in its IPv6 table
        assert!(eventually(|| b.session.node_count() == 2).await);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_paired_ipv4_node_seeds_the_ipv6_table() {
        let dir = scratch_dir("v6-seed");
        // a dual-stack remote: its IPv6 table knows one node, X
        let remote4 = node(&dir, "remote", V4_LOOPBACK).await;
        let remote6 = node(&dir, "remote", V6_LOOPBACK).await;
        remote4.session.pair_with(&remote6.session);
        let x = node(&dir, "x", V6_LOOPBACK).await;
        x.session.bootstrap(vec![remote6.session.local_addr()]).await.unwrap();
        assert!(eventually(|| remote6.session.node_count() == 1).await);

        // and us, dual-stack too, bootstrapping over IPv4 only
        let us4 = node(&dir, "us", V4_LOOPBACK).await;
        let us6 = node(&dir, "us", V6_LOOPBACK).await;
        us4.session.pair_with(&us6.session);
        us4.session.bootstrap(vec![remote4.session.local_addr()]).await.unwrap();

        // our IPv4 lookups asked for nodes6 too, and they went to the IPv6 table
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while us6.session.node_count() == 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let x_id = x.session.handle().our_id();
        let seeded = us6
            .session
            .routing_table
            .find_exact(&x_id)
            .expect("X came in as nodes6");
        assert_eq!(seeded.end_point(), x.session.local_addr());
        assert_eq!(us4.session.routing_table.find_exact(&x_id), None);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::message::Krpc;
    use crate::message::get_peers_query::GetPeersQuery;
    use crate::message::ping_query::PingQuery;
    use crate::test_support::{node, scratch_dir};
    use crate::types::TransactionId;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_ping_is_answered_while_store_queries_wait_on_the_database() {
        let dir = scratch_dir("flood");
        let us = node(&dir, "us", SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await;
        // every connection of the pool taken: whatever needs the store waits
        let held: Vec<_> = (0..us.session.state.conn.max_size())
            .map(|_| us.session.state.conn.get().unwrap())
            .collect();
        let them = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let to = us.session.local_addr();
        for i in 0..600u16 {
            let query = KrpcBody::GetPeersQuery(GetPeersQuery::new(NodeId([1; 20]), InfoHash([2; 20])));
            let packet = Krpc::new(TransactionId::from_bytes(&i.to_be_bytes()), query).encode();
            them.send_to(&packet, to).await.unwrap();
        }
        let ping = Krpc::new(
            TransactionId::from_bytes(b"pi"),
            KrpcBody::PingQuery(PingQuery::new(NodeId([1; 20]))),
        );
        // sent a few times: a flood this size can overrun the socket's buffer
        let mut buf = [0u8; 1500];
        let mut answered = false;
        for _ in 0..10 {
            them.send_to(&ping.encode(), to).await.unwrap();
            let heard = tokio::time::timeout(Duration::from_millis(200), async {
                loop {
                    let (n, _) = them.recv_from(&mut buf).await.unwrap();
                    if Krpc::decode(&buf[..n]).is_ok_and(|msg| msg.txn_id == ping.txn_id) {
                        break;
                    }
                }
            });
            if heard.await.is_ok() {
                answered = true;
                break;
            }
        }
        assert!(answered, "the ping waited behind the flood");
        drop(held);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stopped_node_lets_go_of_its_port() {
        let dir = scratch_dir("stop");
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = socket.local_addr().unwrap();
        let db = dir.join("dht.db");
        let session = Arc::new(DhtSession::with_stable_id(socket, None, db.to_str().unwrap()).unwrap());
        let run = tokio::spawn({
            let session = session.clone();
            async move { session.run().await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        run.abort();
        let _ = run.await;
        drop(session);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let rebound = loop {
            match UdpSocket::bind(addr).await {
                Ok(socket) => break Ok(socket),
                Err(e) if tokio::time::Instant::now() > deadline => break Err(e),
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };
        assert!(rebound.is_ok(), "the port is still bound: {rebound:?}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[cfg(test)]
mod spoofing_tests {
    use super::*;
    use crate::message::Krpc;
    use crate::message::find_node_get_peers_response::Builder as ResBuilder;
    use crate::test_support::{node, scratch_dir};
    use crate::types::TransactionId;

    const LOOPBACK: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    #[tokio::test(flavor = "multi_thread")]
    async fn an_answer_to_nothing_we_asked_teaches_the_table_nothing() {
        let dir = scratch_dir("unsolicited");
        let us = node(&dir, "us", LOOPBACK).await;
        let liar = UdpSocket::bind(LOOPBACK).await.unwrap();
        let mentioned = NodeInfo::new(NodeId([0x77; 20]), SocketAddr::from((Ipv4Addr::LOCALHOST, 9)));
        let body = KrpcBody::FindNodeGetPeersResponse(ResBuilder::new(NodeId([0x66; 20])).with_node(mentioned).build());
        let packet = Krpc::new(TransactionId::from_bytes(b"zz"), body).encode();
        liar.send_to(&packet, us.session.local_addr()).await.unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(us.session.node_count(), 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[cfg(test)]
mod bep51_tests {
    use super::*;
    use crate::test_support::{node, scratch_dir};
    use std::sync::atomic::Ordering::Relaxed;

    const LOOPBACK: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    fn store(session: &DhtSession, info_hash: InfoHash) {
        let peer = "10.0.0.1:6881".parse().unwrap();
        session.state.store_peer(&info_hash, peer, false).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_node_answers_with_a_sample_of_what_it_stores() {
        let dir = scratch_dir("bep51-answer");
        let a = node(&dir, "a", LOOPBACK).await;
        let b = node(&dir, "b", LOOPBACK).await;
        let stored = [InfoHash([1; 20]), InfoHash([2; 20])];
        for hash in stored {
            store(&a.session, hash);
        }
        b.session.bootstrap(vec![a.session.local_addr()]).await.unwrap();

        let sampled = b
            .session
            .handle()
            .sample_infohashes(a.session.local_addr(), NodeId([0x55; 20]))
            .await
            .unwrap();
        assert_eq!(sampled.num, 2);
        let mut samples = sampled.samples.clone();
        samples.sort_by_key(|h| h.0);
        assert_eq!(samples, stored);
        assert!(sampled.interval <= state::SAMPLE_INTERVAL && sampled.interval > Duration::ZERO);
        assert_eq!(sampled.node.id(), a.session.handle().our_id());
        assert_eq!(sampled.nodes.len(), 1, "A knows B, and says so");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_crawler_walks_from_node_to_node_and_keeps_what_they_sample() {
        let dir = scratch_dir("bep51-crawl");
        // A knows C, B knows only A: the crawler on B must reach C through A's `nodes`
        let a = node(&dir, "a", LOOPBACK).await;
        let c = node(&dir, "c", LOOPBACK).await;
        store(&a.session, InfoHash([1; 20]));
        store(&c.session, InfoHash([1; 20]));
        store(&c.session, InfoHash([3; 20]));
        c.session.bootstrap(vec![a.session.local_addr()]).await.unwrap();
        let b = node(&dir, "b", LOOPBACK).await;
        b.session.bootstrap(vec![a.session.local_addr()]).await.unwrap();
        b.session.routing_table.evict(&c.session.handle().our_id());

        let crawler = b.session.crawler(50);
        let crawl = {
            let crawler = crawler.clone();
            tokio::spawn(async move { crawler.run().await })
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while b.session.sampled_count() < 2 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(b.session.sampled_count(), 2);
        // and politely: each node once, however fast the crawler may go
        tokio::time::sleep(Duration::from_millis(200)).await;
        crawl.abort();
        assert_eq!(crawler.stats().answered.load(Relaxed), 2);
        assert_eq!(crawler.stats().new_info_hashes.load(Relaxed), 2);
        assert_eq!(crawler.stats().samples.load(Relaxed), 3);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[cfg(test)]
mod bep33_tests {
    use super::*;
    use crate::test_support::{node, scratch_dir};

    const LOOPBACK: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    fn store(session: &DhtSession, info_hash: InfoHash, ips: std::ops::Range<u8>, seed: bool) {
        for i in ips {
            let addr = SocketAddr::from(([10, 0, 0, i], 6881));
            session.state.store_peer(&info_hash, addr, seed).unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_scrape_counts_seeds_and_peers_across_nodes_once_each() {
        let dir = scratch_dir("bep33");
        let hash = InfoHash([0x33; 20]);
        // A holds seeds .0-.29 and peers .100-.119; B the seeds .20-.39 and no peers
        let a = node(&dir, "a", LOOPBACK).await;
        let b = node(&dir, "b", LOOPBACK).await;
        store(&a.session, hash, 0..30, true);
        store(&a.session, hash, 100..120, false);
        store(&b.session, hash, 20..40, true);
        b.session.bootstrap(vec![a.session.local_addr()]).await.unwrap();
        let us = node(&dir, "us", LOOPBACK).await;
        us.session
            .bootstrap(vec![a.session.local_addr(), b.session.local_addr()])
            .await
            .unwrap();

        let estimate = us.session.handle().scrape(hash).await;
        assert_eq!(estimate.nodes, 2);
        assert!(estimate.seeds.abs_diff(40) <= 2, "{estimate:?}");
        assert!(estimate.peers.abs_diff(20) <= 1, "{estimate:?}");

        // a hash nobody holds: nothing, and no filters
        let none = us.session.handle().scrape(InfoHash([0x44; 20])).await;
        assert_eq!((none.seeds, none.peers, none.nodes), (0, 0, 0));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn noseed_puts_the_peers_that_are_not_seeds_first() {
        let pool = crate::test_support::memory_pool();
        let socket = UdpSocket::bind(LOOPBACK).await.unwrap();
        let rpc = RpcManager::new(socket, pool.clone(), Arc::new(TxnIdGenerator::new()), None);
        let id = NodeId([7; 20]);
        let state = SharedState::new(id, RoutingTable::new(id, rpc.clone(), pool.clone()), rpc, pool);
        let hash = InfoHash([1; 20]);
        // the seed is the latest announce, so it comes first unless asked otherwise
        store_in(&state, hash, 2, false);
        store_in(&state, hash, 1, true);
        let first = |noseed| state.swarm_peers_preferring(&hash, Family::V4, noseed)[0];
        assert_eq!(first(false), SocketAddr::from(([10, 0, 0, 1], 6881)));
        assert_eq!(first(true), SocketAddr::from(([10, 0, 0, 2], 6881)));
    }

    fn store_in(state: &SharedState, info_hash: InfoHash, i: u8, seed: bool) {
        let addr = SocketAddr::from(([10, 0, 0, i], 6881));
        state.store_peer(&info_hash, addr, seed).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}

#[cfg(test)]
mod bep45_tests {
    use super::*;
    use crate::test_support::{eventually, node, node_of, scratch_dir};

    const LOOPBACK: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    async fn own_address(db: &std::path::Path) -> DhtSession {
        let socket = UdpSocket::bind(LOOPBACK).await.unwrap();
        DhtSession::on_own_address(socket, None, db.to_str().unwrap()).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_second_address_is_a_node_of_its_own_sharing_the_store() {
        let dir = scratch_dir("bep45");
        let db = dir.join("us.db");
        let x = node(&dir, "x", LOOPBACK).await;
        let primary = node(&dir, "us", LOOPBACK).await;
        let second = node_of(Arc::new(own_address(&db).await));
        let (primary_id, second_id) = (primary.session.handle().our_id(), second.session.handle().our_id());
        assert_ne!(primary_id, second_id, "BEP 45: a node id per address");

        // one routing table each, in the one database
        primary.session.bootstrap(vec![x.session.local_addr()]).await.unwrap();
        assert_eq!(primary.session.node_count(), 1);
        assert_eq!(second.session.node_count(), 0);
        second.session.bootstrap(vec![x.session.local_addr()]).await.unwrap();
        // X, and the primary X told it of: to the second, just another node
        assert!(eventually(|| second.session.node_count() == 2).await);
        assert!(second.session.routing_table.contains(&primary_id));
        assert!(
            eventually(|| x.session.node_count() == 2).await,
            "the network sees two nodes"
        );

        // a token is the socket's that issued it
        let hash = InfoHash([0x45; 20]);
        let x_client = x.session.handle();
        let found = x_client.get_peers(hash).await;
        let (_, primarys_token) = found
            .announce_candidates
            .iter()
            .find(|(n, _)| n.id() == primary_id)
            .cloned()
            .expect("the primary hands out a token");
        let refused = x_client
            .announce_peers(
                second.session.local_addr(),
                hash,
                Some(1),
                primarys_token.clone(),
                false,
            )
            .await;
        assert!(refused.is_err());
        x_client
            .announce_peers(primary.session.local_addr(), hash, Some(1), primarys_token, false)
            .await
            .unwrap();
        // and what's announced to one is stored for both
        assert_eq!(second.session.stored_peers(&hash).len(), 1);

        // the address keeps its id across starts
        drop(second);
        assert_eq!(own_address(&db).await.handle().our_id(), second_id);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[cfg(test)]
mod bep43_tests {
    use super::*;
    use crate::message::ping_query::PingQuery;
    use crate::test_support::{node, node_of, scratch_dir};

    const LOOPBACK: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    #[tokio::test(flavor = "multi_thread")]
    async fn a_read_only_node_asks_but_neither_answers_nor_gets_kept() {
        let dir = scratch_dir("bep43");
        let a = node(&dir, "a", LOOPBACK).await;
        let socket = UdpSocket::bind(LOOPBACK).await.unwrap();
        let db = dir.join("ro.db");
        let ro = DhtSession::with_stable_id(socket, None, db.to_str().unwrap())
            .unwrap()
            .with_read_only(true);
        let ro = node_of(Arc::new(ro));

        // its lookups work, and A serves them, but leaves it out of its table
        ro.session.bootstrap(vec![a.session.local_addr()]).await.unwrap();
        assert_eq!(ro.session.node_count(), 1);
        let found = ro.session.get_peers(InfoHash([1; 20])).await;
        assert_eq!(found.announce_candidates.len(), 1, "A answered with a token");
        assert_eq!(a.session.node_count(), 0);

        // and it answers nothing
        let ping = KrpcBody::PingQuery(PingQuery::new(a.session.handle().our_id()));
        let answer = a
            .session
            .rpc_manager
            .query(ping, ro.session.local_addr(), Duration::from_millis(300))
            .await;
        assert!(answer.is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[cfg(test)]
mod bep44_tests {
    use super::*;
    use crate::message::item_queries::{PutQuery, Signed};
    use crate::test_support::{Node, node, scratch_dir};
    use item::SigningKey;

    const LOOPBACK: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    /// Three storing nodes that know each other, and two clients that know only the first
    async fn network(dir: &std::path::Path) -> (Vec<Node>, Node, Node) {
        let mut stores = vec![];
        for name in ["a", "b", "c"] {
            stores.push(node(dir, name, LOOPBACK).await);
        }
        let first = stores[0].session.local_addr();
        for store in &stores[1..] {
            store.session.bootstrap(vec![first]).await.unwrap();
        }
        let writer = node(dir, "writer", LOOPBACK).await;
        let reader = node(dir, "reader", LOOPBACK).await;
        writer.session.bootstrap(vec![first]).await.unwrap();
        reader.session.bootstrap(vec![first]).await.unwrap();
        (stores, writer, reader)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_immutable_item_put_by_one_is_got_by_another() {
        let dir = scratch_dir("bep44-immutable");
        let (_stores, writer, reader) = network(&dir).await;
        let value = b"d4:name5:hello4:sizei42ee".to_vec();
        let put = writer.session.handle().put_immutable(value.clone()).await.unwrap();
        assert_eq!(put.target, item::immutable_target(&value));
        assert!(put.stored >= 3, "{put:?}");

        assert_eq!(reader.session.handle().get_immutable(put.target).await, Some(value));
        assert_eq!(reader.session.handle().get_immutable(NodeId([9; 20])).await, None);

        // not one canonical value, or too big: refused before anything is sent
        let handle = writer.session.handle();
        assert!(handle.put_immutable(b"d1:bi1e1:ai2ee".to_vec()).await.is_err());
        let big = format!("1000:{}", "x".repeat(1000)).into_bytes();
        assert!(handle.put_immutable(big).await.is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_mutable_item_is_got_at_its_newest() {
        let dir = scratch_dir("bep44-mutable");
        let (_stores, writer, reader) = network(&dir).await;
        let key = SigningKey::from_bytes(&[5; 32]);
        let public = key.verifying_key().to_bytes();
        let handle = writer.session.handle();
        handle
            .put_mutable(&key, b"salt", 1, b"5:first".to_vec(), None)
            .await
            .unwrap();
        let put = handle
            .put_mutable(&key, b"salt", 2, b"6:second".to_vec(), Some(1))
            .await
            .unwrap();
        assert!(put.stored >= 3, "{put:?}");

        let got = reader
            .session
            .handle()
            .get_mutable(public, b"salt", None)
            .await
            .unwrap();
        assert_eq!((got.seq, got.value.as_slice()), (2, b"6:second".as_slice()));
        assert_eq!(
            reader.session.handle().get_mutable(public, b"salt", Some(2)).await,
            None
        );
        assert_eq!(reader.session.handle().get_mutable(public, b"other", None).await, None);

        // an old seq, or a cas that no longer holds, is refused everywhere
        let stale = handle
            .put_mutable(&key, b"salt", 1, b"5:first".to_vec(), None)
            .await
            .unwrap();
        assert_eq!(stale.stored, 0);
        let lost_race = handle
            .put_mutable(&key, b"salt", 3, b"5:third".to_vec(), Some(1))
            .await
            .unwrap();
        assert_eq!(lost_race.stored, 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anyone_can_put_a_signed_item_again() {
        let dir = scratch_dir("bep44-republish");
        let (stores, writer, reader) = network(&dir).await;
        let key = SigningKey::from_bytes(&[8; 32]);
        writer
            .session
            .handle()
            .put_mutable(&key, b"", 4, b"3:abc".to_vec(), None)
            .await
            .unwrap();
        let found = reader
            .session
            .handle()
            .get_mutable(key.verifying_key().to_bytes(), b"", None)
            .await
            .unwrap();

        // a store the writer never reached takes it from the reader
        let late = node(&dir, "late", LOOPBACK).await;
        late.session
            .bootstrap(vec![stores[0].session.local_addr()])
            .await
            .unwrap();
        reader.session.bootstrap(vec![late.session.local_addr()]).await.unwrap();
        let put = reader.session.handle().put_signed(&found).await.unwrap();
        assert!(put.stored >= 4, "{put:?}");
        let target = item::mutable_target(&found.key, b"");
        assert!(late.session.state.stored_item(&target, None).is_some());

        let mut forged = found.clone();
        forged.value = b"3:xyz".to_vec();
        assert_eq!(reader.session.handle().put_signed(&forged).await.unwrap().stored, 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_put_that_breaks_the_rules_gets_bep_44s_error() {
        let dir = scratch_dir("bep44-errors");
        let store = node(&dir, "store", LOOPBACK).await;
        let us = node(&dir, "us", LOOPBACK).await;
        us.session.bootstrap(vec![store.session.local_addr()]).await.unwrap();
        let found = us.session.get_peers(InfoHash([1; 20])).await;
        let (_, token) = found.announce_candidates[0].clone();
        let key = SigningKey::from_bytes(&[6; 32]);
        let signed = |salt: &[u8], seq, value: &[u8]| Signed {
            key: key.verifying_key().to_bytes(),
            salt: salt.to_vec(),
            seq,
            sig: item::sign(&key, salt, seq, value),
        };
        let put = |value: &[u8], signed: Option<Signed>| {
            KrpcBody::PutQuery(PutQuery::new(
                us.session.handle().our_id(),
                token.clone(),
                value.to_vec(),
                signed,
            ))
        };
        let code = async |body: KrpcBody| match us
            .session
            .rpc_manager
            .query(body, store.session.local_addr(), Duration::from_secs(1))
            .await
        {
            Err(OurError::Remote(e)) => Some(e),
            Ok(_) => None,
            Err(e) => panic!("{e}"),
        };

        let big = format!("1001:{}", "x".repeat(1001)).into_bytes();
        assert_eq!(code(put(&big, None)).await.map(|e| e.code()), Some(205));
        let mut forged = signed(b"", 1, b"i1e");
        forged.seq = 2;
        assert_eq!(code(put(b"i1e", Some(forged))).await.map(|e| e.code()), Some(206));
        let salt = [0u8; 65];
        assert_eq!(
            code(put(b"i1e", Some(signed(&salt, 1, b"i1e"))))
                .await
                .map(|e| e.code()),
            Some(207)
        );
        assert_eq!(code(put(b"i1e", Some(signed(b"", 5, b"i1e")))).await, None);
        // the same seq again: with the same value it's a refresh, with another it's refused
        assert_eq!(code(put(b"i1e", Some(signed(b"", 5, b"i1e")))).await, None);
        assert_eq!(
            code(put(b"i9e", Some(signed(b"", 5, b"i9e")))).await.map(|e| e.code()),
            Some(302)
        );
        assert_eq!(
            code(put(b"i2e", Some(signed(b"", 4, b"i2e")))).await.map(|e| e.code()),
            Some(302)
        );
        let cas = KrpcBody::PutQuery(
            PutQuery::new(
                us.session.handle().our_id(),
                token.clone(),
                b"i3e".to_vec(),
                Some(signed(b"", 6, b"i3e")),
            )
            .with_cas(Some(4)),
        );
        assert_eq!(code(cas).await.map(|e| e.code()), Some(301));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
