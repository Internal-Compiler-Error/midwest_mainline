//! The k-bucket contact store, persisted in SQLite so contacts survive restarts.
//!
//! The table learns passively: `run` consumes the broker's inbound message fan-out and
//! records every sender we hear from. Liveness is tracked with a `failed_requests`
//! counter — 3+ failures and 15 minutes unheard from land a node on the replacement queue,
//! and a failed refresh ping tombstones it (`removed`). Tombstones are purged on the
//! periodic `refresh_table` tick.
//!
//! IPv4 and IPv6 nodes share the `node` table, told apart by its `family` column: each
//! [`RoutingTable`] sees only the rows of its own family (BEP 32's separate tables).

use std::collections::HashSet;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use diesel::r2d2::PooledConnection;
use diesel::{
    ExpressionMethods, SqliteConnection,
    r2d2::{ConnectionManager, Pool},
};
use diesel::{insert_into, prelude::*};
use futures::future::join_all;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::dht::bep42::compliant;
use crate::dht::xor;
use crate::message::Krpc;
use crate::message::ping_query::PingQuery;
use crate::models::NodeNoMetaInfo;
use crate::utils::unix_timestmap_ms;
use crate::{
    message::KrpcBody,
    types::{self, Family, NodeId, NodeInfo},
};

use super::rpc_manager::RpcManager;
use super::state::REQ_TIMEOUT;

/// What the one-node-per-address rule (see `RoutingTable::ip_taken`) groups `ip` under: the
/// address itself for IPv4, its /64 for IPv6, since one IPv6 host typically holds a whole /64
/// and can pick any address in it. `None` where the rule doesn't apply: a LAN (private,
/// loopback, link local, unique local) can hold many real nodes.
pub(crate) fn sybil_group(ip: &IpAddr) -> Option<IpAddr> {
    match ip {
        IpAddr::V4(v4) => {
            let exempt = v4.is_private() || v4.is_loopback() || v4.is_link_local() || v4.is_unspecified();
            (!exempt).then_some(*ip)
        }
        IpAddr::V6(v6) => {
            let exempt = v6.is_loopback() || v6.is_unspecified() || v6.is_unique_local() || v6.is_unicast_link_local();
            let prefix = u128::from(*v6) & !(u128::MAX >> 64);
            (!exempt).then_some(IpAddr::V6(Ipv6Addr::from(prefix)))
        }
    }
}

/// Applies the one-node-per-address rule to a table saved before it existed (or by a version
/// that didn't enforce it): of the nodes sharing an address group, the one heard from last
/// stays.
pub(crate) fn purge_shared_ips(conn: &mut SqliteConnection) -> Result<usize, diesel::result::Error> {
    use crate::schema::node::dsl::*;
    let mut rows: Vec<(Vec<u8>, i32, String, i64)> = node
        .filter(removed.eq(false))
        .select((id, family, ip_addr, last_contacted))
        .load(conn)?;
    rows.sort_by_key(|row| std::cmp::Reverse(row.3));
    let mut groups = HashSet::new();
    let mut doomed: Vec<(Vec<u8>, i32)> = vec![];
    for (node_id, fam, ip, _) in rows {
        let Some(group) = ip.parse::<IpAddr>().ok().and_then(|ip| sybil_group(&ip)) else {
            continue;
        };
        // per table: nodes of another address's table (BEP 45) don't compete
        if !groups.insert((fam, group)) {
            doomed.push((node_id, fam));
        }
    }
    let mut purged = 0;
    for (node_id, fam) in doomed {
        purged += diesel::delete(node.filter(id.eq(node_id)).filter(family.eq(fam))).execute(conn)?;
    }
    Ok(purged)
}

/// Works out BEP 42 compliance for rows saved before it was recorded.
pub(crate) fn fill_bep42(conn: &mut SqliteConnection) -> Result<usize, diesel::result::Error> {
    use crate::schema::node::dsl::*;
    let rows: Vec<(Vec<u8>, i32, String)> = node.filter(bep42.is_null()).select((id, family, ip_addr)).load(conn)?;
    conn.transaction(|conn| {
        for (node_id, fam, ip) in &rows {
            let (Some(nid), Ok(ip)) = (NodeId::try_from_bytes(node_id), ip.parse::<IpAddr>()) else {
                continue;
            };
            diesel::update(node.filter(id.eq(node_id)).filter(family.eq(fam)))
                .set(bep42.eq(compliant(&nid, ip)))
                .execute(conn)?;
        }
        Ok(rows.len())
    })
}

/// Which of the 160 buckets `target` falls into, relative to `our_id`.
pub(crate) fn bucket_index(our_id: &NodeId, target: &NodeId) -> i32 {
    let dist = our_id.dist(target);
    if dist == types::ZERO_DIST {
        // all zero means we're finding ourself, then we go look for in the last bucket
        return 159;
    }

    let first_nonzero_byte = dist.into_iter().position(|radix| radix != 0).unwrap();
    let byte = dist[first_nonzero_byte];

    let bucket_idx = 159 - (first_nonzero_byte * 8 + byte.leading_zeros() as usize);
    bucket_idx.try_into().unwrap()
}

const REFRESH_EVERY: Duration = Duration::from_secs(180);
/// BEP 5's questionable node: not heard from in this long
const QUESTIONABLE_AFTER: Duration = Duration::from_secs(15 * 60);
/// Failed queries that make a questionable node worth a refresh ping
const FAILURES_TO_REFRESH: i32 = 3;

#[derive(Debug, Clone)]
/// A RoutingTable will tell you who are the closest nodes that we know
pub struct RoutingTable {
    id: NodeId,
    family: Family,
    /// the `node` rows that are this table's: its family's, or with BEP 45, its address's
    scope_table: i32,
    table: Pool<ConnectionManager<SqliteConnection>>,
    rpc_manager: RpcManager,
    /// NOTE(deviation): BEP 5 specifies k = 8 per bucket with split-when-covers-self.
    /// We keep 160 flat buckets of 1024 and let `find_closest` order the whole table by
    /// distance; eviction of dead nodes (failed_requests >= 3 → refresh → mark_as_dead) keeps
    /// the table fresh. Revisit if the table ever outgrows this.
    bucket_capacity: usize,
    /// buckets with a refresh under way, see `add`
    refreshing: Arc<Mutex<HashSet<i32>>>,
}

impl RoutingTable {
    /// The table holds the nodes of the broker's address family, in the broker's scope.
    pub fn new(id: NodeId, rpc_manager: RpcManager, table: Pool<ConnectionManager<SqliteConnection>>) -> RoutingTable {
        RoutingTable {
            id,
            family: rpc_manager.family(),
            scope_table: rpc_manager.scope().table,
            table,
            rpc_manager,
            bucket_capacity: 1024, // TODO: make this configurable in the future
            refreshing: Arc::default(),
        }
    }

    pub fn family(&self) -> Family {
        self.family
    }

    fn add_new_nodes(&self, from: SocketAddr, message: &Krpc) {
        // errors carry no id
        let Some(node_id) = message.node_id() else {
            return;
        };
        // BEP 43: a read-only node wouldn't answer us; its query is served, it isn't kept
        if message.is_query() && message.read_only {
            return;
        }

        {
            let mut conn = self.conn();
            self.marks_as_good(&node_id, &mut conn);
        }
        self.add(node_id, from);

        // if it's response from find_peers or get_nodes, they have additional info; the other
        // family's nodes are the session's business (see `DhtSession::pair_with`)
        if let KrpcBody::FindNodeGetPeersResponse(res) = &message.body {
            for node in res.nodes_of(self.family) {
                self.add(node.id(), node.end_point());
            }
        }
    }

    /// keep listening for all incoming responses and update our table; the inbox is the
    /// caller's subscription to the broker's inbound queue
    pub async fn run(&self, mut inbound: mpsc::Receiver<(Krpc, SocketAddr)>) {
        // the refresh runs beside the inbox, which it would otherwise hold up for as long as
        // it takes
        let mut refresh = tokio::time::interval_at(tokio::time::Instant::now() + REFRESH_EVERY, REFRESH_EVERY);
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut refreshing: Option<tokio::task::JoinHandle<()>> = None;
        loop {
            tokio::select! {
                maybe_msg = inbound.recv() => {
                    if let Some((msg, origin)) = maybe_msg {
                        self.add_new_nodes(origin, &msg);
                    } else {
                        // Channel closed, exit loop
                        break;
                    }
                }
                _ = refresh.tick() => {
                    if refreshing.as_ref().is_none_or(|task| task.is_finished()) {
                        let this = self.clone();
                        refreshing = Some(tokio::spawn(async move { this.refresh_table().await }));
                    }
                }
            }
        }
    }

    /// Returns the bucket index that the target node belongs in
    fn index(&self, target: &NodeId) -> i32 {
        bucket_index(&self.id, target)
    }

    /// The k (8) nodes we know closest to `target`, closest first
    pub fn find_closest(&self, target: NodeId) -> Vec<NodeInfo> {
        self.find_closest_n(target, 8)
    }

    /// The `total` nodes we know closest to `target`, closest first.
    pub fn find_closest_n(&self, target: NodeId, total: u16) -> Vec<NodeInfo> {
        use crate::schema::node::dsl::*;
        node.select(NodeNoMetaInfo::as_select())
            .filter(family.eq(self.scope_table))
            .filter(removed.eq(false))
            .order(xor(id, target.as_bytes()))
            .limit(total.into())
            .load(&mut self.conn())
            .inspect_err(|e| error!("couldn't read the routing table: {e}"))
            .unwrap_or_default()
            .into_iter()
            .map(NodeInfo::from)
            .collect()
    }

    pub fn find_exact(&self, target: &NodeId) -> Option<NodeInfo> {
        use crate::schema::node::dsl::*;
        let mut conn = self.table.get().unwrap();
        let target_id = target.0.to_vec();
        node.filter(family.eq(self.scope_table))
            .filter(removed.eq(false))
            .filter(id.eq(target_id))
            .select(NodeNoMetaInfo::as_select())
            .first(&mut conn)
            .ok()
            .map(|nodee| nodee.into())
    }

    pub fn contains(&self, target: &NodeId) -> bool {
        self.find_exact(target).is_some()
    }

    /// Whether the ith bucket is at capacity. Adds race (a refresh's insert against the inbox's),
    /// so a bucket can end up a node or two over; that's harmless.
    pub fn full_bucket(&self, i: i32) -> bool {
        self.bucket_size(i) >= self.bucket_capacity
    }

    /// Add a new node to the routing table, if the buckets are full, the node will be ignored.
    /// So is a node of the other address family.
    #[tracing::instrument(skip(self))]
    pub fn add(&self, new_node_id: NodeId, addr: SocketAddr) {
        if Family::of(&addr) != self.family {
            debug!("{addr} is not an {} address, skipping", self.family);
            return;
        }
        if new_node_id == self.id {
            return;
        }
        // TODO: contains will request another connection from the pool..., should be fine
        // for now
        if self.contains(&new_node_id) {
            debug!("Already contains this node in routing table, skipping");
            return;
        }
        if self.ip_taken(&addr.ip()) {
            debug!("{addr} already has a node in the routing table, skipping {new_node_id:?}");
            return;
        }

        let bucket_idx = self.index(&new_node_id);
        if !self.full_bucket(bucket_idx) {
            debug!("Bucket {bucket_idx} has capacity, inserting");
            let node = NodeInfo::new(new_node_id, addr);
            let mut conn = self.conn();
            self.put_to_bucket(node, &mut conn);
            return;
        }

        // BEP 42: a compliant node takes the place of a non-compliant one, the one heard from
        // least recently
        if compliant(&new_node_id, addr.ip())
            && let Some(victim) = self.least_recent_noncompliant(bucket_idx)
        {
            debug!("Bucket {bucket_idx} full, {new_node_id:?} replaces a non-BEP 42 node");
            let mut conn = self.conn();
            self.mark_as_dead(&victim, &mut conn);
            self.put_to_bucket(NodeInfo::new(new_node_id, addr), &mut conn);
            return;
        }

        // one refresh of a bucket at a time; a node turning up meanwhile is let go
        if !self.refreshing.lock().unwrap().insert(bucket_idx) {
            debug!("Bucket {bucket_idx} full and being refreshed, skipping {new_node_id:?}");
            return;
        }
        info!("Bucket {bucket_idx} full, refreshing it to evict");
        let this = self.clone();
        let work = async move {
            let node = NodeInfo::new(new_node_id, addr);

            // instead of going from least recently seen and probe one by one, just refresh the
            // entire bucket
            this.refresh_bucket(bucket_idx).await;
            this.refreshing.lock().unwrap().remove(&bucket_idx);

            if this.full_bucket(bucket_idx) {
                info!("Bucket {bucket_idx} remains full after refreshing, node not added");
                return;
            }

            info!("Bucket {bucket_idx} now has spare capacity, adding");
            let mut conn = this.conn();
            this.put_to_bucket(node, &mut conn);
        };
        tokio::spawn(work);
    }

    /// One node per public IPv4 address or IPv6 /64, as libtorrent does: someone running a
    /// thousand node ids from one machine (a Sybil parked next to popular info hashes,
    /// typically) gets one slot, not a thousand. LAN addresses are exempt, see `sybil_group`.
    fn ip_taken(&self, ip: &IpAddr) -> bool {
        use crate::schema::node::dsl::*;
        let Some(group) = sybil_group(ip) else {
            return false;
        };
        let mut conn = self.conn();
        node.filter(family.eq(self.scope_table))
            .filter(ip_group.eq(group.to_string()))
            .filter(removed.eq(false))
            .count()
            .get_result::<i64>(&mut conn)
            .is_ok_and(|n| n > 0)
    }

    fn least_recent_noncompliant(&self, i: i32) -> Option<NodeId> {
        use crate::schema::node::dsl::*;
        let mut conn = self.conn();
        node.filter(family.eq(self.scope_table))
            .filter(removed.eq(false))
            .filter(bucket.eq(i))
            .filter(bep42.eq(false))
            .order(last_contacted.asc())
            .select(id)
            .first::<Vec<u8>>(&mut conn)
            .ok()
            .and_then(|raw| NodeId::try_from_bytes(&raw))
    }

    /// How many nodes in the table have BEP 42 compliant ids (LAN ones count as compliant),
    /// and how many nodes there are
    pub fn bep42_compliance(&self) -> (usize, usize) {
        use crate::schema::node::dsl::*;
        let mut conn = self.conn();
        let alive = node.filter(family.eq(self.scope_table)).filter(removed.eq(false));
        let good: i64 = alive
            .filter(bep42.eq(true))
            .count()
            .get_result(&mut conn)
            .unwrap_or_default();
        let all: i64 = alive.count().get_result(&mut conn).unwrap_or_default();
        (good as usize, all as usize)
    }

    fn conn(&self) -> PooledConnection<ConnectionManager<SqliteConnection>> {
        self.table.get().expect("Pool should just work")
    }

    pub fn node_count(&self) -> usize {
        use crate::schema::node::dsl::*;
        let mut conn = self.conn();
        let count: i64 = node
            .filter(family.eq(self.scope_table))
            .filter(removed.eq(false))
            .count()
            .get_result(&mut conn)
            .unwrap();
        count as usize
    }

    /// How many nodes are in the ith bucket (0-indexed)
    pub fn bucket_size(&self, i: i32) -> usize {
        use crate::schema::node::dsl::*;
        let mut conn = self.conn();
        let count: i64 = node
            .filter(family.eq(self.scope_table))
            .filter(removed.eq(false))
            .filter(bucket.eq(i))
            .count()
            .get_result(&mut conn)
            .unwrap();
        count as usize
    }

    /// The questionable nodes of the ith bucket (BEP 5: not heard from in 15 minutes), the ones
    /// that failed a few queries since: a refresh pings them, and drops those that don't answer
    fn replacement_queue(&self, i: i32, conn: &mut SqliteConnection) -> Vec<crate::models::NodeRow> {
        use crate::schema::node::dsl::*;
        let questionable = unix_timestmap_ms() - QUESTIONABLE_AFTER.as_millis() as i64;
        node.filter(family.eq(self.scope_table))
            .filter(removed.eq(false))
            .filter(bucket.eq(i))
            .filter(failed_requests.ge(FAILURES_TO_REFRESH))
            .filter(last_contacted.le(questionable))
            .order(last_contacted.desc())
            .select(crate::models::NodeRow::as_select())
            .get_results(conn)
            .inspect_err(|e| error!("{e}"))
            .unwrap_or_default()
    }

    // Send a ping to fresh the node
    async fn refresh_node(&self, target: &NodeInfo) {
        let ping_msg = KrpcBody::PingQuery(PingQuery::new(self.id));
        let response = self.rpc_manager.query(ping_msg, target, REQ_TIMEOUT).await;
        let mut conn = self.conn();
        match response {
            Ok(_) => self.marks_as_good(&target.id(), &mut conn),
            Err(_) => self.mark_as_dead(&target.id(), &mut conn),
        }
    }

    // TODO: introduce an inflight table to only allow one refresh operation per bucket at each
    // time
    async fn refresh_bucket(&self, i: i32) {
        let mut conn = self.conn();
        let problematics = self.replacement_queue(i, &mut conn);

        let tasks = problematics.into_iter().filter_map(|n| {
            let id = NodeId::try_from_bytes(&n.id)?;
            let ip: IpAddr = n.ip_addr.parse().ok()?;
            let target = NodeInfo::new(id, SocketAddr::new(ip, n.port as u16));
            Some(async move { self.refresh_node(&target).await })
        });
        join_all(tasks).await;
    }

    pub async fn refresh_table(&self) {
        // tombstoned nodes go for good, so the table doesn't grow unboundedly
        {
            use crate::schema::node;
            let mut conn = self.conn();
            let _ = diesel::delete(
                node::table
                    .filter(node::family.eq(self.scope_table))
                    .filter(node::removed.eq(true)),
            )
            .execute(&mut conn)
            .inspect_err(|e| error!("{e}"));
        }

        join_all((0..160).map(|i| async move { self.refresh_bucket(i).await })).await;
        let (good, all) = self.bep42_compliance();
        info!("{} routing table: {all} nodes, {good} with BEP 42 ids", self.family);
    }

    // TODO: use AsRef or Into to make it take in anything that can turn into an ID
    fn marks_as_good(&self, nodee: &NodeId, conn: &mut SqliteConnection) {
        use crate::schema::node::dsl::*;
        let now = unix_timestmap_ms();
        let idd = nodee.0.to_vec();
        let _ = diesel::update(node)
            .filter(family.eq(self.scope_table))
            .filter(id.eq(idd))
            .set((last_contacted.eq(now), failed_requests.eq(0)))
            .execute(conn)
            .inspect_err(|e| error!("{e}"));
    }

    /// Record a failed RPC to a known node; enough of these lands it on the replacement
    /// queue (see `replacement_queue`), which is how dead nodes get evicted.
    pub fn mark_failed(&self, nodee: &NodeId) {
        use crate::schema::node::dsl::*;

        let idd = nodee.0.to_vec();
        let mut conn = self.conn();
        let _ = diesel::update(node)
            .filter(family.eq(self.scope_table))
            .filter(id.eq(idd))
            .set(failed_requests.eq(failed_requests + 1))
            .execute(&mut conn)
            .inspect_err(|e| error!("{e}"));
    }

    /// Takes a node out of the table now, for misbehaving rather than for being unreachable.
    pub fn evict(&self, nodee: &NodeId) {
        let mut conn = self.conn();
        self.mark_as_dead(nodee, &mut conn);
    }

    fn mark_as_dead(&self, nodee: &NodeId, conn: &mut SqliteConnection) {
        use crate::schema::node::dsl::*;

        let idd = nodee.0.to_vec();
        let _ = diesel::update(node)
            .filter(family.eq(self.scope_table))
            .filter(id.eq(idd))
            .set(removed.eq(true))
            .execute(conn)
            .inspect_err(|e| error!("{e}"));
    }

    fn put_to_bucket(&self, nodee: NodeInfo, conn: &mut SqliteConnection) {
        use crate::schema::node::dsl::*;

        let index = self.index(&nodee.id());
        let ip = nodee.end_point().ip();
        let _ = insert_into(node)
            .values(crate::models::NodeRow {
                id: nodee.id().0.to_vec(),
                family: self.scope_table,
                bucket: index,
                last_contacted: unix_timestmap_ms(),
                ip_addr: ip.to_string(),
                ip_group: sybil_group(&ip).map(|g| g.to_string()),
                port: nodee.end_point().port() as i32,
                failed_requests: 0,
                removed: false,
                bep42: Some(compliant(&nodee.id(), ip)),
            })
            .on_conflict_do_nothing()
            .execute(conn)
            .inspect_err(|e| error!("{e}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::txn_id_generator::TxnIdGenerator;
    use crate::test_support::memory_pool;
    use std::net::{Ipv4Addr, SocketAddrV4, SocketAddrV6};
    use std::sync::Arc;
    use tokio::net::UdpSocket;

    #[test]
    fn one_node_per_public_ip_survives_the_purge() {
        let pool = memory_pool();
        let mut conn = pool.get().unwrap();
        let row = |byte: u8, fam: Family, ip: &str, contacted: i64| crate::models::NodeRow {
            id: vec![byte; 20],
            family: fam.db(),
            bucket: 0,
            last_contacted: contacted,
            ip_addr: ip.to_string(),
            ip_group: None,
            port: 6881,
            failed_requests: 0,
            removed: false,
            bep42: None,
        };
        let rows = [
            row(1, Family::V4, "35.167.186.212", 10),
            row(2, Family::V4, "35.167.186.212", 30),
            row(3, Family::V4, "35.167.186.212", 20),
            row(4, Family::V4, "192.168.1.5", 10),
            row(5, Family::V4, "192.168.1.5", 20),
            row(6, Family::V4, "8.8.8.8", 10),
            // one /64, two addresses
            row(7, Family::V6, "2001:470:1:2::7", 10),
            row(8, Family::V6, "2001:470:1:2::8", 20),
            row(9, Family::V6, "2001:470:1:3::9", 10),
            row(10, Family::V6, "fd00::1", 10),
            row(11, Family::V6, "fd00::1", 20),
            // the same id in the other family is another node
            row(6, Family::V6, "2a02:752::6", 10),
        ];
        diesel::insert_into(crate::schema::node::table)
            .values(&rows[..])
            .execute(&mut *conn)
            .unwrap();

        assert_eq!(purge_shared_ips(&mut conn).unwrap(), 3);
        let mut left: Vec<(i32, u8)> = crate::schema::node::table
            .select((crate::schema::node::family, crate::schema::node::id))
            .load::<(i32, Vec<u8>)>(&mut *conn)
            .unwrap()
            .into_iter()
            .map(|(f, id)| (f, id[0]))
            .collect();
        left.sort();
        assert_eq!(
            left,
            [(4, 2), (4, 4), (4, 5), (4, 6), (6, 6), (6, 8), (6, 9), (6, 10), (6, 11)],
            "the latest of the shared public IP or /64, and all of the LAN's"
        );
    }

    #[test]
    fn ipv6_groups_by_slash_64() {
        let a: IpAddr = "2001:470:1:2:aaaa::1".parse().unwrap();
        let b: IpAddr = "2001:470:1:2:bbbb::2".parse().unwrap();
        let c: IpAddr = "2001:470:1:3::1".parse().unwrap();
        assert_eq!(sybil_group(&a), sybil_group(&b));
        assert_ne!(sybil_group(&a), sybil_group(&c));
        assert_eq!(sybil_group(&a), Some("2001:470:1:2::".parse().unwrap()));
        assert_eq!(sybil_group(&"::1".parse().unwrap()), None);
        assert_eq!(sybil_group(&"fe80::1".parse().unwrap()), None);
        assert_eq!(sybil_group(&"10.1.2.3".parse().unwrap()), None);
    }

    async fn test_routing_table_on(our_id: NodeId, bind: SocketAddr) -> RoutingTable {
        let pool = memory_pool();

        let socket = UdpSocket::bind(bind).await.unwrap();
        let broker = RpcManager::new(socket, pool.clone(), Arc::new(TxnIdGenerator::new()), None);
        RoutingTable::new(our_id, broker, pool)
    }

    async fn test_routing_table(our_id: NodeId) -> RoutingTable {
        test_routing_table_on(our_id, SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into()).await
    }

    fn id_with_first_byte(b: u8) -> NodeId {
        let mut id = [0u8; 20];
        id[0] = b;
        NodeId(id)
    }

    fn addr(i: u8) -> SocketAddr {
        SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, i), 6881).into()
    }

    #[tokio::test]
    async fn find_closest_orders_by_distance_to_the_target_not_to_us() {
        let routing_table = test_routing_table(NodeId([0x00; 20])).await;

        // distances to target [0xFF..]: a=0x0F, b=0xF0, c=0xFE  ->  a < b < c
        // distances to us [0x00..]:     c=0x01, b=0x0F, a=0xF0  ->  c < b < a
        let a = id_with_first_byte(0xF0);
        let b = id_with_first_byte(0x0F);
        let c = id_with_first_byte(0x01);
        routing_table.add(a, addr(1));
        routing_table.add(b, addr(2));
        routing_table.add(c, addr(3));

        let closest = routing_table.find_closest(id_with_first_byte(0xFF));
        let ids: Vec<NodeId> = closest.iter().map(|n| n.id()).collect();
        assert_eq!(ids, vec![a, b, c], "must be ordered by xor distance to the target");
    }

    #[tokio::test]
    async fn the_tables_of_the_two_families_are_separate() {
        let v4 = test_routing_table(NodeId([0x00; 20])).await;
        let v6 = RoutingTable::new(
            NodeId([0x00; 20]),
            RpcManager::new(
                UdpSocket::bind((Ipv6Addr::LOCALHOST, 0)).await.unwrap(),
                v4.table.clone(),
                Arc::new(TxnIdGenerator::new()),
                None,
            ),
            v4.table.clone(),
        );
        assert_eq!(v6.family(), Family::V6);

        let shared = id_with_first_byte(0xF0);
        let public_v6: SocketAddr = SocketAddrV6::new("2001:470:1:2::1".parse().unwrap(), 6881, 0, 0).into();
        v4.add(shared, addr(1));
        v6.add(shared, public_v6);
        // wrong family: ignored by both
        v4.add(id_with_first_byte(0x01), public_v6);
        v6.add(id_with_first_byte(0x02), addr(2));
        // the same /64 is taken
        v6.add(
            id_with_first_byte(0x03),
            SocketAddrV6::new("2001:470:1:2::2".parse().unwrap(), 6881, 0, 0).into(),
        );

        assert_eq!(v4.node_count(), 1);
        assert_eq!(v6.node_count(), 1);
        assert_eq!(v6.find_exact(&shared).unwrap().end_point(), public_v6);
        assert_eq!(v4.find_exact(&shared).unwrap().end_point(), addr(1));

        v6.evict(&shared);
        assert_eq!(v6.node_count(), 0);
        assert_eq!(v4.node_count(), 1, "evicting from one family leaves the other alone");
    }

    #[tokio::test]
    async fn failed_queries_increment_and_good_news_resets() {
        use crate::schema::node::dsl::*;

        let routing_table = test_routing_table(NodeId([0x00; 20])).await;
        let a = id_with_first_byte(0xF0);
        routing_table.add(a, addr(1));

        routing_table.mark_failed(&a);
        routing_table.mark_failed(&a);

        let mut conn = routing_table.table.get().unwrap();
        let failed: i32 = node
            .filter(id.eq(a.0.to_vec()))
            .select(failed_requests)
            .first(&mut conn)
            .unwrap();
        assert_eq!(failed, 2, "each failed RPC must increment the counter");

        routing_table.marks_as_good(&a, &mut conn);
        let failed: i32 = node
            .filter(id.eq(a.0.to_vec()))
            .select(failed_requests)
            .first(&mut conn)
            .unwrap();
        assert_eq!(failed, 0, "hearing from the node resets the counter");
    }

    #[tokio::test]
    async fn a_compliant_node_takes_a_full_buckets_place_from_a_non_compliant_one() {
        use crate::schema::node::dsl::*;

        let mut table = test_routing_table(NodeId([0x00; 20])).await;
        table.bucket_capacity = 2;
        // all in bucket 159 (top bit set), at public addresses, ids made up so not compliant
        let older = NodeId([0xFF; 20]);
        let newer = NodeId([0xFE; 20]);
        table.add(older, "1.1.1.1:6881".parse().unwrap());
        table.add(newer, "2.2.2.2:6881".parse().unwrap());
        let mut conn = table.conn();
        diesel::update(node.filter(id.eq(older.0.to_vec())))
            .set(last_contacted.eq(1))
            .execute(&mut conn)
            .unwrap();
        drop(conn);
        assert_eq!(table.bep42_compliance(), (0, 2));

        let ip: IpAddr = "9.9.9.9".parse().unwrap();
        let good = (0..=255)
            .map(|r| crate::dht::bep42::mint(ip, r))
            .find(|n| n.0[0] & 0x80 != 0)
            .unwrap();
        table.add(good, SocketAddr::new(ip, 6881));
        assert!(table.contains(&good));
        assert!(
            !table.contains(&older),
            "the least recently heard from non-compliant node goes"
        );
        assert!(table.contains(&newer));
        assert_eq!(table.bep42_compliance(), (1, 2));

        // a non-compliant newcomer doesn't get in that way
        table.add(NodeId([0xFD; 20]), "3.3.3.3:6881".parse().unwrap());
        assert!(!table.contains(&NodeId([0xFD; 20])));
    }

    #[test]
    fn compliance_of_old_rows_is_worked_out() {
        let pool = memory_pool();
        let mut conn = pool.get().unwrap();
        let ip: IpAddr = "9.9.9.9".parse().unwrap();
        let good = crate::dht::bep42::mint(ip, 3);
        for (node_id, addr) in [
            (good, "9.9.9.9"),
            (NodeId([1; 20]), "8.8.8.8"),
            (NodeId([2; 20]), "10.0.0.1"),
        ] {
            diesel::insert_into(crate::schema::node::table)
                .values(crate::models::NodeRow {
                    id: node_id.0.to_vec(),
                    family: 4,
                    bucket: 0,
                    last_contacted: 0,
                    ip_addr: addr.to_string(),
                    ip_group: None,
                    port: 6881,
                    failed_requests: 0,
                    removed: false,
                    bep42: None,
                })
                .execute(&mut *conn)
                .unwrap();
        }
        assert_eq!(fill_bep42(&mut conn).unwrap(), 3);
        let mut flags: Vec<(u8, Option<bool>)> = crate::schema::node::table
            .select((crate::schema::node::id, crate::schema::node::bep42))
            .load::<(Vec<u8>, Option<bool>)>(&mut *conn)
            .unwrap()
            .into_iter()
            .map(|(i, b)| (i[0], b))
            .collect();
        flags.sort();
        let mut expected = vec![(good.0[0], Some(true)), (1, Some(false)), (2, Some(true))];
        expected.sort();
        assert_eq!(flags, expected, "the LAN node is exempt");
    }

    #[tokio::test]
    async fn a_node_that_never_answered_is_refreshed_once_it_failed_enough() {
        use crate::schema::node::dsl::*;

        let routing_table = test_routing_table(NodeId([0x00; 20])).await;
        // a node another node referred us to, which never answered a query of ours
        let dead = id_with_first_byte(0xF0);
        routing_table.add(dead, addr(1));
        for _ in 0..3 {
            routing_table.mark_failed(&dead);
        }
        let mut conn = routing_table.table.get().unwrap();
        diesel::update(node.filter(id.eq(dead.0.to_vec())))
            .set(last_contacted.eq(1))
            .execute(&mut conn)
            .unwrap();
        let queued: Vec<Vec<u8>> = routing_table
            .replacement_queue(routing_table.index(&dead), &mut conn)
            .into_iter()
            .map(|row| row.id)
            .collect();
        assert_eq!(queued, vec![dead.0.to_vec()]);
    }

    #[tokio::test]
    async fn the_closest_come_from_the_buckets_below_the_targets_before_those_above() {
        let routing_table = test_routing_table(NodeId([0x00; 20])).await;
        // the target is in bucket 158; 0x80.. sits in 159, 0x20.. in 157, and to the target
        // they're 0xC0.. and 0x60.. away
        let above = id_with_first_byte(0x80);
        let below = id_with_first_byte(0x20);
        routing_table.add(above, addr(1));
        routing_table.add(below, addr(2));

        let target = id_with_first_byte(0x40);
        let ids = |n| -> Vec<NodeId> { routing_table.find_closest_n(target, n).iter().map(|n| n.id()).collect() };
        assert_eq!(ids(1), vec![below]);
        assert_eq!(ids(2), vec![below, above]);
    }

    #[tokio::test]
    async fn find_closest_scans_outwards_from_the_targets_bucket() {
        let routing_table = test_routing_table(NodeId([0x00; 20])).await;

        // put two nodes *near the target* (high first byte) and six near us (low first
        // byte); the eight closest to the target must include both high nodes, even
        // though they are the farthest from us
        let near_target = [0xF0u8, 0xE0];
        for (i, b) in near_target.iter().enumerate() {
            routing_table.add(id_with_first_byte(*b), addr(i as u8));
        }
        for i in 0..6u8 {
            routing_table.add(id_with_first_byte(i + 1), addr(10 + i));
        }

        let closest = routing_table.find_closest(id_with_first_byte(0xFF));
        let ids: Vec<NodeId> = closest.iter().map(|n| n.id()).collect();
        assert_eq!(ids.len(), 8);
        assert_eq!(ids[0], id_with_first_byte(0xF0));
        assert_eq!(ids[1], id_with_first_byte(0xE0));
    }
}
