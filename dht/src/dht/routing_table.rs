//! The k-bucket contact store. It lives in memory, where every packet's lookups and updates
//! go, and is written behind to SQLite (a batch at most every second, from a thread of its
//! own) so contacts survive restarts.
//!
//! The table learns passively: `run` consumes the broker's inbound message fan-out and
//! records every node that answers a query of ours; a node that queries us is pinged before
//! it may join, and nodes named in answers are lookup candidates, not members.
//! Liveness is tracked with a `failed_requests` counter — 3+ failures and 15 minutes unheard
//! from land a node on the replacement queue, and a failed refresh ping drops it.
//!
//! IPv4 and IPv6 nodes share the `node` table, told apart by its `family` column: each
//! [`RoutingTable`] sees only the rows of its own family (BEP 32's separate tables).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::mpsc::{self as std_mpsc, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::upsert::excluded;
use futures::future::join_all;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::dht::bep42::compliant;
use crate::message::ping_query::PingQuery;
use crate::message::{Krpc, KrpcBody};
use crate::models::NodeRow;
use crate::schema::node;
use crate::types::{self, Family, NodeId, NodeInfo};
use crate::utils::unix_timestmap_ms;

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
/// How often at most the table's changes are written out, as one transaction
const PERSIST_EVERY: Duration = Duration::from_secs(1);
/// Changes that make a batch be written early
const PERSIST_BATCH: usize = 4096;
/// Pings at once to nodes that would join the table if they answered (see `vet`)
const MAX_VETTING: usize = 64;

type Db = Pool<ConnectionManager<SqliteConnection>>;

/// What the table knows of a node
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Contact {
    addr: SocketAddr,
    /// when we last heard from it, unix milliseconds
    last_contacted: i64,
    failed: i32,
    bep42: bool,
}

impl Contact {
    fn new(id: &NodeId, addr: SocketAddr, last_contacted: i64) -> Self {
        Self {
            addr,
            last_contacted,
            failed: 0,
            bep42: compliant(id, addr.ip()),
        }
    }
}

/// The table itself: every node by id, each bucket's members, and who holds each address group
#[derive(Debug)]
struct Nodes {
    our_id: NodeId,
    by_id: BTreeMap<NodeId, Contact>,
    buckets: Vec<HashSet<NodeId>>,
    groups: HashMap<IpAddr, NodeId>,
    compliant: usize,
}

impl Nodes {
    fn new(our_id: NodeId) -> Self {
        Self {
            our_id,
            by_id: BTreeMap::new(),
            buckets: vec![HashSet::new(); 160],
            groups: HashMap::new(),
            compliant: 0,
        }
    }

    fn bucket_of(&self, id: &NodeId) -> usize {
        bucket_index(&self.our_id, id) as usize
    }

    fn ip_taken(&self, ip: &IpAddr) -> bool {
        sybil_group(ip).is_some_and(|group| self.groups.contains_key(&group))
    }

    /// Puts `id` in; the caller checked it isn't there and its address group is free
    fn insert(&mut self, id: NodeId, contact: Contact) {
        let bucket = self.bucket_of(&id);
        self.buckets[bucket].insert(id);
        if let Some(group) = sybil_group(&contact.addr.ip()) {
            self.groups.insert(group, id);
        }
        self.compliant += usize::from(contact.bep42);
        self.by_id.insert(id, contact);
    }

    fn remove(&mut self, id: &NodeId) -> Option<Contact> {
        let contact = self.by_id.remove(id)?;
        let bucket = self.bucket_of(id);
        self.buckets[bucket].remove(id);
        if let Some(group) = sybil_group(&contact.addr.ip()) {
            self.groups.remove(&group);
        }
        self.compliant -= usize::from(contact.bep42);
        Some(contact)
    }

    /// The `n` nodes closest to `target`, closest first. Ids sharing a longer prefix with the
    /// target are closer than any sharing a shorter one, and those sharing a prefix are a
    /// range of the sorted ids: the narrowest range holding `n` holds the closest `n`.
    fn closest(&self, target: &NodeId, n: usize) -> Vec<NodeInfo> {
        if n == 0 {
            return vec![];
        }
        let holds_n = |prefix| {
            let (low, high) = prefix_range(target, prefix);
            self.by_id.range(low..=high).nth(n - 1).is_some()
        };
        // the longest prefix whose range holds n, by bisection: a shorter prefix's range holds
        // a longer one's
        let (mut shortest, mut longest) = (0usize, 160);
        while shortest < longest {
            let mid = (shortest + longest).div_ceil(2);
            match holds_n(mid) {
                true => shortest = mid,
                false => longest = mid - 1,
            }
        }
        let (low, high) = prefix_range(target, shortest);
        let mut found: Vec<(NodeId, SocketAddr)> = self.by_id.range(low..=high).map(|(id, c)| (*id, c.addr)).collect();
        let by_distance = |(id, _): &(NodeId, SocketAddr)| id.dist(target);
        if found.len() > n {
            found.select_nth_unstable_by_key(n - 1, by_distance);
            found.truncate(n);
        }
        found.sort_unstable_by_key(by_distance);
        found.into_iter().map(|(id, addr)| NodeInfo::new(id, addr)).collect()
    }
}

/// The lowest and the highest id sharing the first `prefix` bits with `target`
fn prefix_range(target: &NodeId, prefix: usize) -> (NodeId, NodeId) {
    let (mut low, mut high) = (target.0, target.0);
    let (byte, bit) = (prefix / 8, prefix % 8);
    if byte < low.len() {
        let mask = 0xFFu8 >> bit;
        low[byte] &= !mask;
        high[byte] |= mask;
        low[byte + 1..].fill(0);
        high[byte + 1..].fill(0xFF);
    }
    (NodeId(low), NodeId(high))
}

/// A node as it is now, to be written out, or `None` once it's gone
enum Change {
    Node(NodeId, Option<Contact>),
    /// write what's pending now, and say so
    #[cfg(test)]
    Flush(std_mpsc::Sender<()>),
}

/// Writes the table's changes to its rows, a batch at a time, until every sender is gone
fn persist(db: Db, table: i32, our_id: NodeId, changes: std_mpsc::Receiver<Change>) {
    let mut batch: HashMap<NodeId, Option<Contact>> = HashMap::new();
    while let Ok(first) = changes.recv() {
        let mut next = Some(first);
        let deadline = Instant::now() + PERSIST_EVERY;
        let mut open = true;
        while let Some(change) = next.take() {
            match change {
                Change::Node(id, contact) => {
                    batch.insert(id, contact);
                }
                #[cfg(test)]
                Change::Flush(done) => {
                    write(&db, table, &our_id, std::mem::take(&mut batch));
                    let _ = done.send(());
                }
            }
            if batch.len() >= PERSIST_BATCH {
                break;
            }
            match changes.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(change) => next = Some(change),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => open = false,
            }
        }
        write(&db, table, &our_id, std::mem::take(&mut batch));
        if !open {
            return;
        }
    }
}

fn write(db: &Db, table: i32, our_id: &NodeId, batch: HashMap<NodeId, Option<Contact>>) {
    if batch.is_empty() {
        return;
    }
    let mut conn = match db.get() {
        Ok(conn) => conn,
        Err(e) => {
            warn!("couldn't save {} routing table changes: {e}", batch.len());
            return;
        }
    };
    let written = conn.transaction(|conn| {
        for (id, contact) in &batch {
            let this = node::table
                .filter(node::family.eq(table))
                .filter(node::id.eq(id.as_bytes()));
            let Some(contact) = contact else {
                diesel::delete(this).execute(conn)?;
                continue;
            };
            let ip = contact.addr.ip();
            diesel::insert_into(node::table)
                .values(NodeRow {
                    id: id.0.to_vec(),
                    family: table,
                    bucket: bucket_index(our_id, id),
                    last_contacted: contact.last_contacted,
                    ip_addr: ip.to_string(),
                    ip_group: sybil_group(&ip).map(|g| g.to_string()),
                    port: contact.addr.port().into(),
                    failed_requests: contact.failed,
                    removed: false,
                    bep42: Some(contact.bep42),
                })
                .on_conflict((node::family, node::id))
                .do_update()
                .set((
                    node::bucket.eq(excluded(node::bucket)),
                    node::last_contacted.eq(excluded(node::last_contacted)),
                    node::ip_addr.eq(excluded(node::ip_addr)),
                    node::ip_group.eq(excluded(node::ip_group)),
                    node::port.eq(excluded(node::port)),
                    node::failed_requests.eq(excluded(node::failed_requests)),
                    node::removed.eq(false),
                    node::bep42.eq(excluded(node::bep42)),
                ))
                .execute(conn)?;
        }
        Ok::<_, diesel::result::Error>(())
    });
    if let Err(e) = written {
        warn!("couldn't save {} routing table changes: {e}", batch.len());
    }
}

/// The table's saved rows. Rows marked removed (by versions that tombstoned) go now.
fn load(db: &Db, table: i32, our_id: NodeId) -> Nodes {
    let mut nodes = Nodes::new(our_id);
    let rows = db.get().map_err(|e| e.to_string()).and_then(|mut conn| {
        diesel::delete(
            node::table
                .filter(node::family.eq(table))
                .filter(node::removed.eq(true)),
        )
        .execute(&mut conn)
        .map_err(|e| e.to_string())?;
        node::table
            .filter(node::family.eq(table))
            .select((
                node::id,
                node::ip_addr,
                node::port,
                node::last_contacted,
                node::failed_requests,
            ))
            .load::<(Vec<u8>, String, i32, i64, i32)>(&mut conn)
            .map_err(|e| e.to_string())
    });
    let rows = rows
        .inspect_err(|e| warn!("couldn't read the saved routing table: {e}"))
        .unwrap_or_default();
    for (id, ip, port, last_contacted, failed) in rows {
        let (Some(id), Ok(ip), Ok(port)) = (NodeId::try_from_bytes(&id), ip.parse::<IpAddr>(), u16::try_from(port))
        else {
            continue;
        };
        let addr = SocketAddr::new(ip, port);
        if id == our_id || nodes.by_id.contains_key(&id) || nodes.ip_taken(&ip) {
            continue;
        }
        nodes.insert(
            id,
            Contact {
                failed,
                ..Contact::new(&id, addr, last_contacted)
            },
        );
    }
    nodes
}

#[derive(Debug, Clone)]
/// A RoutingTable will tell you who are the closest nodes that we know
pub struct RoutingTable {
    id: NodeId,
    family: Family,
    nodes: Arc<Mutex<Nodes>>,
    /// to the thread writing the table out (see `persist`)
    changes: std_mpsc::Sender<Change>,
    rpc_manager: RpcManager,
    /// NOTE(deviation): BEP 5 specifies k = 8 per bucket with split-when-covers-self.
    /// We keep 160 flat buckets of 1024 and let `find_closest` pick by distance from the
    /// whole table; eviction of dead nodes (failed_requests >= 3 → refresh) keeps the table
    /// fresh. Revisit if the table ever outgrows this.
    bucket_capacity: usize,
    /// buckets with a refresh under way, see `add`
    refreshing: Arc<Mutex<HashSet<i32>>>,
    /// addresses being pinged before they may join, see `vet`
    vetting: Arc<Mutex<HashSet<SocketAddr>>>,
}

impl RoutingTable {
    /// The table holds the nodes of the broker's address family, in the broker's scope, as
    /// last saved in `db`.
    pub fn new(id: NodeId, rpc_manager: RpcManager, db: Db) -> RoutingTable {
        let table = rpc_manager.scope().table;
        let nodes = load(&db, table, id);
        let (changes, saved) = std_mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name(format!("routing table {table}"))
            .spawn(move || persist(db, table, id, saved));
        if let Err(e) = spawned {
            error!("the routing table won't be saved: {e}");
        }
        RoutingTable {
            id,
            family: rpc_manager.family(),
            nodes: Arc::new(Mutex::new(nodes)),
            changes,
            rpc_manager,
            bucket_capacity: 1024,
            refreshing: Arc::default(),
            vetting: Arc::default(),
        }
    }

    pub fn family(&self) -> Family {
        self.family
    }

    fn nodes(&self) -> std::sync::MutexGuard<'_, Nodes> {
        self.nodes.lock().unwrap()
    }

    /// Writes `id` as `contact` (or its removal) out, in the next batch
    fn save(&self, id: NodeId, contact: Option<Contact>) {
        let _ = self.changes.send(Change::Node(id, contact));
    }

    /// The sender is alive, and so maybe are the nodes it refers us to
    fn learn_from(&self, from: SocketAddr, message: &Krpc) {
        // errors carry no id
        let Some(sender) = message.node_id() else {
            return;
        };
        let sender = NodeInfo::new(sender, from);
        if !message.is_query() {
            // an answer to a query of ours, from where the query went (the broker sees to that)
            if !self.mark_good(&sender) {
                self.add(sender.id(), from);
            }
            // the other family's nodes are the session's business (see `DhtSession::pair_with`)
            if let KrpcBody::FindNodeGetPeersResponse(res) = &message.body {
                for node in res.nodes_of(self.family) {
                    self.vet(node.id(), node.end_point());
                }
            }
            return;
        }
        // BEP 43: a read-only node wouldn't answer us; its query is served, it isn't kept
        if message.read_only {
            return;
        }
        // BEP 5: a node that answered us stays good while it keeps querying us; one that never
        // did is asked first, so a query from a spoofed address or a node that won't answer
        // gets nobody in
        if !self.mark_good(&sender) {
            self.vet(sender.id(), from);
        }
    }

    /// Pings a node we heard of but never heard answer, if the table would take it: its answer
    /// comes back through the broker and adds it (see `learn_from`). Nodes named in answers
    /// are only candidates for lookups until then.
    pub fn vet(&self, id: NodeId, addr: SocketAddr) {
        if Family::of(&addr) != self.family || id == self.id || !self.would_take(&id, &addr) {
            return;
        }
        {
            let mut vetting = self.vetting.lock().unwrap();
            if vetting.len() >= MAX_VETTING || !vetting.insert(addr) {
                return;
            }
        }
        let this = self.clone();
        tokio::spawn(async move {
            let ping = KrpcBody::PingQuery(PingQuery::new(this.id));
            let _ = this.rpc_manager.query(ping, addr, REQ_TIMEOUT).await;
            this.vetting.lock().unwrap().remove(&addr);
        });
    }

    /// Whether `id` at `addr` would get in now: it's new, its address group is free, and its
    /// bucket has room, or would make some for it (BEP 42)
    fn would_take(&self, id: &NodeId, addr: &SocketAddr) -> bool {
        let nodes = self.nodes();
        if nodes.by_id.contains_key(id) || nodes.ip_taken(&addr.ip()) {
            return false;
        }
        let bucket = self.index(id);
        nodes.buckets[bucket as usize].len() < self.bucket_capacity
            || (compliant(id, addr.ip()) && least_recent_noncompliant(&nodes, bucket).is_some())
    }

    /// Learns from every message in `inbound` (the caller's subscription to the broker's
    /// inbound queue), and refreshes the table every REFRESH_EVERY
    pub async fn run(&self, mut inbound: mpsc::Receiver<(Krpc, SocketAddr)>) {
        // the refresh runs beside the inbox, which it would otherwise hold up for as long as
        // it takes
        let refreshing = async {
            let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + REFRESH_EVERY, REFRESH_EVERY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                self.refresh_table().await;
            }
        };
        let learning = async {
            while let Some((msg, from)) = inbound.recv().await {
                self.learn_from(from, &msg);
            }
        };
        tokio::select! {
            _ = refreshing => {}
            _ = learning => {}
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
        self.nodes().closest(&target, total.into())
    }

    pub fn find_exact(&self, target: &NodeId) -> Option<NodeInfo> {
        let contact = *self.nodes().by_id.get(target)?;
        Some(NodeInfo::new(*target, contact.addr))
    }

    pub fn contains(&self, target: &NodeId) -> bool {
        self.nodes().by_id.contains_key(target)
    }

    /// Adds a node we heard of, if its bucket has room or can make some: a full bucket takes a
    /// BEP 42 compliant node in place of a non-compliant one; otherwise it's refreshed, and
    /// the node goes in if that made room. A node of the other address family is ignored.
    #[tracing::instrument(skip(self))]
    pub fn add(&self, new_node_id: NodeId, addr: SocketAddr) {
        if Family::of(&addr) != self.family || new_node_id == self.id {
            return;
        }
        let bucket_idx = self.index(&new_node_id);
        let contact = Contact::new(&new_node_id, addr, unix_timestmap_ms());
        {
            let mut nodes = self.nodes();
            if nodes.by_id.contains_key(&new_node_id) || nodes.ip_taken(&addr.ip()) {
                return;
            }
            if nodes.buckets[bucket_idx as usize].len() < self.bucket_capacity {
                nodes.insert(new_node_id, contact);
                drop(nodes);
                self.save(new_node_id, Some(contact));
                return;
            }
            // BEP 42: a compliant node takes the place of a non-compliant one, the one heard
            // from least recently
            if contact.bep42
                && let Some(victim) = least_recent_noncompliant(&nodes, bucket_idx)
            {
                debug!("Bucket {bucket_idx} full, {new_node_id:?} replaces a non-BEP 42 node");
                nodes.remove(&victim);
                nodes.insert(new_node_id, contact);
                drop(nodes);
                self.save(victim, None);
                self.save(new_node_id, Some(contact));
                return;
            }
        }

        // one refresh of a bucket at a time; a node turning up meanwhile is let go
        if !self.refreshing.lock().unwrap().insert(bucket_idx) {
            return;
        }
        info!("Bucket {bucket_idx} full, refreshing it to evict");
        let this = self.clone();
        tokio::spawn(async move {
            this.refresh_bucket(bucket_idx).await;
            this.refreshing.lock().unwrap().remove(&bucket_idx);
            let mut nodes = this.nodes();
            if nodes.buckets[bucket_idx as usize].len() >= this.bucket_capacity {
                info!("Bucket {bucket_idx} remains full after refreshing, node not added");
            } else if !nodes.by_id.contains_key(&new_node_id) && !nodes.ip_taken(&addr.ip()) {
                nodes.insert(new_node_id, contact);
                drop(nodes);
                this.save(new_node_id, Some(contact));
            }
        });
    }

    /// How many nodes in the table have BEP 42 compliant ids (LAN ones count as compliant),
    /// and how many nodes there are
    pub fn bep42_compliance(&self) -> (usize, usize) {
        let nodes = self.nodes();
        (nodes.compliant, nodes.by_id.len())
    }

    pub fn node_count(&self) -> usize {
        self.nodes().by_id.len()
    }

    /// The questionable nodes of the ith bucket (BEP 5: not heard from in 15 minutes), the ones
    /// that failed a few queries since: a refresh pings them, and drops those that don't answer
    fn replacement_queue(&self, i: i32) -> Vec<NodeInfo> {
        let questionable = unix_timestmap_ms() - QUESTIONABLE_AFTER.as_millis() as i64;
        let nodes = self.nodes();
        let mut queue: Vec<(NodeId, Contact)> = nodes.buckets[i as usize]
            .iter()
            .filter_map(|id| Some((*id, *nodes.by_id.get(id)?)))
            .filter(|(_, c)| c.failed >= FAILURES_TO_REFRESH && c.last_contacted <= questionable)
            .collect();
        queue.sort_by_key(|(_, c)| std::cmp::Reverse(c.last_contacted));
        queue.into_iter().map(|(id, c)| NodeInfo::new(id, c.addr)).collect()
    }

    /// Pings each node of the ith bucket's replacement queue, and drops those that don't answer
    async fn refresh_bucket(&self, i: i32) {
        let queue = self.replacement_queue(i);
        join_all(queue.into_iter().map(|target| async move {
            let ping = KrpcBody::PingQuery(PingQuery::new(self.id));
            let answered = self
                .rpc_manager
                .query(ping, target.end_point(), REQ_TIMEOUT)
                .await
                .is_ok();
            match answered {
                true => _ = self.mark_good(&target),
                false => self.evict(&target.id()),
            }
        }))
        .await;
    }

    pub async fn refresh_table(&self) {
        join_all((0..160).map(|i| self.refresh_bucket(i))).await;
        let (good, all) = self.bep42_compliance();
        info!("{} routing table: {all} nodes, {good} with BEP 42 ids", self.family);
    }

    /// We heard from `node`; another address claiming its id doesn't count. Whether the table
    /// has it.
    fn mark_good(&self, node: &NodeInfo) -> bool {
        let mut nodes = self.nodes();
        let Some(contact) = nodes.by_id.get_mut(&node.id()) else {
            return false;
        };
        if contact.addr != node.end_point() {
            return false;
        }
        contact.last_contacted = unix_timestmap_ms();
        contact.failed = 0;
        let contact = *contact;
        drop(nodes);
        self.save(node.id(), Some(contact));
        true
    }

    /// Record a failed RPC to a known node; enough of these lands it on the replacement
    /// queue (see `replacement_queue`), which is how dead nodes get evicted.
    pub fn mark_failed(&self, id: &NodeId) {
        let mut nodes = self.nodes();
        let Some(contact) = nodes.by_id.get_mut(id) else {
            return;
        };
        contact.failed += 1;
        let contact = *contact;
        drop(nodes);
        self.save(*id, Some(contact));
    }

    /// Takes a node out of the table now
    pub fn evict(&self, id: &NodeId) {
        if self.nodes().remove(id).is_some() {
            self.save(*id, None);
        }
    }
}

fn least_recent_noncompliant(nodes: &Nodes, bucket: i32) -> Option<NodeId> {
    nodes.buckets[bucket as usize]
        .iter()
        .filter_map(|id| Some((*id, nodes.by_id.get(id)?)))
        .filter(|(_, c)| !c.bep42)
        .min_by_key(|(_, c)| c.last_contacted)
        .map(|(id, _)| id)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::txn_id_generator::TxnIdGenerator;
    use crate::test_support::memory_pool;
    use rand::RngExt;
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

    async fn test_routing_table_in(our_id: NodeId, bind: SocketAddr, pool: Db) -> RoutingTable {
        let socket = UdpSocket::bind(bind).await.unwrap();
        let broker = RpcManager::new(socket, pool.clone(), Arc::new(TxnIdGenerator::new()), None);
        RoutingTable::new(our_id, broker, pool)
    }

    async fn test_routing_table(our_id: NodeId) -> RoutingTable {
        test_routing_table_in(our_id, SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into(), memory_pool()).await
    }

    fn id_with_first_byte(b: u8) -> NodeId {
        let mut id = [0u8; 20];
        id[0] = b;
        NodeId(id)
    }

    fn addr(i: u8) -> SocketAddr {
        SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, i), 6881).into()
    }

    impl RoutingTable {
        fn contact(&self, id: &NodeId) -> Contact {
            *self.nodes().by_id.get(id).expect("in the table")
        }

        fn last_heard(&self, id: &NodeId, at: i64) {
            self.nodes().by_id.get_mut(id).expect("in the table").last_contacted = at;
        }

        /// Waits for what the table changed so far to be written out
        fn flush(&self) {
            let (done, flushed) = std_mpsc::channel();
            self.changes.send(Change::Flush(done)).unwrap();
            flushed.recv().unwrap();
        }
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
    async fn find_closest_is_what_sorting_the_whole_table_gives() {
        let mut routing_table = test_routing_table(NodeId([0x00; 20])).await;
        routing_table.bucket_capacity = usize::MAX;
        let mut all = vec![];
        for i in 0..3000u32 {
            let id = NodeId(rand::rng().random());
            let [_, a, b, c] = i.to_be_bytes();
            routing_table.add(id, SocketAddr::from(([10, a, b, c], 6881)));
            all.push(id);
        }
        for _ in 0..200 {
            let target = match rand::rng().random::<bool>() {
                true => NodeId(rand::rng().random()),
                // right next to a node, where the narrow ranges are
                false => all[rand::rng().random_range(0..all.len())],
            };
            let n = rand::rng().random_range(0..40u16);
            let mut sorted = all.clone();
            sorted.sort_by_cached_key(|id| id.dist(&target));
            sorted.truncate(n.into());
            let found: Vec<NodeId> = routing_table.find_closest_n(target, n).iter().map(|n| n.id()).collect();
            assert_eq!(found, sorted);
        }
    }

    #[tokio::test]
    async fn the_table_is_saved_and_read_back() {
        let pool = memory_pool();
        let socket = || async { UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap() };
        let broker = RpcManager::new(socket().await, pool.clone(), Arc::new(TxnIdGenerator::new()), None);
        let table = RoutingTable::new(NodeId([0; 20]), broker, pool.clone());
        let (kept, gone) = (id_with_first_byte(0xF0), id_with_first_byte(0x0F));
        table.add(kept, addr(1));
        table.add(gone, addr(2));
        table.mark_failed(&kept);
        table.evict(&gone);
        table.flush();

        let broker = RpcManager::new(socket().await, pool.clone(), Arc::new(TxnIdGenerator::new()), None);
        let again = RoutingTable::new(NodeId([0; 20]), broker, pool);
        assert_eq!(again.node_count(), 1);
        assert_eq!(again.find_exact(&kept).unwrap().end_point(), addr(1));
        assert_eq!(again.contact(&kept).failed, 1);
    }

    #[tokio::test]
    async fn the_tables_of_the_two_families_are_separate() {
        let pool = memory_pool();
        let v4 = test_routing_table_in(NodeId([0x00; 20]), (Ipv4Addr::LOCALHOST, 0).into(), pool.clone()).await;
        let v6 = test_routing_table_in(NodeId([0x00; 20]), (Ipv6Addr::LOCALHOST, 0).into(), pool).await;
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
        let routing_table = test_routing_table(NodeId([0x00; 20])).await;
        let a = id_with_first_byte(0xF0);
        routing_table.add(a, addr(1));

        routing_table.mark_failed(&a);
        routing_table.mark_failed(&a);
        assert_eq!(
            routing_table.contact(&a).failed,
            2,
            "each failed RPC must increment the counter"
        );

        routing_table.mark_good(&NodeInfo::new(a, addr(1)));
        assert_eq!(
            routing_table.contact(&a).failed,
            0,
            "hearing from the node resets the counter"
        );
    }

    #[tokio::test]
    async fn a_compliant_node_takes_a_full_buckets_place_from_a_non_compliant_one() {
        let mut table = test_routing_table(NodeId([0x00; 20])).await;
        table.bucket_capacity = 2;
        // all in bucket 159 (top bit set), at public addresses, ids made up so not compliant
        let older = NodeId([0xFF; 20]);
        let newer = NodeId([0xFE; 20]);
        table.add(older, "1.1.1.1:6881".parse().unwrap());
        table.add(newer, "2.2.2.2:6881".parse().unwrap());
        table.last_heard(&older, 1);
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
        // and the evicted node's address is free again
        table.evict(&newer);
        table.add(NodeId([0xFC; 20]), "1.1.1.1:6881".parse().unwrap());
        assert!(table.contains(&NodeId([0xFC; 20])));
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
        let routing_table = test_routing_table(NodeId([0x00; 20])).await;
        // a node another node referred us to, which never answered a query of ours
        let dead = id_with_first_byte(0xF0);
        routing_table.add(dead, addr(1));
        for _ in 0..3 {
            routing_table.mark_failed(&dead);
        }
        routing_table.last_heard(&dead, 1);
        let queued: Vec<NodeId> = routing_table
            .replacement_queue(routing_table.index(&dead))
            .into_iter()
            .map(|n| n.id())
            .collect();
        assert_eq!(queued, vec![dead]);
    }

    #[tokio::test]
    async fn only_the_node_at_its_own_address_vouches_for_its_id() {
        let routing_table = test_routing_table(NodeId([0x00; 20])).await;
        let dead = id_with_first_byte(0xF0);
        routing_table.add(dead, addr(1));
        for _ in 0..3 {
            routing_table.mark_failed(&dead);
        }
        routing_table.last_heard(&dead, 1);

        // someone elsewhere claiming the dead node's id doesn't keep it in the table
        let claim = Krpc::new(
            types::TransactionId::from_bytes(b"aa"),
            KrpcBody::PingQuery(PingQuery::new(dead)),
        );
        routing_table.learn_from(addr(2), &claim);
        let queued: Vec<NodeId> = routing_table
            .replacement_queue(routing_table.index(&dead))
            .into_iter()
            .map(|n| n.id())
            .collect();
        assert_eq!(queued, vec![dead]);

        // the node itself does
        routing_table.learn_from(addr(1), &claim);
        assert!(routing_table.replacement_queue(routing_table.index(&dead)).is_empty());
    }

    #[tokio::test]
    async fn a_node_named_in_an_answer_is_only_a_candidate_until_it_answers_itself() {
        use crate::message::find_node_get_peers_response::Builder;

        let table = test_routing_table(NodeId([0x00; 20])).await;
        let answerer = id_with_first_byte(0xA0);
        let named = NodeInfo::new(id_with_first_byte(0xB0), addr(2));
        let answer = Krpc::new(
            types::TransactionId::from_bytes(b"aa"),
            KrpcBody::FindNodeGetPeersResponse(Builder::new(answerer).with_node(named).build()),
        );
        table.learn_from(addr(1), &answer);
        assert!(table.contains(&answerer));
        assert!(!table.contains(&named.id()));
    }

    #[tokio::test]
    async fn a_node_that_queries_us_joins_once_it_answers_a_ping() {
        let pool = memory_pool();
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let broker = RpcManager::new(socket, pool.clone(), Arc::new(TxnIdGenerator::new()), None);
        let us = broker.local_addr().unwrap();
        let table = RoutingTable::new(NodeId([0x00; 20]), broker.clone(), pool);
        let inbox = broker.subscribe_inbound();
        tokio::spawn({
            let broker = broker.clone();
            async move { broker.run().await }
        });
        tokio::spawn({
            let table = table.clone();
            async move { table.run(inbox).await }
        });

        // nobody is at a spoofed source address to answer, so it never gets in
        let ping = |id| KrpcBody::PingQuery(PingQuery::new(id));
        let spoofed = Krpc::new(types::TransactionId::from_bytes(b"sp"), ping(id_with_first_byte(0xD0)));
        table.learn_from(addr(9), &spoofed);
        assert!(!table.contains(&id_with_first_byte(0xD0)));

        // a real node is asked back, and joins with its answer
        let them = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let their_id = id_with_first_byte(0xC0);
        let query = Krpc::new(types::TransactionId::from_bytes(b"qq"), ping(their_id));
        them.send_to(&query.encode(), us).await.unwrap();
        let mut buf = [0u8; 1500];
        let asked = loop {
            let (n, _) = tokio::time::timeout(Duration::from_secs(2), them.recv_from(&mut buf))
                .await
                .expect("pinged back")
                .unwrap();
            let msg = Krpc::decode(&buf[..n]).unwrap();
            if msg.is_query() {
                break msg;
            }
        };
        assert!(!table.contains(&their_id));
        let answer = Krpc::new(
            asked.txn_id,
            KrpcBody::PingAnnouncePeerResponse(
                crate::message::ping_announce_peer_response::PingAnnouncePeerResponse::new(their_id),
            ),
        );
        them.send_to(&answer.encode(), us).await.unwrap();
        assert!(crate::test_support::eventually(|| table.contains(&their_id)).await);
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
