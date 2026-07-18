use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
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
use tokio::time::sleep;
use tracing::{error, info};

use crate::dht::xor;
use crate::message::Krpc;
use crate::message::ping_query::PingQuery;
use crate::models::NodeNoMetaInfo;
use crate::utils::unix_timestmap_ms;
use crate::{
    message::KrpcBody,
    types::{self, NodeId, NodeInfo},
};

use super::rpc_manager::RpcManager;
use super::state::REQ_TIMEOUT;

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

#[derive(Debug, Clone)]
/// A RoutingTable will tell you who are the closest nodes that we know
pub struct RoutingTable {
    id: NodeId,
    table: Pool<ConnectionManager<SqliteConnection>>,
    rpc_manager: RpcManager,
    /// NOTE(deviation): BEP 5 specifies k = 8 per bucket with split-when-covers-self.
    /// We keep 160 flat buckets of 1024 and let `find_closest` gather across buckets;
    /// eviction of dead nodes (failed_requests >= 3 → refresh → mark_as_dead) keeps the
    /// table fresh. Revisit if the table ever outgrows this.
    bucket_size: usize,
    inbound_messages: Arc<Mutex<Option<mpsc::Receiver<(Krpc, SocketAddrV4)>>>>,
}

impl RoutingTable {
    pub fn new(
        id: NodeId,
        rpc_manager: RpcManager,
        table: Pool<ConnectionManager<SqliteConnection>>,
        inbound_messages: mpsc::Receiver<(Krpc, SocketAddrV4)>,
    ) -> RoutingTable {
        RoutingTable {
            id,
            table,
            rpc_manager,
            bucket_size: 1024, // TODO: make this configurable in the future
            inbound_messages: Arc::new(Mutex::new(Some(inbound_messages))),
        }
    }

    fn add_new_nodes(&self, from: SocketAddrV4, message: &Krpc) {
        if let KrpcBody::ErrorResponse(_) = message.body {
            return;
        }

        let node_id = match &message.body {
            KrpcBody::AnnouncePeerQuery(announce_peer_query) => *announce_peer_query.requestor(),
            KrpcBody::FindNodeQuery(find_node_query) => find_node_query.requestor(),
            KrpcBody::GetPeersQuery(get_peers_query) => *get_peers_query.requestor(),
            KrpcBody::PingQuery(ping_query) => *ping_query.requestor(),
            KrpcBody::PingAnnouncePeerResponse(ping_announce_peer_response) => *ping_announce_peer_response.target_id(),
            KrpcBody::FindNodeGetPeersResponse(find_node_get_peers_response) => *find_node_get_peers_response.queried(),
            KrpcBody::ErrorResponse(_) => unreachable!("errors should get early returned"),
        };

        {
            let mut conn = self.conn();
            self.marks_as_good(&node_id, &mut conn);
        }
        self.add(node_id, from);

        // if it's response from find_peers or get_nodes, they have additional info
        if let KrpcBody::FindNodeGetPeersResponse(res) = &message.body {
            for node in res.nodes() {
                // TODO: this is a bit stupid as we destroy the structure just to copy but fix later
                self.add(node.id(), node.end_point());
            }
        }
    }

    /// keep listening for all incoming responses and update our table
    pub async fn run(&self) {
        let mut inbound = {
            self.inbound_messages
                .lock()
                .unwrap()
                .take()
                .expect("run for RoutingTable is only called once")
        };

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
                // TODO: refresh duration, make it configurable
                _ = sleep(Duration::from_secs(180)) => {
                    self.refresh_table().await;
                }
            }
        }
    }

    /// Returns the bucket index that the target node belongs in
    fn index(&self, target: &NodeId) -> i32 {
        bucket_index(&self.id, target)
    }

    fn closest_in_bucket(&self, target: &NodeId, i: i32, limit: i64, conn: &mut SqliteConnection) -> Vec<NodeInfo> {
        use crate::schema::node::dsl::*;

        let nodes = node
            .select(NodeNoMetaInfo::as_select())
            .filter(removed.eq(false))
            .filter(bucket.eq(i))
            // order by Kadamlia XOR distance
            .order(xor(id, target.as_bytes()))
            .limit(limit)
            .load(conn)
            .expect("Writers don't block readers");

        nodes.into_iter().map(|nodee| nodee.into()).collect::<Vec<_>>()
    }

    pub fn find_closest(&self, target: NodeId) -> Vec<NodeInfo> {
        // NOTE: Start with the center and alternating left and right expansion, none of this is
        // done in a transaction so we don't block other writers due to sqlite only allowing one
        // writers at anytime. It's possible that other writers may modify the table while we
        // fetch, that's ok, the DHT is allowed to be somewhat sloppy.

        let total: u16 = 8; // TODO: make this as a param

        let mut conn = self.conn();

        let target_idx = self.index(&target);
        let mut closest = self.closest_in_bucket(&target, target_idx, total.into(), &mut conn);

        let mut offset = 1;
        let mut remaining: u16 = total.saturating_sub(closest.len().try_into().expect("we spcified the limit"));
        loop {
            // if got we wanted, or both sides are out of bounds, then there's no more we can do
            if remaining == 0 || (target_idx - offset < 0 && target_idx + offset >= 160) {
                break;
            }

            // favours the nodes closer to the target, i.e buckets with larger index
            let right_bucket = target_idx + offset;
            if remaining != 0 && right_bucket < 160 {
                let mut additional = self.closest_in_bucket(&target, right_bucket, remaining.into(), &mut conn);
                remaining = remaining.saturating_sub(additional.len().try_into().expect("we spcified the limit"));
                closest.append(&mut additional);
            }

            let left_bucket = target_idx - offset;
            if remaining != 0 && left_bucket >= 0 {
                let mut additional = self.closest_in_bucket(&target, left_bucket, remaining.into(), &mut conn);
                remaining = remaining.saturating_sub(additional.len().try_into().expect("we spcified the limit"));
                closest.append(&mut additional);
            }

            offset += 1;
        }

        closest.sort_unstable_by(|a, b| types::cmp_resp(&a.id(), &b.id(), &target));

        closest
    }

    pub fn find_exact(&self, target: &NodeId) -> Option<NodeInfo> {
        use crate::schema::node::dsl::*;
        let mut conn = self.table.get().unwrap();
        let target_id = target.0.to_vec();
        node.filter(removed.eq(false))
            .filter(id.eq(target_id))
            .select(NodeNoMetaInfo::as_select())
            .first(&mut conn)
            .ok()
            .map(|nodee| nodee.into())
    }

    pub fn contains(&self, target: &NodeId) -> bool {
        self.find_exact(target).is_some()
    }

    pub fn full_bucket(&self, i: i32) -> bool {
        let size = self.bucket_size(i);
        assert!(
            !(size > self.bucket_size),
            "bucket managed to grow beyond the size limit"
        );
        self.bucket_size(i) == self.bucket_size
    }

    /// Add a new node to the routing table, if the buckets are full, the node will be ignored.
    #[tracing::instrument(skip(self))]
    pub fn add(&self, new_node_id: NodeId, addr: SocketAddrV4) {
        // TODO: contains will request another connection from the pool..., should be fine
        // for now
        if self.contains(&new_node_id) {
            info!("Already contains this node in routing table, skipping");
            return;
        }

        let bucket_idx = self.index(&new_node_id);
        if !self.full_bucket(bucket_idx) {
            info!("Bucket {bucket_idx} has capacity, inserting");
            let node = NodeInfo::new(new_node_id, addr);
            let mut conn = self.conn();
            self.put_to_bucket(node, &mut conn);
            return;
        }

        info!("Bucket {bucket_idx} full, refreshing all buckets to evict");
        let this = self.clone();
        let work = async move {
            let node = NodeInfo::new(new_node_id, addr);

            // instead of going from least recently seen and probe one by one, just refresh the
            // entire bucket
            let bucket_idx = this.index(&new_node_id);
            this.refresh_bucket(bucket_idx).await;

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

    fn conn(&self) -> PooledConnection<ConnectionManager<SqliteConnection>> {
        self.table.get().expect("Pool should just work")
    }

    pub fn node_count(&self) -> usize {
        use crate::schema::node::dsl::*;
        let mut conn = self.conn();
        let count: i64 = node.filter(removed.eq(false)).count().get_result(&mut conn).unwrap();
        count as usize
    }

    /// How many nodes are in the ith bucket (0-indexed)
    pub fn bucket_size(&self, i: i32) -> usize {
        use crate::schema::node::dsl::*;
        let mut conn = self.conn();
        let count: i64 = node
            .filter(removed.eq(false))
            .filter(bucket.eq(i))
            .count()
            .get_result(&mut conn)
            .unwrap();
        count as usize
    }

    /// Find the list of "problematic" nodes that if not responded, should be removed
    fn replacement_queue(&self, i: i32, conn: &mut SqliteConnection) -> Vec<crate::models::NodeRow> {
        use crate::schema::node::dsl::*;

        fn cutoff() -> i64 {
            let fifteenth_mins_ms = 15 * 60 * 1000;
            unix_timestmap_ms() - fifteenth_mins_ms
        }

        node.filter(removed.eq(false))
            .filter(bucket.eq(i))
            .filter(failed_requests.ge(3)) // TODO: make this configurable
            .filter(last_sent.le(cutoff()))
            .order(last_contacted.desc())
            .select(crate::models::NodeRow::as_select())
            .get_results(conn)
            .unwrap()
    }

    // Send a ping to fresh the node
    async fn refresh_node(&self, target: &NodeInfo) {
        let ping_msg = KrpcBody::PingQuery(PingQuery::new(self.id));

        {
            let mut conn = self.conn();
            update_last_sent(&target.id(), unix_timestmap_ms(), &mut conn);
        }

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

        let tasks = problematics.into_iter().map(|n| async move {
            let id = NodeId::from_bytes(&*n.id);
            let ip: Ipv4Addr = n.ip_addr.parse().unwrap();
            let endpoint = SocketAddrV4::new(ip, n.port as u16);
            let target = NodeInfo::new(id, endpoint);
            self.refresh_node(&target).await
        });
        join_all(tasks).await;
    }

    pub async fn refresh_table(&self) {
        // housekeeping: expired peers and tombstoned nodes go for good, so the store
        // doesn't grow unboundedly
        {
            use crate::schema::{node, peer};
            let cutoff = unix_timestmap_ms() - 45 * 60 * 1000;
            let mut conn = self.conn();
            let _ = diesel::delete(peer::table.filter(peer::last_announced.lt(cutoff)))
                .execute(&mut conn)
                .inspect_err(|e| error!("{e}"));
            let _ = diesel::delete(node::table.filter(node::removed.eq(true)))
                .execute(&mut conn)
                .inspect_err(|e| error!("{e}"));
        }

        join_all((0..160).map(|i| async move { self.refresh_bucket(i).await })).await;
    }

    pub async fn refresh_table_loop(&self) {
        // TODO: make this configurable
        let refresh_duration = Duration::from_secs(3 * 60);
        loop {
            self.refresh_table().await;
            sleep(refresh_duration).await;
        }
    }

    // TODO: use AsRef or Into to make it take in anything that can turn into an ID
    fn marks_as_good(&self, nodee: &NodeId, conn: &mut SqliteConnection) {
        use crate::schema::node::dsl::*;
        let now = unix_timestmap_ms();
        let idd = nodee.0.to_vec();
        let _ = diesel::update(node)
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
            .filter(id.eq(idd))
            .set(failed_requests.eq(failed_requests + 1))
            .execute(&mut conn)
            .inspect_err(|e| error!("{e}"));
    }

    fn mark_as_dead(&self, nodee: &NodeId, conn: &mut SqliteConnection) {
        use crate::schema::node::dsl::*;

        let idd = nodee.0.to_vec();
        let _ = diesel::update(node)
            .filter(id.eq(idd))
            .set(removed.eq(true))
            .execute(conn)
            .inspect_err(|e| error!("{e}"));
    }

    fn put_to_bucket(&self, nodee: NodeInfo, conn: &mut SqliteConnection) {
        use crate::schema::node::dsl::*;

        let index = self.index(&nodee.id());
        let _ = insert_into(node)
            .values(crate::models::NodeRow {
                id: nodee.id().0.to_vec(),
                bucket: index,
                last_contacted: unix_timestmap_ms(),
                ip_addr: nodee.end_point().ip().to_string(),
                port: nodee.end_point().port() as i32,
                failed_requests: 0,
                removed: false,
            })
            .on_conflict_do_nothing()
            .execute(conn)
            .inspect_err(|e| error!("{e}"));
    }
}

pub fn update_last_sent(nodee: &NodeId, sent_timestamp: i64, conn: &mut SqliteConnection) {
    use crate::schema::node::dsl::*;

    let idd = nodee.0.to_vec();
    let _ = diesel::update(node)
        .set(last_sent.eq(sent_timestamp))
        .filter(id.eq(idd))
        .execute(conn)
        .inspect_err(|e| error!("{e}"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::SensibleOptions;
    use crate::dht::txn_id_generator::TxnIdGenerator;
    use tokio::net::UdpSocket;

    /// A RoutingTable over a single-connection in-memory sqlite pool (max_size 1 so every
    /// checkout shares the same in-memory database), with the same connection
    /// customizer as production (the custom `xor()` SQL function lives there).
    async fn test_routing_table(our_id: NodeId) -> RoutingTable {
        let manager = ConnectionManager::<SqliteConnection>::new(":memory:");
        let pool = Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(SensibleOptions))
            .build(manager)
            .unwrap();

        let mut conn = pool.get().unwrap();
        diesel::sql_query(
            "CREATE TABLE node (
                id BLOB PRIMARY KEY,
                bucket INTEGER NOT NULL,
                last_contacted BIGINT NOT NULL,
                ip_addr TEXT NOT NULL,
                port INTEGER NOT NULL,
                failed_requests INTEGER NOT NULL,
                removed BOOLEAN NOT NULL,
                last_sent BIGINT,
                added BIGINT NOT NULL DEFAULT 0
            )",
        )
        .execute(&mut *conn)
        .unwrap();

        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let broker = RpcManager::new(
            socket,
            pool.clone(),
            Arc::new(TxnIdGenerator::new()),
            Ipv4Addr::LOCALHOST,
        );
        let (_tx, rx) = mpsc::channel(1);
        RoutingTable::new(our_id, broker, pool, rx)
    }

    fn id_with_first_byte(b: u8) -> NodeId {
        let mut id = [0u8; 20];
        id[0] = b;
        NodeId(id)
    }

    fn addr(i: u8) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, i), 6881)
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
