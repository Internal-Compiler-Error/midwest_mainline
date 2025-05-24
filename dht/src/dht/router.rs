use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{net::SocketAddrV4, usize};

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
use crate::models::NodeNoMetaInfo;
use crate::types::TXN_ID_PLACEHOLDER;
use crate::utils::unix_timestmap_ms;
use crate::{
    message::Krpc,
    types::{self, NodeId, NodeInfo},
};

use super::dht_handle::REQ_TIMEOUT;
use super::krpc_broker::KrpcBroker;

#[derive(Debug, Clone)]
/// A Router will tell you who are the closest nodes that we know
pub struct Router {
    id: NodeId,
    table: Pool<ConnectionManager<SqliteConnection>>,
    message_broker: KrpcBroker,
    bucket_size: usize,
    inbound_messages: Arc<Mutex<Option<mpsc::Receiver<(Krpc, SocketAddrV4)>>>>,
}

impl Router {
    pub fn new(
        id: NodeId,
        message_broker: KrpcBroker,
        table: Pool<ConnectionManager<SqliteConnection>>,
        inbound_messages: mpsc::Receiver<(Krpc, SocketAddrV4)>,
    ) -> Router {
        Router {
            id,
            table,
            message_broker,
            bucket_size: 1024, // TODO: make this configurable in the future
            inbound_messages: Arc::new(Mutex::new(Some(inbound_messages))),
        }
    }

    fn add_new_nodes(&self, from: SocketAddrV4, message: &Krpc) {
        if let Krpc::ErrorResponse(_) = message {
            return;
        }

        let node_id = match message {
            Krpc::AnnouncePeerQuery(announce_peer_query) => *announce_peer_query.querier(),
            Krpc::FindNodeQuery(find_node_query) => find_node_query.querier(),
            Krpc::GetPeersQuery(get_peers_query) => *get_peers_query.querier(),
            Krpc::PingQuery(ping_query) => *ping_query.querier(),
            Krpc::PingAnnouncePeerResponse(ping_announce_peer_response) => *ping_announce_peer_response.target_id(),
            Krpc::FindNodeGetPeersResponse(find_node_get_peers_response) => *find_node_get_peers_response.queried(),
            Krpc::ErrorResponse(_) => unreachable!("errors should get early returned"),
        };

        {
            let mut conn = self.conn();
            self.marks_as_good(&node_id, &mut conn);
        }
        self.add(node_id, from);

        // if it's response from find_peers or get_nodes, they have additional info
        if let Krpc::FindNodeGetPeersResponse(res) = message {
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
                .expect("run for Router is only called once")
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
        let dist = self.id.dist(&target);
        if dist == types::ZERO_DIST {
            // all zero means we're finding ourself, then we go look for in the last bucket
            return 159;
        }

        let first_nonzero_byte = dist.into_iter().position(|radix| radix != 0).unwrap();
        let byte = dist[first_nonzero_byte];

        let bucket_idx = 159 - (first_nonzero_byte * 8 + byte.leading_zeros() as usize);
        bucket_idx.try_into().unwrap()
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

        let target_idx = self.index(&self.id);
        let mut closest = self.closest_in_bucket(&target, target_idx, total.into(), &mut conn);

        let mut offset = 1;
        let mut remaining: u16 = total.saturating_sub(closest.len().try_into().expect("we spcified the limit"));
        loop {
            // if got we wanted, or both sides are out of bounds, then there's no more we can do
            if remaining == 0 || (target_idx - offset < 0 && target_idx + offset >= 256) {
                break;
            }

            // favours the nodes closer to us, i.e buckets with larger index, I have no theory on
            // why this might be better
            let right_bucket = target_idx + offset;
            if remaining != 0 && right_bucket < 256 {
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

        closest.sort_unstable_by(|a, b| {
            let a_dist = &self.id.dist(&a.id());
            let b_dist = &self.id.dist(&b.id());

            a_dist.cmp(b_dist)
        });

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
    fn replacement_queue(&self, i: i32, conn: &mut SqliteConnection) -> Vec<crate::models::Node> {
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
            .select(crate::models::Node::as_select())
            .get_results(conn)
            .unwrap()
    }

    // Send a ping to fresh the node
    async fn refresh_node(&self, target: &NodeInfo) {
        let ping_msg = Krpc::new_ping_query(TXN_ID_PLACEHOLDER, self.id);

        {
            let mut conn = self.conn();
            update_last_sent(&target.id(), unix_timestmap_ms(), &mut conn);
        }

        let response = self.message_broker.query(ping_msg, target, REQ_TIMEOUT).await;
        let mut conn = self.conn();
        match response {
            Ok(_) => self.marks_as_good(&target.id(), &mut conn),
            Err(_) => self.mark_as_dead(&target.id(), &mut conn),
        }
    }

    async fn refresh_bucket(&self, i: i32) {
        let mut conn = self.conn();
        let problematics = self.replacement_queue(i, &mut conn);

        let tasks = problematics.into_iter().map(|n| async move {
            let id = NodeId::from_bytes_unchecked(&*n.id);
            let ip: Ipv4Addr = n.ip_addr.parse().unwrap();
            let endpoint = SocketAddrV4::new(ip, n.port as u16);
            let target = NodeInfo::new(id, endpoint);
            self.refresh_node(&target).await
        });
        join_all(tasks).await;
    }

    pub async fn refresh_table(&self) {
        let tasks = (0..20).map(|i| async move { self.refresh_bucket(i).await });
        for i in 0..20 {
            self.refresh_bucket(i).await;
        }
        join_all(tasks).await;
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
            .values(crate::models::Node {
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
