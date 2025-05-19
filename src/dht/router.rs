use std::net::Ipv4Addr;
use std::{net::SocketAddrV4, sync::Arc, usize};

use diesel::r2d2::PooledConnection;
use diesel::{insert_into, prelude::*};
use diesel::{
    r2d2::{ConnectionManager, Pool},
    ExpressionMethods, SqliteConnection,
};
use futures::future::join_all;
use tokio::sync::mpsc;
use tracing::info;

use crate::models::NodeNoMetaInfo;
use crate::utils::unix_timestmap_ms;
use crate::{
    types::{self, NodeId, NodeInfo, TransactionId},
    message::Krpc,
};

use super::dht_handle::REQ_TIMEOUT;
use super::{krpc_broker::MessageBroker, transaction_id_pool::TransactionIdPool};

#[allow(unused)]
macro_rules! bail_on_err {
    ($result:expr) => {
        match $result {
            Ok(val) => val,
            Err(_e) => return,
        }
    };
}

#[allow(unused)]
macro_rules! bail_on_none {
    ($result:expr) => {
        match $result {
            Some(val) => val,
            None => return,
        }
    };
}

#[derive(Debug, Clone)]
/// A Router will tell you who are the closest nodes that we know
pub struct Router {
    id: NodeId,
    table: Pool<ConnectionManager<SqliteConnection>>,
    message_broker: MessageBroker,
    transaction_id_pool: Arc<TransactionIdPool>,
    bucket_size: usize,
}

impl Router {
    pub fn new(
        id: NodeId,
        message_broker: MessageBroker,
        transaction_id_pool: Arc<TransactionIdPool>,
        table: Pool<ConnectionManager<SqliteConnection>>,
    ) -> Router {
        Router {
            id,
            table,
            message_broker,
            transaction_id_pool,
            bucket_size: 1024, // TODO: make this configurable in the future
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

        self.add(node_id, from);
        info!("Add {from} with {:?} to the routing table", node_id);

        // if it's response from find_peers or get_nodes, they have additional info
        if let Krpc::FindNodeGetPeersResponse(res) = message {
            for node in res.nodes() {
                // TODO: this is a bit stupid as we destroy the structure just to copy but fix later
                self.add(node.id(), node.end_point());
            }
        }
    }

    /// keep listening for all incoming responses and update our table
    pub async fn run(&self, mut inbound: mpsc::Receiver<(Krpc, SocketAddrV4)>) {
        loop {
            // TODO: worry about the unwrap later
            let (msg, origin) = inbound.recv().await.unwrap();

            self.add_new_nodes(origin, &msg);
        }
    }

    /// Returns the bucket index that the target node belongs in
    fn index(&self, target: &NodeId) -> i32 {
        let dist = self.id.dist(&target);
        if dist == types::ZERO_DIST {
            // all zero means we're finding ourself, then we go look for in the 0th bucket.
            return 0;
        }

        let first_nonzero_byte = dist.into_iter().position(|radix| radix != 0).unwrap();
        let byte = dist[first_nonzero_byte];
        let leading_zeros_inx = 7 - byte.leading_zeros() as usize;

        let bucket_idx = (19 - first_nonzero_byte) * 8 + leading_zeros_inx;
        bucket_idx.try_into().unwrap()
    }

    pub fn find_closest(&self, target: NodeId) -> Vec<NodeInfo> {
        use crate::schema::node::dsl::*;
        let mut conn = self.table.get().unwrap();
        let nodes = node
            .filter(bucket.eq(self.index(&target)))
            .order(last_contacted.desc())
            .limit(64)
            .select(NodeNoMetaInfo::as_select())
            .load(&mut conn)
            .unwrap();

        nodes.into_iter().map(|nodee| nodee.into()).collect::<Vec<_>>()
    }

    pub fn find_exact(&self, target: NodeId) -> Option<NodeInfo> {
        use crate::schema::node::dsl::*;
        let mut conn = self.table.get().unwrap();
        let target_id = target.0.to_vec();
        node.filter(id.eq(target_id))
            .select(NodeNoMetaInfo::as_select())
            .first(&mut conn)
            .ok()
            .map(|nodee| nodee.into())
    }

    /// Add a new node to the routing table, if the buckets are full, the node will be ignored.
    pub fn add(&self, new_node_id: NodeId, addr: SocketAddrV4) {
        let this = self.clone();
        // let node_info = NodeInfo::new(new_node_id, addr);

        // TODO: find_exact will request another connection from the pool..., should be fine
        // for now
        let exact = self.find_exact(new_node_id);
        match exact {
            Some(_node) => {
                // Only need to update the quality metric, no IO needed
                let mut conn = self.conn();
                self.marks_as_good(&new_node_id, &mut conn);
                return;
            }
            None => {
                // Need to ping the worst quality node in the bucket to see if it should be replaced
            }
        }

        let work = async move {
            let node_info = NodeInfo::new(new_node_id, addr);
            // let replacment = NodeEntry::new(node_info);

            // instead of going from least recently seen and probe one by one, just refresh the
            // entire bucket
            let bucket_idx = this.index(&new_node_id);
            this.refresh_bucket(bucket_idx).await;

            let bucket_count = this.bucket_count(bucket_idx);
            // all nodes are good, no slots left for new node
            if bucket_count >= this.bucket_size {
                return;
            }
            let mut conn = this.conn();
            this.append_to_bucket(node_info, &mut conn);
        };
        tokio::spawn(work);
    }

    fn conn(&self) -> PooledConnection<ConnectionManager<SqliteConnection>> {
        self.table.get().unwrap()
    }
    pub fn node_count(&self) -> usize {
        use crate::schema::node::dsl::*;
        let mut conn = self.conn();
        let count: i64 = node.filter(removed.eq(false)).count().get_result(&mut conn).unwrap();
        count as usize
    }

    /// How many nodes are in the ith bucket (0-indexed)
    pub fn bucket_count(&self, i: i32) -> usize {
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
        let txn_id = self.transaction_id_pool.next();
        let ping_msg = Krpc::new_ping_query(TransactionId::from(txn_id), self.id);

        {
            let mut conn = self.conn();
            update_last_sent(&target.id(), unix_timestmap_ms(), &mut conn);
        }

        let response = self
            .message_broker
            .send_and_wait_timeout(ping_msg, target.end_point(), REQ_TIMEOUT)
            .await;
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

    pub async fn refresh_table_loop(&self) {}

    // TODO: use AsRef or Into to make it take in anything that can turn into an ID
    fn marks_as_good(&self, nodee: &NodeId, conn: &mut SqliteConnection) {
        use crate::schema::node::dsl::*;
        let now = unix_timestmap_ms();
        // let mut conn = self.table.get().unwrap();
        let idd = nodee.0.to_vec();
        diesel::update(node)
            .filter(id.eq(idd))
            .set((last_contacted.eq(now), failed_requests.eq(0)))
            .execute(conn);
    }

    fn mark_as_dead(&self, nodee: &NodeId, conn: &mut SqliteConnection) {
        use crate::schema::node::dsl::*;

        let idd = nodee.0.to_vec();
        diesel::update(node)
            .filter(id.eq(idd))
            .set(removed.eq(true))
            .execute(conn);
    }

    fn append_to_bucket(&self, nodee: NodeInfo, conn: &mut SqliteConnection) {
        use crate::schema::node::dsl::*;

        insert_into(node)
            .values(crate::models::Node {
                id: nodee.id().0.to_vec(),
                last_contacted: unix_timestmap_ms(),
                ip_addr: nodee.end_point().ip().to_string(),
                port: nodee.end_point().port() as i32,
                failed_requests: 0,
                removed: false,
            })
            .execute(conn);
    }
}

pub fn update_last_sent(nodee: &NodeId, sent_timestamp: i64, conn: &mut SqliteConnection) {
    use crate::schema::node::dsl::*;

    let idd = nodee.0.to_vec();
    diesel::update(node)
        .set(last_sent.eq(sent_timestamp))
        .filter(id.eq(idd))
        .execute(conn);
}
