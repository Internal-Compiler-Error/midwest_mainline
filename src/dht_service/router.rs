use std::{
    collections::HashMap,
    net::SocketAddrV4,
    sync::{Arc, RwLock},
    time::Duration,
    usize,
};

use tokio::{sync::mpsc, time::Instant};
use tracing::info;

use crate::{
    dht_service::dht_server::REQ_TIMEOUT,
    domain_knowledge::{self, NodeId, NodeInfo, PeerContact, TransactionId},
    message::Krpc,
};

use super::{message_broker::MessageBroker, transaction_id_pool::TransactionIdPool};

#[allow(unused)]
macro_rules! bail_on_err {
    ($result:expr) => {
        match $result {
            Ok(val) => val,
            Err(_e) => return,
        }
    };
}

macro_rules! bail_on_none {
    ($result:expr) => {
        match $result {
            Some(val) => val,
            None => return,
        }
    };
}

// invaraint: each bucket is sorted by quality?

/// The routing table at the heart of the Kademlia DHT. It keep the near neighbors of ourself.
#[derive(Debug, PartialEq, Eq, Clone)]
struct RoutingTable {
    bucket_size: usize,
    /// The node id of the ourself.
    id: NodeId,

    pub(crate) buckets: Box<[Option<NodeEntry>]>,
    // for index `i`, `last_refreshed[i]` is the last time anything was modified about this bucket
    last_refreshed: [Instant; 160],

    id_to_ind: HashMap<NodeId, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
struct NodeEntry {
    pub(crate) contact: NodeInfo,
    pub(crate) last_checked: Instant,
    /// if current time - last_checked >= 15 mins, how many requests this node *didn't* respond?
    failed_request_count: u8,
    quality: Quality,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
enum Quality {
    Good,
    #[allow(unused)]
    Questionable(u8),
    #[allow(unused)]
    Bad,
}

impl NodeEntry {
    pub fn new(contact: NodeInfo) -> Self {
        Self {
            contact,
            last_checked: Instant::now(),
            failed_request_count: 0,
            quality: Quality::Good,
        }
    }
    pub fn extract(&self) -> NodeInfo {
        self.contact
    }

    pub fn expired(&self) -> bool {
        let now = Instant::now();
        const EXPIRING: Duration = Duration::from_secs(60 * 15); // 15 mins

        now.saturating_duration_since(self.last_checked) >= EXPIRING
    }
    pub fn quality(&self) -> Quality {
        let expired = self.expired();
        let failed_count = self.failed_request_count;

        match (expired, failed_count) {
            (false, _) if failed_count == 0 => Quality::Good,
            (false, count) => Quality::Questionable(count),
            _ => Quality::Bad,
        }
    }

    pub fn update_quality(&mut self, failed_request: bool) {
        self.last_checked = Instant::now();
        if failed_request {
            self.failed_request_count = self.failed_request_count.saturating_add(1);
        } else {
            self.failed_request_count = self.failed_request_count.saturating_sub(1);
            self.quality = Quality::Questionable(self.failed_request_count);
        }
    }
}

impl RoutingTable {
    pub fn new(id: NodeId) -> Self {
        let bucket_size = 16;
        let mem: Vec<Option<NodeEntry>> = vec![None; 160 * bucket_size];
        RoutingTable {
            bucket_size,
            id,
            buckets: mem.into_boxed_slice(),
            last_refreshed: [Instant::now(); 160],
            id_to_ind: HashMap::new(),
        }
    }

    // if we want to insert, what should the insertion point be
    fn insertion_candidate_mut(&mut self, replacement: &NodeId) -> Option<&mut Option<NodeEntry>> {
        let begin = self.index(replacement);
        let kbucket = &mut self.buckets[begin..begin + self.bucket_size];
        let empty_slot = kbucket.iter_mut().find(|n| n.is_none());
        empty_slot
    }

    // if we want to replace, what should the replacement point be
    fn replacement_candidate_mut(&mut self, replacement: &NodeId) -> Option<&mut Option<NodeEntry>> {
        let begin = self.index(replacement);
        let kbucket = &mut self.buckets[begin..begin + self.bucket_size];
        let empty_slot = kbucket.iter_mut().find(|n| n.is_none());
        match empty_slot {
            Some(_) => None,
            None => kbucket.last_mut(),
        }
    }

    #[allow(unused)]
    fn replacement_candidate(&self, replacement: &NodeId) -> Option<&Option<NodeEntry>> {
        let begin = self.index(replacement);
        let kbucket = &self.buckets[begin..begin + self.bucket_size];
        let empty_slot = kbucket.iter().find(|n| n.is_none());
        match empty_slot {
            Some(_) => None,
            None => kbucket.last(),
        }
    }

    pub fn node_count(&self) -> usize {
        // TODO: optimize this
        self.buckets.iter().filter(|n| n.is_some()).count()
    }

    pub fn find_closest(&self, target: NodeId) -> Vec<&NodeEntry> {
        let (bucket_i, bucket_i_end) = self.indices(&target);
        let bucket = &self.buckets[bucket_i..bucket_i_end];

        let mut valid_entries: Vec<_> = bucket.iter().flatten().collect();
        valid_entries.sort_unstable_by_key(|e| e.contact.id());

        valid_entries.into_iter().collect()
    }

    pub fn find(&self, target: NodeId) -> Option<NodeInfo> {
        self.buckets
            .iter()
            .flatten()
            .find(|node| node.contact.id() == target)
            .map(|n| n.contact)
            .clone()
    }

    /// Returns the `index` where `self.buckets[index]` is the first entry in the corresponding
    /// k-bucket, and `index + (self.bucket_size - 1)` is the last entry (inclusive), so
    /// `index..(index + self.bucket_size)` is the valid range.
    fn index(&self, target: &NodeId) -> usize {
        // Each k-bucket at index i stores nodes of distance [2^i, 2^(i + 1)) from ourself. The
        // first byte with index `j`, meaning there are (19 - j) trailing non zeros bytes, each
        // bytes has 8 bits, so 2^(19 - j) <= i < 2(19 - j + 1). Furthermore, the first bit from
        // MSB within that non zero byte tell us how many additional bits we need to offset to from
        // 2^(19 - j)
        let dist = self.id.dist(&target);
        if dist == domain_knowledge::ZERO_DIST {
            // all zero means we're finding ourself, then we go look for in the 0th bucket.
            return 0;
        }

        let first_nonzero = dist.into_iter().position(|radix| radix != 0).unwrap();
        let byte = dist[first_nonzero];
        let leading_zeros_inx = 7 - byte.leading_zeros() as usize;

        let bucket_idx = (19 - first_nonzero) * 8 + leading_zeros_inx;
        bucket_idx * self.bucket_size
    }

    /// [begin, end) range of the corresponding k-bucket for `target`, the range should be accessed
    /// directly like `self.buckets[begin]`, the stride jumping is already done for you.
    fn indices(&self, target: &NodeId) -> (usize, usize) {
        let begin = self.index(target);
        let end = begin + self.bucket_size;
        (begin, end)
    }

    #[allow(unused)]
    fn bucket_for(&self, target: &NodeId) -> (&[Option<NodeEntry>], usize) {
        let (begin, end) = self.indices(target);
        (&self.buckets[begin..end], begin)
    }
}

#[derive(Debug, Clone)]
/// A Router will tell you who are the closest nodes that we know
pub struct Router {
    id: NodeId,
    routing_table: Arc<std::sync::RwLock<RoutingTable>>,
    message_broker: MessageBroker,
    transaction_id_pool: Arc<TransactionIdPool>,
}

async fn send(
    message_broker: MessageBroker,
    txid: TransactionId,
    node: &NodeInfo,
    quering_id: NodeId,
    timeout: Duration,
) -> Option<(Krpc, SocketAddrV4)> {
    let rx = message_broker.subscribe_one(txid.clone());
    message_broker.send_msg_to_node(Krpc::new_ping_query(txid, quering_id), node);

    tokio::time::timeout(timeout, async move { rx.await.ok() })
        .await
        .ok()
        .flatten()
}

impl Router {
    pub fn new(id: NodeId, message_broker: MessageBroker, transaction_id_pool: Arc<TransactionIdPool>) -> Router {
        Router {
            id,
            routing_table: Arc::new(RwLock::new(RoutingTable::new(id))),
            message_broker,
            transaction_id_pool,
        }
    }

    /// keep listening for all incoming responses and update our table
    pub async fn run(&self, mut inbound: mpsc::Receiver<(Krpc, SocketAddrV4)>) {
        loop {
            // TODO: worry about the unwrap later
            let (msg, origin) = inbound.recv().await.unwrap();

            let msg = match msg {
                Krpc::FindNodeGetPeersResponse(msg) => msg,
                _ => break,
            };

            {
                let nodes = msg.nodes();

                for node in nodes {
                    let node = node.clone();
                    self.add(node.id(), origin);
                }
            }
        }
    }

    pub fn find_closest(&self, target: NodeId) -> Vec<NodeInfo> {
        let routing_table = self.routing_table.read().unwrap();
        routing_table.find_closest(target).iter().map(|n| n.extract()).collect()
    }

    pub fn find_exact(&self, target: NodeId) -> Option<NodeInfo> {
        let routing_table = self.routing_table.read().unwrap();
        routing_table.find(target)
    }

    /// Add a new node to the routing table, if the buckets are full, the node will be ignored.
    pub fn add(&self, new_node_id: NodeId, addr: SocketAddrV4) {
        let this = self.clone();
        let work = async move {
            // None: has empty slots, should insert
            // Some(good): only update, don't insert
            // Some(bad): replace it

            // invariant: kbuckets are always sorted by quality
            let node_info = NodeInfo::new(new_node_id, PeerContact(addr));
            let replacment = NodeEntry::new(node_info);
            let to_contact = {
                let mut routing_table = this.routing_table.write().unwrap();
                let slot = routing_table.replacement_candidate_mut(&new_node_id);
                match slot {
                    Some(inner) => {
                        let inner = inner.as_mut().unwrap();
                        if update_only(inner, &new_node_id) {
                            inner.update_quality(false);
                            return;
                        } else {
                            inner.contact
                        }
                    }
                    None => return,
                }
            };

            let message_broker = this.message_broker.clone();
            let quering_id = this.id;
            let timeout = REQ_TIMEOUT;

            let txid = this.transaction_id_pool.next();
            let txid = TransactionId::from(txid);
            let ttxid = txid.clone();
            let ping_response = send(message_broker, ttxid, &to_contact, quering_id, timeout).await;
            {
                let mut routing_table = this.routing_table.write().unwrap();
                let slot = routing_table.replacement_candidate_mut(&new_node_id);

                let slot = match slot {
                    Some(inner) => inner.as_mut().unwrap(),
                    None => {
                        let insertion_point = routing_table.insertion_candidate_mut(&new_node_id).unwrap();
                        *insertion_point = Some(replacment);
                        return;
                    }
                };

                // SIMPLIFIED design: if the existing node doens't reply, we'll just replace it imediately,
                // otherwise update
                match ping_response {
                    // TODO: handle when the response is an error message
                    Some((_response, _origin)) => {
                        slot.update_quality(false);
                    }
                    None => {
                        let insertion_point = routing_table.insertion_candidate_mut(&new_node_id).unwrap();
                        *insertion_point = Some(replacment);
                        return;
                    }
                }
            }
        };
        tokio::spawn(work);
    }

    pub fn node_count(&self) -> usize {
        self.routing_table.read().unwrap().node_count()
    }
}

fn update_only(old: &NodeEntry, new: &NodeId) -> bool {
    let old_id = old.extract().id();
    old_id == *new
}
