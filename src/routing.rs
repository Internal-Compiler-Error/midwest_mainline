use crate::domain_knowledge::{self, NodeId, NodeInfo};
use std::time::Instant;
use tracing::info;

/// The routing table at the heart of the Kademlia DHT. It keep the near neighbors of ourself.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct RoutingTable {
    bucket_size: usize,
    /// The node id of the ourself.
    id: NodeId,

    pub(crate) buckets: Box<[Option<NodeEntry>]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct NodeEntry {
    pub(crate) contact: NodeInfo,
    pub(crate) last_checked: Instant,
}

impl RoutingTable {
    pub fn new(id: NodeId) -> Self {
        let bucket_size = 16;
        let mem: Vec<Option<NodeEntry>> = vec![None; 160 * bucket_size];
        RoutingTable {
            bucket_size,
            id,
            buckets: mem.into_boxed_slice(),
        }
    }

    pub fn node_count(&self) -> usize {
        // TODO: optimize this
        self.buckets.iter().filter(|n| n.is_some()).count()
    }

    /// Add a new node to the routing table, if the buckets are full, the node will be ignored.
    pub fn add_new_node(&mut self, contact: NodeInfo) {
        let size = self.node_count();
        info!("Adding new node to routing table of size: {size}");
        // there is a special case, when we already know this node, in that case, we just update the
        // last_checked timestamp.
        let exact_match = self
            .buckets
            .iter_mut()
            .flatten()
            .find(|node| node.contact.id() == contact.id());
        if let Some(n) = exact_match {
            n.last_checked = Instant::now();
            return;
        }

        let bucket = self.bucket_for_mut(&contact.id());
        let slot = bucket.iter_mut().find(|n| n.is_none());

        // TODO: I recall there is more sophisticated to whether to ignore the insertion or not
        match slot {
            Some(inner) => {
                inner.replace(NodeEntry {
                    contact,
                    last_checked: Instant::now(),
                });
                info!("{contact:?} added to routing table");
                return ();
            }
            None => {
                info!("table full, {contact:?} not added");
                return (); // we're full
            }
        }
    }

    // TODO: return an iterator instead?
    pub fn find_closest(&self, target: NodeId) -> Vec<NodeInfo> {
        let (bucket_i, bucket_i_end) = self.indices(&target);
        let bucket = &self.buckets[bucket_i..bucket_i_end];

        let mut valid_entries: Vec<_> = bucket.iter().flatten().collect();
        valid_entries.sort_unstable_by_key(|e| e.contact.id());

        valid_entries.into_iter().map(|n| n.contact).collect()
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
        // first byte with index `j`, meaning there are (19 - j) trailling non zeros bytes, each
        // bytes has 8 bits, so 2^(19 - j) <= i < 2(19 - j + 1). Furthermore, the first bit from
        // MSB within that non zero byte tell us how many additional bits we need to offset to from
        // 2^(19 - j)
        let dist = self.id.dist(&target);
        if dist == domain_knowledge::ZERO_DIST {
            // all zero means we're finding outself, then we go look for in the 0th bucket.
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
    fn bucket_for(&self, target: &NodeId) -> &[Option<NodeEntry>] {
        let (begin, end) = self.indices(target);
        &self.buckets[begin..end]
    }

    fn bucket_for_mut(&mut self, target: &NodeId) -> &mut [Option<NodeEntry>] {
        let (begin, end) = self.indices(target);
        &mut self.buckets[begin..end]
    }
}
