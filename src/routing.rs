use crate::domain_knowledge::{NodeId, NodeInfo};
use num::BigUint;
use std::{ops::BitXor, str::FromStr, time::Instant};
use tracing::{info, trace};

/// The routing table at the heart of the Kademlia DHT. It keep the near neighbors of ourself.
#[derive(Debug)]
pub struct RoutingTable {
    /// The node id of the ourself.
    id: BigUint,

    /// each bucket contains
    pub(crate) buckets: Vec<Bucket>,
}

#[derive(Debug)]
pub struct Bucket {
    /// inclusive
    lower_bound: BigUint,
    /// exclusive
    upper_bound: BigUint,

    // TODO: technically a bucket is at most 8 nodes, use a fixed size vector
    nodes: Vec<Node>,
}

impl Bucket {
    pub fn full(&self) -> bool {
        assert!(self.nodes.len() <= 8);
        self.nodes.len() >= 8
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
// TODO: this name is shit, think of a better one
pub struct Node {
    pub(crate) contact: NodeInfo,
    pub(crate) last_checked: Instant,
}

impl RoutingTable {
    pub fn new(id: NodeId) -> Self {
        let default_bucket = Bucket {
            lower_bound: BigUint::from(0u8),
            // 2^160
            upper_bound: BigUint::from_str("1461501637330902918203684832716283019655932542976").unwrap(),
            nodes: Vec::new(),
        };

        RoutingTable {
            id: BigUint::from_bytes_be(id.as_bytes()),
            buckets: vec![default_bucket],
        }
    }

    pub fn node_count(&self) -> usize {
        self.buckets.iter().map(|b| b.nodes.len()).sum()
    }

    /// Add a new node to the routing table, if the buckets are full, the node will be ignored.
    pub fn add_new_node(&mut self, contact: NodeInfo) {
        // TODO: handle duplicate nodes

        // there is a special case, when we already know this node, in that case, we just update the
        // last_checked timestamp.
        if let Some(node) = self
            .buckets
            .iter_mut()
            .map(|b| b.nodes.iter_mut())
            .flatten()
            .find(|node| node.contact.node_id() == contact.node_id())
        {
            node.last_checked = Instant::now();
            return;
        }

        let our_id = &self.id;
        let distance = our_id.bitxor(BigUint::from_bytes_be(contact.node_id().as_bytes()));

        // first, find the bucket that this node belongs in
        let target_bucket = self
            .buckets
            .iter_mut()
            .find(|bucket| bucket.lower_bound <= distance && distance < bucket.upper_bound)
            .unwrap();

        let (full, within_our_bucket) = (
            target_bucket.full(),
            &target_bucket.lower_bound <= our_id && our_id < &target_bucket.upper_bound,
        );
        match (full, within_our_bucket) {
            // if the bucket is full and our id is within our bucket, we need to split it
            (true, true) => {
                // split the bucket, the new bucket is the upper half of the old bucket
                let mut new_bucket = Bucket {
                    lower_bound: &target_bucket.upper_bound / 2u8,
                    upper_bound: target_bucket.upper_bound.clone(),
                    nodes: Vec::new(),
                };

                // transfer all the nodes that should go into the new bucket into the right place
                // do I prefer the draining_filter API? yes but that's sadly nightly only
                let mut i = 0;
                while i < target_bucket.nodes.len() {
                    let target_bucket_node_id =
                        BigUint::from_bytes_be(target_bucket.nodes[i].contact.node_id().as_bytes());
                    if &target_bucket_node_id <= &new_bucket.lower_bound {
                        let node = target_bucket.nodes.remove(i);
                        new_bucket.nodes.push(node);
                    } else {
                        i += 1;
                    }
                }

                target_bucket.upper_bound = &target_bucket.upper_bound / 2u8;
                self.buckets.push(new_bucket);
                trace!("bucket split");
            }
            // if the bucket id range is not within our id and the bucket is full, we don't need to do
            // anything
            (true, false) => {
                trace!("node not added, bucket full and not within our id");
            }
            // if the buckets are not full, then happy days, we just add the new node
            (false, _) => {
                target_bucket.nodes.push(Node {
                    contact,
                    last_checked: Instant::now(),
                });
                trace!("node added");
            }
        }
        info!("node processed, node count: {}", self.node_count());
    }

    pub fn find_closest(&self, target: NodeId) -> Vec<NodeInfo> {
        let mut closest_nodes: Vec<_> = self
            .buckets
            .iter()
            .map(|bucket| {
                bucket.nodes.iter().map(|node| {
                    let node_id = node.contact.node_id();
                    let node_id = node_id.as_bytes();
                    let target = target.as_bytes();

                    let mut distance = [0u8; 20];

                    // zip for array is sadly unstable
                    let mut i = 0;
                    while i < 20 {
                        distance[i] = node_id[i] ^ target[i];
                        i += 1;
                    }

                    (BigUint::from_bytes_be(&distance), &node.contact)
                })
            })
            .flatten()
            .collect();

        closest_nodes.sort_unstable_by_key(|x| x.0.clone());
        closest_nodes
            .iter()
            .filter(|(_, node)| node.node_id() != target)
            .take(8)
            .map(|x| x.1)
            .cloned()
            .collect()
    }

    pub fn find(&self, target: NodeId) -> Option<Node> {
        self.buckets
            .iter()
            .map(|bucket| bucket.nodes.iter())
            .flatten()
            .find(|node| node.contact.node_id() == target)
            .cloned()
    }
}
