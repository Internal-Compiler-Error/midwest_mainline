use std::time::Instant;
use num_bigint::BigUint;

pub type NodeId = BigUint;

pub struct RoutingTable {
    buckets: Vec<Bucket>,
}

pub struct Bucket {
    /// inclusive
    lower_bound: NodeId,
    /// exclusive
    upper_bound: BigNodeIdUint,

    // TODO: technically a bucket is at most 8 nodes, use a fixed size vector
    nodes: Vec<Node>,
}

pub struct Node {
    id: NodeId,
    last_checked: Instant,
}
