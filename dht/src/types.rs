use bendy::encoding::ToBencode;
use num::traits::ops::bytes;
use smallvec::SmallVec;
use std::{
    cmp::Ordering,
    fmt::Debug,
    net::{Ipv4Addr, SocketAddrV4},
};

use crate::{dht::krpc_broker::Routable, models::NodeNoMetaInfo, utils::base64_enc};

pub const NODE_ID_LEN: usize = 20;
pub const ZERO_DIST: [u8; NODE_ID_LEN] = [0; NODE_ID_LEN];
pub const MAX_DIST: [u8; NODE_ID_LEN] = [u8::MAX; NODE_ID_LEN];

#[derive(PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct NodeId(pub [u8; NODE_ID_LEN]);

impl Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use base64::prelude::*;
        // let hex = hex::encode(self.0);
        let str = BASE64_STANDARD.encode(self.0);
        write!(f, "{}", str)
    }
}

impl NodeId {
    /// Panics if the length is not exactly NODE_ID_LEN
    pub fn from_bytes(bytes: &[u8]) -> Self {
        if bytes.len() != NODE_ID_LEN {
            panic!("Node id must be exactly {NODE_ID_LEN} bytes got {} bytes", bytes.len());
        }

        let mut arr = [0u8; NODE_ID_LEN];
        arr.copy_from_slice(bytes);
        NodeId(arr)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn dist(&self, rhs: &Self) -> [u8; NODE_ID_LEN] {
        let mut dist = [0u8; NODE_ID_LEN];
        for i in 0..NODE_ID_LEN {
            dist[i] = self.0[i] ^ rhs.0[i]
        }
        dist
    }
}

/// Compare the xor distance between `lhs` and `rhs` with respect the reference point
pub fn cmp_resp(lhs: &NodeId, rhs: &NodeId, reference: &NodeId) -> Ordering {
    let lhs_dist = reference.dist(&lhs);
    let rhs_dist = reference.dist(&rhs);

    lhs_dist.cmp(&rhs_dist)
}

impl ToBencode for NodeId {
    const MAX_DEPTH: usize = 0 as usize;

    fn encode(&self, encoder: bendy::encoding::SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        encoder.emit_bytes(&self.0)
    }
}

// impl PartialOrd for NodeId {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         for (lhs, rhs) in self.0.iter().zip(other.0.iter()) {
//             match lhs.cmp(rhs) {
//                 Ordering::Equal => continue,
//                 Ordering::Less => return Some(Ordering::Less),
//                 Ordering::Greater => return Some(Ordering::Greater),
//             }
//         }
//         Some(Ordering::Equal)
//     }
// }
//
// impl Ord for NodeId {
//     fn cmp(&self, other: &Self) -> Ordering {
//         self.partial_cmp(other).unwrap()
//     }
// }

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct InfoHash(pub [u8; NODE_ID_LEN]);

impl InfoHash {
    /// Panics if `bytes` is not 20 bytes in length
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() == NODE_ID_LEN);

        let mut arr = [0u8; NODE_ID_LEN];
        arr.copy_from_slice(bytes);
        InfoHash(arr)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for InfoHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use base64::prelude::*;
        // let hex = hex::encode(self.0);
        let str = BASE64_STANDARD.encode(self.0);
        write!(f, "{}", str)
    }
}

impl ToBencode for InfoHash {
    const MAX_DEPTH: usize = 0 as usize;

    fn encode(&self, encoder: bendy::encoding::SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        encoder.emit_bytes(&self.0)
    }
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct Token(pub SmallVec<[u8; 10]>); // 10 is purely based on vibes

impl Token {
    pub fn from_bytes(bytes: &[u8]) -> Token {
        let vec = SmallVec::from_slice(bytes);
        Token(vec)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use base64::prelude::*;
        // let hex = hex::encode(self.0);
        let str = BASE64_STANDARD.encode(&self.0);
        write!(f, "{}", str)
    }
}

impl ToBencode for Token {
    const MAX_DEPTH: usize = 0 as usize;

    fn encode(&self, encoder: bendy::encoding::SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        encoder.emit_bytes(&self.0)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct NodeInfo {
    id: NodeId,
    end_point: SocketAddrV4,
}

impl NodeInfo {
    pub fn new(id: NodeId, end_point: SocketAddrV4) -> NodeInfo {
        NodeInfo { id, end_point }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn end_point(&self) -> SocketAddrV4 {
        self.end_point
    }
}

impl Routable for NodeInfo {
    fn endpoint(&self) -> SocketAddrV4 {
        self.end_point()
    }
}

impl From<NodeNoMetaInfo> for NodeInfo {
    fn from(value: NodeNoMetaInfo) -> Self {
        let idd = NodeId::from_bytes(&*value.id);
        // TODO: add err msg
        let ip: Ipv4Addr = value.ip_addr.parse().unwrap();
        let portt = value.port;
        let endpoint = SocketAddrV4::new(ip, portt as u16);
        NodeInfo::new(idd, endpoint)
    }
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct TransactionId(pub SmallVec<[u8; 2]>);

impl Debug for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", base64_enc(&self.0))
    }
}

impl TransactionId {
    pub fn from_bytes(bytes: &[u8]) -> TransactionId {
        let vec = SmallVec::from_slice(bytes);
        TransactionId(vec)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0.as_slice()
    }
}

impl<T, const N: usize> From<T> for TransactionId
where
    T: num::Integer + bytes::ToBytes<Bytes = [u8; N]>,
{
    fn from(value: T) -> Self {
        TransactionId::from_bytes(&value.to_be_bytes())
    }
}
