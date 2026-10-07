//! The shared vocabulary: node ids, info hashes, tokens, transaction ids.
//!
//! [`NodeId`] and [`InfoHash`] are both 160-bit values in the same key space — that is
//! what makes Kademlia work: "peers for this info hash" are stored at the nodes whose
//! ids are closest to the hash, by xor distance ([`NodeId::dist`], [`cmp_resp`]).

use bendy::encoding::ToBencode;
use num::traits::ops::bytes;
use smallvec::SmallVec;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    net::{IpAddr, SocketAddr},
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{models::NodeNoMetaInfo, utils::base64_enc};

pub const NODE_ID_LEN: usize = 20;
pub const ZERO_DIST: [u8; NODE_ID_LEN] = [0; NODE_ID_LEN];

#[derive(PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct NodeId(pub [u8; NODE_ID_LEN]);

impl Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use base64::prelude::*;
        let str = BASE64_STANDARD.encode(self.0);
        write!(f, "{}", str)
    }
}

impl NodeId {
    /// Returns `None` if the length is not exactly NODE_ID_LEN (e.g. malformed network input).
    pub fn try_from_bytes(bytes: &[u8]) -> Option<Self> {
        let arr: &[u8; NODE_ID_LEN] = bytes.try_into().ok()?;
        Some(NodeId(*arr))
    }

    /// Panics if the length is not exactly NODE_ID_LEN
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self::try_from_bytes(bytes)
            .unwrap_or_else(|| panic!("Node id must be exactly {NODE_ID_LEN} bytes got {} bytes", bytes.len()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn dist(&self, rhs: &Self) -> [u8; NODE_ID_LEN] {
        let mut dist = [0u8; NODE_ID_LEN];
        for ((d, a), b) in dist.iter_mut().zip(self.0).zip(rhs.0) {
            *d = a ^ b;
        }
        dist
    }
}

/// Compare the xor distance between `lhs` and `rhs` with respect the reference point
pub fn cmp_resp(lhs: &NodeId, rhs: &NodeId, reference: &NodeId) -> Ordering {
    let lhs_dist = reference.dist(lhs);
    let rhs_dist = reference.dist(rhs);

    lhs_dist.cmp(&rhs_dist)
}

impl ToBencode for NodeId {
    const MAX_DEPTH: usize = 0_usize;

    fn encode(&self, encoder: bendy::encoding::SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        encoder.emit_bytes(&self.0)
    }
}

#[derive(Hash, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable, KnownLayout)]
#[repr(C, packed)]
pub struct InfoHash(pub [u8; NODE_ID_LEN]);

impl InfoHash {
    /// Returns `None` if the length is not exactly NODE_ID_LEN (e.g. malformed network input).
    pub fn try_from_bytes(bytes: &[u8]) -> Option<Self> {
        let arr: &[u8; NODE_ID_LEN] = bytes.try_into().ok()?;
        Some(InfoHash(*arr))
    }

    /// Panics if `bytes` is not 20 bytes in length
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self::try_from_bytes(bytes).unwrap_or_else(|| {
            panic!(
                "Info hash must be exactly {NODE_ID_LEN} bytes got {} bytes",
                bytes.len()
            )
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// 40 hex digits, the way magnet links and trackers' web pages show it.
impl std::fmt::Display for InfoHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.iter().try_for_each(|b| write!(f, "{b:02x}"))
    }
}

impl Debug for InfoHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use base64::prelude::*;
        let str = BASE64_STANDARD.encode(self.0);
        write!(f, "{}", str)
    }
}

impl ToBencode for InfoHash {
    const MAX_DEPTH: usize = 0_usize;

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
        let str = BASE64_STANDARD.encode(&self.0);
        write!(f, "{}", str)
    }
}

impl ToBencode for Token {
    const MAX_DEPTH: usize = 0_usize;

    fn encode(&self, encoder: bendy::encoding::SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        encoder.emit_bytes(&self.0)
    }
}

/// Which of the two DHTs: BEP 32 runs IPv4 and IPv6 as independent networks, each with its
/// own routing table, sharing only the wire format.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub enum Family {
    V4,
    V6,
}

impl Family {
    pub fn of(addr: &SocketAddr) -> Family {
        Self::of_ip(&addr.ip())
    }

    pub fn of_ip(ip: &IpAddr) -> Family {
        match ip {
            IpAddr::V4(_) => Family::V4,
            IpAddr::V6(_) => Family::V6,
        }
    }

    pub fn other(self) -> Family {
        match self {
            Family::V4 => Family::V6,
            Family::V6 => Family::V4,
        }
    }

    /// how the `node` table's `family` column spells it
    pub(crate) fn db(self) -> i32 {
        match self {
            Family::V4 => 4,
            Family::V6 => 6,
        }
    }
}

impl Display for Family {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Family::V4 => "v4",
            Family::V6 => "v6",
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct NodeInfo {
    id: NodeId,
    end_point: SocketAddr,
}

impl NodeInfo {
    pub fn new(id: NodeId, end_point: impl Into<SocketAddr>) -> NodeInfo {
        NodeInfo {
            id,
            end_point: end_point.into(),
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn end_point(&self) -> SocketAddr {
        self.end_point
    }

    pub fn family(&self) -> Family {
        Family::of(&self.end_point)
    }
}

impl From<NodeNoMetaInfo> for NodeInfo {
    fn from(value: NodeNoMetaInfo) -> Self {
        let idd = NodeId::from_bytes(&value.id);
        let ip: IpAddr = value
            .ip_addr
            .parse()
            .unwrap_or_else(|_| panic!("invalid ip address in the node table: {}", value.ip_addr));
        NodeInfo::new(idd, SocketAddr::new(ip, value.port as u16))
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
        self.0.as_slice()
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
