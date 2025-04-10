use bendy::encoding::ToBencode;
use num::{traits::ops::bytes, BigUint};
use smallvec::SmallVec;
use std::{fmt::Debug, net::SocketAddrV4};

const NODE_ID_LEN: usize = 20;
pub const ZERO_DIST: [u8; NODE_ID_LEN] = [0; NODE_ID_LEN];

#[derive(PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct NodeId(pub [u8; 20]);

impl Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use base64::prelude::*;
        // let hex = hex::encode(self.0);
        let str = BASE64_STANDARD.encode(self.0);
        write!(f, "{}", str)
    }
}

impl NodeId {
    /// Panics if the length is not exactly 20
    pub fn from_bytes_unchecked(bytes: &[u8]) -> Self {
        if bytes.len() != 20 {
            panic!("Node id must be exactly 20 bytes");
        }

        let mut arr = [0u8; 20];
        arr.copy_from_slice(bytes);
        NodeId(arr)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn dist_big_unit(&self, rhs: &Self) -> BigUint {
        let our_id = BigUint::from_bytes_be(self.as_bytes());
        let node_id = BigUint::from_bytes_be(rhs.as_bytes());
        our_id ^ node_id
    }

    pub fn dist(&self, rhs: &Self) -> [u8; 20] {
        let mut dist = [0u8; 20];
        for i in 0..20 {
            dist[i] = self.0[i] ^ rhs.0[i]
        }
        dist
    }
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
pub struct InfoHash(pub [u8; 20]);

impl InfoHash {
    pub fn from_bytes_unchecked(bytes: &[u8]) -> Self {
        if bytes.len() != 20 {
            panic!("Info hash must be exactly 20 bytes");
        }

        let mut arr = [0u8; 20];
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

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct PeerContact(pub SocketAddrV4);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct NodeInfo {
    id: NodeId,
    contact: PeerContact,
}

impl NodeInfo {
    pub fn new(id: NodeId, contact: PeerContact) -> NodeInfo {
        NodeInfo { id, contact }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn contact(&self) -> PeerContact {
        self.contact
    }
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct TransactionId(pub SmallVec<[u8; 2]>);

impl Debug for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use base64::prelude::*;
        // let hex = hex::encode(self.0);
        let str = BASE64_STANDARD.encode(&self.0);
        write!(f, "{}", str)
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
