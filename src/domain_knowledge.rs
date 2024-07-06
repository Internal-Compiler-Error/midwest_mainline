use bendy::encoding::ToBencode;
use num::traits::ops::bytes;
use smallvec::SmallVec;
use std::{fmt::Debug, net::SocketAddrV4};

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
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
    /// Panics if the lenght is not exactly 20
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
}

impl ToBencode for NodeId {
    const MAX_DEPTH: usize = 0 as usize;

    fn encode(&self, encoder: bendy::encoding::SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        encoder.emit_bytes(&self.0)
    }
}

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
    pub id: NodeId,
    pub contact: PeerContact,
}

impl NodeInfo {
    pub fn new(id: NodeId, contact: PeerContact) -> NodeInfo {
        NodeInfo { id, contact }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.id
    }

    pub fn contact(&self) -> &PeerContact {
        &self.contact
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
