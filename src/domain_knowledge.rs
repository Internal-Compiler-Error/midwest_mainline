use bendy::encoding::ToBencode;
use smallvec::SmallVec;
use std::net::SocketAddrV4;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct BetterNodeId(pub [u8; 20]);

impl BetterNodeId {
    /// Panics if the lenght is not exactly 20
    pub fn from_bytes_unchecked(bytes: &[u8]) -> Self {
        if bytes.len() != 20 {
            panic!("Node id must be exactly 20 bytes");
        }

        let mut arr = [0u8; 20];
        arr.copy_from_slice(bytes);
        BetterNodeId(arr)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl ToBencode for BetterNodeId {
    const MAX_DEPTH: usize = 0 as usize;

    fn encode(&self, encoder: bendy::encoding::SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        encoder.emit_bytes(&self.0)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct BetterInfoHash(pub [u8; 20]);

impl BetterInfoHash {
    pub fn from_bytes_unchecked(bytes: &[u8]) -> Self {
        if bytes.len() != 20 {
            panic!("Node id must be exactly 20 bytes");
        }

        let mut arr = [0u8; 20];
        arr.copy_from_slice(bytes);
        BetterInfoHash(arr)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl ToBencode for BetterInfoHash {
    const MAX_DEPTH: usize = 0 as usize;

    fn encode(&self, encoder: bendy::encoding::SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        encoder.emit_bytes(&self.0)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterCompactPeerContact(pub SocketAddrV4);

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterCompactNodeInfo {
    pub id: BetterNodeId,
    pub contact: BetterCompactPeerContact,
}

impl BetterCompactNodeInfo {
    pub fn new(id: BetterNodeId, contact: BetterCompactPeerContact) -> BetterCompactNodeInfo {
        BetterCompactNodeInfo { id, contact }
    }

    pub fn node_id(&self) -> &BetterNodeId {
        &self.id
    }

    pub fn contact(&self) -> &BetterCompactPeerContact {
        &self.contact
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct TransactionId(pub SmallVec<[u8; 2]>);

impl TransactionId {
    pub fn from_bytes(bytes: &[u8]) -> TransactionId {
        let vec = SmallVec::from_slice(bytes);
        TransactionId(vec)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0.as_slice()
    }
}
