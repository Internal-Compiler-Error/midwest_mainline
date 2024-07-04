use bendy::encoding::ToBencode;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};
use std::{
    fmt::{Debug, Formatter},
    net::{Ipv4Addr, SocketAddrV4}, ops::Deref,
};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterNodeId(pub String);

impl BetterNodeId {
    // todo: think of a better error type
    pub fn new(id: String) -> Result<Self, ()> {
        if !id.is_ascii() || id.len() != 20 {
            return Err(());
        }

        Ok(BetterNodeId(id))
    }
}

impl Deref for BetterNodeId {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToBencode for BetterNodeId {
    const MAX_DEPTH: usize = 0 as usize;

    fn encode(&self, encoder: bendy::encoding::SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        encoder.emit_str(&self.0)
    }
}


#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterInfoHash(pub String);
impl BetterInfoHash {
    // todo: think of a better error type
    pub fn new(id: String) -> Result<Self, ()> {
        if !id.is_ascii() || id.len() != 20 {
            return Err(());
        }

        Ok(Self(id))
    }
}

impl Deref for BetterInfoHash {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToBencode for BetterInfoHash {
    const MAX_DEPTH: usize = 0 as usize;

    fn encode(&self, encoder: bendy::encoding::SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        encoder.emit_str(&self.0)
    }
}
pub type NodeId = [u8; 20];

#[serde_as]
#[derive(PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
#[serde(transparent)]
pub struct CompactNodeContact {
    #[serde_as(as = "Bytes")]
    pub(crate) bytes: [u8; 26],
}

impl Debug for CompactNodeContact {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ip: SocketAddrV4 = self.into();
        write!(f, "id: {}, ip =  {ip}", hex::encode(&self.bytes[0..20]))
    }
}

impl Into<SocketAddrV4> for &CompactNodeContact {
    fn into(self) -> SocketAddrV4 {
        let ip = Ipv4Addr::new(self.bytes[20], self.bytes[21], self.bytes[22], self.bytes[23]);
        let port = u16::from_be_bytes([self.bytes[24], self.bytes[25]]);

        SocketAddrV4::new(ip, port)
    }
}

impl Into<SocketAddrV4> for CompactNodeContact {
    fn into(self) -> SocketAddrV4 {
        (&self).into()
    }
}

impl CompactNodeContact {
    pub fn new(bytes: [u8; 26]) -> Self {
        CompactNodeContact { bytes }
    }

    pub fn from_node_id_and_addr(node_id: &NodeId, addr: &SocketAddrV4) -> Self {
        let mut bytes = [0u8; 26];
        bytes[..20].copy_from_slice(node_id);
        bytes[20..24].copy_from_slice(&addr.ip().octets());
        bytes[24..26].copy_from_slice(&addr.port().to_be_bytes());
        CompactNodeContact { bytes }
    }
    pub fn node_id(&self) -> &NodeId {
        self.bytes[0..20].try_into().unwrap()
    }
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Clone, Hash, Deserialize)]
#[serde(transparent)]
pub struct CompactPeerContact {
    #[serde_as(as = "Bytes")]
    pub(crate) bytes: [u8; 6],
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
        BetterCompactNodeInfo {
            id,
            contact
        }
    }

    pub fn node_id(&self) -> &BetterNodeId {
        &self.id
    }
}

impl From<SocketAddrV4> for CompactPeerContact {
    fn from(addr: SocketAddrV4) -> Self {
        (&addr).into()
    }
}

impl From<&SocketAddrV4> for CompactPeerContact {
    fn from(addr: &SocketAddrV4) -> Self {
        let mut bytes = [0u8; 6];
        bytes[..4].copy_from_slice(&addr.ip().octets());
        bytes[4..6].copy_from_slice(&addr.port().to_be_bytes());
        CompactPeerContact { bytes }
    }
}

impl Into<SocketAddrV4> for &CompactPeerContact {
    fn into(self) -> SocketAddrV4 {
        let ip = Ipv4Addr::new(self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3]);
        let port = u16::from_be_bytes([self.bytes[4], self.bytes[5]]);

        SocketAddrV4::new(ip, port)
    }
}

impl Into<SocketAddrV4> for CompactPeerContact {
    fn into(self) -> SocketAddrV4 {
        (&self).into()
    }
}

pub(crate) trait ToConcatedNodeContact {
    fn to_concated_node_contact(&self) -> Box<[u8]>;
}

// todo: use some trait magic to allow vec of owned values, references and as well as poniters to
// be converted to a concated array of bytes.

impl ToConcatedNodeContact for Vec<CompactNodeContact> {
    fn to_concated_node_contact(&self) -> Box<[u8]> {
        let bytes = self.iter().map(|contact| contact.bytes).flatten().collect::<Vec<_>>();
        bytes.into_boxed_slice()
    }
}

impl ToConcatedNodeContact for Vec<&CompactNodeContact> {
    fn to_concated_node_contact(&self) -> Box<[u8]> {
        let bytes = self.iter().map(|contact| contact.bytes).flatten().collect::<Vec<_>>();
        bytes.into_boxed_slice()
    }
}
