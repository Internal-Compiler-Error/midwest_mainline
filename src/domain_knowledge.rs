use std::net::{Ipv4Addr, SocketAddrV4};
use rand::Fill;
use serde_with::{serde_as, Bytes};
use serde::{Deserialize, Serialize};

pub type NodeId = [u8; 20];

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct CompactNodeContact {
    #[serde_as(as = "Bytes")]
    bytes: [u8; 26],
}

impl Into<SocketAddrV4> for &CompactNodeContact {
    fn into(self) -> SocketAddrV4 {
        let ip = Ipv4Addr::new(self.bytes[20], self.bytes[21], self.bytes[22], self.bytes[23]);
        let port = u16::from_be_bytes([self.bytes[24], self.bytes[25]]);

        SocketAddrV4::new(ip, port)
    }
}


impl CompactNodeContact {
    pub fn new(bytes: [u8; 26]) -> Self {
        CompactNodeContact { bytes }
    }

    pub fn node_id(&self) -> &NodeId {
        self.bytes[0..20].try_into().unwrap()
    }
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactPeerContact {
    #[serde_as(as = "Bytes")]
    bytes: [u8; 6],
}
