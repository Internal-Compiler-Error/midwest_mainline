use crate::{
    domain_knowledge::{BetterCompactPeerContact, BetterCompactPeerInfo, BetterNodeId, NodeId},
    message::TransactionId,
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

use super::ToRawKrpc;

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPeersDeferredResponse {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "r")]
    pub(crate) body: GetPeersDeferredResponseBody,
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPeersDeferredResponseBody {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,

    #[serde_as(as = "Bytes")]
    pub token: Box<[u8]>,

    #[serde_as(as = "Bytes")]
    pub nodes: Box<[u8]>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterGetPeersDeferredResponse {
    transaction_id: String,
    querier: BetterNodeId,
    token: String,
    nodes: Vec<BetterCompactPeerInfo>,
}

impl BetterGetPeersDeferredResponse {
    pub fn new(transaction_id: String, querier: BetterNodeId, token: String, nodes: Vec<BetterCompactPeerInfo>) -> Self {
        Self {
            transaction_id,
            querier,
            token,
            nodes,
        }
    }
}

impl ToRawKrpc for BetterGetPeersDeferredResponse {

    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();

        encoder.emit_and_sort_dict(|e| {
            e.emit_pair(b"t", &self.transaction_id);
            e.emit_pair(b"y", &"r");

            e.emit_pair_with(b"r", |e| {
                e.emit_unsorted_dict(|e| {
                    e.emit_pair(b"id", &self.querier);
                    e.emit_pair(b"token", &self.token);

                    // values is a list of compact peer contacts, which are a 4 byte ipv4 address and a 2 byte port
                    // number. Unfortunately, the bittorrent people are insane and decided to encode this as a string
                    // using ascii in network/big endian.
                    e.emit_pair_with(b"nodes", |e| {
                        use std::str;

                        let combined = self.nodes.iter().map(|peer| {
                            let octets = peer.0.ip().octets();
                            let port_in_ne = peer.0.port().to_be_bytes();

                            let ip_as_ascii = str::from_utf8(octets.as_slice()).expect("we know the ip is ascii");
                            let port_as_ascii =
                                str::from_utf8(port_in_ne.as_slice()).expect("we know the port is ascii");

                            format!("{}{}", ip_as_ascii, port_as_ascii)
                        });
                        e.emit_unchecked_list(combined)
                    })
                })
            })
        });

        encoder
            .get_output()
            .expect("we know all the fields upfront, this should never error")
            .into_boxed_slice()
    }
}
