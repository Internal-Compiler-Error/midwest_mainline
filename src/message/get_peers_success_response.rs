use crate::{
    domain_knowledge::{CompactPeerContact, NodeId, BetterPeerContact, BetterNodeId},
    message::TransactionId,
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

use super::ToRawKrpc;

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPeersSuccessResponse {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "r")]
    pub(crate) body: GetPeersSuccessResponseBody,
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPeersSuccessResponseBody {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,

    #[serde_as(as = "Bytes")]
    pub token: Box<[u8]>,

    pub values: Vec<CompactPeerContact>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterGetPeersSuccessResponse {
    transaction_id: String,
    target_id: BetterNodeId,
    token: String,
    values: Vec<BetterPeerContact>, 
}

impl BetterGetPeersSuccessResponse {
    pub fn new(transaction_id: String, target_id: BetterNodeId, token: String, values: Vec<BetterPeerContact>) ->  Self {

        Self {
            transaction_id,
            target_id,
            token,
            values,
        }
    }


    pub fn add_peer(&mut self, peer: BetterPeerContact) {
        self.values.push(peer);
    } 
}

impl ToRawKrpc for BetterGetPeersSuccessResponse {

    #[allow(unused_must_use)]
    // If you are the poor soul who has to read this, I offer my condolences.
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();
        encoder.emit_and_sort_dict(|e| {
            e.emit_pair(b"t", &self.transaction_id);
            e.emit_pair(b"y", "r");
            e.emit_pair_with(b"r", |e| {
                e.emit_unsorted_dict(|e| {
                    e.emit_pair(b"id", &self.target_id);
                    e.emit_pair(b"token", &self.token);

                    // values is a list of compact peer contacts, which are a 4 byte ipv4 address and a 2 byte port
                    // number. Unfortunately, the bittorrent people are insane and decided to encode this as a string
                    // using ascii in network/big endian.
                    e.emit_pair_with(b"values", |e| { 
                        use std::str;    

                        let combined = self.values.iter().map(|peer| {
                            let octets = peer.0.ip().octets();
                            let port_in_ne = peer.0.port().to_be_bytes();

                            let ip_as_ascii = str::from_utf8(octets.as_slice()).expect("we know the ip is ascii");
                            let port_as_ascii = str::from_utf8(port_in_ne.as_slice()).expect("we know the port is ascii");

                            format!("{}{}", ip_as_ascii, port_as_ascii)
                        });
                        e.emit_unchecked_list(combined)
                    })
                })
            })
        });

        encoder.get_output().expect("we know the keys upfront, this should never error").into_boxed_slice()
    }
}

#[cfg(test)]
mod tests {
    use std::net::{SocketAddrV4, Ipv4Addr};

    use super::*;

    #[test]
    fn can_encode_example() {
       use std::str;

       let response = super::BetterGetPeersSuccessResponse::new(
           "aa".to_string(),
            BetterNodeId::new("abcdefghij0123456789".to_string()).unwrap(),
            "aoeusnth".to_string(),
           vec![
            BetterPeerContact(SocketAddrV4::new(Ipv4Addr::new(97, 120, 106, 101), 11893)),
            BetterPeerContact(SocketAddrV4::new(Ipv4Addr::new(105, 100, 104, 116), 28269)),
           ]);
       
        let encoded = response.to_raw_krpc(); 
        let expected = "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re";
        
        assert_eq!(str::from_utf8(&*encoded).unwrap(), expected);
    }
}
