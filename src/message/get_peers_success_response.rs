use crate::domain_knowledge::{NodeId, PeerContact, Token, TransactionId};

use super::ToRawKrpc;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterGetPeersSuccessResponse {
    // TODO: make them private
    transaction_id: TransactionId,
    target_id: NodeId,
    pub token: Token,
    pub values: Vec<PeerContact>,
}

impl BetterGetPeersSuccessResponse {
    pub fn new(transaction_id: TransactionId, target_id: NodeId, token: Token, values: Vec<PeerContact>) -> Self {
        Self {
            transaction_id,
            target_id,
            token,
            values,
        }
    }

    pub fn add_peer(&mut self, peer: PeerContact) {
        self.values.push(peer);
    }

    pub fn txn_id(&self) -> &TransactionId {
        &self.transaction_id
    }
}

impl ToRawKrpc for BetterGetPeersSuccessResponse {
    #[allow(unused_must_use)]
    // If you are the poor soul who has to read this, I offer my condolences.
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();
        encoder.emit_and_sort_dict(|e| {
            e.emit_pair_with(b"t", |e| e.emit_bytes(self.transaction_id.as_bytes()));
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
            .expect("we know the keys upfront, this should never error")
            .into_boxed_slice()
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddrV4};

    use super::*;

    #[test]
    fn can_encode_example() {
        use std::str;

        let response = super::BetterGetPeersSuccessResponse::new(
            TransactionId::from_bytes(*&b"aa"),
            NodeId::from_bytes_unchecked(*&b"abcdefghij0123456789"),
            Token::from_bytes(*&b"aoeusnth"),
            vec![
                PeerContact(SocketAddrV4::new(Ipv4Addr::new(97, 120, 106, 101), 11893)),
                PeerContact(SocketAddrV4::new(Ipv4Addr::new(105, 100, 104, 116), 28269)),
            ],
        );

        let encoded = response.to_raw_krpc();
        let expected = "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re";

        assert_eq!(str::from_utf8(&*encoded).unwrap(), expected);
    }
}
