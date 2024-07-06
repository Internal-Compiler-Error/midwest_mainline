#![allow(unused_variables, dead_code)]
use crate::domain_knowledge::{NodeId, NodeInfo, PeerContact, Token, TransactionId};

use super::ToRawKrpc;

/// TODO:
///
/// TLDR is that responses are not tagged to indicate what kind of message they are so it's much
/// easier to just keep all of them in one place
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct FindNodeGetPeersResponse {
    transaction_id: TransactionId,
    peer_id: NodeId,
    token: Option<Token>,
    values: Vec<PeerContact>,
    nodes: Vec<NodeInfo>,
}

#[derive(Debug, Hash, Clone)]
pub struct Builder {
    transaction_id: TransactionId,
    peer_id: NodeId,
    token: Option<Token>,
    values: Vec<PeerContact>,
    nodes: Vec<NodeInfo>,
}

impl Builder {
    pub fn new(transaction_id: TransactionId, peer_id: NodeId) -> Builder {
        Self {
            transaction_id,
            peer_id,
            token: None,
            values: vec![],
            nodes: vec![],
        }
    }

    pub fn with_token(mut self, token: Token) -> Self {
        self.token = Some(token);
        self
    }

    pub fn with_node(mut self, node: NodeInfo) -> Self {
        self.nodes.push(node);
        self
    }

    pub fn with_nodes(mut self, nodes: &[NodeInfo]) -> Self {
        self.nodes.extend_from_slice(nodes);
        self
    }

    pub fn with_value(mut self, value: PeerContact) -> Self {
        self.values.push(value);
        self
    }

    pub fn with_values(mut self, values: &[PeerContact]) -> Self {
        self.values.extend_from_slice(values);
        self
    }

    pub fn build(self) -> FindNodeGetPeersResponse {
        FindNodeGetPeersResponse {
            transaction_id: self.transaction_id,
            peer_id: self.peer_id,
            token: self.token,
            values: self.values,
            nodes: self.nodes,
        }
    }
}

impl FindNodeGetPeersResponse {
    pub fn txn_id(&self) -> &TransactionId {
        &self.transaction_id
    }
}

impl ToRawKrpc for FindNodeGetPeersResponse {
    #[allow(unused_must_use)]
    // If you are the poor soul who has to read this, I offer my condolences.
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;
        use bendy::value::Value;
        use std::borrow::Cow;

        let mut encoder = Encoder::new();
        encoder.emit_and_sort_dict(|e| {
            e.emit_pair_with(b"t", |e| e.emit_bytes(self.transaction_id.as_bytes()));
            e.emit_pair(b"y", "r");
            e.emit_pair_with(b"r", |e| {
                e.emit_unsorted_dict(|e| {
                    e.emit_pair(b"id", &self.peer_id);
                    if let Some(ref token) = self.token {
                        e.emit_pair(b"token", token);
                    }

                    if !self.values.is_empty() {
                        // values is a list of compact peer contacts, which are a 4 byte ipv4 address and a 2 byte port
                        // number. Unfortunately, the bittorrent people are insane and decided to encode this as a string
                        // using ascii in network/big endian.
                        e.emit_pair_with(b"values", |e| {
                            let combined = self.values.iter().map(|peer| {
                                let octets = peer.0.ip().octets();
                                let port_in_be = peer.0.port().to_be_bytes();

                                let mut arr = [0u8; 6];
                                let ip = &mut arr[0..4];
                                ip.copy_from_slice(&octets);

                                let port = &mut arr[4..6];
                                port.copy_from_slice(&port_in_be);

                                // bendy is stupid
                                Value::Bytes(Cow::Owned(arr.as_slice().to_vec()))
                            });

                            e.emit_unchecked_list(combined)
                        });
                    }

                    if !self.nodes.is_empty() {
                        // nodes is a list of node id concated with the their ip and port
                        e.emit_pair_with(b"nodes", |e| {
                            let combined = self.nodes.iter().map(|peer| {
                                let node_id = &peer.id;

                                let octets = peer.contact.0.ip().octets();
                                let port_in_be = peer.contact.0.port().to_be_bytes();

                                let mut arr = [0u8; 26];
                                let id = &mut arr[0..20];
                                id.copy_from_slice(&node_id.0);

                                let ip = &mut arr[20..24];
                                ip.copy_from_slice(&octets);

                                let port = &mut arr[24..26];
                                port.copy_from_slice(&port_in_be);

                                // because bendy is stupid
                                Vec::from_iter(arr)
                            });
                            e.emit_unchecked_list(combined)
                        });
                    }

                    Ok(())
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

        let response = Builder::new(
            TransactionId::from_bytes(*&b"aa"),
            NodeId::from_bytes_unchecked(*&b"abcdefghij0123456789"),
        )
        .with_token(Token::from_bytes(*&b"aoeusnth"))
        .with_value(PeerContact(SocketAddrV4::new(Ipv4Addr::new(97, 120, 106, 101), 11893)))
        .with_value(PeerContact(SocketAddrV4::new(Ipv4Addr::new(105, 100, 104, 116), 28269)))
        .build();

        let encoded = response.to_raw_krpc();
        let encoded = str::from_utf8(&*encoded).unwrap();

        let expected = "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re";

        assert_eq!(encoded, expected);
    }
}
