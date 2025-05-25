#![allow(unused_variables, dead_code)]

use std::net::SocketAddrV4;

use bendy::encoding::SingleItemEncoder;

use crate::types::{NodeId, NodeInfo, Token};

use super::ToKrpcBody;

/// TODO:
///
/// TLDR is that responses are not tagged to indicate what kind of message they are so it's much
/// easier to just keep all of them in one place
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct FindNodeGetPeersResponse {
    queried: NodeId,
    token: Option<Token>,
    values: Vec<SocketAddrV4>,
    nodes: Vec<NodeInfo>,
}

#[derive(Debug, Hash, Clone)]
pub struct Builder {
    queried: NodeId,
    token: Option<Token>,
    values: Vec<SocketAddrV4>,
    nodes: Vec<NodeInfo>,
}

impl Builder {
    pub fn new(peer_id: NodeId) -> Builder {
        Self {
            queried: peer_id,
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

    pub fn with_value(mut self, value: SocketAddrV4) -> Self {
        self.values.push(value);
        self
    }

    pub fn with_values(mut self, values: &[SocketAddrV4]) -> Self {
        self.values.extend_from_slice(values);
        self
    }

    pub fn build(self) -> FindNodeGetPeersResponse {
        FindNodeGetPeersResponse {
            queried: self.queried,
            token: self.token,
            values: self.values,
            nodes: self.nodes,
        }
    }
}

impl FindNodeGetPeersResponse {
    pub fn is_get_peer_success(&self) -> bool {
        // TODO: do we need to care if the token exists?
        !self.values.is_empty()
    }

    pub fn is_get_peer_defferred(&self) -> bool {
        !self.is_get_peer_success()
    }

    pub fn has_token(&self) -> bool {
        self.token.is_some()
    }

    pub fn queried(&self) -> &NodeId {
        &self.queried
    }

    pub fn token(&self) -> Option<&Token> {
        self.token.as_ref()
    }

    pub fn values(&self) -> &Vec<SocketAddrV4> {
        &self.values
    }

    pub fn nodes(&self) -> &Vec<NodeInfo> {
        &self.nodes
    }
}

impl ToKrpcBody for FindNodeGetPeersResponse {
    #[allow(unused_must_use)]
    // If you are the poor soul who has to read this, I offer my condolences.
    fn encode_body(&self, enc: SingleItemEncoder) {
        enc.emit_unsorted_dict(|enc| {
            use bendy::value::Value;
            use std::borrow::Cow;

            enc.emit_pair(b"id", &self.queried);
            if let Some(ref token) = self.token {
                enc.emit_pair(b"token", token);
            }

            if !self.values.is_empty() {
                // values is a list of compact peer contacts, which are a 4 byte ipv4 address and a 2 byte port
                // number. Unfortunately, the bittorrent people are insane and decided to encode this as a string
                // using ascii in network/big endian.
                enc.emit_pair_with(b"values", |e| {
                    let combined = self.values.iter().map(|peer| {
                        let octets = peer.ip().octets();
                        let port_in_be = peer.port().to_be_bytes();

                        let mut arr = [0u8; 6];
                        let ip = &mut arr[0..4];
                        ip.copy_from_slice(&octets);

                        let port = &mut arr[4..6];
                        port.copy_from_slice(&port_in_be);

                        Value::Bytes(Cow::Owned(arr.as_slice().to_vec()))
                    });

                    e.emit_unchecked_list(combined)
                });
            }

            if !self.nodes.is_empty() {
                // nodes is a giant binary string, its length is some product of 26, each
                // 26 byte is compromised of 20 bytes of node id, 4 bytes of ip address and
                // 2 bytes of port number
                enc.emit_pair_with(b"nodes", |e| {
                    let combined: Vec<u8> = self
                        .nodes
                        .iter()
                        .map(|peer| {
                            let node_id = &peer.id();

                            let octets = peer.end_point().ip().octets();
                            let port_in_be = peer.end_point().port().to_be_bytes();

                            let mut arr = [0u8; 26];
                            let id = &mut arr[0..20];
                            id.copy_from_slice(&node_id.0);

                            let ip = &mut arr[20..24];
                            ip.copy_from_slice(&octets);

                            let port = &mut arr[24..26];
                            port.copy_from_slice(&port_in_be);

                            arr
                        })
                        .flatten()
                        .collect();

                    e.emit_bytes(&combined)
                });
            }
            Ok(())
        })
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddrV4};

    use crate::{
        message::{Krpc, KrpcBody},
        types::TransactionId,
    };

    use super::*;

    #[test]
    fn can_encode_has_peers_example() {
        use std::str;

        let txn_id = TransactionId::from_bytes(*&b"aa");
        let response = Builder::new(NodeId::from_bytes(*&b"abcdefghij0123456789"))
            .with_token(Token::from_bytes(*&b"aoeusnth"))
            .with_value(SocketAddrV4::new(Ipv4Addr::new(97, 120, 106, 101), 11893))
            .with_value(SocketAddrV4::new(Ipv4Addr::new(105, 100, 104, 116), 28269))
            .build();

        let encoded = Krpc::new_with_body(txn_id, KrpcBody::FindNodeGetPeersResponse(response)).encode();
        let encoded = str::from_utf8(&*encoded).unwrap();

        let expected = "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re";

        assert_eq!(encoded, expected);
    }

    #[test]
    fn can_encode_no_peers() {
        use std::str;

        let txn_id = TransactionId::from_bytes(*&b"aa");
        let response = Builder::new(NodeId::from_bytes(*&b"abcdefghij0123456789"))
            .with_token(Token::from_bytes(*&b"aoeusnth"))
            .with_node(NodeInfo::new(
                NodeId::from_bytes(*&b"lmnopqrstuvxyz098765"),
                SocketAddrV4::new(Ipv4Addr::new(97, 120, 106, 101), 11893),
            ))
            .build();

        let encoded = Krpc::new_with_body(txn_id, KrpcBody::FindNodeGetPeersResponse(response)).encode();
        let encoded = str::from_utf8(&*encoded).unwrap();

        let expected =
            "d1:rd2:id20:abcdefghij01234567895:nodes26:lmnopqrstuvxyz098765axje.u5:token8:aoeusnthe1:t2:aa1:y1:re";

        assert_eq!(encoded, expected);
    }
}
