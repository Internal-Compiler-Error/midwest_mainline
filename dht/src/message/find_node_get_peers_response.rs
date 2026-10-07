use std::net::SocketAddr;

use bendy::encoding::SingleItemEncoder;

use crate::types::{Family, NodeId, NodeInfo, Token};

use super::{ToKrpcBody, compact_addr};

/// KRPC responses are not tagged with the query they answer, so find_node and get_peers
/// responses share one struct: `nodes`/`nodes6` cover find_node (and the get_peers fallback),
/// `values` + `token` cover get_peers.
///
/// `nodes` and `nodes6` are `None` when the key is absent, which is not the same as present
/// and empty: an answer without peers must carry at least one of them.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct FindNodeGetPeersResponse {
    queried: NodeId,
    token: Option<Token>,
    values: Vec<SocketAddr>,
    nodes: Option<Vec<NodeInfo>>,
    nodes6: Option<Vec<NodeInfo>>,
}

#[derive(Debug, Hash, Clone)]
pub struct Builder {
    queried: NodeId,
    token: Option<Token>,
    values: Vec<SocketAddr>,
    nodes: Option<Vec<NodeInfo>>,
    nodes6: Option<Vec<NodeInfo>>,
}

impl Builder {
    pub fn new(peer_id: NodeId) -> Builder {
        Self {
            queried: peer_id,
            token: None,
            values: vec![],
            nodes: None,
            nodes6: None,
        }
    }

    pub fn with_token(mut self, token: Token) -> Self {
        self.token = Some(token);
        self
    }

    /// An IPv4 node goes to `nodes`, an IPv6 one to `nodes6`
    pub fn with_node(mut self, node: NodeInfo) -> Self {
        let list = match node.family() {
            Family::V4 => &mut self.nodes,
            Family::V6 => &mut self.nodes6,
        };
        list.get_or_insert_default().push(node);
        self
    }

    /// The `nodes` key, present even if `nodes` is empty
    pub fn with_nodes(mut self, nodes: &[NodeInfo]) -> Self {
        self.nodes.get_or_insert_default().extend_from_slice(nodes);
        self
    }

    /// The `nodes6` key, present even if `nodes` is empty
    pub fn with_nodes6(mut self, nodes: &[NodeInfo]) -> Self {
        self.nodes6.get_or_insert_default().extend_from_slice(nodes);
        self
    }

    /// `with_nodes` or `with_nodes6`, by family
    pub fn with_nodes_of(self, family: Family, nodes: &[NodeInfo]) -> Self {
        match family {
            Family::V4 => self.with_nodes(nodes),
            Family::V6 => self.with_nodes6(nodes),
        }
    }

    pub fn with_value(mut self, value: impl Into<SocketAddr>) -> Self {
        self.values.push(value.into());
        self
    }

    pub fn with_values(mut self, values: &[SocketAddr]) -> Self {
        self.values.extend_from_slice(values);
        self
    }

    /// A response with neither peers nor any nodes key gets an empty `nodes`
    pub fn build(self) -> FindNodeGetPeersResponse {
        let nodes = match (&self.nodes, &self.nodes6) {
            (None, None) if self.values.is_empty() => Some(vec![]),
            _ => self.nodes,
        };
        FindNodeGetPeersResponse {
            queried: self.queried,
            token: self.token,
            values: self.values,
            nodes,
            nodes6: self.nodes6,
        }
    }
}

impl FindNodeGetPeersResponse {
    pub fn has_token(&self) -> bool {
        self.token.is_some()
    }

    pub fn queried(&self) -> &NodeId {
        &self.queried
    }

    pub fn token(&self) -> Option<&Token> {
        self.token.as_ref()
    }

    pub fn values(&self) -> &[SocketAddr] {
        &self.values
    }

    /// IPv4 nodes, the `nodes` key
    pub fn nodes(&self) -> &[NodeInfo] {
        self.nodes.as_deref().unwrap_or_default()
    }

    /// IPv6 nodes, the `nodes6` key (BEP 32)
    pub fn nodes6(&self) -> &[NodeInfo] {
        self.nodes6.as_deref().unwrap_or_default()
    }

    pub fn nodes_of(&self, family: Family) -> &[NodeInfo] {
        match family {
            Family::V4 => self.nodes(),
            Family::V6 => self.nodes6(),
        }
    }
}

/// Compact node info: the 20-byte id, then the compact address, back to back
fn compact_nodes(nodes: &[NodeInfo]) -> Vec<u8> {
    nodes
        .iter()
        .flat_map(|node| {
            let mut raw = node.id().0.to_vec();
            raw.extend(compact_addr(&node.end_point()));
            raw
        })
        .collect()
}

impl ToKrpcBody for FindNodeGetPeersResponse {
    #[allow(unused_must_use)]
    // If you are the poor soul who has to read this, I offer my condolences.
    fn encode_body(&self, enc: SingleItemEncoder) {
        enc.emit_unsorted_dict(|enc| {
            use bendy::value::Value;
            use std::borrow::Cow;

            enc.emit_pair(b"id", self.queried);
            if let Some(ref token) = self.token {
                enc.emit_pair(b"token", token);
            }

            if !self.values.is_empty() {
                // values is a list of compact peer contacts: the address and the port as a
                // string in network byte order, 6 bytes for IPv4 and 18 for IPv6
                enc.emit_pair_with(b"values", |e| {
                    let combined = self
                        .values
                        .iter()
                        .map(|peer| Value::Bytes(Cow::Owned(compact_addr(peer))));
                    e.emit_unchecked_list(combined)
                });
            }

            if let Some(nodes) = &self.nodes {
                enc.emit_pair_with(b"nodes", |e| e.emit_bytes(&compact_nodes(nodes)));
            }
            if let Some(nodes6) = &self.nodes6 {
                enc.emit_pair_with(b"nodes6", |e| e.emit_bytes(&compact_nodes(nodes6)));
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

        let txn_id = TransactionId::from_bytes(b"aa");
        let response = Builder::new(NodeId::from_bytes(b"abcdefghij0123456789"))
            .with_token(Token::from_bytes(b"aoeusnth"))
            .with_value(SocketAddrV4::new(Ipv4Addr::new(97, 120, 106, 101), 11893))
            .with_value(SocketAddrV4::new(Ipv4Addr::new(105, 100, 104, 116), 28269))
            .build();

        let encoded = Krpc::new_with_body(txn_id, KrpcBody::FindNodeGetPeersResponse(response)).encode();
        let encoded = str::from_utf8(&encoded).unwrap();

        let expected = "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re";

        assert_eq!(encoded, expected);
    }

    #[test]
    fn can_encode_no_peers() {
        use std::str;

        let txn_id = TransactionId::from_bytes(b"aa");
        let response = Builder::new(NodeId::from_bytes(b"abcdefghij0123456789"))
            .with_token(Token::from_bytes(b"aoeusnth"))
            .with_node(NodeInfo::new(
                NodeId::from_bytes(b"lmnopqrstuvxyz098765"),
                SocketAddrV4::new(Ipv4Addr::new(97, 120, 106, 101), 11893),
            ))
            .build();

        let encoded = Krpc::new_with_body(txn_id, KrpcBody::FindNodeGetPeersResponse(response)).encode();
        let encoded = str::from_utf8(&encoded).unwrap();

        let expected =
            "d1:rd2:id20:abcdefghij01234567895:nodes26:lmnopqrstuvxyz098765axje.u5:token8:aoeusnthe1:t2:aa1:y1:re";

        assert_eq!(encoded, expected);
    }

    #[test]
    fn empty_response_still_carries_the_nodes_key() {
        use std::str;

        let txn_id = TransactionId::from_bytes(b"aa");
        let response = Builder::new(NodeId::from_bytes(b"abcdefghij0123456789")).build();

        let encoded = Krpc::new_with_body(txn_id, KrpcBody::FindNodeGetPeersResponse(response)).encode();
        let encoded = str::from_utf8(&encoded).unwrap();

        let expected = "d1:rd2:id20:abcdefghij01234567895:nodes0:e1:t2:aa1:y1:re";

        assert_eq!(encoded, expected);
    }
}
