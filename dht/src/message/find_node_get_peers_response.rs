use std::net::SocketAddr;

use bendy::encoding::SingleItemEncoder;

use crate::bloom::BloomFilter;
use crate::types::{Family, InfoHash, NodeId, NodeInfo, Token};

use super::{ToKrpcBody, compact_addr, emit_raw};

/// KRPC responses are not tagged with the query they answer, so find_node and get_peers
/// responses share one struct: `nodes`/`nodes6` cover find_node (and the get_peers fallback),
/// `values` + `token` cover get_peers.
///
/// `nodes` and `nodes6` are `None` when the key is absent, which is not the same as present
/// and empty: an answer without peers must carry at least one of them.
///
/// The extensions that answer with nodes too ride along: BEP 51's samples, BEP 33's filters,
/// BEP 44's items.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct FindNodeGetPeersResponse {
    pub(super) queried: NodeId,
    pub(super) token: Option<Token>,
    pub(super) values: Vec<SocketAddr>,
    pub(super) nodes: Option<Vec<NodeInfo>>,
    pub(super) nodes6: Option<Vec<NodeInfo>>,
    pub(super) samples: Option<Samples>,
    /// boxed: 512 bytes most answers don't carry
    pub(super) scrape: Option<Box<ScrapeFilters>>,
    pub(super) item: Option<Item>,
}

/// BEP 44's answer to `get`: the value stored at the target, bencoded, and for a mutable item
/// what it was signed with
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Item {
    pub value: Vec<u8>,
    pub signature: Option<ItemSignature>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ItemSignature {
    /// `k`, the ed25519 public key
    pub key: [u8; 32],
    pub seq: i64,
    pub sig: [u8; 64],
}

/// BEP 33's answer to a get_peers with `scrape`: the node's seeds and other peers
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Default)]
pub struct ScrapeFilters {
    /// `BFsd`
    pub seeds: BloomFilter,
    /// `BFpe`
    pub peers: BloomFilter,
}

/// BEP 51's answer to sample_infohashes
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Samples {
    /// seconds until the node has a fresh sample; BEP 51 allows 0 to 6 hours
    pub interval: u32,
    /// how many info hashes the node stores in all
    pub num: u64,
    pub samples: Vec<InfoHash>,
}

/// BEP 51's limit on `interval`
pub const MAX_SAMPLE_INTERVAL: u32 = 6 * 60 * 60;

#[derive(Debug, Hash, Clone)]
pub struct Builder(FindNodeGetPeersResponse);

impl Builder {
    pub fn new(queried: NodeId) -> Builder {
        Self(FindNodeGetPeersResponse {
            queried,
            token: None,
            values: vec![],
            nodes: None,
            nodes6: None,
            samples: None,
            scrape: None,
            item: None,
        })
    }

    pub fn with_item(mut self, item: Item) -> Self {
        self.0.item = Some(item);
        self
    }

    pub fn with_scrape(mut self, scrape: ScrapeFilters) -> Self {
        self.0.scrape = Some(Box::new(scrape));
        self
    }

    pub fn with_samples(mut self, samples: Samples) -> Self {
        self.0.samples = Some(samples);
        self
    }

    pub fn with_token(mut self, token: Token) -> Self {
        self.0.token = Some(token);
        self
    }

    /// An IPv4 node goes to `nodes`, an IPv6 one to `nodes6`
    pub fn with_node(self, node: NodeInfo) -> Self {
        self.with_nodes_of(node.family(), &[node])
    }

    /// The `nodes` key, present even if `nodes` is empty
    pub fn with_nodes(self, nodes: &[NodeInfo]) -> Self {
        self.with_nodes_of(Family::V4, nodes)
    }

    /// The `nodes6` key, present even if `nodes` is empty
    pub fn with_nodes6(self, nodes: &[NodeInfo]) -> Self {
        self.with_nodes_of(Family::V6, nodes)
    }

    /// `with_nodes` or `with_nodes6`, by family
    pub fn with_nodes_of(mut self, family: Family, nodes: &[NodeInfo]) -> Self {
        let list = match family {
            Family::V4 => &mut self.0.nodes,
            Family::V6 => &mut self.0.nodes6,
        };
        list.get_or_insert_default().extend_from_slice(nodes);
        self
    }

    pub fn with_value(mut self, value: impl Into<SocketAddr>) -> Self {
        self.0.values.push(value.into());
        self
    }

    pub fn with_values(mut self, values: &[SocketAddr]) -> Self {
        self.0.values.extend_from_slice(values);
        self
    }

    /// A response with neither peers nor any nodes key gets an empty `nodes`
    pub fn build(mut self) -> FindNodeGetPeersResponse {
        let response = &mut self.0;
        if response.nodes.is_none() && response.nodes6.is_none() && response.values.is_empty() {
            response.nodes = Some(vec![]);
        }
        self.0
    }
}

impl FindNodeGetPeersResponse {
    pub fn queried(&self) -> NodeId {
        self.queried
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

    /// BEP 51, in an answer to sample_infohashes
    pub fn samples(&self) -> Option<&Samples> {
        self.samples.as_ref()
    }

    /// BEP 33, in an answer to a get_peers with `scrape`
    pub fn scrape(&self) -> Option<&ScrapeFilters> {
        self.scrape.as_deref()
    }

    /// BEP 44, in an answer to `get`
    pub fn item(&self) -> Option<&Item> {
        self.item.as_ref()
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
    fn encode_body(&self, enc: SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        enc.emit_unsorted_dict(|enc| {
            enc.emit_pair(b"id", self.queried)?;
            if let Some(token) = &self.token {
                enc.emit_pair(b"token", token)?;
            }
            if !self.values.is_empty() {
                enc.emit_pair_with(b"values", |e| {
                    e.emit_list(|e| {
                        self.values
                            .iter()
                            .try_for_each(|peer| e.emit_bytes(&compact_addr(peer)))
                    })
                })?;
            }
            if let Some(nodes) = &self.nodes {
                enc.emit_pair_with(b"nodes", |e| e.emit_bytes(&compact_nodes(nodes)))?;
            }
            if let Some(nodes6) = &self.nodes6 {
                enc.emit_pair_with(b"nodes6", |e| e.emit_bytes(&compact_nodes(nodes6)))?;
            }
            if let Some(samples) = &self.samples {
                enc.emit_pair(b"interval", samples.interval)?;
                enc.emit_pair(b"num", samples.num)?;
                let raw: Vec<u8> = samples.samples.iter().flat_map(|h| h.0).collect();
                enc.emit_pair_with(b"samples", |e| e.emit_bytes(&raw))?;
            }
            if let Some(scrape) = &self.scrape {
                enc.emit_pair_with(b"BFsd", |e| e.emit_bytes(&scrape.seeds.0))?;
                enc.emit_pair_with(b"BFpe", |e| e.emit_bytes(&scrape.peers.0))?;
            }
            if let Some(item) = &self.item {
                enc.emit_pair_with(b"v", |e| emit_raw(e, &item.value))?;
                if let Some(signature) = &item.signature {
                    enc.emit_pair_with(b"k", |e| e.emit_bytes(&signature.key))?;
                    enc.emit_pair(b"seq", signature.seq)?;
                    enc.emit_pair_with(b"sig", |e| e.emit_bytes(&signature.sig))?;
                }
            }
            Ok(())
        })
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

        let encoded = Krpc::new(txn_id, KrpcBody::FindNodeGetPeersResponse(response)).encode();
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

        let encoded = Krpc::new(txn_id, KrpcBody::FindNodeGetPeersResponse(response)).encode();
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

        let encoded = Krpc::new(txn_id, KrpcBody::FindNodeGetPeersResponse(response)).encode();
        let encoded = str::from_utf8(&encoded).unwrap();

        let expected = "d1:rd2:id20:abcdefghij01234567895:nodes0:e1:t2:aa1:y1:re";

        assert_eq!(encoded, expected);
    }
}
