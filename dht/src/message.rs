//! The KRPC wire protocol (BEP 5): bencoded dicts with a transaction id (`t`), a message
//! type (`y` = q/r/e), and a body. Parsing is one pass over the packet (see `bencode`),
//! borrowing the fields. KRPC responses are not self-describing: the answers to find_node,
//! get_peers, sample_infohashes and get all map to one [`FindNodeGetPeersResponse`], those to
//! ping, announce_peer and put to one [`PingAnnouncePeerResponse`].

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use bendy::encoding::{Encoder, SingleItemEncoder};
use bendy::value;
use eyre::eyre;
use tracing::{debug, instrument};

use crate::bloom::BloomFilter;
use crate::our_error::OurError;
use crate::types::{Family, InfoHash, NodeId, NodeInfo, Token, TransactionId};
use announce_peer_query::AnnouncePeerQuery;
use bencode::{Dict, Value};
use error::KrpcError;
use find_node_get_peers_response::{
    FindNodeGetPeersResponse, Item, ItemSignature, MAX_SAMPLE_INTERVAL, Samples, ScrapeFilters,
};
use find_node_query::FindNodeQuery;
use get_peers_query::GetPeersQuery;
use item_queries::{GetQuery, PutQuery, Signed};
use ping_announce_peer_response::PingAnnouncePeerResponse;
use ping_query::PingQuery;
use sample_infohashes_query::SampleInfohashesQuery;

pub mod announce_peer_query;
mod bencode;
pub mod error;
pub mod find_node_get_peers_response;
pub mod find_node_query;
pub mod get_peers_query;
pub mod item_queries;
pub mod ping_announce_peer_response;
pub mod ping_query;
pub mod sample_infohashes_query;

/// Compact peer info: 4 (IPv4) or 16 (IPv6) address bytes, then the port, all big endian.
pub fn compact_addr(addr: &SocketAddr) -> Vec<u8> {
    let mut raw = match addr.ip() {
        IpAddr::V4(ip) => ip.octets().to_vec(),
        IpAddr::V6(ip) => ip.octets().to_vec(),
    };
    raw.extend_from_slice(&addr.port().to_be_bytes());
    raw
}

/// The inverse of [`compact_addr`]; `None` for any length but 6 and 18.
pub fn parse_compact_addr(raw: &[u8]) -> Option<SocketAddr> {
    let (ip, port): (IpAddr, _) = match raw.len() {
        6 => (Ipv4Addr::from(<[u8; 4]>::try_from(&raw[..4]).ok()?).into(), &raw[4..]),
        18 => (
            Ipv6Addr::from(<[u8; 16]>::try_from(&raw[..16]).ok()?).into(),
            &raw[16..],
        ),
        _ => return None,
    };
    Some(SocketAddr::new(ip, u16::from_be_bytes([port[0], port[1]])))
}

/// Compact node info's length: the 20-byte id, then the compact address
pub(crate) fn compact_node_len(family: Family) -> usize {
    match family {
        Family::V4 => 26,
        Family::V6 => 38,
    }
}

/// An address no one can be reached at: no address, or no port
fn unroutable(addr: &SocketAddr) -> bool {
    addr.ip().is_unspecified() || addr.port() == 0
}

/// BEP 32's `want`: which families' nodes a find_node or get_peers querier would like back
/// (`n4` → `nodes`, `n6` → `nodes6`). Absent, the answer carries the family the query
/// arrived over.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Default)]
pub struct Want {
    pub v4: bool,
    pub v6: bool,
}

impl Want {
    pub const BOTH: Want = Want { v4: true, v6: true };

    pub fn only(family: Family) -> Want {
        Want {
            v4: family == Family::V4,
            v6: family == Family::V6,
        }
    }

    pub fn includes(&self, family: Family) -> bool {
        match family {
            Family::V4 => self.v4,
            Family::V6 => self.v6,
        }
    }

    pub(crate) fn encode(&self, enc: SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        let wanted = [(self.v4, b"n4"), (self.v6, b"n6")];
        enc.emit_list(|e| {
            wanted
                .iter()
                .filter(|(on, _)| *on)
                .try_for_each(|(_, w)| e.emit_bytes(*w))
        })
    }
}

/// A body of a KRPC message: the `a` of a query, the `r` of a response, the `e` of an error
pub(crate) trait ToKrpcBody {
    fn encode_body(&self, enc: SingleItemEncoder) -> Result<(), bendy::encoding::Error>;
}

/// `raw` as one bencoded value (nested at most 64 deep); `None` if it's anything else
pub(crate) fn parse_value(raw: &[u8]) -> Option<value::Value<'static>> {
    use bendy::decoding::{Decoder, FromBencode};
    let mut decoder = Decoder::new(raw).with_max_depth(64);
    let value = {
        let object = decoder.next_object().ok()??;
        value::Value::decode_bencode_object(object).ok()?.into_owned()
    };
    matches!(decoder.next_object(), Ok(None)).then_some(value)
}

/// Emits `raw`, a bencoded value, as itself rather than as a string of its bytes
pub(crate) fn emit_raw(enc: SingleItemEncoder, raw: &[u8]) -> Result<(), bendy::encoding::Error> {
    match parse_value(raw) {
        Some(value) => enc.emit(&value),
        // values are checked on the way in, so this is a bug; a string is still valid KRPC
        None => enc.emit_bytes(raw),
    }
}

fn malformed(what: impl std::fmt::Display) -> OurError {
    OurError::DecodeError(eyre!("{what}"))
}

/// A KRPC dict, its keys taken out as they're read. A key of the wrong type is a decode error;
/// what nobody read is logged as unknown.
struct Fields<'a> {
    dict: Dict<'a>,
    /// what the dict is, for the log
    of: &'static str,
}

impl<'a> Fields<'a> {
    fn new(dict: Dict<'a>, of: &'static str) -> Self {
        Self { dict, of }
    }

    fn take(&mut self, key: &str) -> Option<Value<'a>> {
        self.dict.remove(key.as_bytes())
    }

    fn typed<T>(
        &mut self,
        key: &str,
        kind: &str,
        pick: impl FnOnce(Value<'a>) -> Option<T>,
    ) -> Result<Option<T>, OurError> {
        match self.take(key) {
            None => Ok(None),
            Some(value) => pick(value)
                .map(Some)
                .ok_or_else(|| malformed(format!("'{key}' is not {kind}"))),
        }
    }

    fn bytes(&mut self, key: &str) -> Result<Option<&'a [u8]>, OurError> {
        self.typed(key, "a string", |v| match v {
            Value::Bytes(b) => Some(b),
            _ => None,
        })
    }

    fn int(&mut self, key: &str) -> Result<Option<i64>, OurError> {
        self.typed(key, "an integer", |v| match v {
            Value::Int(i) => Some(i),
            _ => None,
        })
    }

    fn list(&mut self, key: &str) -> Result<Option<Vec<Value<'a>>>, OurError> {
        self.typed(key, "a list", |v| match v {
            Value::List(l) => Some(l),
            _ => None,
        })
    }

    fn dict(&mut self, key: &str) -> Result<Option<Dict<'a>>, OurError> {
        self.typed(key, "a dict", |v| match v {
            Value::Dict(d) => Some(d),
            _ => None,
        })
    }

    fn required<T>(key: &str, found: Option<T>) -> Result<T, OurError> {
        found.ok_or_else(|| malformed(format!("no '{key}'")))
    }

    fn required_bytes(&mut self, key: &str) -> Result<&'a [u8], OurError> {
        let found = self.bytes(key)?;
        Self::required(key, found)
    }

    /// A 20-byte string: a node id, a target, an info hash
    fn id(&mut self, key: &str) -> Result<NodeId, OurError> {
        let raw = self.required_bytes(key)?;
        NodeId::try_from_bytes(raw).ok_or_else(|| malformed(format!("'{key}' is not 20 bytes")))
    }

    fn info_hash(&mut self, key: &str) -> Result<InfoHash, OurError> {
        Ok(InfoHash(self.id(key)?.0))
    }

    /// An integer flag that's on when 1, as BEP 33 and BEP 43 spell them; off when absent or
    /// anything else
    fn flag(&mut self, key: &str) -> bool {
        matches!(self.take(key), Some(Value::Int(1)))
    }

    /// BEP 32's `want`. Unknown entries are ignored, as BEP 32 asks, so later families can be
    /// added.
    fn want(&mut self) -> Result<Option<Want>, OurError> {
        Ok(self.list("want")?.map(|entries| {
            let mut want = Want::default();
            for entry in entries {
                match entry {
                    Value::Bytes(b"n4") => want.v4 = true,
                    Value::Bytes(b"n6") => want.v6 = true,
                    _ => {}
                }
            }
            want
        }))
    }

    /// `nodes` (IPv4, 26 bytes a node) or `nodes6` (IPv6, 38 bytes a node, BEP 32); a stub
    /// at the end and unroutable nodes are dropped
    fn nodes(&mut self, family: Family) -> Result<Option<Vec<NodeInfo>>, OurError> {
        let key = match family {
            Family::V4 => "nodes",
            Family::V6 => "nodes6",
        };
        let Some(raw) = self.bytes(key)? else {
            return Ok(None);
        };
        let len = compact_node_len(family);
        if raw.len() % len != 0 {
            debug!("`{key}` string length {} is not a multiple of {len}", raw.len());
        }
        let nodes = raw
            .chunks_exact(len)
            .filter_map(|info| {
                let node = NodeInfo::new(NodeId::try_from_bytes(&info[..20])?, parse_compact_addr(&info[20..])?);
                (!unroutable(&node.end_point())).then_some(node)
            })
            .collect();
        Ok(Some(nodes))
    }

    /// `values`: 6-byte (IPv4) or 18-byte (IPv6) strings, which BEP 32 lets a list mix; the
    /// rest are dropped
    fn peers(&mut self) -> Result<Option<Vec<SocketAddr>>, OurError> {
        let Some(values) = self.list("values")? else {
            return Ok(None);
        };
        let peers = values
            .iter()
            .filter_map(|value| match value {
                Value::Bytes(raw) => parse_compact_addr(raw),
                _ => None,
            })
            .filter(|addr| !unroutable(addr))
            .collect();
        Ok(Some(peers))
    }

    /// BEP 51's `samples`, `interval` and `num`: present when `samples` is. Whole info hashes
    /// only, the interval clamped to BEP 51's range.
    fn samples(&mut self) -> Result<Option<Samples>, OurError> {
        let Some(samples) = self.bytes("samples")? else {
            return Ok(None);
        };
        let mut int = |key| match self.take(key) {
            Some(Value::Int(i)) => i,
            _ => 0,
        };
        Ok(Some(Samples {
            interval: int("interval").clamp(0, MAX_SAMPLE_INTERVAL.into()) as u32,
            num: int("num").max(0) as u64,
            samples: samples.as_chunks::<20>().0.iter().map(|h| InfoHash(*h)).collect(),
        }))
    }

    /// BEP 33's `BFsd` and `BFpe`: both, each a whole filter, or nothing
    fn scrape(&mut self) -> Result<Option<ScrapeFilters>, OurError> {
        let seeds = self.bytes("BFsd")?.and_then(BloomFilter::from_bytes);
        let peers = self.bytes("BFpe")?.and_then(BloomFilter::from_bytes);
        Ok(seeds.zip(peers).map(|(seeds, peers)| ScrapeFilters { seeds, peers }))
    }

    /// BEP 44's item in an answer to `get`: `v`, and `k`, `seq` and `sig` if it's mutable.
    /// Those three are taken only whole; a mutable item without them doesn't verify anyway.
    fn item(&mut self) -> Result<Option<Item>, OurError> {
        let key = self.bytes("k")?.and_then(|k| k.try_into().ok());
        let seq = self.int("seq")?;
        let sig = self.bytes("sig")?.and_then(|s| s.try_into().ok());
        let Some(value) = self.take("v") else {
            return Ok(None);
        };
        let signature = match (key, seq, sig) {
            (Some(key), Some(seq), Some(sig)) => Some(ItemSignature { key, seq, sig }),
            _ => None,
        };
        Ok(Some(Item {
            value: value.encode(),
            signature,
        }))
    }

    /// BEP 44's mutable put: the key, salt, seq and signature, or `None` for an immutable one
    fn signed(&mut self) -> Result<Option<Signed>, OurError> {
        let Some(key) = self.bytes("k")? else {
            return Ok(None);
        };
        let malformed = || malformed("a mutable put needs a 32-byte k, a seq, a 64-byte sig");
        Ok(Some(Signed {
            key: key.try_into().map_err(|_| malformed())?,
            salt: self.bytes("salt")?.unwrap_or_default().to_vec(),
            seq: self.int("seq")?.ok_or_else(malformed)?,
            sig: self
                .bytes("sig")?
                .and_then(|sig| sig.try_into().ok())
                .ok_or_else(malformed)?,
        }))
    }

    fn log_unknown(&self) {
        if !self.dict.is_empty() {
            let keys: Vec<_> = self.dict.keys().map(|k| String::from_utf8_lossy(k)).collect();
            debug!("{} has unknown keys: {}", self.of, keys.join(","));
        }
    }
}

/// The query `method` with arguments `a`; `None` if we don't know the method
fn parse_query(method: &[u8], mut a: Fields) -> Result<Option<KrpcBody>, OurError> {
    let body = match method {
        b"ping" => KrpcBody::PingQuery(PingQuery::new(a.id("id")?)),
        b"find_node" => KrpcBody::FindNodeQuery(FindNodeQuery::new(a.id("id")?, a.id("target")?).with_want(a.want()?)),
        b"get_peers" => KrpcBody::GetPeersQuery(
            GetPeersQuery::new(a.id("id")?, a.info_hash("info_hash")?)
                .with_want(a.want()?)
                .with_scrape(a.flag("scrape"))
                .with_noseed(a.flag("noseed")),
        ),
        b"announce_peer" => {
            let requestor = a.id("id")?;
            // BEP 5: any non-zero value means the packet's source port
            let implied_port = matches!(a.take("implied_port"), Some(Value::Int(i)) if i != 0);
            let port = Fields::required("port", a.int("port")?)?;
            let port = u16::try_from(port).map_err(|_| malformed(format!("'port' out of range: {port}")))?;
            let token = Token::from_bytes(a.required_bytes("token")?);
            let info_hash = a.info_hash("info_hash")?;
            KrpcBody::AnnouncePeerQuery(
                AnnouncePeerQuery::new(requestor, implied_port, port, info_hash, token).with_seed(a.flag("seed")),
            )
        }
        b"sample_infohashes" => KrpcBody::SampleInfohashesQuery(
            SampleInfohashesQuery::new(a.id("id")?, a.id("target")?).with_want(a.want()?),
        ),
        b"get" => KrpcBody::GetQuery(
            GetQuery::new(a.id("id")?, a.id("target")?)
                .with_seq(a.int("seq")?)
                .with_want(a.want()?),
        ),
        b"put" => {
            let requestor = a.id("id")?;
            let token = Token::from_bytes(a.required_bytes("token")?);
            let value = Fields::required("v", a.take("v"))?.encode();
            let signed = a.signed()?;
            KrpcBody::PutQuery(PutQuery::new(requestor, token, value, signed).with_cas(a.int("cas")?))
        }
        _ => return Ok(None),
    };
    a.log_unknown();
    Ok(Some(body))
}

fn parse_response(mut r: Fields) -> Result<KrpcBody, OurError> {
    let queried = r.id("id")?;
    let nodes = r.nodes(Family::V4)?;
    let nodes6 = r.nodes(Family::V6)?;
    let values = r.peers()?;
    let token = r.bytes("token")?.map(Token::from_bytes);
    let samples = r.samples()?;
    let scrape = r.scrape()?;
    let item = r.item()?;
    r.log_unknown();

    let bare = nodes.is_none()
        && nodes6.is_none()
        && values.is_none()
        && token.is_none()
        && samples.is_none()
        && scrape.is_none()
        && item.is_none();
    if bare {
        return Ok(KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(
            queried,
        )));
    }
    Ok(KrpcBody::FindNodeGetPeersResponse(FindNodeGetPeersResponse {
        queried,
        token,
        values: values.unwrap_or_default(),
        nodes,
        nodes6,
        samples,
        scrape: scrape.map(Box::new),
        item,
    }))
}

/// `e`: the error code, then a description
fn parse_error(e: Vec<Value>) -> Result<KrpcError, OurError> {
    match e.as_slice() {
        [Value::Int(code), Value::Bytes(message), ..] => Ok(KrpcError::new(
            *code as u32,
            String::from_utf8_lossy(message).into_owned(),
        )),
        _ => Err(malformed("'e' is not a code and a description")),
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct Krpc {
    pub txn_id: TransactionId,
    pub body: KrpcBody,
    /// BEP 42: the sender's view of the recipient's external address. In a message we
    /// received it's what that node sees of us; in a response we send it's the querier.
    pub ip: Option<SocketAddr>,
    /// BEP 43's `ro`: a query from a node that answers none, so it's no use in a routing table
    pub read_only: bool,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum KrpcBody {
    AnnouncePeerQuery(AnnouncePeerQuery),
    FindNodeQuery(FindNodeQuery),
    GetPeersQuery(GetPeersQuery),
    PingQuery(PingQuery),
    SampleInfohashesQuery(SampleInfohashesQuery),
    GetQuery(GetQuery),
    PutQuery(PutQuery),

    PingAnnouncePeerResponse(PingAnnouncePeerResponse),
    FindNodeGetPeersResponse(FindNodeGetPeersResponse),

    ErrorResponse(KrpcError),
}

impl KrpcBody {
    /// The `q` of a query; `None` for responses and errors
    pub fn method(&self) -> Option<&'static str> {
        Some(match self {
            KrpcBody::AnnouncePeerQuery(_) => "announce_peer",
            KrpcBody::FindNodeQuery(_) => "find_node",
            KrpcBody::GetPeersQuery(_) => "get_peers",
            KrpcBody::PingQuery(_) => "ping",
            KrpcBody::SampleInfohashesQuery(_) => "sample_infohashes",
            KrpcBody::GetQuery(_) => "get",
            KrpcBody::PutQuery(_) => "put",
            KrpcBody::PingAnnouncePeerResponse(_)
            | KrpcBody::FindNodeGetPeersResponse(_)
            | KrpcBody::ErrorResponse(_) => return None,
        })
    }

    pub fn is_query(&self) -> bool {
        self.method().is_some()
    }

    pub fn is_response(&self) -> bool {
        matches!(
            self,
            KrpcBody::PingAnnouncePeerResponse(_) | KrpcBody::FindNodeGetPeersResponse(_)
        )
    }

    pub fn is_error(&self) -> bool {
        matches!(self, KrpcBody::ErrorResponse(_))
    }

    fn as_encodable(&self) -> &dyn ToKrpcBody {
        match self {
            KrpcBody::AnnouncePeerQuery(b) => b,
            KrpcBody::FindNodeQuery(b) => b,
            KrpcBody::GetPeersQuery(b) => b,
            KrpcBody::PingQuery(b) => b,
            KrpcBody::SampleInfohashesQuery(b) => b,
            KrpcBody::GetQuery(b) => b,
            KrpcBody::PutQuery(b) => b,
            KrpcBody::PingAnnouncePeerResponse(b) => b,
            KrpcBody::FindNodeGetPeersResponse(b) => b,
            KrpcBody::ErrorResponse(b) => b,
        }
    }
}

impl Krpc {
    pub fn new(txn_id: TransactionId, body: KrpcBody) -> Self {
        Self {
            txn_id,
            body,
            ip: None,
            read_only: false,
        }
    }

    /// The message in `raw`, a packet off the wire. A query of a method we don't know is
    /// [`OurError::UnsupportedQuery`], so it can be answered with BEP 5's 204.
    #[instrument(skip_all)]
    pub fn decode(raw: &[u8]) -> Result<Krpc, OurError> {
        let dict = bencode::parse_dict(raw).ok_or_else(|| malformed("not one bencoded dict"))?;
        let mut msg = Fields::new(dict, "message");
        let kind = msg.required_bytes("y")?;
        let txn_id = TransactionId::from_bytes(msg.required_bytes("t")?);

        let body = match kind {
            b"q" => {
                let method = msg.required_bytes("q")?;
                let args = Fields::required("a", msg.dict("a")?)?;
                match parse_query(method, Fields::new(args, "query"))? {
                    Some(body) => body,
                    None => {
                        debug!("unsupported query method: {}", String::from_utf8_lossy(method));
                        return Err(OurError::UnsupportedQuery(txn_id));
                    }
                }
            }
            b"r" => parse_response(Fields::new(Fields::required("r", msg.dict("r")?)?, "response"))?,
            b"e" => KrpcBody::ErrorResponse(parse_error(Fields::required("e", msg.list("e")?)?)?),
            other => {
                return Err(malformed(format!(
                    "unknown message type {}",
                    String::from_utf8_lossy(other)
                )));
            }
        };

        let ip = match msg.take("ip") {
            Some(Value::Bytes(raw)) => parse_compact_addr(raw),
            _ => None,
        };
        // the client's version: nothing we act on
        msg.take("v");
        let read_only = msg.flag("ro");
        msg.log_unknown();
        Ok(Krpc {
            txn_id,
            body,
            ip,
            read_only,
        })
    }

    pub fn encode(&self) -> Box<[u8]> {
        self.encode_with(None)
    }

    /// Encoded with `v`, the client version BEP 5 has every message carry
    pub(crate) fn encode_with_version(&self, version: &[u8]) -> Box<[u8]> {
        self.encode_with(Some(version))
    }

    fn encode_with(&self, version: Option<&[u8]>) -> Box<[u8]> {
        let mut enc = Encoder::new();
        enc.emit_and_sort_dict(|enc| {
            enc.emit_pair_with(b"t", |e| e.emit_bytes(&self.txn_id.0))?;
            let (kind, body_key): (&str, &[u8]) = match self.body.method() {
                Some(method) => {
                    enc.emit_pair(b"q", method)?;
                    ("q", b"a")
                }
                None if self.body.is_error() => ("e", b"e"),
                None => ("r", b"r"),
            };
            enc.emit_pair(b"y", kind)?;
            enc.emit_pair_with(body_key, |e| self.body.as_encodable().encode_body(e))?;
            if let Some(ip) = self.ip {
                enc.emit_pair_with(b"ip", |e| e.emit_bytes(&compact_addr(&ip)))?;
            }
            if self.read_only {
                enc.emit_pair(b"ro", 1)?;
            }
            if let Some(version) = version {
                enc.emit_pair_with(b"v", |e| e.emit_bytes(version))?;
            }
            Ok(())
        })
        .expect("a KRPC message is shallow, and its keys are distinct");
        enc.get_output().expect("one value was emitted").into_boxed_slice()
    }

    /// The id of the node on the other end of this message: the requestor for queries,
    /// the responder for responses. Error messages carry no id, hence the `Option`.
    pub fn node_id(&self) -> Option<NodeId> {
        match &self.body {
            KrpcBody::AnnouncePeerQuery(q) => Some(q.requestor()),
            KrpcBody::FindNodeQuery(q) => Some(q.requestor()),
            KrpcBody::GetPeersQuery(q) => Some(q.requestor()),
            KrpcBody::PingQuery(q) => Some(q.requestor()),
            KrpcBody::SampleInfohashesQuery(q) => Some(q.requestor()),
            KrpcBody::GetQuery(q) => Some(q.requestor()),
            KrpcBody::PutQuery(q) => Some(q.requestor()),
            KrpcBody::PingAnnouncePeerResponse(r) => Some(r.queried()),
            KrpcBody::FindNodeGetPeersResponse(r) => Some(r.queried()),
            KrpcBody::ErrorResponse(_) => None,
        }
    }

    pub fn transaction_id(&self) -> &TransactionId {
        &self.txn_id
    }

    pub fn is_query(&self) -> bool {
        self.body.is_query()
    }

    pub fn is_response(&self) -> bool {
        self.body.is_response()
    }

    pub fn is_error(&self) -> bool {
        self.body.is_error()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use find_node_get_peers_response::Builder;
    use std::net::{Ipv4Addr, SocketAddrV4};

    /// A message with transaction id `aa`, as BEP 5's examples have
    fn aa(body: KrpcBody) -> Krpc {
        Krpc::new(TransactionId::from_bytes(b"aa"), body)
    }

    const QUERIER: &[u8] = b"abcdefghij0123456789";
    const OTHER: &[u8] = b"mnopqrstuvwxyz123456";

    // BEP 5's examples
    #[test]
    fn can_parse_example_queries_and_ping_response() {
        let examples: [(&[u8], KrpcBody); 5] = [
            (
                b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
                KrpcBody::PingQuery(PingQuery::new(NodeId::from_bytes(QUERIER))),
            ),
            (
                b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe",
                KrpcBody::FindNodeQuery(FindNodeQuery::new(NodeId::from_bytes(QUERIER), NodeId::from_bytes(OTHER))),
            ),
            (
                b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe",
                KrpcBody::GetPeersQuery(GetPeersQuery::new(
                    NodeId::from_bytes(QUERIER),
                    InfoHash::from_bytes(OTHER),
                )),
            ),
            (
                b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe",
                KrpcBody::AnnouncePeerQuery(AnnouncePeerQuery::new(
                    NodeId::from_bytes(QUERIER),
                    true,
                    6881,
                    InfoHash::from_bytes(OTHER),
                    Token::from_bytes(b"aoeusnth"),
                )),
            ),
            (
                b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re",
                KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(NodeId::from_bytes(OTHER))),
            ),
        ];
        for (raw, body) in examples {
            let expected = aa(body);
            assert_eq!(Krpc::decode(raw).unwrap(), expected);
            assert_eq!(&*expected.encode(), raw, "and encoded, the same bytes");
        }
    }

    #[test]
    fn get_peers_success_response_deserializing() {
        let bencoded = hex::decode("64323a6970363a434545f1c8d6313a7264323a696432303a23307bc01f5e7cc56ba66314b36e69246304f870353a6e6f6465733230383a233b7b388eaded578cb8b62a1ddfef3277bf01945c202537c8d5233a010302bab6e6726e991228571f8807a9f77eb2aae6fd12a42339069106980533f8df5b5b9a17d6b704740b7bde6241279f032338bd5ff8d5779c7170d17343b8b3fe405fe71eb96b5f496d86233f9938fa19e256821896495e11e0f63ff032706ad22134e1ed233e6bd6ae529049f1f1bbe9ebb3a6db3c870ce15a9a5df9bbc8233dafab3b38a789a3e53433380dd825c45b3f57b9a76343c8d5233cddccbe1f9e5041e3b3d4d124f9c252697ef0755dab53c8d5353a746f6b656e32303a3704f7737408c5fef0f96bca389e4100f972859d363a76616c7565736c363ab28f20fc5f41363ab025e789900e363a5bd6f27f042e6565313a74323a11ec313a76343a5554b50c313a79313a7265").unwrap();
        let decoded = Krpc::decode(&bencoded).unwrap();

        let txn_id = hex::decode("11ec").unwrap();
        let txn_id = TransactionId::from_bytes(&txn_id);

        let responding = hex::decode("23307bc01f5e7cc56ba66314b36e69246304f870").unwrap();
        let responding = NodeId::from_bytes(&responding);

        let res_token = hex::decode("3704f7737408c5fef0f96bca389e4100f972859d").unwrap();
        let res_token = Token::from_bytes(&res_token);

        // the fixture also carries 8 nodes (208-byte compact string); the expected response
        // must include them
        let expected_nodes: Vec<NodeInfo> = hex::decode("233b7b388eaded578cb8b62a1ddfef3277bf01945c202537c8d5233a010302bab6e6726e991228571f8807a9f77eb2aae6fd12a42339069106980533f8df5b5b9a17d6b704740b7bde6241279f032338bd5ff8d5779c7170d17343b8b3fe405fe71eb96b5f496d86233f9938fa19e256821896495e11e0f63ff032706ad22134e1ed233e6bd6ae529049f1f1bbe9ebb3a6db3c870ce15a9a5df9bbc8233dafab3b38a789a3e53433380dd825c45b3f57b9a76343c8d5233cddccbe1f9e5041e3b3d4d124f9c252697ef0755dab53c8d5")
            .unwrap()
            .chunks(26)
            .map(|c| {
                NodeInfo::new(
                    NodeId::from_bytes(&c[..20]),
                    SocketAddrV4::new(
                        Ipv4Addr::new(c[20], c[21], c[22], c[23]),
                        u16::from_be_bytes([c[24], c[25]]),
                    ),
                )
            })
            .collect();

        let body = Builder::new(responding)
            .with_token(res_token)
            .with_value(SocketAddrV4::new(Ipv4Addr::new(178, 143, 32, 252), 24385))
            .with_value(SocketAddrV4::new(Ipv4Addr::new(176, 37, 231, 137), 36878))
            .with_value(SocketAddrV4::new(Ipv4Addr::new(91, 214, 242, 127), 1070))
            .with_nodes(&expected_nodes)
            .build();
        let body = KrpcBody::FindNodeGetPeersResponse(body);
        let expected = Krpc {
            txn_id,
            body,
            // the responder's view of the querier's address, `434545f1c8d6` in the fixture
            ip: Some(SocketAddrV4::new(Ipv4Addr::new(67, 69, 69, 241), 51414).into()),
            read_only: false,
        };

        assert_eq!(decoded, expected);
    }

    #[test]
    fn can_parse_example_find_node_response() {
        // the BEP 5 document's own example uses a legacy 20-byte `nodes` string; the
        // compact format is 26 bytes per node (id + contact), so the fixture here uses
        // that: node id "mnopqrstuvwxyz123456" at 1.2.3.4:6881
        let message =
            b"d1:rd2:id20:0123456789abcdefghij5:nodes26:mnopqrstuvwxyz123456\x01\x02\x03\x04\x1a\xe1e1:t2:aa1:y1:re"
                as &[u8];
        let decoded = Krpc::decode(message).unwrap();

        let txn_id = TransactionId::from_bytes(b"aa");
        let expected = Builder::new(NodeId::from_bytes(b"0123456789abcdefghij"))
            .with_node(NodeInfo::new(
                NodeId::from_bytes(b"mnopqrstuvwxyz123456"),
                SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 6881),
            ))
            .build();
        let body = KrpcBody::FindNodeGetPeersResponse(expected);
        let expected = Krpc::new(txn_id, body);

        assert_eq!(expected, decoded);
    }

    #[test]
    fn the_ip_field_is_parsed_and_encoded() {
        let message = b"d2:ip6:\x05\x06\x07\x08\x1a\xe11:rd2:id20:0123456789abcdefghije1:t2:aa1:y1:re" as &[u8];
        let decoded = Krpc::decode(message).unwrap();
        let seen = SocketAddrV4::new(Ipv4Addr::new(5, 6, 7, 8), 6881);
        assert_eq!(decoded.ip, Some(seen.into()));
        assert!(matches!(decoded.body, KrpcBody::PingAnnouncePeerResponse(_)));

        let again = decoded.encode();
        assert_eq!(Krpc::decode(&again).unwrap(), decoded);

        // an ipv6 node reports 18 bytes
        let message = b"d2:ip18:\x20\x01\x04\x70\x00\x01\x00\x02\x00\x00\x00\x00\x00\x00\x00\x05\x1a\xe11:rd2:id20:0123456789abcdefghije1:t2:aa1:y1:re" as &[u8];
        let decoded = Krpc::decode(message).unwrap();
        assert_eq!(decoded.ip, Some("[2001:470:1:2::5]:6881".parse().unwrap()));
        assert_eq!(Krpc::decode(&decoded.encode()).unwrap(), decoded);

        // and anything else is nothing
        let message = b"d2:ip5:\x05\x06\x07\x08\x1a1:rd2:id20:0123456789abcdefghije1:t2:aa1:y1:re" as &[u8];
        assert_eq!(Krpc::decode(message).unwrap().ip, None);
    }

    fn v6(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn nodes6_and_ipv6_values_round_trip() {
        let body = Builder::new(NodeId::from_bytes(b"0123456789abcdefghij"))
            .with_token(Token::from_bytes(b"tok"))
            .with_nodes(&[NodeInfo::new(
                NodeId::from_bytes(b"mnopqrstuvwxyz123456"),
                v6("1.2.3.4:6881"),
            )])
            .with_nodes6(&[
                NodeInfo::new(
                    NodeId::from_bytes(b"abcdefghijklmnopqrst"),
                    v6("[2001:470:1:2::5]:6881"),
                ),
                NodeInfo::new(NodeId::from_bytes(b"ABCDEFGHIJKLMNOPQRST"), v6("[2a02:752::1]:25401")),
            ])
            .with_values(&[v6("[2001:470:1:2::9]:51413"), v6("5.6.7.8:1")])
            .build();
        let msg = Krpc::new(
            TransactionId::from_bytes(b"aa"),
            KrpcBody::FindNodeGetPeersResponse(body),
        );
        let encoded = msg.encode();

        let text = String::from_utf8_lossy(&encoded);
        assert!(text.contains("6:nodes676:"), "two 38-byte nodes: {text}");
        assert!(text.contains("5:nodes26:"), "one 26-byte node: {text}");
        assert!(text.contains("6:valuesl18:"), "an 18-byte value: {text}");

        let decoded = Krpc::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
        let KrpcBody::FindNodeGetPeersResponse(res) = decoded.body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(res.nodes6()[1].end_point(), v6("[2a02:752::1]:25401"));
        assert_eq!(res.nodes_of(Family::V4).len(), 1);
    }

    #[test]
    fn an_answer_with_only_nodes6_is_a_find_node_answer() {
        let mut message = b"d1:rd2:id20:0123456789abcdefghij6:nodes638:".to_vec();
        message.extend_from_slice(b"mnopqrstuvwxyz123456");
        message.extend_from_slice(&[0x20, 0x01, 0x04, 0x70, 0, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 5, 0x1a, 0xe1]);
        message.extend_from_slice(b"e1:t2:aa1:y1:re");
        let decoded = Krpc::decode(&message).unwrap();
        let KrpcBody::FindNodeGetPeersResponse(res) = decoded.body else {
            panic!("expected a find_node/get_peers response")
        };
        assert!(res.nodes().is_empty());
        assert_eq!(
            res.nodes6(),
            &[NodeInfo::new(
                NodeId::from_bytes(b"mnopqrstuvwxyz123456"),
                v6("[2001:470:1:2::5]:6881")
            )]
        );
    }

    #[test]
    fn want_round_trips_and_ignores_what_it_does_not_know() {
        let query = FindNodeQuery::new(
            NodeId::from_bytes(b"abcdefghij0123456789"),
            NodeId::from_bytes(b"mnopqrstuvwxyz123456"),
        )
        .with_want(Some(Want::BOTH));
        let msg = Krpc::new(TransactionId::from_bytes(b"aa"), KrpcBody::FindNodeQuery(query));
        let encoded = msg.encode();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz1234564:wantl2:n42:n6ee1:q9:find_node1:t2:aa1:y1:qe"
        );
        assert_eq!(Krpc::decode(&encoded).unwrap(), msg);

        let query = GetPeersQuery::new(
            NodeId::from_bytes(b"abcdefghij0123456789"),
            InfoHash::from_bytes(b"mnopqrstuvwxyz123456"),
        )
        .with_want(Some(Want::only(Family::V6)));
        let msg = Krpc::new(TransactionId::from_bytes(b"aa"), KrpcBody::GetPeersQuery(query));
        assert_eq!(Krpc::decode(&msg.encode()).unwrap(), msg);

        // BEP 32: unknown entries are ignored
        let msg = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:wantl2:n62:n9i3eee1:q9:get_peers1:t2:aa1:y1:qe" as &[u8];
        let KrpcBody::GetPeersQuery(query) = Krpc::decode(msg).unwrap().body else {
            panic!("expected a get_peers query")
        };
        assert_eq!(query.want(), Some(Want::only(Family::V6)));
    }

    #[test]
    fn malformed_ipv6_wire_data_is_skipped_or_a_decode_error_not_a_panic() {
        // `want` that isn't a list
        let msg =
            b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz1234564:want2:n6e1:q9:find_node1:t2:aa1:y1:qe"
                as &[u8];
        assert!(Krpc::decode(msg).is_err());

        // `nodes6` that isn't a string
        let msg = b"d1:rd2:id20:0123456789abcdefghij6:nodes6i5ee1:t2:aa1:y1:re" as &[u8];
        assert!(Krpc::decode(msg).is_err());

        // `nodes6` cut short: the whole nodes are kept, the stub dropped
        let mut msg = b"d1:rd2:id20:0123456789abcdefghij6:nodes650:".to_vec();
        msg.extend_from_slice(b"mnopqrstuvwxyz123456");
        msg.extend_from_slice(&[0x20, 0x01, 0x04, 0x70, 0, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 5, 0x1a, 0xe1]);
        msg.extend_from_slice(b"twelve bytes");
        msg.extend_from_slice(b"e1:t2:aa1:y1:re");
        let KrpcBody::FindNodeGetPeersResponse(res) = Krpc::decode(&msg).unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(res.nodes6().len(), 1);

        // `nodes6` shorter than one node, `nodes` holding a 38-byte (IPv6) node: nothing usable
        let msg = b"d1:rd2:id20:0123456789abcdefghij6:nodes63:abc5:nodes38:abcdefghijklmnopqrstabcdefghijklmnopqre1:t2:aa1:y1:re" as &[u8];
        let KrpcBody::FindNodeGetPeersResponse(res) = Krpc::decode(msg).unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        assert!(res.nodes6().is_empty());
        assert_eq!(
            res.nodes().len(),
            1,
            "the first 26 bytes make a node; the rest is dropped"
        );

        // values of 17 and 19 bytes among an IPv6 one, and an IPv6 node at :: or on port 0
        let mut msg = b"d1:rd2:id20:0123456789abcdefghij5:token2:aa6:valuesl17:".to_vec();
        msg.extend_from_slice(&[1; 17]);
        msg.extend_from_slice(b"19:");
        msg.extend_from_slice(&[1; 19]);
        msg.extend_from_slice(b"18:");
        msg.extend_from_slice(&[0x20, 0x01, 0x04, 0x70, 0, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 5, 0x1a, 0xe1]);
        msg.extend_from_slice(b"18:");
        msg.extend_from_slice(&[0; 18]);
        msg.extend_from_slice(b"ee1:t2:aa1:y1:re");
        let KrpcBody::FindNodeGetPeersResponse(res) = Krpc::decode(&msg).unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(res.values(), &[v6("[2001:470:1:2::5]:6881")]);
    }

    #[test]
    fn unroutable_contacts_are_dropped() {
        // a node at 0.0.0.0 and a peer on port 0 can't be reached, so they never get in
        let message = b"d1:rd2:id20:0123456789abcdefghij5:nodes52:mnopqrstuvwxyz123456\x00\x00\x00\x00\x1a\xe1abcdefghijklmnopqrst\x01\x02\x03\x04\x1a\xe16:valuesl6:\x05\x06\x07\x08\x00\x006:\x05\x06\x07\x08\x1a\xe1ee1:t2:aa1:y1:re" as &[u8];
        let decoded = Krpc::decode(message).unwrap();
        let KrpcBody::FindNodeGetPeersResponse(body) = decoded.body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(body.nodes().len(), 1);
        assert_eq!(body.values().len(), 1);
    }

    #[test]
    fn a_live_ping_with_a_four_byte_transaction_id_of_nul_bytes() {
        let message = hex::decode("64313a6164323a696432303a8351db2997d2f0b603af85ca58ec32ad6693429a65313a71343a70696e67313a74343a706e0000313a79313a7165").unwrap();
        let decoded = Krpc::decode(&message).unwrap();
        assert_eq!(decoded.txn_id, TransactionId::from_bytes(b"pn\0\0"));
        assert!(matches!(decoded.body, KrpcBody::PingQuery(_)));
    }

    #[test]
    fn can_parse_example_generic_error() {
        let message = b"d1:eli201e24:A Generic Error Occurrede1:t2:aa1:y1:ee" as &[u8];
        let expected = aa(KrpcBody::ErrorResponse(KrpcError::new(
            201,
            "A Generic Error Occurred".to_string(),
        )));
        assert_eq!(Krpc::decode(message).unwrap(), expected);
        assert_eq!(&*expected.encode(), message);

        // a description that isn't UTF-8 is still the node's error
        let message = b"d1:eli202e2:\xff\xfee1:t2:aa1:y1:ee" as &[u8];
        let KrpcBody::ErrorResponse(e) = Krpc::decode(message).unwrap().body else {
            panic!("expected an error")
        };
        assert_eq!(e.code(), 202);
        // and one that isn't a code and a description isn't an error message
        assert!(Krpc::decode(b"d1:eli202ee1:t2:aa1:y1:ee").is_err());
    }

    #[test]
    fn malformed_wire_lengths_are_decode_errors_not_panics() {
        // ping query with a 19-byte id
        let msg = b"d1:ad2:id19:0123456789abcdefghijeq1:q4:ping1:t2:aa1:y1:qe" as &[u8];
        assert!(Krpc::decode(msg).is_err());

        // find_node query with a 3-byte target
        let msg = b"d1:ad2:id20:0123456789abcdefghij6:target3:fooeq1:q9:find_node1:t2:aa1:y1:qe" as &[u8];
        assert!(Krpc::decode(msg).is_err());

        // get_peers query with a 3-byte info_hash
        let msg = b"d1:ad2:id20:0123456789abcdefghij9:info_hash3:fooeq1:q9:get_peers1:t2:aa1:y1:qe" as &[u8];
        assert!(Krpc::decode(msg).is_err());

        // trailing bytes after an otherwise valid message
        let msg = b"d1:ad2:id20:0123456789abcdefghijeq1:q4:ping1:t2:aa1:y1:qeJUNK" as &[u8];
        assert!(Krpc::decode(msg).is_err());
    }

    #[test]
    fn short_values_entries_are_skipped() {
        // a get_peers response whose values list has a 3-byte entry among valid ones
        let msg = b"d1:rd2:id20:0123456789abcdefghij5:token2:aa6:valuesl3:abc6:\x01\x02\x03\x04\x05\x06ee1:t2:aa1:y1:re"
            as &[u8];
        let parsed = Krpc::decode(msg).unwrap();
        let KrpcBody::FindNodeGetPeersResponse(resp) = parsed.body else {
            panic!("expected a find_node/get_peers response");
        };
        assert_eq!(
            resp.values(),
            &[SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 0x0506).into()]
        );
    }

    #[test]
    fn implied_port_is_any_nonzero_value() {
        // BEP 5: implied_port counts when non-zero, not only when it is exactly 1
        let msg = b"d1:ad2:id20:abcdefghij012345678912:implied_porti2e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe" as &[u8];
        let parsed = Krpc::decode(msg).unwrap();
        let KrpcBody::AnnouncePeerQuery(query) = parsed.body else {
            panic!("expected an announce_peer query");
        };
        assert!(query.implied_port());
    }

    #[test]
    fn sample_infohashes_round_trips() {
        let query = SampleInfohashesQuery::new(
            NodeId::from_bytes(b"abcdefghij0123456789"),
            NodeId::from_bytes(b"mnopqrstuvwxyz123456"),
        );
        let msg = Krpc::new(TransactionId::from_bytes(b"aa"), KrpcBody::SampleInfohashesQuery(query));
        let encoded = msg.encode();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q17:sample_infohashes1:t2:aa1:y1:qe"
        );
        assert_eq!(Krpc::decode(&encoded).unwrap(), msg);

        let res = Builder::new(NodeId::from_bytes(b"0123456789abcdefghij"))
            .with_samples(Samples {
                interval: 900,
                num: 7,
                samples: vec![InfoHash([1; 20]), InfoHash([2; 20])],
            })
            .with_nodes(&[])
            .build();
        let msg = Krpc::new(
            TransactionId::from_bytes(b"aa"),
            KrpcBody::FindNodeGetPeersResponse(res),
        );
        let encoded = msg.encode();
        let text = String::from_utf8_lossy(&encoded);
        assert!(text.contains("8:intervali900e5:nodes0:3:numi7e7:samples40:"), "{text}");
        assert_eq!(Krpc::decode(&encoded).unwrap(), msg);

        // no samples at all is still an answer to sample_infohashes
        let msg = b"d1:rd2:id20:0123456789abcdefghij8:intervali0e3:numi0e7:samples0:e1:t2:aa1:y1:re" as &[u8];
        let KrpcBody::FindNodeGetPeersResponse(res) = Krpc::decode(msg).unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(res.samples().unwrap().samples, vec![]);
    }

    #[test]
    fn malformed_bep_51_wire_data_is_trimmed_or_a_decode_error_not_a_panic() {
        // no target
        let msg = b"d1:ad2:id20:abcdefghij0123456789e1:q17:sample_infohashes1:t2:aa1:y1:qe" as &[u8];
        assert!(Krpc::decode(msg).is_err());
        // a short target
        let msg = b"d1:ad2:id20:abcdefghij01234567896:target3:abce1:q17:sample_infohashes1:t2:aa1:y1:qe" as &[u8];
        assert!(Krpc::decode(msg).is_err());
        // samples that aren't a string
        let msg = b"d1:rd2:id20:0123456789abcdefghij7:samplesi5ee1:t2:aa1:y1:re" as &[u8];
        assert!(Krpc::decode(msg).is_err());
        // 30 bytes of samples, a negative num, an interval past 6 hours, and no interval at all
        let mut msg = b"d1:rd2:id20:0123456789abcdefghij8:intervali99999999e3:numi-4e7:samples30:".to_vec();
        msg.extend_from_slice(&[7; 30]);
        msg.extend_from_slice(b"e1:t2:aa1:y1:re");
        let KrpcBody::FindNodeGetPeersResponse(res) = Krpc::decode(&msg).unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        let samples = res.samples().unwrap();
        assert_eq!(samples.samples, vec![InfoHash([7; 20])]);
        assert_eq!(samples.num, 0);
        assert_eq!(samples.interval, MAX_SAMPLE_INTERVAL);
        let msg = b"d1:rd2:id20:0123456789abcdefghij7:samples0:e1:t2:aa1:y1:re" as &[u8];
        let KrpcBody::FindNodeGetPeersResponse(res) = Krpc::decode(msg).unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(res.samples().unwrap().interval, 0);
    }

    #[test]
    fn bep_33_flags_and_filters_round_trip() {
        let query = GetPeersQuery::new(
            NodeId::from_bytes(b"abcdefghij0123456789"),
            InfoHash::from_bytes(b"mnopqrstuvwxyz123456"),
        )
        .with_scrape(true)
        .with_noseed(true);
        let msg = Krpc::new(TransactionId::from_bytes(b"aa"), KrpcBody::GetPeersQuery(query));
        let encoded = msg.encode();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234566:noseedi1e6:scrapei1ee1:q9:get_peers1:t2:aa1:y1:qe"
        );
        assert_eq!(Krpc::decode(&encoded).unwrap(), msg);

        let announce = AnnouncePeerQuery::new(
            NodeId::from_bytes(b"abcdefghij0123456789"),
            false,
            6881,
            InfoHash::from_bytes(b"mnopqrstuvwxyz123456"),
            Token::from_bytes(b"tok"),
        )
        .with_seed(true);
        let msg = Krpc::new(TransactionId::from_bytes(b"aa"), KrpcBody::AnnouncePeerQuery(announce));
        let encoded = msg.encode();
        assert!(String::from_utf8_lossy(&encoded).contains("4:seedi1e"));
        assert_eq!(Krpc::decode(&encoded).unwrap(), msg);

        let mut filters = ScrapeFilters::default();
        filters.seeds.insert("1.2.3.4".parse().unwrap());
        filters.peers.insert("2001:db8::1".parse().unwrap());
        let res = Builder::new(NodeId::from_bytes(b"0123456789abcdefghij"))
            .with_token(Token::from_bytes(b"tok"))
            .with_value("5.6.7.8:1".parse::<SocketAddr>().unwrap())
            .with_scrape(filters)
            .build();
        let msg = Krpc::new(
            TransactionId::from_bytes(b"aa"),
            KrpcBody::FindNodeGetPeersResponse(res),
        );
        let encoded = msg.encode();
        assert!(String::from_utf8_lossy(&encoded).contains("4:BFpe256:"));
        assert_eq!(Krpc::decode(&encoded).unwrap(), msg);
    }

    #[test]
    fn malformed_bep_33_wire_data_is_ignored_or_a_decode_error_not_a_panic() {
        // flags that aren't 1 are off
        let msg = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234566:noseed1:16:scrapei2ee1:q9:get_peers1:t2:aa1:y1:qe" as &[u8];
        let KrpcBody::GetPeersQuery(query) = Krpc::decode(msg).unwrap().body else {
            panic!("expected a get_peers query")
        };
        assert!(!query.scrape() && !query.noseed());

        // a filter of the wrong length, or one without the other: no filters
        let mut msg = b"d1:rd4:BFpe3:abc4:BFsd256:".to_vec();
        msg.extend_from_slice(&[0xff; 256]);
        msg.extend_from_slice(b"2:id20:0123456789abcdefghij5:token3:toke1:t2:aa1:y1:re");
        let KrpcBody::FindNodeGetPeersResponse(res) = Krpc::decode(&msg).unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(res.scrape(), None);
        let mut msg = b"d1:rd4:BFsd256:".to_vec();
        msg.extend_from_slice(&[0xff; 256]);
        msg.extend_from_slice(b"2:id20:0123456789abcdefghij5:token3:toke1:t2:aa1:y1:re");
        let KrpcBody::FindNodeGetPeersResponse(res) = Krpc::decode(&msg).unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(res.scrape(), None);

        // a filter that isn't a string
        let msg = b"d1:rd4:BFpei1e2:id20:0123456789abcdefghij5:token3:toke1:t2:aa1:y1:re" as &[u8];
        assert!(Krpc::decode(msg).is_err());
    }

    #[test]
    fn bep_43s_ro_flag_round_trips_and_only_1_counts() {
        let mut msg = aa(KrpcBody::PingQuery(PingQuery::new(NodeId::from_bytes(QUERIER))));
        msg.read_only = true;
        let encoded = msg.encode();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "d1:ad2:id20:abcdefghij0123456789e1:q4:ping2:roi1e1:t2:aa1:y1:qe"
        );
        assert_eq!(Krpc::decode(&encoded).unwrap(), msg);

        let msg = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping2:roi2e1:t2:aa1:y1:qe" as &[u8];
        assert!(!Krpc::decode(msg).unwrap().read_only);
        let msg = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping2:ro1:11:t2:aa1:y1:qe" as &[u8];
        assert!(!Krpc::decode(msg).unwrap().read_only);
    }

    #[test]
    fn bep_44_get_and_put_round_trip() {
        let id = NodeId::from_bytes(b"abcdefghij0123456789");
        let get = GetQuery::new(id, NodeId::from_bytes(b"mnopqrstuvwxyz123456")).with_seq(Some(4));
        let msg = Krpc::new(TransactionId::from_bytes(b"aa"), KrpcBody::GetQuery(get));
        let encoded = msg.encode();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "d1:ad2:id20:abcdefghij01234567893:seqi4e6:target20:mnopqrstuvwxyz123456e1:q3:get1:t2:aa1:y1:qe"
        );
        assert_eq!(Krpc::decode(&encoded).unwrap(), msg);

        // the value goes out as itself, not as a string of its bytes
        let put = PutQuery::new(id, Token::from_bytes(b"tok"), b"d1:ai1ee".to_vec(), None);
        let msg = Krpc::new(TransactionId::from_bytes(b"aa"), KrpcBody::PutQuery(put));
        let encoded = msg.encode();
        assert!(String::from_utf8_lossy(&encoded).contains("1:vd1:ai1ee"));
        assert_eq!(Krpc::decode(&encoded).unwrap(), msg);

        let signed = Signed {
            key: [1; 32],
            salt: b"foobar".to_vec(),
            seq: 9,
            sig: [2; 64],
        };
        let put =
            PutQuery::new(id, Token::from_bytes(b"tok"), b"12:Hello World!".to_vec(), Some(signed)).with_cas(Some(8));
        let msg = Krpc::new(TransactionId::from_bytes(b"aa"), KrpcBody::PutQuery(put));
        assert_eq!(Krpc::decode(&msg.encode()).unwrap(), msg);

        let res = Builder::new(id)
            .with_token(Token::from_bytes(b"tok"))
            .with_nodes(&[])
            .with_item(Item {
                value: b"li1ei2ee".to_vec(),
                signature: Some(ItemSignature {
                    key: [3; 32],
                    seq: 1,
                    sig: [4; 64],
                }),
            })
            .build();
        let msg = Krpc::new(
            TransactionId::from_bytes(b"aa"),
            KrpcBody::FindNodeGetPeersResponse(res),
        );
        assert_eq!(Krpc::decode(&msg.encode()).unwrap(), msg);
    }

    #[test]
    fn malformed_bep_44_wire_data_is_a_decode_error_or_dropped_not_a_panic() {
        // a put without v, with a short k, with k but no sig
        let msgs: [&[u8]; 3] = [
            b"d1:ad2:id20:abcdefghij01234567895:token3:toke1:q3:put1:t2:aa1:y1:qe",
            b"d1:ad2:id20:abcdefghij01234567891:k3:abc3:seqi1e3:sig3:abc5:token3:tok1:vi1ee1:q3:put1:t2:aa1:y1:qe",
            b"d1:ad2:id20:abcdefghij01234567891:k32:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3:seqi1e5:token3:tok1:vi1ee1:q3:put1:t2:aa1:y1:qe",
        ];
        for msg in msgs {
            assert!(Krpc::decode(msg).is_err(), "{}", String::from_utf8_lossy(msg));
        }
        // a get with a short target, or a seq that isn't a number
        let msg = b"d1:ad2:id20:abcdefghij01234567896:target3:abce1:q3:get1:t2:aa1:y1:qe" as &[u8];
        assert!(Krpc::decode(msg).is_err());
        let msg = b"d1:ad2:id20:abcdefghij01234567893:seq1:16:target20:mnopqrstuvwxyz123456e1:q3:get1:t2:aa1:y1:qe"
            as &[u8];
        assert!(Krpc::decode(msg).is_err());
        // an answer whose k is short: the value, unsigned
        let msg =
            b"d1:rd2:id20:0123456789abcdefghij1:k3:abc3:seqi1e3:sig3:abc5:token3:tok1:vi7ee1:t2:aa1:y1:re" as &[u8];
        let KrpcBody::FindNodeGetPeersResponse(res) = Krpc::decode(msg).unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        let item = res.item().unwrap();
        assert_eq!((item.value.as_slice(), &item.signature), (b"i7e".as_slice(), &None));
    }

    #[test]
    fn an_integer_past_i64_is_a_decode_error_not_a_panic() {
        let msg = b"d1:ad2:id20:abcdefghij01234567894:porti99999999999999999999ee1:q4:ping1:t2:aa1:y1:qe" as &[u8];
        assert!(Krpc::decode(msg).is_err());
        let msg = b"d1:rd2:id20:0123456789abcdefghij3:seqi-99999999999999999999ee1:t2:aa1:y1:re" as &[u8];
        assert!(Krpc::decode(msg).is_err());
    }

    #[test]
    fn out_of_range_announce_port_is_a_decode_error() {
        let msg = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti70000e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe" as &[u8];
        assert!(Krpc::decode(msg).is_err());
    }
}
