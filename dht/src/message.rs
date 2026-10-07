//! The KRPC wire protocol (BEP 5): bencoded dicts with a transaction id (`t`), a message
//! type (`y` = q/r/e), and a body. Parsing is one pass with juicy_bencode,
//! juicy_bencode borrows the fields. KRPC responses are not self-describing, so both
//! find_node and get_peers responses map to one [`FindNodeGetPeersResponse`] struct.

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::our_error::OurError;
use crate::types::{Family, NodeInfo, Token, TransactionId};

use bendy::encoding::{Encoder, SingleItemEncoder};
use bendy::value;
use eyre::eyre;
use find_node_get_peers_response::{Builder, FindNodeGetPeersResponse};
use ping_announce_peer_response::PingAnnouncePeerResponse;
use tracing::{info, instrument};

use crate::bloom::BloomFilter;
use crate::message::announce_peer_query::AnnouncePeerQuery;
use crate::message::error::KrpcError;
use crate::message::find_node_query::FindNodeQuery;
use crate::message::get_peers_query::GetPeersQuery;
use crate::message::ping_query::PingQuery;
use crate::message::sample_infohashes_query::SampleInfohashesQuery;
use crate::types::{InfoHash, NodeId};
use find_node_get_peers_response::{Item, ItemSignature, MAX_SAMPLE_INTERVAL, Samples, ScrapeFilters};
use item_queries::{GetQuery, PutQuery, Signed};
use juicy_bencode::{BencodeItemView, parse_bencode_dict};

pub mod announce_peer_query;
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
    let (ip, port) = match raw.len() {
        6 => (
            IpAddr::V4(Ipv4Addr::from(<[u8; 4]>::try_from(&raw[..4]).ok()?)),
            &raw[4..],
        ),
        18 => (
            IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(&raw[..16]).ok()?)),
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
        let mut wanted: Vec<&[u8]> = vec![];
        if self.v4 {
            wanted.push(b"n4");
        }
        if self.v6 {
            wanted.push(b"n6");
        }
        enc.emit_list(|e| wanted.iter().try_for_each(|w| e.emit_bytes(w)))
    }
}

/// Unknown entries are ignored, as BEP 32 asks, so later families can be added.
fn extract_want(arguments: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<Option<Want>, OurError> {
    let Some(want) = arguments.remove(&b"want".as_slice()) else {
        return Ok(None);
    };
    let BencodeItemView::List(want) = want else {
        return Err(OurError::DecodeError(eyre!("'want' key is not a list")));
    };
    let mut wanted = Want::default();
    for item in want {
        match item {
            BencodeItemView::ByteString(b"n4") => wanted.v4 = true,
            BencodeItemView::ByteString(b"n6") => wanted.v6 = true,
            _ => {}
        }
    }
    Ok(Some(wanted))
}

pub trait ToKrpcBody {
    fn encode_body(&self, enc: SingleItemEncoder);
}

pub trait ParseKrpc {
    fn parse(&self) -> Result<Krpc, OurError>;
}

fn extract_error_content(body: &Vec<BencodeItemView>) -> Result<KrpcError, OurError> {
    let code = body.first().ok_or(OurError::DecodeError(eyre!(
        "Error message has no first elem/error code"
    )))?;

    let message = body.get(1).ok_or(OurError::DecodeError(eyre!(
        "Error message has no second elem/description"
    )))?;

    let BencodeItemView::Integer(code) = code else {
        return Err(OurError::DecodeError(eyre!("First element is not an int")));
    };

    let BencodeItemView::ByteString(message) = message else {
        return Err(OurError::DecodeError(eyre!("Second element is not a binary string")));
    };
    let message = str::from_utf8(message)
        .map_err(|_e| OurError::DecodeError(eyre!("error description is not valid utf8")))?
        .to_string();

    let code: u32 = *code as u32;
    Ok(KrpcError::new(code, message))
}

fn extract_node_id(argument: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<NodeId, OurError> {
    let querier = argument
        .remove(&b"id".as_slice())
        .ok_or(OurError::DecodeError(eyre!("query doesn't have an `id` key")))?;

    let BencodeItemView::ByteString(querier) = querier else {
        return Err(OurError::DecodeError(eyre!("'id' key is not a binary string")));
    };

    NodeId::try_from_bytes(querier).ok_or(OurError::DecodeError(eyre!("'id' key is not 20 bytes")))
}

fn report_unused_keys<V>(dict: &BTreeMap<&[u8], V>, err_template: &'static str) {
    if !dict.is_empty() {
        let keys = dict
            .keys()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .collect::<Vec<_>>()
            .join(",");
        info!("{err_template}: {keys}");
    }
}

fn extract_ping(arguments: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<PingQuery, OurError> {
    let node_id = extract_node_id(arguments)?;
    let ping = PingQuery::new(node_id);

    report_unused_keys(arguments, "Ping query body has unused keys");
    Ok(ping)
}

fn extract_find_node(arguments: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<FindNodeQuery, OurError> {
    let querier = extract_node_id(arguments)?;

    let target = arguments
        .remove(&b"target".as_slice())
        .ok_or(OurError::DecodeError(eyre!("Query message has no 'target' key")))?;
    let BencodeItemView::ByteString(target) = target else {
        return Err(OurError::DecodeError(eyre!("'target' key is not a binary string")));
    };

    let target = NodeId::try_from_bytes(target).ok_or(OurError::DecodeError(eyre!("'target' key is not 20 bytes")))?;
    let find_node_request = FindNodeQuery::new(querier, target).with_want(extract_want(arguments)?);

    report_unused_keys(arguments, "Find_node query body has unused keys");
    Ok(find_node_request)
}

fn extract_sample_infohashes(
    arguments: &mut BTreeMap<&[u8], BencodeItemView>,
) -> Result<SampleInfohashesQuery, OurError> {
    let querier = extract_node_id(arguments)?;
    let Some(BencodeItemView::ByteString(target)) = arguments.remove(&b"target".as_slice()) else {
        return Err(OurError::DecodeError(eyre!("sample_infohashes has no 'target' string")));
    };
    let target = NodeId::try_from_bytes(target).ok_or(OurError::DecodeError(eyre!("'target' key is not 20 bytes")))?;
    let query = SampleInfohashesQuery::new(querier, target).with_want(extract_want(arguments)?);

    report_unused_keys(arguments, "sample_infohashes query body has unused keys");
    Ok(query)
}

/// BEP 51's `samples`, `interval` and `num`: present when `samples` is. Whole info hashes
/// only, the interval clamped to BEP 51's range.
fn extract_samples(response: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<Option<Samples>, OurError> {
    let Some(samples) = response.remove(b"samples".as_slice()) else {
        return Ok(None);
    };
    let BencodeItemView::ByteString(samples) = samples else {
        return Err(OurError::DecodeError(eyre!("'samples' key is not a binary string")));
    };
    let int = |v: Option<BencodeItemView>| match v {
        Some(BencodeItemView::Integer(i)) => Some(i),
        _ => None,
    };
    let interval = int(response.remove(b"interval".as_slice())).unwrap_or(0);
    let num = int(response.remove(b"num".as_slice())).unwrap_or(0);
    Ok(Some(Samples {
        interval: interval.clamp(0, MAX_SAMPLE_INTERVAL.into()) as u32,
        num: num.max(0) as u64,
        samples: samples.as_chunks::<20>().0.iter().map(|h| InfoHash(*h)).collect(),
    }))
}

/// `view` bencoded again. For canonical bencode (sorted keys, which BEP 44 values must be) it's
/// the bytes it was parsed from, which is what signatures and targets are computed over.
pub(crate) fn encode_view(view: &BencodeItemView) -> Vec<u8> {
    fn write(view: &BencodeItemView, out: &mut Vec<u8>) {
        match view {
            BencodeItemView::Integer(i) => out.extend_from_slice(format!("i{i}e").as_bytes()),
            BencodeItemView::ByteString(s) => {
                out.extend_from_slice(format!("{}:", s.len()).as_bytes());
                out.extend_from_slice(s);
            }
            BencodeItemView::List(items) => {
                out.push(b'l');
                items.iter().for_each(|item| write(item, out));
                out.push(b'e');
            }
            BencodeItemView::Dictionary(dict) => {
                out.push(b'd');
                for (k, v) in dict {
                    write(&BencodeItemView::ByteString(k), out);
                    write(v, out);
                }
                out.push(b'e');
            }
        }
    }
    let mut out = vec![];
    write(view, &mut out);
    out
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

fn extract_int(dict: &mut BTreeMap<&[u8], BencodeItemView>, key: &[u8]) -> Result<Option<i64>, OurError> {
    match dict.remove(key) {
        None => Ok(None),
        Some(BencodeItemView::Integer(i)) => Ok(Some(i)),
        Some(_) => Err(OurError::DecodeError(eyre!(
            "'{}' key is not an integer",
            String::from_utf8_lossy(key)
        ))),
    }
}

fn extract_bytes<'a>(
    dict: &mut BTreeMap<&[u8], BencodeItemView<'a>>,
    key: &[u8],
) -> Result<Option<&'a [u8]>, OurError> {
    match dict.remove(key) {
        None => Ok(None),
        Some(BencodeItemView::ByteString(s)) => Ok(Some(s)),
        Some(_) => Err(OurError::DecodeError(eyre!(
            "'{}' key is not a binary string",
            String::from_utf8_lossy(key)
        ))),
    }
}

fn extract_get(arguments: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<GetQuery, OurError> {
    let querier = extract_node_id(arguments)?;
    let target = extract_bytes(arguments, b"target")?
        .and_then(NodeId::try_from_bytes)
        .ok_or(OurError::DecodeError(eyre!("get has no 20-byte 'target'")))?;
    let query = GetQuery::new(querier, target)
        .with_seq(extract_int(arguments, b"seq")?)
        .with_want(extract_want(arguments)?);
    report_unused_keys(arguments, "get query body has unused keys");
    Ok(query)
}

fn extract_put(arguments: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<PutQuery, OurError> {
    let querier = extract_node_id(arguments)?;
    let token = extract_bytes(arguments, b"token")?.ok_or(OurError::DecodeError(eyre!("put has no 'token'")))?;
    let value = arguments
        .remove(b"v".as_slice())
        .ok_or(OurError::DecodeError(eyre!("put has no 'v'")))?;
    let signed = match extract_bytes(arguments, b"k")? {
        None => None,
        Some(key) => {
            let malformed = || OurError::DecodeError(eyre!("a mutable put needs a 32-byte k, a seq, a 64-byte sig"));
            Some(Signed {
                key: key.try_into().map_err(|_| malformed())?,
                salt: extract_bytes(arguments, b"salt")?.unwrap_or_default().to_vec(),
                seq: extract_int(arguments, b"seq")?.ok_or_else(malformed)?,
                sig: extract_bytes(arguments, b"sig")?
                    .and_then(|sig| sig.try_into().ok())
                    .ok_or_else(malformed)?,
            })
        }
    };
    let query = PutQuery::new(querier, Token::from_bytes(token), encode_view(&value), signed)
        .with_cas(extract_int(arguments, b"cas")?);
    report_unused_keys(arguments, "put query body has unused keys");
    Ok(query)
}

/// BEP 44's item in an answer to `get`: `v`, and `k`, `seq` and `sig` if it's mutable. Those
/// three are taken only whole; a mutable item without them doesn't verify anyway.
fn extract_item(response: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<Option<Item>, OurError> {
    let key = extract_bytes(response, b"k")?.and_then(|k| <[u8; 32]>::try_from(k).ok());
    let seq = extract_int(response, b"seq")?;
    let sig = extract_bytes(response, b"sig")?.and_then(|s| <[u8; 64]>::try_from(s).ok());
    let Some(value) = response.remove(b"v".as_slice()) else {
        return Ok(None);
    };
    let signature = match (key, seq, sig) {
        (Some(key), Some(seq), Some(sig)) => Some(ItemSignature { key, seq, sig }),
        _ => None,
    };
    Ok(Some(Item {
        value: encode_view(&value),
        signature,
    }))
}

/// An integer flag that's on when 1, as BEP 33 and BEP 43 spell them; off when absent or
/// anything else
fn extract_flag(dict: &mut BTreeMap<&[u8], BencodeItemView>, key: &[u8]) -> bool {
    matches!(dict.remove(key), Some(BencodeItemView::Integer(1)))
}

/// BEP 33's `BFsd` and `BFpe`: both, each a whole filter, or nothing
fn extract_scrape(response: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<Option<ScrapeFilters>, OurError> {
    let mut filter = |key: &[u8]| match response.remove(key) {
        None => Ok(None),
        Some(BencodeItemView::ByteString(raw)) => Ok(BloomFilter::from_bytes(raw)),
        Some(_) => Err(OurError::DecodeError(eyre!(
            "'{}' key is not a binary string",
            String::from_utf8_lossy(key)
        ))),
    };
    let seeds = filter(b"BFsd")?;
    let peers = filter(b"BFpe")?;
    Ok(seeds.zip(peers).map(|(seeds, peers)| ScrapeFilters { seeds, peers }))
}

fn extract_get_peers(arguments: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<GetPeersQuery, OurError> {
    let querier = extract_node_id(arguments)?;

    let info_hash = arguments
        .remove(&b"info_hash".as_slice())
        .ok_or(OurError::DecodeError(eyre!("Query message has no 'info_hash' key")))?;

    let BencodeItemView::ByteString(info_hash) = info_hash else {
        return Err(OurError::DecodeError(eyre!("'info_hash' key is not a binary string")));
    };

    let info_hash =
        InfoHash::try_from_bytes(info_hash).ok_or(OurError::DecodeError(eyre!("'info_hash' key is not 20 bytes")))?;
    let get_peers = GetPeersQuery::new(querier, info_hash)
        .with_want(extract_want(arguments)?)
        .with_scrape(extract_flag(arguments, b"scrape"))
        .with_noseed(extract_flag(arguments, b"noseed"));

    report_unused_keys(arguments, "Get_peers query body has unused keys");
    Ok(get_peers)
}

fn extract_announce_peer(arguments: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<AnnouncePeerQuery, OurError> {
    let querier = extract_node_id(arguments)?;

    let implied_port = arguments.remove(&b"implied_port".as_slice());
    let implied_port = match implied_port {
        // BEP 5: any non-zero value means use the packet's origin port
        Some(BencodeItemView::Integer(i)) => i != 0,
        _ => false,
    };

    let port = arguments.remove(&b"port".as_slice());
    let Some(BencodeItemView::Integer(port)) = port else {
        return Err(OurError::DecodeError(eyre!("'port' key is not a number")));
    };
    let port = u16::try_from(port).map_err(|_| OurError::DecodeError(eyre!("'port' out of range: {port}")))?;

    let token = arguments
        .remove(&b"token".as_slice())
        .ok_or(OurError::DecodeError(eyre!("Query message has no 'token' key")))?;
    let BencodeItemView::ByteString(token) = token else {
        return Err(OurError::DecodeError(eyre!("'token' key is not a binary string")));
    };
    let token = Token::from_bytes(token);

    // todo: look at the non compliant ones
    let info_hash = arguments
        .remove(&b"info_hash".as_slice())
        .ok_or(OurError::DecodeError(eyre!("Query message has no 'info_hash' key")))?;
    let BencodeItemView::ByteString(info_hash) = info_hash else {
        return Err(OurError::DecodeError(eyre!("'info_hash' key is not a binary string")));
    };

    let info_hash =
        InfoHash::try_from_bytes(info_hash).ok_or(OurError::DecodeError(eyre!("'info_hash' key is not 20 bytes")))?;
    let announce_peer = AnnouncePeerQuery::new(querier, implied_port, port, info_hash, token)
        .with_seed(extract_flag(arguments, b"seed"));

    report_unused_keys(arguments, "Announce_peer query body has unused keys");
    Ok(announce_peer)
}

/// `nodes` (IPv4, 26 bytes a node) or `nodes6` (IPv6, 38 bytes a node, BEP 32)
fn extract_nodes(
    response: &mut BTreeMap<&[u8], BencodeItemView>,
    family: Family,
) -> Result<Option<Vec<NodeInfo>>, OurError> {
    let key: &[u8] = match family {
        Family::V4 => b"nodes",
        Family::V6 => b"nodes6",
    };
    let Some(compact_nodes) = response.remove(key) else {
        return Ok(None);
    };

    let BencodeItemView::ByteString(nodes) = compact_nodes else {
        return Err(OurError::DecodeError(eyre!(
            "'{}' key is not a binary string",
            String::from_utf8_lossy(key)
        )));
    };

    let len = compact_node_len(family);
    if nodes.len() % len != 0 {
        info!(
            "`{}` string length {} is not a multiple of {len}",
            String::from_utf8_lossy(key),
            nodes.len()
        );
    }
    let contacts: Vec<_> = nodes
        .chunks_exact(len)
        .filter_map(|info| {
            let node_id = NodeId::try_from_bytes(&info[..20])?;
            let contact = parse_compact_addr(&info[20..])?;
            if contact.ip().is_unspecified() || contact.port() == 0 {
                return None;
            }
            Some(NodeInfo::new(node_id, contact))
        })
        .collect();

    Ok(Some(contacts))
}

/// Peers come as 6-byte (IPv4) or 18-byte (IPv6) strings; BEP 32 lets a list mix them.
fn extract_peers(response: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<Option<Vec<SocketAddr>>, OurError> {
    let values = response.remove(&b"values".as_slice());
    let Some(values) = values else {
        return Ok(None);
    };

    let BencodeItemView::List(values) = values else {
        return Err(OurError::DecodeError(eyre!("'values' key is not a list")));
    };

    let contacts: Vec<SocketAddr> = values
        .iter()
        .filter_map(|x| match x {
            BencodeItemView::ByteString(s) => Some(s),
            _ => {
                info!("Encoutered one element in `values` list that isn't a string");
                None
            }
        })
        .filter_map(|sock_addr| {
            let Some(addr) = parse_compact_addr(sock_addr) else {
                info!("Encoutered one string in `values` list that is neither 6 nor 18 bytes long");
                return None;
            };
            if addr.ip().is_unspecified() || addr.port() == 0 {
                return None;
            }
            Some(addr)
        })
        .collect();

    Ok(Some(contacts))
}

fn extract_token(response: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<Option<Token>, OurError> {
    let token = response.remove(&b"token".as_slice());
    let Some(token) = token else {
        return Ok(None);
    };

    let BencodeItemView::ByteString(token) = token else {
        return Err(OurError::DecodeError(eyre!("'token' key is not a binary string")));
    };
    let token = Token::from_bytes(token);
    Ok(Some(token))
}

impl ParseKrpc for &[u8] {
    /// parse out a krpc message we can do something with
    #[instrument(skip(self))]
    fn parse(&self) -> Result<Krpc, OurError> {
        // dicts with unsorted keys are accepted: plenty of live nodes send them
        let (unused, mut parsed) =
            parse_bencode_dict(self).map_err(|e| OurError::DecodeError(eyre!("nom complained: {e}")))?;
        if !unused.is_empty() {
            return Err(OurError::DecodeError(eyre!("trailing bytes after the bencode dict")));
        }

        let message_type_indicator = parsed
            .remove(b"y".as_slice())
            .ok_or(OurError::DecodeError(eyre!("Message as no 'y' key")))?;
        let BencodeItemView::ByteString(message_type) = message_type_indicator else {
            return Err(OurError::DecodeError(eyre!("Message 'y' key is not a binary string")));
        };

        let transaction_id = parsed
            .remove(&b"t".as_slice())
            .ok_or(OurError::DecodeError(eyre!("Message has no 't' key")))?;
        let BencodeItemView::ByteString(transaction_id) = transaction_id else {
            return Err(OurError::DecodeError(eyre!("Message 't' key is not a binary string")));
        };
        let txn_id = TransactionId::from_bytes(transaction_id);

        let body = if message_type == b"e" {
            // Error message
            let error_body = parsed
                .remove(b"e".as_slice())
                .ok_or(OurError::DecodeError(eyre!("Error message has no 'e' key")))?;
            let BencodeItemView::List(code_and_message) = error_body else {
                return Err(OurError::DecodeError(eyre!("'e' key is not a list")));
            };

            KrpcBody::ErrorResponse(extract_error_content(&code_and_message)?)
        } else if message_type == b"q" {
            // queries
            let query_type: Box<[u8]> = {
                let query_type = parsed
                    .remove(&b"q".as_slice())
                    .ok_or(OurError::DecodeError(eyre!("Query message has no 'q' key")))?;

                match query_type {
                    BencodeItemView::ByteString(query_type) => query_type.to_vec().into_boxed_slice(),
                    _ => return Err(OurError::DecodeError(eyre!("'q' key is not a binary string"))),
                }
            };

            let arguments = parsed
                .remove(&b"a".as_slice())
                .ok_or(OurError::DecodeError(eyre!("Query message has no 'a' key")))?;
            let BencodeItemView::Dictionary(mut arguments) = arguments else {
                return Err(OurError::DecodeError(eyre!("'a' key is not a dict")));
            };

            if &*query_type == b"ping" {
                KrpcBody::PingQuery(extract_ping(&mut arguments)?)
            } else if &*query_type == b"find_node" {
                KrpcBody::FindNodeQuery(extract_find_node(&mut arguments)?)
            } else if &*query_type == b"get_peers" {
                KrpcBody::GetPeersQuery(extract_get_peers(&mut arguments)?)
            } else if &*query_type == b"announce_peer" {
                KrpcBody::AnnouncePeerQuery(extract_announce_peer(&mut arguments)?)
            } else if &*query_type == b"sample_infohashes" {
                KrpcBody::SampleInfohashesQuery(extract_sample_infohashes(&mut arguments)?)
            } else if &*query_type == b"get" {
                KrpcBody::GetQuery(extract_get(&mut arguments)?)
            } else if &*query_type == b"put" {
                KrpcBody::PutQuery(extract_put(&mut arguments)?)
            } else {
                let query_type = String::from_utf8_lossy(&query_type);
                info!("Unsupported query type: {query_type}");
                return Err(OurError::UnsupportedQuery(txn_id));
            }
        } else if message_type == b"r" {
            // responses
            let body = parsed
                .remove(b"r".as_slice())
                .ok_or(OurError::DecodeError(eyre!("Response message has no 'r' key")))?;
            let BencodeItemView::Dictionary(mut response) = body else {
                return Err(OurError::DecodeError(eyre!("'r' key is not a dict")));
            };

            let id = response
                .remove(b"id".as_slice())
                .ok_or(OurError::DecodeError(eyre!("Response message has no 'id' key")))?;
            let BencodeItemView::ByteString(target_id) = id else {
                return Err(OurError::DecodeError(eyre!("'id' key is not a binary string")));
            };
            let target_id = NodeId::try_from_bytes(target_id)
                .ok_or(OurError::DecodeError(eyre!("response 'id' key is not 20 bytes")))?;

            let nodes = extract_nodes(&mut response, Family::V4)?;
            let nodes6 = extract_nodes(&mut response, Family::V6)?;
            let values = extract_peers(&mut response)?;
            let token = extract_token(&mut response)?;
            let samples = extract_samples(&mut response)?;
            let scrape = extract_scrape(&mut response)?;
            let item = extract_item(&mut response)?;

            if nodes.is_none()
                && nodes6.is_none()
                && values.is_none()
                && token.is_none()
                && samples.is_none()
                && scrape.is_none()
                && item.is_none()
            {
                // when they have none of these, then it's just a response to ping to announce query
                KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(target_id))
            } else {
                // otherwise it's response to get_peers or find_node
                let builder = Builder::new(target_id);

                let builder = match token {
                    Some(token) => builder.with_token(token),
                    None => builder,
                };

                let builder = match nodes {
                    Some(nodes) => builder.with_nodes(&nodes),
                    None => builder,
                };

                let builder = match nodes6 {
                    Some(nodes6) => builder.with_nodes6(&nodes6),
                    None => builder,
                };

                let builder = match values {
                    Some(values) => builder.with_values(&values),
                    None => builder,
                };

                let builder = match samples {
                    Some(samples) => builder.with_samples(samples),
                    None => builder,
                };

                let builder = match scrape {
                    Some(scrape) => builder.with_scrape(scrape),
                    None => builder,
                };

                let builder = match item {
                    Some(item) => builder.with_item(item),
                    None => builder,
                };

                KrpcBody::FindNodeGetPeersResponse(builder.build())
            }
        } else {
            // invalid message
            let message_type = String::from_utf8_lossy(message_type);
            return Err(OurError::DecodeError(eyre!("Unknown message type: {message_type}")));
        };

        let ip = match parsed.remove(b"ip".as_slice()) {
            Some(BencodeItemView::ByteString(raw)) => parse_compact_addr(raw),
            _ => None,
        };
        let _ = parsed.remove(b"v".as_slice()); // user agent string
        let read_only = extract_flag(&mut parsed, b"ro");

        if !parsed.is_empty() {
            let keys = parsed
                .keys()
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .collect::<Vec<_>>()
                .join(",");
            info!("Message has unused fields at top level: {keys}");
        }
        Ok(Krpc {
            txn_id,
            body,
            ip,
            read_only,
        })
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
    pub fn is_response(&self) -> bool {
        matches!(
            self,
            KrpcBody::PingAnnouncePeerResponse(_) | KrpcBody::FindNodeGetPeersResponse(_)
        )
    }

    pub fn is_error(&self) -> bool {
        matches!(self, KrpcBody::ErrorResponse(_))
    }

    pub fn is_query(&self) -> bool {
        matches!(
            self,
            KrpcBody::PingQuery(_)
                | KrpcBody::FindNodeQuery(_)
                | KrpcBody::GetPeersQuery(_)
                | KrpcBody::AnnouncePeerQuery(_)
                | KrpcBody::SampleInfohashesQuery(_)
                | KrpcBody::GetQuery(_)
                | KrpcBody::PutQuery(_)
        )
    }
}

impl Krpc {
    pub fn encode(&self) -> Box<[u8]> {
        self.encode_with_additional(&HashMap::new())
    }

    //
    // TODO: maybe allow it to take in a list of additional fields to encode
    pub fn encode_with_additional(&self, additional: &HashMap<&[u8], value::Value>) -> Box<[u8]> {
        let mut enc = Encoder::new();

        enc.emit_and_sort_dict(|enc| {
            enc.emit_pair_with(b"t", |enc| enc.emit_bytes(&self.txn_id.0))?;

            match &self.body {
                KrpcBody::AnnouncePeerQuery(q) => {
                    enc.emit_pair(b"y", "q")?;
                    enc.emit_pair(b"q", "announce_peer")?;
                    enc.emit_pair_with(b"a", |e| {
                        let _: () = q.encode_body(e);
                        Ok(())
                    })?;
                }
                KrpcBody::FindNodeQuery(q) => {
                    enc.emit_pair(b"y", "q")?;
                    enc.emit_pair(b"q", "find_node")?;
                    enc.emit_pair_with(b"a", |e| {
                        let _: () = q.encode_body(e);
                        Ok(())
                    })?;
                }
                KrpcBody::GetPeersQuery(q) => {
                    enc.emit_pair(b"y", "q")?;
                    enc.emit_pair(b"q", "get_peers")?;
                    enc.emit_pair_with(b"a", |e| {
                        let _: () = q.encode_body(e);
                        Ok(())
                    })?;
                }
                KrpcBody::PingQuery(q) => {
                    enc.emit_pair(b"y", "q")?;
                    enc.emit_pair(b"q", "ping")?;
                    enc.emit_pair_with(b"a", |e| {
                        let _: () = q.encode_body(e);
                        Ok(())
                    })?;
                }
                KrpcBody::SampleInfohashesQuery(q) => {
                    enc.emit_pair(b"y", "q")?;
                    enc.emit_pair(b"q", "sample_infohashes")?;
                    enc.emit_pair_with(b"a", |e| {
                        let _: () = q.encode_body(e);
                        Ok(())
                    })?;
                }
                KrpcBody::GetQuery(q) => {
                    enc.emit_pair(b"y", "q")?;
                    enc.emit_pair(b"q", "get")?;
                    enc.emit_pair_with(b"a", |e| {
                        let _: () = q.encode_body(e);
                        Ok(())
                    })?;
                }
                KrpcBody::PutQuery(q) => {
                    enc.emit_pair(b"y", "q")?;
                    enc.emit_pair(b"q", "put")?;
                    enc.emit_pair_with(b"a", |e| {
                        let _: () = q.encode_body(e);
                        Ok(())
                    })?;
                }

                KrpcBody::PingAnnouncePeerResponse(r) => {
                    enc.emit_pair(b"y", "r")?;
                    enc.emit_pair_with(b"r", |e| {
                        let _: () = r.encode_body(e);
                        Ok(())
                    })?;
                }
                KrpcBody::FindNodeGetPeersResponse(r) => {
                    enc.emit_pair(b"y", "r")?;
                    enc.emit_pair_with(b"r", |e| {
                        let _: () = r.encode_body(e);
                        Ok(())
                    })?;
                }
                KrpcBody::ErrorResponse(err) => {
                    enc.emit_pair(b"y", "e")?;
                    enc.emit_pair_with(b"e", |e| {
                        let _: () = err.encode_body(e);
                        Ok(())
                    })?;
                }
            }

            if let Some(ip) = self.ip {
                enc.emit_pair_with(b"ip", |enc| enc.emit_bytes(&compact_addr(&ip)))?;
            }
            if self.read_only {
                enc.emit_pair(b"ro", 1)?;
            }

            for (k, v) in additional.iter() {
                enc.emit_pair(k, v)?;
            }

            Ok(())
        })
        .unwrap();

        enc.get_output().unwrap().into_boxed_slice()
    }

    pub fn set_txn_id(&mut self, txn_id: TransactionId) {
        self.txn_id = txn_id;
    }

    /// The id of the node on the other end of this message: the requestor for queries,
    /// the responder for responses. Error messages carry no id, hence the `Option`.
    pub fn node_id(&self) -> Option<NodeId> {
        match &self.body {
            KrpcBody::AnnouncePeerQuery(announce_peer_query) => Some(*announce_peer_query.requestor()),
            KrpcBody::FindNodeQuery(find_node_query) => Some(find_node_query.requestor()),
            KrpcBody::GetPeersQuery(get_peers_query) => Some(*get_peers_query.requestor()),
            KrpcBody::PingQuery(ping_query) => Some(*ping_query.requestor()),
            KrpcBody::SampleInfohashesQuery(query) => Some(query.requestor()),
            KrpcBody::GetQuery(query) => Some(query.requestor()),
            KrpcBody::PutQuery(query) => Some(query.requestor()),

            KrpcBody::PingAnnouncePeerResponse(ping_announce_peer_response) => {
                Some(*ping_announce_peer_response.target_id())
            }
            KrpcBody::FindNodeGetPeersResponse(find_node_get_peers_response) => {
                Some(*find_node_get_peers_response.queried())
            }
            KrpcBody::ErrorResponse(_) => None,
        }
    }

    pub fn is_response(&self) -> bool {
        self.body.is_response()
    }

    pub fn transaction_id(&self) -> &TransactionId {
        &self.txn_id
    }

    pub fn is_error(&self) -> bool {
        self.body.is_error()
    }

    pub fn is_query(&self) -> bool {
        self.body.is_query()
    }

    pub fn new_ping_query(transaction_id: TransactionId, querying_id: NodeId) -> Self {
        Self {
            txn_id: transaction_id,
            ip: None,
            read_only: false,
            body: KrpcBody::PingQuery(PingQuery::new(querying_id)),
        }
    }

    pub fn new_find_node_query(transaction_id: TransactionId, querying_id: NodeId, target_id: NodeId) -> Self {
        Self {
            txn_id: transaction_id,
            ip: None,
            read_only: false,
            body: KrpcBody::FindNodeQuery(FindNodeQuery::new(querying_id, target_id)),
        }
    }

    pub fn new_get_peers_query(transaction_id: TransactionId, querying_id: NodeId, info_hash: InfoHash) -> Self {
        Self {
            txn_id: transaction_id,
            ip: None,
            read_only: false,
            body: KrpcBody::GetPeersQuery(GetPeersQuery::new(querying_id, info_hash)),
        }
    }

    pub fn new_with_body(txn_id: TransactionId, body: KrpcBody) -> Self {
        Self {
            txn_id,
            body,
            ip: None,
            read_only: false,
        }
    }

    pub fn new_announce_peer_query(
        transaction_id: TransactionId,
        info_hash: InfoHash,
        querying_id: NodeId,
        port: u16,
        implied_port: bool,
        token: Token,
    ) -> Self {
        Self {
            txn_id: transaction_id,
            ip: None,
            read_only: false,
            body: KrpcBody::AnnouncePeerQuery(AnnouncePeerQuery::new(
                querying_id,
                implied_port,
                port,
                info_hash,
                token,
            )),
        }
    }

    pub fn new_ping_response(transaction_id: TransactionId, responding_id: NodeId) -> Self {
        Self {
            txn_id: transaction_id,
            ip: None,
            read_only: false,
            body: KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(responding_id)),
        }
    }

    pub fn new_announce_peer_response(transaction_id: TransactionId, responding_id: NodeId) -> Self {
        Self {
            txn_id: transaction_id,
            ip: None,
            read_only: false,
            body: KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(responding_id)),
        }
    }

    pub fn new_standard_generic_error_response(transaction_id: TransactionId) -> Self {
        Self {
            txn_id: transaction_id,
            ip: None,
            read_only: false,
            body: KrpcBody::ErrorResponse(KrpcError::new(201, "A Generic Error Occurred".to_string())),
        }
    }

    pub fn new_standard_server_error(transaction_id: TransactionId) -> Self {
        Self {
            txn_id: transaction_id,
            ip: None,
            read_only: false,
            body: KrpcBody::ErrorResponse(KrpcError::new(202, "A Server Error Occurred".to_string())),
        }
    }

    pub fn new_standard_protocol_error(transaction_id: TransactionId) -> Self {
        Self {
            txn_id: transaction_id,
            ip: None,
            read_only: false,
            body: KrpcBody::ErrorResponse(KrpcError::new(203, "A Protocol Error Occurred".to_string())),
        }
    }

    pub fn new_unsupported_error(transaction_id: TransactionId) -> Self {
        Self {
            txn_id: transaction_id,
            ip: None,
            read_only: false,
            body: KrpcBody::ErrorResponse(KrpcError::new(204, "A Unsupported Method Error Occurred".to_string())),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn can_parse_example_ping_query() {
        let message = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe" as &[u8];
        let deserialized = message.parse().unwrap();
        let expected = Krpc::new_ping_query(
            TransactionId::from_bytes(b"aa"),
            NodeId::from_bytes(b"abcdefghij0123456789"),
        );
        assert_eq!(deserialized, expected);
    }

    #[test]
    fn can_parse_example_find_node_query() {
        let message =
            b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe" as &[u8];
        let deserialized = message.parse().unwrap();

        let expected = Krpc::new_find_node_query(
            TransactionId::from_bytes(b"aa"),
            NodeId::from_bytes(b"abcdefghij0123456789"),
            NodeId::from_bytes(b"mnopqrstuvwxyz123456"),
        );

        assert_eq!(deserialized, expected);
    }

    #[test]
    fn can_parse_example_get_peers_query() {
        let message = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe"
            as &[u8];
        let deserialized = message.parse().unwrap();

        let expected = Krpc::new_get_peers_query(
            TransactionId::from_bytes(b"aa"),
            NodeId::from_bytes(b"abcdefghij0123456789"),
            InfoHash::from_bytes(b"mnopqrstuvwxyz123456"),
        );

        // taken directly from the spec
        assert_eq!(deserialized, expected);
    }

    #[test]
    fn can_parse_example_announce_peers_query() {
        let message =
                b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe" as &[u8];
        let deserialized = message.parse().unwrap();

        let expected = Krpc::new_announce_peer_query(
            TransactionId::from_bytes(b"aa"),
            InfoHash::from_bytes(b"mnopqrstuvwxyz123456"),
            NodeId::from_bytes(b"abcdefghij0123456789"),
            6881,
            true,
            Token::from_bytes(b"aoeusnth"),
        );

        // taken directly from the spec
        assert_eq!(deserialized, expected);
    }

    #[test]
    fn can_parse_example_ping_response() {
        let message = b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re" as &[u8];
        let decoded = message.parse().unwrap();

        let expected = Krpc::new_ping_response(
            TransactionId::from_bytes(b"aa"),
            NodeId::from_bytes(b"mnopqrstuvwxyz123456"),
        );
        assert_eq!(decoded, expected);
    }

    #[test]
    fn get_peers_success_response_deserializing() {
        let bencoded = hex::decode("64323a6970363a434545f1c8d6313a7264323a696432303a23307bc01f5e7cc56ba66314b36e69246304f870353a6e6f6465733230383a233b7b388eaded578cb8b62a1ddfef3277bf01945c202537c8d5233a010302bab6e6726e991228571f8807a9f77eb2aae6fd12a42339069106980533f8df5b5b9a17d6b704740b7bde6241279f032338bd5ff8d5779c7170d17343b8b3fe405fe71eb96b5f496d86233f9938fa19e256821896495e11e0f63ff032706ad22134e1ed233e6bd6ae529049f1f1bbe9ebb3a6db3c870ce15a9a5df9bbc8233dafab3b38a789a3e53433380dd825c45b3f57b9a76343c8d5233cddccbe1f9e5041e3b3d4d124f9c252697ef0755dab53c8d5353a746f6b656e32303a3704f7737408c5fef0f96bca389e4100f972859d363a76616c7565736c363ab28f20fc5f41363ab025e789900e363a5bd6f27f042e6565313a74323a11ec313a76343a5554b50c313a79313a7265").unwrap();
        let decoded = bencoded.as_slice().parse().unwrap();

        let txn_id = hex::decode("11ec").unwrap();
        let txn_id = TransactionId::from_bytes(&txn_id);

        let responding = hex::decode("23307bc01f5e7cc56ba66314b36e69246304f870").unwrap();
        let responding = NodeId::from_bytes(&responding);

        let res_token = hex::decode("3704f7737408c5fef0f96bca389e4100f972859d").unwrap();
        let res_token = Token::from_bytes(&res_token);

        use find_node_get_peers_response::Builder;

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
        let decoded = message.parse().unwrap();

        use find_node_get_peers_response::Builder;
        let txn_id = TransactionId::from_bytes(b"aa");
        let expected = Builder::new(NodeId::from_bytes(b"0123456789abcdefghij"))
            .with_node(NodeInfo::new(
                NodeId::from_bytes(b"mnopqrstuvwxyz123456"),
                SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 6881),
            ))
            .build();
        let body = KrpcBody::FindNodeGetPeersResponse(expected);
        let expected = Krpc::new_with_body(txn_id, body);

        assert_eq!(expected, decoded);
    }

    #[test]
    fn the_ip_field_is_parsed_and_encoded() {
        let message = b"d2:ip6:\x05\x06\x07\x08\x1a\xe11:rd2:id20:0123456789abcdefghije1:t2:aa1:y1:re" as &[u8];
        let decoded = message.parse().unwrap();
        let seen = SocketAddrV4::new(Ipv4Addr::new(5, 6, 7, 8), 6881);
        assert_eq!(decoded.ip, Some(seen.into()));
        assert!(matches!(decoded.body, KrpcBody::PingAnnouncePeerResponse(_)));

        let again = decoded.encode();
        assert_eq!(again.as_ref().parse().unwrap(), decoded);

        // an ipv6 node reports 18 bytes
        let message = b"d2:ip18:\x20\x01\x04\x70\x00\x01\x00\x02\x00\x00\x00\x00\x00\x00\x00\x05\x1a\xe11:rd2:id20:0123456789abcdefghije1:t2:aa1:y1:re" as &[u8];
        let decoded = message.parse().unwrap();
        assert_eq!(decoded.ip, Some("[2001:470:1:2::5]:6881".parse().unwrap()));
        assert_eq!(decoded.encode().as_ref().parse().unwrap(), decoded);

        // and anything else is nothing
        let message = b"d2:ip5:\x05\x06\x07\x08\x1a1:rd2:id20:0123456789abcdefghije1:t2:aa1:y1:re" as &[u8];
        assert_eq!(message.parse().unwrap().ip, None);
    }

    fn v6(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn nodes6_and_ipv6_values_round_trip() {
        use find_node_get_peers_response::Builder;
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
        let msg = Krpc::new_with_body(
            TransactionId::from_bytes(b"aa"),
            KrpcBody::FindNodeGetPeersResponse(body),
        );
        let encoded = msg.encode();

        let text = String::from_utf8_lossy(&encoded);
        assert!(text.contains("6:nodes676:"), "two 38-byte nodes: {text}");
        assert!(text.contains("5:nodes26:"), "one 26-byte node: {text}");
        assert!(text.contains("6:valuesl18:"), "an 18-byte value: {text}");

        let decoded = encoded.as_ref().parse().unwrap();
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
        let decoded = message.as_slice().parse().unwrap();
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
        let msg = Krpc::new_with_body(TransactionId::from_bytes(b"aa"), KrpcBody::FindNodeQuery(query));
        let encoded = msg.encode();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz1234564:wantl2:n42:n6ee1:q9:find_node1:t2:aa1:y1:qe"
        );
        assert_eq!(encoded.as_ref().parse().unwrap(), msg);

        let query = GetPeersQuery::new(
            NodeId::from_bytes(b"abcdefghij0123456789"),
            InfoHash::from_bytes(b"mnopqrstuvwxyz123456"),
        )
        .with_want(Some(Want::only(Family::V6)));
        let msg = Krpc::new_with_body(TransactionId::from_bytes(b"aa"), KrpcBody::GetPeersQuery(query));
        assert_eq!(msg.encode().as_ref().parse().unwrap(), msg);

        // BEP 32: unknown entries are ignored
        let msg = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:wantl2:n62:n9i3eee1:q9:get_peers1:t2:aa1:y1:qe" as &[u8];
        let KrpcBody::GetPeersQuery(query) = msg.parse().unwrap().body else {
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
        assert!(msg.parse().is_err());

        // `nodes6` that isn't a string
        let msg = b"d1:rd2:id20:0123456789abcdefghij6:nodes6i5ee1:t2:aa1:y1:re" as &[u8];
        assert!(msg.parse().is_err());

        // `nodes6` cut short: the whole nodes are kept, the stub dropped
        let mut msg = b"d1:rd2:id20:0123456789abcdefghij6:nodes650:".to_vec();
        msg.extend_from_slice(b"mnopqrstuvwxyz123456");
        msg.extend_from_slice(&[0x20, 0x01, 0x04, 0x70, 0, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 5, 0x1a, 0xe1]);
        msg.extend_from_slice(b"twelve bytes");
        msg.extend_from_slice(b"e1:t2:aa1:y1:re");
        let KrpcBody::FindNodeGetPeersResponse(res) = msg.as_slice().parse().unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(res.nodes6().len(), 1);

        // `nodes6` shorter than one node, `nodes` holding a 38-byte (IPv6) node: nothing usable
        let msg = b"d1:rd2:id20:0123456789abcdefghij6:nodes63:abc5:nodes38:abcdefghijklmnopqrstabcdefghijklmnopqre1:t2:aa1:y1:re" as &[u8];
        let KrpcBody::FindNodeGetPeersResponse(res) = msg.parse().unwrap().body else {
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
        let KrpcBody::FindNodeGetPeersResponse(res) = msg.as_slice().parse().unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(res.values(), &[v6("[2001:470:1:2::5]:6881")]);
    }

    #[test]
    fn unroutable_contacts_are_dropped() {
        // a node at 0.0.0.0 and a peer on port 0 can't be reached, so they never get in
        let message = b"d1:rd2:id20:0123456789abcdefghij5:nodes52:mnopqrstuvwxyz123456\x00\x00\x00\x00\x1a\xe1abcdefghijklmnopqrst\x01\x02\x03\x04\x1a\xe16:valuesl6:\x05\x06\x07\x08\x00\x006:\x05\x06\x07\x08\x1a\xe1ee1:t2:aa1:y1:re" as &[u8];
        let decoded = message.parse().unwrap();
        let KrpcBody::FindNodeGetPeersResponse(body) = decoded.body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(body.nodes().len(), 1);
        assert_eq!(body.values().len(), 1);
    }

    #[test]
    // no, I can't remember why it's called that either now
    fn oi() {
        let message = hex::decode("64313a6164323a696432303a8351db2997d2f0b603af85ca58ec32ad6693429a65313a71343a70696e67313a74343a706e0000313a79313a7165").unwrap();
        let decoded = message.as_slice().parse().unwrap();
        println!("{:?}", decoded);
    }

    #[test]
    fn can_parse_example_generic_error() {
        let message = b"d1:eli201e24:A Generic Error Occurrede1:t2:aa1:y1:ee" as &[u8];
        let decoded: Krpc = message.parse().unwrap();

        let expected = Krpc::new_standard_generic_error_response(TransactionId::from_bytes(b"aa"));
        assert_eq!(expected, decoded);
    }

    #[test]
    fn malformed_wire_lengths_are_decode_errors_not_panics() {
        // ping query with a 19-byte id
        let msg = b"d1:ad2:id19:0123456789abcdefghijeq1:q4:ping1:t2:aa1:y1:qe" as &[u8];
        assert!(msg.parse().is_err());

        // find_node query with a 3-byte target
        let msg = b"d1:ad2:id20:0123456789abcdefghij6:target3:fooeq1:q9:find_node1:t2:aa1:y1:qe" as &[u8];
        assert!(msg.parse().is_err());

        // get_peers query with a 3-byte info_hash
        let msg = b"d1:ad2:id20:0123456789abcdefghij9:info_hash3:fooeq1:q9:get_peers1:t2:aa1:y1:qe" as &[u8];
        assert!(msg.parse().is_err());

        // trailing bytes after an otherwise valid message
        let msg = b"d1:ad2:id20:0123456789abcdefghijeq1:q4:ping1:t2:aa1:y1:qeJUNK" as &[u8];
        assert!(msg.parse().is_err());
    }

    #[test]
    fn short_values_entries_are_skipped() {
        // a get_peers response whose values list has a 3-byte entry among valid ones
        let msg = b"d1:rd2:id20:0123456789abcdefghij5:token2:aa6:valuesl3:abc6:\x01\x02\x03\x04\x05\x06ee1:t2:aa1:y1:re"
            as &[u8];
        let parsed = msg.parse().unwrap();
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
        let parsed = msg.parse().unwrap();
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
        let msg = Krpc::new_with_body(TransactionId::from_bytes(b"aa"), KrpcBody::SampleInfohashesQuery(query));
        let encoded = msg.encode();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q17:sample_infohashes1:t2:aa1:y1:qe"
        );
        assert_eq!(encoded.as_ref().parse().unwrap(), msg);

        let res = Builder::new(NodeId::from_bytes(b"0123456789abcdefghij"))
            .with_samples(Samples {
                interval: 900,
                num: 7,
                samples: vec![InfoHash([1; 20]), InfoHash([2; 20])],
            })
            .with_nodes(&[])
            .build();
        let msg = Krpc::new_with_body(
            TransactionId::from_bytes(b"aa"),
            KrpcBody::FindNodeGetPeersResponse(res),
        );
        let encoded = msg.encode();
        let text = String::from_utf8_lossy(&encoded);
        assert!(text.contains("8:intervali900e5:nodes0:3:numi7e7:samples40:"), "{text}");
        assert_eq!(encoded.as_ref().parse().unwrap(), msg);

        // no samples at all is still an answer to sample_infohashes
        let msg = b"d1:rd2:id20:0123456789abcdefghij8:intervali0e3:numi0e7:samples0:e1:t2:aa1:y1:re" as &[u8];
        let KrpcBody::FindNodeGetPeersResponse(res) = msg.parse().unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(res.samples().unwrap().samples, vec![]);
    }

    #[test]
    fn malformed_bep_51_wire_data_is_trimmed_or_a_decode_error_not_a_panic() {
        // no target
        let msg = b"d1:ad2:id20:abcdefghij0123456789e1:q17:sample_infohashes1:t2:aa1:y1:qe" as &[u8];
        assert!(msg.parse().is_err());
        // a short target
        let msg = b"d1:ad2:id20:abcdefghij01234567896:target3:abce1:q17:sample_infohashes1:t2:aa1:y1:qe" as &[u8];
        assert!(msg.parse().is_err());
        // samples that aren't a string
        let msg = b"d1:rd2:id20:0123456789abcdefghij7:samplesi5ee1:t2:aa1:y1:re" as &[u8];
        assert!(msg.parse().is_err());
        // 30 bytes of samples, a negative num, an interval past 6 hours, and no interval at all
        let mut msg = b"d1:rd2:id20:0123456789abcdefghij8:intervali99999999e3:numi-4e7:samples30:".to_vec();
        msg.extend_from_slice(&[7; 30]);
        msg.extend_from_slice(b"e1:t2:aa1:y1:re");
        let KrpcBody::FindNodeGetPeersResponse(res) = msg.as_slice().parse().unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        let samples = res.samples().unwrap();
        assert_eq!(samples.samples, vec![InfoHash([7; 20])]);
        assert_eq!(samples.num, 0);
        assert_eq!(samples.interval, MAX_SAMPLE_INTERVAL);
        let msg = b"d1:rd2:id20:0123456789abcdefghij7:samples0:e1:t2:aa1:y1:re" as &[u8];
        let KrpcBody::FindNodeGetPeersResponse(res) = msg.parse().unwrap().body else {
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
        let msg = Krpc::new_with_body(TransactionId::from_bytes(b"aa"), KrpcBody::GetPeersQuery(query));
        let encoded = msg.encode();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234566:noseedi1e6:scrapei1ee1:q9:get_peers1:t2:aa1:y1:qe"
        );
        assert_eq!(encoded.as_ref().parse().unwrap(), msg);

        let announce = AnnouncePeerQuery::new(
            NodeId::from_bytes(b"abcdefghij0123456789"),
            false,
            6881,
            InfoHash::from_bytes(b"mnopqrstuvwxyz123456"),
            Token::from_bytes(b"tok"),
        )
        .with_seed(true);
        let msg = Krpc::new_with_body(TransactionId::from_bytes(b"aa"), KrpcBody::AnnouncePeerQuery(announce));
        let encoded = msg.encode();
        assert!(String::from_utf8_lossy(&encoded).contains("4:seedi1e"));
        assert_eq!(encoded.as_ref().parse().unwrap(), msg);

        let mut filters = ScrapeFilters::default();
        filters.seeds.insert("1.2.3.4".parse().unwrap());
        filters.peers.insert("2001:db8::1".parse().unwrap());
        let res = Builder::new(NodeId::from_bytes(b"0123456789abcdefghij"))
            .with_token(Token::from_bytes(b"tok"))
            .with_value("5.6.7.8:1".parse::<SocketAddr>().unwrap())
            .with_scrape(filters)
            .build();
        let msg = Krpc::new_with_body(
            TransactionId::from_bytes(b"aa"),
            KrpcBody::FindNodeGetPeersResponse(res),
        );
        let encoded = msg.encode();
        assert!(String::from_utf8_lossy(&encoded).contains("4:BFpe256:"));
        assert_eq!(encoded.as_ref().parse().unwrap(), msg);
    }

    #[test]
    fn malformed_bep_33_wire_data_is_ignored_or_a_decode_error_not_a_panic() {
        // flags that aren't 1 are off
        let msg = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234566:noseed1:16:scrapei2ee1:q9:get_peers1:t2:aa1:y1:qe" as &[u8];
        let KrpcBody::GetPeersQuery(query) = msg.parse().unwrap().body else {
            panic!("expected a get_peers query")
        };
        assert!(!query.scrape() && !query.noseed());

        // a filter of the wrong length, or one without the other: no filters
        let mut msg = b"d1:rd4:BFpe3:abc4:BFsd256:".to_vec();
        msg.extend_from_slice(&[0xff; 256]);
        msg.extend_from_slice(b"2:id20:0123456789abcdefghij5:token3:toke1:t2:aa1:y1:re");
        let KrpcBody::FindNodeGetPeersResponse(res) = msg.as_slice().parse().unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(res.scrape(), None);
        let mut msg = b"d1:rd4:BFsd256:".to_vec();
        msg.extend_from_slice(&[0xff; 256]);
        msg.extend_from_slice(b"2:id20:0123456789abcdefghij5:token3:toke1:t2:aa1:y1:re");
        let KrpcBody::FindNodeGetPeersResponse(res) = msg.as_slice().parse().unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        assert_eq!(res.scrape(), None);

        // a filter that isn't a string
        let msg = b"d1:rd4:BFpei1e2:id20:0123456789abcdefghij5:token3:toke1:t2:aa1:y1:re" as &[u8];
        assert!(msg.parse().is_err());
    }

    #[test]
    fn bep_43s_ro_flag_round_trips_and_only_1_counts() {
        let mut msg = Krpc::new_ping_query(
            TransactionId::from_bytes(b"aa"),
            NodeId::from_bytes(b"abcdefghij0123456789"),
        );
        msg.read_only = true;
        let encoded = msg.encode();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "d1:ad2:id20:abcdefghij0123456789e1:q4:ping2:roi1e1:t2:aa1:y1:qe"
        );
        assert_eq!(encoded.as_ref().parse().unwrap(), msg);

        let msg = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping2:roi2e1:t2:aa1:y1:qe" as &[u8];
        assert!(!msg.parse().unwrap().read_only);
        let msg = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping2:ro1:11:t2:aa1:y1:qe" as &[u8];
        assert!(!msg.parse().unwrap().read_only);
    }

    #[test]
    fn bep_44_get_and_put_round_trip() {
        let id = NodeId::from_bytes(b"abcdefghij0123456789");
        let get = GetQuery::new(id, NodeId::from_bytes(b"mnopqrstuvwxyz123456")).with_seq(Some(4));
        let msg = Krpc::new_with_body(TransactionId::from_bytes(b"aa"), KrpcBody::GetQuery(get));
        let encoded = msg.encode();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "d1:ad2:id20:abcdefghij01234567893:seqi4e6:target20:mnopqrstuvwxyz123456e1:q3:get1:t2:aa1:y1:qe"
        );
        assert_eq!(encoded.as_ref().parse().unwrap(), msg);

        // the value goes out as itself, not as a string of its bytes
        let put = PutQuery::new(id, Token::from_bytes(b"tok"), b"d1:ai1ee".to_vec(), None);
        let msg = Krpc::new_with_body(TransactionId::from_bytes(b"aa"), KrpcBody::PutQuery(put));
        let encoded = msg.encode();
        assert!(String::from_utf8_lossy(&encoded).contains("1:vd1:ai1ee"));
        assert_eq!(encoded.as_ref().parse().unwrap(), msg);

        let signed = Signed {
            key: [1; 32],
            salt: b"foobar".to_vec(),
            seq: 9,
            sig: [2; 64],
        };
        let put =
            PutQuery::new(id, Token::from_bytes(b"tok"), b"12:Hello World!".to_vec(), Some(signed)).with_cas(Some(8));
        let msg = Krpc::new_with_body(TransactionId::from_bytes(b"aa"), KrpcBody::PutQuery(put));
        assert_eq!(msg.encode().as_ref().parse().unwrap(), msg);

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
        let msg = Krpc::new_with_body(
            TransactionId::from_bytes(b"aa"),
            KrpcBody::FindNodeGetPeersResponse(res),
        );
        assert_eq!(msg.encode().as_ref().parse().unwrap(), msg);
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
            assert!(msg.parse().is_err(), "{}", String::from_utf8_lossy(msg));
        }
        // a get with a short target, or a seq that isn't a number
        let msg = b"d1:ad2:id20:abcdefghij01234567896:target3:abce1:q3:get1:t2:aa1:y1:qe" as &[u8];
        assert!(msg.parse().is_err());
        let msg = b"d1:ad2:id20:abcdefghij01234567893:seq1:16:target20:mnopqrstuvwxyz123456e1:q3:get1:t2:aa1:y1:qe"
            as &[u8];
        assert!(msg.parse().is_err());
        // an answer whose k is short: the value, unsigned
        let msg =
            b"d1:rd2:id20:0123456789abcdefghij1:k3:abc3:seqi1e3:sig3:abc5:token3:tok1:vi7ee1:t2:aa1:y1:re" as &[u8];
        let KrpcBody::FindNodeGetPeersResponse(res) = msg.parse().unwrap().body else {
            panic!("expected a find_node/get_peers response")
        };
        let item = res.item().unwrap();
        assert_eq!((item.value.as_slice(), &item.signature), (b"i7e".as_slice(), &None));
    }

    #[test]
    fn out_of_range_announce_port_is_a_decode_error() {
        let msg = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti70000e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe" as &[u8];
        assert!(msg.parse().is_err());
    }
}
