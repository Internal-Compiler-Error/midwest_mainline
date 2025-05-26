use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddrV4};

use crate::our_error::OurError;
use crate::types::{NodeInfo, Token, TransactionId};
use bendy::decoding::{Decoder, Object};

use bendy::encoding::{Encoder, SingleItemEncoder};
use eyre::eyre;
use find_node_get_peers_response::{Builder, FindNodeGetPeersResponse};
use ping_announce_peer_response::PingAnnouncePeerResponse;
use tracing::{info, instrument};

use crate::message::announce_peer_query::AnnouncePeerQuery;
use crate::message::error::KrpcError;
use crate::message::find_node_query::FindNodeQuery;
use crate::message::get_peers_query::GetPeersQuery;
use crate::message::ping_query::PingQuery;
use crate::types::{InfoHash, NodeId};
use juicy_bencode::{BencodeItemView, parse_bencode_dict};

pub mod announce_peer_query;
pub mod error;
pub mod find_node_get_peers_response;
pub mod find_node_query;
pub mod get_peers_query;
pub mod ping_announce_peer_response;
pub mod ping_query;

pub trait ToKrpcBody {
    fn encode_body(&self, enc: SingleItemEncoder);
}

pub trait ParseKrpc {
    fn parse(&self) -> Result<Krpc, OurError>;
}

fn extract_error_content(body: &Vec<BencodeItemView>) -> Result<KrpcError, OurError> {
    let code = body.get(0).ok_or(OurError::DecodeError(eyre!(
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
        .map_err(|_e| OurError::DecodeError(eyre!("Fuck, utf8 bet is wrong")))?
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

    Ok(NodeId::from_bytes(querier))
}

fn report_unused_keys<V>(dict: &BTreeMap<&[u8], V>, err_template: &'static str) {
    if dict.len() != 0 {
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

    report_unused_keys(&arguments, "Ping query body has unused keys");
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

    let find_node_request = FindNodeQuery::new(querier, NodeId::from_bytes(target));

    report_unused_keys(&arguments, "Find_node query body has unused keys");
    Ok(find_node_request)
}

fn extract_get_peers(arguments: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<GetPeersQuery, OurError> {
    let querier = extract_node_id(arguments)?;

    let info_hash = arguments
        .remove(&b"info_hash".as_slice())
        .ok_or(OurError::DecodeError(eyre!("Query message has no 'info_hash' key")))?;

    let BencodeItemView::ByteString(info_hash) = info_hash else {
        return Err(OurError::DecodeError(eyre!("'info_hash' key is not a binary string")));
    };

    let get_peers = GetPeersQuery::new(querier, InfoHash::from_bytes(info_hash));

    report_unused_keys(&arguments, "Get_peers query body has unused keys");
    Ok(get_peers)
}

fn extract_announce_peer(arguments: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<AnnouncePeerQuery, OurError> {
    let querier = extract_node_id(arguments)?;

    let implied_port = arguments.remove(&b"implied_port".as_slice());
    let implied_port = match implied_port {
        Some(BencodeItemView::Integer(i)) if i == 1 => true,
        _ => false,
    };

    let port = arguments.remove(&b"port".as_slice());
    let Some(BencodeItemView::Integer(port)) = port else {
        return Err(OurError::DecodeError(eyre!("'port' key is not a number")));
    };

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

    let announce_peer = AnnouncePeerQuery::new(
        querier,
        implied_port,
        port as u16,
        InfoHash::from_bytes(info_hash),
        token,
    );

    report_unused_keys(&arguments, "Announce_peer query body has unused keys");
    Ok(announce_peer)
}

fn extract_nodes(response: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<Option<Vec<NodeInfo>>, OurError> {
    let compact_nodes = response.remove(&b"nodes".as_slice());

    let Some(compact_nodes) = compact_nodes else {
        return Ok(None);
    };

    let BencodeItemView::ByteString(nodes) = compact_nodes else {
        return Err(OurError::DecodeError(eyre!("'nodes' key is not a binary string")));
    };

    let contacts: Vec<_> = nodes
        .chunks(26)
        .filter_map(|info| {
            if info.len() != 26 {
                info!("`nodes` string length is not a product of 26, {}", nodes.len());
                return None;
            }

            let node_id = &info[0..20];
            let contact = &info[20..26];

            let node_id = NodeId::from_bytes(node_id);

            let ip = Ipv4Addr::new(contact[0], contact[1], contact[2], contact[3]);
            let port = u16::from_be_bytes([contact[4], contact[5]]);
            let contact = SocketAddrV4::new(ip, port);

            Some(NodeInfo::new(node_id, contact))
        })
        .collect();

    Ok(Some(contacts))
}

fn extract_peers(response: &mut BTreeMap<&[u8], BencodeItemView>) -> Result<Option<Vec<SocketAddrV4>>, OurError> {
    let values = response.remove(&b"values".as_slice());
    let Some(values) = values else {
        return Ok(None);
    };

    let BencodeItemView::List(values) = values else {
        return Err(OurError::DecodeError(eyre!("'values' key is not a list")));
    };

    let contacts: Vec<SocketAddrV4> = values
        .iter()
        .filter_map(|x| match x {
            BencodeItemView::ByteString(s) => Some(s),
            _ => {
                info!("Encoutered one element in `values` list that isn't a string");
                None
            }
        })
        .filter_map(|sock_addr| {
            if sock_addr.len() != 6 {
                info!("Encoutered one string in `values` list that isn't 6 bytes long");
            }

            let ip = Ipv4Addr::new(sock_addr[0], sock_addr[1], sock_addr[2], sock_addr[3]);
            let port = u16::from_be_bytes([sock_addr[4], sock_addr[5]]);

            Some(SocketAddrV4::new(ip, port))
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
        // Use bendy to validate the input is valid bencode message, extracting the fields is done
        // with juicy_bencode instead
        let mut decoder = Decoder::new(self);
        let message = decoder
            .next_object()
            .map_err(|e| OurError::BendyDecodeError(e))?
            .ok_or(OurError::DecodeError(eyre!("Empty input")))?;

        let Object::Dict(mut dict) = message else {
            return Err(OurError::DecodeError(eyre!("Message is not a dict")));
        };
        dict.consume_all()
            .map_err(|e| OurError::DecodeError(eyre!("Message structure is invalid: {e}")))?;

        let (unused, mut parsed) =
            parse_bencode_dict(self).map_err(|e| OurError::DecodeError(eyre!("nom complained: {e}")))?;
        assert!(unused.len() == 0);

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
            } else {
                let query_type = String::from_utf8_lossy(&*query_type);
                return Err(OurError::DecodeError(eyre!("Unsupported query type: {query_type}")));
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
            let target_id = NodeId::from_bytes(target_id);

            // if the message contains a "nodes", then try to parse it out
            let nodes = extract_nodes(&mut response)?;
            // if the message contains a "values" key, then try to parse it out
            let values = extract_peers(&mut response)?;
            // if the message contains a "token" key, then try to parse it out
            let token = extract_token(&mut response)?;

            if nodes.is_none() && values.is_none() && token.is_none() {
                // when they have none of these, then it's just a response to ping to announce query
                let body = KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(target_id));
                return Ok(Krpc::new_with_body(txn_id, body));
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

                let builder = match values {
                    Some(values) => builder.with_values(&values),
                    None => builder,
                };

                KrpcBody::FindNodeGetPeersResponse(builder.build())
            }
        } else {
            // invalid message
            let message_type = String::from_utf8_lossy(message_type);
            return Err(OurError::DecodeError(eyre!("Unknown message type: {message_type}")));
        };

        let _ = parsed.remove(b"ip".as_slice()); // see https://bittorrent.org/beps/bep_0042.html
        let _ = parsed.remove(b"v".as_slice()); // user agent string

        if parsed.len() != 0 {
            let keys = parsed
                .keys()
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .collect::<Vec<_>>()
                .join(",");
            info!("Message has unused fields at top level: {keys}");
        }
        Ok(Krpc::new_with_body(txn_id, body))
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct Krpc {
    pub txn_id: TransactionId,
    pub body: KrpcBody,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum KrpcBody {
    AnnouncePeerQuery(AnnouncePeerQuery),
    FindNodeQuery(FindNodeQuery),
    GetPeersQuery(GetPeersQuery),
    PingQuery(PingQuery),

    PingAnnouncePeerResponse(PingAnnouncePeerResponse),
    FindNodeGetPeersResponse(FindNodeGetPeersResponse),

    ErrorResponse(KrpcError),
}

impl KrpcBody {
    pub fn is_response(&self) -> bool {
        match self {
            KrpcBody::PingAnnouncePeerResponse(_) => true,
            KrpcBody::FindNodeGetPeersResponse(_) => true,
            _ => false,
        }
    }

    pub fn is_error(&self) -> bool {
        match self {
            KrpcBody::ErrorResponse(_) => true,
            _ => false,
        }
    }

    pub fn is_query(&self) -> bool {
        match self {
            KrpcBody::PingQuery(_) => true,
            KrpcBody::FindNodeQuery(_) => true,
            KrpcBody::GetPeersQuery(_) => true,
            KrpcBody::AnnouncePeerQuery(_) => true,
            _ => false,
        }
    }
}

impl Krpc {
    // TODO: maybe allow it to take in a list of additional fields to encode
    pub fn encode(&self) -> Box<[u8]> {
        let mut enc = Encoder::new();

        enc.emit_and_sort_dict(|enc| {
            enc.emit_pair_with(b"t", |enc| enc.emit_bytes(&self.txn_id.0))?;

            match &self.body {
                KrpcBody::AnnouncePeerQuery(q) => {
                    enc.emit_pair(b"y", "q")?;
                    enc.emit_pair(b"q", "announce_peer")?;
                    enc.emit_pair_with(b"a", |e| Ok(q.encode_body(e)))
                }
                KrpcBody::FindNodeQuery(q) => {
                    enc.emit_pair(b"y", "q")?;
                    enc.emit_pair(b"q", "find_node")?;
                    enc.emit_pair_with(b"a", |e| Ok(q.encode_body(e)))
                }
                KrpcBody::GetPeersQuery(q) => {
                    enc.emit_pair(b"y", "q")?;
                    enc.emit_pair(b"q", "get_peers")?;
                    enc.emit_pair_with(b"a", |e| Ok(q.encode_body(e)))
                }
                KrpcBody::PingQuery(q) => {
                    enc.emit_pair(b"y", "q")?;
                    enc.emit_pair(b"q", "ping")?;
                    enc.emit_pair_with(b"a", |e| Ok(q.encode_body(e)))
                }

                KrpcBody::PingAnnouncePeerResponse(r) => {
                    enc.emit_pair(b"y", "r")?;
                    enc.emit_pair_with(b"r", |e| Ok(r.encode_body(e)))
                }
                KrpcBody::FindNodeGetPeersResponse(r) => {
                    enc.emit_pair(b"y", "r")?;
                    enc.emit_pair_with(b"r", |e| Ok(r.encode_body(e)))
                }
                KrpcBody::ErrorResponse(err) => {
                    enc.emit_pair(b"y", "e")?;
                    enc.emit_pair_with(b"e", |e| Ok(err.encode_body(e)))
                }
            }
        })
        .unwrap();

        enc.get_output().unwrap().into_boxed_slice()
    }
    pub fn set_txn_id(&mut self, txn_id: TransactionId) {
        self.txn_id = txn_id;
    }

    // IS this function a good idea?
    pub fn node_id(&self) -> Option<NodeId> {
        // TODO: we really need to agree on whether to copy or share with node_id by default
        match &self.body {
            KrpcBody::AnnouncePeerQuery(announce_peer_query) => Some(*announce_peer_query.querier()),
            KrpcBody::FindNodeQuery(find_node_query) => Some(find_node_query.querier()),
            KrpcBody::GetPeersQuery(get_peers_query) => Some(*get_peers_query.querier()),
            KrpcBody::PingQuery(ping_query) => Some(*ping_query.querier()),

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
            body: KrpcBody::PingQuery(PingQuery::new(querying_id)),
        }
    }

    pub fn new_find_node_query(transaction_id: TransactionId, querying_id: NodeId, target_id: NodeId) -> Self {
        Self {
            txn_id: transaction_id,
            body: KrpcBody::FindNodeQuery(FindNodeQuery::new(querying_id, target_id)),
        }
    }

    pub fn new_get_peers_query(transaction_id: TransactionId, querying_id: NodeId, info_hash: InfoHash) -> Self {
        Self {
            txn_id: transaction_id,
            body: KrpcBody::GetPeersQuery(GetPeersQuery::new(querying_id, info_hash)),
        }
    }

    pub fn new_with_body(txn_id: TransactionId, body: KrpcBody) -> Self {
        Self { txn_id, body }
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
            body: KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(responding_id)),
        }
    }

    pub fn new_announce_peer_response(transaction_id: TransactionId, responding_id: NodeId) -> Self {
        Self {
            txn_id: transaction_id,
            body: KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(responding_id)),
        }
    }

    pub fn new_standard_generic_error_response(transaction_id: TransactionId) -> Self {
        Self {
            txn_id: transaction_id,
            body: KrpcBody::ErrorResponse(KrpcError::new(201 as u32, "A Generic Error Occurred".to_string())),
        }
    }

    pub fn new_standard_server_error(transaction_id: TransactionId) -> Self {
        Self {
            txn_id: transaction_id,
            body: KrpcBody::ErrorResponse(KrpcError::new(202 as u32, "A Server Error Occurred".to_string())),
        }
    }

    pub fn new_standard_protocol_error(transaction_id: TransactionId) -> Self {
        Self {
            txn_id: transaction_id,
            body: KrpcBody::ErrorResponse(KrpcError::new(203 as u32, "A Protocol Error Occurred".to_string())),
        }
    }

    pub fn new_unsupported_error(transaction_id: TransactionId) -> Self {
        Self {
            txn_id: transaction_id,
            body: KrpcBody::ErrorResponse(KrpcError::new(
                204 as u32,
                "A Unsupported Method Error Occurred".to_string(),
            )),
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
            TransactionId::from_bytes(*&b"aa"),
            NodeId::from_bytes(*&b"abcdefghij0123456789"),
        );
        assert_eq!(deserialized, expected);
    }

    #[test]
    fn can_parse_example_find_node_query() {
        let message =
            b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe" as &[u8];
        let deserialized = message.parse().unwrap();

        let expected = Krpc::new_find_node_query(
            TransactionId::from_bytes(*&b"aa"),
            NodeId::from_bytes(*&b"abcdefghij0123456789"),
            NodeId::from_bytes(*&b"mnopqrstuvwxyz123456"),
        );

        assert_eq!(deserialized, expected);
    }

    #[test]
    fn can_parse_example_get_peers_query() {
        let message = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe"
            as &[u8];
        let deserialized = message.parse().unwrap();

        let expected = Krpc::new_get_peers_query(
            TransactionId::from_bytes(*&b"aa"),
            NodeId::from_bytes(*&b"abcdefghij0123456789"),
            InfoHash::from_bytes(*&b"mnopqrstuvwxyz123456"),
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
            TransactionId::from_bytes(*&b"aa"),
            InfoHash::from_bytes(&*b"mnopqrstuvwxyz123456"),
            NodeId::from_bytes(&*b"abcdefghij0123456789"),
            6881,
            true,
            Token::from_bytes(*&b"aoeusnth"),
        );

        // taken directly from the spec
        assert_eq!(deserialized, expected);
    }

    #[test]
    fn can_parse_example_ping_response() {
        let message = b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re" as &[u8];
        let decoded = message.parse().unwrap();

        let expected = Krpc::new_ping_response(
            TransactionId::from_bytes(*&b"aa"),
            NodeId::from_bytes(*&b"mnopqrstuvwxyz123456"),
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
        let body = Builder::new(responding)
            .with_token(res_token)
            .with_value(SocketAddrV4::new(Ipv4Addr::new(178, 143, 32, 252), 24385))
            .with_value(SocketAddrV4::new(Ipv4Addr::new(176, 37, 231, 137), 36878))
            .with_value(SocketAddrV4::new(Ipv4Addr::new(91, 214, 242, 127), 1070))
            .build();
        let body = KrpcBody::FindNodeGetPeersResponse(body);
        let expected = Krpc::new_with_body(txn_id, body);

        assert_eq!(decoded, expected);
    }

    #[test]
    fn can_parse_example_find_node_response() {
        let message = b"d1:rd2:id20:0123456789abcdefghij5:nodes20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re" as &[u8];
        let decoded = message.parse().unwrap();

        use find_node_get_peers_response::Builder;
        let txn_id = TransactionId::from_bytes(*&b"aa");
        let expected = Builder::new(NodeId::from_bytes(*&b"0123456789abcdefghij"))
            .with_node(NodeInfo::new(
                NodeId::from_bytes(*&b"mnopqrstuvwxyz123456"),
                SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0),
            ))
            .build();
        let body = KrpcBody::FindNodeGetPeersResponse(expected);
        let expected = Krpc::new_with_body(txn_id, body);

        assert_eq!(expected, decoded);
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

        let expected = Krpc::new_standard_generic_error_response(TransactionId::from_bytes(*&b"aa"));
        assert_eq!(expected, decoded);
    }

    // #[test]
    // fn deserialize_con_compliant_get_peers() {
    //     // if all this following is painful to read, trust me, it was painful to write
    //     // and even more painful to realize this has to be supported
    //     let bencoded = hex::decode(
    //         "64323a6970363ab8972559c8d6313a7264323a696432303\
    //         a32f54e697351ff4aec29cdbaabf2fbe3467cc267353a6e6f6465733431363aa5490d805d411f43c4cd594d9\
    //         d4818c4cc675e256317945fc491513c322f8cbab9f21cb9b9c12336923f016634d8c35829b00605ab2ec7148\
    //         90ba9e6348c1409c291096bb95988d35e658c5bc15ce06915d1eefec905aa7224c1c9dbd0c4a1f990596db1f\
    //         5581ae1b13de8394dbb53bf3d886142880584f3bf1f6edbbcfeb16f5d4d07e637b49d82b743e9e2a4c47889f\
    //         f37cd2604bfd808b5d1282b6bf699d52b44c0e5275b41fc933e4acfeb49499dc6391bf2eba979b76b5a58a4d\
    //         59fecffb360095658264c7a1ec9aa9692727a8a139c04a5ba0a9d60d1e27c5cda0823f39e76275fc95001c9a\
    //         48974d45ab9647301de8a187ea8e9976ba28abc63dd68b295b6c491d6fc73087d29a2d45aebefd2155f7753e\
    //         4ddc89fb1dff65f315c327e3131966d1b4edf83f572322a6a4d9f00e525b46b5c3c98b31506518750a5c57bc\
    //         b76d11fc800ecde98a20da371fdc158350eed510bb4c7f04904ea794ef82cac713edd9f1db75403918979a7\
    //         8abacbf3267657c26e095e73f75abf9398e0f6e6bd9a26b5bda700000000000000000000000000000000000\
    //         000005778ea621cb665313a74323a025f313a79313a7265",
    //     )
    //     .unwrap();
    //
    //     let expected = Krpc::new_get_peers_deferred_response_con_compliant(
    //         hex::decode("025f")?.as_slice().try_into().unwrap(),
    //         hex::decode("32f54e697351ff4aec29cdbaabf2fbe3467cc267")?
    //             .try_into()
    //             .unwrap(),
    //         Box::from(
    //             hex::decode(
    //                 "a5490d805d411f43c4cd594d9d4818c4cc675e25631\
    //             7945fc491513c322f8cbab9f21cb9b9c12336923f016634d8c35829b00605ab2ec714890ba9e6348c140\
    //             9c291096bb95988d35e658c5bc15ce06915d1eefec905aa7224c1c9dbd0c4a1f990596db1f5581ae1b13\
    //             de8394dbb53bf3d886142880584f3bf1f6edbbcfeb16f5d4d07e637b49d82b743e9e2a4c47889ff37cd26\
    //             04bfd808b5d1282b6bf699d52b44c0e5275b41fc933e4acfeb49499dc6391bf2eba979b76b5a58a4d59f\
    //             ecffb360095658264c7a1ec9aa9692727a8a139c04a5ba0a9d60d1e27c5cda0823f39e76275fc95001c9\
    //             a48974d45ab9647301de8a187ea8e9976ba28abc63dd68b295b6c491d6fc73087d29a2d45aebefd2155f\
    //             7753e4ddc89fb1dff65f315c327e3131966d1b4edf83f572322a6a4d9f00e525b46b5c3c98b315065187\
    //             50a5c57bcb76d11fc800ecde98a20da371fdc158350eed510bb4c7f04904ea794ef82cac713edd9f1db75\
    //             403918979a78abacbf3267657c26e095e73f75abf9398e0f6e6bd9a26b5bda70000000000000000000000\
    //             0000000000000000005778ea621cb6",
    //             )?
    //             .as_slice(),
    //         ),
    //         // Box::from(hex::decode("b8972559c8d6")?.as_slice()),
    //     );
    //
    //     assert_eq!(expected, from_bytes::<Krpc>(&bencoded).unwrap());
    // }
}
