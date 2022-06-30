use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::error::Error;
use std::fmt::{Display, Formatter};
use juicy_bencode::BencodeItemView;
use serde::ser::{SerializeMap};
use serde_with::{serde_as, Bytes};
use serde::{Deserialize, Serialize, Serializer};
use crate::message::query::{FindNodeArgs, GetPeersArgs, PingArgs, QueryType};
use crate::message::response::{FindNodeResponse, GetPeersResponse, GetPeersResponseType, PingResponse, ResponseType};

pub type PeerContact = [u8; 6];
pub type NodeId = [u8; 20];
pub type InfoHash = [u8; 20];
pub type TransactionId = [u8; 2];
pub type Token = [u8; 20];


#[derive(Debug)]
pub struct CompactNodeContact {
    pub node_id: NodeId,
    pub peer_contact: PeerContact,
}


impl Serialize for CompactNodeContact {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let mut byte_representation: [u8; 26] = [0; 26];

        byte_representation[0..20].copy_from_slice(&self.node_id);
        byte_representation[20..26].copy_from_slice(&self.peer_contact);

        serializer.serialize_bytes(&byte_representation)
    }
}


enum MessageType<'a> {
    Query(QueryType),
    Response(ResponseType<'a>),
    Error,
}

/// The common parts of all KRPC messages
pub struct Message<'a> {
    transaction_id: TransactionId,
    message: MessageType<'a>,
}

#[derive(Debug)]
pub struct ParseError {
    message: String,
}


impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "error while trying to parse a KRPC message, reason: {}", self.message)
    }
}

impl Error for ParseError {}

impl Message<'static> {
    pub fn new_ping_query(transaction_id: TransactionId, querying_id: NodeId) -> Message<'static> {
        Message {
            transaction_id,
            message: MessageType::Query(
                QueryType::Ping(
                    PingArgs {
                        id: querying_id,
                    }
                )
            ),
        }
    }

    pub fn new_find_node_query(transaction_id: TransactionId, querying_id: NodeId, target_id: NodeId) -> Message<'static> {
        Message {
            transaction_id,
            message: MessageType::Query(
                QueryType::FindNode(
                    FindNodeArgs {
                        id: querying_id,
                        target: target_id,
                    }
                )
            ),
        }
    }

    pub fn new_get_peers_query(transaction_id: TransactionId, querying_id: NodeId, info_hash: InfoHash) -> Message<'static> {
        Message {
            transaction_id,
            message: MessageType::Query(
                QueryType::GetPeers(
                    GetPeersArgs {
                        id: querying_id,
                        info_hash,
                    }
                )
            ),
        }
    }

    #[allow(unused_variables)]
    pub fn new_announce_peer_query(transaction_id: TransactionId, querying_id: NodeId, info_hash: InfoHash, port: u16) -> Message<'static> {
        // this is impossible under NAT
        unimplemented!()
    }
}

impl<'a> Message<'a> {
    pub fn new_ping_response(transaction_id: TransactionId, responding_id: &'a NodeId) -> Message<'a> {
        Message {
            transaction_id,
            message: MessageType::Response(
                ResponseType::Ping(
                    PingResponse {
                        id: responding_id,
                    }
                )
            ),
        }
    }

    /// construct a response to a find_node query
    pub fn new_find_node_response(transaction_id: TransactionId, responding_id: &'a NodeId, nodes: Vec<&'a CompactNodeContact>) -> Message<'a> {
        Message {
            transaction_id,
            message: MessageType::Response(
                ResponseType::FindNode(
                    FindNodeResponse {
                        id: responding_id,
                        nodes,
                    }
                )
            ),
        }
    }

    /// construct a response to a get_peers query when the peer is directly found
    pub fn new_get_peers_success_response(transaction_id: TransactionId,
                                          responding_id: &'a NodeId,
                                          response_token: &'a Token,
                                          node: &'a CompactNodeContact) -> Message<'a> {
        Message {
            transaction_id,
            message: MessageType::Response(
                ResponseType::GetPeers(
                    GetPeersResponse {
                        id: responding_id,
                        token: response_token,
                        response: GetPeersResponseType::Success(node),
                    }
                )
            ),
        }
    }

    /// construct a response to a get_peers query when the peer is not directly found and the closest
    /// nodes are returned
    pub fn new_get_peers_deferred_response(transaction_id: TransactionId,
                                           responding_id: &'a NodeId,
                                           response_token: &'a Token,
                                           closest_nodes: Vec<&'a CompactNodeContact>) -> Message<'a> {
        Message {
            transaction_id,
            message: MessageType::Response(
                ResponseType::GetPeers(
                    GetPeersResponse {
                        id: responding_id,
                        token: response_token,
                        response: GetPeersResponseType::Deferred(closest_nodes),
                    }
                )
            ),
        }
    }

    /// parse a bencoded KRPC response message from bytes, it will not accept any other type of
    /// message
    pub fn parse_response_from_bytes(input: &'a [u8], resquest_id: &TransactionId) -> Result<Message<'a>, Box<dyn Error + 'a>> {
        let (remaining, parsed_tree) = juicy_bencode::parse_bencode_dict(input)?;

        // transaction_id cannot be empty
        let transaction_id = parsed_tree.get(b"t".as_slice()).ok_or(ParseError {
            message: "transaction_id is missing".to_string(),
        })?;

        let transction_id = match transaction_id {
            BencodeItemView::ByteString(transaction_id) => {
                if transaction_id.len() != 2 {
                    return Err(ParseError {
                        message: "transaction_id is not 2 bytes long".to_string(),
                    }.into());
                } else {
                    transaction_id.clone()
                }
            }
            _ => {
                return Err(ParseError {
                    message: "transaction_id is not a byte array".to_string(),
                }.into());
            }
        };


        // we only parse responses, anything else is an error
        let message_type = parsed_tree.get(b"y".as_slice()).ok_or(ParseError {
            message: "message_type is missing".to_string(),
        })?;


        let message_type = match message_type {
            BencodeItemView::ByteString(b"r") => {
                b"r".as_slice().clone()
            }
            _ => {
                return Err(ParseError {
                    message: "message_type is not a byte string containing 'r' exactly".to_string(),
                }.into());
            }
        };

        let response_body = parsed_tree.get(b"r".as_slice()).ok_or(ParseError {
            message: "response_body is missing".to_string(),
        })?;

        let response_body = match response_body {
            BencodeItemView::Dictionary(response_body) => {
                response_body
            }
            _ => {
                return Err(ParseError {
                    message: "response_body is not a dictionary".to_string(),
                }.into());
            }
        };


        todo!()
    }


    /// extracts the
    fn extract_response_common(input: &'a [u8]) -> Result<(&'a TransactionId, [u8; 1], BTreeMap<&'a [u8], BencodeItemView<'a>>), Box<dyn Error + 'a>> {
        let (_, mut parsed_tree) = juicy_bencode::parse_bencode_dict(input)?;
        if parsed_tree.len() < 3 {
            return Err(ParseError {
                message: "response is missing required fields".to_string(),
            }.into());
        }

        let transaction_id = parsed_tree.get(b"t".as_slice()).ok_or(ParseError {
            message: "transaction_id is missing".to_string(),
        })?;
        let transaction_id: &TransactionId = match *transaction_id {
            BencodeItemView::ByteString(transaction_id) => {
                transaction_id.try_into()?
            }
            _ => {
                return Err(ParseError {
                    message: "transaction_id is not a byte array".to_string(),
                }.into());
            }
        };

        let message_type = parsed_tree.get(b"y".as_slice()).ok_or(ParseError {
            message: "message_type is missing".to_string(),
        })?;
        let message_type = match message_type {
            BencodeItemView::ByteString(b"r") => {
                [b'r']
            }
            _ => {
                return Err(ParseError {
                    message: "message_type is not a byte string containing 'r' exactly".to_string(),
                }.into());
            }
        };

        let response_body = parsed_tree.remove(b"r".as_slice()).ok_or(ParseError {
            message: "response_body is missing".to_string(),
        })?;

        let response_body = match response_body {
            BencodeItemView::Dictionary(response_body) => {
                response_body
            }
            _ => {
                return Err(ParseError {
                    message: "response_body is not a dictionary".to_string(),
                }.into());
            }
        };

        Ok((transaction_id, message_type, response_body))
    }


    fn parse_ping_response(transaction_id: TransactionId, response_body: &'a BTreeMap<&'a [u8], BencodeItemView<'a>>) -> Result<Message<'a>, Box<dyn Error + 'a>>
    {
        if response_body.len() != 1 {
            return Err(ParseError {
                message: "ping response should be just one key".to_string(),
            }.into());
        }

        let ping_response = response_body.get(b"id".as_slice()).ok_or(ParseError {
            message: "ping response is missing".to_string(),
        })?;
        let our_id: &NodeId = match *ping_response {
            BencodeItemView::ByteString(queried_id) => {
                queried_id.try_into()?
            }
            _ => {
                return Err(ParseError {
                    message: "ping response is not a byte array".to_string(),
                }.into());
            }
        };

        Ok(Message {
            transaction_id,
            message: MessageType::Response(
                ResponseType::Ping(
                    PingResponse {
                        id: our_id,
                    }
                )
            ),
        })
    }

    fn parse_find_node_response(transction_id: TransactionId, response_body: &'a BTreeMap<&'a [u8], BencodeItemView<'a>>) -> Result<Message<'a>, Box<dyn Error + 'a>>
    {
        todo!()
    }
}


impl<'a> Serialize for Message<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
    {
        let mut message_fields = serializer.serialize_map(None)?;


        // all messages have a transaction_id as key "t"
        // the stupid conversion to string is because without it otherwise, serde thinks it's an
        // array of numbers
        message_fields.serialize_entry("t", String::from_utf8_lossy(&self.transaction_id).as_ref())?;

        match &self.message {
            MessageType::Query(query) => {
                message_fields.serialize_entry("y", "q")?;
                match query {
                    QueryType::Ping(args) => {
                        message_fields.serialize_entry("q", "ping")?;
                        message_fields.serialize_entry("a", &args)?;
                    }
                    QueryType::FindNode(args) => {
                        message_fields.serialize_entry("q", "find_node")?;
                        message_fields.serialize_entry("a", &args)?;
                    }
                    QueryType::GetPeers(args) => {
                        message_fields.serialize_entry("q", "get_peers")?;
                        message_fields.serialize_entry("a", &args)?;
                    }
                    QueryType::AnnouncePeer(args) => {
                        message_fields.serialize_entry("q", "announce_peer")?;
                        message_fields.serialize_entry("a", &args)?;
                    }
                };
            }
            MessageType::Response(_) => { message_fields.serialize_entry("y", "r")?; }
            MessageType::Error => { message_fields.serialize_entry("y", "e")?; }
        }

        message_fields.end()
    }
}

pub mod query {
    use super::*;
    use serde::Serialize;

    pub enum QueryType {
        Ping(PingArgs),
        FindNode(FindNodeArgs),
        GetPeers(GetPeersArgs),
        // TODO: this is basically impossible under NAT
        AnnouncePeer(AnnouncePeerArgs),
    }

    #[serde_as]
    #[derive(Debug, Serialize)]
    pub struct PingArgs {
        #[serde_as(as = "Bytes")]
        pub id: NodeId,
    }

    #[serde_as]
    #[derive(Debug, Serialize)]
    pub struct FindNodeArgs {
        #[serde_as(as = "Bytes")]
        pub id: NodeId,

        #[serde_as(as = "Bytes")]
        pub target: NodeId,
    }

    #[serde_as]
    #[derive(Debug, Serialize)]
    pub struct GetPeersArgs {
        #[serde_as(as = "Bytes")]
        pub id: NodeId,

        #[serde_as(as = "Bytes")]
        pub info_hash: InfoHash,
    }

    #[serde_as]
    #[derive(Debug, Serialize)]
    pub struct AnnouncePeerArgs {
        #[serde_as(as = "Bytes")]
        pub id: NodeId,
        pub implied_port: u8,

        #[serde_as(as = "Bytes")]
        pub info_hash: InfoHash,
        pub port: u16,

        #[serde_as(as = "Bytes")]
        pub token: Token,
    }
}

pub mod response {
    use super::*;


    pub enum ResponseType<'a> {
        Ping(PingResponse<'a>),
        FindNode(FindNodeResponse<'a>),
        GetPeers(GetPeersResponse<'a>),
        // can we receive this under NAT?
        AnnouncePeer(AnnouncePeerResponse<'a>),
    }


    pub struct PingResponse<'a> {
        pub id: &'a NodeId,
    }

    pub struct FindNodeResponse<'a> {
        pub id: &'a NodeId,
        pub nodes: Vec<&'a CompactNodeContact>,
    }


    pub struct GetPeersResponse<'a> {
        pub id: &'a NodeId,
        pub token: &'a Token,
        pub response: GetPeersResponseType<'a>,
    }

    pub enum GetPeersResponseType<'a> {
        Success(&'a CompactNodeContact),
        Deferred(Vec<&'a CompactNodeContact>),
    }

    pub struct AnnouncePeerResponse<'a> {
        pub id: &'a NodeId,
    }
}

pub mod error {
    pub enum Error {
        Generic,
        ServerError,
        ProtocolError,
        MethodUnknown,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bendy::serde::{to_bytes};


    #[test]
    fn serialize_ping_query() {
        let message = Message::new_ping_query(b"aa".clone(), b"abcdefghij0123456789".clone());
        let bytes = to_bytes(&message).unwrap();

        // taken directly from the spec
        assert_eq!(bytes, b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe");
    }

    #[test]
    fn serialize_find_node_query() {
        let message = Message::new_find_node_query(b"aa".clone(), b"abcdefghij0123456789".clone(), b"mnopqrstuvwxyz123456".clone());
        let bytes = to_bytes(&message).unwrap();

        // taken directly from the spec
        assert_eq!(bytes, b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe");
    }

    #[test]
    fn serialize_get_peers_query() {
        let message = Message::new_get_peers_query(b"aa".clone(), b"abcdefghij0123456789".clone(), b"mnopqrstuvwxyz123456".clone());
        let bytes = to_bytes(&message).unwrap();

        // taken directly from the spec
        assert_eq!(bytes, b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe");
    }
}
