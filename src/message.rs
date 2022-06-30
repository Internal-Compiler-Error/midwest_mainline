use std::fmt::{Display, Formatter};
use serde_with::{serde_as, Bytes};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use crate::message::query::{AnnouncePeerArgs, FindNodeArgs, GetPeersArgs, PingArgs, QueryBody};
use crate::message::response::{AnnouncePeerResponse, FindNodeResponse, GetPeersResponse, GetPeersResponseType, PingResponse, ResponseBody};

pub type NodeId = [u8; 20];
pub type InfoHash = [u8; 20];
pub type TransactionId = [u8; 2];
pub type Token = [u8; 20];


#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactNodeContact<'a> {
    #[serde_as(as = "Bytes")]
    #[serde(borrow)]
    bytes: &'a [u8; 26],
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactPeerContact<'a> {
    #[serde_as(as = "Bytes")]
    #[serde(borrow)]
    bytes: &'a [u8; 6],
}


#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
enum MessageBody<'a> {
    #[serde(rename = "a")]
    Query(QueryBody),

    #[serde(rename = "r")]
    #[serde(borrow)]
    Response(ResponseBody<'a>),

    #[serde(rename = "e")]
    Error,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
enum MessageType {
    #[serde(rename = "q")]
    Query,
    #[serde(rename = "r")]
    Response,
    #[serde(rename = "e")]
    Error,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
enum QueryMethod {
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "find_node")]
    FindNode,
    #[serde(rename = "get_peers")]
    GetPeers,
    #[serde(rename = "announce_peer")]
    AnnouncePeer,
    // this is a massive hack, the bendy crate will treat Some(T) as a list of T, but we want a
    // single T, so this stands in for the None case
    None,
}


impl QueryMethod {
    // see the comment in the enum for why is idiotic function exists
    fn is_none(&self) -> bool {
        match self {
            QueryMethod::None => true,
            _ => false,
        }
    }

    fn not_a_query() -> QueryMethod {
        QueryMethod::None
    }
}

/// All KRPC messages are of this type
#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message<'a> {
    #[serde(rename = "t")]
    #[serde_as(as = "Bytes")]
    transaction_id: TransactionId,

    #[serde(rename = "y")]
    message_type: MessageType,

    #[serde(rename = "q")]
    #[serde(skip_serializing_if = "QueryMethod::is_none")]
    #[serde(default = "QueryMethod::not_a_query")]
    // see the comment in the enum for why it's not using Optional<T>
    query_method: QueryMethod,

    #[serde(borrow)]
    #[serde(flatten)]
    body: MessageBody<'a>,
}

impl Message<'static> {
    pub fn new_ping_query(transaction_id: TransactionId, querying_id: NodeId) -> Message<'static> {
        Message {
            transaction_id,
            message_type: MessageType::Query,
            query_method: QueryMethod::Ping,
            body: MessageBody::Query(
                QueryBody::Ping(
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
            message_type: MessageType::Query,
            query_method: QueryMethod::FindNode,
            body: MessageBody::Query(
                QueryBody::FindNode(
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
            message_type: MessageType::Query,
            query_method: QueryMethod::GetPeers,
            body: MessageBody::Query(
                QueryBody::GetPeers(
                    GetPeersArgs {
                        id: querying_id,
                        info_hash,
                    }
                )
            ),
        }
    }

    pub fn new_announce_peer_query(transaction_id: TransactionId,
                                   info_hash: InfoHash, querying_id: NodeId, port: u16, implied_port: bool, token: Token) -> Message<'static> {
        Message {
            transaction_id,
            message_type: MessageType::Query,
            query_method: QueryMethod::AnnouncePeer,
            body: MessageBody::Query(
                QueryBody::AnnouncePeer(
                    AnnouncePeerArgs {
                        id: querying_id,
                        info_hash,
                        port,
                        implied_port: if implied_port { 1 } else { 0 },
                        token,
                    }
                )
            ),
        }
    }
}

impl<'a> Message<'a> {
    pub fn new_ping_response(transaction_id: TransactionId, responding_id: &'a NodeId) -> Message<'a> {
        Message {
            transaction_id,
            message_type: MessageType::Response,
            query_method: QueryMethod::None,
            body: MessageBody::Response(
                ResponseBody::Ping(
                    PingResponse {
                        id: responding_id,
                    }
                )
            ),
        }
    }

    /// construct a response to a find_node query
    pub fn new_find_node_response(transaction_id: TransactionId, responding_id: &'a NodeId, nodes: &'a [u8]) -> Message<'a> {
        Message {
            transaction_id,
            message_type: MessageType::Response,
            query_method: QueryMethod::None,
            body: MessageBody::Response(
                ResponseBody::FindNode(
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
                                          node: Vec<CompactPeerContact<'a>>) -> Message<'a> {
        Message {
            transaction_id,
            message_type: MessageType::Response,
            query_method: QueryMethod::None,
            body: MessageBody::Response(
                ResponseBody::GetPeers(
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
                                           closest_nodes: &'a [u8]) -> Message<'a> {
        Message {
            transaction_id,
            message_type: MessageType::Response,
            query_method: QueryMethod::None,
            body: MessageBody::Response(
                ResponseBody::GetPeers(
                    GetPeersResponse {
                        id: responding_id,
                        token: response_token,
                        response: GetPeersResponseType::Deferred(closest_nodes),
                    }
                )
            ),
        }
    }

    pub fn new_announce_peer_response(transaction_id: TransactionId, responding_id: &'a NodeId) -> Message<'a> {
        Message {
            transaction_id,
            message_type: MessageType::Response,
            query_method: QueryMethod::None,
            body: MessageBody::Response(
                ResponseBody::AnnouncePeer(
                    AnnouncePeerResponse {
                        id: responding_id,
                    }
                )
            ),
        }
    }
}

pub mod query {
    use super::*;
    use serde::Serialize;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(untagged)]
    pub enum QueryBody {
        Ping(PingArgs),
        FindNode(FindNodeArgs),
        GetPeers(GetPeersArgs),
        AnnouncePeer(AnnouncePeerArgs),
    }

    #[serde_as]
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct PingArgs {
        #[serde_as(as = "Bytes")]
        pub id: NodeId,
    }

    #[serde_as]
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct FindNodeArgs {
        #[serde_as(as = "Bytes")]
        pub id: NodeId,

        #[serde_as(as = "Bytes")]
        pub target: NodeId,
    }

    #[serde_as]
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct GetPeersArgs {
        #[serde_as(as = "Bytes")]
        pub id: NodeId,

        #[serde_as(as = "Bytes")]
        pub info_hash: InfoHash,
    }

    #[serde_as]
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
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

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(untagged)]
    pub enum ResponseBody<'a> {
        #[serde(borrow)]
        Ping(PingResponse<'a>),
        #[serde(borrow)]
        FindNode(FindNodeResponse<'a>),
        #[serde(borrow)]
        GetPeers(GetPeersResponse<'a>),
        // can we receive this under NAT?
        #[serde(borrow)]
        AnnouncePeer(AnnouncePeerResponse<'a>),
    }

    #[serde_as]
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct PingResponse<'a> {
        #[serde_as(as = "Bytes")]
        #[serde(borrow)]
        pub id: &'a NodeId,
    }

    #[serde_as]
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct FindNodeResponse<'a> {
        #[serde_as(as = "Bytes")]
        #[serde(borrow)]
        pub id: &'a NodeId,

        #[serde(borrow)]
        #[serde_as(as = "Bytes")]
        pub nodes: &'a [u8],
    }


    #[serde_as]
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct GetPeersResponse<'a> {
        #[serde_as(as = "Bytes")]
        pub id: &'a NodeId,

        #[serde_as(as = "Bytes")]
        pub token: &'a Token,

        #[serde(borrow)]
        pub response: GetPeersResponseType<'a>,
    }

    #[serde_as]
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub enum GetPeersResponseType<'a> {
        #[serde(borrow)]
        #[serde(rename = "values")]
        Success(Vec<CompactPeerContact<'a>>),
        #[serde(borrow)]
        #[serde(rename = "node")]
        Deferred(&'a [u8]),
    }

    #[serde_as]
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct AnnouncePeerResponse<'a> {
        #[serde_as(as = "Bytes")]
        #[serde(borrow)]
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

    mod deserializing {
        use super::*;
        use bendy::serde::from_bytes;


        #[test]
        fn ping_response_deserializing() {
            let message = b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
            let decoded: Message = from_bytes(message).unwrap();

            let expected = Message::new_ping_response(b"aa".clone(), b"mnopqrstuvwxyz123456");
            assert_eq!(decoded, expected);
        }

        #[test]
        fn find_node_response_deserialize() {
            let message = b"d1:rd2:id20:0123456789abcdefghij5:nodes9:def456...e1:t2:aa1:y1:re";
            let decoded: Message = from_bytes(message).unwrap();

            // let expected = Message::new_find_node_response(
            //     b"aa".clone(),
            //     b"0123456789abcdefghij".clone(),
            // );
        }
    }

    mod serializing {
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
}
