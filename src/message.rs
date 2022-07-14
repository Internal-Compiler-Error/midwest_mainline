use num_bigint::BigUint;
use serde_with::{Bytes, serde_as};
use serde::{Deserialize, Serialize};
use announce_peer_query::{AnnouncePeerArgs, AnnouncePeerQuery};
use find_node_query::{FindNodeArgs, FindNodeQuery};
use find_node_response::{FindNodeResponse, FindNodeResponseBody};
use get_peers_deferred_response::GetPeersDeferredResponse;
use get_peers_query::{GetPeersArgs, GetPeersQuery};
use get_peers_success_response::GetPeersSuccessResponse;
use ping_announce_peer_response::{PingAnnouncePeerResponse, PingAnnouncePeerResponseBody};
use ping_query::PingArgs;
use get_peers_success_response::GetPeersSuccessResponseBody;
use crate::domain_knowledge::{CompactPeerContact, NodeId};
use crate::message::ping_query::PingQuery;

pub mod ping_query;
pub mod find_node_query;
pub mod get_peers_query;
pub mod announce_peer_query;
pub mod ping_announce_peer_response;
pub mod find_node_response;
pub mod get_peers_success_response;
pub mod get_peers_deferred_response;


pub type InfoHash = [u8; 20];
pub type TransactionId = [u8; 2];
pub type Token = [u8; 20];

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Krpc {
    PingQuery(PingQuery),
    FindNodeQuery(FindNodeQuery),
    AnnouncePeerQuery(AnnouncePeerQuery),
    GetPeersQuery(GetPeersQuery),
    FindNodeResponse(FindNodeResponse),
    GetPeersSuccessResponse(GetPeersSuccessResponse),
    GetPeersDeferredResponse(GetPeersDeferredResponse),
    PingAnnouncePeerResponse(PingAnnouncePeerResponse),
    Error(Error),
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "e")]
    pub(crate) error: Vec<Box<[u8]>>,
}


#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum MessageType {
    #[serde(rename = "q")]
    Query,
    #[serde(rename = "r")]
    Response,
    #[serde(rename = "e")]
    Error,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum QueryMethod {
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


impl Krpc {
    pub fn is_response(&self) -> bool {
        match self {
            Krpc::PingAnnouncePeerResponse(_) => true,
            Krpc::FindNodeResponse(_) => true,
            Krpc::GetPeersSuccessResponse(_) => true,
            Krpc::GetPeersDeferredResponse(_) => true,
            _ => false,
        }
    }

    pub fn transaction_id(&self) -> TransactionId {
        match self {
            Krpc::PingAnnouncePeerResponse(msg) => msg.transaction_id,
            Krpc::FindNodeResponse(msg) => msg.transaction_id,
            Krpc::GetPeersSuccessResponse(msg) => msg.transaction_id,
            Krpc::GetPeersDeferredResponse(msg) => msg.transaction_id,
            Krpc::FindNodeQuery(msg) => msg.transaction_id,
            Krpc::GetPeersQuery(msg) => msg.transaction_id,
            Krpc::AnnouncePeerQuery(msg) => msg.transaction_id,
            Krpc::PingQuery(msg) => msg.transaction_id,
            Krpc::Error(msg) => msg.transaction_id,
        }
    }

    pub fn is_error(&self) -> bool {
        match self {
            Krpc::Error(_) => true,
            _ => false,
        }
    }

    pub fn is_query(&self) -> bool {
        match self {
            Krpc::PingQuery(_) => true,
            Krpc::FindNodeQuery(_) => true,
            Krpc::GetPeersQuery(_) => true,
            Krpc::AnnouncePeerQuery(_) => true,
            _ => false,
        }
    }

    pub fn new_ping_query(transaction_id: TransactionId, querying_id: NodeId) -> Krpc {
        let ping_query = PingQuery {
            transaction_id,
            message_type: Box::new(b"q".clone()),
            query_method: QueryMethod::Ping,
            body: PingArgs {
                id: querying_id
            },
        };

        Krpc::PingQuery(ping_query)
    }


    pub fn new_find_node_query(transaction_id: TransactionId, querying_id: NodeId, target_id: NodeId) -> Krpc {
        let find_node_query = FindNodeQuery {
            transaction_id,
            message_type: Box::new(b"q".clone()),
            query_method: QueryMethod::FindNode,
            body: FindNodeArgs {
                id: querying_id,
                target: target_id,
            },
        };

        Krpc::FindNodeQuery(find_node_query)
    }


    pub fn new_get_peers_query(transaction_id: TransactionId, querying_id: NodeId, info_hash: InfoHash) -> Krpc {
        let get_peers_query = GetPeersQuery {
            transaction_id,
            message_type: Box::new(b"q".clone()),
            query_method: QueryMethod::GetPeers,
            body: GetPeersArgs {
                id: querying_id,
                info_hash,
            },
        };

        Krpc::GetPeersQuery(get_peers_query)
    }

    pub fn new_announce_peer_query(transaction_id: TransactionId,
                                   info_hash: InfoHash,
                                   querying_id: NodeId,
                                   port: u16,
                                   implied_port: bool,
                                   token: Token) -> Krpc {
        let announce_peer_query = AnnouncePeerQuery {
            transaction_id,
            message_type: Box::new(b"q".clone()),
            query_method: QueryMethod::AnnouncePeer,
            body: AnnouncePeerArgs {
                id: querying_id,
                implied_port: if implied_port { 1 } else { 0 },
                info_hash,
                port,
                token,
            },
        };

        Krpc::AnnouncePeerQuery(announce_peer_query)
    }

    pub fn new_ping_response(transaction_id: TransactionId, responding_id: NodeId) -> Krpc {
        let ping_response = PingAnnouncePeerResponse {
            transaction_id,
            message_type: Box::new(b"r".clone()),
            body: PingAnnouncePeerResponseBody {
                id: responding_id,
            },
        };
        Krpc::PingAnnouncePeerResponse(ping_response)
    }

    /// construct a response to a find_node query
    pub fn new_find_node_response(transaction_id: TransactionId, responding_id: NodeId, nodes: Box<[u8]>) -> Krpc {
        let find_node_response = FindNodeResponse {
            transaction_id,
            message_type: Box::new(b"r".clone()),
            body: FindNodeResponseBody {
                id: responding_id,
                nodes,
            },
        };

        Krpc::FindNodeResponse(find_node_response)
    }

    /// construct a response to a get_peers query when the peer is directly found
    pub fn new_get_peers_success_response(transaction_id: TransactionId,
                                          responding_id: NodeId,
                                          response_token: Token,
                                          node: Vec<CompactPeerContact>) -> Krpc {
        let get_peers_success_response = GetPeersSuccessResponse {
            transaction_id,
            message_type: Box::new(b"r".clone()),
            body: GetPeersSuccessResponseBody {
                id: responding_id,
                token: response_token,
                values: node,
            },
        };

        Krpc::GetPeersSuccessResponse(get_peers_success_response)
    }

    /// construct a response to a get_peers query when the peer is not directly found and the closest
    /// nodes are returned
    pub fn new_get_peers_deferred_response(transaction_id: TransactionId,
                                           responding_id: NodeId,
                                           response_token: Token,
                                           closest_nodes: Box<[u8]>) -> Krpc {
        let get_peers_deferred_response = GetPeersDeferredResponse {
            transaction_id,
            message_type: Box::new(b"r".clone()),
            body: get_peers_deferred_response::GetPeersDeferredResponseBody {
                id: responding_id,
                token: response_token,
                nodes: closest_nodes,
            },
        };

        Krpc::GetPeersDeferredResponse(get_peers_deferred_response)
    }

    pub fn new_announce_peer_response(transaction_id: TransactionId, responding_id: NodeId) -> Krpc {
        let announce_peer_response = PingAnnouncePeerResponse {
            transaction_id,
            message_type: Box::new(b"r".clone()),
            body: PingAnnouncePeerResponseBody {
                id: responding_id,
            },
        };
        Krpc::PingAnnouncePeerResponse(announce_peer_response)
    }

    pub fn id_as_u16(&self) -> u16 {
        match self {
            Krpc::Error(error) => {
                u16::from_be_bytes(error.transaction_id)
            }
            Krpc::PingAnnouncePeerResponse(ping) => {
                u16::from_be_bytes(ping.transaction_id)
            }
            Krpc::GetPeersDeferredResponse(peers) => {
                u16::from_be_bytes(peers.transaction_id)
            }
            Krpc::GetPeersSuccessResponse(peers) => {
                u16::from_be_bytes(peers.transaction_id)
            }
            Krpc::FindNodeResponse(node) => {
                u16::from_be_bytes(node.transaction_id)
            }
            Krpc::PingQuery(ping) => {
                u16::from_be_bytes(ping.transaction_id)
            }
            Krpc::FindNodeQuery(node) => {
                u16::from_be_bytes(node.transaction_id)
            }
            Krpc::GetPeersQuery(peers) => {
                u16::from_be_bytes(peers.transaction_id)
            }
            Krpc::AnnouncePeerQuery(peers) => {
                u16::from_be_bytes(peers.transaction_id)
            }
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;

    mod deserializing {
        use super::*;
        use bendy::serde::{from_bytes, to_bytes};

        #[test]
        fn ping_response_deserializing() {
            let message = b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
            let decoded: Krpc = from_bytes(message).unwrap();


            let expected = Krpc::new_ping_response(b"aa".clone(), b"mnopqrstuvwxyz123456".clone());
            assert_eq!(decoded, expected);
        }

        #[test]
        fn find_node_response_deserialize() {
            let message = b"d1:rd2:id20:0123456789abcdefghij5:nodes20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
            let decoded: Krpc = from_bytes(message).unwrap();


            let expected = Krpc::new_find_node_response(
                b"aa".clone(),
                b"0123456789abcdefghij".clone(),
                Box::new(b"mnopqrstuvwxyz123456".clone()),
            );
            assert_eq!(expected, decoded)
        }
    }

    mod serializing {
        use super::*;
        use bendy::serde::to_bytes;

        #[test]
        fn serialize_ping_query() {
            let message = Krpc::new_ping_query(b"aa".clone(), b"abcdefghij0123456789".clone());
            let bytes = to_bytes(&message).unwrap();

            // taken directly from the spec
            assert_eq!(bytes, b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe");
        }


        #[test]
        fn serialize_find_node_query() {
            let message = Krpc::new_find_node_query(b"aa".clone(), b"abcdefghij0123456789".clone(), b"mnopqrstuvwxyz123456".clone());
            let bytes = to_bytes(&message).unwrap();

            // taken directly from the spec
            assert_eq!(bytes, b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe");
        }

        #[test]
        fn serialize_get_peers_query() {
            let message = Krpc::new_get_peers_query(b"aa".clone(), b"abcdefghij0123456789".clone(), b"mnopqrstuvwxyz123456".clone());
            let bytes = to_bytes(&message).unwrap();

            // taken directly from the spec
            assert_eq!(bytes, b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe");
        }
    }
}
