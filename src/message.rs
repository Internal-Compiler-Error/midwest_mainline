use std::net::{Ipv4Addr, SocketAddrV4};

use bendy::decoding::{Decoder, Object};
use crate::domain_knowledge::{BetterCompactPeerContact, BetterCompactPeerInfo};

use find_node_get_peers_non_compliant_response::
    BetterFindNodeResponse
;
use get_peers_deferred_response::BetterGetPeersDeferredResponse;
use get_peers_success_response::BetterGetPeersSuccessResponse;
use ping_announce_peer_response::BetterPingAnnouncePeerResponse;

use juicy_bencode;
use juicy_bencode::{BencodeItemView, parse_bencode_dict};
use crate::domain_knowledge::{BetterInfoHash, BetterNodeId};
use crate::message::announce_peer_query::BetterAnnouncePeerQuery;
use crate::message::error::KrpcError;
use crate::message::find_node_query::BetterFindNodeQuery;
use crate::message::get_peers_query::BetterGetPeersQuery;
use crate::message::ping_query::BetterPingQuery;

pub mod announce_peer_query;
pub mod error_response;
pub mod find_node_get_peers_non_compliant_response;
pub mod find_node_query;
pub mod get_peers_deferred_response;
pub mod get_peers_query;
pub mod get_peers_success_response;
pub mod ping_announce_peer_response;
pub mod ping_query;
pub mod error;

pub type InfoHash = [u8; 20];
pub type TransactionId = Box<[u8]>;

pub(crate) trait ToRawKrpc {
    fn to_raw_krpc(&self) -> Box<[u8]>;
}

pub(crate) trait ParseKrpc {
    fn parse(&self) -> Result<Krpc, ()>;
}

impl ParseKrpc for &[u8] {
    /// parse out a krpc message we can do something with
    fn parse(&self) -> Result<Krpc, ()> {
        use std::str;

        let mut decoder = Decoder::new(self);
        let message = decoder
            .next_object()
            .map_err(|_| ())?
            .ok_or(())?;

        let Object::Dict(mut dict) = message else {
            // invalid message
            return Err(());
        };

        // we're only using it to validate the message structure
        dict.consume_all().map_err(|_| ())?;

        let (_remaining, parsed) = parse_bencode_dict(self).map_err(|_| ())?;

        let message_type_indicator = parsed.get(b"y".as_slice()).ok_or(())?;
        let BencodeItemView::ByteString(ref message_type) = message_type_indicator else {
            // invalid message
            return Err(());
        };

        let transaction_id = parsed.get(&b"t".as_slice()).ok_or(())?;
        let BencodeItemView::ByteString(ref transaction_id) = transaction_id else {
            return Err(());
        };
        // TODO: are all transaction ids strings?
        let transaction_id = str::from_utf8(transaction_id).map_err(|_| ())?.to_string();

        if message_type == b"e" { // Error message
            let code_and_message = parsed.get(b"e".as_slice()).ok_or(())?;
            let BencodeItemView::List(code_and_message) = code_and_message else {
                return Err(());
            };
            let code = code_and_message.get(0).ok_or(())?;
            let message = code_and_message.get(1).ok_or(())?;

            let BencodeItemView::Integer(code) = code else {
                return Err(());
            };

            let BencodeItemView::ByteString(ref message) = message else {
                return Err(());
            };
            let message = str::from_utf8(message).map_err(|_| ())?.to_string();
            // todo: cast safely
            let code: u32 = *code as u32;
            return Ok(Krpc::ErrorResponse(KrpcError::new(
                transaction_id,
                code,
                message,
            )));

        } else if message_type == &b"q" { // queries
            // pull out common fields
            let query_type = parsed.get(&b"q".as_slice()).ok_or(())?;
            let BencodeItemView::ByteString(query_type) = query_type else {
                return Err(());
            };

            let arguments = parsed.get(&b"a".as_slice()).ok_or(())?;
            let BencodeItemView::Dictionary(arguments) = arguments else {
                return Err(());
            };

            let querier = arguments.get(&b"id".as_slice()).ok_or(())?;
            let BencodeItemView::ByteString(querier) = querier else {
                return Err(());
            };
            let querier = str::from_utf8(querier).map_err(|_| ())?.to_string();

            if query_type == &b"ping" {
                let ping = BetterPingQuery::new(transaction_id,
                                                BetterNodeId::new(querier).map_err(|_| ())?);
                return Ok(Krpc::PingQuery(ping));
            } else if query_type == &b"find_node" {
                let target = arguments.get(&b"target".as_slice()).ok_or(())?;
                let BencodeItemView::ByteString(target) = target else {
                    return Err(());
                };
                let target = str::from_utf8(target).map_err(|_| ())?.to_string();

                let find_node_request = BetterFindNodeQuery::new(transaction_id,
                                                                 BetterNodeId::new(querier).map_err(|_| ())?,
                                                                 BetterNodeId::new(target).map_err(|_| ())?,
                );
                return Ok(Krpc::FindNodeQuery(find_node_request));
            } else if query_type == &b"get_peers" {
                let info_hash = parsed.get(&b"info_hash".as_slice()).ok_or(())?;
                let BencodeItemView::ByteString(info_hash) = info_hash else {
                    return Err(());
                };
                let info_hash = str::from_utf8(info_hash).map_err(|_| ())?.to_string();
                let get_peers = BetterGetPeersQuery::new(transaction_id,
                                                         BetterNodeId::new(querier).map_err(|_| ())?,
                                                         BetterInfoHash::new(info_hash).map_err(|_| ())?,
                );

                return Ok(Krpc::GetPeersQuery(get_peers));
            } else if query_type == &b"announce_peer" {
                // todo: some stupid clients might not send this
                let implied_port = parsed.get(&b"implied_port".as_slice()).ok_or(())?;
                let implied_port = match implied_port {
                    BencodeItemView::Integer(i) => Some(i),
                    _ => None
                };

                // TODO: revisit this, the semantics seems wrong
                let port = if implied_port.is_some() {
                    let port = parsed.get(&b"port".as_slice()).ok_or(())?;
                    let BencodeItemView::Integer(port) = port else {
                        return Err(());
                    };
                    // todo: cast safely
                    Some(*port as u16)
                } else {
                    None
                };


                let token = parsed.get(&b"token".as_slice()).ok_or(())?;
                let BencodeItemView::ByteString(token) = token else {
                    return Err(());
                };
                let token = str::from_utf8(token).map_err(|_| ())?.to_string();


                // todo: look at the non compliant ones
                let info_hash = parsed.get(&b"info_hash".as_slice()).ok_or(())?;
                let BencodeItemView::ByteString(info_hash) = info_hash else {
                    return Err(());
                };
                let info_hash = str::from_utf8(info_hash).map_err(|_| ())?.to_string();

                let announce_peer = BetterAnnouncePeerQuery::new(
                    transaction_id,
                    BetterNodeId::new(querier).map_err(|_| ())?,
                    port,
                    BetterInfoHash::new(info_hash).map_err(|_| ())?,
                    token,
                );

                return Ok(Krpc::AnnouncePeerQuery(announce_peer));
            } else {
                return Err(());
            }
        } else if message_type == &b"r" { // responses
            let body = parsed.get(b"r".as_slice()).ok_or(())?;
            let BencodeItemView::Dictionary(response) = body else {
                return Err(());
            };

            let id = response.get(b"id".as_slice()).ok_or(())?;
            let BencodeItemView::ByteString(ref target_id) = id else {
                return Err(());
            };
            let target_id = BetterNodeId::new(String::from_utf8(target_id.to_vec()).unwrap()).unwrap();


            if response.contains_key(b"nodes".as_slice()) {
                let nodes = response.get(b"nodes".as_slice()).unwrap();
                let BencodeItemView::ByteString(ref nodes) = nodes else {
                    return Err(());
                };

                let contacts: Vec<_> = nodes
                    .chunks(26)
                    .map(|info| {
                        let node_id = &info[0..20];
                        let contact = &info[20..26];

                        let node_id = BetterNodeId::new(String::from_utf8(node_id.to_vec()).unwrap()).unwrap();

                        let ip = Ipv4Addr::new(contact[0], contact[1], contact[2], contact[3]);
                        let port = u16::from_be_bytes([contact[4], contact[5]]);
                        let contact = BetterCompactPeerContact(SocketAddrV4::new(ip, port));

                        BetterCompactPeerInfo {
                            id: node_id,
                            contact
                        }
                    })
                    .collect();

                let res = BetterFindNodeResponse {
                    transaction_id,
                    target_id,
                    nodes: contacts,
                };
                let msg = Krpc::FindNodeGetPeersNonCompliantResponse(res) ;
                return Ok(msg);
                // find nodes response
            } else if response.contains_key(b"token".as_slice()) {
                // get_peers response
                let token = response.get(b"token".as_slice()).unwrap();
                let BencodeItemView::ByteString(token) = token else {
                    return Err(());
                };
                let token = String::from_utf8(token.to_vec()).unwrap();

                if let Some(BencodeItemView::List(values)) = response.get(b"values".as_slice()) {
                    // success, the peers can be contacted immediately

                    let contacts: Vec<_> = values
                        .iter()
                        .filter_map(|x| {
                            match x {
                                BencodeItemView::ByteString(s) => Some(s),
                                _ => None,
                            }
                        })
                        .map(|sock_addr| {
                            // TODO: worry about segfault later
                            let ip = Ipv4Addr::new(sock_addr[0], sock_addr[1], sock_addr[2], sock_addr[3]);
                            let port = u16::from_be_bytes([sock_addr[4], sock_addr[5]]);

                            BetterCompactPeerContact(SocketAddrV4::new(ip, port))
                        }).collect();

                    let res = BetterGetPeersSuccessResponse::new(transaction_id, target_id, token, contacts);
                    let msg = Krpc::GetPeersSuccessResponse(res);
                    return Ok(msg);
                } else if let Some(BencodeItemView::ByteString(nodes)) =  response.get(b"nodes".as_slice()) {
                    // deferred, nearest nodes returned
                    let contacts: Vec<_> = nodes
                        .chunks(26)
                        .map(|info| {
                            let node_id = &info[0..20];
                            let contact = &info[20..26];

                            let node_id = BetterNodeId::new(String::from_utf8(node_id.to_vec()).unwrap()).unwrap();

                            let ip = Ipv4Addr::new(contact[0], contact[1], contact[2], contact[3]);
                            let port = u16::from_be_bytes([contact[4], contact[5]]);
                            let contact = BetterCompactPeerContact(SocketAddrV4::new(ip, port));

                            BetterCompactPeerInfo {
                                id: node_id,
                                contact
                            }
                        })
                        .collect();

                    let res = BetterGetPeersDeferredResponse::new(transaction_id, target_id, token, contacts);
                    let msg = Krpc::GetPeersDeferredResponse(res);
                    return Ok(msg);
                } else {
                    // invalid message
                    return Err(());
                }
            } else if response.len() == 1 {
                // only has id in the response
                //
                // could be a ping, or, announce peer, due to the horrible protocol design of KRPC
                let res = BetterPingAnnouncePeerResponse::new(transaction_id, target_id);
                let msg = Krpc::PingAnnouncePeerResponse(res);
                return Ok(msg);
            } else {
                // non-comliant response
                return Err(());
            }
        } else { // invalid message
            return Err(());
        }
    }
}


#[derive(Debug, PartialEq, Eq)]
pub enum Krpc {
    AnnouncePeerQuery(BetterAnnouncePeerQuery),
    FindNodeQuery(BetterFindNodeQuery),
    GetPeersQuery(BetterGetPeersQuery),
    PingQuery(BetterPingQuery),

    GetPeersSuccessResponse(BetterGetPeersSuccessResponse),
    GetPeersDeferredResponse(BetterGetPeersDeferredResponse),
    FindNodeGetPeersNonCompliantResponse(BetterFindNodeResponse),
    PingAnnouncePeerResponse(BetterPingAnnouncePeerResponse),

    ErrorResponse(KrpcError),
}

#[allow(non_camel_case_types)]
pub(crate) mod query_methods {
    use serde_unit_struct::{Deserialize_unit_struct, Serialize_unit_struct};

    #[derive(Debug, PartialEq, Eq, Deserialize_unit_struct, Serialize_unit_struct)]
    pub(crate) struct find_node;

    #[derive(Debug, PartialEq, Eq, Deserialize_unit_struct, Serialize_unit_struct)]
    //#[serde(rename = "ping")]
    pub(crate) struct ping;

    #[derive(Debug, PartialEq, Eq, Deserialize_unit_struct, Serialize_unit_struct)]
    pub(crate) struct announce_peer;

    #[derive(Debug, PartialEq, Eq, Deserialize_unit_struct, Serialize_unit_struct)]
    pub(crate) struct get_peers;
}

impl Krpc {
    pub fn is_response(&self) -> bool {
        match self {
            Krpc::PingAnnouncePeerResponse(_) => true,
            Krpc::FindNodeGetPeersNonCompliantResponse(_) => true,
            Krpc::GetPeersSuccessResponse(_) => true,
            Krpc::GetPeersDeferredResponse(_) => true,
            _ => false,
        }
    }

    pub fn transaction_id(&self) -> &str {
        match self {
            Krpc::PingAnnouncePeerResponse(msg) => msg.txn_id(),
            Krpc::FindNodeGetPeersNonCompliantResponse(msg) => &msg.transaction_id,
            Krpc::GetPeersSuccessResponse(msg) => msg.txn_id(),
            Krpc::GetPeersDeferredResponse(msg) => msg.txn_id(),
            Krpc::FindNodeQuery(msg) => msg.txn_id(),
            Krpc::GetPeersQuery(msg) => msg.txn_id(),
            Krpc::AnnouncePeerQuery(msg) => msg.txn_id(),
            Krpc::PingQuery(msg) => msg.txn_id(),
            Krpc::ErrorResponse(msg) => msg.txn_id(),
        }
    }

    pub fn is_error(&self) -> bool {
        match self {
            Krpc::ErrorResponse(_) => true,
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

    pub fn new_ping_query(transaction_id: String, querying_id: BetterNodeId) -> Krpc {
        // let ping_query = PingQuery {
        //     transaction_id,
        //     message_type: Box::new(b"q".clone()),
        //     query_method: query_methods::ping,
        //     body: PingArgs { id: querying_id },
        // };

        // Krpc::PingQuery(ping_query)
        let ping = BetterPingQuery::new(transaction_id, querying_id);
        Krpc::PingQuery(ping)
    }

    pub fn new_find_node_query(transaction_id: String, querying_id: BetterNodeId, target_id: BetterNodeId) -> Krpc {
        // let find_node_query = FindNodeQuery {
        //     transaction_id,
        //     message_type: Box::new(b"q".clone()),
        //     query_method: query_methods::find_node,
        //     body: FindNodeArgs {
        //         id: querying_id,
        //         target: target_id,
        //     },
        // };
        //
        // Krpc::FindNodeQuery(find_node_query)
        let find_node = BetterFindNodeQuery::new(transaction_id, querying_id, target_id);
        Krpc::FindNodeQuery(find_node)
    }

    pub fn new_get_peers_query(transaction_id: String, querying_id: BetterNodeId, info_hash: BetterInfoHash) -> Krpc {
        // let get_peers_query = GetPeersQuery {
        //     transaction_id,
        //     message_type: Box::new(b"q".clone()),
        //     query_method: query_methods::get_peers,
        //     body: GetPeersArgs {
        //         id: querying_id,
        //         info_hash,
        //     },
        // };

        // Krpc::GetPeersQuery(get_peers_query)
        let get_peers = BetterGetPeersQuery::new(transaction_id, querying_id, info_hash);
        Krpc::GetPeersQuery(get_peers)
    }

    pub fn new_announce_peer_query(
        transaction_id: String,
        info_hash: BetterInfoHash,
        querying_id: BetterNodeId,
        port: u16,
        implied_port: bool,
        token: String,
    ) -> Krpc {
        // let announce_peer_query = AnnouncePeerQuery {
        //     transaction_id,
        //     message_type: Box::new(b"q".clone()),
        //     query_method: query_methods::announce_peer,
        //     body: AnnouncePeerArgs {
        //         id: querying_id,
        //         implied_port: if implied_port { 1 } else { 0 },
        //         info_hash,
        //         port,
        //         token,
        //     },
        // };
        //
        // Krpc::AnnouncePeerQuery(announce_peer_query)
        let port = if implied_port {Some(port)} else {None};
        let announce_peer = BetterAnnouncePeerQuery::new(transaction_id, querying_id, port, info_hash, token);
        Krpc::AnnouncePeerQuery(announce_peer)
    }

    pub fn new_ping_response(transaction_id: String, responding_id: BetterNodeId) -> Krpc {
        // let ping_response = PingAnnouncePeerResponse {
        //     transaction_id,
        //     message_type: Box::new(b"r".clone()),
        //     body: PingAnnouncePeerResponseBody { id: responding_id },
        // };
        let ping_res = BetterPingAnnouncePeerResponse::new(transaction_id, responding_id);
        Krpc::PingAnnouncePeerResponse(ping_res)
    }

    // construct a response to a find_node query
    pub fn new_find_node_response(transaction_id: String, responding_id: BetterNodeId, nodes: Vec<BetterCompactPeerInfo>) -> Krpc {
        // let find_node_response = FindNodeGetPeersNonCompliantResponse {
        //     transaction_id,
        //     message_type: Box::new(b"r".clone()),
        //     body: FindNodeGetPeersNonCompliantResponseBody {
        //         id: responding_id,
        //         nodes,
        //     },
        // };
        //
        // Krpc::FindNodeGetPeersNonCompliantResponse(find_node_response)
        let find_node_res = BetterFindNodeResponse {
            transaction_id,
            target_id: responding_id,
            nodes,
        };
        Krpc::FindNodeGetPeersNonCompliantResponse(find_node_res)
    }

    // construct a response to a get_peers query when the peer is directly found
    pub fn new_get_peers_success_response(
        transaction_id: String,
        responding_id: BetterNodeId,
        response_token: String,
        peers: Vec<BetterCompactPeerContact>,
    ) -> Krpc {
        // let get_peers_success_response = GetPeersSuccessResponse {
        //     transaction_id,
        //     message_type: Box::new(b"r".clone()),
        //     body: GetPeersSuccessResponseBody {
        //         id: responding_id,
        //         token: response_token,
        //         values: peers,
        //     },
        // };

        let get_peers_success_response = BetterGetPeersSuccessResponse::new(transaction_id, responding_id, response_token, peers);
        Krpc::GetPeersSuccessResponse(get_peers_success_response)
    }

    // construct a response to a get_peers query when the peer is not directly found and the closest
    // nodes are returned
    pub fn new_get_peers_deferred_response(
        transaction_id: String,
        responding_id: BetterNodeId,
        response_token: String,
        closest_nodes: Vec<BetterCompactPeerInfo>
    ) -> Krpc {
        // let get_peers_deferred_response = GetPeersDeferredResponse {
        //     transaction_id,
        //     message_type: Box::new(b"r".clone()),
        //     body: get_peers_deferred_response::GetPeersDeferredResponseBody {
        //         id: responding_id,
        //         token: response_token,
        //         nodes: closest_nodes,
        //     },
        // };

        let get_peers_deferred_response = BetterGetPeersDeferredResponse::new(transaction_id, responding_id, token, nodes);
        Krpc::GetPeersDeferredResponse(get_peers_deferred_response)
    }

    // construct a response to a get_peers query when the peer is not directly found and the closest
    // nodes are returned
    pub fn new_get_peers_deferred_response_con_compliant(
        transaction_id: String,
        responding_id: BetterNodeId,
        closest_nodes: Vec<BetterCompactPeerInfo>,
    ) -> Krpc {
        // let get_peers_deferred_response = FindNodeGetPeersNonCompliantResponse {
        //     transaction_id,
        //     message_type: Box::new(b"r".clone()),
        //     body: FindNodeGetPeersNonCompliantResponseBody {
        //         id: responding_id,
        //         nodes: closest_nodes,
        //     },
        // };

        // let get_peers_deferred_response = BetterGetPeersDeferredResponse::new(transaction_id, querier, token, nodes)
        // Krpc::FindNodeGetPeersNonCompliantResponse(get_peers_deferred_response)
        todo!()
    }

    pub fn new_announce_peer_response(transaction_id: String, responding_id: BetterNodeId) -> Krpc {
        // let announce_peer_response = PingAnnouncePeerResponse {
        //     transaction_id,
        //     message_type: Box::new(b"r".clone()),
        //     body: PingAnnouncePeerResponseBody { id: responding_id },
        // };
        // Krpc::PingAnnouncePeerResponse(announce_peer_response)
        let announce_peer_res = BetterPingAnnouncePeerResponse::new(transaction_id, responding_id);
        Krpc::PingAnnouncePeerResponse(announce_peer_res)
    }

    pub fn new_standard_generic_error_response(transaction_id: String) -> Krpc {
        // let error_response = ErrorResponse {
        //     transaction_id,
        //     message_type: Box::new(b"e".clone()),
        //     error: (201 as u32, "A Generic Error Occurred".to_string()),
        // };

        let error_response = KrpcError::new(transaction_id, 201 as u32, "A Generic Error Occurred".to_string());
        Krpc::ErrorResponse(error_response)
    }

    pub fn new_standard_server_error(transaction_id: String) -> Krpc {
        // let error_response = ErrorResponse {
        //     transaction_id,
        //     message_type: Box::new(b"e".clone()),
        //     error: (202 as u32, "A Server Error Occurred".to_string()),
        // };

        let error_response = KrpcError::new(transaction_id, 202 as u32, "A Server Error Occurred".to_string());
        Krpc::ErrorResponse(error_response)
    }

    pub fn new_standard_protocol_error(transaction_id: String) -> Krpc {
        // let error_response = ErrorResponse {
        //     transaction_id,
        //     message_type: Box::new(b"e".clone()),
        //     error: (203 as u32, "A Protocol Error Occurred".to_string()),
        // };
        let error_response = KrpcError::new(transaction_id, 203 as u32, "A Protocol Error Occurred".to_string());
        Krpc::ErrorResponse(error_response)
    }

    pub fn new_unsupported_error(transaction_id: String) -> Krpc {
        // let error_response = ErrorResponse {
        //     transaction_id,
        //     message_type: Box::new(b"e".clone()),
        //     error: (204 as u32, "A Unsupported Method Error Occurred".to_string()),
        // };
        let error_response = KrpcError::new(transaction_id, 204 as u32, "A Unsupported Method Error Occurred".to_string());
        Krpc::ErrorResponse(error_response)
    }

    // pub fn id_as_u16(&self) -> u16 {
    //     match self {
    //         Krpc::ErrorResponse(error) => u16::from_be_bytes(error.transaction_id),
    //         Krpc::PingAnnouncePeerResponse(ping) => u16::from_be_bytes(ping.transaction_id),
    //         Krpc::GetPeersDeferredResponse(peers) => u16::from_be_bytes(peers.transaction_id),
    //         Krpc::GetPeersSuccessResponse(peers) => u16::from_be_bytes(peers.transaction_id),
    //         Krpc::FindNodeGetPeersNonCompliantResponse(node) => u16::from_be_bytes(node.transaction_id),
    //         Krpc::PingQuery(ping) => u16::from_be_bytes(ping.transaction_id),
    //         Krpc::FindNodeQuery(node) => u16::from_be_bytes(node.transaction_id),
    //         Krpc::GetPeersQuery(peers) => u16::from_be_bytes(peers.transaction_id),
    //         Krpc::AnnouncePeerQuery(peers) => u16::from_be_bytes(peers.transaction_id),
    //     }
    // }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Once;

    static COLOR_EYRE_INIT: Once = Once::new();

    mod deserializing {
        use super::*;
        use bendy::serde::from_bytes;
        use std::net::{Ipv4Addr, SocketAddrV4};

        #[test]
        fn ping_query_deserializing() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let message = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";
            let deserialized = from_bytes::<Krpc>(message)?;
            let expected = Krpc::new_ping_query(Box::new([b'a', b'a']), b"abcdefghij0123456789".clone());
            //println!("{:?}", String::from_utf8_lossy(&to_bytes(&expected)?));
            assert_eq!(deserialized, expected);
            Ok(())
        }

        #[test]
        fn find_node_query_deserializing() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let message =
                b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe";
            let deserialized = from_bytes::<Krpc>(message)?;

            let expected = Krpc::new_find_node_query(
                Box::from(b"aa".as_slice()),
                b"abcdefghij0123456789".clone(),
                b"mnopqrstuvwxyz123456".clone(),
            );

            assert_eq!(deserialized, expected);
            Ok(())
        }

        #[test]
        fn get_peers_query_deserializing() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let message =
                b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe";
            let deserialized = from_bytes::<Krpc>(message)?;

            let expected = Krpc::new_get_peers_query(
                Box::from(b"aa".clone()),
                b"abcdefghij0123456789".clone(),
                b"mnopqrstuvwxyz123456".clone(),
            );

            // taken directly from the spec
            assert_eq!(deserialized, expected);

            Ok(())
        }

        #[test]
        fn announce_peers_query_deserializing() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let message =
                b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe";
            let deserialized = from_bytes::<Krpc>(message)?;

            let expected = Krpc::new_announce_peer_query(
                Box::from(b"aa".clone()),
                b"mnopqrstuvwxyz123456".clone(),
                b"abcdefghij0123456789".clone(),
                6881,
                true,
                Box::from(b"aoeusnth".as_slice()),
            );

            // taken directly from the spec
            assert_eq!(deserialized, expected);

            Ok(())
        }

        #[test]
        fn ping_response_deserializing() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let message = b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
            let decoded: Krpc = from_bytes(message).unwrap();

            let expected = Krpc::new_ping_response(Box::from(b"aa".clone()), b"mnopqrstuvwxyz123456".clone());
            assert_eq!(decoded, expected);

            Ok(())
        }

        #[test]
        fn get_peers_success_response_deserializing() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let bencoded = hex::decode("64323a6970363a434545f1c8d6313a7264323a696432303a23307bc01f5e7cc56ba66314b36e69246304f870353a6e6f6465733230383a233b7b388eaded578cb8b62a1ddfef3277bf01945c202537c8d5233a010302bab6e6726e991228571f8807a9f77eb2aae6fd12a42339069106980533f8df5b5b9a17d6b704740b7bde6241279f032338bd5ff8d5779c7170d17343b8b3fe405fe71eb96b5f496d86233f9938fa19e256821896495e11e0f63ff032706ad22134e1ed233e6bd6ae529049f1f1bbe9ebb3a6db3c870ce15a9a5df9bbc8233dafab3b38a789a3e53433380dd825c45b3f57b9a76343c8d5233cddccbe1f9e5041e3b3d4d124f9c252697ef0755dab53c8d5353a746f6b656e32303a3704f7737408c5fef0f96bca389e4100f972859d363a76616c7565736c363ab28f20fc5f41363ab025e789900e363a5bd6f27f042e6565313a74323a11ec313a76343a5554b50c313a79313a7265")?;
            let decoded: Krpc = from_bytes(bencoded.as_slice())?;

            let expected = Krpc::new_get_peers_success_response(
                hex::decode("11ec")?.try_into().unwrap(),
                hex::decode("23307bc01f5e7cc56ba66314b36e69246304f870")?
                    .try_into()
                    .unwrap(),
                hex::decode("3704f7737408c5fef0f96bca389e4100f972859d")?
                    .try_into()
                    .unwrap(),
                vec![
                    SocketAddrV4::new(Ipv4Addr::new(178, 143, 32, 252), 24385).into(),
                    SocketAddrV4::new(Ipv4Addr::new(176, 37, 231, 137), 36878).into(),
                    SocketAddrV4::new(Ipv4Addr::new(91, 214, 242, 127), 1070).into(),
                ],
            );

            assert_eq!(decoded, expected);

            Ok(())
        }

        #[test]
        fn find_node_response_deserialize() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let message = b"d1:rd2:id20:0123456789abcdefghij5:nodes20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
            let decoded: Krpc = from_bytes(message).unwrap();

            let expected = Krpc::new_find_node_response(
                Box::from(b"aa".clone()),
                b"0123456789abcdefghij".clone(),
                Box::new(b"mnopqrstuvwxyz123456".clone()),
            );
            assert_eq!(expected, decoded);
            Ok(())
        }

        #[test]
        fn oi() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let message = hex::decode("64313a6164323a696432303a8351db2997d2f0b603af85ca58ec32ad6693429a65313a71343a70696e67313a74343a706e0000313a79313a7165")?;
            let decoded: PingQuery = from_bytes(message.as_slice())?;
            println!("{:?}", decoded);

            Ok(())
        }

        #[test]
        fn deserialize_generic_error() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let message = b"d1:eli201e24:A Generic Error Occurrede1:t2:aa1:y1:ee";
            let decoded: Krpc = from_bytes(message).unwrap();

            let expected = Krpc::new_standard_generic_error_response(Box::from(b"aa".clone()));
            assert_eq!(expected, decoded);
            Ok(())
        }

        #[test]
        fn deserialize_con_compliant_get_peers() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            // if all this following is painful to read, trust me, it was painful to write
            // and even more painful to realize this has to be supported
            let bencoded = hex::decode(
                "64323a6970363ab8972559c8d6313a7264323a696432303\
            a32f54e697351ff4aec29cdbaabf2fbe3467cc267353a6e6f6465733431363aa5490d805d411f43c4cd594d9\
            d4818c4cc675e256317945fc491513c322f8cbab9f21cb9b9c12336923f016634d8c35829b00605ab2ec7148\
            90ba9e6348c1409c291096bb95988d35e658c5bc15ce06915d1eefec905aa7224c1c9dbd0c4a1f990596db1f\
            5581ae1b13de8394dbb53bf3d886142880584f3bf1f6edbbcfeb16f5d4d07e637b49d82b743e9e2a4c47889f\
            f37cd2604bfd808b5d1282b6bf699d52b44c0e5275b41fc933e4acfeb49499dc6391bf2eba979b76b5a58a4d\
            59fecffb360095658264c7a1ec9aa9692727a8a139c04a5ba0a9d60d1e27c5cda0823f39e76275fc95001c9a\
            48974d45ab9647301de8a187ea8e9976ba28abc63dd68b295b6c491d6fc73087d29a2d45aebefd2155f7753e\
            4ddc89fb1dff65f315c327e3131966d1b4edf83f572322a6a4d9f00e525b46b5c3c98b31506518750a5c57bc\
            b76d11fc800ecde98a20da371fdc158350eed510bb4c7f04904ea794ef82cac713edd9f1db75403918979a7\
            8abacbf3267657c26e095e73f75abf9398e0f6e6bd9a26b5bda700000000000000000000000000000000000\
            000005778ea621cb665313a74323a025f313a79313a7265",
            )?;
            let expected = Krpc::new_get_peers_deferred_response_con_compliant(
                hex::decode("025f")?.as_slice().try_into().unwrap(),
                hex::decode("32f54e697351ff4aec29cdbaabf2fbe3467cc267")?
                    .try_into()
                    .unwrap(),
                Box::from(
                    hex::decode(
                        "a5490d805d411f43c4cd594d9d4818c4cc675e25631\
                7945fc491513c322f8cbab9f21cb9b9c12336923f016634d8c35829b00605ab2ec714890ba9e6348c140\
                9c291096bb95988d35e658c5bc15ce06915d1eefec905aa7224c1c9dbd0c4a1f990596db1f5581ae1b13\
                de8394dbb53bf3d886142880584f3bf1f6edbbcfeb16f5d4d07e637b49d82b743e9e2a4c47889ff37cd26\
                04bfd808b5d1282b6bf699d52b44c0e5275b41fc933e4acfeb49499dc6391bf2eba979b76b5a58a4d59f\
                ecffb360095658264c7a1ec9aa9692727a8a139c04a5ba0a9d60d1e27c5cda0823f39e76275fc95001c9\
                a48974d45ab9647301de8a187ea8e9976ba28abc63dd68b295b6c491d6fc73087d29a2d45aebefd2155f\
                7753e4ddc89fb1dff65f315c327e3131966d1b4edf83f572322a6a4d9f00e525b46b5c3c98b315065187\
                50a5c57bcb76d11fc800ecde98a20da371fdc158350eed510bb4c7f04904ea794ef82cac713edd9f1db75\
                403918979a78abacbf3267657c26e095e73f75abf9398e0f6e6bd9a26b5bda70000000000000000000000\
                0000000000000000005778ea621cb6",
                    )?
                        .as_slice(),
                ),
                // Box::from(hex::decode("b8972559c8d6")?.as_slice()),
            );

            assert_eq!(expected, from_bytes::<Krpc>(&bencoded).unwrap());

            Ok(())
        }
    }

    mod serializing {
        use super::*;
        use bendy::serde::to_bytes;

        #[test]
        fn serialize_ping_query() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let message = Krpc::new_ping_query(Box::from(b"aa".clone()), b"abcdefghij0123456789".clone());
            let bytes = to_bytes(&message).unwrap();

            // taken directly from the spec
            assert_eq!(bytes, b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe");

            Ok(())
        }

        #[test]
        fn serialize_find_node_query() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let message = Krpc::new_find_node_query(
                Box::from(b"aa".clone()),
                b"abcdefghij0123456789".clone(),
                b"mnopqrstuvwxyz123456".clone(),
            );
            let bytes = to_bytes(&message).unwrap();

            // taken directly from the spec
            assert_eq!(
                bytes,
                b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe"
            );

            Ok(())
        }

        #[test]
        fn serialize_get_peers_query() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let message = Krpc::new_get_peers_query(
                Box::from(b"aa".clone()),
                b"abcdefghij0123456789".clone(),
                b"mnopqrstuvwxyz123456".clone(),
            );
            let bytes = to_bytes(&message).unwrap();

            // taken directly from the spec
            assert_eq!(
                bytes,
                b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe"
            );

            Ok(())
        }

        #[test]
        fn serialize_get_peers_success_response() -> color_eyre::Result<()> {
            COLOR_EYRE_INIT.call_once(|| {
                color_eyre::install().expect("Initialization is only called once");
            });

            let message = Krpc::new_get_peers_success_response(
                Box::from(b"aa".clone()),
                b"abcdefghij0123456789".clone(),
                Box::new(b"aoeusnth".clone()),
                vec![
                    CompactPeerContact {
                        bytes: b"axje.u".clone(),
                    },
                    CompactPeerContact {
                        bytes: b"idhtnm".clone(),
                    },
                ],
            );
            let expected =
                b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re";
            let bytes = to_bytes(&message).unwrap();
            assert_eq!(bytes, expected);
            Ok(())
        }
    }
}
