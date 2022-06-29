use std::net::Ipv4Addr;
use serde::ser::{SerializeMap, SerializeStruct};
use serde::Serialize;
use crate::message::query::{FindNodeArgs, GetPeersArgs, PingArgs, QueryType};

pub struct PeerContact(Ipv4Addr);

pub type CompactNodeContact = [u8; 20];
pub type InfoHash = [u8; 20];
pub type TransactionId = [u8; 2];

pub struct NodeContact {
    pub node_contact: CompactNodeContact,
    pub peer_contact: PeerContact,
}

enum MessageType {
    Query(QueryType),
    Response(QueryType),
    Error,
}

/// The common parts of all KRPC messages
pub struct Message {
    transaction_id: TransactionId,
    message: MessageType,
}

impl Message {
    pub fn new_ping_query(transaction_id: TransactionId, querying_id: CompactNodeContact) -> Message {
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

    pub fn new_find_node_query(transaction_id: TransactionId, querying_id: CompactNodeContact, target_id: InfoHash) -> Message {
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

    pub fn new_get_peers_query(transaction_id: TransactionId, querying_id: CompactNodeContact, info_hash: InfoHash) -> Message {
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
    pub fn new_announce_peer_query(transaction_id: TransactionId, querying_id: CompactNodeContact, info_hash: InfoHash, port: u16) -> Message {
        // this is impossible under NAT
        unimplemented!()
    }
}


impl Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
    {
        let mut message_fields = serializer.serialize_map(None)?;


        // all messages have a transaction_id as key "t"
        message_fields.serialize_entry("t", &self.transaction_id)?;

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
                    QueryType::AnnouncePeer => {
                        unimplemented!()
                    }
                };
            }
            MessageType::Response(_) => { message_fields.serialize_entry("y", "r")?; }
            MessageType::Error => { message_fields.serialize_entry("y", "e")?; }
        }

        //


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
        AnnouncePeer,
    }

    #[derive(Debug, Serialize)]
    pub struct PingArgs {
        #[serde(with = "serde_bytes")]
        pub id: CompactNodeContact,
    }

    #[derive(Debug, Serialize)]
    pub struct FindNodeArgs {
        pub id: CompactNodeContact,
        pub target: CompactNodeContact,
    }

    #[derive(Debug, Serialize)]
    pub struct GetPeersArgs {
        pub id: CompactNodeContact,
        pub info_hash: InfoHash,
    }
}

pub mod response {
    use super::*;

    pub enum ResponseType {
        Ping(PingResponse),
        FindNode(FindNodeResponse),
        GetPeers(GetPeersResponse),
    }

    pub struct PingResponse {
        pub id: CompactNodeContact,
    }

    pub struct FindNodeResponse {
        pub id: CompactNodeContact,
        pub nodes: Vec<CompactNodeContact>,
    }


    pub struct GetPeersResponse {
        pub id: CompactNodeContact,
        pub token: [u8; 20],
        pub response: GetPeersResponseType,
    }

    pub enum GetPeersResponseType {
        Nodes(CompactNodeContact),
        Peers(Vec<CompactNodeContact>),
    }

    pub struct AnnouncePeerResponse {
        pub id: CompactNodeContact,
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
    fn serialize_ping_args() {
        let args = PingArgs {
            id: [b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i', b'j', b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9'].into(),
        };
        let bytes = to_bytes(&args).unwrap();
        dbg!(String::from_utf8_lossy(&bytes));
    }

    #[test]
    fn serialize_ping_query() {
        let message = Message::new_ping_query([b'a', b'a'], b"abcdefghij0123456789".clone());
        let bytes = to_bytes(&message).unwrap();

        dbg!(String::from_utf8_lossy(&bytes));

        assert_eq!(bytes, b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe");
    }
}
