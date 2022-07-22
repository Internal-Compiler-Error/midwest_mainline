use crate::{
    domain_knowledge::NodeId,
    message::{InfoHash, QueryMethod, TransactionId},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnouncePeerQuery {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "q")]
    #[serde(default = "QueryMethod::announce_peer")]
    #[serde(skip_deserializing)]
    pub(crate) query_method: QueryMethod,

    #[serde(rename = "a")]
    pub(crate) body: AnnouncePeerArgs,
}

impl AnnouncePeerQuery {
    pub fn new(
        transaction_id: TransactionId,
        id: NodeId,
        token: Box<[u8]>,
        port: u16,
        implied_port: bool,
        info_hash: InfoHash,
    ) -> Self {
        AnnouncePeerQuery {
            transaction_id,
            message_type: b"q".to_vec().into_boxed_slice(),
            query_method: QueryMethod::AnnouncePeer,
            body: AnnouncePeerArgs {
                id,
                token,
                info_hash,
                implied_port: if implied_port { 1 } else { 0 },
                port,
            },
        }
    }
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
    pub token: Box<[u8]>,
}
