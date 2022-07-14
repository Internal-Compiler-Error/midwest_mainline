use crate::message::{InfoHash, QueryMethod, TransactionId};
use crate::message::ping_query::PingArgs;
use serde_with::{Bytes, serde_as};
use serde::{Deserialize, Serialize};
use crate::domain_knowledge::NodeId;

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPeersQuery {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "q")]
    pub(crate) query_method: QueryMethod,

    #[serde(rename = "a")]
    pub(crate) body: GetPeersArgs,
}

impl GetPeersQuery {
    pub fn new(transaction_id: TransactionId, id: NodeId, info_hash: InfoHash) -> Self {
        GetPeersQuery {
            transaction_id,
            message_type: b"q".to_vec().into_boxed_slice(),
            query_method: QueryMethod::GetPeers,
            body: GetPeersArgs { info_hash, id },
        }
    }
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPeersArgs {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,

    #[serde_as(as = "Bytes")]
    pub info_hash: InfoHash,
}
