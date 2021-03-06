use crate::{
    domain_knowledge::NodeId,
    message::{query_methods, TransactionId},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingQuery {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "q")]
    pub(crate) query_method: query_methods::ping,

    #[serde(rename = "a")]
    pub(crate) body: PingArgs,
}

impl PingQuery {
    pub fn new(transaction_id: TransactionId, body: PingArgs) -> Self {
        PingQuery {
            transaction_id,
            message_type: b"q".to_vec().into_boxed_slice(),
            query_method: query_methods::ping,
            body,
        }
    }
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingArgs {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,
}
