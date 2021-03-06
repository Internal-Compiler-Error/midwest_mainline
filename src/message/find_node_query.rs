use crate::{
    domain_knowledge::NodeId,
    message::{query_methods, TransactionId},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindNodeQuery {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "q")]
    pub(crate) query_method: query_methods::find_node,

    #[serde(rename = "a")]
    pub(crate) body: FindNodeArgs,
}

impl FindNodeQuery {
    pub fn new(transaction_id: TransactionId, id: NodeId, target: NodeId) -> Self {
        FindNodeQuery {
            transaction_id,
            message_type: b"q".to_vec().into_boxed_slice(),
            query_method: query_methods::find_node,
            body: FindNodeArgs { id, target },
        }
    }
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindNodeArgs {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,

    #[serde_as(as = "Bytes")]
    pub target: NodeId,
}
