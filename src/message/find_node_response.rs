use crate::{
    domain_knowledge::NodeId,
    message::{QueryMethod, TransactionId},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindNodeResponse {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "r")]
    pub(crate) body: FindNodeResponseBody,
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindNodeResponseBody {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,

    #[serde_as(as = "Bytes")]
    pub nodes: Box<[u8]>,
}
