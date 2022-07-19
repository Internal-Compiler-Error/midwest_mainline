use crate::{
    domain_knowledge::NodeId,
    message::{QueryMethod, TransactionId},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
/// this response is either to a find node query or a *very evil* client that sends out a response
/// to get peers query without the token when they don't know the exact peer that controls the
/// info hash.
pub struct FindNodeGetPeersNonCompliantResponse {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "r")]
    pub(crate) body: FindNodeGetPeersNonCompliantResponseBody,
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindNodeGetPeersNonCompliantResponseBody {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,

    #[serde_as(as = "Bytes")]
    pub nodes: Box<[u8]>,
}
