use crate::{
    domain_knowledge::NodeId,
    message::{QueryMethod, Token, TransactionId},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};
use crate::domain_knowledge::CompactPeerContact;

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPeersDeferredResponseNonCompliant {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    // without this, we can't disambiguate from a find_node_response
    #[serde_as(as = "Bytes")]
    pub(crate) ip: Box<[u8]>,

    #[serde(rename = "r")]
    pub(crate) body: GetPeersDeferredResponseBodyNonCompliant,
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPeersDeferredResponseBodyNonCompliant {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,

    #[serde_as(as = "Bytes")]
    pub nodes: Box<[u8]>,
}
