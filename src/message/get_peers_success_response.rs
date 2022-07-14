use crate::message::{QueryMethod, Token, TransactionId};
use crate::message::ping_query::PingArgs;
use serde_with::{Bytes, serde_as};
use serde::{Deserialize, Serialize};
use crate::domain_knowledge::{CompactPeerContact, NodeId};

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPeersSuccessResponse {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "r")]
    pub(crate) body: GetPeersSuccessResponseBody,
}


#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPeersSuccessResponseBody {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,

    #[serde_as(as = "Bytes")]
    pub token: Token,

    pub values: Vec<CompactPeerContact>,
}
