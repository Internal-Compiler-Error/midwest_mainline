use crate::{
    domain_knowledge::NodeId,
    message::{ping_query::PingArgs, QueryMethod, TransactionId},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingAnnouncePeerResponse {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "r")]
    pub(crate) body: PingAnnouncePeerResponseBody,
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
// ping and announce peer response look exactly the same, no can't distinguish them by just
// looking at the fields
pub struct PingAnnouncePeerResponseBody {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,
}
