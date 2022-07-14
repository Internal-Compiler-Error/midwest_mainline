use crate::message::{QueryMethod, TransactionId};
use serde_with::{Bytes, serde_as};
use serde::{Deserialize, Serialize};
use crate::domain_knowledge::NodeId;

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
    pub(crate) query_method: QueryMethod,

    #[serde(rename = "a")]
    pub(crate) body: PingArgs,
}

impl PingQuery {
    pub fn new(transaction_id: TransactionId, body: PingArgs) -> Self {
        PingQuery {
            transaction_id,
            message_type: b"q".to_vec().into_boxed_slice(),
            query_method: QueryMethod::Ping,
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
