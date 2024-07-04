use crate::{
    domain_knowledge::{NodeId, BetterNodeId},
    message::{query_methods, TransactionId},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

use super::ToRawKrpc;

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

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterPingQuery {
    transaction_id: String,
    target_id: BetterNodeId,
}

impl BetterPingQuery {
    // TODO: think about a better error type 
    pub fn new(transaction_id: String, target_id: BetterNodeId) ->  Self {
        Self {
            transaction_id,
            target_id,
        }
    }

    pub fn txn_id(&self) -> &str {
        &self.transaction_id
    }

    pub fn target_id(&self) -> &BetterNodeId {
        &self.target_id
    }
}

impl ToRawKrpc for BetterPingQuery {

    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();
        encoder.emit_and_sort_dict(|e| {
            e.emit_pair(b"t", &self.transaction_id);
            e.emit_pair(b"y", "q");
            e.emit_pair(b"q", "ping");
            e.emit_pair_with(b"a", |e| {
                e.emit_dict(|mut e| { e.emit_pair(b"id", &self.target_id) })
            })
        });

        encoder.get_output().expect("we know all the fields upfront, this should never error").into_boxed_slice()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_encode_from_example() {
        use std::str;

        let ping_query = BetterPingQuery::new(
            "aa".into(),
            BetterNodeId::new("abcdefghij0123456789".to_string()).unwrap()
        );

        let serailzied = ping_query.to_raw_krpc(); 
        let expected = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";

        assert_eq!(str::from_utf8(&*serailzied).unwrap(), str::from_utf8(expected.as_ref()).unwrap()); 
    }
}
