use crate::{
    domain_knowledge::{NodeId, BetterNodeId},
    message::{query_methods, TransactionId},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

use super::ToRawKrpc;

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

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterFindNodeQuery {
    transaction_id: String,
    ourself: BetterNodeId,
    target: BetterNodeId,
}

impl BetterFindNodeQuery {
    pub fn new(transaction_id: String, ourself: BetterNodeId, target: BetterNodeId) -> Self {
        Self {
            transaction_id,
            ourself,
            target,
        }
    }

    pub fn txn_id(&self) -> &str {
        &self.transaction_id
    }

    pub fn target_id(&self) -> &BetterNodeId {
        &self.target
    }
}

impl ToRawKrpc for BetterFindNodeQuery {
    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();
        encoder.emit_and_sort_dict(|e| {
            e.emit_pair(b"t", &self.transaction_id);
            e.emit_pair(b"y", &"q");
            e.emit_pair(b"q", &"find_node");

            e.emit_pair_with(b"a", |e| {
                e.emit_unsorted_dict(|e| {
                    e.emit_pair(b"id", &self.ourself);
                    e.emit_pair(b"target", &self.target)
                })
            })
        });

        encoder.get_output().expect("we know the fields up front, this should never error").into_boxed_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_encode_example() {
        use std::str;

        let query = BetterFindNodeQuery::new(
            "aa".to_string(),
            BetterNodeId::new("abcdefghij0123456789".to_string()).unwrap(),
            BetterNodeId::new("mnopqrstuvwxyz123456".to_string()).unwrap(),
        );

        let encoded = query.to_raw_krpc();
        let expected = "d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe";

        assert_eq!(expected, str::from_utf8(&*encoded).unwrap())
    }
}
