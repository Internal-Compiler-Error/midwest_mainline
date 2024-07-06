use crate::domain_knowledge::{BetterNodeId, TransactionId};

use super::ToRawKrpc;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterFindNodeQuery {
    transaction_id: TransactionId,
    ourself: BetterNodeId,
    target: BetterNodeId,
}

impl BetterFindNodeQuery {
    pub fn new(transaction_id: TransactionId, ourself: BetterNodeId, target: BetterNodeId) -> Self {
        Self {
            transaction_id,
            ourself,
            target,
        }
    }

    pub fn txn_id(&self) -> &TransactionId {
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
            e.emit_pair_with(b"t", |e| e.emit_bytes(self.transaction_id.as_bytes()));
            e.emit_pair(b"y", &"q");
            e.emit_pair(b"q", &"find_node");

            e.emit_pair_with(b"a", |e| {
                e.emit_unsorted_dict(|e| {
                    e.emit_pair(b"id", &self.ourself);
                    e.emit_pair(b"target", &self.target)
                })
            })
        });

        encoder
            .get_output()
            .expect("we know the fields up front, this should never error")
            .into_boxed_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_encode_example() {
        use std::str;

        let query = BetterFindNodeQuery::new(
            TransactionId::from_bytes(*&b"aa"),
            BetterNodeId::from_bytes_unchecked(*&b"abcdefghij0123456789"),
            BetterNodeId::from_bytes_unchecked(*&b"mnopqrstuvwxyz123456"),
        );

        let encoded = query.to_raw_krpc();
        let expected = "d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe";

        assert_eq!(expected, str::from_utf8(&*encoded).unwrap())
    }
}
