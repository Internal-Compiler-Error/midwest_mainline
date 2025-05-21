use crate::types::{InfoHash, NodeId, TransactionId};

use super::ToRawKrpc;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct GetPeersQuery {
    pub transaction_id: TransactionId,
    querier: NodeId,
    info_hash: InfoHash,
}

impl GetPeersQuery {
    pub fn new(transaction_id: TransactionId, ourself: NodeId, info_hash: InfoHash) -> Self {
        Self {
            transaction_id,
            querier: ourself,
            info_hash,
        }
    }

    pub fn txn_id(&self) -> &TransactionId {
        &self.transaction_id
    }

    pub fn querier(&self) -> &NodeId {
        &self.querier
    }

    pub fn info_hash(&self) -> &InfoHash {
        &self.info_hash
    }
}

impl ToRawKrpc for GetPeersQuery {
    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();

        encoder.emit_and_sort_dict(|e| {
            e.emit_pair_with(b"t", |e| e.emit_bytes(self.transaction_id.as_bytes()));
            e.emit_pair(b"y", &"q");
            e.emit_pair(b"q", &"get_peers");

            e.emit_pair_with(b"a", |e| {
                e.emit_unsorted_dict(|e| {
                    e.emit_pair(b"id", &self.querier);
                    e.emit_pair(b"info_hash", &self.info_hash)
                })
            })
        });

        encoder
            .get_output()
            .expect("we know all the fields upfront, this should never error")
            .into_boxed_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_encode_exmaple() {
        use std::str;

        let query = super::GetPeersQuery::new(
            TransactionId::from_bytes(*&b"aa"),
            NodeId::from_bytes_unchecked(*&b"abcdefghij0123456789"),
            InfoHash::from_bytes_unchecked(*&b"mnopqrstuvwxyz123456"),
        );

        let encoded = query.to_raw_krpc();
        let expected =
            "d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe";

        assert_eq!(expected, str::from_utf8(&encoded).unwrap());
    }
}
