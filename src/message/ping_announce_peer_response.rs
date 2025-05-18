use crate::domain_knowledge::{NodeId, TransactionId};

use super::ToRawKrpc;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct PingAnnouncePeerResponse {
    transaction_id: TransactionId,
    target_id: NodeId,
}

impl PingAnnouncePeerResponse {
    pub fn new(transaction_id: TransactionId, target_id: NodeId) -> Self {
        Self {
            transaction_id,
            target_id,
        }
    }

    pub fn txn_id(&self) -> &TransactionId {
        &self.transaction_id
    }

    pub fn target_id(&self) -> &NodeId {
        &self.target_id
    }
}

impl ToRawKrpc for PingAnnouncePeerResponse {
    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();
        encoder.emit_and_sort_dict(|e| {
            e.emit_pair_with(b"t", |e| e.emit_bytes(self.transaction_id.as_bytes()));
            e.emit_pair(b"y", "r");
            e.emit_pair_with(b"r", |e| e.emit_dict(|mut e| e.emit_pair(b"id", &self.target_id)))
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
    fn can_encode_example() {
        use std::str;

        let ping_response = PingAnnouncePeerResponse::new(
            TransactionId::from_bytes(*&b"aa"),
            NodeId::from_bytes_unchecked(*&b"mnopqrstuvwxyz123456"),
        );

        let expected = "d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
        let encoded = ping_response.to_raw_krpc();

        assert_eq!(expected, str::from_utf8(&encoded).unwrap());
    }
}
