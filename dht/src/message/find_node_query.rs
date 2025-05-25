use bendy::encoding::SingleItemEncoder;

use crate::types::NodeId;

use super::ToKrpcBody;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct FindNodeQuery {
    querier: NodeId,
    target: NodeId,
}

impl FindNodeQuery {
    pub fn new(ourself: NodeId, target: NodeId) -> Self {
        Self {
            querier: ourself,
            target,
        }
    }

    pub fn target_id(&self) -> NodeId {
        self.target
    }

    pub fn querier(&self) -> NodeId {
        self.querier
    }
}

impl ToKrpcBody for FindNodeQuery {
    #[allow(unused_must_use)]
    fn encode_body(&self, enc: SingleItemEncoder) {
        enc.emit_unsorted_dict(|enc| {
            enc.emit_pair(b"id", &self.querier)?;
            enc.emit_pair(b"target", &self.target)
        })
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        message::{Krpc, KrpcBody},
        types::TransactionId,
    };

    use super::*;

    #[test]
    fn can_encode_example() {
        use std::str;

        let txn_id = TransactionId::from_bytes(*&b"aa");
        let query = {
            let ourself = NodeId::from_bytes(*&b"abcdefghij0123456789");
            let target = NodeId::from_bytes(*&b"mnopqrstuvwxyz123456");
            FindNodeQuery {
                querier: ourself,
                target,
            }
        };

        let encoded = Krpc::new_with_body(txn_id, KrpcBody::FindNodeQuery(query)).encode();
        let expected = "d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe";

        assert_eq!(expected, str::from_utf8(&*encoded).unwrap())
    }
}
