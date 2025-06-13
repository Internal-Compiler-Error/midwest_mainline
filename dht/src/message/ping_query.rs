use bendy::encoding::SingleItemEncoder;

use crate::types::NodeId;

use super::ToKrpcBody;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct PingQuery {
    requestor: NodeId,
}

impl PingQuery {
    pub fn new(requestor: NodeId) -> Self {
        Self { requestor }
    }

    pub fn requestor(&self) -> &NodeId {
        &self.requestor
    }
}

impl ToKrpcBody for PingQuery {
    #[allow(unused_must_use)]
    fn encode_body(&self, enc: SingleItemEncoder) {
        enc.emit_unsorted_dict(|enc| enc.emit_pair(b"id", &self.requestor))
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
    fn can_encode_from_example() {
        use std::str;

        let txn_id = TransactionId::from_bytes(*&b"aa");
        let ping_query = PingQuery::new(NodeId::from_bytes(*&b"abcdefghij0123456789"));

        let serailzied = Krpc::new_with_body(txn_id, KrpcBody::PingQuery(ping_query)).encode();
        let expected = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";

        assert_eq!(
            str::from_utf8(&*serailzied).unwrap(),
            str::from_utf8(expected.as_ref()).unwrap()
        );
    }
}
