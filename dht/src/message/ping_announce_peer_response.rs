use bendy::encoding::SingleItemEncoder;

use crate::types::NodeId;

use super::ToKrpcBody;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct PingAnnouncePeerResponse {
    target_id: NodeId,
}

impl PingAnnouncePeerResponse {
    pub fn new(target_id: NodeId) -> Self {
        Self { target_id }
    }

    pub fn target_id(&self) -> &NodeId {
        &self.target_id
    }
}

impl ToKrpcBody for PingAnnouncePeerResponse {
    #[allow(unused_must_use)]
    fn encode_body(&self, enc: SingleItemEncoder) {
        enc.emit_unsorted_dict(|enc| enc.emit_pair(b"id", &self.target_id))
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
        let ping_response = PingAnnouncePeerResponse::new(NodeId::from_bytes(*&b"mnopqrstuvwxyz123456"));

        let expected = "d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
        let encoded = Krpc::new_with_body(txn_id, KrpcBody::PingAnnouncePeerResponse(ping_response)).encode();

        assert_eq!(expected, str::from_utf8(&encoded).unwrap());
    }
}
