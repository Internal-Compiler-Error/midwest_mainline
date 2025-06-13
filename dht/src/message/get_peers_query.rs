use bendy::encoding::SingleItemEncoder;

use crate::types::{InfoHash, NodeId};

use super::ToKrpcBody;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct GetPeersQuery {
    requestor: NodeId,
    info_hash: InfoHash,
}

impl GetPeersQuery {
    pub fn new(requestor: NodeId, info_hash: InfoHash) -> Self {
        Self { requestor, info_hash }
    }

    pub fn requestor(&self) -> &NodeId {
        &self.requestor
    }

    pub fn info_hash(&self) -> &InfoHash {
        &self.info_hash
    }
}

impl ToKrpcBody for GetPeersQuery {
    #[allow(unused_must_use)]
    fn encode_body(&self, enc: SingleItemEncoder) {
        enc.emit_unsorted_dict(|enc| {
            enc.emit_pair(b"id", &self.requestor)?;
            enc.emit_pair(b"info_hash", &self.info_hash)
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
    fn can_encode_exmaple() {
        use std::str;

        let txn_id = TransactionId::from_bytes(*&b"aa");
        let query = GetPeersQuery::new(
            NodeId::from_bytes(*&b"abcdefghij0123456789"),
            InfoHash::from_bytes(*&b"mnopqrstuvwxyz123456"),
        );

        let encoded = Krpc::new_with_body(txn_id, KrpcBody::GetPeersQuery(query)).encode();
        let expected =
            "d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe";

        assert_eq!(expected, str::from_utf8(&encoded).unwrap());
    }
}
