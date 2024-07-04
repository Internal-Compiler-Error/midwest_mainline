use crate::{domain_knowledge::NodeId, message::TransactionId, domain_knowledge::BetterNodeId};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

use super::ToRawKrpc;

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingAnnouncePeerResponse {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "r")]
    pub(crate) body: PingAnnouncePeerResponseBody,
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
// ping and announce peer response look exactly the same, no can't distinguish them by just
// looking at the fields
pub struct PingAnnouncePeerResponseBody {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,
}


#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterPingAnnouncePeerResponse {
    transaction_id: String,
    target_id: BetterNodeId,
}

impl BetterPingAnnouncePeerResponse {
    pub fn new(transaction_id: String, target_id: BetterNodeId) -> Self {
        Self {
            transaction_id,
            target_id,
        }
    }

    pub fn txn_id(&self) -> &str {
        &self.transaction_id
    }
}

impl ToRawKrpc for BetterPingAnnouncePeerResponse {
    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();
        encoder.emit_and_sort_dict(|e| {
            e.emit_pair(b"t", &self.transaction_id);
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

        let ping_response = BetterPingAnnouncePeerResponse::new("aa".into(), BetterNodeId::new("mnopqrstuvwxyz123456".into()).unwrap());

        let expected = "d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
        let encoded = ping_response.to_raw_krpc();

        assert_eq!(expected, str::from_utf8(&encoded).unwrap());
    }
}
