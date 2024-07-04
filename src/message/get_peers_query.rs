use crate::{
    domain_knowledge::{NodeId, BetterNodeId, BetterInfoHash},
    message::{query_methods, InfoHash, TransactionId},
};
use bendy::encoding::ToBencode;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

use super::ToRawKrpc;

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPeersQuery {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "q")]
    pub(crate) query_method: query_methods::get_peers,

    #[serde(rename = "a")]
    pub(crate) body: GetPeersArgs,
}

impl GetPeersQuery {
    pub fn new(transaction_id: TransactionId, id: NodeId, info_hash: InfoHash) -> Self {
        GetPeersQuery {
            transaction_id,
            message_type: b"q".to_vec().into_boxed_slice(),
            query_method: query_methods::get_peers,
            body: GetPeersArgs { info_hash, id },
        }
    }
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPeersArgs {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,

    #[serde_as(as = "Bytes")]
    pub info_hash: InfoHash,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterGetPeersQuery {
    transaction_id: String,
    ourself: BetterNodeId,
    info_hash: BetterInfoHash,
}

impl BetterGetPeersQuery {
    pub fn new(transaction_id: String, ourself: BetterNodeId, info_hash: BetterInfoHash) ->  Self {
        Self {
            transaction_id,
            ourself,
            info_hash,
        }
    }

    pub fn txn_id(&self) -> &str {
        &self.transaction_id
    }
}

impl ToRawKrpc for BetterGetPeersQuery {

    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();

        encoder.emit_and_sort_dict(|e| {
            e.emit_pair(b"t", &self.transaction_id);
            e.emit_pair(b"y", &"q");
            e.emit_pair(b"q", &"get_peers"); 

            e.emit_pair_with(b"a", |e| {
                e.emit_unsorted_dict(|e| {
                   e.emit_pair(b"id", &self.ourself);
                 e.emit_pair(b"info_hash", &self.info_hash)
                })
            })
        });

        encoder.get_output().expect("we know all the fields upfront, this should never error").into_boxed_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_encode_exmaple() {
        use std::str;

        let query  = super::BetterGetPeersQuery::new(
            "aa".to_string(),
            BetterNodeId::new("abcdefghij0123456789".to_string()).unwrap(),
            BetterInfoHash::new("mnopqrstuvwxyz123456".to_string()).unwrap()
        );

        let encoded = query.to_raw_krpc();
        let expected = "d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe";

        assert_eq!(expected, str::from_utf8(&encoded).unwrap());
    }
}

