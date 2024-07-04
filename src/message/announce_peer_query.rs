use crate::{
    domain_knowledge::NodeId,
    message::{query_methods, InfoHash, TransactionId},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};
use crate::domain_knowledge::{BetterInfoHash, BetterNodeId};
use crate::message::ToRawKrpc;

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnouncePeerQuery {
    #[serde_as(as = "Bytes")]
    #[serde(rename = "t")]
    pub(crate) transaction_id: TransactionId,

    #[serde(rename = "y")]
    #[serde_as(as = "Bytes")]
    pub(crate) message_type: Box<[u8]>,

    #[serde(rename = "q")]
    pub(crate) query_method: query_methods::announce_peer,

    #[serde(rename = "a")]
    pub(crate) body: AnnouncePeerArgs,
}

impl AnnouncePeerQuery {
    pub fn new(
        transaction_id: TransactionId,
        id: NodeId,
        token: Box<[u8]>,
        port: u16,
        implied_port: bool,
        info_hash: InfoHash,
    ) -> Self {
        AnnouncePeerQuery {
            transaction_id,
            message_type: b"q".to_vec().into_boxed_slice(),
            query_method: query_methods::announce_peer,
            body: AnnouncePeerArgs {
                id,
                token,
                info_hash,
                implied_port: if implied_port { 1 } else { 0 },
                port,
            },
        }
    }
}

#[serde_as]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnouncePeerArgs {
    #[serde_as(as = "Bytes")]
    pub id: NodeId,
    pub implied_port: u8,

    #[serde_as(as = "Bytes")]
    pub info_hash: InfoHash,
    pub port: u16,

    #[serde_as(as = "Bytes")]
    pub token: Box<[u8]>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterAnnouncePeerQuery {
    transaction_id: String,
    ourself: BetterNodeId,
    implied_port: bool,
    info_hash: BetterInfoHash,
    port: u16,
    token: String,
}

impl BetterAnnouncePeerQuery {
    pub fn new(transaction_id: String, ourself: BetterNodeId, port: Option<u16>, info_hash: BetterInfoHash, token: String) -> Self {
        Self {
            transaction_id,
            ourself,
            implied_port: port.is_none(),
            info_hash,
            port: port.unwrap_or(6881),
            token,
        }
    }

    pub fn txn_id(&self) -> &str {
        &self.transaction_id
    }
}

impl ToRawKrpc for BetterAnnouncePeerQuery {
    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();
        encoder.emit_and_sort_dict(|e| {
            e.emit_pair(b"t", &self.transaction_id);
            e.emit_pair(b"y", &"q");
            e.emit_pair(b"q", &"announce_peer");

            e.emit_pair_with(b"a", |e| {
                e.emit_unsorted_dict(|e| {
                    e.emit_pair(b"id", &self.ourself);
                    e.emit_pair(b"token", &self.token);
                    e.emit_pair(b"implied_port", if self.implied_port { 1 } else { 0 });
                    e.emit_pair(b"info_hash", &self.info_hash);
                    e.emit_pair(b"port", &self.port)
                })
            })
        });

        encoder.get_output().expect("we know the encoder is valid").into_boxed_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_encode_example() {
        use std::str;

        let announce = BetterAnnouncePeerQuery::new(
            "aa".to_string(),
            BetterNodeId::new("abcdefghij0123456789".to_string()).unwrap(),
            None,
            BetterInfoHash::new("mnopqrstuvwxyz123456".to_string()).unwrap(),
            "aoeusnth".to_string(),
        );

        let encoded = announce.to_raw_krpc();
        let expected = "d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe";

        assert_eq!(expected, str::from_utf8(&*encoded).unwrap());
    }
}
