use bendy::encoding::SingleItemEncoder;

use crate::message::ToKrpcBody;
use crate::types::{InfoHash, NodeId, Token};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct AnnouncePeerQuery {
    requestor: NodeId,
    implied_port: bool,
    info_hash: InfoHash,
    port: u16,
    token: Token,
}

impl AnnouncePeerQuery {
    pub fn new(requestor: NodeId, implied_port: bool, port: u16, info_hash: InfoHash, token: Token) -> Self {
        Self {
            requestor,
            implied_port,
            info_hash,
            port,
            token,
        }
    }

    pub fn token(&self) -> &Token {
        &self.token
    }

    pub fn requestor(&self) -> &NodeId {
        &self.requestor
    }

    pub fn implied_port(&self) -> bool {
        self.implied_port
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn info_hash(&self) -> &InfoHash {
        &self.info_hash
    }
}

impl ToKrpcBody for AnnouncePeerQuery {
    #[allow(unused_must_use)]
    fn encode_body(&self, enc: SingleItemEncoder) {
        enc.emit_unsorted_dict(|enc| {
            enc.emit_pair(b"id", &self.requestor)?;
            enc.emit_pair(b"token", &self.token)?;
            enc.emit_pair(b"implied_port", if self.implied_port { 1 } else { 0 })?;
            enc.emit_pair(b"info_hash", &self.info_hash)?;
            enc.emit_pair(b"port", &self.port)
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
        let announce = AnnouncePeerQuery::new(
            NodeId::from_bytes(*&b"abcdefghij0123456789"),
            true,
            6881u16,
            InfoHash::from_bytes(*&b"mnopqrstuvwxyz123456"),
            Token::from_bytes(*&b"aoeusnth"),
        );
        let announce = Krpc::new_with_body(txn_id, KrpcBody::AnnouncePeerQuery(announce));

        let encoded = announce.encode();
        let expected = "d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe";

        assert_eq!(expected, str::from_utf8(&*encoded).unwrap());
    }
}
