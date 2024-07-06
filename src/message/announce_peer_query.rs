use crate::domain_knowledge::{BetterInfoHash, NodeId, Token, TransactionId};
use crate::message::ToRawKrpc;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterAnnouncePeerQuery {
    transaction_id: TransactionId,
    ourself: NodeId,
    implied_port: bool,
    info_hash: BetterInfoHash,
    port: u16,
    token: Token,
}

impl BetterAnnouncePeerQuery {
    pub fn new(
        transaction_id: TransactionId,
        ourself: NodeId,
        port: Option<u16>,
        info_hash: BetterInfoHash,
        token: Token,
    ) -> Self {
        Self {
            transaction_id,
            ourself,
            implied_port: port.is_none(),
            info_hash,
            port: port.unwrap_or(6881),
            token,
        }
    }

    /// Transaction id
    pub fn txn_id(&self) -> &TransactionId {
        &self.transaction_id
    }

    pub fn token(&self) -> &Token {
        &self.token
    }

    pub fn querying(&self) -> &NodeId {
        &self.ourself
    }

    pub fn implied_port(&self) -> bool {
        self.implied_port
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn info_hash(&self) -> &BetterInfoHash {
        &self.info_hash
    }
}

impl ToRawKrpc for BetterAnnouncePeerQuery {
    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();
        encoder.emit_and_sort_dict(|e| {
            e.emit_pair_with(b"t", |e| e.emit_bytes(self.transaction_id.as_bytes()));
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

        encoder
            .get_output()
            .expect("we know the encoder is valid")
            .into_boxed_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_encode_example() {
        use std::str;

        let announce = BetterAnnouncePeerQuery::new(
            TransactionId::from_bytes(*&b"aa"),
            NodeId::from_bytes_unchecked(*&b"abcdefghij0123456789"),
            None,
            BetterInfoHash::from_bytes_unchecked(*&b"mnopqrstuvwxyz123456"),
            Token::from_bytes(*&b"aoeusnth"),
        );

        let encoded = announce.to_raw_krpc();
        let expected = "d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe";

        assert_eq!(expected, str::from_utf8(&*encoded).unwrap());
    }
}
