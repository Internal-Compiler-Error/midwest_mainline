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
    /// BEP 33: the announcer is a seed
    seed: bool,
}

impl AnnouncePeerQuery {
    pub fn new(requestor: NodeId, implied_port: bool, port: u16, info_hash: InfoHash, token: Token) -> Self {
        Self {
            requestor,
            implied_port,
            info_hash,
            port,
            token,
            seed: false,
        }
    }

    pub fn with_seed(mut self, seed: bool) -> Self {
        self.seed = seed;
        self
    }

    pub fn seed(&self) -> bool {
        self.seed
    }

    pub fn token(&self) -> &Token {
        &self.token
    }

    pub fn requestor(&self) -> NodeId {
        self.requestor
    }

    pub fn implied_port(&self) -> bool {
        self.implied_port
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn info_hash(&self) -> InfoHash {
        self.info_hash
    }
}

impl ToKrpcBody for AnnouncePeerQuery {
    fn encode_body(&self, enc: SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        enc.emit_unsorted_dict(|enc| {
            enc.emit_pair(b"id", self.requestor)?;
            enc.emit_pair(b"token", &self.token)?;
            enc.emit_pair(b"implied_port", if self.implied_port { 1 } else { 0 })?;
            enc.emit_pair(b"info_hash", self.info_hash)?;
            if self.seed {
                enc.emit_pair(b"seed", 1)?;
            }
            enc.emit_pair(b"port", self.port)
        })
    }
}
