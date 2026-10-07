//! BEP 44's two queries: `get` an item stored at a target, and `put` one there.

use bendy::encoding::SingleItemEncoder;

use crate::types::{NodeId, Token};

use super::{ToKrpcBody, Want, emit_raw};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct GetQuery {
    requestor: NodeId,
    target: NodeId,
    /// a mutable item only if its sequence number is above this
    seq: Option<i64>,
    want: Option<Want>,
}

impl GetQuery {
    pub fn new(requestor: NodeId, target: NodeId) -> Self {
        Self {
            requestor,
            target,
            seq: None,
            want: None,
        }
    }

    pub fn with_seq(mut self, seq: Option<i64>) -> Self {
        self.seq = seq;
        self
    }

    pub fn with_want(mut self, want: Option<Want>) -> Self {
        self.want = want;
        self
    }

    pub fn requestor(&self) -> NodeId {
        self.requestor
    }

    pub fn target(&self) -> NodeId {
        self.target
    }

    pub fn seq(&self) -> Option<i64> {
        self.seq
    }

    pub fn want(&self) -> Option<Want> {
        self.want
    }
}

impl ToKrpcBody for GetQuery {
    #[allow(unused_must_use)]
    fn encode_body(&self, enc: SingleItemEncoder) {
        enc.emit_unsorted_dict(|enc| {
            enc.emit_pair(b"id", self.requestor)?;
            if let Some(seq) = self.seq {
                enc.emit_pair(b"seq", seq)?;
            }
            if let Some(want) = self.want {
                enc.emit_pair_with(b"want", |e| want.encode(e))?;
            }
            enc.emit_pair(b"target", self.target)
        })
        .unwrap()
    }
}

/// What makes an item mutable: the key it's signed with, and the signature over its salt,
/// sequence number and value
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Signed {
    pub key: [u8; 32],
    pub salt: Vec<u8>,
    pub seq: i64,
    pub sig: [u8; 64],
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct PutQuery {
    requestor: NodeId,
    token: Token,
    /// the value, bencoded
    value: Vec<u8>,
    signed: Option<Signed>,
    /// compare and swap: store only if what's stored has this sequence number
    cas: Option<i64>,
}

impl PutQuery {
    /// `value` is bencoded
    pub fn new(requestor: NodeId, token: Token, value: Vec<u8>, signed: Option<Signed>) -> Self {
        Self {
            requestor,
            token,
            value,
            signed,
            cas: None,
        }
    }

    pub fn with_cas(mut self, cas: Option<i64>) -> Self {
        self.cas = cas;
        self
    }

    pub fn requestor(&self) -> NodeId {
        self.requestor
    }

    pub fn token(&self) -> &Token {
        &self.token
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn signed(&self) -> Option<&Signed> {
        self.signed.as_ref()
    }

    pub fn cas(&self) -> Option<i64> {
        self.cas
    }
}

impl ToKrpcBody for PutQuery {
    #[allow(unused_must_use)]
    fn encode_body(&self, enc: SingleItemEncoder) {
        enc.emit_unsorted_dict(|enc| {
            enc.emit_pair(b"id", self.requestor)?;
            enc.emit_pair(b"token", &self.token)?;
            enc.emit_pair_with(b"v", |e| emit_raw(e, &self.value))?;
            if let Some(cas) = self.cas {
                enc.emit_pair(b"cas", cas)?;
            }
            if let Some(signed) = &self.signed {
                enc.emit_pair_with(b"k", |e| e.emit_bytes(&signed.key))?;
                if !signed.salt.is_empty() {
                    enc.emit_pair_with(b"salt", |e| e.emit_bytes(&signed.salt))?;
                }
                enc.emit_pair(b"seq", signed.seq)?;
                enc.emit_pair_with(b"sig", |e| e.emit_bytes(&signed.sig))?;
            }
            Ok(())
        })
        .unwrap()
    }
}
