use bendy::encoding::SingleItemEncoder;

use crate::types::NodeId;

use super::{ToKrpcBody, Want};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct FindNodeQuery {
    requestor: NodeId,
    target: NodeId,
    want: Option<Want>,
}

impl FindNodeQuery {
    pub fn new(requestor: NodeId, target: NodeId) -> Self {
        Self {
            requestor,
            target,
            want: None,
        }
    }

    pub fn with_want(mut self, want: Option<Want>) -> Self {
        self.want = want;
        self
    }

    pub fn target(&self) -> NodeId {
        self.target
    }

    pub fn requestor(&self) -> NodeId {
        self.requestor
    }

    pub fn want(&self) -> Option<Want> {
        self.want
    }
}

impl ToKrpcBody for FindNodeQuery {
    fn encode_body(&self, enc: SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        enc.emit_unsorted_dict(|enc| {
            enc.emit_pair(b"id", self.requestor)?;
            if let Some(want) = self.want {
                enc.emit_pair_with(b"want", |e| want.encode(e))?;
            }
            enc.emit_pair(b"target", self.target)
        })
    }
}
