use bendy::encoding::SingleItemEncoder;

use crate::types::NodeId;

use super::{ToKrpcBody, Want};

/// BEP 51: a sample of the info hashes a node stores, plus, like find_node, the nodes it knows
/// closest to `target` (so one query per node walks the keyspace)
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct SampleInfohashesQuery {
    requestor: NodeId,
    target: NodeId,
    want: Option<Want>,
}

impl SampleInfohashesQuery {
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

    pub fn requestor(&self) -> NodeId {
        self.requestor
    }

    pub fn target(&self) -> NodeId {
        self.target
    }

    pub fn want(&self) -> Option<Want> {
        self.want
    }
}

impl ToKrpcBody for SampleInfohashesQuery {
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
