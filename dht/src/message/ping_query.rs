use bendy::encoding::SingleItemEncoder;

use crate::types::NodeId;

use super::ToKrpcBody;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct PingQuery {
    requestor: NodeId,
}

impl PingQuery {
    pub fn new(requestor: NodeId) -> Self {
        Self { requestor }
    }

    pub fn requestor(&self) -> NodeId {
        self.requestor
    }
}

impl ToKrpcBody for PingQuery {
    fn encode_body(&self, enc: SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        enc.emit_unsorted_dict(|enc| enc.emit_pair(b"id", self.requestor))
    }
}
