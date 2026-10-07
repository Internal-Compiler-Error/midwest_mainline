use bendy::encoding::SingleItemEncoder;

use crate::types::NodeId;

use super::ToKrpcBody;

/// The answer to ping, announce_peer and put: just who answered
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct PingAnnouncePeerResponse {
    queried: NodeId,
}

impl PingAnnouncePeerResponse {
    pub fn new(queried: NodeId) -> Self {
        Self { queried }
    }

    pub fn queried(&self) -> NodeId {
        self.queried
    }
}

impl ToKrpcBody for PingAnnouncePeerResponse {
    fn encode_body(&self, enc: SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        enc.emit_unsorted_dict(|enc| enc.emit_pair(b"id", self.queried))
    }
}
