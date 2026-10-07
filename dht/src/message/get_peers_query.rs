use bendy::encoding::SingleItemEncoder;

use crate::types::{InfoHash, NodeId};

use super::{ToKrpcBody, Want};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct GetPeersQuery {
    requestor: NodeId,
    info_hash: InfoHash,
    want: Option<Want>,
    /// BEP 33: answer with bloom filters of the seeds and peers too
    scrape: bool,
    /// BEP 33: rather peers than seeds in `values`
    noseed: bool,
}

impl GetPeersQuery {
    pub fn new(requestor: NodeId, info_hash: InfoHash) -> Self {
        Self {
            requestor,
            info_hash,
            want: None,
            scrape: false,
            noseed: false,
        }
    }

    pub fn with_want(mut self, want: Option<Want>) -> Self {
        self.want = want;
        self
    }

    pub fn with_scrape(mut self, scrape: bool) -> Self {
        self.scrape = scrape;
        self
    }

    pub fn with_noseed(mut self, noseed: bool) -> Self {
        self.noseed = noseed;
        self
    }

    pub fn scrape(&self) -> bool {
        self.scrape
    }

    pub fn noseed(&self) -> bool {
        self.noseed
    }

    pub fn requestor(&self) -> NodeId {
        self.requestor
    }

    pub fn info_hash(&self) -> InfoHash {
        self.info_hash
    }

    pub fn want(&self) -> Option<Want> {
        self.want
    }
}

impl ToKrpcBody for GetPeersQuery {
    fn encode_body(&self, enc: SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        enc.emit_unsorted_dict(|enc| {
            enc.emit_pair(b"id", self.requestor)?;
            if let Some(want) = self.want {
                enc.emit_pair_with(b"want", |e| want.encode(e))?;
            }
            if self.scrape {
                enc.emit_pair(b"scrape", 1)?;
            }
            if self.noseed {
                enc.emit_pair(b"noseed", 1)?;
            }
            enc.emit_pair(b"info_hash", self.info_hash)
        })
    }
}
