use crate::domain_knowledge::{BetterCompactNodeInfo, BetterNodeId};

use super::ToRawKrpc;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterFindNodeNonComGetPeersResponse {
    pub transaction_id: String,
    pub target_id: BetterNodeId,
    pub nodes: Vec<BetterCompactNodeInfo>,
}

impl BetterFindNodeNonComGetPeersResponse {
    pub fn new(txn_id: String, target: BetterNodeId, nodes: Vec<BetterCompactNodeInfo>) -> BetterFindNodeNonComGetPeersResponse {
        BetterFindNodeNonComGetPeersResponse {
            transaction_id: txn_id,
            target_id: target,
            nodes,
        }
    }

    pub fn nodes(&self) -> &Vec<BetterCompactNodeInfo> {
        &self.nodes
    }

    pub fn target_id(&self) -> &BetterNodeId {
        &self.target_id
    }

    pub fn txn_id(&self) -> &str {
        &self.transaction_id
    }
}

impl ToRawKrpc for BetterFindNodeNonComGetPeersResponse {
    fn to_raw_krpc(&self) -> Box<[u8]> {
        todo!()
    }
}
