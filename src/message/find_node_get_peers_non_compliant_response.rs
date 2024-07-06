use crate::domain_knowledge::{BetterCompactNodeInfo, BetterNodeId, TransactionId};

use super::ToRawKrpc;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterFindNodeNonComGetPeersResponse {
    pub transaction_id: TransactionId,
    pub target_id: BetterNodeId,
    pub nodes: Vec<BetterCompactNodeInfo>,
}

impl BetterFindNodeNonComGetPeersResponse {
    pub fn new(
        txn_id: TransactionId,
        target: BetterNodeId,
        nodes: Vec<BetterCompactNodeInfo>,
    ) -> BetterFindNodeNonComGetPeersResponse {
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

    pub fn txn_id(&self) -> &TransactionId {
        &self.transaction_id
    }
}

impl ToRawKrpc for BetterFindNodeNonComGetPeersResponse {
    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();
        encoder.emit_and_sort_dict(|e| {
            e.emit_pair(b"t", self.transaction_id.as_bytes());
            e.emit_pair(b"y", "r");

            e.emit_pair_with(b"r", |e| {
                e.emit_unsorted_dict(|e| {
                    e.emit_pair(b"id", &self.target_id);

                    // values is a list of compact peer contacts, which are a 4 byte ipv4 address and a 2 byte port
                    // number. Unfortunately, the bittorrent people are insane and decided to encode this as a string
                    // using ascii in network/big endian.
                    e.emit_pair_with(b"values", |e| {
                        let combined = self.nodes.iter().map(|peer| {
                            let node_id = &peer.id;

                            let octets = peer.contact.0.ip().octets();
                            let port_in_ne = peer.contact.0.port().to_be_bytes();

                            let mut arr = [0u8; 26];
                            let id = &mut arr[0..20];
                            id.copy_from_slice(&node_id.0);

                            let ip = &mut arr[20..24];
                            ip.copy_from_slice(&octets);

                            let port = &mut arr[24..26];
                            port.copy_from_slice(&port_in_ne);

                            // because bendy is stupid
                            Vec::from_iter(arr)
                        });
                        e.emit_unchecked_list(combined)
                    })
                })
            })
        });

        encoder
            .get_output()
            .expect("we know the keys upfront, this should never error")
            .into_boxed_slice()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        domain_knowledge::{BetterNodeId, TransactionId},
        message::{find_node_query::BetterFindNodeQuery, ToRawKrpc},
    };

    #[test]
    fn can_encode_example() {
        let message = BetterFindNodeQuery::new(
            TransactionId::from_bytes(*&b"aa"),
            BetterNodeId::from_bytes_unchecked(*&b"abcdefghij0123456789"),
            BetterNodeId::from_bytes_unchecked(*&b"mnopqrstuvwxyz123456"),
        );
        let bytes = message.to_raw_krpc();

        // taken directly from the spec
        assert_eq!(
            &*bytes,
            b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe" as &[u8]
        );
    }
}
