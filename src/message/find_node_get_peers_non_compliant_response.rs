use crate::domain_knowledge::{BetterCompactNodeInfo, BetterNodeId};

use super::ToRawKrpc;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterFindNodeNonComGetPeersResponse {
    pub transaction_id: String,
    pub target_id: BetterNodeId,
    pub nodes: Vec<BetterCompactNodeInfo>,
}

impl BetterFindNodeNonComGetPeersResponse {
    pub fn new(
        txn_id: String,
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

    pub fn txn_id(&self) -> &str {
        &self.transaction_id
    }
}

impl ToRawKrpc for BetterFindNodeNonComGetPeersResponse {
    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();
        encoder.emit_and_sort_dict(|e| {
            e.emit_pair(b"t", &self.transaction_id);
            e.emit_pair(b"y", "r");

            e.emit_pair_with(b"r", |e| {
                e.emit_unsorted_dict(|e| {
                    e.emit_pair(b"id", &self.target_id);

                    // values is a list of compact peer contacts, which are a 4 byte ipv4 address and a 2 byte port
                    // number. Unfortunately, the bittorrent people are insane and decided to encode this as a string
                    // using ascii in network/big endian.
                    e.emit_pair_with(b"values", |e| {
                        use std::str;

                        let combined = self.nodes.iter().map(|peer| {
                            let node_id = &peer.id;
                            let _id_as_ascii = node_id.0.as_bytes();

                            let octets = peer.contact.0.ip().octets();
                            let port_in_ne = peer.contact.0.port().to_be_bytes();

                            let ip_as_ascii = str::from_utf8(octets.as_slice()).expect("we know the ip is ascii");
                            let port_as_ascii =
                                str::from_utf8(port_in_ne.as_slice()).expect("we know the port is ascii");

                            // TODO: shouldn't they be transmitted raw instead?
                            format!("{}{}{}", node_id.0, ip_as_ascii, port_as_ascii)
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
    use crate::{domain_knowledge::BetterNodeId, message::{find_node_query::BetterFindNodeQuery, ToRawKrpc}};

        #[test]
        fn can_encode_example() {
            let message = BetterFindNodeQuery::new("aa".to_string(), BetterNodeId("abcdefghij0123456789".to_string()), BetterNodeId("mnopqrstuvwxyz123456".to_string()));
            let bytes = message.to_raw_krpc();

            // taken directly from the spec
            assert_eq!(
                &*bytes,
                b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe" as &[u8]
            );
    }
}
