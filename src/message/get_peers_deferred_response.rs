use crate::domain_knowledge::{BetterCompactNodeInfo, BetterNodeId};

use super::ToRawKrpc;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BetterGetPeersDeferredResponse {
    // TODO: make them private
    transaction_id: String,
    pub querier: BetterNodeId,
    pub token: String,
    pub nodes: Vec<BetterCompactNodeInfo>,
}

impl BetterGetPeersDeferredResponse {
    pub fn new(
        transaction_id: String,
        querier: BetterNodeId,
        token: String,
        nodes: Vec<BetterCompactNodeInfo>,
    ) -> Self {
        Self {
            transaction_id,
            querier,
            token,
            nodes,
        }
    }

    pub fn txn_id(&self) -> &str {
        &self.transaction_id
    }
}

impl ToRawKrpc for BetterGetPeersDeferredResponse {
    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();

        encoder.emit_and_sort_dict(|e| {
            e.emit_pair(b"t", &self.transaction_id);
            e.emit_pair(b"y", &"r");

            e.emit_pair_with(b"r", |e| {
                e.emit_unsorted_dict(|e| {
                    e.emit_pair(b"id", &self.querier);
                    e.emit_pair(b"token", &self.token);

                    // values is a list of compact peer contacts, which are a 4 byte ipv4 address and a 2 byte port
                    // number. Unfortunately, the bittorrent people are insane and decided to encode this as a string
                    // using ascii in network/big endian.
                    e.emit_pair_with(b"nodes", |e| {
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
            .expect("we know all the fields upfront, this should never error")
            .into_boxed_slice()
    }
}

#[cfg(test)]
mod tests {
    // TODO:
}
