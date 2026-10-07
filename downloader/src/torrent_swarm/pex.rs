//! BEP 11: telling peers about each other.

use crate::settings::PEX_MAX_ADDED_PEERS;
use rand::seq::IndexedRandom;
use std::net::SocketAddr;

use super::TorrentSwarm;

impl TorrentSwarm {
    /// BEP 11 (PEX): tell each peer about every *other* peer we know of. No per-peer diffing
    /// against what we've told them before ("added"/"dropped" bookkeeping) -- we just resend
    /// the current full membership every round, which is redundant but simple and spec-legal
    /// (PEX is a discovery hint, not an authoritative membership feed).
    pub(super) fn run_pex_round(&mut self) {
        // BEP 27: a private torrent's peers must come only from its trackers.
        if self.torrent.private {
            return;
        }

        let all: Vec<(SocketAddr, u8)> = self.peers.iter().map(|p| (p.remote_addr, p.pex_flags())).collect();
        self.broadcast(|peer| {
            // BEP 11 recommends capping a single PEX message at roughly 50 added peers; a
            // fresh random sample each round, so over time every peer hears of the whole swarm
            let added: Vec<(SocketAddr, u8)> = all
                .sample(&mut rand::rng(), PEX_MAX_ADDED_PEERS + 1)
                .copied()
                .filter(|(a, _)| *a != peer.remote_addr)
                .take(PEX_MAX_ADDED_PEERS)
                .collect();
            if added.is_empty() {
                return Ok(());
            }
            peer.send_pex(&added)
        });
    }
}
