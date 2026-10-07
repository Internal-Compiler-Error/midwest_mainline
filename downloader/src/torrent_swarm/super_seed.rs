//! BEP 16 super-seeding, and BEP 21's partial seed.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use super::TorrentSwarm;

/// BEP 16, torrent-wide; each peer's own view is its `Peer::super_seed`.
pub(super) struct SuperSeed {
    /// switched on, whether or not there's everything to show yet (see `super_seeding`)
    pub(super) on: bool,
    /// how many peers each piece has been revealed to
    pub(super) offers: Vec<u32>,
}

impl SuperSeed {
    pub(super) fn new(pieces: usize) -> Self {
        SuperSeed {
            on: false,
            offers: vec![0; pieces],
        }
    }
}

impl TorrentSwarm {
    /// BEP 16: super-seeding is on, and there's every piece to show.
    pub(super) fn super_seeding(&self) -> bool {
        self.super_seed.on && self.stat.all_verified()
    }

    pub(super) fn set_super_seed(&mut self, on: bool) {
        self.super_seed.on = on;
        if on {
            // peers already connected have seen everything; it applies to newcomers
            return;
        }
        for idx in (0..self.peers.len()).rev() {
            let peer = &mut self.peers[idx];
            let Some(view) = peer.super_seed.take() else { continue };
            let hidden: Vec<u32> = self
                .stat
                .verified
                .iter_ones()
                .map(|p| p as u32)
                .filter(|p| !view.offered.contains(p) && !peer.they_have(*p))
                .collect();
            for piece in hidden {
                if peer.send_have(piece).is_err() {
                    self.drop_peer(idx, "send failed");
                    break;
                }
            }
        }
    }

    /// BEP 16: shows a super-seeded peer one more piece it lacks: the least common, counting
    /// both who has it and who it was shown to, so one copy of each goes out before seconds.
    pub(super) fn reveal_next_piece(&mut self, idx: usize) {
        let peer = &self.peers[idx];
        let Some(view) = &peer.super_seed else { return };
        let scatter = rand::random::<u32>();
        let pick = (0..self.picker.availability.len() as u32)
            .filter(|&p| !peer.they_have(p) && !view.offered.contains(&p))
            .min_by_key(|&p| {
                let seen = self.picker.availability[p as usize] + self.super_seed.offers[p as usize];
                (seen, p.wrapping_mul(0x9E37_79B9) ^ scatter)
            });
        let peer = &mut self.peers[idx];
        let view = peer.super_seed.as_mut().expect("checked above");
        let Some(piece) = pick else {
            view.current = None;
            return;
        };
        view.offered.insert(piece);
        view.current = Some((piece, self.picker.availability[piece as usize], Instant::now()));
        self.super_seed.offers[piece as usize] += 1;
        if peer.send_have(piece).is_err() {
            self.drop_peer(idx, "send failed");
        }
    }

    /// BEP 16: a peer gets its next piece once the one it was shown turns up at another peer,
    /// which means it passed it on. Alone in the swarm, or holding its piece for a while with
    /// no taker, it gets the next one anyway rather than waiting forever.
    pub(super) fn reveal_where_spread(&mut self) {
        if !self.super_seed.on || !self.peers.iter().any(|p| p.super_seed.is_some()) {
            return;
        }
        const PATIENCE: Duration = Duration::from_secs(120);
        let lone = self.peers.len() == 1;
        let ready: Vec<SocketAddr> = self
            .peers
            .iter()
            .filter(|p| {
                let Some((piece, others_then, shown)) = p.super_seed.as_ref().and_then(|v| v.current) else {
                    return false;
                };
                let theirs = p.they_have(piece);
                let others_now = self.picker.availability[piece as usize] - theirs as u32;
                others_now > others_then || (theirs && (lone || shown.elapsed() >= PATIENCE))
            })
            .map(|p| p.remote_addr)
            .collect();
        for addr in ready {
            if let Some(idx) = self.peer_index(addr) {
                self.reveal_next_piece(idx);
            }
        }
    }

    /// BEP 21: everything selected is in, but not everything there is.
    pub(super) fn partial_seed(&self) -> bool {
        self.stat.completed && !self.stat.all_verified()
    }
}

#[cfg(test)]
mod test {
    use super::super::test_support::*;
    use super::*;

    /// BEP 16: super-seeding hides our pieces, shows each newcomer a different one, shows the
    /// next only once the last has spread, serves only what was shown, and switching it off
    /// reveals the rest.
    #[tokio::test]
    async fn super_seeding_reveals_a_piece_at_a_time() {
        let (swarm, handle, path) = swarm_with("superseed", true);
        tokio::spawn(swarm.work_loop());
        handle.set_super_seed(true).await;

        async fn next_have(peer: &mut Wire) -> u32 {
            let have = async {
                loop {
                    match peer.next().await {
                        Some(Ok(BtMessage::Have(have))) => return have.checked,
                        Some(Ok(BtMessage::HaveAll(_) | BtMessage::BitField(_))) => panic!("revealed everything"),
                        Some(Ok(_)) => {}
                        other => panic!("connection ended: {other:?}"),
                    }
                }
            };
            tokio::time::timeout(Duration::from_secs(5), have)
                .await
                .expect("no Have")
        }
        let mut a = fake_peer_with(&handle, "10.0.0.1:6881", true).await;
        let Some(Ok(BtMessage::HaveNone(_))) = a.next().await else {
            panic!("a super-seed greets with HaveNone");
        };
        let first = next_have(&mut a).await;
        let mut b = fake_peer_with(&handle, "10.0.0.2:6881", true).await;
        let shown_b = next_have(&mut b).await;
        assert_ne!(first, shown_b, "each newcomer is shown a different piece");

        // b got a's piece from a: a passed it on, so a is shown another
        b.send(BtMessage::Have(crate::wire::Have { checked: first }))
            .await
            .unwrap();
        let second = next_have(&mut a).await;
        assert_ne!(second, first);

        a.send(BtMessage::Interested(crate::wire::Interested)).await.unwrap();
        let unchoked = async {
            loop {
                if let Some(Ok(BtMessage::Unchoke(_))) = a.next().await {
                    break;
                }
            }
        };
        tokio::time::timeout(CHOKING_ROUND_INTERVAL + Duration::from_secs(5), unchoked)
            .await
            .expect("never unchoked");
        let hidden = (0..3).find(|p| ![first, second].contains(p)).unwrap();
        let ask = |index| BlockRef {
            index,
            begin: 0,
            length: 16,
        };
        a.send(BtMessage::Request(ask(hidden))).await.unwrap();
        a.send(BtMessage::Request(ask(first))).await.unwrap();
        let (mut rejected, mut served) = (false, false);
        let answers = async {
            while !(rejected && served) {
                match a.next().await {
                    Some(Ok(BtMessage::RejectRequest(r))) => {
                        assert_eq!(r.index, hidden);
                        rejected = true;
                    }
                    Some(Ok(BtMessage::Piece(piece))) => {
                        assert_eq!(piece.index, first);
                        served = true;
                    }
                    Some(Ok(_)) => {}
                    other => panic!("connection ended: {other:?}"),
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), answers).await.unwrap();

        handle.set_super_seed(false).await;
        assert_eq!(next_have(&mut a).await, hidden, "switching off reveals the rest");
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
