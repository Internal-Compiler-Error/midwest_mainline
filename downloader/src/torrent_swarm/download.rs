//! Pieces coming in: each block into its piece, whole pieces off to be hashed and written, and
//! what a bad hash or a failed write means. Also BEP 52 piece layers fetched for a magnet.

use crate::events::Event;
use crate::layers::Received;
use crate::wire::{BtMessage, Hashes, Piece};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Instant;
use tracing::{info, warn};

use super::{
    SwarmEvent, TorrentSwarm,
    in_flight::{InFlight, Stored},
};

impl TorrentSwarm {
    /// BEP 52: asks peers for the piece layers we lack, each from a peer that has some of the
    /// file's pieces (and so must be able to answer) and speaks v2 (a hybrid's v1-only peers
    /// don't know hash requests).
    pub(super) fn request_layers(&mut self) {
        if self.layers.is_empty() {
            return;
        }
        let addrs: Vec<SocketAddr> = self.peers.iter().map(|p| p.remote_addr).collect();
        let (torrent, peers) = (&self.torrent, &self.peers);
        let has = |addr: SocketAddr, file: usize| {
            peers
                .binary_search_by_key(&addr, |p| p.remote_addr)
                .is_ok_and(|i| peers[i].v2 && torrent.pieces_of_file(file).any(|piece| peers[i].they_have(piece)))
        };
        let requests = self.layers.assign(torrent, &addrs, has, Instant::now());
        for (addr, req) in requests {
            if let Some(idx) = self.peer_index(addr)
                && self.peers[idx].send(BtMessage::HashRequest(req)).is_err()
            {
                self.drop_peer(idx, "send failed");
            }
        }
    }

    /// BEP 52: piece hashes we asked for; a layer that's complete makes its file's pieces
    /// checkable, so they can be picked.
    pub(super) fn hashes_arrived(&mut self, idx: usize, hashes: Hashes) {
        let addr = self.peers[idx].remote_addr;
        match self.layers.received(&self.torrent, addr, &hashes) {
            Received::Partial => {}
            Received::Layer(file) => {
                info!("piece layer of {:?} in from {addr}", self.torrent.files[file].path);
                self.schedule();
            }
            Received::Bad => {
                warn!("{addr} sent piece hashes that don't add up, disconnecting");
                self.drop_peer(idx, "bad hashes");
                self.ban(addr);
            }
        }
    }

    pub(super) fn block_arrived(&mut self, idx: usize, block: Piece) {
        let peer = &mut self.peers[idx];
        let from = peer.remote_addr;
        if peer.block_received(&block).is_none() {
            tracing::debug!("{from} sent a block we weren't waiting for, ignoring");
            self.wasted(from, block.len(), "unexpected");
            return;
        }
        self.stat.downloaded += block.len() as u64;
        self.store_block(from, block);
    }

    /// Bytes that came in for nothing.
    pub(super) fn wasted(&mut self, from: SocketAddr, len: u32, why: &'static str) {
        self.stat.wasted += len as u64;
        self.shared.events.emit(Event::BlockWasted {
            info_hash: self.torrent.info_hash,
            addr: from,
            len,
            why,
        });
    }

    /// A block someone (a peer or a web seed) owed us is in: it goes into its piece, and a
    /// piece with every block in goes off to be checked.
    pub(super) fn store_block(&mut self, from: SocketAddr, block: Piece) {
        let piece = block.index;
        let Some(in_flight) = self.in_flight.get_mut(piece) else {
            return;
        };
        let raced = in_flight.racers() > 1;
        match in_flight.store(from, &block) {
            Stored::Padding => {}
            Stored::Malformed => {
                warn!("{from} sent a malformed block for piece {piece}, giving up on it there");
                self.release_claim(piece, from);
            }
            Stored::Duplicate => {
                self.wasted(from, block.len(), "lost race");
                if let Some(idx) = self.peer_index(from) {
                    self.refill(idx);
                }
            }
            Stored::Added { blocks_left: 0 } => {
                let in_flight = self.in_flight.finish(piece).expect("stored into it");
                self.cancel_losers(piece, &in_flight);
                self.piece_assembled(piece, in_flight);
            }
            Stored::Added { .. } => {
                if raced {
                    self.cancel_duplicates(&block, from);
                }
                if let Some(idx) = self.peer_index(from) {
                    self.schedule_peer(idx);
                }
            }
        }
    }

    /// Hashes the piece and writes it if it's good, on the blocking pool: neither belongs on
    /// this loop, where a slow disk would hold up every peer. The buffer is hashed before it's
    /// written, so nothing is read back. `piece_done` takes it from there.
    fn piece_assembled(&mut self, piece: u32, in_flight: InFlight) {
        self.hashing.insert(piece);
        let senders = in_flight.senders();
        let torrent = self.torrent.clone();
        let storage = self.storage.clone();
        let events = self.events_tx.clone();
        tokio::task::spawn_blocking(move || {
            let buf = in_flight.buf;
            let outcome = tracing::info_span!(parent: &in_flight.span, "piece.check", piece).in_scope(|| {
                if torrent.valid_piece(piece, &buf) {
                    storage
                        .write_piece(piece, &buf)
                        .map(|()| true)
                        .map_err(|e| format!("{e:#}"))
                } else {
                    Ok(false)
                }
            });
            in_flight.span.record(
                "outcome",
                match &outcome {
                    Ok(true) => "verified".to_string(),
                    Ok(false) => "hash mismatch".to_string(),
                    Err(e) => format!("write failed: {e}"),
                },
            );
            if let Some(events) = events.upgrade() {
                let _ = events.blocking_send(SwarmEvent::PieceDone {
                    piece,
                    senders,
                    outcome,
                });
            }
        });
    }

    pub(super) fn piece_done(&mut self, piece: u32, senders: BTreeSet<SocketAddr>, outcome: Result<bool, String>) {
        self.hashing.remove(&piece);
        match outcome {
            Ok(true) => self.piece_verified(piece, senders),
            Ok(false) => self.piece_failed(piece, senders),
            Err(e) => self.storage_failed(piece, e),
        }
    }

    fn piece_verified(&mut self, piece: u32, senders: BTreeSet<SocketAddr>) {
        let size = self.torrent.nth_piece_size(piece).expect("piece index in range");
        self.stat.verified.set(piece as usize, true);
        self.shared.events.emit(Event::PieceVerified {
            info_hash: self.torrent.info_hash,
            piece,
            len: size,
            peers: senders.into_iter().collect(),
        });
        info!("piece {piece} is completed");
        self.stat.written += size;
        let was_complete = self.stat.completed;
        // what `refresh` would work out, without recounting the whole bitfield per piece
        if self.stat.wanted[piece as usize] {
            self.stat.left -= size;
        }
        self.stat.completed = self.stat.left == 0;
        if self.stat.completed && !was_complete {
            info!("download complete, {} bytes were received twice", self.stat.wasted);
            if self.partial_seed() {
                // BEP 21: done with what's selected, but not a seed: tell peers we won't ask
                let (size, private, port) = (
                    self.torrent.metadata_size(),
                    self.torrent.private,
                    self.shared.id.serving.port(),
                );
                self.broadcast(|peer| peer.send_extended_handshake(size, private, true, port));
            }
        }
        // announcers watch this to send a prompt event=completed rather than waiting for
        // their next periodic announce, which could be minutes away
        self.publish_stats();

        self.broadcast(|peer| peer.send_have(piece));
        self.schedule();
    }

    /// A piece is downloaded whole from one sender unless it was raced, so a bad one usually
    /// convicts its sender; a raced one can't tell who lied, and is just fetched again.
    fn piece_failed(&mut self, piece: u32, senders: BTreeSet<SocketAddr>) {
        self.shared.events.emit(Event::PieceFailed {
            info_hash: self.torrent.info_hash,
            piece,
            peers: senders.iter().copied().collect(),
        });
        let lone = senders.first().filter(|_| senders.len() == 1).copied();
        match lone.map(|from| (from, self.web_seed_index(from))) {
            Some((_, Some(seed))) => {
                warn!(
                    "piece {piece} from web seed {} failed hash verification",
                    self.web_seeds[seed].url
                );
                self.give_up_web_seed(seed, "sent a bad piece".to_string());
            }
            Some((from, None)) => {
                warn!("piece {piece} from {from} failed hash verification, banning the peer");
                if let Some(idx) = self.peer_index(from) {
                    self.drop_peer(idx, "sent a bad piece");
                }
                self.ban(from);
            }
            None => warn!("piece {piece} failed hash verification, and came from {senders:?}; will retry"),
        }
        self.put_back(piece);
        self.schedule();
    }

    /// Retrying would download the same piece forever into a disk that can't take it, so the
    /// torrent stops here and says why; the resume data keeps what was verified so far.
    fn storage_failed(&mut self, piece: u32, e: String) {
        warn!("couldn't write piece {piece}: {e}; stopping the download");
        self.put_back(piece);
        if self.stat.storage_error.is_none() {
            self.stat.storage_error = Some(e);
            for (piece, f) in self.in_flight.take_all() {
                for addr in f.claimants() {
                    if let Some(idx) = self.peer_index(addr) {
                        self.peers[idx].forget_piece(piece);
                    }
                }
                self.put_back(piece);
            }
        }
        self.publish_stats();
    }

    /// A block of a raced piece just arrived from `from`: any other racer that asked for the
    /// same block is told not to bother.
    fn cancel_duplicates(&mut self, block: &Piece, from: SocketAddr) {
        let req = block.block();
        let Some(in_flight) = self.in_flight.get(block.index) else {
            return;
        };
        let racers: Vec<SocketAddr> = in_flight.claimants().filter(|&addr| addr != from).collect();
        for addr in racers {
            let Some(idx) = self.peer_index(addr) else {
                continue;
            };
            let peer = &mut self.peers[idx];
            if peer.requested.remove(&req).is_some() && peer.send_cancel(req).is_err() {
                self.drop_peer(idx, "send failed");
            }
        }
    }

    /// A raced piece just completed: every other claimant is told to stop sending the blocks
    /// it still owes. A peer whose socket fails here is dropped.
    fn cancel_losers(&mut self, piece: u32, in_flight: &InFlight) {
        for addr in in_flight.claimants() {
            let Some(idx) = self.peer_index(addr) else {
                continue;
            };
            let peer = &mut self.peers[idx];
            for req in peer.forget_piece(piece) {
                if peer.send_cancel(req).is_err() {
                    self.drop_peer(idx, "send failed");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::test_support::*;
    use super::*;

    #[tokio::test]
    async fn downloads_pieces_from_a_peer_and_announces_them() {
        let (swarm, handle, path) = swarm("happy");
        let mut stats = handle.stats();
        tokio::spawn(swarm.work_loop());

        let mut seeder = fake_peer(&handle, "10.0.0.1:6881").await;
        open_as_seeder(&mut seeder).await;

        // serve every request, and note every Have the swarm sends back
        let mut haves = Vec::new();
        let serving = async {
            while haves.len() < 3 {
                match seeder.next().await {
                    Some(Ok(BtMessage::Request(req))) => seeder.send(block(req)).await.unwrap(),
                    Some(Ok(BtMessage::Have(have))) => haves.push(have.checked),
                    Some(Ok(other)) => panic!("unexpected {other:?}"),
                    other => panic!("peer socket ended: {other:?}"),
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(10), serving).await.unwrap();
        haves.sort_unstable();
        assert_eq!(
            haves,
            vec![0, 1, 2],
            "BEP 3: every verified piece is announced with Have"
        );

        wait_until_complete(&mut stats).await;
        let stats = stats.borrow().clone();
        assert_eq!(stats.verified_cnt(), 3);
        assert_eq!((stats.left, stats.written), (0, TOTAL));
        assert_eq!(stats.downloaded, TOTAL as u64);
        assert_eq!(std::fs::read(&path).unwrap(), content());
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// A piece is downloaded whole from one peer, so a piece that fails its hash convicts its
    /// sender: it's dropped, and refused when it comes back.
    #[tokio::test]
    async fn a_peer_that_sends_a_bad_piece_is_banned() {
        let (swarm, handle, path) = swarm("banned");
        tokio::spawn(swarm.work_loop());

        let mut liar = fake_peer(&handle, "10.0.0.9:6881").await;
        open_as_seeder(&mut liar).await;
        let cut_off = async {
            loop {
                match liar.next().await {
                    Some(Ok(BtMessage::Request(req))) => {
                        let garbage = Piece {
                            index: req.index,
                            begin: req.begin,
                            data: vec![0u8; req.length as usize].into(),
                        };
                        if liar.send(BtMessage::Piece(garbage)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(other)) => panic!("unexpected {other:?}"),
                    Some(Err(_)) | None => break,
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), cut_off)
            .await
            .expect("the swarm should have hung up on the liar");

        let mut again = fake_peer(&handle, "10.0.0.9:6881").await;
        let refused = tokio::time::timeout(Duration::from_secs(5), again.next()).await;
        assert!(
            matches!(refused, Ok(None) | Ok(Some(Err(_)))),
            "the banned peer should be refused without an opening exchange, got {refused:?}"
        );
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// A peer that vanishes mid-piece must not keep that piece's slot: another peer has to be
    /// asked for it.
    #[tokio::test]
    async fn pieces_abandoned_by_a_dead_peer_are_requested_from_another() {
        let (swarm, handle, path) = swarm("dead");
        let mut stats = handle.stats();
        tokio::spawn(swarm.work_loop());

        let mut flaky = fake_peer(&handle, "10.0.0.1:6881").await;
        open_as_seeder(&mut flaky).await;
        let Some(Ok(BtMessage::Request(_))) = flaky.next().await else {
            panic!("expected a request");
        };
        drop(flaky);

        let mut steady = fake_peer(&handle, "10.0.0.2:6881").await;
        open_as_seeder(&mut steady).await;
        let mut asked = BTreeSet::new();
        let serving = async {
            loop {
                match steady.next().await {
                    Some(Ok(BtMessage::Request(req))) => {
                        asked.insert(req.index);
                        steady.send(block(req)).await.unwrap();
                    }
                    Some(Ok(BtMessage::Have(_))) => {
                        if stats.borrow().completed {
                            break;
                        }
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(10), serving).await.unwrap();
        assert_eq!(
            asked,
            BTreeSet::from([0, 1, 2]),
            "every piece the dead peer held must be re-requested"
        );

        wait_until_complete(&mut stats).await;
        assert_eq!(std::fs::read(&path).unwrap(), content());
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
