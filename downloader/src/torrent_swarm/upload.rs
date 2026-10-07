//! Serving peers: block requests read off disk and sent as the upload limit allows, the
//! choking algorithm, ut_metadata pieces and BEP 52 hash requests.

use crate::events::Event;
use crate::layers::{self, MAX_HASH_READS};
use crate::peer::parse_ut_metadata_request;
use crate::settings::{
    MAX_QUEUED_UPLOADS, MAX_SERVED_BLOCK, MAX_UNCHOKED_PEERS, METADATA_PIECE_SIZE, OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS,
};
use crate::wire::{BtMessage, HashRequest, Piece, Request};
use rand::seq::IndexedRandom;
use std::net::SocketAddr;
use tracing::warn;

use super::{SwarmEvent, TorrentSwarm};

/// Regular (non-optimistic) upload slots for `interested` peers. A handful of slots reciprocates
/// with only a handful of a big swarm's leechers, and the rest have no reason to send us
/// anything, so the count grows with the square root of the demand.
pub(super) fn upload_slots(interested: usize) -> usize {
    MAX_UNCHOKED_PEERS.max(interested.isqrt() + 1)
}

impl TorrentSwarm {
    /// BEP 9: we always have the full metadata, so any piece of it in range is served.
    pub(super) fn serve_metadata(&mut self, idx: usize, payload: &[u8]) {
        let Some(piece) = parse_ut_metadata_request(payload) else {
            return;
        };
        let info = &self.torrent.raw_info;
        let start = piece as usize * METADATA_PIECE_SIZE;
        if start >= info.len() {
            return;
        }
        let data = &info[start..(start + METADATA_PIECE_SIZE).min(info.len())];
        let total_size = self.torrent.metadata_size();
        if self.peers[idx].send_metadata_piece(piece, total_size, data).is_err() {
            self.drop_peer(idx, "send failed");
        }
    }

    /// BEP 52: hashes at or above the piece layer come from the layers we have; below it,
    /// from the data of pieces we've verified, read on the blocking pool (`answer_from_data`).
    /// A hybrid whose halves disagree answers nothing: its v2 hashes aren't to be trusted.
    pub(super) fn answer_hash_request(&mut self, idx: usize, req: HashRequest) {
        let reply = match layers::pieces_for(&self.torrent, &req) {
            _ if !self.torrent.v2_consistent() => BtMessage::HashReject(req),
            Some(pieces) => {
                let had = pieces.clone().all(|p| self.stat.verified[p as usize]);
                if had && self.hash_reads < MAX_HASH_READS {
                    self.answer_from_data(self.peers[idx].remote_addr, req, pieces);
                    return;
                }
                BtMessage::HashReject(req)
            }
            None => match layers::answer(&self.torrent, &mut self.hash_trees, &req) {
                Some(hashes) => BtMessage::Hashes(hashes),
                None => BtMessage::HashReject(req),
            },
        };
        if self.peers[idx].send(reply).is_err() {
            self.drop_peer(idx, "send failed");
        }
    }

    /// Sends blocks the upload limit held back, as far as it allows now. A block for a peer
    /// that has since gone is dropped.
    pub(super) fn send_held_uploads(&mut self) {
        while let Some((to, block)) = self.held_uploads.pop_front() {
            let Some(idx) = self.peer_index(to) else {
                continue;
            };
            if let Some(block) = self.deliver(idx, block) {
                self.held_uploads.push_front((to, block));
                break;
            }
        }
    }

    /// Sends a block read for the peer at `idx`, if it still wants it (no Cancel came) and may
    /// have it (it isn't choked: then it's rejected). A block stays in the peer's `uploads`
    /// until it's sent, and is handed back if the upload limit says not yet.
    fn deliver(&mut self, idx: usize, block: Piece) -> Option<Piece> {
        let peer = &mut self.peers[idx];
        let request = Request::from(&block);
        if !peer.uploads.contains(&request) {
            return None;
        }
        if !peer.choked_them && !self.limiter.take_upload(block.length as usize) {
            return Some(block);
        }
        peer.uploads.remove(&request);
        let sent = if peer.choked_them {
            peer.send_reject(request)
        } else {
            self.stat.uploaded += block.length as u64;
            peer.send_block(block)
        };
        if sent.is_err() {
            self.drop_peer(idx, "send failed");
        }
        None
    }

    /// BEP 3: a choked peer isn't entitled to any data, full stop. BEP 6 turns "ignore it"
    /// into "must say so": once Fast Extension is negotiated a declined request needs an
    /// explicit RejectRequest, which `send_reject` no-ops on its own if it isn't.
    pub(super) fn serve_request(&mut self, idx: usize, request: Request) {
        let peer = &mut self.peers[idx];
        if peer.choked_them {
            if peer.send_reject(request).is_err() {
                self.drop_peer(idx, "send failed");
            }
            return;
        }

        // never serve a piece we haven't hash-verified, nor a block size or queue depth past
        // what a well-behaved peer asks for: each accepted request costs a disk read and its
        // block in memory until sent
        let verified = self.stat.verified.get(request.index as usize).is_some_and(|b| *b)
            && peer
                .super_seed
                .as_ref()
                .is_none_or(|view| view.offered.contains(&request.index));
        let sane = request.length > 0 && request.length <= MAX_SERVED_BLOCK;
        if !verified || !sane || peer.uploads.len() >= MAX_QUEUED_UPLOADS {
            if peer.send_reject(request).is_err() {
                self.drop_peer(idx, "send failed");
            }
            return;
        }

        if !peer.uploads.insert(request) {
            // asked twice; the first one is already on its way
            return;
        }
        // The disk read happens off this loop, so a slow disk doesn't hold up every other
        // peer; the block comes back as an event and is written to the socket then. Only
        // the requested bytes are read, not the whole piece.
        let storage = self.storage.clone();
        let events = self.events_tx.clone();
        let to = peer.remote_addr;
        tokio::task::spawn_blocking(move || {
            let block = storage
                .read_block(request.index, request.begin, request.length)
                .map(|data| Piece {
                    index: request.index,
                    begin: request.begin,
                    length: request.length,
                    data,
                })
                .map_err(|e| {
                    warn!("couldn't read {request:?} for {to}: {e:#}");
                    request
                });
            if let Some(events) = events.upgrade() {
                let _ = events.blocking_send(SwarmEvent::BlockRead { to, block });
            }
        });
    }

    /// Answers a hash request below the piece layer from `pieces`' data, read and hashed on the
    /// blocking pool; the answer (or a reject, if the data didn't hash right) comes back as
    /// `HashesRead`.
    pub(super) fn answer_from_data(&mut self, to: SocketAddr, req: HashRequest, pieces: std::ops::Range<u32>) {
        self.hash_reads += 1;
        let (torrent, storage, events) = (self.torrent.clone(), self.storage.clone(), self.events_tx.clone());
        tokio::task::spawn_blocking(move || {
            let answer = layers::answer_from_data(&torrent, &req, |piece| {
                debug_assert!(pieces.contains(&piece));
                storage.read_piece(piece).ok()
            });
            let reply = match answer {
                Some(hashes) => BtMessage::Hashes(hashes),
                None => {
                    warn!("couldn't answer {to}'s hash request from pieces {pieces:?} on disk");
                    BtMessage::HashReject(req)
                }
            };
            if let Some(events) = events.upgrade() {
                let _ = events.blocking_send(SwarmEvent::HashesRead { to, reply });
            }
        });
    }

    pub(super) fn hashes_read(&mut self, to: SocketAddr, reply: BtMessage) {
        self.hash_reads -= 1;
        if let Some(idx) = self.peer_index(to)
            && self.peers[idx].send(reply).is_err()
        {
            self.drop_peer(idx, "send failed");
        }
    }

    /// Second half of `serve_request`: the bytes are in hand (or the read failed, in which
    /// case the request is declined). The peer may have been choked or dropped meanwhile.
    pub(super) fn send_block(&mut self, to: SocketAddr, block: Result<Piece, Request>) {
        let Some(idx) = self.peer_index(to) else {
            return;
        };
        match block {
            Ok(block) => {
                if let Some(block) = self.deliver(idx, block) {
                    self.held_uploads.push_back((to, block));
                }
            }
            Err(request) => {
                let peer = &mut self.peers[idx];
                if peer.uploads.remove(&request) && peer.send_reject(request).is_err() {
                    self.drop_peer(idx, "send failed");
                }
            }
        }
    }

    /// A peer that just declared interest gets a free upload slot now rather than at the next
    /// choking round, up to 10 s away; the round still decides who keeps one.
    pub(super) fn unchoke_if_slot_free(&mut self, idx: usize) {
        let interested = self.peers.iter().filter(|p| p.interested_us).count();
        let unchoked = self.peers.iter().filter(|p| !p.choked_them).count();
        let peer = &mut self.peers[idx];
        if !peer.choked_them || unchoked >= upload_slots(interested) {
            return;
        }
        self.bus.emit(Event::ChokeChanged {
            info_hash: self.torrent.info_hash,
            addr: peer.remote_addr,
            choked: false,
            by_us: true,
        });
        if peer.unchoke().is_err() {
            self.drop_peer(idx, "send failed");
        }
    }

    /// Tit-for-tat unchoking, run periodically. Ranks interested peers (those who want to
    /// download from us) by the download rate they've been giving us -- reciprocation is the
    /// point -- and unchokes the top `upload_slots`. Every OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS
    /// rounds, one additional peer is unchoked at random so a new or under-rated peer gets a
    /// chance to prove itself instead of the same top N being unchoked forever.
    pub(super) fn run_choking_algorithm(&mut self, round: u64) {
        let mut interested: Vec<(SocketAddr, f64)> = self
            .peers
            .iter()
            .filter(|p| p.interested_us)
            .map(|p| (p.remote_addr, p.stats.rx_rate))
            .collect();
        interested.sort_by(|a, b| b.1.total_cmp(&a.1));

        let slots = upload_slots(interested.len());
        let mut to_unchoke: Vec<SocketAddr> = interested.iter().take(slots).map(|p| p.0).collect();

        if round.is_multiple_of(OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS) {
            let candidates: Vec<_> = interested.iter().filter(|p| !to_unchoke.contains(&p.0)).collect();
            if let Some(pick) = candidates.choose(&mut rand::rng()) {
                to_unchoke.push(pick.0);
            }
        }

        for peer in &self.peers {
            let should_unchoke = to_unchoke.contains(&peer.remote_addr);
            if should_unchoke == peer.choked_them {
                self.bus.emit(Event::ChokeChanged {
                    info_hash: self.torrent.info_hash,
                    addr: peer.remote_addr,
                    choked: !should_unchoke,
                    by_us: true,
                });
            }
        }
        self.broadcast(|peer| {
            let should_unchoke = to_unchoke.contains(&peer.remote_addr);
            if should_unchoke && peer.choked_them {
                peer.unchoke()
            } else if !should_unchoke && !peer.choked_them {
                peer.choke()
            } else {
                Ok(())
            }
        });
    }
}

#[cfg(test)]
mod test {
    use super::super::test_support::*;

    /// BEP 3: a choked peer gets no data. Without Fast Extension there's nothing to send back
    /// either -- the request is simply dropped.
    #[tokio::test]
    async fn requests_from_a_choked_peer_are_not_served() {
        let (swarm, handle, path) = swarm("choked");
        tokio::spawn(swarm.work_loop());

        let mut leech = fake_peer(&handle, "10.0.0.3:6881").await;
        let Some(Ok(BtMessage::BitField(_))) = leech.next().await else {
            panic!("expected our bitfield first");
        };
        let Some(Ok(BtMessage::Interested(_))) = leech.next().await else {
            panic!("expected Interested");
        };
        leech
            .send(BtMessage::Request(Request {
                index: 0,
                begin: 0,
                length: 16,
            }))
            .await
            .unwrap();
        let answer = tokio::time::timeout(Duration::from_millis(500), leech.next()).await;
        assert!(answer.is_err(), "a choked peer must get nothing back, got {answer:?}");
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// A block the upload limit holds back is still the peer's to cancel: it isn't sent once
    /// the limit allows.
    #[tokio::test]
    async fn a_cancelled_block_held_by_the_upload_limit_is_not_sent() {
        let settings = crate::config::Settings {
            upload_limit: 16_000,
            ..Default::default()
        };
        let (swarm, handle, path) = swarm_with_settings("held", true, settings);
        tokio::spawn(swarm.work_loop());
        let mut leech = fake_peer_with(&handle, "10.0.0.4:6881", true).await;
        leech
            .send(BtMessage::Interested(crate::wire::Interested))
            .await
            .unwrap();
        let next_piece = async |leech: &mut Wire| loop {
            match leech.next().await {
                Some(Ok(BtMessage::Piece(piece))) => return piece,
                Some(Ok(_)) => {}
                other => panic!("connection ended: {other:?}"),
            }
        };
        loop {
            if let Some(Ok(BtMessage::Unchoke(_))) = leech.next().await {
                break;
            }
        }
        let (first, second) = (
            Request {
                index: 0,
                begin: 0,
                length: 16_000,
            },
            Request {
                index: 1,
                begin: 0,
                length: 16_000,
            },
        );
        leech.send(BtMessage::Request(first)).await.unwrap();
        leech.send(BtMessage::Request(second)).await.unwrap();
        // the reads run in parallel, so either block may go first and spend the other's
        // allowance; the other is read, and held
        let piece = tokio::time::timeout(Duration::from_secs(5), next_piece(&mut leech))
            .await
            .unwrap();
        let held = if piece.index == 0 { second } else { first };
        tokio::time::sleep(Duration::from_millis(200)).await;
        leech
            .send(BtMessage::Cancel(crate::wire::Cancel {
                index: held.index,
                begin: held.begin,
                length: held.length,
            }))
            .await
            .unwrap();
        let late = tokio::time::timeout(Duration::from_millis(2500), next_piece(&mut leech)).await;
        assert!(late.is_err(), "the cancelled block was sent");
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// The upload path: an interested peer gets unchoked by the next choking round, its block
    /// requests are answered with the right bytes, and (BEP 6) a request past the end of a
    /// piece is rejected rather than served or dropped.
    #[tokio::test]
    async fn serves_blocks_to_an_unchoked_peer() {
        let (swarm, handle, path) = swarm_with("seed", true);
        let mut stats = handle.stats();
        tokio::spawn(swarm.work_loop());

        let mut leech = fake_peer_with(&handle, "10.0.0.4:6881", true).await;
        let Some(Ok(BtMessage::HaveAll(_))) = leech.next().await else {
            panic!("a seeder greets a fast peer with HaveAll");
        };
        let Some(Ok(BtMessage::Interested(_))) = leech.next().await else {
            panic!("expected Interested");
        };
        leech
            .send(BtMessage::Interested(crate::wire::Interested))
            .await
            .unwrap();

        // the choking algorithm runs every CHOKING_ROUND_INTERVAL; we're the only candidate
        let unchoked = async {
            loop {
                if let Some(Ok(BtMessage::Unchoke(_))) = leech.next().await {
                    break;
                }
            }
        };
        tokio::time::timeout(CHOKING_ROUND_INTERVAL + Duration::from_secs(5), unchoked)
            .await
            .expect("never unchoked");

        let good = Request {
            index: 2,
            begin: 4_000,
            length: 16_000, // the last piece is 20_000 bytes
        };
        let past_the_end = Request {
            index: 2,
            begin: 4_001,
            length: 16_000,
        };
        leech.send(BtMessage::Request(good)).await.unwrap();
        leech.send(BtMessage::Request(past_the_end)).await.unwrap();

        let mut got_block = false;
        let mut got_reject = false;
        let answers = async {
            while !(got_block && got_reject) {
                match leech.next().await {
                    Some(Ok(BtMessage::Piece(piece))) => {
                        assert_eq!((piece.index, piece.begin, piece.length), (2, 4_000, 16_000));
                        assert_eq!(&*piece.data, &content()[2 * PIECE + 4_000..2 * PIECE + 20_000]);
                        got_block = true;
                    }
                    Some(Ok(BtMessage::RejectRequest(reject))) => {
                        assert_eq!((reject.index, reject.begin, reject.length), (2, 4_001, 16_000));
                        got_reject = true;
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), answers).await.unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            while stats.borrow_and_update().uploaded != 16_000 {
                stats.changed().await.unwrap();
            }
        })
        .await
        .expect("uploaded bytes never reached the stats");
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
