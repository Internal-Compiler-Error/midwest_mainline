//! Who is asked for which piece: UCB over the peers (exploring only a subsample of the unproven
//! ones), rarest first or sequential, and endgame racing.

use crate::events::Event;
use crate::peer::Peer;
use crate::settings::{BLOCK_SIZE, ENDGAME_LAST_PIECES, ENDGAME_LAST_RACERS, ENDGAME_RACERS, MAX_INFLIGHT_BYTES};
use rand::RngExt;
use rand::seq::IndexedRandom;
use std::net::SocketAddr;
use std::time::Instant;

use super::{TorrentSwarm, in_flight::InFlight};

/// Who a piece can be handed to.
#[derive(Clone, Copy)]
pub(super) enum Source {
    Peer(usize),
    Web(usize),
}

impl TorrentSwarm {
    /// Keeps up to `MAX_INFLIGHT_BYTES` of pieces on the wire. UCB orders the peers and each
    /// takes the rarest piece it has (see `assign_pieces`). A piece is assigned whole to one
    /// peer, but its blocks are only requested as that peer's window allows (see `refill`).
    ///
    /// Endgame: when nothing is left to assign, the pieces in flight would otherwise wait on
    /// whichever peer holds them, slow ones included. So peers with room also take the pieces
    /// furthest from done, up to ENDGAME_RACERS per piece, from the other end; each block
    /// that arrives is cancelled at the other racers (`cancel_duplicates`).
    pub(super) fn schedule(&mut self) {
        if self.stat.storage_error.is_some() {
            return;
        }
        self.admit_to_exploring();
        self.assign_pieces(None);
        if self.missing.is_empty() {
            self.race_the_last_pieces();
            self.race_web_seeds();
        }
        for idx in (0..self.peers.len()).rev() {
            self.refill(idx);
        }
    }

    /// `schedule` for one peer that just got room (a block arrived) or something new to offer
    /// (a Have): cheap enough to run per message, unlike a full pass.
    pub(super) fn schedule_peer(&mut self, idx: usize) {
        if self.stat.storage_error.is_some() {
            return;
        }
        // usually the pieces it already holds have blocks left to ask for, and that's all
        let addr = self.peers[idx].remote_addr;
        self.refill(idx);
        let Some(idx) = self.peer_index(addr) else {
            return;
        };
        let peer = &self.peers[idx];
        if peer.requested.len() < peer.request_window() {
            if self.missing.is_empty() {
                self.race_for_peer(idx);
            } else {
                self.assign_pieces(Some(idx));
            }
            self.refill(idx);
        }
    }

    fn eligible(&self, peer: &Peer) -> bool {
        peer.ready() && (peer.proven() || self.exploring.contains(&peer.remote_addr))
    }

    /// Whether the peer's window has room past what it's asked for and what its pieces still
    /// have queued for it.
    fn has_room(&self, peer: &Peer) -> bool {
        peer.requested.len() + self.in_flight.backlog(peer.remote_addr) < peer.request_window()
    }

    /// Hands out missing pieces to eligible peers with room in their window, best UCB score
    /// first, each taking the rarest piece it has (lowest, when sequential). Peers go round
    /// by round so the in-flight budget is shared out rather than taken by the first peer.
    /// `only` limits this to one peer.
    pub(super) fn assign_pieces(&mut self, only: Option<usize>) {
        let mut in_flight_bytes = self.in_flight.bytes();
        let rate_scale = self.rate_scale();
        let mut order: Vec<(Source, f64)> = self
            .peers
            .iter()
            .enumerate()
            .filter(|&(idx, p)| only.is_none_or(|o| o == idx) && self.eligible(p) && self.has_room(p))
            .map(|(idx, p)| (Source::Peer(idx), p.stats.score(self.total_picks, rate_scale)))
            .collect();
        if only.is_none() {
            let now = Instant::now();
            order.extend(
                self.web_seeds
                    .iter()
                    .enumerate()
                    .filter(|(_, w)| w.has_room(now))
                    .map(|(i, w)| (Source::Web(i), w.stats.score(self.total_picks, rate_scale))),
            );
        }
        order.sort_by(|a, b| b.1.total_cmp(&a.1));

        loop {
            let mut assigned = false;
            for &(source, _) in &order {
                if in_flight_bytes >= MAX_INFLIGHT_BYTES {
                    return;
                }
                let idx = match source {
                    Source::Peer(idx) => idx,
                    Source::Web(seed) => {
                        if let Some(bytes) = self.assign_web_run(seed, MAX_INFLIGHT_BYTES - in_flight_bytes) {
                            in_flight_bytes += bytes;
                            assigned = true;
                        }
                        continue;
                    }
                };
                if !self.has_room(&self.peers[idx]) {
                    continue;
                }
                let Some(pos) = self.pick_piece_for(idx) else {
                    continue;
                };
                let piece = self.missing.swap_remove(pos);
                let addr = self.peers[idx].remote_addr;
                self.emit_pick(idx, piece, rate_scale);
                let in_flight = InFlight::new(&self.torrent, piece, addr, addr);
                in_flight_bytes += in_flight.buf.len();
                self.in_flight.start(piece, in_flight);
                assigned = true;
                tracing::debug!(
                    "requesting piece {piece} from {addr} (window {})",
                    self.peers[idx].request_window()
                );
            }
            if !assigned {
                return;
            }
        }
    }

    /// Where in `missing` the piece for this peer is: the rarest one it has, ties broken at
    /// random so peers starting together spread out; the lowest one when sequential.
    pub(super) fn pick_piece_for(&self, idx: usize) -> Option<usize> {
        let peer = &self.peers[idx];
        let n = self.missing.len();
        if n == 0 {
            return None;
        }
        // the scan starts somewhere random and keeps the first of the best it meets, which
        // breaks ties at random with one draw rather than one per tied piece (early on, that's
        // nearly every piece)
        let offset = rand::rng().random_range(0..n);
        let mut best: Option<(usize, u32)> = None;
        for pos in (offset..n).chain(0..offset) {
            let piece = self.missing[pos];
            if !peer.they_have(piece) || !self.verifiable(piece) {
                continue;
            }
            let rank = if self.sequential {
                piece
            } else {
                self.availability[piece as usize]
            };
            if best.is_none_or(|(_, best_rank)| rank < best_rank) {
                best = Some((pos, rank));
            }
        }
        best.map(|(pos, _)| pos)
    }

    /// The fastest rate in the swarm, what UCB scales rates by (see `PeerStatistics::ucb_terms`).
    pub(super) fn rate_scale(&self) -> f64 {
        self.peers
            .iter()
            .map(|p| p.stats.rx_rate)
            .chain(self.web_seeds.iter().map(|w| w.stats.rx_rate))
            .fold(1.0, f64::max)
    }

    /// Seconds until `f` is done at the combined rate of everyone on it.
    pub(super) fn eta(&self, f: &InFlight) -> f64 {
        let rate: f64 = f
            .claimants()
            .filter_map(|addr| match self.peer_index(addr) {
                Some(idx) => Some(self.peers[idx].stats.rx_rate),
                None => self.web_seed_index(addr).map(|seed| self.web_seeds[seed].stats.rx_rate),
            })
            .sum();
        (f.blocks_left() * BLOCK_SIZE) as f64 / rate.max(1.0)
    }

    pub(super) fn racers_per_piece(&self) -> usize {
        if self.in_flight.len() <= ENDGAME_LAST_PIECES {
            ENDGAME_LAST_RACERS
        } else {
            ENDGAME_RACERS
        }
    }

    /// Endgame for every peer with room, furthest-from-done pieces first.
    pub(super) fn race_the_last_pieces(&mut self) {
        let rate_scale = self.rate_scale();
        let mut by_eta: Vec<(f64, u32)> = self.in_flight.iter().map(|(piece, f)| (self.eta(f), piece)).collect();
        by_eta.sort_by(|a, b| b.0.total_cmp(&a.0));
        for (_, piece) in by_eta {
            while self.in_flight.get(piece).expect("in flight").racers() < self.racers_per_piece() {
                let Some(idx) = self.best_peer(piece, rate_scale) else {
                    break;
                };
                let addr = self.peers[idx].remote_addr;
                self.emit_pick(idx, piece, rate_scale);
                self.in_flight.add_racer(piece, addr);
                tracing::debug!("endgame: also requesting piece {piece} from {addr}");
            }
        }
    }

    /// Endgame for one peer that has room: it joins the pieces furthest from done that it can
    /// help with until its window is full. Cheap enough to run per block, unlike the full pass.
    pub(super) fn race_for_peer(&mut self, idx: usize) {
        let peer = &self.peers[idx];
        if !self.eligible(peer) {
            return;
        }
        let addr = peer.remote_addr;
        let rate_scale = self.rate_scale();
        let racers = self.racers_per_piece();
        while self.has_room(&self.peers[idx]) {
            let peer = &self.peers[idx];
            let Some((_, piece)) = self
                .in_flight
                .iter()
                .filter(|&(p, f)| f.racers() < racers && !f.claimed_by(addr) && peer.they_have(p))
                .map(|(piece, f)| (self.eta(f), piece))
                .max_by(|a, b| a.0.total_cmp(&b.0))
            else {
                return;
            };
            self.emit_pick(idx, piece, rate_scale);
            self.in_flight.add_racer(piece, addr);
            tracing::debug!("endgame: also requesting piece {piece} from {addr}");
        }
    }

    /// Tops the peer's outstanding requests up to its window from the pieces assigned to it.
    /// The peer is dropped if a send fails, so callers must not hold an index past this.
    pub(super) fn refill(&mut self, idx: usize) {
        let peer = &mut self.peers[idx];
        if !peer.ready() {
            return;
        }
        let room = peer.request_window().saturating_sub(peer.requested.len());
        for _ in 0..room.min(self.in_flight.backlog(peer.remote_addr)) {
            // over the download limit for now; housekeeping's schedule() retries
            if !self.limiter.take_download(BLOCK_SIZE) {
                return;
            }
            let Some(req) = self.in_flight.next_request(peer.remote_addr) else {
                return;
            };
            self.total_picks += 1;
            if peer.request_block(req).is_err() {
                self.drop_peer(idx, "send failed");
                return;
            }
        }
    }

    /// Ends the trials that are over (the peer delivered, choked us, or left) and starts new
    /// ones in the free slots, picking at random among the unproven peers that are ready.
    pub(super) fn admit_to_exploring(&mut self) {
        let peers = &self.peers;
        self.exploring.retain(|addr| {
            peers
                .binary_search_by_key(addr, |p| p.remote_addr)
                .is_ok_and(|idx| peers[idx].ready() && !peers[idx].proven())
        });
        let free = self.explore_slots.saturating_sub(self.exploring.len());
        if free == 0 {
            return;
        }
        let candidates: Vec<SocketAddr> = self
            .peers
            .iter()
            .filter(|p| p.ready() && !p.proven() && !self.exploring.contains(&p.remote_addr))
            .map(|p| p.remote_addr)
            .collect();
        for addr in candidates.sample(&mut rand::rng(), free) {
            self.exploring.insert(*addr);
        }
    }

    /// The pick just made, with the two halves of the score that decided it.
    pub(super) fn emit_pick(&self, idx: usize, piece: u32, rate_scale: f64) {
        let peer = &self.peers[idx];
        let (exploit, explore) = if self.total_picks == 0 || peer.stats.picked_count == 0 {
            (0.0, None)
        } else {
            let (exploit, explore) = peer.stats.ucb_terms(self.total_picks, rate_scale);
            (exploit, Some(explore))
        };
        self.bus.emit(Event::PeerPicked {
            info_hash: self.torrent.info_hash,
            addr: peer.remote_addr,
            piece,
            exploit,
            explore,
            picked_count: peer.stats.picked_count,
            total_picks: self.total_picks,
        });
    }

    /// UCB peer selection for an endgame racer: of the eligible peers that have `piece`,
    /// aren't already on it, and have room in their request window for more work, the one
    /// with the highest upper confidence bound on its download speed.
    fn best_peer(&self, piece: u32, rate_scale: f64) -> Option<usize> {
        let already_on_it = |p: &Peer| self.in_flight.get(piece).is_some_and(|f| f.claimed_by(p.remote_addr));
        self.peers
            .iter()
            .enumerate()
            .filter(|(_, p)| self.eligible(p) && p.they_have(piece) && self.has_room(p) && !already_on_it(p))
            .map(|(idx, p)| (idx, p.stats.score(self.total_picks, rate_scale)))
            .max_by(|(_, l), (_, r)| l.total_cmp(r))
            .map(|(idx, _)| idx)
    }
}

#[cfg(test)]
mod test {
    use super::super::test_support::*;
    use super::*;

    /// A download limit of two blocks a second lets two requests out at once, then nothing
    /// until the next second's allowance.
    #[tokio::test]
    async fn the_download_limit_paces_requests() {
        let settings = crate::config::Settings {
            download_limit: 2 * BLOCK_SIZE as u64,
            ..crate::config::Settings::default()
        };
        let (swarm, handle, path) = swarm_with_settings("limit", false, settings);
        tokio::spawn(swarm.work_loop());

        let mut seeder = fake_peer(&handle, "10.0.0.1:6881").await;
        open_as_seeder(&mut seeder).await;
        for _ in 0..2 {
            let Some(Ok(BtMessage::Request(_))) = seeder.next().await else {
                panic!("expected a request");
            };
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(300), seeder.next())
                .await
                .is_err(),
            "the second's allowance is spent"
        );
        let Some(Ok(BtMessage::Request(_))) = tokio::time::timeout(Duration::from_secs(3), seeder.next())
            .await
            .expect("the next second brings more")
        else {
            panic!("expected a request");
        };
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// Requests are pipelined: a peer with no measured rate yet gets MIN_REQUEST_WINDOW
    /// requests and nothing more until it delivers, rather than every block of every
    /// assigned piece landing on it at once.
    #[tokio::test]
    async fn requests_are_paced_by_the_peers_window() {
        let (swarm, handle, path) = swarm("window");
        tokio::spawn(swarm.work_loop());

        let mut seeder = fake_peer(&handle, "10.0.0.1:6881").await;
        open_as_seeder(&mut seeder).await;

        let mut requests = Vec::new();
        for _ in 0..MIN_REQUEST_WINDOW {
            let Some(Ok(BtMessage::Request(req))) = seeder.next().await else {
                panic!("expected a request");
            };
            requests.push(req);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(300), seeder.next())
                .await
                .is_err(),
            "nothing more until a block is delivered"
        );

        // a delivery both frees a slot and gives the peer a measured rate, which over
        // localhost is enormous, so the window opens up from here
        seeder.send(block(requests[0])).await.unwrap();
        let Some(Ok(BtMessage::Request(_))) = seeder.next().await else {
            panic!("a delivery makes room for more requests");
        };
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// Sequential mode asks for piece 0's blocks first, then piece 1's; rarest first would
    /// start anywhere.
    #[tokio::test]
    async fn sequential_asks_for_pieces_in_order() {
        let (swarm, handle, path) = swarm("sequential");
        tokio::spawn(swarm.work_loop());
        handle.set_sequential(true).await;

        let mut seeder = fake_peer(&handle, "10.0.0.1:6881").await;
        open_as_seeder(&mut seeder).await;
        let mut pieces = vec![];
        while pieces.len() < 4 {
            let Some(Ok(BtMessage::Request(req))) = seeder.next().await else {
                panic!("expected a request");
            };
            pieces.push(req.index);
        }
        assert_eq!(pieces, [0, 0, 0, 1]);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// With 3 pieces the subsample holds 2 peers. A third ready peer isn't asked for anything,
    /// even when it's the only one UCB hasn't tried, until a member disconnects.
    #[tokio::test]
    async fn only_the_subsample_is_asked_for_pieces() {
        let (swarm, handle, path) = swarm("subsample");
        assert_eq!(swarm.explore_slots, 2);
        tokio::spawn(swarm.work_loop());

        let mut a = fake_peer(&handle, "10.0.0.1:6881").await;
        open_as_seeder(&mut a).await;
        let Some(Ok(BtMessage::Request(first))) = a.next().await else {
            panic!("expected a request");
        };
        let mut b = fake_peer(&handle, "10.0.0.2:6881").await;
        open_as_seeder(&mut b).await;
        let Some(Ok(BtMessage::Request(_))) = b.next().await else {
            panic!("expected a request");
        };
        let mut c = fake_peer(&handle, "10.0.0.3:6881").await;
        open_as_seeder(&mut c).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        // a hands a piece back; without subsampling the untried c would get it
        a.send(BtMessage::RejectRequest(crate::wire::RejectRequest {
            index: first.index,
            begin: first.begin,
            length: first.length,
        }))
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(300), c.next())
                .await
                .is_err(),
            "a peer outside the subsample is never asked"
        );

        drop(a);
        let asked_c = async {
            loop {
                match c.next().await {
                    Some(Ok(BtMessage::Request(_))) => break,
                    Some(Ok(_)) => {}
                    other => panic!("c's socket ended: {other:?}"),
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), asked_c)
            .await
            .expect("a member leaving frees its slot for the next peer");
        drop(b);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// Endgame: once nothing is left to assign, the fast peer is also given the pieces the
    /// slow one is sitting on, walking each from the end, and the slow peer gets Cancel for
    /// what it still owed once the fast one finishes.
    #[tokio::test]
    async fn the_last_pieces_are_raced_and_the_loser_is_cancelled() {
        let (swarm, handle, path) = swarm("endgame");
        let mut stats = handle.stats();
        tokio::spawn(swarm.work_loop());

        // slow takes two pieces and never delivers a block
        let mut slow = fake_peer(&handle, "10.0.0.1:6881").await;
        open_as_seeder(&mut slow).await;
        for _ in 0..MIN_REQUEST_WINDOW {
            let Some(Ok(BtMessage::Request(_))) = slow.next().await else {
                panic!("expected a request");
            };
        }

        let mut fast = fake_peer(&handle, "10.0.0.2:6881").await;
        open_as_seeder(&mut fast).await;
        let mut begins_by_piece: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        let serving = async {
            loop {
                match fast.next().await {
                    Some(Ok(BtMessage::Request(req))) => {
                        begins_by_piece.entry(req.index).or_default().push(req.begin);
                        fast.send(block(req)).await.unwrap();
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
        wait_until_complete(&mut stats).await;

        assert_eq!(begins_by_piece.len(), 3, "the fast peer ends up delivering every piece");
        let (own, raced): (Vec<_>, Vec<_>) = begins_by_piece
            .values()
            .partition(|begins| begins.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(
            own.len(),
            1,
            "one piece was the fast peer's own, requested front to back"
        );
        assert_eq!(raced.len(), 2, "the two raced pieces were requested back to front");
        for begins in raced {
            assert!(begins.windows(2).all(|w| w[0] > w[1]), "{begins:?}");
        }

        let mut cancelled = BTreeSet::new();
        let drain = async {
            while let Some(Ok(msg)) = slow.next().await {
                if let BtMessage::Cancel(c) = msg {
                    cancelled.insert(c.index);
                }
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(300), drain).await;
        // both of the slow peer's pieces were taken from it; the fast peer's own piece may
        // have been raced with the slow peer too, so there can be a third
        assert!(
            cancelled.len() >= 2,
            "the slow peer was told to stop on both raced pieces: {cancelled:?}"
        );
        assert_eq!(stats.borrow().wasted, 0, "the slow peer never sent anything to waste");
        assert_eq!(std::fs::read(&path).unwrap(), content());
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// A peer whose queue is full rejects a request now and then: the block is asked for
    /// again, from the same peer, and what the piece already has is kept.
    #[tokio::test]
    async fn a_rejected_block_is_asked_for_again() {
        let (swarm, handle, path) = swarm("reject-once");
        tokio::spawn(swarm.work_loop());
        let mut a = fake_peer(&handle, "10.0.0.6:6881").await;
        open_as_seeder(&mut a).await;
        let Some(Ok(BtMessage::Request(first))) = a.next().await else {
            panic!("expected a request");
        };
        a.send(BtMessage::RejectRequest(crate::wire::RejectRequest {
            index: first.index,
            begin: first.begin,
            length: first.length,
        }))
        .await
        .unwrap();
        // it serves everything else, which drains its queue
        let again = async {
            loop {
                match a.next().await {
                    Some(Ok(BtMessage::Request(req))) if req == first => break,
                    Some(Ok(BtMessage::Request(req))) => a.send(block(req)).await.unwrap(),
                    Some(Ok(_)) => {}
                    other => panic!("a's socket ended: {other:?}"),
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), again)
            .await
            .expect("the rejected block was never asked for again");
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// A piece one peer rejects must be re-requested from the *other* ready peer. This used to
    /// fail: after its first request the picked peer's UCB score was NaN, which sorts above the
    /// fresh peer's infinity, so it was picked again and again.
    #[tokio::test]
    async fn a_rejected_piece_goes_to_another_ready_peer() {
        let (swarm, handle, path) = swarm("reject");
        tokio::spawn(swarm.work_loop());

        // a is ready first, so it gets the work; b only has the piece a is about to reject,
        // and is ready before a rejects it
        let mut a = fake_peer(&handle, "10.0.0.6:6881").await;
        open_as_seeder(&mut a).await;
        let Some(Ok(BtMessage::Request(rejected))) = a.next().await else {
            panic!("expected a request");
        };
        let mut b = fake_peer(&handle, "10.0.0.7:6881").await;
        open_with(&mut b, 0x80 >> rejected.index).await;
        // b's bitfield and unchoke travel on a different socket than a's reject below; give
        // the swarm a moment to have seen them, or a is the only ready peer when it reschedules
        tokio::time::sleep(Duration::from_millis(300)).await;

        // a rejects every request for that piece, as one that won't serve it would; one
        // reject is taken for a full queue and retried, a few mean the piece goes elsewhere
        let reject = |req: Request| {
            BtMessage::RejectRequest(crate::wire::RejectRequest {
                index: req.index,
                begin: req.begin,
                length: req.length,
            })
        };
        a.send(reject(rejected)).await.unwrap();
        tokio::spawn(async move {
            while let Some(Ok(msg)) = a.next().await {
                if let BtMessage::Request(req) = msg
                    && req.index == rejected.index
                {
                    let _ = a.send(reject(req)).await;
                }
            }
        });

        let b_gets_it = async {
            loop {
                match b.next().await {
                    Some(Ok(BtMessage::Request(req))) if req.index == rejected.index => break,
                    Some(Ok(_)) => {}
                    other => panic!("b's socket ended: {other:?}"),
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), b_gets_it)
            .await
            .expect("the rejected piece was never offered to the other peer");
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
