//! BEP 19 web seeds: runs of pieces scheduled next to the peers, fetched by jobs on tasks of
//! their own, and their part in the endgame.

use crate::peer::PeerSnapshot;
use crate::settings::BLOCK_SIZE;
use crate::webseed::{self, Failure, WebJob};
use crate::wire::Piece;
use std::net::SocketAddr;
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{SwarmEvent, TorrentSwarm, in_flight::InFlight};

impl TorrentSwarm {
    /// The web seeds still in use, as the peer list shows them.
    pub(super) fn web_seed_snapshots(&self) -> impl Iterator<Item = PeerSnapshot> + '_ {
        let piece_size = self.torrent.piece_size;
        self.web_seeds
            .iter()
            .filter(|w| w.gave_up.is_none())
            .map(move |w| PeerSnapshot {
                addr: w.addr,
                client: "web seed".to_string(),
                progress: 1.0,
                downloaded: w.stats.received as u64,
                uploaded: 0,
                download_bps: w.stats.rx_rate,
                choked_us: false,
                choked_them: true,
                interested_us: false,
                interested_them: true,
                outstanding: w.outstanding_blocks(piece_size),
                encrypted: w.url.starts_with("https:"),
                utp: false,
                web_seed: Some(w.url.clone()),
            })
    }

    pub(super) fn web_seed_index(&self, addr: SocketAddr) -> Option<usize> {
        self.web_seeds.iter().position(|w| w.addr == addr)
    }

    /// Where in `missing` a web seed's next run starts: the rarest piece, ties going to the
    /// lowest so consecutive jobs read the files front to back; the lowest when sequential.
    pub(super) fn pick_piece_for_web(&self) -> Option<usize> {
        let rank = |piece: u32| {
            if self.sequential {
                (0, piece)
            } else {
                (self.availability[piece as usize], piece)
            }
        };
        (0..self.missing.len())
            .filter(|&pos| self.verifiable(self.missing[pos]))
            .min_by_key(|&pos| rank(self.missing[pos]))
    }

    /// Gives web seed `seed` a run of consecutive missing pieces, as long as its rate earns
    /// (see `WebSeed::run_bytes`) within `budget` bytes, and starts fetching it. Returns the
    /// bytes it took on.
    pub(super) fn assign_web_run(&mut self, seed: usize, budget: usize) -> Option<usize> {
        if !self.web_seeds[seed].has_room(Instant::now()) {
            return None;
        }
        let first_pos = self.pick_piece_for_web()?;
        let first = self.missing[first_pos];
        let piece_size = self.torrent.piece_size as usize;
        let max_pieces = (self.web_seeds[seed].run_bytes().min(budget) / piece_size).max(1);
        // where each of the next pieces sits in `missing`, if it's there
        let mut next = vec![None; max_pieces - 1];
        for (pos, &piece) in self.missing.iter().enumerate() {
            if piece > first && ((piece - first) as usize) < max_pieces && self.verifiable(piece) {
                next[(piece - first - 1) as usize] = Some(pos);
            }
        }
        let limit = match self.settings.borrow().download_limit {
            0 => usize::MAX,
            limit => limit as usize,
        };
        let mut positions = vec![];
        let mut bytes = 0;
        for (i, pos) in std::iter::once(Some(first_pos)).chain(next).enumerate() {
            let Some(pos) = pos else {
                break;
            };
            let size = self
                .torrent
                .nth_piece_size(first + i as u32)
                .expect("piece index in range");
            if !self.limiter.take_download(size.min(limit)) {
                break;
            }
            positions.push(pos);
            bytes += size;
        }
        if positions.is_empty() {
            return None;
        }
        let last = first + positions.len() as u32 - 1;
        // highest first, so each swap_remove moves in an element that isn't one of ours
        positions.sort_unstable_by(|a, b| b.cmp(a));
        for pos in positions {
            self.missing.swap_remove(pos);
        }

        let addr = self.web_seeds[seed].addr;
        for piece in first..=last {
            let in_flight = InFlight::new(&self.torrent, piece, addr, &self.web_seeds[seed].host);
            self.in_flight.start(piece, in_flight);
            self.in_flight.claim_rest(piece, addr);
        }
        let start = first as u64 * piece_size as u64;
        self.start_web_job(seed, start, start + bytes as u64);
        tracing::debug!(
            "requesting pieces {first}..={last} from web seed {}",
            self.web_seeds[seed].url
        );
        Some(bytes)
    }

    /// Endgame for the web seeds: each with room joins the piece furthest from done that it
    /// isn't on yet, fetching just the blocks not in.
    pub(super) fn race_web_seeds(&mut self) {
        let now = Instant::now();
        let racers = self.racers_per_piece();
        let rate_scale = self.rate_scale();
        let mut seeds: Vec<(usize, f64)> = self
            .web_seeds
            .iter()
            .enumerate()
            .map(|(i, w)| (i, w.stats.score(self.total_picks, rate_scale)))
            .collect();
        seeds.sort_by(|a, b| b.1.total_cmp(&a.1));
        for (seed, _) in seeds {
            while self.web_seeds[seed].has_room(now) {
                let addr = self.web_seeds[seed].addr;
                let Some((_, piece)) = self
                    .in_flight
                    .iter()
                    .filter(|(_, f)| f.racers() < racers && !f.claimed_by(addr))
                    .map(|(piece, f)| (self.eta(f), piece))
                    .max_by(|a, b| a.0.total_cmp(&b.0))
                else {
                    break;
                };
                let offset = piece as u64 * self.torrent.piece_size as u64;
                let in_flight = self.in_flight.get(piece).expect("just looked up");
                let Some((start, end)) = in_flight.missing_span(offset) else {
                    break;
                };
                tracing::debug!(parent: &in_flight.span, peer = %self.web_seeds[seed].host, "racer joined");
                self.in_flight.claim_rest(piece, addr);
                self.start_web_job(seed, start, end);
                tracing::debug!(
                    "endgame: also fetching piece {piece} from web seed {}",
                    self.web_seeds[seed].url
                );
            }
        }
    }

    /// Fetches torrent bytes `start..end` from web seed `seed` on a task of its own, whose
    /// blocks and ending come back as events.
    pub(super) fn start_web_job(&mut self, seed: usize, start: u64, end: u64) {
        let piece_size = self.torrent.piece_size as u64;
        let pieces = (start / piece_size) as u32..=((end - 1) / piece_size) as u32;
        let blocks = (end - start).div_ceil(BLOCK_SIZE as u64) as usize;
        self.total_picks += blocks;
        let id = self.next_web_job;
        self.next_web_job += 1;
        let w = &mut self.web_seeds[seed];
        if w.jobs.is_empty() {
            w.stats.requests_started(Instant::now());
        }
        w.stats.picked_count += blocks;
        let cancel = CancellationToken::new();
        w.jobs.insert(
            id,
            WebJob {
                pieces,
                _cancel: cancel.clone().drop_guard(),
            },
        );
        let job = webseed::Job {
            torrent: self.torrent.clone(),
            base: w.url.clone(),
            host: w.host.clone(),
            start,
            end,
            redirects: w.redirects.clone(),
        };
        let events = self.events_tx.clone();
        tokio::spawn(async move {
            let blocks = events.clone();
            let run = job.run(move |block| {
                let events = blocks.upgrade();
                async move {
                    let Some(events) = events else {
                        return false;
                    };
                    let event = SwarmEvent::WebSeedBlock { seed, job: id, block };
                    events.send(event).await.is_ok()
                }
            });
            let outcome = tokio::select! {
                outcome = run => outcome,
                () = cancel.cancelled() => return,
            };
            if let Some(events) = events.upgrade() {
                let _ = events.send(SwarmEvent::WebSeedDone { seed, job: id, outcome }).await;
            }
        });
    }

    pub(super) fn web_block_arrived(&mut self, seed: usize, job: u64, block: Piece) {
        let w = &mut self.web_seeds[seed];
        w.stats.block_received(block.length as usize, Instant::now());
        self.stat.downloaded += block.length as u64;
        let addr = w.addr;
        if self.in_flight.holds(addr, block.index) {
            self.store_block(addr, block);
            return;
        }
        // finished by a racer, or released after a hash failure
        self.wasted(addr, block.length, "lost race");
        let rest_unwanted = self.web_seeds[seed]
            .jobs
            .get(&job)
            .is_some_and(|j| (block.index..=*j.pieces.end()).all(|p| !self.in_flight.holds(addr, p)));
        if rest_unwanted {
            self.web_seeds[seed].jobs.remove(&job);
            self.schedule();
        }
    }

    pub(super) fn web_job_done(&mut self, seed: usize, job: u64, outcome: Result<(), Failure>) {
        let Some(job) = self.web_seeds[seed].jobs.remove(&job) else {
            return;
        };
        let w = &mut self.web_seeds[seed];
        let addr = w.addr;
        match &outcome {
            Ok(()) => w.succeeded(),
            Err(failure) => {
                warn!("web seed {} failed: {failure}", w.url);
                w.failed(failure, Instant::now());
            }
        }
        // whatever it didn't deliver goes back up for grabs
        for piece in job.pieces.clone() {
            if self.in_flight.holds(addr, piece) {
                self.release_claim(piece, addr);
            }
        }
        if let Some(why) = self.web_seeds[seed].gave_up.clone() {
            self.give_up_web_seed(seed, why);
        }
        self.schedule();
    }

    /// Stops asking web seed `seed` for anything, and puts what it was fetching back up for
    /// grabs.
    pub(super) fn give_up_web_seed(&mut self, seed: usize, why: String) {
        let w = &mut self.web_seeds[seed];
        info!("giving up on web seed {}: {why}", w.url);
        w.gave_up = Some(why);
        w.jobs.clear();
        let addr = w.addr;
        for piece in self.in_flight.held_by(addr) {
            self.release_claim(piece, addr);
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::test_support::*;

    /// A web seed serving `body` as `swarm.bin`, its URL ending in '/' so the torrent's name
    /// is appended.
    async fn web_seed(body: Vec<u8>) -> String {
        crate::webseed::test::serve([("/files/swarm.bin".to_string(), body)].into()).await + "files/"
    }

    fn assert_file_is_content(path: &PathBuf) {
        assert!(
            std::fs::read(path).unwrap() == content(),
            "the file on disk is the content"
        );
    }

    #[tokio::test]
    async fn downloads_from_a_web_seed_alone() {
        let url = web_seed(content()).await;
        let (swarm, handle, path) = swarm_with_web_seeds("webseed", false, Default::default(), vec![url.clone()]);
        let mut stats = handle.stats();
        let peers = handle.peers();
        tokio::spawn(swarm.work_loop());

        wait_until_complete(&mut stats).await;
        assert_file_is_content(&path);
        let done = stats.borrow().clone();
        assert_eq!((done.downloaded, done.wasted), (TOTAL as u64, 0));

        tokio::time::timeout(Duration::from_secs(3), async {
            let mut peers = peers;
            loop {
                let seen = peers.borrow_and_update().clone();
                if let Some(seed) = seen.iter().find(|p| p.downloaded == TOTAL as u64) {
                    assert_eq!(seed.web_seed.as_deref(), Some(url.as_str()));
                    break;
                }
                peers.changed().await.unwrap();
            }
        })
        .await
        .expect("the web seed shows up in the peer list");
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// Serves every request from `peer` with `content()` until the swarm hangs up or the test
    /// stops polling.
    async fn serve_everything(peer: &mut Wire) {
        while let Some(Ok(msg)) = peer.next().await {
            if let BtMessage::Request(req) = msg
                && peer.send(block(req)).await.is_err()
            {
                return;
            }
        }
    }

    /// A web seed that accepts connections and then says nothing holds its pieces until its
    /// timeouts, so in endgame a working one races it for them.
    #[tokio::test]
    async fn a_stalled_web_seeds_pieces_are_raced() {
        let black_hole = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let stalled = format!("http://{}/", black_hole.local_addr().unwrap());
        tokio::spawn(async move {
            let mut held = vec![];
            while let Ok((socket, _)) = black_hole.accept().await {
                held.push(socket);
            }
        });
        let good = web_seed(content()).await;
        let (swarm, handle, path) = swarm_with_web_seeds("stalled", false, Default::default(), vec![stalled, good]);
        let mut stats = handle.stats();
        tokio::spawn(swarm.work_loop());

        wait_until_complete(&mut stats).await;
        assert_file_is_content(&path);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// A web seed whose URL 404s is given up on, and a lying one is dropped at its first bad
    /// piece; either way the peer finishes the download.
    #[tokio::test]
    async fn broken_web_seeds_are_given_up_and_peers_finish() {
        let missing = web_seed(content()).await.replace("files/", "elsewhere/");
        let liar = web_seed(vec![7; TOTAL]).await;
        let (swarm, handle, path) = swarm_with_web_seeds("badseeds", false, Default::default(), vec![missing, liar]);
        let mut stats = handle.stats();
        tokio::spawn(swarm.work_loop());
        // the seeds get the first picks, before a peer is even there
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut seeder = fake_peer(&handle, "10.0.0.1:6881").await;
        open_as_seeder(&mut seeder).await;
        tokio::select! {
            () = wait_until_complete(&mut stats) => {}
            () = serve_everything(&mut seeder) => panic!("the swarm hung up on the peer"),
        }
        assert_file_is_content(&path);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
