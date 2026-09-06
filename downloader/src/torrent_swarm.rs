use crate::announcer::{TrackerStatus, spawn_announcers};
use crate::config::SettingsWatch;
use crate::defs::Identity;
use crate::dht::DhtWatch;
use crate::limiter::RateLimiter;
use crate::peer::{
    PEX_UTP, Peer, PeerSnapshot, PeerStatistics, ProtocolViolation, UT_METADATA_ID, UT_PEX_ID, parse_pex_message,
    parse_ut_metadata_request,
};
use crate::settings::{
    BAD_PEER_BAN, BLOCK_REQUEST_TIMEOUT, BLOCK_SIZE, CHOKING_ROUND_INTERVAL, DIAL_BACKOFF, DIAL_BACKOFF_MAX,
    ENDGAME_MAX_RACED_BYTES, ENDGAME_RACERS, FRUITLESS_PEER_COOLDOWN, KEEPALIVE_INTERVAL, MAX_INFLIGHT_BYTES,
    MAX_UNCHOKED_PEERS, METADATA_PIECE_SIZE, OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS, PEER_TIMEOUT, PEX_INTERVAL,
    PEX_MAX_ADDED_PEERS,
};
use crate::storage::TorrentStorage;
use crate::stream::{DialHints, PeerStream};
use crate::torrent::Torrent;
use crate::utp::UtpWatch;
use crate::wire::{BitField, BtMessage, Piece, Request};
use anyhow::Context;
use bitvec::prelude::*;
use futures::StreamExt;
use futures::future::select_all;
use rand::seq::IndexedRandom;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::net::{SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::time::interval;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{info, warn};

#[derive(Clone, Debug, PartialEq)]
pub struct TorrentSwarmStats {
    pub uploaded: u64,
    pub downloaded: u64,
    /// bytes received that we already had: endgame races, and stray blocks
    pub wasted: u64,
    /// how many bytes we don't have yet
    pub left: usize,
    // how many bytes we've written
    pub written: usize,
    /// indexed by piece number, indicates which pieces have been verified, note it also implies we
    /// have a piece if it's verified
    ///
    /// stored MSB-first (`Msb0`) so `as_raw_slice()` matches BEP 3's bitfield byte layout
    /// directly: piece 0 is the high bit of byte 0.
    pub verified: BitBox<u8, Msb0>,
    /// pieces that belong to a selected file (see `Torrent::wanted_pieces`); `left` and
    /// `completed` only count these
    pub wanted: BitBox<u8, Msb0>,

    pub completed: bool,
}

impl TorrentSwarmStats {
    /// The stats of a swarm that has `verified` on disk and hasn't transferred anything yet.
    pub fn for_verified(torrent: &Torrent, verified: BitBox<u8, Msb0>, wanted: BitBox<u8, Msb0>) -> Self {
        let written: usize = verified
            .iter_ones()
            .map(|p| torrent.nth_piece_size(p as u32).expect("index came from the bitfield"))
            .sum();
        let mut stats = Self {
            uploaded: 0,
            downloaded: 0,
            wasted: 0,
            left: 0,
            written,
            completed: false,
            verified,
            wanted,
        };
        stats.refresh(torrent);
        stats
    }

    /// Recomputes `left` and `completed` from `verified` and `wanted`.
    pub fn refresh(&mut self, torrent: &Torrent) {
        self.left = self
            .wanted
            .iter_ones()
            .filter(|&p| !self.verified[p])
            .map(|p| torrent.nth_piece_size(p as u32).expect("index came from the bitfield"))
            .sum();
        self.completed = self.wanted.iter_ones().all(|p| self.verified[p]);
    }

    /// Verified pieces that are wanted; unwanted pieces aren't progress towards anything.
    pub fn verified_cnt(&self) -> usize {
        self.wanted.iter_ones().filter(|&p| self.verified[p]).count()
    }

    /// Wanted pieces, what `verified_cnt` is out of.
    pub fn total_pieces(&self) -> usize {
        self.wanted.count_ones()
    }

    pub fn all_verified(&self) -> bool {
        self.verified.iter().all(|v| *v)
    }
}

/// Keeps its `TorrentSwarm` running: the swarm's event loop ends when the last handle is
/// dropped, closing every peer connection and (via its own token) stopping its announcers with
/// a farewell announce. Everything the swarm spawns for itself holds only a weak sender, so
/// nothing but a handle can keep it alive.
#[derive(Debug, Clone)]
pub struct TorrentSwarmHandle {
    tx: mpsc::Sender<SwarmEvent>,
    stats: watch::Receiver<TorrentSwarmStats>,
    peers: watch::Receiver<Vec<PeerSnapshot>>,
    trackers: watch::Receiver<Vec<TrackerStatus>>,
}

impl TorrentSwarmHandle {
    /// A live view of the torrent's aggregate progress (uploaded/downloaded/left/written/
    /// verified/completed). Keeps updating for as long as the swarm runs, and outlives this
    /// handle -- it only depends on the channel, not on the swarm still being reachable.
    pub fn stats(&self) -> watch::Receiver<TorrentSwarmStats> {
        self.stats.clone()
    }

    /// The connected peers as of the last housekeeping tick (once a second).
    pub fn peers(&self) -> watch::Receiver<Vec<PeerSnapshot>> {
        self.peers.clone()
    }

    /// Hands a freshly handshaken socket to the swarm, which owns it from here on. Used by both
    /// the inbound listener (`BtClient::accept_incoming`) and the swarm's own dial tasks.
    pub(crate) async fn peer_connected(&self, connected: ConnectedPeer) {
        let _ = self.tx.send(SwarmEvent::PeerConnected(connected)).await;
    }

    /// What each tracker (and the DHT) has done for this torrent lately.
    pub fn trackers(&self) -> watch::Receiver<Vec<TrackerStatus>> {
        self.trackers.clone()
    }

    /// Addresses worth dialing, from wherever the caller got them.
    pub(crate) async fn peers_discovered(&self, peers: Vec<SocketAddr>) {
        let _ = self.tx.send(SwarmEvent::PeersDiscovered(peers)).await;
    }

    /// One flag per file; only pieces of selected files are downloaded.
    pub(crate) async fn select_files(&self, selected: Vec<bool>) {
        let _ = self.tx.send(SwarmEvent::FilesSelected(selected)).await;
    }

    /// Fetch pieces in order (for playing a file while it downloads) rather than rarest first.
    pub(crate) async fn set_sequential(&self, on: bool) {
        let _ = self.tx.send(SwarmEvent::Sequential(on)).await;
    }
}

/// What every swarm of one client has in common: who we are and the client-wide services.
#[derive(Clone)]
pub(crate) struct Shared {
    pub id: Arc<Identity>,
    pub dht: DhtWatch,
    pub utp: UtpWatch,
    /// live settings; the connection cap and the rate limits are read from it
    pub settings: SettingsWatch,
    pub limiter: Arc<RateLimiter>,
}

/// A stream that has completed the BitTorrent handshake and is ready to become a `Peer`.
pub(crate) struct ConnectedPeer {
    pub stream: PeerStream,
    /// we opened it (as opposed to accepting it), so its encryption says what the peer takes
    pub dialed: bool,
    pub remote_addr: SocketAddr,
    pub remote_supports_extensions: bool,
    pub remote_supports_fast: bool,
    pub remote_supports_dht: bool,
    pub peer_id: [u8; 20],
}

/// Everything that reaches the swarm's event loop from outside it: things that happened in
/// tasks it doesn't poll itself (tracker announcers, dial tasks, the inbound listener). The
/// swarm decides what to do about each; the sender never asks it for anything.
pub(crate) enum SwarmEvent {
    /// a tracker answered with peers
    PeersDiscovered(Vec<SocketAddr>),
    /// one flag per file, see `TorrentSwarmHandle::select_files`
    FilesSelected(Vec<bool>),
    /// see `TorrentSwarmHandle::set_sequential`
    Sequential(bool),
    /// a socket finished its handshake and is ours to own
    PeerConnected(ConnectedPeer),
    /// a block a peer asked for has been read off disk (or couldn't be), see `serve_request`
    BlockRead {
        to: SocketAddr,
        block: Result<Piece, Request>,
    },
    /// A dial spawned by `connect_to_discovered_peers` failed. Without this the address would
    /// sit in `dialing` forever, and since dedup against re-discovering the same address checks
    /// `dialing`, it could never be retried -- an address a peer keeps re-gossiping over PEX
    /// needs to actually leave the set on failure.
    DialFailed(SocketAddr),
}

/// One peer's share of an in-flight piece: where it is in requesting the blocks.
struct Claim {
    /// index of the next block to request
    cursor: usize,
    /// walk the piece from the end: the second peer racing for a piece goes the other way,
    /// so the two meet in the middle and the bytes fetched twice are roughly halved
    reverse: bool,
}

/// A piece we're in the middle of downloading. Normally one peer holds it; in endgame
/// (see `schedule`) several race for it, and each block records who delivered it so a
/// failed hash can still convict a lone sender.
struct InFlight {
    buf: Vec<u8>,
    /// per block, the peer it arrived from
    received: Vec<Option<SocketAddr>>,
    claims: BTreeMap<SocketAddr, Claim>,
}

impl InFlight {
    fn new(size: usize, peer: SocketAddr) -> Self {
        let blocks = size.div_ceil(BLOCK_SIZE);
        let mut claims = BTreeMap::new();
        claims.insert(
            peer,
            Claim {
                cursor: 0,
                reverse: false,
            },
        );
        Self {
            buf: vec![0u8; size],
            received: vec![None; blocks],
            claims,
        }
    }

    fn add_racer(&mut self, peer: SocketAddr) {
        let reverse = self.claims.len() % 2 == 1;
        let cursor = if reverse { self.received.len() - 1 } else { 0 };
        self.claims.insert(peer, Claim { cursor, reverse });
    }

    fn request(&self, piece: u32, block: usize) -> Request {
        let begin = block * BLOCK_SIZE;
        Request {
            index: piece,
            begin: begin as u32,
            length: (self.buf.len() - begin).min(BLOCK_SIZE) as u32,
        }
    }

    /// The next block `peer` should ask for: the first one past its cursor that hasn't
    /// arrived from anyone yet.
    fn next_request(&mut self, piece: u32, peer: SocketAddr) -> Option<Request> {
        let claim = self.claims.get_mut(&peer)?;
        loop {
            let block = claim.cursor;
            if block >= self.received.len() {
                return None;
            }
            claim.cursor = if claim.reverse {
                block.wrapping_sub(1)
            } else {
                block + 1
            };
            if self.received[block].is_none() {
                return Some(self.request(piece, block));
            }
        }
    }

    /// Blocks `peer` has still to request
    fn unrequested_blocks(&self, peer: SocketAddr) -> usize {
        let Some(claim) = self.claims.get(&peer) else {
            return 0;
        };
        if claim.cursor >= self.received.len() {
            return 0;
        }
        let ahead = if claim.reverse {
            &self.received[..=claim.cursor]
        } else {
            &self.received[claim.cursor..]
        };
        ahead.iter().filter(|r| r.is_none()).count()
    }

    fn blocks_left(&self) -> usize {
        self.received.iter().filter(|r| r.is_none()).count()
    }

    fn senders(&self) -> BTreeSet<SocketAddr> {
        self.received.iter().flatten().copied().collect()
    }
}

/// What the swarm remembers about an address between connections to it. Trackers and PEX
/// hand out the same addresses over and over, so what happened last time decides whether
/// it's worth dialing again, and a returning peer resumes with the statistics UCB built up
/// on it rather than as a stranger to be explored from scratch.
#[derive(Default)]
struct KnownPeer {
    stats: PeerStatistics,
    consecutive_dial_failures: u32,
    dial_after: Option<Instant>,
    banned_until: Option<Instant>,
    /// PEX flagged it uTP-capable
    prefers_utp: bool,
    /// it refused the encrypted opening when we dialled it
    plaintext_only: bool,
}

impl KnownPeer {
    fn dial_hints(&self) -> DialHints {
        DialHints {
            prefer_utp: self.prefers_utp,
            plaintext: self.plaintext_only,
        }
    }
}

impl KnownPeer {
    fn dial_failed(&mut self, now: Instant) {
        self.consecutive_dial_failures += 1;
        let backoff = DIAL_BACKOFF.saturating_mul(1 << (self.consecutive_dial_failures - 1).min(16));
        self.dial_after = Some(now + backoff.min(DIAL_BACKOFF_MAX));
    }

    fn connected(&mut self) {
        self.consecutive_dial_failures = 0;
        self.dial_after = None;
    }

    fn disconnected(&mut self, stats: &PeerStatistics, now: Instant) {
        if stats.received + stats.sent == 0 {
            self.dial_after = Some(now + FRUITLESS_PEER_COOLDOWN);
        }
        self.stats = stats.for_reconnect();
    }

    fn ban(&mut self, now: Instant) {
        self.banned_until = Some(now + BAD_PEER_BAN);
    }

    fn banned(&self, now: Instant) -> bool {
        self.banned_until.is_some_and(|until| until > now)
    }

    fn may_dial(&self, now: Instant) -> bool {
        !self.banned(now) && !self.dial_after.is_some_and(|after| after > now)
    }
}

/// An IPv4 peer accepted on a dual-stack `[::]` listener shows up as `::ffff:a.b.c.d`;
/// collapsing that to plain `a.b.c.d` gives the peer the same address whether we dialed it or
/// it dialed us, which everything keyed by address depends on.
fn canonical(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(addr.ip().to_canonical(), addr.port())
}

pub struct TorrentSwarm {
    /// sorted by `remote_addr`; there's only ever one connection per address
    peers: Vec<Peer>,
    /// addresses with a dial in progress, so the same peer isn't dialed twice
    dialing: BTreeSet<SocketAddr>,
    /// every address that ever connected, disconnected, or failed to dial; not pruned, a
    /// swarm sees a few thousand at most
    known: BTreeMap<SocketAddr, KnownPeer>,
    /// rotates the order peers' sockets are polled in, so a chatty peer at the front of the
    /// list can't starve the rest (`select_all` returns the first ready future in order)
    poll_offset: usize,

    torrent: Arc<Torrent>,
    storage: Arc<TorrentStorage>,

    id: Arc<Identity>,
    /// the client's DHT node, if it has one, for pinging the nodes peers tell us about
    dht: DhtWatch,
    utp: UtpWatch,
    /// live user settings: the connection cap
    settings: SettingsWatch,
    /// the client-wide download and upload limits
    limiter: Arc<RateLimiter>,
    /// blocks read off disk that the upload limit didn't allow out yet, oldest first;
    /// housekeeping sends what the limit allows
    held_uploads: VecDeque<(SocketAddr, Piece)>,

    events_rx: mpsc::Receiver<SwarmEvent>,
    /// for the tasks the swarm spawns for itself (announcers, dials, block reads) to report
    /// back on; weak so they can't keep the swarm alive, only a `TorrentSwarmHandle` can
    events_tx: mpsc::WeakSender<SwarmEvent>,

    /// The peers UCB chooses between. Bayati et al., "The Unreasonable Effectiveness of
    /// Greedy Algorithms in Multi-Armed Bandit with Many Arms": when there are more arms
    /// than about sqrt(horizon), UCB over all of them is provably sub-optimal because trying
    /// each once already costs order-k regret, and running it on a random subsample of
    /// sqrt(horizon) arms is rate-optimal. The horizon here is piece assignments, and a
    /// swarm offers hundreds of peers for a few hundred pieces. Peers are admitted in the
    /// order they become ready until the set is full, which for a swarm is as good as
    /// uniform, and a member's slot only frees when it disconnects.
    subsample: BTreeSet<SocketAddr>,
    subsample_size: usize,

    /// pieces neither verified nor in flight
    missing: Vec<u32>,
    /// pick the lowest missing piece instead of the rarest
    sequential: bool,
    in_flight: BTreeMap<u32, InFlight>,
    /// block requests sent to any peer this session, UCB's `t`
    total_picks: usize,

    stat: TorrentSwarmStats,
    stat_snapshot_tx: watch::Sender<TorrentSwarmStats>,
    peers_snapshot_tx: watch::Sender<Vec<PeerSnapshot>>,

    /// cancels the announcers' token when the swarm goes away, so they get to say goodbye
    _stop_announcers: DropGuard,
}

impl TorrentSwarm {
    /// Starts a swarm for `torrent` on the current runtime and returns the handle that is its
    /// entire interface (see `TorrentSwarmHandle`). Pieces set in `verified` are taken to be on
    /// disk and correct already: they won't be requested again and count as had for `left`.
    pub(crate) fn spawn(
        torrent: Arc<Torrent>,
        storage: Arc<TorrentStorage>,
        verified: BitBox<u8, Msb0>,
        shared: Shared,
    ) -> TorrentSwarmHandle {
        let (swarm, handle) = Self::new(torrent, storage, verified, shared);
        tokio::spawn(swarm.work_loop());
        handle
    }

    fn new(
        torrent: Arc<Torrent>,
        storage: Arc<TorrentStorage>,
        verified: BitBox<u8, Msb0>,
        shared: Shared,
    ) -> (TorrentSwarm, TorrentSwarmHandle) {
        let Shared {
            id,
            dht,
            utp,
            settings,
            limiter,
        } = shared;
        assert_eq!(
            verified.len(),
            torrent.pieces.len(),
            "verified bitfield must have one bit per piece"
        );
        let missing: Vec<u32> = verified.iter_zeros().map(|p| p as u32).collect();
        let subsample_size = (missing.len() as f64).sqrt().ceil() as usize;
        let wanted = bitvec![u8, Msb0; 1; torrent.pieces.len()].into_boxed_bitslice();
        let stat = TorrentSwarmStats::for_verified(&torrent, verified, wanted);
        let (stat_tx, stat_rx) = watch::channel(stat.clone());

        let (events_tx, events_rx) = mpsc::channel(512);
        let (peers_tx, peers_rx) = watch::channel(vec![]);
        let events_tx_weak = events_tx.downgrade();
        let announcers = CancellationToken::new();
        let trackers = spawn_announcers(
            &torrent.all_trackers(),
            torrent.info_hash,
            id.clone(),
            stat_rx.clone(),
            events_tx_weak.clone(),
            announcers.clone(),
            dht.clone(),
        );
        let handle = TorrentSwarmHandle {
            tx: events_tx,
            stats: stat_rx,
            peers: peers_rx,
            trackers,
        };
        let events_tx = events_tx_weak;

        let swarm = TorrentSwarm {
            peers: vec![],
            dialing: BTreeSet::new(),
            known: BTreeMap::new(),
            subsample: BTreeSet::new(),
            subsample_size,
            poll_offset: 0,
            torrent,
            storage,
            id,
            dht,
            utp,
            settings,
            limiter,
            held_uploads: VecDeque::new(),
            events_rx,
            events_tx,
            missing,
            sequential: false,
            in_flight: BTreeMap::new(),
            total_picks: 0,
            stat,
            stat_snapshot_tx: stat_tx,
            peers_snapshot_tx: peers_tx,
            _stop_announcers: announcers.drop_guard(),
        };
        (swarm, handle)
    }

    fn publish_stats(&self) {
        self.stat_snapshot_tx.send_if_modified(|published| {
            if *published == self.stat {
                return false;
            }
            *published = self.stat.clone();
            true
        });
    }

    /// The one event loop for this torrent. Every peer socket is polled from here, and every
    /// piece of per-torrent state is mutated from here, so nothing needs a lock or a channel to
    /// reach it. The flip side, chosen deliberately: a write to one peer that blocks (its
    /// kernel send buffer is full because it stopped reading) stalls this whole loop, every
    /// other peer included, until it drains or the socket dies.
    async fn work_loop(mut self) {
        let mut housekeeping_ticker = interval(Duration::from_secs(1));
        let mut keepalive_ticker = interval(KEEPALIVE_INTERVAL);
        keepalive_ticker.tick().await; // the first tick fires immediately; skip it
        let mut choking_ticker = interval(CHOKING_ROUND_INTERVAL);
        let mut choking_round: u64 = 0;
        let mut pex_ticker = interval(PEX_INTERVAL);

        loop {
            let offset = self.poll_offset;
            tokio::select! {
                (idx, next) = next_peer_message(&mut self.peers, offset) => {
                    self.poll_offset = self.poll_offset.wrapping_add(1);
                    match next {
                        Some(Ok(msg)) => self.on_peer_message(idx, msg).await,
                        Some(Err(e)) => {
                            info!("{} read failed ({e}), disconnecting", self.peers[idx].remote_addr);
                            self.drop_peer(idx);
                        }
                        None => {
                            info!("{} hung up", self.peers[idx].remote_addr);
                            self.drop_peer(idx);
                        }
                    }
                }
                event = self.events_rx.recv() => match event {
                    Some(event) => self.process_event(event).await,
                    // the last TorrentSwarmHandle is gone: this torrent is being dropped
                    None => break,
                },
                _ = housekeeping_ticker.tick() => self.housekeeping().await,
                _ = keepalive_ticker.tick() => {
                    self.broadcast(|peer| Box::pin(peer.send_keepalive())).await;
                }
                _ = choking_ticker.tick() => {
                    choking_round += 1;
                    self.run_choking_algorithm(choking_round).await;
                }
                _ = pex_ticker.tick() => self.run_pex_round().await,
            }
        }
        info!(
            "{} swarm stopped, {} peers dropped",
            self.torrent.name,
            self.peers.len()
        );
    }

    async fn process_event(&mut self, event: SwarmEvent) {
        match event {
            SwarmEvent::PeersDiscovered(peers) => self.connect_to_discovered_peers(peers),
            SwarmEvent::FilesSelected(selected) => self.select_files(&selected).await,
            SwarmEvent::Sequential(on) => self.sequential = on,
            SwarmEvent::PeerConnected(connected) => self.add_peer(connected).await,
            SwarmEvent::BlockRead { to, block } => self.send_block(to, block).await,
            SwarmEvent::DialFailed(addr) => {
                self.dialing.remove(&addr);
                self.known
                    .entry(canonical(addr))
                    .or_default()
                    .dial_failed(Instant::now());
            }
        }
    }

    /// Wants only the pieces of the selected files from now on. Pieces that stopped being
    /// wanted leave the pile, and ones in flight are allowed to finish; newly wanted pieces
    /// join the pile. Completion and `left` follow the new selection.
    async fn select_files(&mut self, selected: &[bool]) {
        self.stat.wanted = self.torrent.wanted_pieces(selected);
        self.missing = self
            .stat
            .wanted
            .iter_ones()
            .filter(|&p| !self.stat.verified[p] && !self.in_flight.contains_key(&(p as u32)))
            .map(|p| p as u32)
            .collect();
        self.stat.refresh(&self.torrent);
        self.publish_stats();
        self.schedule().await;
    }

    /// Once a second: time out stalled requests, drop silent peers, keep the request pipeline
    /// full, and publish progress.
    async fn housekeeping(&mut self) {
        let mut stalled = Vec::new();
        let mut silent = Vec::new();
        for (idx, peer) in self.peers.iter().enumerate() {
            if peer.last_received.elapsed() > PEER_TIMEOUT {
                silent.push(idx);
                continue;
            }
            // a peer that accepted requests and then went quiet (as opposed to disconnecting
            // outright) would otherwise hold its pieces' slots forever
            if peer.stalled(BLOCK_REQUEST_TIMEOUT) {
                stalled.extend(peer.requested.keys().map(|req| (req.index, peer.remote_addr)));
            }
        }
        for idx in silent.into_iter().rev() {
            info!(
                "{} timed out (no messages for {:?}), disconnecting",
                self.peers[idx].remote_addr, PEER_TIMEOUT
            );
            self.drop_peer(idx);
        }
        stalled.sort_unstable();
        stalled.dedup();
        for (piece, peer) in stalled {
            info!("piece {piece} stalled at {peer}, will retry");
            self.release_claim(piece, peer);
        }

        self.schedule().await;
        self.send_held_uploads().await;
        self.publish_stats();
        let _ = self
            .peers_snapshot_tx
            .send(self.peers.iter().map(Peer::snapshot).collect());
    }

    /// Sends blocks the upload limit held back, as far as it allows now. A block for a peer
    /// that has since gone is dropped.
    async fn send_held_uploads(&mut self) {
        while let Some((to, block)) = self.held_uploads.pop_front() {
            let Some(idx) = self.peer_index(to) else {
                continue;
            };
            if !self.limiter.take_upload(block.length as usize) {
                self.held_uploads.push_front((to, block));
                break;
            }
            self.stat.uploaded += block.length as u64;
            if self.peers[idx].send_block(block).await.is_err() {
                self.drop_peer(idx);
            }
        }
    }

    fn ban(&mut self, addr: SocketAddr) {
        self.known.entry(addr).or_default().ban(Instant::now());
    }

    fn pieces_held_by(&self, addr: SocketAddr) -> Vec<u32> {
        self.in_flight
            .iter()
            .filter(|(_, f)| f.claims.contains_key(&addr))
            .map(|(piece, _)| *piece)
            .collect()
    }

    fn peer_index(&self, addr: SocketAddr) -> Option<usize> {
        self.peers.binary_search_by_key(&addr, |p| p.remote_addr).ok()
    }

    /// Removes a peer and puts whatever it was downloading for us back up for grabs.
    fn drop_peer(&mut self, idx: usize) {
        let peer = self.peers.remove(idx);
        self.subsample.remove(&peer.remote_addr);
        self.known
            .entry(peer.remote_addr)
            .or_default()
            .disconnected(&peer.stats, Instant::now());
        for piece in self.pieces_held_by(peer.remote_addr) {
            self.release_claim(piece, peer.remote_addr);
        }
        info!("{} disconnected, {} peers left", peer.remote_addr, self.peers.len());
    }

    /// Takes `peer` off an in-flight piece and forgets its outstanding requests for it. The
    /// piece goes back to `missing` if no one else holds it. Not telling the peer is right in
    /// every case this is used: it choked us, rejected or stalled the request, sent garbage,
    /// or went away. A block for it arriving later is ignored, not a protocol violation.
    fn release_claim(&mut self, piece: u32, peer: SocketAddr) {
        if let Some(idx) = self.peer_index(peer) {
            self.peers[idx].forget_piece(piece);
        }
        let Some(in_flight) = self.in_flight.get_mut(&piece) else {
            return;
        };
        in_flight.claims.remove(&peer);
        if in_flight.claims.is_empty() {
            self.in_flight.remove(&piece);
            self.missing.push(piece);
        }
    }

    /// Sends the same message to every peer, dropping any the write fails for.
    async fn broadcast<F>(&mut self, mut send: F)
    where
        F: for<'p> FnMut(&'p mut Peer) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'p>>,
    {
        let mut dead = Vec::new();
        for (idx, peer) in self.peers.iter_mut().enumerate() {
            if send(peer).await.is_err() {
                dead.push(idx);
            }
        }
        for idx in dead.into_iter().rev() {
            self.drop_peer(idx);
        }
    }

    async fn on_peer_message(&mut self, idx: usize, msg: BtMessage) {
        let peer = &mut self.peers[idx];
        let choked = matches!(msg, BtMessage::Choke(_));
        let msg = match peer.apply(msg) {
            Ok(None) => {
                if choked {
                    // BEP 3: a choke discards our outstanding requests, and nothing more
                    // will be asked of the peer until it unchokes, so its pieces go back
                    // on the pile now rather than after a stall timeout
                    let addr = peer.remote_addr;
                    for piece in self.pieces_held_by(addr) {
                        self.release_claim(piece, addr);
                    }
                }
                // a Have/BitField/Unchoke may have just made a piece requestable
                self.schedule().await;
                return;
            }
            Ok(Some(msg)) => msg,
            Err(ProtocolViolation(what)) => {
                warn!("{} sent {what}, disconnecting", peer.remote_addr);
                let addr = peer.remote_addr;
                self.drop_peer(idx);
                self.ban(addr);
                return;
            }
        };

        match msg {
            BtMessage::Port(port) => {
                // BEP 5: the peer runs a DHT node there; pinging it puts it in our routing
                // table, which is how the table fills from a swarm rather than the routers
                let dht = self.dht.borrow().clone();
                if let (Some(dht), SocketAddr::V4(addr)) = (dht, peer.remote_addr) {
                    let node = SocketAddrV4::new(*addr.ip(), port.port);
                    tokio::spawn(async move {
                        let _ = dht.client.ping(node).await;
                    });
                }
            }
            BtMessage::Request(request) => self.serve_request(idx, request).await,
            BtMessage::Piece(piece) => self.block_arrived(idx, piece).await,
            BtMessage::RejectRequest(reject) => {
                // BEP 6: the peer is declining a request we made; the piece it belonged to
                // goes back on the pile rather than idling out BLOCK_REQUEST_TIMEOUT
                let req = Request {
                    index: reject.index,
                    begin: reject.begin,
                    length: reject.length,
                };
                if peer.requested.remove(&req).is_some() {
                    tracing::debug!(
                        "{} rejected {req:?}, giving up piece {} there",
                        peer.remote_addr,
                        req.index
                    );
                    let addr = peer.remote_addr;
                    self.release_claim(req.index, addr);
                    self.schedule().await;
                }
            }
            BtMessage::Extended(ext) if ext.ext_id == UT_METADATA_ID => {
                // BEP 9: we always have the full metadata, so any in-range piece is served
                // unconditionally. Without a negotiated id there's nothing to reply on.
                if peer.their_ut_metadata_id.is_none() {
                    return;
                }
                let Some(piece) = parse_ut_metadata_request(&ext.payload) else {
                    return;
                };
                let start = piece as usize * METADATA_PIECE_SIZE;
                if start >= self.torrent.raw_info.len() {
                    return;
                }
                let end = (start + METADATA_PIECE_SIZE).min(self.torrent.raw_info.len());
                let total_size = self.torrent.metadata_size();
                let data = &self.torrent.raw_info[start..end];
                if peer.send_metadata_piece(piece, total_size, data).await.is_err() {
                    self.drop_peer(idx);
                }
            }
            BtMessage::Extended(ext) if ext.ext_id == UT_PEX_ID => {
                // BEP 27: don't act on PEX for a private torrent even if some peer sends it
                // anyway (we don't advertise ut_pex when private, so a compliant peer won't)
                if !self.torrent.private {
                    let gossiped = parse_pex_message(&ext.payload)
                        .into_iter()
                        .map(|(addr, flags)| (addr, flags & PEX_UTP != 0))
                        .collect();
                    self.connect_to_peers(gossiped);
                }
            }
            BtMessage::Extended(ext) => {
                tracing::debug!(
                    "{} sent an unsupported extended message id {}",
                    peer.remote_addr,
                    ext.ext_id
                );
            }
            other => unreachable!("Peer::apply handles everything else: {other:?}"),
        }
    }

    /// BEP 3: a choked peer isn't entitled to any data, full stop. BEP 6 turns "ignore it"
    /// into "must say so": once Fast Extension is negotiated a declined request needs an
    /// explicit RejectRequest, which `send_reject` no-ops on its own if it isn't.
    async fn serve_request(&mut self, idx: usize, request: Request) {
        let peer = &mut self.peers[idx];
        if peer.choked_them {
            if peer.send_reject(request).await.is_err() {
                self.drop_peer(idx);
            }
            return;
        }

        // never serve a piece we haven't hash-verified
        let verified = self.stat.verified.get(request.index as usize).is_some_and(|b| *b);
        if !verified {
            if peer.send_reject(request).await.is_err() {
                self.drop_peer(idx);
            }
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

    /// Second half of `serve_request`: the bytes are in hand (or the read failed, in which
    /// case the request is declined). The peer may have been choked or dropped meanwhile.
    async fn send_block(&mut self, to: SocketAddr, block: Result<Piece, Request>) {
        let Some(idx) = self.peer_index(to) else {
            return;
        };
        let peer = &mut self.peers[idx];
        let sent = match block {
            Ok(block) if !peer.choked_them => {
                if !self.limiter.take_upload(block.length as usize) {
                    self.held_uploads.push_back((to, block));
                    return;
                }
                self.stat.uploaded += block.length as u64;
                peer.send_block(block).await
            }
            Ok(block) => {
                peer.send_reject(Request {
                    index: block.index,
                    begin: block.begin,
                    length: block.length,
                })
                .await
            }
            Err(request) => peer.send_reject(request).await,
        };
        if sent.is_err() {
            self.drop_peer(idx);
        }
    }

    async fn block_arrived(&mut self, idx: usize, block: Piece) {
        let peer = &mut self.peers[idx];
        let from = peer.remote_addr;
        if peer.block_received(&block).is_none() {
            tracing::debug!("{from} sent a block we weren't waiting for, ignoring");
            self.stat.wasted += block.length as u64;
            return;
        }
        self.stat.downloaded += block.length as u64;

        let Some(in_flight) = self.in_flight.get_mut(&block.index) else {
            return;
        };
        let begin = block.begin as usize;
        let end = begin + block.data.len();
        // the block length is remote-controlled; a mismatch must not panic via copy_from_slice
        if block.data.len() != block.length as usize || end > in_flight.buf.len() || !begin.is_multiple_of(BLOCK_SIZE) {
            warn!(
                "{from} sent a malformed block for piece {}, giving up on it there",
                block.index
            );
            self.release_claim(block.index, from);
            return;
        }
        let slot = &mut in_flight.received[begin / BLOCK_SIZE];
        if slot.is_some() {
            // a racer lost this block
            self.stat.wasted += block.length as u64;
            self.refill(idx).await;
            return;
        }
        *slot = Some(from);
        in_flight.buf[begin..end].copy_from_slice(&block.data);
        if in_flight.blocks_left() > 0 {
            self.refill(idx).await;
            return;
        }

        let piece = block.index;
        let in_flight = self.in_flight.remove(&piece).expect("checked above");
        self.cancel_losers(piece, &in_flight).await;
        let senders = in_flight.senders();
        if let Err(e) = self.storage.write_piece(piece, &in_flight.buf) {
            warn!("couldn't write piece {piece}: {e:#}");
            self.missing.push(piece);
            return;
        }
        match self.verify_hash(piece) {
            Some(true) => {}
            Some(false) => {
                if senders.len() == 1 {
                    warn!("piece {piece} from {from} failed hash verification, banning the peer");
                    if let Some(idx) = self.peer_index(from) {
                        self.drop_peer(idx);
                    }
                    self.ban(from);
                } else {
                    warn!("piece {piece} failed hash verification, and came from {senders:?}; will retry");
                }
                self.missing.push(piece);
                self.schedule().await;
                return;
            }
            None => {
                self.missing.push(piece);
                self.schedule().await;
                return;
            }
        }

        info!("piece {piece} is completed");
        self.stat.written += self.torrent.nth_piece_size(piece).expect("piece index in range");
        let was_complete = self.stat.completed;
        self.stat.refresh(&self.torrent);
        if self.stat.completed && !was_complete {
            info!("download complete, {} bytes were received twice", self.stat.wasted);
        }
        // announcers watch this to send a prompt event=completed rather than waiting for
        // their next periodic announce, which could be minutes away
        self.publish_stats();

        self.broadcast(move |peer| Box::pin(peer.send_have(piece))).await;
        self.schedule().await;
    }

    /// A raced piece just completed: every other claimant is told to stop sending the blocks
    /// it still owes. A peer whose socket fails here is dropped.
    async fn cancel_losers(&mut self, piece: u32, in_flight: &InFlight) {
        for &addr in in_flight.claims.keys() {
            let Some(idx) = self.peer_index(addr) else {
                continue;
            };
            let peer = &mut self.peers[idx];
            for req in peer.forget_piece(piece) {
                if peer.send_cancel(req).await.is_err() {
                    self.drop_peer(idx);
                    break;
                }
            }
        }
    }

    /// Keeps up to `MAX_INFLIGHT_BYTES` of pieces on the wire. Rarest-first picks *which piece*
    /// to go after next; UCB scoring separately picks *which peer* to ask -- the two compose
    /// rather than compete. A piece is assigned whole to one peer, but its blocks are only
    /// requested as that peer's window allows (see `refill`).
    ///
    /// Endgame: when nothing is left to assign and peers still have room, the last pieces
    /// would otherwise wait on whichever peer happens to hold them. So the in-flight pieces
    /// furthest from done are also given to up to ENDGAME_RACERS peers in total, bounded by
    /// ENDGAME_MAX_RACED_FRACTION of the torrent at once; the first to finish wins and the
    /// rest are cancelled (`cancel_losers`).
    async fn schedule(&mut self) {
        self.admit_to_subsample();
        loop {
            let in_flight: usize = self.in_flight.values().map(|f| f.buf.len()).sum();
            if in_flight >= MAX_INFLIGHT_BYTES {
                break;
            }
            let Some(piece) = self.next_piece() else {
                break;
            };
            let Some(idx) = self.best_peer(piece) else {
                break;
            };

            let size = self.torrent.nth_piece_size(piece).expect("piece index in range");
            self.missing.retain(|p| *p != piece);
            let addr = self.peers[idx].remote_addr;
            self.in_flight.insert(piece, InFlight::new(size, addr));
            info!(
                "requesting piece {piece} from {addr} (window {})",
                self.peers[idx].request_window()
            );
        }
        if self.missing.is_empty() {
            self.race_the_last_pieces();
        }
        for idx in (0..self.peers.len()).rev() {
            self.refill(idx).await;
        }
    }

    fn race_the_last_pieces(&mut self) {
        let mut raced_bytes: usize = self
            .in_flight
            .values()
            .filter(|f| f.claims.len() > 1)
            .map(|f| f.buf.len())
            .sum();
        // furthest from done first: bytes still missing over the rate of everyone on it
        let mut by_eta: Vec<(f64, u32)> = self
            .in_flight
            .iter()
            .map(|(&piece, f)| {
                let rate: f64 = f
                    .claims
                    .keys()
                    .filter_map(|addr| self.peer_index(*addr))
                    .map(|idx| self.peers[idx].stats.rx_rate)
                    .sum();
                ((f.blocks_left() * BLOCK_SIZE) as f64 / rate.max(1.0), piece)
            })
            .collect();
        by_eta.sort_by(|a, b| b.0.total_cmp(&a.0));
        for (_, piece) in by_eta {
            let already_raced = self.in_flight[&piece].claims.len() > 1;
            if !already_raced && raced_bytes > 0 && raced_bytes >= ENDGAME_MAX_RACED_BYTES {
                continue;
            }
            while self.in_flight[&piece].claims.len() < ENDGAME_RACERS {
                let Some(idx) = self.best_peer(piece) else {
                    break;
                };
                let addr = self.peers[idx].remote_addr;
                self.in_flight.get_mut(&piece).expect("just looked up").add_racer(addr);
                info!("endgame: also requesting piece {piece} from {addr}");
            }
            if !already_raced && self.in_flight[&piece].claims.len() > 1 {
                raced_bytes += self.in_flight[&piece].buf.len();
            }
        }
    }

    /// Tops the peer's outstanding requests up to its window from the pieces assigned to it.
    /// The peer is dropped if a send fails, so callers must not hold an index past this.
    async fn refill(&mut self, idx: usize) {
        let peer = &mut self.peers[idx];
        if !peer.ready() {
            return;
        }
        let addr = peer.remote_addr;
        let mut room = peer.request_window().saturating_sub(peer.requested.len());
        let mut failed = false;
        'pieces: for (&piece, f) in self.in_flight.iter_mut().filter(|(_, f)| f.claims.contains_key(&addr)) {
            while room > 0 {
                if !self.limiter.take_download(BLOCK_SIZE) {
                    // over the download limit for now; housekeeping's schedule() retries
                    break 'pieces;
                }
                let Some(req) = f.next_request(piece, addr) else {
                    continue 'pieces;
                };
                self.total_picks += 1;
                if peer.request_block(req).await.is_err() {
                    failed = true;
                    break 'pieces;
                }
                room -= 1;
            }
            break;
        }
        if failed {
            self.drop_peer(idx);
        }
    }

    /// The next piece to fetch. Rarest first: among the missing pieces, the one held by the
    /// fewest connected peers (ties broken randomly, so many peers starting at once don't all
    /// pile onto the same single rarest piece). Sequential: the lowest one anyone has. `None`
    /// if no connected peer has any of them.
    fn next_piece(&self) -> Option<u32> {
        let availability = |piece: u32| self.peers.iter().filter(|p| p.they_have(piece)).count();
        if self.sequential {
            return self.missing.iter().copied().filter(|&p| availability(p) > 0).min();
        }

        let mut by_availability: Vec<(u32, usize)> = self
            .missing
            .iter()
            .map(|&p| (p, availability(p)))
            .filter(|&(_, count)| count > 0)
            .collect();
        let rarest_count = by_availability.iter().map(|&(_, count)| count).min()?;
        by_availability.retain(|&(_, count)| count == rarest_count);

        by_availability.choose(&mut rand::rng()).map(|&(piece, _)| piece)
    }

    /// Fills free slots in the subsample (see the field) with peers that are ready to serve
    /// us, in peer order.
    fn admit_to_subsample(&mut self) {
        for peer in &self.peers {
            if self.subsample.len() >= self.subsample_size {
                break;
            }
            if peer.ready() {
                self.subsample.insert(peer.remote_addr);
            }
        }
    }

    /// UCB peer selection: of the subsample members that have `piece`, aren't choking us,
    /// aren't already on it, and have room in their request window for more work, the one
    /// with the highest upper confidence bound on its download speed.
    fn best_peer(&self, piece: u32) -> Option<usize> {
        let rate_scale = self.peers.iter().map(|p| p.stats.rx_rate).fold(1.0, f64::max);
        let mut backlog: BTreeMap<SocketAddr, usize> = BTreeMap::new();
        for f in self.in_flight.values() {
            for &addr in f.claims.keys() {
                *backlog.entry(addr).or_default() += f.unrequested_blocks(addr);
            }
        }
        let already_on_it = |p: &Peer| {
            self.in_flight
                .get(&piece)
                .is_some_and(|f| f.claims.contains_key(&p.remote_addr))
        };
        let has_room =
            |p: &Peer| p.requested.len() + backlog.get(&p.remote_addr).copied().unwrap_or(0) < p.request_window();
        self.peers
            .iter()
            .enumerate()
            .filter(|(_, p)| {
                self.subsample.contains(&p.remote_addr)
                    && p.ready()
                    && p.they_have(piece)
                    && has_room(p)
                    && !already_on_it(p)
            })
            .map(|(idx, p)| (idx, p.stats.score(self.total_picks, rate_scale)))
            .max_by(|(_, l), (_, r)| l.total_cmp(r))
            .map(|(idx, _)| idx)
    }

    /// `None` when the piece couldn't be read back: unverified, but through no fault of the
    /// peer that sent it. Never panics, this runs on the swarm's own event loop.
    fn verify_hash(&mut self, piece: u32) -> Option<bool> {
        let Ok(written_data) = self.storage.read_piece(piece) else {
            warn!("couldn't read piece {piece} back off disk to verify it");
            return None;
        };

        let valid_piece = self.torrent.valid_piece(piece, &written_data);
        if valid_piece {
            self.stat.verified.set(piece as usize, true);
        }
        Some(valid_piece)
    }

    /// Takes ownership of a handshaken socket. Sends our side of the opening exchange (BEP 10
    /// extended handshake, then BitField/HaveAll/HaveNone, then Interested) before the peer
    /// joins `peers`, so nothing else can be written to it first.
    async fn add_peer(&mut self, connected: ConnectedPeer) {
        let remote_addr = canonical(connected.remote_addr);
        self.dialing.remove(&remote_addr);
        let Err(insert_at) = self.peers.binary_search_by_key(&remote_addr, |p| p.remote_addr) else {
            info!("{remote_addr} is already connected, dropping the duplicate");
            return;
        };
        let known = self.known.entry(remote_addr).or_default();
        if known.banned(Instant::now()) {
            info!("{remote_addr} is banned, refusing it");
            return;
        }
        known.connected();
        // a dialled peer that came up plaintext under `Prefer` refused the encrypted opening
        if connected.dialed && self.id.encryption == crate::config::Encryption::Prefer {
            known.plaintext_only = !connected.stream.is_encrypted();
        }
        if self.peers.len() >= self.settings.borrow().max_peers_per_torrent {
            tracing::debug!("{remote_addr} refused, at the connection cap");
            return;
        }

        let dht_port = self.dht.borrow().as_ref().map(|dht| dht.udp_port);
        let mut peer = Peer::new(
            connected.stream,
            remote_addr,
            self.torrent.pieces.len(),
            connected.remote_supports_fast,
            connected.peer_id,
        );
        peer.stats = known.stats.clone();
        let opening = async {
            if connected.remote_supports_extensions {
                peer.send_extended_handshake(self.torrent.metadata_size(), self.torrent.private)
                    .await?;
            }
            // BEP 6: a peer that advertised Fast Extension support accepts HaveAll/HaveNone in
            // place of a BitField for the "everything"/"nothing" cases
            if peer.remote_supports_fast && self.stat.all_verified() {
                peer.send_have_all().await?;
            } else if peer.remote_supports_fast && self.stat.verified_cnt() == 0 {
                peer.send_have_none().await?;
            } else {
                let has = Box::from(self.stat.verified.clone().as_raw_slice());
                peer.send_bitfield(BitField { has }).await?;
            }
            // BEP 5: a peer that has a DHT node too gets told where ours listens
            if connected.remote_supports_dht
                && let Some(port) = dht_port
            {
                peer.send_port(port).await?;
            }
            // BEP 3: connections start choked; whether to unchoke is the choking algorithm's
            // call, not an automatic grant on connect
            peer.show_interest().await
        };
        if let Err(e) = opening.await {
            info!("{remote_addr} went away during the opening exchange ({e})");
            self.known
                .entry(remote_addr)
                .or_default()
                .disconnected(&peer.stats, Instant::now());
            return;
        }

        info!("{remote_addr} connected, {} peers now", self.peers.len() + 1);
        self.peers.insert(insert_at, peer);
    }

    /// Dials every address in `peers` we're not already connected to or dialing. Shared by
    /// tracker-discovered peers and BEP 11 (PEX) peers -- both are just addresses.
    fn connect_to_discovered_peers(&mut self, peers: Vec<SocketAddr>) {
        self.connect_to_peers(peers.into_iter().map(|addr| (addr, false)).collect());
    }

    /// Dials what a tracker, the DHT, LSD or PEX handed out, as far as the peer cap allows.
    /// The flag marks an address PEX said speaks uTP; it's remembered only for addresses
    /// that get dialled, so gossip about peers we never call doesn't pile up in `known`.
    fn connect_to_peers(&mut self, peers: Vec<(SocketAddr, bool)>) {
        let now = Instant::now();
        let cap = self.settings.borrow().max_peers_per_torrent;
        for (addr, utp_capable) in peers {
            let addr = canonical(addr);
            if self.peers.len() + self.dialing.len() >= cap {
                break;
            }
            // trackers and PEX both hand out port 0 for peers whose port they don't know
            if addr.port() == 0 {
                continue;
            }
            let worth_it = self.known.get(&addr).is_none_or(|k| k.may_dial(now));
            if !worth_it || self.peer_index(addr).is_some() || !self.dialing.insert(addr) {
                continue;
            }
            if utp_capable {
                self.known.entry(addr).or_default().prefers_utp = true;
            }
            let events = self.events_tx.clone();
            let torrent = self.torrent.clone();
            let our_id = self.id.clone();
            let utp = self.utp.borrow().clone();
            let hints = self.known.get(&addr).map(KnownPeer::dial_hints).unwrap_or_default();
            tokio::spawn(async move {
                let result = match dial(addr, &torrent, &our_id, utp, hints).await {
                    Ok(connected) => SwarmEvent::PeerConnected(connected),
                    Err(e) => {
                        tracing::debug!("couldn't connect to {addr}: {e:#}");
                        SwarmEvent::DialFailed(addr)
                    }
                };
                // if the swarm is gone meanwhile, the socket just drops here
                if let Some(events) = events.upgrade() {
                    let _ = events.send(result).await;
                }
            });
        }
    }

    /// Tit-for-tat unchoking, run periodically. Ranks interested peers (those who want to
    /// download from us) by the download rate they've been giving us -- reciprocation is the
    /// point -- and unchokes the top MAX_UNCHOKED_PEERS. Every OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS
    /// rounds, one additional peer is unchoked at random so a new or under-rated peer gets a
    /// chance to prove itself instead of the same top N being unchoked forever.
    async fn run_choking_algorithm(&mut self, round: u64) {
        let mut interested: Vec<(SocketAddr, f64)> = self
            .peers
            .iter()
            .filter(|p| p.interested_us)
            .map(|p| (p.remote_addr, p.stats.rx_rate))
            .collect();
        interested.sort_by(|a, b| b.1.total_cmp(&a.1));

        let mut to_unchoke: Vec<SocketAddr> = interested.iter().take(MAX_UNCHOKED_PEERS).map(|p| p.0).collect();

        if round.is_multiple_of(OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS) {
            let candidates: Vec<_> = interested.iter().filter(|p| !to_unchoke.contains(&p.0)).collect();
            if let Some(pick) = candidates.choose(&mut rand::rng()) {
                to_unchoke.push(pick.0);
            }
        }

        self.broadcast(move |peer| {
            let should_unchoke = to_unchoke.contains(&peer.remote_addr);
            Box::pin(async move {
                if should_unchoke && peer.choked_them {
                    peer.unchoke().await
                } else if !should_unchoke && !peer.choked_them {
                    peer.choke().await
                } else {
                    Ok(())
                }
            })
        })
        .await;
    }

    /// BEP 11 (PEX): tell each peer about every *other* peer we know of. No per-peer diffing
    /// against what we've told them before ("added"/"dropped" bookkeeping) -- we just resend
    /// the current full membership every round, which is redundant but simple and spec-legal
    /// (PEX is a discovery hint, not an authoritative membership feed).
    async fn run_pex_round(&mut self) {
        // BEP 27: a private torrent's peers must come only from its trackers.
        if self.torrent.private {
            return;
        }

        let all: Vec<(SocketAddr, u8)> = self.peers.iter().map(|p| (p.remote_addr, p.pex_flags())).collect();
        self.broadcast(move |peer| {
            // BEP 11 recommends capping a single PEX message at roughly 50 added peers
            let added: Vec<(SocketAddr, u8)> = all
                .iter()
                .copied()
                .filter(|(a, _)| *a != peer.remote_addr)
                .take(PEX_MAX_ADDED_PEERS)
                .collect();
            Box::pin(async move {
                if added.is_empty() {
                    return Ok(());
                }
                peer.send_pex(&added).await
            })
        })
        .await;
    }
}

/// The next message from any peer, polled starting at `offset` so no peer is always first.
/// Pends forever with no peers, so the caller's `select!` just waits on its other arms.
async fn next_peer_message(peers: &mut [Peer], offset: usize) -> (usize, Option<io::Result<BtMessage>>) {
    if peers.is_empty() {
        return std::future::pending().await;
    }
    let n = peers.len();
    let order: Vec<usize> = (0..n).map(|i| (i + offset) % n).collect();
    let mut sockets: Vec<Option<&mut Peer>> = peers.iter_mut().map(Some).collect();
    let nexts: Vec<_> = order
        .iter()
        .map(|&i| sockets[i].take().expect("each index visited once").socket.next())
        .collect();
    let (msg, position, _rest) = select_all(nexts).await;
    (order[position], msg)
}

async fn dial(
    addr: SocketAddr,
    torrent: &Torrent,
    our_id: &Identity,
    utp: Option<Arc<librqbit_utp::UtpSocketUdp>>,
    hints: DialHints,
) -> anyhow::Result<ConnectedPeer> {
    let (stream, handshake) = tokio::time::timeout(
        crate::settings::HANDSHAKE_TIMEOUT,
        crate::stream::connect(addr, &torrent.info_hash, our_id, utp.as_ref(), hints),
    )
    .await
    .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into()))
    .with_context(|| format!("Failed to connect to {addr}"))?;
    info!(
        "Peer connection to {addr} established{}{}",
        if stream.is_utp() { " over uTP" } else { "" },
        if stream.is_encrypted() { " (encrypted)" } else { "" }
    );
    Ok(ConnectedPeer {
        stream,
        dialed: true,
        remote_addr: addr,
        remote_supports_extensions: handshake.supports_extensions(),
        remote_supports_fast: handshake.supports_fast_extension(),
        remote_supports_dht: handshake.supports_dht(),
        peer_id: handshake.peer_id,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::metadata::build_torrent_file;
    use crate::settings::MIN_REQUEST_WINDOW;
    use crate::torrent::parse_torrent;
    use crate::wire::BtCodec;
    use futures::SinkExt;
    use sha1::{Digest, Sha1};
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::path::PathBuf;
    use tokio::net::TcpListener;
    use tokio_util::codec::Framed;

    const PIECE: usize = 40_000; // 2 full blocks and a short one
    const TOTAL: usize = 100_000; // 3 pieces, the last one short

    fn content() -> Vec<u8> {
        (0..TOTAL).map(|i| (i * 31 % 253) as u8).collect()
    }

    /// A swarm for a single-file torrent of `content()`, its target file in a scratch dir, and
    /// no announcers (the only tracker URL has a scheme no announcer handles).
    fn swarm(name: &str) -> (TorrentSwarm, TorrentSwarmHandle, PathBuf) {
        swarm_with(name, false)
    }

    /// `seeding`: the file already holds `content()` and every piece counts as verified.
    fn swarm_with(name: &str, seeding: bool) -> (TorrentSwarm, TorrentSwarmHandle, PathBuf) {
        swarm_with_settings(name, seeding, crate::config::Settings::default())
    }

    fn swarm_with_settings(
        name: &str,
        seeding: bool,
        settings: crate::config::Settings,
    ) -> (TorrentSwarm, TorrentSwarmHandle, PathBuf) {
        let bytes = content();
        let pieces: Vec<u8> = bytes.chunks(PIECE).flat_map(|c| Sha1::digest(c).to_vec()).collect();
        let mut info = format!(
            "d6:lengthi{TOTAL}e4:name9:swarm.bin12:piece lengthi{PIECE}e6:pieces{}:",
            pieces.len()
        )
        .into_bytes();
        info.extend_from_slice(&pieces);
        info.push(b'e');
        let mut torrent =
            parse_torrent(&build_torrent_file(&info, &["wss://unused.test/announce".to_string()])).unwrap();

        let dir = std::env::temp_dir().join(format!("downloader-swarm-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("swarm.bin");
        torrent.files[0].1 = path.clone();
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.set_len(TOTAL as u64).unwrap();
        if seeding {
            std::fs::write(&path, &bytes).unwrap();
        }

        let torrent = Arc::new(torrent);
        let storage = Arc::new(TorrentStorage::new(torrent.clone(), vec![file]));
        let id = Arc::new(Identity {
            peer_id: *b"-DL0100-swarm-test..",
            serving: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into(),
            dht: false,
            encryption: crate::config::Encryption::Prefer,
        });
        let verified = bitvec![u8, Msb0; seeding as u8; 3].into_boxed_bitslice();
        let (settings_tx, settings_rx) = watch::channel(settings);
        std::mem::forget(settings_tx);
        let shared = Shared {
            id,
            dht: crate::dht::Dht::none(),
            utp: crate::utp::none(),
            settings: settings_rx.clone(),
            limiter: Arc::new(RateLimiter::new(settings_rx)),
        };
        let (swarm, handle) = TorrentSwarm::new(torrent, storage, verified, shared);
        (swarm, handle, path)
    }

    /// Connects a fake remote peer to the swarm: the swarm gets one end of a localhost socket
    /// (as if it had just completed a handshake), the test keeps the other.
    async fn fake_peer(handle: &TorrentSwarmHandle, pretend_addr: &str) -> Framed<tokio::net::TcpStream, BtCodec> {
        fake_peer_with(handle, pretend_addr, false).await
    }

    async fn fake_peer_with(
        handle: &TorrentSwarmHandle,
        pretend_addr: &str,
        fast: bool,
    ) -> Framed<tokio::net::TcpStream, BtCodec> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let ours = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (theirs, _) = listener.accept().await.unwrap();
        handle
            .peer_connected(ConnectedPeer {
                stream: PeerStream::Tcp(ours),
                dialed: false,
                remote_addr: pretend_addr.parse().unwrap(),
                remote_supports_extensions: false,
                remote_supports_fast: fast,
                remote_supports_dht: false,
                peer_id: *b"-TS0001-fake-peer-id",
            })
            .await;
        Framed::new(theirs, BtCodec)
    }

    /// The fake peer's side of the opening exchange: it expects our BitField and Interested,
    /// then declares it has everything and unchokes us.
    async fn open_as_seeder(peer: &mut Framed<tokio::net::TcpStream, BtCodec>) {
        open_with(peer, 0xFF).await;
    }

    /// Like `open_as_seeder`, but the peer declares only the pieces set in `bitfield`.
    async fn open_with(peer: &mut Framed<tokio::net::TcpStream, BtCodec>, bitfield: u8) {
        let Some(Ok(BtMessage::BitField(_))) = peer.next().await else {
            panic!("expected our bitfield first");
        };
        let Some(Ok(BtMessage::Interested(_))) = peer.next().await else {
            panic!("expected Interested after the bitfield");
        };
        peer.send(BtMessage::BitField(BitField {
            has: vec![bitfield; 1].into(),
        }))
        .await
        .unwrap();
        peer.send(BtMessage::Unchoke(crate::wire::Unchoke)).await.unwrap();
    }

    fn block(req: Request) -> BtMessage {
        let start = req.index as usize * PIECE + req.begin as usize;
        BtMessage::Piece(Piece {
            index: req.index,
            begin: req.begin,
            length: req.length,
            data: Box::from(&content()[start..start + req.length as usize]),
        })
    }

    async fn wait_until_complete(stats: &mut watch::Receiver<TorrentSwarmStats>) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !stats.borrow_and_update().completed {
                stats.changed().await.unwrap();
            }
        })
        .await
        .expect("download didn't complete in time");
    }

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

    /// Two files over three pieces: `a` is pieces 0 and 1, `b` is pieces 1 and 2. Deselecting
    /// `b` means piece 2 is never asked for and the torrent completes with two pieces; selecting
    /// it again fetches the third.
    #[tokio::test]
    async fn deselected_files_pieces_are_not_requested() {
        let bytes = content();
        let pieces: Vec<u8> = bytes.chunks(PIECE).flat_map(|c| Sha1::digest(c).to_vec()).collect();
        let mut info = format!(
            "d5:filesld6:lengthi60000e4:pathl1:aeed6:lengthi40000e4:pathl1:beee4:name5:multi12:piece lengthi{PIECE}e6:pieces{}:",
            pieces.len()
        )
        .into_bytes();
        info.extend_from_slice(&pieces);
        info.push(b'e');
        let mut torrent = parse_torrent(&build_torrent_file(&info, &[])).unwrap();
        assert_eq!((torrent.pieces_of_file(0), torrent.pieces_of_file(1)), (0..2, 1..3));

        let dir = std::env::temp_dir().join(format!("downloader-swarm-select-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut handles = vec![];
        for (size, path) in &mut torrent.files {
            *path = dir.join(path.file_name().unwrap());
            let file = std::fs::File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            file.set_len(*size as u64).unwrap();
            handles.push(file);
        }
        let torrent = Arc::new(torrent);
        let storage = Arc::new(TorrentStorage::new(torrent.clone(), handles));
        let id = Arc::new(Identity {
            peer_id: *b"-DL0100-swarm-test..",
            serving: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into(),
            dht: false,
            encryption: crate::config::Encryption::Prefer,
        });
        let shared = Shared {
            id,
            dht: crate::dht::Dht::none(),
            utp: crate::utp::none(),
            settings: crate::bt_client::default_settings(),
            limiter: Arc::new(RateLimiter::new(crate::bt_client::default_settings())),
        };
        let verified = bitvec![u8, Msb0; 0; 3].into_boxed_bitslice();
        let (swarm, handle) = TorrentSwarm::new(torrent, storage, verified, shared);
        let stats = handle.stats();
        tokio::spawn(swarm.work_loop());
        handle.select_files(vec![true, false]).await;

        let mut seeder = fake_peer(&handle, "10.0.0.1:6881").await;
        open_as_seeder(&mut seeder).await;
        let mut asked = BTreeSet::new();
        let serving = async {
            loop {
                match seeder.next().await {
                    Some(Ok(BtMessage::Request(req))) => {
                        asked.insert(req.index);
                        seeder.send(block(req)).await.unwrap();
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
            BTreeSet::from([0, 1]),
            "piece 2 belongs to the unwanted file only"
        );
        let snapshot = stats.borrow().clone();
        assert_eq!((snapshot.verified_cnt(), snapshot.total_pieces()), (2, 2));
        assert_eq!(snapshot.left, 0);

        handle.select_files(vec![true, true]).await;
        let third = async {
            loop {
                match seeder.next().await {
                    Some(Ok(BtMessage::Request(req))) => {
                        assert_eq!(req.index, 2);
                        seeder.send(block(req)).await.unwrap();
                    }
                    Some(Ok(BtMessage::Have(have))) if have.checked == 2 => break,
                    Some(Ok(_)) => {}
                    other => panic!("unexpected {other:?}"),
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(10), third)
            .await
            .expect("selecting the file again fetches its piece");
        assert_eq!(std::fs::read(dir.join("a")).unwrap(), &content()[..60_000]);
        assert_eq!(std::fs::read(dir.join("b")).unwrap(), &content()[60_000..]);
        std::fs::remove_dir_all(dir).unwrap();
    }

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

    #[test]
    fn known_peer_backs_off_failed_dials_and_forgets_on_connect() {
        let t0 = Instant::now();
        let mut k = KnownPeer::default();
        assert!(k.may_dial(t0), "nothing known against it");

        k.dial_failed(t0);
        assert!(!k.may_dial(t0 + DIAL_BACKOFF / 2));
        assert!(k.may_dial(t0 + DIAL_BACKOFF));
        k.dial_failed(t0);
        assert!(!k.may_dial(t0 + DIAL_BACKOFF), "the second failure waits twice as long");
        assert!(k.may_dial(t0 + 2 * DIAL_BACKOFF));
        for _ in 0..40 {
            k.dial_failed(t0);
        }
        assert!(k.may_dial(t0 + DIAL_BACKOFF_MAX), "the backoff is capped");

        k.connected();
        assert!(k.may_dial(t0));
    }

    #[test]
    fn known_peer_cools_down_after_a_fruitless_connection_and_bans_block_dialing() {
        let t0 = Instant::now();
        let mut k = KnownPeer::default();
        k.disconnected(&PeerStatistics::default(), t0);
        assert!(!k.may_dial(t0 + FRUITLESS_PEER_COOLDOWN / 2));
        assert!(k.may_dial(t0 + FRUITLESS_PEER_COOLDOWN));

        let mut useful = PeerStatistics::default();
        useful.block_received(16_384, t0);
        let mut k = KnownPeer::default();
        k.disconnected(&useful, t0);
        assert!(k.may_dial(t0), "a peer that delivered is welcome straight back");
        assert_eq!(k.stats.received, 0, "only the rate and pick count carry over");

        k.ban(t0);
        assert!(k.banned(t0 + BAD_PEER_BAN / 2));
        assert!(!k.may_dial(t0 + BAD_PEER_BAN / 2));
        assert!(!k.banned(t0 + BAD_PEER_BAN));
    }

    /// Trackers and PEX keep handing out the same addresses. One that connected and then hung
    /// up without a block exchanged isn't dialed again for a while; one that delivered is.
    #[tokio::test]
    async fn fruitless_peers_are_not_redialed_but_useful_ones_are() {
        let (swarm, handle, path) = swarm("redial");
        tokio::spawn(swarm.work_loop());

        let fruitless_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let fruitless_addr = fruitless_listener.local_addr().unwrap();
        let mut fruitless = fake_peer_with(&handle, &fruitless_addr.to_string(), false).await;
        open_as_seeder(&mut fruitless).await;
        let Some(Ok(BtMessage::Request(_))) = fruitless.next().await else {
            panic!("expected a request");
        };
        drop(fruitless);

        let useful_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let useful_addr = useful_listener.local_addr().unwrap();
        let mut useful = fake_peer_with(&handle, &useful_addr.to_string(), false).await;
        open_as_seeder(&mut useful).await;
        let Some(Ok(BtMessage::Request(req))) = useful.next().await else {
            panic!("expected a request");
        };
        useful.send(block(req)).await.unwrap();
        // let the swarm take the block before the socket goes away under it
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(useful);
        tokio::time::sleep(Duration::from_millis(100)).await;

        handle
            .tx
            .send(SwarmEvent::PeersDiscovered(vec![fruitless_addr, useful_addr]))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), useful_listener.accept())
            .await
            .expect("a peer that delivered is dialed again")
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(300), fruitless_listener.accept())
                .await
                .is_err(),
            "a peer that delivered nothing is left alone"
        );
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
        assert_eq!(swarm.subsample_size, 2);
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
                            length: req.length,
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

    /// The handle is what keeps a swarm alive: once the last one is gone the loop ends and
    /// every peer's socket closes with it.
    #[tokio::test]
    async fn dropping_the_last_handle_stops_the_swarm_and_its_peers() {
        let (swarm, handle, path) = swarm("lifetime");
        let running = tokio::spawn(swarm.work_loop());

        let mut peer = fake_peer(&handle, "10.0.0.5:6881").await;
        let Some(Ok(BtMessage::BitField(_))) = peer.next().await else {
            panic!("expected our bitfield");
        };

        drop(handle);
        tokio::time::timeout(Duration::from_secs(5), running)
            .await
            .expect("the swarm loop kept running without a handle")
            .unwrap();
        // whatever's buffered (Interested) drains, then the socket is closed
        let closed = async { while let Some(Ok(_)) = peer.next().await {} };
        tokio::time::timeout(Duration::from_secs(5), closed)
            .await
            .expect("peer socket stayed open after the swarm stopped");
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

        a.send(BtMessage::RejectRequest(crate::wire::RejectRequest {
            index: rejected.index,
            begin: rejected.begin,
            length: rejected.length,
        }))
        .await
        .unwrap();

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
