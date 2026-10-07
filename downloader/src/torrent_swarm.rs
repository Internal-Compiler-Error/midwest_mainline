use crate::announcer::{Announcing, TrackerStatus, spawn_announcers};
use crate::config::SettingsWatch;
use crate::defs::Identity;
use crate::dht::DhtWatch;
use crate::events::{Event, EventBus, PeerSource};
use crate::external::ExternalAddress;
use crate::layers::{self, LayerFetch, Received};
use crate::limiter::RateLimiter;
use crate::merkle::Hash;
use crate::peer::{
    Holepunch, HolepunchError, Inbox, Incoming, LT_DONTHAVE_ID, PEX_UTP, Peer, PeerSnapshot, PeerStatistics,
    ProtocolViolation, UT_HOLEPUNCH_ID, UT_METADATA_ID, UT_PEX_ID, parse_pex_message, parse_ut_metadata_request,
};
use crate::settings::{
    BAD_PEER_BAN, BLOCK_REQUEST_TIMEOUT, BLOCK_SIZE, CHOKING_ROUND_INTERVAL, DIAL_BACKOFF, DIAL_BACKOFF_MAX,
    ENDGAME_LAST_PIECES, ENDGAME_LAST_RACERS, ENDGAME_RACERS, FRUITLESS_PEER_COOLDOWN, KEEPALIVE_INTERVAL,
    MAX_HALF_OPEN, MAX_INFLIGHT_BYTES, MAX_QUEUED_UPLOADS, MAX_SERVED_BLOCK, MAX_UNCHOKED_PEERS, METADATA_PIECE_SIZE,
    OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS, PEER_TIMEOUT, PEX_INTERVAL, PEX_MAX_ADDED_PEERS, SWARM_INBOX,
};
use crate::storage::TorrentStorage;
use crate::stream::{DialHints, PeerStream};
use crate::torrent::Torrent;
use crate::utp::UtpWatch;
use crate::webseed::{self, Failure, WebJob, WebSeed};
use crate::wire::{BitField, BtMessage, Piece, Request, V2Support};
use anyhow::Context;
use bitvec::prelude::*;
use rand::RngExt;
use rand::seq::IndexedRandom;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::time::interval;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::Instrument;
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
    /// the files couldn't be written (disk full, drive gone, ...); downloading has stopped
    pub storage_error: Option<String>,
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
            storage_error: None,
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
    /// what handshakes for this torrent say about BEP 52, for the inbound listener's answer
    pub(crate) v2: V2Support,
}

impl TorrentSwarmHandle {
    /// A live view of the torrent's aggregate progress (uploaded/downloaded/left/written/
    /// verified/completed). Keeps updating for as long as the swarm runs, and outlives this
    /// handle -- it only depends on the channel, not on the swarm still being reachable.
    pub fn stats(&self) -> watch::Receiver<TorrentSwarmStats> {
        self.stats.clone()
    }

    /// Whether both handles are to one swarm (a hybrid is registered under two hashes).
    pub(crate) fn same_swarm(&self, other: &TorrentSwarmHandle) -> bool {
        self.tx.same_channel(&other.tx)
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
    pub(crate) async fn peers_discovered(&self, peers: Vec<SocketAddr>, source: PeerSource) {
        let _ = self.tx.send(SwarmEvent::PeersDiscovered(peers, source)).await;
    }

    /// One flag per file; only pieces of selected files are downloaded.
    pub(crate) async fn select_files(&self, selected: Vec<bool>) {
        let _ = self.tx.send(SwarmEvent::FilesSelected(selected)).await;
    }

    /// Fetch pieces in order (for playing a file while it downloads) rather than rarest first.
    pub(crate) async fn set_sequential(&self, on: bool) {
        let _ = self.tx.send(SwarmEvent::Sequential(on)).await;
    }

    /// BEP 16: while we have every piece, show each new peer only a piece at a time.
    pub(crate) async fn set_super_seed(&self, on: bool) {
        let _ = self.tx.send(SwarmEvent::SuperSeed(on)).await;
    }
}

/// What every swarm of one client has in common: who we are and the client-wide services.
#[derive(Clone)]
pub(crate) struct Shared {
    pub events: EventBus,
    pub id: Arc<Identity>,
    pub dht: DhtWatch,
    pub utp: UtpWatch,
    /// live settings; the connection cap and the rate limits are read from it
    pub settings: SettingsWatch,
    pub limiter: Arc<RateLimiter>,
    pub external: ExternalAddress,
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
    /// BEP 52: the connection can carry hash requests (see `V2Support::v2_peer`)
    pub remote_supports_v2: bool,
    pub peer_id: [u8; 20],
}

/// Everything that reaches the swarm's event loop from outside it: things that happened in
/// tasks it doesn't poll itself (tracker announcers, dial tasks, the inbound listener). The
/// swarm decides what to do about each; the sender never asks it for anything.
pub(crate) enum SwarmEvent {
    /// a tracker, the DHT, LSD or the metadata fetch handed out peers
    PeersDiscovered(Vec<SocketAddr>, PeerSource),
    /// one flag per file, see `TorrentSwarmHandle::select_files`
    FilesSelected(Vec<bool>),
    /// see `TorrentSwarmHandle::set_sequential`
    Sequential(bool),
    SuperSeed(bool),
    /// a socket finished its handshake and is ours to own
    PeerConnected(ConnectedPeer),
    /// a block a peer asked for has been read off disk (or couldn't be), see `serve_request`
    BlockRead {
        to: SocketAddr,
        block: Result<Piece, Request>,
    },
    /// A completed piece was hashed and, if it was good, written (see `piece_assembled`).
    PieceDone {
        piece: u32,
        len: usize,
        senders: BTreeSet<SocketAddr>,
        /// whether it matched its hash, or why it couldn't be written
        outcome: Result<bool, String>,
    },
    /// A dial spawned by `connect_to_discovered_peers` failed. Without this the address would
    /// sit in `dialing` forever, and since dedup against re-discovering the same address checks
    /// `dialing`, it could never be retried -- an address a peer keeps re-gossiping over PEX
    /// needs to actually leave the set on failure.
    DialFailed(SocketAddr),
    /// a hybrid's pieces that failed `recheck_by_layer`
    Rechecked(Vec<u32>),
    /// a web seed's job (see `start_web_job`) fetched a block
    WebSeedBlock {
        seed: usize,
        job: u64,
        block: Piece,
    },
    /// a web seed's job ended; every block it delivered came before this
    WebSeedDone {
        seed: usize,
        job: u64,
        outcome: Result<(), Failure>,
    },
}

/// One peer's share of an in-flight piece: where it is in requesting the blocks.
struct Claim {
    /// index of the next block to request
    cursor: usize,
    /// walk the piece from the end: the second peer racing for a piece goes the other way,
    /// so the two meet in the middle and the bytes fetched twice are roughly halved
    reverse: bool,
}

/// Who "delivered" a block of padding (BEP 47): nobody, it's zeros from the start.
const PADDING: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0));

/// A piece we're in the middle of downloading. Normally one peer holds it; in endgame
/// (see `schedule`) several race for it, and each block records who delivered it so a
/// failed hash can still convict a lone sender.
struct InFlight {
    buf: Vec<u8>,
    /// per block, the peer it arrived from
    received: Vec<Option<SocketAddr>>,
    claims: BTreeMap<SocketAddr, Claim>,
    /// from assignment to verification, for the traces
    span: tracing::Span,
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
            span: tracing::Span::none(),
        }
    }

    /// Marks the blocks that are nothing but padding as in already: the buffer starts out
    /// zeroed, so there's nothing to request.
    fn skip_padding(&mut self, torrent: &Torrent, piece: u32) {
        for pad in torrent.padding_in_piece(piece) {
            let first = pad.start.div_ceil(BLOCK_SIZE);
            let end = if pad.end == self.buf.len() {
                self.received.len()
            } else {
                pad.end / BLOCK_SIZE
            };
            for block in first..end {
                self.received[block] = Some(PADDING);
            }
        }
    }

    /// A claim on every block not in yet, all requested at once: a web seed's.
    fn claim_rest(&mut self, source: SocketAddr) {
        let cursor = self.received.len();
        self.claims.insert(source, Claim { cursor, reverse: false });
        self.span.record("racers", self.claims.len());
    }

    /// Torrent-relative bytes from the first block not in yet to the end of the last one.
    fn missing_span(&self, piece_offset: u64) -> Option<(u64, u64)> {
        let first = self.received.iter().position(Option::is_none)?;
        let last = self.received.iter().rposition(Option::is_none)?;
        let end = ((last + 1) * BLOCK_SIZE).min(self.buf.len());
        Some((piece_offset + (first * BLOCK_SIZE) as u64, piece_offset + end as u64))
    }

    fn add_racer(&mut self, peer: SocketAddr) {
        let reverse = self.claims.len() % 2 == 1;
        let cursor = if reverse { self.received.len() - 1 } else { 0 };
        self.claims.insert(peer, Claim { cursor, reverse });
        self.span.record("racers", self.claims.len());
        tracing::debug!(parent: &self.span, %peer, "racer joined");
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
        self.received
            .iter()
            .flatten()
            .copied()
            .filter(|&a| a != PADDING)
            .collect()
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
    /// the peer whose PEX told us about it: who to ask for a holepunch if dialing fails
    via: Option<SocketAddr>,
    /// a holepunch was asked for already; one try per address
    holepunched: bool,
}

impl KnownPeer {
    fn dial_hints(&self) -> DialHints {
        DialHints {
            prefer_utp: self.prefers_utp,
            plaintext: self.plaintext_only,
            utp_only: false,
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
    /// every address that connected, disconnected, or failed to dial; pruned once it's large
    /// (see `prune_known`)
    known: BTreeMap<SocketAddr, KnownPeer>,
    /// every peer's reader delivers here (see `Peer`)
    inbox: Inbox,
    incoming: mpsc::Receiver<Incoming>,
    /// the next connection's `Peer::conn`
    next_conn: u64,

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
    bus: EventBus,
    /// blocks read off disk that the upload limit didn't allow out yet, oldest first;
    /// housekeeping sends what the limit allows
    held_uploads: VecDeque<(SocketAddr, Piece)>,

    events_rx: mpsc::Receiver<SwarmEvent>,
    /// for the tasks the swarm spawns for itself (announcers, dials, block reads) to report
    /// back on; weak so they can't keep the swarm alive, only a `TorrentSwarmHandle` can
    events_tx: mpsc::WeakSender<SwarmEvent>,

    /// Peers that haven't sent us anything yet and are being given pieces to find out how
    /// they do. Bayati et al., "The Unreasonable Effectiveness of Greedy Algorithms in
    /// Multi-Armed Bandit with Many Arms": with more arms than about sqrt(horizon), trying
    /// each one already costs order-k regret, and sampling sqrt(horizon) of them is
    /// rate-optimal. Unlike the bandit, a swarm can play every good arm at once, so the bound
    /// applies to exploration only: a peer that has delivered is always eligible, and at most
    /// `explore_slots` unproven ones are on trial at a time. A trial ends when the peer
    /// delivers (it graduates), chokes us, or goes away.
    exploring: BTreeSet<SocketAddr>,
    explore_slots: usize,
    /// per piece, how many connected peers have it; rarest-first reads this instead of
    /// scanning every peer's bitfield for every pick
    availability: Vec<u32>,

    /// pieces neither verified nor in flight
    missing: Vec<u32>,
    /// pick the lowest missing piece instead of the rarest
    sequential: bool,
    super_seed: bool,
    /// BEP 16: how many peers each piece has been revealed to
    super_seed_offers: Vec<u32>,
    in_flight: BTreeMap<u32, InFlight>,
    /// our public address by the votes of peers (`yourip`) and trackers
    external: ExternalAddress,
    /// per peer, the in-flight pieces it holds a claim on: the inverse of `InFlight::claims`,
    /// so a peer's pieces are found without scanning everything in flight (once per block)
    holdings: BTreeMap<SocketAddr, BTreeSet<u32>>,
    /// complete pieces off being hashed and written (see `piece_assembled`)
    hashing: BTreeSet<u32>,
    /// block requests sent to any peer this session, UCB's `t`
    total_picks: usize,
    /// BEP 52: the piece layers a v2 torrent from a magnet still needs from peers; its pieces
    /// can't be checked (so aren't picked) until their file's layer is in
    layers: LayerFetch,
    /// per file, the Merkle tree above its piece layer, built for the first hash request
    hash_trees: BTreeMap<usize, Vec<Vec<Hash>>>,
    /// BEP 19, in `torrent.web_seeds` order; the index is how their jobs report back
    web_seeds: Vec<WebSeed>,
    next_web_job: u64,

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
            events: bus,
            id,
            dht,
            utp,
            settings,
            limiter,
            external,
        } = shared;
        assert_eq!(
            verified.len(),
            torrent.num_pieces(),
            "verified bitfield must have one bit per piece"
        );
        let pieces = torrent.num_pieces();
        let missing: Vec<u32> = verified.iter_zeros().map(|p| p as u32).collect();
        let explore_slots = ((missing.len() as f64).sqrt().ceil() as usize).max(1);
        let wanted = bitvec![u8, Msb0; 1; torrent.num_pieces()].into_boxed_bitslice();
        let stat = TorrentSwarmStats::for_verified(&torrent, verified, wanted);
        let (stat_tx, stat_rx) = watch::channel(stat.clone());

        let (events_tx, events_rx) = mpsc::channel(512);
        let (inbox, incoming) = mpsc::channel(SWARM_INBOX);
        let (peers_tx, peers_rx) = watch::channel(vec![]);
        let events_tx_weak = events_tx.downgrade();
        let announcers = CancellationToken::new();
        let trackers = spawn_announcers(Announcing {
            trackers: torrent.all_trackers(),
            info_hash: torrent.info_hash,
            identity: id.clone(),
            stats: stat_rx.clone(),
            events: events_tx_weak.clone(),
            shutdown: announcers.clone(),
            dht: dht.clone(),
            bus: bus.clone(),
            external: external.clone(),
            v2: torrent.hybrid_v2_hash(),
        });
        bus.emit(Event::PiecesKnown {
            info_hash: torrent.info_hash,
            bitfield: stat
                .verified
                .as_raw_slice()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect(),
        });
        let handle = TorrentSwarmHandle {
            tx: events_tx,
            stats: stat_rx,
            peers: peers_rx,
            trackers,
            v2: torrent.v2_support(),
        };
        let events_tx = events_tx_weak;

        let web_seeds = torrent
            .web_seeds
            .iter()
            .enumerate()
            .map(|(i, url)| WebSeed::new(i, url.clone()))
            .collect();
        let swarm = TorrentSwarm {
            peers: vec![],
            dialing: BTreeSet::new(),
            known: BTreeMap::new(),
            exploring: BTreeSet::new(),
            explore_slots,
            availability: vec![0; pieces],
            layers: LayerFetch::new(&torrent),
            hash_trees: BTreeMap::new(),
            inbox,
            incoming,
            next_conn: 0,
            torrent,
            storage,
            id,
            dht,
            utp,
            settings,
            limiter,
            bus,
            held_uploads: VecDeque::new(),
            events_rx,
            events_tx,
            missing,
            sequential: false,
            super_seed: false,
            super_seed_offers: vec![0; pieces],
            in_flight: BTreeMap::new(),
            holdings: BTreeMap::new(),
            external,
            hashing: BTreeSet::new(),
            total_picks: 0,
            web_seeds,
            next_web_job: 0,
            stat,
            stat_snapshot_tx: stat_tx,
            peers_snapshot_tx: peers_tx,
            _stop_announcers: announcers.drop_guard(),
        };
        (swarm, handle)
    }

    /// Once a second: where every peer stands, and the torrent's running totals.
    fn sample_peers(&self) {
        let info_hash = self.torrent.info_hash;
        for peer in &self.peers {
            self.bus.emit(Event::PeerSample {
                info_hash,
                addr: peer.remote_addr,
                rx_bps: peer.stats.rx_rate,
                downloaded: peer.stats.received as u64,
                uploaded: peer.stats.sent as u64,
                outstanding: peer.requested.len(),
                choked_us: peer.choked_us,
                choked_them: peer.choked_them,
            });
        }
        self.bus.emit(Event::Traffic {
            info_hash,
            downloaded: self.stat.downloaded,
            uploaded: self.stat.uploaded,
            wasted: self.stat.wasted,
            peers: self.peers.len(),
        });
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

    /// The one event loop for this torrent. Every peer's messages arrive here through the
    /// inbox, and every piece of per-torrent state is mutated from here, so nothing needs a
    /// lock to reach it. Sends to peers only queue (see `Peer`), so nothing here waits on a
    /// socket.
    async fn work_loop(mut self) {
        let mut housekeeping_ticker = interval(Duration::from_secs(1));
        let mut keepalive_ticker = interval(KEEPALIVE_INTERVAL);
        keepalive_ticker.tick().await; // the first tick fires immediately; skip it
        let mut choking_ticker = interval(CHOKING_ROUND_INTERVAL);
        let mut choking_round: u64 = 0;
        let mut pex_ticker = interval(PEX_INTERVAL);

        loop {
            tokio::select! {
                Some(Incoming { addr, conn, msg }) = self.incoming.recv() => {
                    // a message from a connection that's already been dropped (possibly
                    // replaced by a newer one to the same address) is stale
                    let Some(idx) = self.peer_index(addr).filter(|&idx| self.peers[idx].conn == conn) else {
                        continue;
                    };
                    match msg {
                        Some(Ok(msg)) => self.on_peer_message(idx, msg).await,
                        Some(Err(e)) => {
                            info!("{} read failed ({e}), disconnecting", self.peers[idx].remote_addr);
                            self.drop_peer(idx, "read failed");
                        }
                        None => {
                            info!("{} hung up", self.peers[idx].remote_addr);
                            self.drop_peer(idx, "hung up");
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
        for w in self.web_seeds.iter().filter(|w| w.stats.received > 0) {
            info!("web seed {} delivered {} bytes", w.url, w.stats.received);
        }
    }

    async fn process_event(&mut self, event: SwarmEvent) {
        match event {
            SwarmEvent::PeersDiscovered(peers, source) => {
                self.bus.emit(Event::PeersDiscovered {
                    info_hash: self.torrent.info_hash,
                    source,
                    count: peers.len(),
                });
                self.connect_to_discovered_peers(peers)
            }
            SwarmEvent::FilesSelected(selected) => self.select_files(&selected).await,
            SwarmEvent::Sequential(on) => self.sequential = on,
            SwarmEvent::SuperSeed(on) => self.set_super_seed(on).await,
            SwarmEvent::PeerConnected(connected) => self.add_peer(connected).await,
            SwarmEvent::BlockRead { to, block } => self.send_block(to, block).await,
            SwarmEvent::PieceDone {
                piece,
                len,
                senders,
                outcome,
            } => self.piece_done(piece, len, senders, outcome).await,
            SwarmEvent::Rechecked(failed) => self.recheck_done(failed).await,
            SwarmEvent::WebSeedBlock { seed, job, block } => self.web_block_arrived(seed, job, block).await,
            SwarmEvent::WebSeedDone { seed, job, outcome } => self.web_job_done(seed, job, outcome).await,
            SwarmEvent::DialFailed(addr) => {
                self.bus.emit(Event::DialFailed {
                    info_hash: self.torrent.info_hash,
                    addr,
                });
                self.dialing.remove(&addr);
                self.known
                    .entry(canonical(addr))
                    .or_default()
                    .dial_failed(Instant::now());
                self.try_holepunch(canonical(addr)).await;
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
            .filter(|&p| {
                !self.stat.verified[p]
                    && !self.in_flight.contains_key(&(p as u32))
                    && !self.hashing.contains(&(p as u32))
            })
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
            self.drop_peer(idx, "timed out");
        }
        stalled.sort_unstable();
        stalled.dedup();
        for (piece, peer) in stalled {
            info!("piece {piece} stalled at {peer}, will retry");
            self.release_claim(piece, peer);
        }

        self.request_layers().await;
        self.schedule().await;
        self.send_held_uploads().await;
        self.prune_known();
        self.publish_stats();
        self.sample_peers();
        let piece_size = self.torrent.piece_size;
        let web_seeds = self
            .web_seeds
            .iter()
            .filter(|w| w.gave_up.is_none())
            .map(|w| PeerSnapshot {
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
            });
        let _ = self
            .peers_snapshot_tx
            .send(self.peers.iter().map(Peer::snapshot).chain(web_seeds).collect());
    }

    /// BEP 52: asks peers for the piece layers we lack, each from a peer that has some of the
    /// file's pieces (and so must be able to answer) and speaks v2 (a hybrid's v1-only peers
    /// don't know hash requests).
    async fn request_layers(&mut self) {
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
                && self.peers[idx].send_message(BtMessage::HashRequest(req)).await.is_err()
            {
                self.drop_peer(idx, "send failed");
            }
        }
    }

    /// A hybrid's pieces of `file` that only SHA-1 vouched for, before the file's piece layer
    /// came, are read back and checked by both hashes on the blocking pool; `recheck_done`
    /// gives up any that fail.
    fn recheck_by_layer(&mut self, file: usize) {
        if self.torrent.v2_only() {
            return;
        }
        let verified = &self.stat.verified;
        let pieces: Vec<u32> = self
            .torrent
            .pieces_of_file(file)
            .filter(|&p| verified[p as usize])
            .collect();
        if pieces.is_empty() {
            return;
        }
        let (torrent, storage, events) = (self.torrent.clone(), self.storage.clone(), self.events_tx.clone());
        tokio::task::spawn_blocking(move || {
            let failed = pieces
                .into_iter()
                .filter(|&p| storage.read_piece(p).is_ok_and(|data| !torrent.valid_piece(p, &data)))
                .collect();
            if let Some(events) = events.upgrade() {
                let _ = events.blocking_send(SwarmEvent::Rechecked(failed));
            }
        });
    }

    /// Pieces that passed SHA-1 but not the Merkle tree: the hybrid's halves disagree about
    /// them, so they're not had after all.
    async fn recheck_done(&mut self, failed: Vec<u32>) {
        for &piece in &failed {
            if !self.stat.verified[piece as usize] {
                continue;
            }
            warn!("piece {piece} matches its SHA-1 hash but not its piece layer; fetching it again");
            self.bus.emit(Event::PieceFailed {
                info_hash: self.torrent.info_hash,
                piece,
                peers: vec![],
            });
            self.stat.verified.set(piece as usize, false);
            self.stat.written -= self.torrent.nth_piece_size(piece).expect("a piece of the torrent");
            if self.stat.wanted[piece as usize] {
                self.missing.push(piece);
            }
        }
        if !failed.is_empty() {
            self.stat.refresh(&self.torrent);
            self.publish_stats();
            self.schedule().await;
        }
    }

    /// Whether `piece` can be checked once it's in; only a v2 piece whose file's layer
    /// hasn't arrived can't.
    fn verifiable(&self, piece: u32) -> bool {
        self.layers.is_empty() || self.torrent.can_verify(piece)
    }

    /// Keeps `known` from growing without bound over a long seed: past `KNOWN_PEERS_MAX`, the
    /// entries that remember nothing worth keeping go (never delivered, not banned, not
    /// connected or being dialled, not waiting out a dial backoff).
    fn prune_known(&mut self) {
        if self.known.len() <= KNOWN_PEERS_MAX {
            return;
        }
        let now = Instant::now();
        let peers = &self.peers;
        let dialing = &self.dialing;
        self.known.retain(|addr, k| {
            k.stats.received > 0
                || k.banned(now)
                || k.dial_after.is_some_and(|after| after > now)
                || dialing.contains(addr)
                || peers.binary_search_by_key(addr, |p| p.remote_addr).is_ok()
        });
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
                self.drop_peer(idx, "send failed");
            }
        }
    }

    /// Our address as peers see it, for BEP 40: the agreed public IP and our listening port.
    fn our_address(&self) -> Option<SocketAddr> {
        let ip = self.external.best()?;
        Some(SocketAddr::new(ip, self.id.serving.port()))
    }

    /// At the cap, the connection `newcomer` may replace (BEP 40): the lowest ranked of those
    /// that have never delivered a block, if `newcomer` outranks it. A peer that has delivered
    /// keeps its place: what UCB measured is better evidence than a hash.
    fn make_room_for(&self, newcomer: SocketAddr) -> Option<usize> {
        let us = self.our_address()?;
        let rank = |addr| crate::priority::peer_priority(us, addr);
        let theirs = rank(newcomer)?;
        let (idx, lowest) = self
            .peers
            .iter()
            .enumerate()
            .filter(|(_, p)| p.stats.received == 0)
            .filter_map(|(idx, p)| Some((idx, rank(p.remote_addr)?)))
            .min_by_key(|&(_, r)| r)?;
        (theirs > lowest).then_some(idx)
    }

    fn ban(&mut self, addr: SocketAddr) {
        self.known.entry(addr).or_default().ban(Instant::now());
    }

    fn pieces_held_by(&self, addr: SocketAddr) -> Vec<u32> {
        self.holdings
            .get(&addr)
            .map(|held| held.iter().copied().collect())
            .unwrap_or_default()
    }

    fn peer_index(&self, addr: SocketAddr) -> Option<usize> {
        self.peers.binary_search_by_key(&addr, |p| p.remote_addr).ok()
    }

    /// Removes a peer and puts whatever it was downloading for us back up for grabs.
    fn drop_peer(&mut self, idx: usize, reason: &'static str) {
        let peer = self.peers.remove(idx);
        peer.span.record("downloaded", peer.stats.received as u64);
        peer.span.record("uploaded", peer.stats.sent as u64);
        peer.span.record("reason", reason);
        self.bus.emit(Event::PeerDisconnected {
            info_hash: self.torrent.info_hash,
            addr: peer.remote_addr,
            downloaded: peer.stats.received as u64,
            uploaded: peer.stats.sent as u64,
            reason,
        });
        self.exploring.remove(&peer.remote_addr);
        for piece in peer.pieces() {
            self.availability[piece as usize] -= 1;
        }
        self.known
            .entry(peer.remote_addr)
            .or_default()
            .disconnected(&peer.stats, Instant::now());
        for piece in self.pieces_held_by(peer.remote_addr) {
            self.release_claim(piece, peer.remote_addr);
        }
        self.holdings.remove(&peer.remote_addr);
        self.layers.give_up(peer.remote_addr);
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
        if let Some(held) = self.holdings.get_mut(&peer) {
            held.remove(&piece);
        }
        tracing::debug!(parent: &in_flight.span, %peer, "claim released");
        if in_flight.claims.is_empty() {
            in_flight.span.record("outcome", "released");
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
            self.drop_peer(idx, "send failed");
        }
    }

    async fn on_peer_message(&mut self, idx: usize, msg: BtMessage) {
        let peer = &mut self.peers[idx];
        let choked = matches!(msg, BtMessage::Choke(_));
        let choked_us_before = peer.choked_us;
        let became_interested = matches!(msg, BtMessage::Interested(_)) && !peer.interested_us;
        let new_piece = match &msg {
            BtMessage::Have(have)
                if (have.checked as usize) < self.availability.len() && !peer.they_have(have.checked) =>
            {
                Some(have.checked)
            }
            _ => None,
        };
        // a whole bitfield replaces whatever the peer claimed before, so its old pieces stop
        // counting towards availability and the new ones start
        let replaces_bitfield = matches!(
            msg,
            BtMessage::BitField(_) | BtMessage::HaveAll(_) | BtMessage::HaveNone(_)
        );
        if replaces_bitfield {
            for piece in peer.pieces() {
                self.availability[piece as usize] -= 1;
            }
        }
        let peer = &mut self.peers[idx];
        let applied = peer.apply(msg);
        if replaces_bitfield {
            // on a violation the old bitfield stands, and dropping the peer takes it off again
            for piece in self.peers[idx].pieces() {
                self.availability[piece as usize] += 1;
            }
        }
        let peer = &mut self.peers[idx];
        if let Some(ip) = peer.yourip.take()
            && let Some(agreed) = self.external.vote(ip, &peer.remote_addr.to_string())
        {
            info!("peers agree our public address is {agreed}");
        }
        let msg = match applied {
            Ok(None) => {
                if peer.choked_us != choked_us_before {
                    tracing::debug!(parent: &peer.span, choked = peer.choked_us, "choke changed by the peer");
                    self.bus.emit(Event::ChokeChanged {
                        info_hash: self.torrent.info_hash,
                        addr: peer.remote_addr,
                        choked: peer.choked_us,
                        by_us: false,
                    });
                }
                if let Some(piece) = new_piece {
                    self.availability[piece as usize] += 1;
                    self.schedule_peer(idx).await;
                } else if choked {
                    // BEP 3: a choke discards our outstanding requests, and nothing more
                    // will be asked of the peer until it unchokes, so its pieces go back
                    // on the pile now for others rather than after a stall timeout
                    let addr = peer.remote_addr;
                    for piece in self.pieces_held_by(addr) {
                        self.release_claim(piece, addr);
                    }
                    self.schedule().await;
                } else if became_interested {
                    self.unchoke_if_slot_free(idx).await;
                } else if replaces_bitfield || peer.choked_us != choked_us_before {
                    // an unchoke or a bitfield may have made pieces requestable
                    self.schedule().await;
                }
                if new_piece.is_some() || replaces_bitfield {
                    self.reveal_where_spread().await;
                }
                return;
            }
            Ok(Some(msg)) => msg,
            Err(ProtocolViolation(what)) => {
                warn!("{} sent {what}, disconnecting", peer.remote_addr);
                let addr = peer.remote_addr;
                self.drop_peer(idx, "protocol violation");
                self.ban(addr);
                return;
            }
        };

        match msg {
            BtMessage::Port(port) => {
                // BEP 5: the peer runs a DHT node there; pinging it puts it in our routing
                // table, which is how the table fills from a swarm rather than the routers
                let client = self
                    .dht
                    .borrow()
                    .as_ref()
                    .and_then(|dht| dht.client_for(&peer.remote_addr).cloned());
                if let Some(client) = client {
                    let node = SocketAddr::new(peer.remote_addr.ip(), port.port);
                    tokio::spawn(async move {
                        let _ = client.ping(node).await;
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
                    self.drop_peer(idx, "send failed");
                }
            }
            BtMessage::Extended(ext) if ext.ext_id == UT_HOLEPUNCH_ID => {
                if let Some(msg) = Holepunch::decode(&ext.payload) {
                    self.on_holepunch(idx, msg).await;
                }
            }
            BtMessage::Extended(ext) if ext.ext_id == LT_DONTHAVE_ID => {
                // BEP 54: the peer dropped a piece; it can't be given that piece any more
                let Ok(raw) = <[u8; 4]>::try_from(&ext.payload[..]) else {
                    return;
                };
                let piece = u32::from_be_bytes(raw);
                if peer.drop_have(piece) {
                    self.availability[piece as usize] -= 1;
                    let addr = peer.remote_addr;
                    if self.holdings.get(&addr).is_some_and(|held| held.contains(&piece)) {
                        self.release_claim(piece, addr);
                        self.schedule().await;
                    }
                }
            }
            BtMessage::Extended(ext) if ext.ext_id == UT_PEX_ID => {
                // BEP 27: don't act on PEX for a private torrent even if some peer sends it
                // anyway (we don't advertise ut_pex when private, so a compliant peer won't)
                if !self.torrent.private {
                    // BEP 11 caps a message at 50 added peers; a peer sending thousands would
                    // otherwise have us dial whoever it likes
                    let gossiped: Vec<(SocketAddr, bool)> = parse_pex_message(&ext.payload)
                        .into_iter()
                        .take(PEX_MAX_ADDED_PEERS)
                        .map(|(addr, flags)| (addr, flags & PEX_UTP != 0))
                        .collect();
                    self.bus.emit(Event::PeersDiscovered {
                        info_hash: self.torrent.info_hash,
                        source: PeerSource::Pex { from: peer.remote_addr },
                        count: gossiped.len(),
                    });
                    let via = peer.remote_addr;
                    self.connect_to_peers(gossiped, Some(via));
                }
            }
            BtMessage::Extended(ext) => {
                tracing::debug!(
                    "{} sent an unsupported extended message id {}",
                    peer.remote_addr,
                    ext.ext_id
                );
            }
            BtMessage::HashRequest(req) => {
                let reply = match layers::answer(&self.torrent, &mut self.hash_trees, &req) {
                    Some(hashes) => BtMessage::Hashes(hashes),
                    None => BtMessage::HashReject(req),
                };
                if self.peers[idx].send_message(reply).await.is_err() {
                    self.drop_peer(idx, "send failed");
                }
            }
            BtMessage::Hashes(hashes) => {
                let addr = self.peers[idx].remote_addr;
                match self.layers.received(&self.torrent, addr, &hashes) {
                    Received::Partial => {}
                    Received::Layer(file) => {
                        info!("piece layer of {:?} in from {addr}", self.torrent.files[file].1);
                        self.recheck_by_layer(file);
                        self.schedule().await;
                    }
                    Received::Bad => {
                        warn!("{addr} sent piece hashes that don't add up, disconnecting");
                        self.drop_peer(idx, "bad hashes");
                        self.ban(addr);
                    }
                }
            }
            BtMessage::HashReject(_) => {
                self.layers.give_up(self.peers[idx].remote_addr);
                self.request_layers().await;
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
            if peer.send_reject(request).await.is_err() {
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

    /// Second half of `serve_request`: the bytes are in hand (or the read failed, in which
    /// case the request is declined). The peer may have been choked or dropped meanwhile.
    async fn send_block(&mut self, to: SocketAddr, block: Result<Piece, Request>) {
        let Some(idx) = self.peer_index(to) else {
            return;
        };
        let peer = &mut self.peers[idx];
        let request = match &block {
            Ok(b) => Request {
                index: b.index,
                begin: b.begin,
                length: b.length,
            },
            Err(request) => *request,
        };
        if !peer.uploads.remove(&request) {
            // cancelled meanwhile
            return;
        }
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
            self.drop_peer(idx, "send failed");
        }
    }

    async fn block_arrived(&mut self, idx: usize, block: Piece) {
        let peer = &mut self.peers[idx];
        let from = peer.remote_addr;
        if peer.block_received(&block).is_none() {
            tracing::debug!("{from} sent a block we weren't waiting for, ignoring");
            self.stat.wasted += block.length as u64;
            self.bus.emit(Event::BlockWasted {
                info_hash: self.torrent.info_hash,
                addr: from,
                len: block.length,
                why: "unexpected",
            });
            return;
        }
        self.stat.downloaded += block.length as u64;
        self.store_block(from, block).await;
    }

    /// A block someone (a peer or a web seed) owed us is in: it goes into its piece, and a
    /// piece with every block in goes off to be checked.
    async fn store_block(&mut self, from: SocketAddr, block: Piece) {
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
        if *slot == Some(PADDING) {
            // a web seed's run covers padding too
            return;
        }
        if slot.is_some() {
            // a racer lost this block
            self.stat.wasted += block.length as u64;
            self.bus.emit(Event::BlockWasted {
                info_hash: self.torrent.info_hash,
                addr: from,
                len: block.length,
                why: "lost race",
            });
            if let Some(idx) = self.peer_index(from) {
                self.refill(idx).await;
            }
            return;
        }
        *slot = Some(from);
        in_flight.buf[begin..end].copy_from_slice(&block.data);
        let raced = in_flight.claims.len() > 1;
        let blocks_left = in_flight.blocks_left();
        if raced && blocks_left > 0 {
            self.cancel_duplicates(&block, from).await;
        }
        if blocks_left > 0 {
            if let Some(idx) = self.peer_index(from) {
                self.schedule_peer(idx).await;
            }
            return;
        }

        let piece = block.index;
        let in_flight = self.in_flight.remove(&piece).expect("checked above");
        for addr in in_flight.claims.keys() {
            if let Some(held) = self.holdings.get_mut(addr) {
                held.remove(&piece);
            }
        }
        self.cancel_losers(piece, &in_flight).await;
        self.piece_assembled(piece, in_flight);
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
                    len: buf.len(),
                    senders,
                    outcome,
                });
            }
        });
    }

    async fn piece_done(
        &mut self,
        piece: u32,
        len: usize,
        senders: BTreeSet<SocketAddr>,
        outcome: Result<bool, String>,
    ) {
        self.hashing.remove(&piece);
        match outcome {
            Ok(true) => {}
            Ok(false) => {
                self.bus.emit(Event::PieceFailed {
                    info_hash: self.torrent.info_hash,
                    piece,
                    peers: senders.iter().copied().collect(),
                });
                if let [from] = senders.iter().copied().collect::<Vec<_>>()[..]
                    && let Some(seed) = self.web_seed_index(from)
                {
                    warn!(
                        "piece {piece} from web seed {} failed hash verification",
                        self.web_seeds[seed].url
                    );
                    self.give_up_web_seed(seed, "sent a bad piece".to_string());
                } else if let [from] = senders.iter().copied().collect::<Vec<_>>()[..] {
                    warn!("piece {piece} from {from} failed hash verification, banning the peer");
                    if let Some(idx) = self.peer_index(from) {
                        self.drop_peer(idx, "sent a bad piece");
                    }
                    self.ban(from);
                } else {
                    warn!("piece {piece} failed hash verification, and came from {senders:?}; will retry");
                }
                self.missing.push(piece);
                self.schedule().await;
                return;
            }
            Err(e) => {
                // Retrying would download the same piece forever into a disk that can't take
                // it, so the torrent stops here and says why; the resume data keeps what was
                // verified so far.
                warn!("couldn't write piece {piece}: {e}; stopping the download");
                self.missing.push(piece);
                if self.stat.storage_error.is_none() {
                    self.stat.storage_error = Some(e);
                    self.holdings.clear();
                    for (piece, f) in std::mem::take(&mut self.in_flight) {
                        for &addr in f.claims.keys() {
                            if let Some(idx) = self.peer_index(addr) {
                                self.peers[idx].forget_piece(piece);
                            }
                        }
                        self.missing.push(piece);
                    }
                }
                self.publish_stats();
                return;
            }
        }

        self.stat.verified.set(piece as usize, true);
        self.bus.emit(Event::PieceVerified {
            info_hash: self.torrent.info_hash,
            piece,
            len,
            peers: senders.iter().copied().collect(),
        });
        info!("piece {piece} is completed");
        let size = self.torrent.nth_piece_size(piece).expect("piece index in range");
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
                    self.id.serving.port(),
                );
                self.broadcast(move |peer| Box::pin(peer.send_extended_handshake(size, private, true, port)))
                    .await;
            }
        }
        // announcers watch this to send a prompt event=completed rather than waiting for
        // their next periodic announce, which could be minutes away
        self.publish_stats();

        self.broadcast(move |peer| Box::pin(peer.send_have(piece))).await;
        self.schedule().await;
    }

    /// A block of a raced piece just arrived from `from`: any other racer that asked for the
    /// same block is told not to bother.
    async fn cancel_duplicates(&mut self, block: &Piece, from: SocketAddr) {
        let req = Request {
            index: block.index,
            begin: block.begin,
            length: block.length,
        };
        let racers: Vec<SocketAddr> = self.in_flight[&block.index]
            .claims
            .keys()
            .copied()
            .filter(|&addr| addr != from)
            .collect();
        for addr in racers {
            let Some(idx) = self.peer_index(addr) else {
                continue;
            };
            let peer = &mut self.peers[idx];
            if peer.requested.remove(&req).is_some() && peer.send_cancel(req).await.is_err() {
                self.drop_peer(idx, "send failed");
            }
        }
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
                    self.drop_peer(idx, "send failed");
                    break;
                }
            }
        }
    }

    /// Keeps up to `MAX_INFLIGHT_BYTES` of pieces on the wire. UCB orders the peers and each
    /// takes the rarest piece it has (see `assign_pieces`). A piece is assigned whole to one
    /// peer, but its blocks are only requested as that peer's window allows (see `refill`).
    ///
    /// Endgame: when nothing is left to assign, the pieces in flight would otherwise wait on
    /// whichever peer holds them, slow ones included. So peers with room also take the pieces
    /// furthest from done, up to ENDGAME_RACERS per piece, from the other end; each block
    /// that arrives is cancelled at the other racers (`cancel_duplicates`).
    async fn schedule(&mut self) {
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
            self.refill(idx).await;
        }
    }

    /// `schedule` for one peer that just got room (a block arrived) or something new to offer
    /// (a Have): cheap enough to run per message, unlike a full pass.
    async fn schedule_peer(&mut self, idx: usize) {
        if self.stat.storage_error.is_some() {
            return;
        }
        // usually the pieces it already holds have blocks left to ask for, and that's all
        let addr = self.peers[idx].remote_addr;
        self.refill(idx).await;
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
            self.refill(idx).await;
        }
    }

    fn eligible(&self, peer: &Peer) -> bool {
        peer.ready() && (peer.proven() || self.exploring.contains(&peer.remote_addr))
    }

    /// Unrequested blocks per peer across the pieces it holds: work it already has queued.
    fn backlogs(&self) -> BTreeMap<SocketAddr, usize> {
        let mut backlog: BTreeMap<SocketAddr, usize> = BTreeMap::new();
        for f in self.in_flight.values() {
            for &addr in f.claims.keys() {
                *backlog.entry(addr).or_default() += f.unrequested_blocks(addr);
            }
        }
        backlog
    }

    /// Hands out missing pieces to eligible peers with room in their window, best UCB score
    /// first, each taking the rarest piece it has (lowest, when sequential). Peers go round
    /// by round so the in-flight budget is shared out rather than taken by the first peer.
    /// `only` limits this to one peer.
    fn assign_pieces(&mut self, only: Option<usize>) {
        let mut in_flight_bytes: usize = self.in_flight.values().map(|f| f.buf.len()).sum();
        let rate_scale = self.rate_scale();
        let mut backlog = self.backlogs();
        let has_room = |p: &Peer, backlog: &BTreeMap<SocketAddr, usize>| {
            p.requested.len() + backlog.get(&p.remote_addr).copied().unwrap_or(0) < p.request_window()
        };
        let mut order: Vec<(Source, f64)> = self
            .peers
            .iter()
            .enumerate()
            .filter(|&(idx, p)| only.is_none_or(|o| o == idx) && self.eligible(p) && has_room(p, &backlog))
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
                if !has_room(&self.peers[idx], &backlog) {
                    continue;
                }
                let Some(pos) = self.pick_piece_for(idx) else {
                    continue;
                };
                let piece = self.missing.swap_remove(pos);
                let size = self.torrent.nth_piece_size(piece).expect("piece index in range");
                let addr = self.peers[idx].remote_addr;
                self.emit_pick(idx, piece, rate_scale);
                let mut in_flight = InFlight::new(size, addr);
                in_flight.skip_padding(&self.torrent, piece);
                in_flight.span = tracing::info_span!(
                    "piece",
                    info_hash = %self.torrent.info_hash,
                    piece,
                    size,
                    peer = %addr,
                    racers = 1,
                    outcome = tracing::field::Empty,
                );
                *backlog.entry(addr).or_default() += in_flight.received.len();
                self.in_flight.insert(piece, in_flight);
                self.holdings.entry(addr).or_default().insert(piece);
                in_flight_bytes += size;
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
    fn pick_piece_for(&self, idx: usize) -> Option<usize> {
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
    fn rate_scale(&self) -> f64 {
        self.peers
            .iter()
            .map(|p| p.stats.rx_rate)
            .chain(self.web_seeds.iter().map(|w| w.stats.rx_rate))
            .fold(1.0, f64::max)
    }

    /// Seconds until `f` is done at the combined rate of everyone on it.
    fn eta(&self, f: &InFlight) -> f64 {
        let rate: f64 = f
            .claims
            .keys()
            .filter_map(|&addr| match self.peer_index(addr) {
                Some(idx) => Some(self.peers[idx].stats.rx_rate),
                None => self.web_seed_index(addr).map(|seed| self.web_seeds[seed].stats.rx_rate),
            })
            .sum();
        (f.blocks_left() * BLOCK_SIZE) as f64 / rate.max(1.0)
    }

    /// BEP 21: everything selected is in, but not everything there is.
    fn super_seeding(&self) -> bool {
        self.super_seed && self.stat.all_verified()
    }

    async fn set_super_seed(&mut self, on: bool) {
        self.super_seed = on;
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
                if peer.send_have(piece).await.is_err() {
                    self.drop_peer(idx, "send failed");
                    break;
                }
            }
        }
    }

    /// BEP 16: shows a super-seeded peer one more piece it lacks: the least common, counting
    /// both who has it and who it was shown to, so one copy of each goes out before seconds.
    async fn reveal_next_piece(&mut self, idx: usize) {
        let peer = &self.peers[idx];
        let Some(view) = &peer.super_seed else { return };
        let scatter = rand::random::<u32>();
        let pick = (0..self.availability.len() as u32)
            .filter(|&p| !peer.they_have(p) && !view.offered.contains(&p))
            .min_by_key(|&p| {
                let seen = self.availability[p as usize] + self.super_seed_offers[p as usize];
                (seen, p.wrapping_mul(0x9E37_79B9) ^ scatter)
            });
        let others = pick.map(|p| self.availability[p as usize]);
        let peer = &mut self.peers[idx];
        let view = peer.super_seed.as_mut().expect("checked above");
        let (Some(piece), Some(others)) = (pick, others) else {
            view.current = None;
            return;
        };
        view.offered.insert(piece);
        view.current = Some((piece, others, Instant::now()));
        self.super_seed_offers[piece as usize] += 1;
        if peer.send_have(piece).await.is_err() {
            self.drop_peer(idx, "send failed");
        }
    }

    /// BEP 16: a peer gets its next piece once the one it was shown turns up at another peer,
    /// which means it passed it on. Alone in the swarm, or holding its piece for a while with
    /// no taker, it gets the next one anyway rather than waiting forever.
    async fn reveal_where_spread(&mut self) {
        if !self.peers.iter().any(|p| p.super_seed.is_some()) {
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
                let others_now = self.availability[piece as usize] - theirs as u32;
                others_now > others_then || (theirs && (lone || shown.elapsed() >= PATIENCE))
            })
            .map(|p| p.remote_addr)
            .collect();
        for addr in ready {
            if let Some(idx) = self.peer_index(addr) {
                self.reveal_next_piece(idx).await;
            }
        }
    }

    fn partial_seed(&self) -> bool {
        self.stat.completed && !self.stat.all_verified()
    }

    fn racers_per_piece(&self) -> usize {
        if self.in_flight.len() <= ENDGAME_LAST_PIECES {
            ENDGAME_LAST_RACERS
        } else {
            ENDGAME_RACERS
        }
    }

    /// Endgame for every peer with room, furthest-from-done pieces first.
    fn race_the_last_pieces(&mut self) {
        let rate_scale = self.rate_scale();
        let mut backlog = self.backlogs();
        let mut by_eta: Vec<(f64, u32)> = self.in_flight.iter().map(|(&piece, f)| (self.eta(f), piece)).collect();
        by_eta.sort_by(|a, b| b.0.total_cmp(&a.0));
        for (_, piece) in by_eta {
            while self.in_flight[&piece].claims.len() < self.racers_per_piece() {
                let Some(idx) = self.best_peer(piece, &backlog, rate_scale) else {
                    break;
                };
                let addr = self.peers[idx].remote_addr;
                self.emit_pick(idx, piece, rate_scale);
                let in_flight = self.in_flight.get_mut(&piece).expect("just looked up");
                in_flight.add_racer(addr);
                *backlog.entry(addr).or_default() += in_flight.unrequested_blocks(addr);
                self.holdings.entry(addr).or_default().insert(piece);
                tracing::debug!("endgame: also requesting piece {piece} from {addr}");
            }
        }
    }

    /// Endgame for one peer that has room: it joins the pieces furthest from done that it can
    /// help with until its window is full. Cheap enough to run per block, unlike the full pass.
    fn race_for_peer(&mut self, idx: usize) {
        let peer = &self.peers[idx];
        if !self.eligible(peer) {
            return;
        }
        let addr = peer.remote_addr;
        let mut backlog: usize = self.in_flight.values().map(|f| f.unrequested_blocks(addr)).sum();
        let rate_scale = self.rate_scale();
        let racers = self.racers_per_piece();
        while self.peers[idx].requested.len() + backlog < self.peers[idx].request_window() {
            let peer = &self.peers[idx];
            let Some((_, piece)) = self
                .in_flight
                .iter()
                .filter(|(p, f)| f.claims.len() < racers && !f.claims.contains_key(&addr) && peer.they_have(**p))
                .map(|(&piece, f)| (self.eta(f), piece))
                .max_by(|a, b| a.0.total_cmp(&b.0))
            else {
                return;
            };
            self.emit_pick(idx, piece, rate_scale);
            let in_flight = self.in_flight.get_mut(&piece).expect("just looked up");
            in_flight.add_racer(addr);
            backlog += in_flight.unrequested_blocks(addr);
            self.holdings.entry(addr).or_default().insert(piece);
            tracing::debug!("endgame: also requesting piece {piece} from {addr}");
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
        'pieces: for &piece in self.holdings.get(&addr).into_iter().flatten() {
            let Some(f) = self.in_flight.get_mut(&piece) else {
                continue;
            };
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
            self.drop_peer(idx, "send failed");
        }
    }

    /// Ends the trials that are over (the peer delivered, choked us, or left) and starts new
    /// ones in the free slots, picking at random among the unproven peers that are ready.
    fn admit_to_exploring(&mut self) {
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
    fn emit_pick(&self, idx: usize, piece: u32, rate_scale: f64) {
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
    fn best_peer(&self, piece: u32, backlog: &BTreeMap<SocketAddr, usize>, rate_scale: f64) -> Option<usize> {
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
            .filter(|(_, p)| self.eligible(p) && p.they_have(piece) && has_room(p) && !already_on_it(p))
            .map(|(idx, p)| (idx, p.stats.score(self.total_picks, rate_scale)))
            .max_by(|(_, l), (_, r)| l.total_cmp(r))
            .map(|(idx, _)| idx)
    }

    fn web_seed_index(&self, addr: SocketAddr) -> Option<usize> {
        self.web_seeds.iter().position(|w| w.addr == addr)
    }

    /// Where in `missing` a web seed's next run starts: the rarest piece, ties going to the
    /// lowest so consecutive jobs read the files front to back; the lowest when sequential.
    fn pick_piece_for_web(&self) -> Option<usize> {
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
    fn assign_web_run(&mut self, seed: usize, budget: usize) -> Option<usize> {
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
            let size = self.torrent.nth_piece_size(piece).expect("piece index in range");
            let mut in_flight = InFlight::new(size, addr);
            in_flight.skip_padding(&self.torrent, piece);
            in_flight.claim_rest(addr);
            in_flight.span = tracing::info_span!(
                "piece",
                info_hash = %self.torrent.info_hash,
                piece,
                size,
                peer = %self.web_seeds[seed].host,
                racers = 1,
                outcome = tracing::field::Empty,
            );
            self.in_flight.insert(piece, in_flight);
            self.holdings.entry(addr).or_default().insert(piece);
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
    fn race_web_seeds(&mut self) {
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
                    .filter(|(_, f)| f.claims.len() < racers && !f.claims.contains_key(&addr))
                    .map(|(&piece, f)| (self.eta(f), piece))
                    .max_by(|a, b| a.0.total_cmp(&b.0))
                else {
                    break;
                };
                let offset = piece as u64 * self.torrent.piece_size as u64;
                let in_flight = self.in_flight.get_mut(&piece).expect("just looked up");
                let Some((start, end)) = in_flight.missing_span(offset) else {
                    break;
                };
                in_flight.claim_rest(addr);
                tracing::debug!(parent: &in_flight.span, peer = %self.web_seeds[seed].host, "racer joined");
                self.holdings.entry(addr).or_default().insert(piece);
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
    fn start_web_job(&mut self, seed: usize, start: u64, end: u64) {
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

    async fn web_block_arrived(&mut self, seed: usize, job: u64, block: Piece) {
        let w = &mut self.web_seeds[seed];
        w.stats.block_received(block.length as usize, Instant::now());
        self.stat.downloaded += block.length as u64;
        let addr = w.addr;
        let ours = |piece: u32| self.in_flight.get(&piece).is_some_and(|f| f.claims.contains_key(&addr));
        if ours(block.index) {
            self.store_block(addr, block).await;
            return;
        }
        // finished by a racer, or released after a hash failure
        self.stat.wasted += block.length as u64;
        self.bus.emit(Event::BlockWasted {
            info_hash: self.torrent.info_hash,
            addr,
            len: block.length,
            why: "lost race",
        });
        let rest_unwanted = self.web_seeds[seed]
            .jobs
            .get(&job)
            .is_some_and(|j| (block.index..=*j.pieces.end()).all(|p| !ours(p)));
        if rest_unwanted {
            self.web_seeds[seed].jobs.remove(&job);
            self.schedule().await;
        }
    }

    async fn web_job_done(&mut self, seed: usize, job: u64, outcome: Result<(), Failure>) {
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
            if self.in_flight.get(&piece).is_some_and(|f| f.claims.contains_key(&addr)) {
                self.release_claim(piece, addr);
            }
        }
        if let Some(why) = self.web_seeds[seed].gave_up.clone() {
            self.give_up_web_seed(seed, why);
        }
        self.schedule().await;
    }

    /// Stops asking web seed `seed` for anything, and puts what it was fetching back up for
    /// grabs.
    fn give_up_web_seed(&mut self, seed: usize, why: String) {
        let w = &mut self.web_seeds[seed];
        info!("giving up on web seed {}: {why}", w.url);
        w.gave_up = Some(why);
        w.jobs.clear();
        let addr = w.addr;
        for piece in self.pieces_held_by(addr) {
            self.release_claim(piece, addr);
        }
        self.holdings.remove(&addr);
    }

    /// Takes ownership of a handshaken socket. Sends our side of the opening exchange (BEP 10
    /// extended handshake, then BitField/HaveAll/HaveNone, then Interested) before the peer
    /// joins `peers`, so nothing else can be written to it first.
    async fn add_peer(&mut self, connected: ConnectedPeer) {
        let remote_addr = canonical(connected.remote_addr);
        self.dialing.remove(&remote_addr);
        if self.peer_index(remote_addr).is_some() {
            info!("{remote_addr} is already connected, dropping the duplicate");
            return;
        }
        if self.known.get(&remote_addr).is_some_and(|k| k.banned(Instant::now())) {
            info!("{remote_addr} is banned, refusing it");
            return;
        }
        if self.peers.len() >= self.settings.borrow().peer_cap() {
            match self.make_room_for(remote_addr) {
                Some(idx) => self.drop_peer(idx, "replaced by a peer of higher BEP 40 priority"),
                None => {
                    tracing::debug!("{remote_addr} refused, at the connection cap");
                    self.known.entry(remote_addr).or_default().connected();
                    return;
                }
            }
        }
        let known = self.known.entry(remote_addr).or_default();
        known.connected();
        // a dialled peer that came up plaintext under `Prefer` refused the encrypted opening
        if connected.dialed && self.id.encryption == crate::config::Encryption::Prefer {
            known.plaintext_only = !connected.stream.is_encrypted();
        }

        let dht_port = self.dht.borrow().as_ref().map(|dht| dht.udp_port_for(&remote_addr));
        self.next_conn += 1;
        let mut peer = Peer::new(
            connected.stream,
            remote_addr,
            self.torrent.num_pieces(),
            connected.remote_supports_fast,
            connected.peer_id,
            self.next_conn,
            self.inbox.clone(),
        );
        peer.stats = known.stats.clone();
        peer.dialed = connected.dialed;
        peer.v2 = connected.remote_supports_v2;
        let opening = async {
            if connected.remote_supports_extensions {
                peer.send_extended_handshake(
                    self.torrent.metadata_size(),
                    self.torrent.private,
                    self.partial_seed(),
                    self.id.serving.port(),
                )
                .await?;
            }
            // BEP 6: a peer that advertised Fast Extension support accepts HaveAll/HaveNone in
            // place of a BitField for the "everything"/"nothing" cases
            if self.super_seeding() {
                peer.super_seed = Some(Default::default());
                if peer.remote_supports_fast {
                    peer.send_have_none().await?;
                } else {
                    let has = vec![0u8; self.torrent.num_pieces().div_ceil(8)].into_boxed_slice();
                    peer.send_bitfield(BitField { has }).await?;
                }
            } else if peer.remote_supports_fast && self.stat.all_verified() {
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
        peer.span = tracing::info_span!(
            "peer",
            info_hash = %self.torrent.info_hash,
            peer = %remote_addr,
            client = %crate::peer::client_name(&peer.peer_id),
            transport = if peer.utp { "utp" } else { "tcp" },
            encrypted = peer.encrypted,
            dialed = connected.dialed,
            downloaded = tracing::field::Empty,
            uploaded = tracing::field::Empty,
            reason = tracing::field::Empty,
        );
        self.bus.emit(Event::PeerConnected {
            info_hash: self.torrent.info_hash,
            addr: remote_addr,
            client: crate::peer::client_name(&peer.peer_id),
            dialed: connected.dialed,
            encrypted: peer.encrypted,
            utp: peer.utp,
        });
        let insert_at = self.peers.partition_point(|p| p.remote_addr < remote_addr);
        self.peers.insert(insert_at, peer);
        self.reveal_next_piece(insert_at).await;
    }

    /// Dials every address in `peers` we're not already connected to or dialing. Shared by
    /// tracker-discovered peers and BEP 11 (PEX) peers -- both are just addresses.
    fn connect_to_discovered_peers(&mut self, peers: Vec<SocketAddr>) {
        self.connect_to_peers(peers.into_iter().map(|addr| (addr, false)).collect(), None);
    }

    /// Dials what a tracker, the DHT, LSD or PEX handed out, as far as the peer cap allows.
    /// The flag marks an address PEX said speaks uTP; it's remembered only for addresses
    /// that get dialled, so gossip about peers we never call doesn't pile up in `known`.
    fn connect_to_peers(&mut self, mut peers: Vec<(SocketAddr, bool)>, via: Option<SocketAddr>) {
        let now = Instant::now();
        let cap = self.settings.borrow().peer_cap();
        // best BEP 40 rank first: what the cap cuts off, and what waits longest for a
        // half-open slot, is the end of the list
        if let Some(us) = self.our_address() {
            peers.sort_by_cached_key(|(addr, _)| std::cmp::Reverse(crate::priority::peer_priority(us, *addr)));
        }
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
            let known = self.known.entry(addr).or_default();
            if utp_capable {
                known.prefers_utp = true;
            }
            if via.is_some() {
                known.via = via;
            }
            let hints = known.dial_hints();
            self.spawn_dial(addr, hints);
        }
    }

    /// Dials `addr` in the background (one of the `MAX_HALF_OPEN` at a time); the outcome comes
    /// back as `PeerConnected` or `DialFailed`. The caller has put it in `dialing`.
    fn spawn_dial(&self, addr: SocketAddr, hints: DialHints) {
        let events = self.events_tx.clone();
        let torrent = self.torrent.clone();
        let our_id = self.id.clone();
        let utp = self.utp.borrow().clone();
        tokio::spawn(async move {
            let Ok(_permit) = HALF_OPEN.acquire().await else {
                return;
            };
            // from here, not from the queueing above: a dial waiting for a slot isn't dialling
            let span = tracing::info_span!(
                "dial",
                info_hash = %torrent.info_hash,
                peer = %addr,
                holepunch = hints.utp_only,
                transport = tracing::field::Empty,
                encrypted = tracing::field::Empty,
                error = tracing::field::Empty,
            );
            let dialed = dial(addr, &torrent, &our_id, utp, hints).instrument(span.clone()).await;
            let result = match dialed {
                Ok(connected) => {
                    span.record("transport", if connected.stream.is_utp() { "utp" } else { "tcp" });
                    span.record("encrypted", connected.stream.is_encrypted());
                    SwarmEvent::PeerConnected(connected)
                }
                Err(e) => {
                    tracing::debug!("couldn't connect to {addr}: {e:#}");
                    span.record("error", format!("{e:#}"));
                    SwarmEvent::DialFailed(addr)
                }
            };
            drop(span);
            // if the swarm is gone meanwhile, the socket just drops here
            if let Some(events) = events.upgrade() {
                let _ = events.send(result).await;
            }
        });
    }

    /// BEP 55, as the initiator: a peer we couldn't dial may be behind a NAT that only lets in
    /// what it sent out to first. The peer that told us about it is connected to it, so it can
    /// tell both of us to connect at once (over uTP), which opens both NATs. Once per address.
    async fn try_holepunch(&mut self, addr: SocketAddr) {
        let Some(known) = self.known.get_mut(&addr) else {
            return;
        };
        let Some(relay) = known.via.filter(|_| !known.holepunched) else {
            return;
        };
        let Some(idx) = self
            .peer_index(relay)
            .filter(|&idx| self.peers[idx].their_ut_holepunch_id.is_some())
        else {
            return;
        };
        if self.utp.borrow().is_none() {
            return;
        }
        if let Some(known) = self.known.get_mut(&addr) {
            known.holepunched = true;
        }
        tracing::debug!("asking {relay} to introduce us to {addr} (holepunch)");
        if self.peers[idx]
            .send_holepunch(Holepunch::Rendezvous(addr))
            .await
            .is_err()
        {
            self.drop_peer(idx, "send failed");
        }
    }

    /// BEP 55: a holepunch message from the peer at `idx`.
    async fn on_holepunch(&mut self, idx: usize, msg: Holepunch) {
        let from = self.peers[idx].remote_addr;
        match msg {
            // we're the relay: introduce the two, or say why not
            Holepunch::Rendezvous(target) => {
                let target = canonical(target);
                let error = if target == from || target == self.peers[idx].reachable_addr() {
                    Some(HolepunchError::NoSelf)
                } else {
                    match self
                        .peers
                        .iter()
                        .position(|p| p.reachable_addr() == target || p.remote_addr == target)
                    {
                        None => Some(HolepunchError::NotConnected),
                        Some(t) if self.peers[t].their_ut_holepunch_id.is_none() => Some(HolepunchError::NoSupport),
                        Some(t) => {
                            let initiator = self.peers[idx].reachable_addr();
                            tracing::debug!("introducing {from} and {target} (holepunch)");
                            let to_target = self.peers[t].send_holepunch(Holepunch::Connect(initiator)).await;
                            let to_initiator = self.peers[idx].send_holepunch(Holepunch::Connect(target)).await;
                            if to_target.is_err() || to_initiator.is_err() {
                                tracing::debug!("couldn't pass on a holepunch between {from} and {target}");
                            }
                            None
                        }
                    }
                };
                if let Some(error) = error {
                    let _ = self.peers[idx].send_holepunch(Holepunch::Error(target, error)).await;
                }
            }
            // a relay introduced us: dial now, over uTP, while the other side dials us
            Holepunch::Connect(addr) => {
                let addr = canonical(addr);
                if self.utp.borrow().is_none() || self.peer_index(addr).is_some() || !self.dialing.insert(addr) {
                    return;
                }
                let mut hints = self.known.get(&addr).map(KnownPeer::dial_hints).unwrap_or_default();
                hints.utp_only = true;
                tracing::debug!("{from} introduced us to {addr}, dialing (holepunch)");
                self.spawn_dial(addr, hints);
            }
            Holepunch::Error(addr, error) => tracing::debug!("{from} couldn't introduce us to {addr}: {error:?}"),
        }
    }

    /// Tit-for-tat unchoking, run periodically. Ranks interested peers (those who want to
    /// download from us) by the download rate they've been giving us -- reciprocation is the
    /// point -- and unchokes the top MAX_UNCHOKED_PEERS. Every OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS
    /// rounds, one additional peer is unchoked at random so a new or under-rated peer gets a
    /// chance to prove itself instead of the same top N being unchoked forever.
    /// A peer that just declared interest gets a free upload slot now rather than at the next
    /// choking round, up to 10 s away; the round still decides who keeps one.
    async fn unchoke_if_slot_free(&mut self, idx: usize) {
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
        if peer.unchoke().await.is_err() {
            self.drop_peer(idx, "send failed");
        }
    }

    async fn run_choking_algorithm(&mut self, round: u64) {
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
            // BEP 11 recommends capping a single PEX message at roughly 50 added peers; a
            // fresh random sample each round, so over time every peer hears of the whole swarm
            let added: Vec<(SocketAddr, u8)> = all
                .sample(&mut rand::rng(), PEX_MAX_ADDED_PEERS + 1)
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
/// Regular (non-optimistic) upload slots for `interested` peers. A handful of slots reciprocates
/// with only a handful of a big swarm's leechers, and the rest have no reason to send us
/// anything, so the count grows with the square root of the demand.
fn upload_slots(interested: usize) -> usize {
    MAX_UNCHOKED_PEERS.max(interested.isqrt() + 1)
}

/// Who a piece can be handed to.
#[derive(Clone, Copy)]
enum Source {
    Peer(usize),
    Web(usize),
}

/// See `prune_known`. A few thousand is a busy swarm's worth over days.
const KNOWN_PEERS_MAX: usize = 20_000;

/// See `MAX_HALF_OPEN`.
static HALF_OPEN: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(MAX_HALF_OPEN);

async fn dial(
    addr: SocketAddr,
    torrent: &Torrent,
    our_id: &Identity,
    utp: Option<Arc<librqbit_utp::UtpSocketUdp>>,
    hints: DialHints,
) -> anyhow::Result<ConnectedPeer> {
    let (stream, handshake) = tokio::time::timeout(
        crate::settings::HANDSHAKE_TIMEOUT,
        crate::stream::connect(
            addr,
            &torrent.info_hash,
            torrent.v2_support(),
            our_id,
            utp.as_ref(),
            hints,
        ),
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
        remote_supports_v2: torrent.v2_support().v2_peer(&handshake),
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
    use futures::StreamExt;
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
        swarm_with_web_seeds(name, seeding, settings, vec![])
    }

    fn swarm_with_web_seeds(
        name: &str,
        seeding: bool,
        settings: crate::config::Settings,
        web_seeds: Vec<String>,
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
        torrent.web_seeds = web_seeds;
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
        let storage = Arc::new(TorrentStorage::new(torrent.clone(), vec![Some(file)]));
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
            events: EventBus::new(),
            id,
            dht: crate::dht::Dht::none(),
            utp: crate::utp::none(),
            settings: settings_rx.clone(),
            limiter: Arc::new(RateLimiter::new(settings_rx)),
            external: ExternalAddress::default(),
        };
        let (swarm, handle) = TorrentSwarm::new(torrent, storage, verified, shared);
        (swarm, handle, path)
    }

    /// BEP 40 at the connection cap: a newcomer that outranks an idle peer takes its place;
    /// one that doesn't is turned away.
    #[tokio::test]
    async fn at_the_cap_a_higher_priority_peer_replaces_an_idle_one() {
        let settings = crate::config::Settings {
            max_peers_per_torrent: 1,
            ..Default::default()
        };
        let (swarm, handle, path) = swarm_with_settings("bep40", true, settings);
        let ours: std::net::IpAddr = "203.0.113.7".parse().unwrap();
        swarm.external.vote(ours, "a");
        swarm.external.vote(ours, "b");
        let us = SocketAddr::new(ours, 0);
        tokio::spawn(swarm.work_loop());
        let mut candidates: Vec<SocketAddr> = (1..=3)
            .map(|n| format!("198.51.{n}.{n}:6881").parse().unwrap())
            .collect();
        candidates.sort_by_key(|addr| crate::priority::peer_priority(us, *addr));
        let [low, mid, high] = candidates[..] else {
            unreachable!()
        };

        // a closed connection reads as the end of the stream, after whatever was sent first
        async fn closed(peer: &mut Framed<tokio::net::TcpStream, BtCodec>) -> bool {
            let drained = async { while let Some(Ok(_)) = peer.next().await {} };
            tokio::time::timeout(Duration::from_secs(5), drained).await.is_ok()
        }
        let mut first = fake_peer(&handle, &mid.to_string()).await;
        let mut outranked = fake_peer(&handle, &low.to_string()).await;
        assert!(closed(&mut outranked).await, "a lower rank is refused");
        let _better = fake_peer(&handle, &high.to_string()).await;
        assert!(closed(&mut first).await, "a higher rank replaces the idle peer");
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// The bus hears a peer arrive and leave, with the reason.
    #[tokio::test]
    async fn the_bus_hears_peers_come_and_go() {
        let (swarm, handle, path) = swarm("bus");
        let mut events = swarm.bus.subscribe();
        tokio::spawn(swarm.work_loop());
        let mut seeder = fake_peer(&handle, "10.0.0.1:6881").await;
        open_as_seeder(&mut seeder).await;
        drop(seeder);

        let mut saw = Vec::new();
        let wanted = async {
            loop {
                let stamped = events.next().await.expect("the bus is alive");
                match stamped.event {
                    Event::PeerConnected { addr, dialed, .. } => {
                        assert_eq!(addr, "10.0.0.1:6881".parse().unwrap());
                        assert!(!dialed);
                        saw.push("connected");
                    }
                    Event::PeerDisconnected { reason, .. } => {
                        saw.push(reason);
                        break;
                    }
                    _ => {}
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), wanted).await.unwrap();
        assert_eq!(saw, ["connected", "hung up"]);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// Connects a fake remote peer to the swarm: the swarm gets one end of a localhost socket
    /// (as if it had just completed a handshake), the test keeps the other.
    /// BEP 55, as the relay: two peers that speak holepunch are introduced to each other on
    /// one's request, and a request for a peer we don't have gets the matching error.
    #[tokio::test]
    async fn relays_a_holepunch_between_two_peers() {
        let (swarm, handle, path) = swarm_with("holepunch", true);
        tokio::spawn(swarm.work_loop());
        let (a_addr, b_addr): (SocketAddr, SocketAddr) =
            ("10.0.0.1:6881".parse().unwrap(), "10.0.0.2:6881".parse().unwrap());
        let mut a = fake_peer(&handle, &a_addr.to_string()).await;
        let mut b = fake_peer(&handle, &b_addr.to_string()).await;
        let speaks_holepunch = BtMessage::Extended(crate::wire::Extended {
            ext_id: 0,
            payload: Box::from(&b"d1:md12:ut_holepunchi9eee"[..]),
        });
        for peer in [&mut a, &mut b] {
            peer.send(speaks_holepunch.clone()).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let ask = |msg: Holepunch| {
            BtMessage::Extended(crate::wire::Extended {
                ext_id: UT_HOLEPUNCH_ID,
                payload: msg.encode().into_boxed_slice(),
            })
        };
        a.send(ask(Holepunch::Rendezvous(b_addr))).await.unwrap();

        // the first holepunch message each gets, skipping the opening exchange
        async fn next_holepunch(peer: &mut Framed<tokio::net::TcpStream, BtCodec>) -> Holepunch {
            loop {
                if let Some(Ok(BtMessage::Extended(ext))) = peer.next().await
                    && ext.ext_id == 9
                {
                    return Holepunch::decode(&ext.payload).unwrap();
                }
            }
        }
        let timeout = Duration::from_secs(5);
        assert_eq!(
            tokio::time::timeout(timeout, next_holepunch(&mut b)).await.unwrap(),
            Holepunch::Connect(a_addr)
        );
        assert_eq!(
            tokio::time::timeout(timeout, next_holepunch(&mut a)).await.unwrap(),
            Holepunch::Connect(b_addr)
        );

        let stranger: SocketAddr = "10.0.0.9:1".parse().unwrap();
        a.send(ask(Holepunch::Rendezvous(stranger))).await.unwrap();
        assert_eq!(
            tokio::time::timeout(timeout, next_holepunch(&mut a)).await.unwrap(),
            Holepunch::Error(stranger, HolepunchError::NotConnected)
        );
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

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
                remote_supports_v2: false,
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
            handles.push(Some(file));
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
            events: EventBus::new(),
            id,
            dht: crate::dht::Dht::none(),
            utp: crate::utp::none(),
            settings: crate::bt_client::default_settings(),
            limiter: Arc::new(RateLimiter::new(crate::bt_client::default_settings())),
            external: ExternalAddress::default(),
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
                    // BEP 21: done with the selection but not a seed, so upload_only goes out
                    Some(Ok(BtMessage::Extended(ext))) if ext.ext_id == 0 => {
                        assert!(ext.payload.windows(13).any(|w| w == b"11:upload_onl"));
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
            .send(SwarmEvent::PeersDiscovered(
                vec![fruitless_addr, useful_addr],
                PeerSource::Lsd,
            ))
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
    async fn serve_everything(peer: &mut Framed<tokio::net::TcpStream, BtCodec>) {
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

    /// BEP 16: super-seeding hides our pieces, shows each newcomer a different one, shows the
    /// next only once the last has spread, serves only what was shown, and switching it off
    /// reveals the rest.
    #[tokio::test]
    async fn super_seeding_reveals_a_piece_at_a_time() {
        let (swarm, handle, path) = swarm_with("superseed", true);
        tokio::spawn(swarm.work_loop());
        handle.set_super_seed(true).await;

        type Wire = Framed<tokio::net::TcpStream, BtCodec>;
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
        let ask = |index| Request {
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
