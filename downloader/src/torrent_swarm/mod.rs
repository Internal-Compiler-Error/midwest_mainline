use crate::announcer::{Announcing, TrackerStatus, spawn_announcers};
use crate::config::SettingsWatch;
use crate::defs::Identity;
use crate::dht::DhtWatch;
use crate::events::{Event, EventBus, PeerSource};
use crate::external::ExternalAddress;
use crate::layers::LayerFetch;
use crate::limiter::RateLimiter;
use crate::merkle::Hash;
use crate::peer::{Inbox, Incoming, Peer, PeerSnapshot};
use crate::settings::{
    BLOCK_REQUEST_TIMEOUT, CHOKING_ROUND_INTERVAL, KEEPALIVE_INTERVAL, PEER_TIMEOUT, PEX_INTERVAL, SWARM_INBOX,
};
use crate::storage::TorrentStorage;
use crate::stream::PeerStream;
use crate::torrent::Torrent;
use crate::utp::UtpWatch;
use crate::webseed::{Failure, WebSeed};
use crate::wire::{BlockRef, BtMessage, Piece, V2Support};
use bitvec::prelude::*;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::time::interval;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::info;

mod connect;
mod download;
mod holepunch;
mod in_flight;
mod known;
mod messages;
mod pex;
mod schedule;
mod super_seed;
#[cfg(test)]
mod test_support;
mod upload;
mod web_seeds;

use in_flight::InFlightPieces;
use known::KnownPeer;

#[derive(Clone, Debug, PartialEq)]
pub struct TorrentSwarmStats {
    pub uploaded: u64,
    pub downloaded: u64,
    /// bytes received that we already had: endgame races, and stray blocks
    pub wasted: u64,
    /// how many bytes we don't have yet
    pub left: usize,
    /// how many bytes we've written
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
    /// BEP 27: kept out of local service discovery
    pub(crate) private: bool,
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
    /// the client's: its announcers say goodbye when this goes, not only when the swarm does
    pub shutdown: CancellationToken,
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
        block: Result<Piece, BlockRef>,
    },
    /// A completed piece was hashed and, if it was good, written (see `piece_assembled`).
    PieceDone {
        piece: u32,
        senders: BTreeSet<SocketAddr>,
        /// whether it matched its hash, or why it couldn't be written
        outcome: Result<bool, String>,
    },
    /// A dial spawned by `connect_to_peers` failed. Without this the address would
    /// sit in `dialing` forever, and since dedup against re-discovering the same address checks
    /// `dialing`, it could never be retried -- an address a peer keeps re-gossiping over PEX
    /// needs to actually leave the set on failure.
    DialFailed(SocketAddr),
    /// the answer to a hash request below the piece layer, see `answer_from_data`
    HashesRead {
        to: SocketAddr,
        reply: BtMessage,
    },
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
    in_flight: InFlightPieces,
    /// our public address by the votes of peers (`yourip`) and trackers
    external: ExternalAddress,
    /// complete pieces off being hashed and written (see `piece_assembled`)
    hashing: BTreeSet<u32>,
    /// block requests sent to any peer this session, UCB's `t`
    total_picks: usize,
    /// BEP 52: the piece layers a v2 torrent from a magnet still needs from peers; its pieces
    /// can't be checked (so aren't picked) until their file's layer is in
    layers: LayerFetch,
    /// per file, the Merkle tree above its piece layer, built for the first hash request
    hash_trees: BTreeMap<usize, Vec<Vec<Hash>>>,
    /// hash requests being answered from data on disk, at most `MAX_HASH_READS`
    hash_reads: usize,
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
            shutdown,
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
        let announcers = shutdown.child_token();
        let trackers = spawn_announcers(Announcing {
            trackers: torrent.all_trackers(),
            private: torrent.private,
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
            bitfield: hex::encode(stat.verified.as_raw_slice()),
        });
        let handle = TorrentSwarmHandle {
            tx: events_tx,
            stats: stat_rx,
            peers: peers_rx,
            trackers,
            v2: torrent.v2_support(),
            private: torrent.private,
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
            hash_reads: 0,
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
            in_flight: InFlightPieces::default(),
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
                        Some(Ok(msg)) => self.on_peer_message(idx, msg),
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
                    Some(event) => self.process_event(event),
                    // the last TorrentSwarmHandle is gone: this torrent is being dropped
                    None => break,
                },
                _ = housekeeping_ticker.tick() => self.housekeeping(),
                _ = keepalive_ticker.tick() => {
                    self.broadcast(Peer::send_keepalive);
                }
                _ = choking_ticker.tick() => {
                    choking_round += 1;
                    self.run_choking_algorithm(choking_round);
                }
                _ = pex_ticker.tick() => self.run_pex_round(),
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

    fn process_event(&mut self, event: SwarmEvent) {
        match event {
            SwarmEvent::PeersDiscovered(peers, source) => self.peers_discovered(peers, source),
            SwarmEvent::FilesSelected(selected) => self.select_files(&selected),
            SwarmEvent::Sequential(on) => self.sequential = on,
            SwarmEvent::SuperSeed(on) => self.set_super_seed(on),
            SwarmEvent::PeerConnected(connected) => self.add_peer(connected),
            SwarmEvent::BlockRead { to, block } => self.send_block(to, block),
            SwarmEvent::PieceDone {
                piece,
                senders,
                outcome,
            } => self.piece_done(piece, senders, outcome),
            SwarmEvent::HashesRead { to, reply } => self.hashes_read(to, reply),
            SwarmEvent::WebSeedBlock { seed, job, block } => self.web_block_arrived(seed, job, block),
            SwarmEvent::WebSeedDone { seed, job, outcome } => self.web_job_done(seed, job, outcome),
            SwarmEvent::DialFailed(addr) => self.dial_failed(addr),
        }
    }

    /// Wants only the pieces of the selected files from now on. Pieces that stopped being
    /// wanted leave the pile, and ones in flight are allowed to finish; newly wanted pieces
    /// join the pile. Completion and `left` follow the new selection.
    fn select_files(&mut self, selected: &[bool]) {
        self.stat.wanted = self.torrent.wanted_pieces(selected);
        self.missing = self
            .stat
            .wanted
            .iter_ones()
            .filter(|&p| {
                !self.stat.verified[p] && !self.in_flight.contains(p as u32) && !self.hashing.contains(&(p as u32))
            })
            .map(|p| p as u32)
            .collect();
        self.stat.refresh(&self.torrent);
        self.publish_stats();
        self.schedule();
    }

    /// Once a second: time out stalled requests, drop silent peers, keep the request pipeline
    /// full, and publish progress.
    fn housekeeping(&mut self) {
        self.time_out_peers();
        self.request_layers();
        self.schedule();
        self.send_held_uploads();
        self.prune_known();
        self.publish_stats();
        self.sample_peers();
        let peers = self.peers.iter().map(Peer::snapshot).chain(self.web_seed_snapshots());
        let _ = self.peers_snapshot_tx.send(peers.collect());
    }

    /// Drops the peers that have gone silent, and takes back the pieces of those that stopped
    /// delivering (as opposed to disconnecting outright), which would otherwise hold their
    /// slots forever.
    fn time_out_peers(&mut self) {
        let mut stalled = Vec::new();
        let mut silent = Vec::new();
        for (idx, peer) in self.peers.iter().enumerate() {
            if peer.last_received.elapsed() > PEER_TIMEOUT {
                silent.push(idx);
                continue;
            }
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
    }

    /// Whether `piece` can be checked once it's in; only a v2 piece whose file's layer
    /// hasn't arrived can't.
    fn verifiable(&self, piece: u32) -> bool {
        self.layers.is_empty() || self.torrent.can_verify(piece)
    }

    fn ban(&mut self, addr: SocketAddr) {
        self.known.entry(addr).or_default().ban(Instant::now());
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
        for piece in self.in_flight.held_by(peer.remote_addr) {
            self.release_claim(piece, peer.remote_addr);
        }
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
        if self.in_flight.release(piece, peer).is_some() {
            self.put_back(piece);
        }
    }

    /// Puts a piece that's no longer in flight back up for grabs, unless its files have been
    /// deselected meanwhile; `select_files` brings it back if they're selected again.
    fn put_back(&mut self, piece: u32) {
        if self.stat.wanted[piece as usize] {
            self.missing.push(piece);
        }
    }

    /// Sends to every peer, dropping any the send fails for.
    fn broadcast(&mut self, mut send: impl FnMut(&mut Peer) -> io::Result<()>) {
        let mut dead = Vec::new();
        for (idx, peer) in self.peers.iter_mut().enumerate() {
            if send(peer).is_err() {
                dead.push(idx);
            }
        }
        for idx in dead.into_iter().rev() {
            self.drop_peer(idx, "send failed");
        }
    }
}

#[cfg(test)]
mod test {
    use super::test_support::*;
    use super::*;

    /// The client shutting down has the swarm's trackers told event=stopped (BEP 3) at once:
    /// the swarm itself may take longer to go than the client waits for.
    #[tokio::test]
    async fn client_shutdown_says_goodbye_to_the_trackers() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let tracker = format!("http://{}/announce", listener.local_addr().unwrap());
        let (requests, mut requested) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = vec![0; 4096];
                let n = stream.read(&mut request).await.unwrap_or(0);
                let _ = requests.send(String::from_utf8_lossy(&request[..n]).into_owned());
                let body = b"d8:intervali1800e5:peers0:e";
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(body).await;
            }
        });

        let mut torrent = single_file_torrent();
        torrent.announce_tiers = vec![vec![tracker]];
        let dir = std::env::temp_dir().join(format!("downloader-swarm-goodbye-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        torrent.files[0].path = dir.join("swarm.bin");
        let file = std::fs::File::create(&torrent.files[0].path).unwrap();
        file.set_len(TOTAL as u64).unwrap();
        let torrent = Arc::new(torrent);
        let storage = Arc::new(TorrentStorage::new(torrent.clone(), vec![Some(file)]));
        let shared = shared(crate::bt_client::default_settings());
        let client = shared.shutdown.clone();
        let (swarm, handle) =
            TorrentSwarm::new(torrent, storage, bitvec![u8, Msb0; 0; 3].into_boxed_bitslice(), shared);
        tokio::spawn(swarm.work_loop());

        async fn announced(requested: &mut tokio::sync::mpsc::UnboundedReceiver<String>, event: &str) {
            while !requested.recv().await.unwrap().contains(&format!("&event={event} ")) {}
        }
        tokio::time::timeout(Duration::from_secs(5), announced(&mut requested, "started"))
            .await
            .unwrap();
        client.cancel();
        tokio::time::timeout(Duration::from_secs(5), announced(&mut requested, "stopped"))
            .await
            .expect("no event=stopped while the swarm lives on");
        drop(handle);
        std::fs::remove_dir_all(dir).unwrap();
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

    /// Two files over three pieces: `a` is pieces 0 and 1, `b` is pieces 1 and 2. Deselecting
    /// `b` means piece 2 is never asked for and the torrent completes with two pieces; selecting
    /// it again fetches the third.
    #[tokio::test]
    async fn deselected_files_pieces_are_not_requested() {
        let (swarm, handle, dir) = two_file_swarm("select");
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

    /// A piece in flight when its file is deselected may finish, but once its peer lets it go
    /// it isn't put back up for grabs.
    #[tokio::test]
    async fn a_released_piece_of_a_deselected_file_stays_unwanted() {
        let (mut swarm, _handle, dir) = two_file_swarm("deselect-release");
        let (connected, _theirs) = fake_connection("10.0.0.1:6881", true).await;
        let addr = connected.remote_addr;
        swarm.add_peer(connected);
        swarm.on_peer_message(0, BtMessage::HaveAll(crate::wire::HaveAll));
        swarm.on_peer_message(0, BtMessage::Unchoke(crate::wire::Unchoke));
        // two of the three pieces fill the peer's first window: 0 or 2 (each in one file only)
        // is among them
        let held = swarm.in_flight.held_by(addr);
        let (unwanted, selection) = if held.contains(&2) {
            (2, vec![true, false])
        } else {
            (0, vec![false, true])
        };
        assert!(held.contains(&unwanted), "{held:?}");

        swarm.select_files(&selection);
        swarm.on_peer_message(0, BtMessage::Choke(crate::wire::Choke));
        assert!(swarm.in_flight.held_by(addr).is_empty(), "a choke releases its pieces");
        assert!(
            !swarm.missing.contains(&unwanted),
            "piece {unwanted} is to be downloaded again: {:?}",
            swarm.missing
        );
        std::fs::remove_dir_all(dir).unwrap();
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
}
