use crate::announcer::spawn_announcers;
use crate::defs::Identity;
use crate::peer::{
    Peer, PeerStatistics, ProtocolViolation, UT_METADATA_ID, UT_PEX_ID, parse_pex_message, parse_ut_metadata_request,
};
use crate::settings::{
    BAD_PEER_BAN, BLOCK_REQUEST_TIMEOUT, BLOCK_SIZE, CHOKING_ROUND_INTERVAL, DIAL_BACKOFF, DIAL_BACKOFF_MAX,
    FRUITLESS_PEER_COOLDOWN, KEEPALIVE_INTERVAL, MAX_INFLIGHT_BYTES, MAX_UNCHOKED_PEERS, METADATA_PIECE_SIZE,
    OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS, PEER_TIMEOUT, PEX_INTERVAL, PEX_MAX_ADDED_PEERS,
};
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use crate::wire::{BitField, BtMessage, Piece, Request, shake_hands};
use anyhow::Context;
use bitvec::prelude::*;
use futures::StreamExt;
use futures::future::select_all;
use rand::seq::IndexedRandom;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::interval;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{info, warn};

#[derive(Clone, Debug, PartialEq)]
pub struct TorrentSwarmStats {
    pub uploaded: u64,
    pub downloaded: u64,
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

    pub completed: bool,
}

impl TorrentSwarmStats {
    pub fn verified_cnt(&self) -> usize {
        self.verified.count_ones()
    }

    pub fn total_pieces(&self) -> usize {
        self.verified.len()
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
}

impl TorrentSwarmHandle {
    /// A live view of the torrent's aggregate progress (uploaded/downloaded/left/written/
    /// verified/completed). Keeps updating for as long as the swarm runs, and outlives this
    /// handle -- it only depends on the channel, not on the swarm still being reachable.
    pub fn stats(&self) -> watch::Receiver<TorrentSwarmStats> {
        self.stats.clone()
    }

    /// Hands a freshly handshaken socket to the swarm, which owns it from here on. Used by both
    /// the inbound listener (`BtClient::accept_incoming`) and the swarm's own dial tasks.
    pub(crate) async fn peer_connected(&self, connected: ConnectedPeer) {
        let _ = self.tx.send(SwarmEvent::PeerConnected(connected)).await;
    }
}

/// A socket that has completed the BitTorrent handshake and is ready to become a `Peer`.
pub(crate) struct ConnectedPeer {
    pub tcp: TcpStream,
    pub remote_addr: SocketAddr,
    pub remote_supports_extensions: bool,
    pub remote_supports_fast: bool,
}

/// Everything that reaches the swarm's event loop from outside it: things that happened in
/// tasks it doesn't poll itself (tracker announcers, dial tasks, the inbound listener). The
/// swarm decides what to do about each; the sender never asks it for anything.
pub(crate) enum SwarmEvent {
    /// a tracker answered with peers
    PeersDiscovered(Vec<SocketAddr>),
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

/// A piece we're in the middle of downloading, from exactly one peer. Its blocks are tracked as
/// `Peer::requested` entries on that peer; this holds the bytes as they land.
struct InFlight {
    peer: SocketAddr,
    buf: Vec<u8>,
    blocks_left: usize,
    /// offset of the first block not yet requested; blocks are requested in order as the
    /// peer's window allows, not all at once
    next_begin: usize,
}

impl InFlight {
    fn next_request(&mut self, piece: u32) -> Option<Request> {
        let size = self.buf.len();
        if self.next_begin >= size {
            return None;
        }
        let begin = self.next_begin;
        self.next_begin += BLOCK_SIZE;
        Some(Request {
            index: piece,
            begin: begin as u32,
            length: (size - begin).min(BLOCK_SIZE) as u32,
        })
    }

    fn unrequested_blocks(&self) -> usize {
        self.buf.len().saturating_sub(self.next_begin).div_ceil(BLOCK_SIZE)
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

    events_rx: mpsc::Receiver<SwarmEvent>,
    /// for the tasks the swarm spawns for itself (announcers, dials, block reads) to report
    /// back on; weak so they can't keep the swarm alive, only a `TorrentSwarmHandle` can
    events_tx: mpsc::WeakSender<SwarmEvent>,

    /// pieces neither verified nor in flight
    missing: Vec<u32>,
    in_flight: BTreeMap<u32, InFlight>,
    /// block requests sent to any peer this session, UCB's `t`
    total_picks: usize,

    stat: TorrentSwarmStats,
    stat_snapshot_tx: watch::Sender<TorrentSwarmStats>,

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
        id: Arc<Identity>,
        verified: BitBox<u8, Msb0>,
    ) -> TorrentSwarmHandle {
        let (swarm, handle) = Self::new(torrent, storage, id, verified);
        tokio::spawn(swarm.work_loop());
        handle
    }

    fn new(
        torrent: Arc<Torrent>,
        storage: Arc<TorrentStorage>,
        id: Arc<Identity>,
        verified: BitBox<u8, Msb0>,
    ) -> (TorrentSwarm, TorrentSwarmHandle) {
        assert_eq!(
            verified.len(),
            torrent.pieces.len(),
            "verified bitfield must have one bit per piece"
        );
        let written: usize = verified
            .iter_ones()
            .map(|p| torrent.nth_piece_size(p as u32).expect("index came from the bitfield"))
            .sum();
        let missing = verified.iter_zeros().map(|p| p as u32).collect();
        let stat = TorrentSwarmStats {
            uploaded: 0,
            downloaded: 0,
            left: torrent.total_size as usize - written,
            written,
            completed: verified.all(),
            verified,
        };
        let (stat_tx, stat_rx) = watch::channel(stat.clone());

        let (events_tx, events_rx) = mpsc::channel(512);
        let handle = TorrentSwarmHandle {
            tx: events_tx,
            stats: stat_rx.clone(),
        };
        let events_tx = handle.tx.downgrade();
        let announcers = CancellationToken::new();
        spawn_announcers(
            &torrent.all_trackers(),
            torrent.info_hash,
            id.clone(),
            stat_rx,
            events_tx.clone(),
            announcers.clone(),
        );

        let swarm = TorrentSwarm {
            peers: vec![],
            dialing: BTreeSet::new(),
            known: BTreeMap::new(),
            poll_offset: 0,
            torrent,
            storage,
            id,
            events_rx,
            events_tx,
            missing,
            in_flight: BTreeMap::new(),
            total_picks: 0,
            stat,
            stat_snapshot_tx: stat_tx,
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
                stalled.extend(peer.requested.keys().map(|req| req.index));
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
        for piece in stalled {
            info!("piece {piece} stalled, will retry");
            self.fail_piece(piece);
        }

        self.schedule().await;
        self.publish_stats();
    }

    fn ban(&mut self, addr: SocketAddr) {
        self.known.entry(addr).or_default().ban(Instant::now());
    }

    fn pieces_held_by(&self, addr: SocketAddr) -> Vec<u32> {
        self.in_flight
            .iter()
            .filter(|(_, f)| f.peer == addr)
            .map(|(piece, _)| *piece)
            .collect()
    }

    fn peer_index(&self, addr: SocketAddr) -> Option<usize> {
        self.peers.binary_search_by_key(&addr, |p| p.remote_addr).ok()
    }

    /// Removes a peer and puts whatever it was downloading for us back up for grabs.
    fn drop_peer(&mut self, idx: usize) {
        let peer = self.peers.remove(idx);
        self.known
            .entry(peer.remote_addr)
            .or_default()
            .disconnected(&peer.stats, Instant::now());
        for piece in self.pieces_held_by(peer.remote_addr) {
            self.in_flight.remove(&piece);
            self.missing.push(piece);
        }
        info!("{} disconnected, {} peers left", peer.remote_addr, self.peers.len());
    }

    /// Gives up on an in-flight piece: it goes back to `missing` and the peer's outstanding
    /// requests for it are forgotten. A block for it arriving later is ignored, not a protocol
    /// violation -- that's just a slow but honest peer.
    fn fail_piece(&mut self, piece: u32) {
        let Some(in_flight) = self.in_flight.remove(&piece) else {
            return;
        };
        if let Some(idx) = self.peer_index(in_flight.peer) {
            self.peers[idx].requested.retain(|req, _| req.index != piece);
        }
        self.missing.push(piece);
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
                        self.fail_piece(piece);
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
                        "{} rejected {req:?}, piece {} goes back on the pile",
                        peer.remote_addr,
                        req.index
                    );
                    self.fail_piece(req.index);
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
                    self.connect_to_discovered_peers(parse_pex_message(&ext.payload));
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
        if peer.block_received(&block).is_none() {
            tracing::debug!("{} sent a block we weren't waiting for, ignoring", peer.remote_addr);
            return;
        }
        self.stat.downloaded += block.length as u64;

        let Some(in_flight) = self.in_flight.get_mut(&block.index) else {
            return;
        };
        let begin = block.begin as usize;
        let end = begin + block.data.len();
        // the block length is remote-controlled; a mismatch must not panic via copy_from_slice
        if block.data.len() != block.length as usize || end > in_flight.buf.len() {
            warn!(
                "{} sent a malformed block for piece {}, giving up on it",
                peer.remote_addr, block.index
            );
            self.fail_piece(block.index);
            return;
        }
        in_flight.buf[begin..end].copy_from_slice(&block.data);
        in_flight.blocks_left -= 1;
        if in_flight.blocks_left > 0 {
            self.refill(idx).await;
            return;
        }

        let piece = block.index;
        let InFlight { peer: from, buf, .. } = self.in_flight.remove(&piece).expect("checked above");
        if let Err(e) = self.storage.write_piece(piece, buf.into_boxed_slice()) {
            warn!("couldn't write piece {piece}: {e:#}");
            self.missing.push(piece);
            return;
        }
        match self.verify_hash(piece) {
            Some(true) => {}
            Some(false) => {
                warn!("piece {piece} from {from} failed hash verification, banning the peer");
                self.missing.push(piece);
                if let Some(idx) = self.peer_index(from) {
                    self.drop_peer(idx);
                }
                self.ban(from);
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
        self.stat.left = self.torrent.total_size as usize - self.stat.written;
        self.stat.completed = self.stat.all_verified();
        // announcers watch this to send a prompt event=completed rather than waiting for
        // their next periodic announce, which could be minutes away
        self.publish_stats();

        self.broadcast(move |peer| Box::pin(peer.send_have(piece))).await;
        self.schedule().await;
    }

    /// Keeps up to `MAX_INFLIGHT_BYTES` of pieces on the wire. Rarest-first picks *which piece*
    /// to go after next; UCB scoring separately picks *which peer* to ask -- the two compose
    /// rather than compete. A piece is assigned whole to one peer, but its blocks are only
    /// requested as that peer's window allows (see `refill`).
    async fn schedule(&mut self) {
        loop {
            let in_flight: usize = self.in_flight.values().map(|f| f.buf.len()).sum();
            if in_flight >= MAX_INFLIGHT_BYTES {
                break;
            }
            let Some(piece) = self.rarest_piece() else {
                break;
            };
            let Some(idx) = self.best_peer(piece) else {
                break;
            };

            let size = self.torrent.nth_piece_size(piece).expect("piece index in range");
            self.missing.retain(|p| *p != piece);
            self.in_flight.insert(
                piece,
                InFlight {
                    peer: self.peers[idx].remote_addr,
                    buf: vec![0u8; size],
                    blocks_left: size.div_ceil(BLOCK_SIZE),
                    next_begin: 0,
                },
            );
            info!(
                "requesting piece {piece} from {} (window {})",
                self.peers[idx].remote_addr,
                self.peers[idx].request_window()
            );
        }
        for idx in (0..self.peers.len()).rev() {
            self.refill(idx).await;
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
        'pieces: for (&piece, f) in self.in_flight.iter_mut().filter(|(_, f)| f.peer == addr) {
            while room > 0 {
                let Some(req) = f.next_request(piece) else {
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

    /// Rarest-first piece selection: among the missing pieces, the one held by the fewest
    /// connected peers (ties broken randomly, so many peers starting at once don't all pile
    /// onto the same single rarest piece). `None` if no connected peer has any of them.
    fn rarest_piece(&self) -> Option<u32> {
        let availability = |piece: u32| self.peers.iter().filter(|p| p.they_have(piece)).count();

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

    /// UCB peer selection: of the peers that have `piece`, aren't choking us, and have room
    /// in their request window for more work, the one with the highest upper confidence
    /// bound on its download speed.
    fn best_peer(&self, piece: u32) -> Option<usize> {
        let rate_scale = self.peers.iter().map(|p| p.stats.rx_rate).fold(1.0, f64::max);
        let mut backlog: BTreeMap<SocketAddr, usize> = BTreeMap::new();
        for f in self.in_flight.values() {
            *backlog.entry(f.peer).or_default() += f.unrequested_blocks();
        }
        let has_room =
            |p: &Peer| p.requested.len() + backlog.get(&p.remote_addr).copied().unwrap_or(0) < p.request_window();
        self.peers
            .iter()
            .enumerate()
            .filter(|(_, p)| p.ready() && p.they_have(piece) && has_room(p))
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

        let mut peer = Peer::new(
            connected.tcp,
            remote_addr,
            self.torrent.pieces.len(),
            connected.remote_supports_fast,
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
        let now = Instant::now();
        for addr in peers.into_iter().map(canonical) {
            let worth_it = self.known.get(&addr).is_none_or(|k| k.may_dial(now));
            if !worth_it || self.peer_index(addr).is_some() || !self.dialing.insert(addr) {
                continue;
            }
            let events = self.events_tx.clone();
            let torrent = self.torrent.clone();
            let our_id = self.id.clone();
            tokio::spawn(async move {
                let result = match dial(addr, &torrent, &our_id).await {
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

        if round % OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS == 0 {
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

        let all_addrs: Vec<SocketAddr> = self.peers.iter().map(|p| p.remote_addr).collect();
        self.broadcast(move |peer| {
            // BEP 11 recommends capping a single PEX message at roughly 50 added peers
            let added: Vec<SocketAddr> = all_addrs
                .iter()
                .copied()
                .filter(|a| *a != peer.remote_addr)
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

async fn dial(addr: SocketAddr, torrent: &Torrent, our_id: &Identity) -> anyhow::Result<ConnectedPeer> {
    let mut tcp = crate::wire::connect(addr)
        .await
        .with_context(|| format!("Failed to establish tcp stream with {addr}"))?;
    let handshake = shake_hands(&mut tcp, &torrent.info_hash, &our_id.peer_id)
        .await
        .with_context(|| format!("Failed to complete handshake with {addr}"))?;
    info!("Peer connection to {addr} established");
    Ok(ConnectedPeer {
        tcp,
        remote_addr: addr,
        remote_supports_extensions: handshake.supports_extensions(),
        remote_supports_fast: handshake.supports_fast_extension(),
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
        });
        let verified = bitvec![u8, Msb0; seeding as u8; 3].into_boxed_bitslice();
        let (swarm, handle) = TorrentSwarm::new(torrent, storage, id, verified);
        (swarm, handle, path)
    }

    /// Connects a fake remote peer to the swarm: the swarm gets one end of a localhost socket
    /// (as if it had just completed a handshake), the test keeps the other.
    async fn fake_peer(handle: &TorrentSwarmHandle, pretend_addr: &str) -> Framed<TcpStream, BtCodec> {
        fake_peer_with(handle, pretend_addr, false).await
    }

    async fn fake_peer_with(handle: &TorrentSwarmHandle, pretend_addr: &str, fast: bool) -> Framed<TcpStream, BtCodec> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let ours = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (theirs, _) = listener.accept().await.unwrap();
        handle
            .peer_connected(ConnectedPeer {
                tcp: ours,
                remote_addr: pretend_addr.parse().unwrap(),
                remote_supports_extensions: false,
                remote_supports_fast: fast,
            })
            .await;
        Framed::new(theirs, BtCodec)
    }

    /// The fake peer's side of the opening exchange: it expects our BitField and Interested,
    /// then declares it has everything and unchokes us.
    async fn open_as_seeder(peer: &mut Framed<TcpStream, BtCodec>) {
        open_with(peer, 0xFF).await;
    }

    /// Like `open_as_seeder`, but the peer declares only the pieces set in `bitfield`.
    async fn open_with(peer: &mut Framed<TcpStream, BtCodec>, bitfield: u8) {
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
