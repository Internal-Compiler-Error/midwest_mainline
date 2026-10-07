use crate::settings::{
    BLOCK_SIZE, MAX_QUEUED_UPLOADS, MAX_REQUEST_WINDOW, MIN_REQUEST_WINDOW, PEER_OUTBOX, RATE_WINDOW,
    REQUEST_PIPELINE_TARGET, WRITE_TIMEOUT,
};
use crate::stream::PeerStream;
use crate::wire::{
    BitField, BtCodec, BtMessage, Cancel, Choke, Extended, Have, HaveAll, HaveNone, Interested, KeepAlive, Piece, Port,
    RejectRequest, Request, Unchoke,
};
use bitvec::prelude::*;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use juicy_bencode::BencodeItemView;
use midwest_mainline::message::{compact_addr, parse_compact_addr};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::AbortHandle;
use tokio_util::codec::Framed;

/// The BEP 10 extensions we speak. Each one's value is the message id we ask peers to send it
/// to us on: entirely our own choice, only the ids peers want from us are negotiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Extension {
    /// BEP 9
    UtMetadata = 1,
    /// BEP 11
    UtPex = 2,
    /// BEP 54; we never drop a piece, so we only ever receive it
    LtDonthave = 3,
    /// BEP 55
    UtHolepunch = 4,
}

impl Extension {
    /// In name order, which is the order bencode wants them in in the handshake's `m`.
    const ALL: [Extension; 4] = [
        Extension::LtDonthave,
        Extension::UtHolepunch,
        Extension::UtMetadata,
        Extension::UtPex,
    ];

    pub fn id(self) -> u8 {
        self as u8
    }

    /// Which extension a message sent to us on `id` is.
    pub fn from_id(id: u8) -> Option<Extension> {
        Self::ALL.into_iter().find(|e| e.id() == id)
    }

    fn name(self) -> &'static str {
        match self {
            Extension::UtMetadata => "ut_metadata",
            Extension::UtPex => "ut_pex",
            Extension::LtDonthave => "lt_donthave",
            Extension::UtHolepunch => "ut_holepunch",
        }
    }
}

/// BEP 55 messages: ask a peer we share with `addr` to introduce us (`Rendezvous`), be told
/// to connect to `addr` now (`Connect`), or hear why an introduction failed (`Error`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Holepunch {
    Rendezvous(SocketAddr),
    Connect(SocketAddr),
    Error(SocketAddr, HolepunchError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HolepunchError {
    NoSuchPeer = 1,
    NotConnected = 2,
    NoSupport = 3,
    NoSelf = 4,
}

impl Holepunch {
    pub fn encode(&self) -> Vec<u8> {
        let (kind, addr, error) = match *self {
            Holepunch::Rendezvous(addr) => (0u8, addr, None),
            Holepunch::Connect(addr) => (1, addr, None),
            Holepunch::Error(addr, e) => (2, addr, Some(e as u32)),
        };
        let mut out = vec![kind];
        match addr.ip().to_canonical() {
            IpAddr::V4(ip) => {
                out.push(0);
                out.extend_from_slice(&ip.octets());
            }
            IpAddr::V6(ip) => {
                out.push(1);
                out.extend_from_slice(&ip.octets());
            }
        }
        out.extend_from_slice(&addr.port().to_be_bytes());
        out.extend(error.into_iter().flat_map(u32::to_be_bytes));
        out
    }

    pub fn decode(payload: &[u8]) -> Option<Holepunch> {
        let (&kind, rest) = payload.split_first()?;
        let (&family, rest) = rest.split_first()?;
        let (ip, rest): (IpAddr, &[u8]) = match family {
            0 => {
                let (ip, rest) = rest.split_first_chunk::<4>()?;
                ((*ip).into(), rest)
            }
            1 => {
                let (ip, rest) = rest.split_first_chunk::<16>()?;
                ((*ip).into(), rest)
            }
            _ => return None,
        };
        let (port, rest) = rest.split_first_chunk::<2>()?;
        let addr = SocketAddr::new(ip, u16::from_be_bytes(*port));
        Some(match kind {
            0 => Holepunch::Rendezvous(addr),
            1 => Holepunch::Connect(addr),
            2 => {
                let code = u32::from_be_bytes(*rest.first_chunk::<4>()?);
                let error = match code {
                    1 => HolepunchError::NoSuchPeer,
                    2 => HolepunchError::NotConnected,
                    3 => HolepunchError::NoSupport,
                    4 => HolepunchError::NoSelf,
                    _ => return None,
                };
                Holepunch::Error(addr, error)
            }
            _ => return None,
        })
    }
}

/// What a peer's reader (or a failing writer) hands the swarm: the next message, `None` when the
/// peer hung up, or the error that ended the connection. `conn` tells this connection apart from
/// a later one to the same address, whose messages must not be mixed up with a dead one's.
pub(crate) struct Incoming {
    pub addr: SocketAddr,
    pub conn: u64,
    pub msg: Option<io::Result<BtMessage>>,
}

pub(crate) type Inbox = mpsc::Sender<Incoming>;

/// BEP 16: the pieces a super-seed has revealed to one peer, and the one it waits to see
/// spread before revealing another.
#[derive(Debug, Default)]
pub(crate) struct SuperSeedView {
    pub offered: BTreeSet<u32>,
    /// the latest piece shown, how many other peers had it then, and when it was shown
    pub current: Option<(u32, u32, Instant)>,
}

/// One connected peer, owned by its `TorrentSwarm`. The socket itself belongs to two tasks: a
/// reader that forwards each message to the swarm's inbox, and a writer that drains this peer's
/// outbox, batching whatever is queued into one flush. The swarm never waits on a socket, so a
/// slow or stalled peer can't hold up the others; sends here only queue (they're plain
/// functions, not async), and a peer that lets its queue fill up is disconnected.
///
/// What lives here is per-connection state and the wire-level sends. Anything that needs the
/// rest of the swarm (serving a `Request`, assembling a piece, dialing PEX peers) is in
/// `TorrentSwarm`.
pub(crate) struct Peer {
    pub remote_addr: SocketAddr,
    /// BEP 6: whether the remote advertised Fast Extension support in its handshake. A fixed
    /// fact about the connection, unlike the ids below which are negotiated later.
    pub remote_supports_fast: bool,
    /// the connection is MSE-obfuscated; shown to the user, decides nothing
    pub encrypted: bool,
    /// BEP 52: it can take hash requests (a v2 torrent's peer, or a hybrid's that supports v2)
    pub v2: bool,
    /// same for running over uTP
    pub utp: bool,
    /// per `Extension`, the message id the remote wants it sent on, from its extended
    /// handshake; `None` until then, or if it doesn't speak that one
    their_ids: [Option<u8>; Extension::ALL.len()],
    /// BEP 21: the peer says it won't download anything more (a partial seed)
    pub upload_only: bool,
    /// BEP 10 `p`: the port the peer listens on, which an inbound TCP connection's own port isn't
    pub listen_port: Option<u16>,
    /// BEP 10 `reqq`: how many requests the peer queues; past it, it rejects or drops them
    pub their_reqq: Option<usize>,
    /// we dialed it, so `remote_addr` is where it listens
    pub dialed: bool,
    /// BEP 16: what super-seeding has shown this peer; `None` when it saw our real bitfield
    pub super_seed: Option<SuperSeedView>,
    /// BEP 10 `yourip`: our address as the peer sees it, until the swarm takes it to vote with
    pub yourip: Option<IpAddr>,
    /// BEP 5: it told us where its DHT node listens
    dht_port: Option<u16>,

    /// identifies this connection in `Incoming`
    pub conn: u64,
    outbox: mpsc::Sender<BtMessage>,
    io_tasks: [AbortHandle; 2],
    /// the connection's lifetime, for the traces; set by the swarm once the peer is in
    pub span: tracing::Span,

    /// the pieces it has, as its bitfield and Have messages say; one bit per piece
    has: BitBox<u8, Msb0>,

    /// We choked the peer, i.e. we won't send data until we unchoke them
    pub choked_them: bool,
    /// The peer choked us, i.e. they won't send data until they unchoke us
    pub choked_us: bool,
    /// We are interested in them, i.e. they have something we want
    pub interested_them: bool,
    /// They are interested in us, i.e. they want something from us
    pub interested_us: bool,

    /// blocks we've asked this peer for and haven't received yet, with when we asked
    pub requested: BTreeMap<Request, Instant>,
    /// blocks the peer asked us for that we've accepted and not sent yet; a Cancel takes one
    /// out, and a block read for a request no longer here isn't sent
    uploads: BTreeSet<Request>,
    /// the bytes `uploads` asks for
    upload_bytes: usize,
    /// bytes of blocks queued for the writer and not yet written
    unsent: Arc<AtomicUsize>,
    /// the last time this peer delivered a block, or was first asked for one after being idle;
    /// what "stalled" is measured from (see `stalled`)
    last_progress: Instant,
    /// bumped on every inbound message (including keep-alives); a peer that sends nothing for
    /// PEER_TIMEOUT is considered dead
    pub last_received: Instant,

    pub stats: PeerStatistics,
    /// the id the peer sent in its handshake, for naming its client
    pub peer_id: [u8; 20],
}

/// One peer as a front end shows it, taken by the swarm once a second.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerSnapshot {
    pub addr: SocketAddr,
    pub client: String,
    /// the share of the torrent's pieces the peer has, in 0.0..=1.0
    pub progress: f32,
    pub downloaded: u64,
    pub uploaded: u64,
    /// download throughput measured by `PeerStatistics::rx_rate`
    pub download_bps: f64,
    pub choked_us: bool,
    pub choked_them: bool,
    pub interested_us: bool,
    pub interested_them: bool,
    /// blocks requested from the peer and not yet delivered
    pub outstanding: usize,
    pub encrypted: bool,
    pub utp: bool,
    /// the URL, when this is a BEP 19 web seed rather than a peer (`addr` is then a stand-in)
    pub web_seed: Option<String>,
}

/// A human-readable client name from a peer id. Azureus-style ids (`-XX1234-...`) name the
/// client by a two-letter code and its version; Shadow-style (`X1234---...`) by one letter;
/// anything else is shown as its printable prefix.
pub fn client_name(peer_id: &[u8; 20]) -> String {
    let printable = |b: &u8| b.is_ascii_graphic();
    if peer_id[0] == b'-' && peer_id[7] == b'-' && peer_id[1..7].iter().all(printable) {
        let code = &peer_id[1..3];
        let name = match code {
            b"qB" => "qBittorrent",
            b"TR" => "Transmission",
            b"UT" | b"\xb5T" => "\u{b5}Torrent",
            b"UM" => "\u{b5}Torrent Mac",
            b"UW" => "\u{b5}Torrent Web",
            b"LT" => "libtorrent",
            b"lt" => "libtorrent",
            b"DE" => "Deluge",
            b"AZ" => "Vuze",
            b"BC" => "BitComet",
            b"BT" => "BitTorrent",
            b"BW" => "BitTorrent Web",
            b"XL" => "Xunlei",
            b"SD" => "Xunlei",
            b"TX" => "Tixati",
            b"FC" => "FileCroc",
            b"FD" => "Free Download Manager",
            b"aD" => "aria2",
            b"A2" => "aria2",
            b"RT" => "rtorrent",
            b"WW" => "WebTorrent",
            b"WD" => "WebTorrent Desktop",
            b"MT" => "MoonlightTorrent",
            b"PI" => "PicoTorrent",
            b"BI" => "BiglyBT",
            b"DL" => "downloader",
            _ => "",
        };
        let version: String = peer_id[3..7]
            .iter()
            .map(|&b| char::from(b))
            .filter(|c| c.is_ascii_alphanumeric())
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(".");
        if name.is_empty() {
            return format!("{} {version}", String::from_utf8_lossy(code));
        }
        return format!("{name} {version}");
    }
    if peer_id[0].is_ascii_uppercase() && peer_id[1..5].iter().all(|b| b.is_ascii_alphanumeric()) && peer_id[5] == b'-'
    {
        let name = match peer_id[0] {
            b'A' => "ABC",
            b'O' => "Osprey",
            b'Q' => "BTQueue",
            b'R' => "Tribler",
            b'S' => "Shad0w",
            b'T' => "BitTornado",
            b'U' => "UPnP NAT Bit Torrent",
            _ => "",
        };
        let version = String::from_utf8_lossy(&peer_id[1..5])
            .trim_end_matches('-')
            .to_string();
        if !name.is_empty() {
            return format!("{name} {version}");
        }
    }
    let prefix: String = peer_id
        .iter()
        .take(8)
        .map(|&b| if printable(&b) { char::from(b) } else { '.' })
        .collect();
    prefix
}

/// The remote broke the protocol (an out-of-range `Have`, a wrong-sized bitfield); the swarm
/// drops the connection rather than risk an out-of-bounds index later.
#[derive(Debug)]
pub(crate) struct ProtocolViolation(pub String);

impl Peer {
    /// Starts the connection's reader and writer tasks on the current runtime; the reader
    /// delivers to `inbox` tagged with `conn`.
    pub fn new(
        stream: PeerStream,
        remote_addr: SocketAddr,
        num_pieces: usize,
        remote_supports_fast: bool,
        peer_id: [u8; 20],
        conn: u64,
        inbox: Inbox,
    ) -> Self {
        let encrypted = stream.is_encrypted();
        let utp = stream.is_utp();
        // room for a whole block and then some, so a 16 KiB Piece doesn't grow the buffer
        let (sink, source) = Framed::with_capacity(stream, BtCodec, 64 * 1024).split();
        let (outbox, queued) = mpsc::channel(PEER_OUTBOX);
        let unsent = Arc::new(AtomicUsize::new(0));
        let writer = tokio::spawn(write_loop(
            sink,
            queued,
            unsent.clone(),
            remote_addr,
            conn,
            inbox.clone(),
        ));
        let reader = tokio::spawn(read_loop(source, remote_addr, conn, inbox));
        Self {
            peer_id,
            remote_addr,
            remote_supports_fast,
            encrypted,
            v2: false,
            utp,
            their_ids: [None; Extension::ALL.len()],
            upload_only: false,
            yourip: None,
            dht_port: None,
            listen_port: None,
            their_reqq: None,
            dialed: false,
            super_seed: None,
            conn,
            outbox,
            io_tasks: [reader.abort_handle(), writer.abort_handle()],
            span: tracing::Span::none(),
            has: bitbox![u8, Msb0; 0; num_pieces],
            // BEP 3: "At the start of the connection, both sides ... are choked."
            choked_them: true,
            choked_us: true,
            interested_them: false,
            interested_us: false,
            requested: BTreeMap::new(),
            uploads: BTreeSet::new(),
            upload_bytes: 0,
            unsent,
            last_progress: Instant::now(),
            last_received: Instant::now(),
            stats: PeerStatistics::default(),
        }
    }

    pub fn they_have(&self, piece: u32) -> bool {
        self.has.get(piece as usize).is_some_and(|b| *b)
    }

    /// The pieces this peer has, as far as its bitfield and Have messages say.
    pub fn pieces(&self) -> impl Iterator<Item = u32> + '_ {
        self.has.iter_ones().map(|p| p as u32)
    }

    /// The message id the peer wants `ext` sent on, if it speaks it.
    pub fn their_id(&self, ext: Extension) -> Option<u8> {
        self.their_ids[ext as usize - 1]
    }

    /// The peer has sent us data on this connection or an earlier one, so it's a known
    /// quantity rather than an arm still to be explored.
    pub fn proven(&self) -> bool {
        self.stats.received > 0
    }

    /// Every piece, as far as its bitfield and Have messages say.
    pub fn is_seed(&self) -> bool {
        !self.has.is_empty() && self.has.all()
    }

    /// BEP 11 "added.f" flags for gossiping this peer to others.
    pub fn pex_flags(&self) -> u8 {
        let mut flags = 0;
        if self.encrypted {
            flags |= PEX_PREFERS_ENCRYPTION;
        }
        // BEP 11: "seed/upload_only"
        if self.is_seed() || self.upload_only {
            flags |= PEX_SEED;
        }
        if self.utp {
            flags |= PEX_UTP;
        }
        flags
    }

    /// Is the peer ready for more requests?
    pub fn ready(&self) -> bool {
        !self.choked_us
    }

    /// Requests to keep outstanding: what the measured rate calls for, within what the peer
    /// says it will queue.
    pub fn request_window(&self) -> usize {
        let ours = self.stats.request_window();
        self.their_reqq.map_or(ours, |reqq| ours.min(reqq.max(1)))
    }

    pub fn snapshot(&self) -> PeerSnapshot {
        PeerSnapshot {
            addr: self.remote_addr,
            client: client_name(&self.peer_id),
            progress: if self.has.is_empty() {
                0.0
            } else {
                self.has.count_ones() as f32 / self.has.len() as f32
            },
            downloaded: self.stats.received as u64,
            uploaded: self.stats.sent as u64,
            download_bps: self.stats.rx_rate,
            choked_us: self.choked_us,
            choked_them: self.choked_them,
            interested_us: self.interested_us,
            interested_them: self.interested_them,
            outstanding: self.requested.len(),
            encrypted: self.encrypted,
            utp: self.utp,
            web_seed: None,
        }
    }

    /// Applies a message that only touches this connection's own state. Anything else (data
    /// requests, blocks, extension payloads) is the swarm's business and is returned as
    /// `Ok(Some(msg))` for it to handle.
    pub fn apply(&mut self, msg: BtMessage) -> Result<Option<BtMessage>, ProtocolViolation> {
        self.last_received = Instant::now();
        match msg {
            BtMessage::KeepAlive(_) => {}
            BtMessage::Cancel(cancel) => {
                self.take_upload(&Request::from(cancel));
            }
            BtMessage::Choke(_) => self.choked_us = true,
            BtMessage::Unchoke(_) => self.choked_us = false,
            // whether to unchoke in return is the choking algorithm's call, not an
            // automatic grant for declaring interest
            BtMessage::Interested(_) => self.interested_us = true,
            BtMessage::NotInterested(_) => self.interested_us = false,
            BtMessage::Have(have) => {
                let Some(mut bit) = self.has.get_mut(have.checked as usize) else {
                    return Err(ProtocolViolation(format!(
                        "Have for out-of-range piece {}",
                        have.checked
                    )));
                };
                *bit = true;
            }
            BtMessage::BitField(bit_field) => {
                let pieces = self.has.len();
                if bit_field.has.len() != pieces.div_ceil(8) {
                    return Err(ProtocolViolation(format!(
                        "bitfield of length {} for {pieces} pieces",
                        bit_field.has.len(),
                    )));
                }
                // the spare bits past the last piece are meant to be zero; whatever they are,
                // they name no piece
                let mut has = BitVec::<u8, Msb0>::from_vec(bit_field.has.into_vec());
                has.truncate(pieces);
                self.has = has.into_boxed_bitslice();
            }
            BtMessage::HaveAll(_) => self.has.fill(true),
            BtMessage::HaveNone(_) => self.has.fill(false),
            // BEP 6: both are advisory-only, and we don't implement request-while-choked
            BtMessage::SuggestPiece(_) | BtMessage::AllowedFast(_) => {}
            BtMessage::Extended(ext) if ext.ext_id == 0 => self.handle_extended_handshake(&ext.payload),
            // only the first is the swarm's business: it pings the node, once
            BtMessage::Port(port) => {
                if self.dht_port.replace(port.port).is_none() {
                    return Ok(Some(BtMessage::Port(port)));
                }
            }
            BtMessage::Unknown(msg_type, _) => {
                tracing::debug!("{} sent unsupported message type {msg_type}", self.remote_addr)
            }
            other => return Ok(Some(other)),
        }
        Ok(None)
    }

    /// BEP 10: the extended handshake dict looks like `{"m": {"ut_metadata": <their id>, ...},
    /// "metadata_size": <n>, ...}`. A malformed or unsupported payload is just ignored, not a
    /// protocol violation worth disconnecting over (this is optional and best-effort).
    fn handle_extended_handshake(&mut self, payload: &[u8]) {
        let Ok((_, dict)) = juicy_bencode::parse_bencode_dict(payload) else {
            return;
        };
        if let Some(BencodeItemView::Dictionary(m)) = dict.get(b"m".as_slice()) {
            for ext in Extension::ALL {
                // a later handshake updates what it mentions; an id of 0 turns an extension off
                if let Some(item) = m.get(ext.name().as_bytes()) {
                    self.their_ids[ext as usize - 1] = match item {
                        BencodeItemView::Integer(id) => u8::try_from(*id).ok().filter(|id| *id != 0),
                        _ => None,
                    };
                }
            }
        }
        if let Some(BencodeItemView::Integer(reqq)) = dict.get(b"reqq".as_slice()) {
            self.their_reqq = usize::try_from(*reqq).ok().filter(|n| *n > 0);
        }
        if let Some(BencodeItemView::Integer(port)) = dict.get(b"p".as_slice()) {
            self.listen_port = u16::try_from(*port).ok().filter(|p| *p != 0);
        }
        if let Some(BencodeItemView::Integer(flag)) = dict.get(b"upload_only".as_slice()) {
            self.upload_only = *flag != 0;
        }
        if let Some(BencodeItemView::ByteString(ip)) = dict.get(b"yourip".as_slice()) {
            self.yourip = <[u8; 4]>::try_from(*ip)
                .map(IpAddr::from)
                .or_else(|_| <[u8; 16]>::try_from(*ip).map(IpAddr::from))
                .ok();
        }
    }

    /// Where others can reach this peer: the address we dialed, the uTP socket it came from
    /// (it listens on that same one), or its advertised listen port.
    pub fn reachable_addr(&self) -> SocketAddr {
        match self.listen_port {
            Some(port) if !self.dialed && !self.utp => SocketAddr::new(self.remote_addr.ip(), port),
            _ => self.remote_addr,
        }
    }

    /// BEP 55; a silent no-op for a peer that never said it speaks holepunch.
    pub fn send_holepunch(&mut self, msg: Holepunch) -> io::Result<()> {
        self.send_extended(Extension::UtHolepunch, msg.encode())
    }

    /// An extension message on the id the peer asked for it on; a silent no-op for a peer
    /// that never said it speaks `ext`, there's no id to send it on.
    fn send_extended(&mut self, ext: Extension, payload: Vec<u8>) -> io::Result<()> {
        let Some(ext_id) = self.their_id(ext) else {
            return Ok(());
        };
        self.send(BtMessage::Extended(Extended {
            ext_id,
            payload: payload.into_boxed_slice(),
        }))
    }

    /// BEP 54: the peer no longer has `piece`. False if it never said it had it.
    pub fn drop_have(&mut self, piece: u32) -> bool {
        match self.has.get_mut(piece as usize) {
            Some(mut bit) if *bit => {
                *bit = false;
                true
            }
            _ => false,
        }
    }

    /// Records a block that answers one of our requests. `None` if we never asked for it (or
    /// gave up waiting), in which case the caller should ignore the data.
    pub fn block_received(&mut self, piece: &Piece) -> Option<()> {
        self.requested.remove(&Request::from(piece))?;
        self.stats.block_received(piece.length as usize, Instant::now());
        self.last_progress = Instant::now();
        Some(())
    }

    /// Takes on serving `req`; false if it's been asked for already.
    pub fn accept_upload(&mut self, req: Request) -> bool {
        let new = self.uploads.insert(req);
        if new {
            self.upload_bytes += req.length as usize;
        }
        new
    }

    /// Whether `req` is still to be served: accepted, and neither cancelled nor sent.
    pub fn wants_upload(&self, req: &Request) -> bool {
        self.uploads.contains(req)
    }

    /// Stops serving `req`; false if it wasn't being served.
    pub fn take_upload(&mut self, req: &Request) -> bool {
        let had = self.uploads.remove(req);
        if had {
            self.upload_bytes -= req.length as usize;
        }
        had
    }

    /// How many blocks it has asked of us that aren't sent yet.
    pub fn queued_uploads(&self) -> usize {
        self.uploads.len()
    }

    /// The bytes of what it asked of us that haven't gone out on the socket yet: blocks being
    /// read or held back, and blocks queued for the writer.
    pub fn upload_backlog(&self) -> usize {
        self.upload_bytes + self.unsent.load(Ordering::Relaxed)
    }

    /// Whether this peer owes us blocks and hasn't delivered any for `limit`. Measured from
    /// its last delivery rather than from each request: a whole piece is requested at once,
    /// so "block older than the limit" would condemn any peer slower than piece size over
    /// limit, however steadily it's sending.
    pub fn stalled(&self, limit: Duration) -> bool {
        !self.requested.is_empty() && self.last_progress.elapsed() > limit
    }

    /// Queues `msg` for the writer; the error is the queue being full or the writer gone,
    /// either way the connection is beyond use. Every send here only queues, so none waits.
    pub(crate) fn send(&mut self, msg: BtMessage) -> io::Result<()> {
        self.outbox.try_send(msg).map_err(|e| match e {
            TrySendError::Full(_) => {
                io::Error::new(io::ErrorKind::WouldBlock, "send queue full, the peer isn't reading")
            }
            TrySendError::Closed(_) => io::ErrorKind::BrokenPipe.into(),
        })
    }

    /// BEP 10, sent once on connecting and again when `upload_only` (BEP 21) turns on.
    pub fn send_extended_handshake(
        &mut self,
        metadata_size: u32,
        private: bool,
        upload_only: bool,
        listen_port: u16,
    ) -> io::Result<()> {
        let payload = build_extended_handshake(
            metadata_size,
            private,
            upload_only,
            listen_port,
            self.remote_addr.ip().to_canonical(),
        );
        self.send(BtMessage::Extended(Extended {
            ext_id: 0,
            payload: payload.into_boxed_slice(),
        }))
    }

    pub fn send_keepalive(&mut self) -> io::Result<()> {
        self.send(BtMessage::KeepAlive(KeepAlive))
    }

    pub fn request_block(&mut self, req: Request) -> io::Result<()> {
        self.stats.block_requested();
        if self.requested.is_empty() {
            self.last_progress = Instant::now();
            self.stats.requests_started(self.last_progress);
        }
        self.requested.insert(req, Instant::now());
        self.send(BtMessage::Request(req))
    }

    pub fn unchoke(&mut self) -> io::Result<()> {
        self.send(BtMessage::Unchoke(Unchoke))?;
        self.choked_them = false;
        Ok(())
    }

    pub fn choke(&mut self) -> io::Result<()> {
        self.send(BtMessage::Choke(Choke))?;
        self.choked_them = true;
        Ok(())
    }

    pub fn show_interest(&mut self) -> io::Result<()> {
        self.send(BtMessage::Interested(Interested))?;
        self.interested_them = true;
        Ok(())
    }

    pub fn send_bitfield(&mut self, bit_field: BitField) -> io::Result<()> {
        self.send(BtMessage::BitField(bit_field))
    }

    /// BEP 6: sent in place of `BitField` when we have every piece.
    pub fn send_have_all(&mut self) -> io::Result<()> {
        self.send(BtMessage::HaveAll(HaveAll))
    }

    /// BEP 6: sent in place of `BitField` when we have no pieces at all.
    pub fn send_have_none(&mut self) -> io::Result<()> {
        self.send(BtMessage::HaveNone(HaveNone))
    }

    /// BEP 3: `Have` isn't the piece's data and isn't subject to choking, so it goes out
    /// regardless of choke/interest state.
    pub fn send_have(&mut self, index: u32) -> io::Result<()> {
        self.send(BtMessage::Have(Have { checked: index }))
    }

    /// Forgets every outstanding request for `piece`; the caller decides whether to tell the
    /// peer (`send_cancel`) or whether the peer already knows (it choked or rejected us).
    pub fn forget_piece(&mut self, piece: u32) -> Vec<Request> {
        let dropped: Vec<Request> = self.requested.keys().filter(|r| r.index == piece).copied().collect();
        for req in &dropped {
            self.requested.remove(req);
        }
        dropped
    }

    /// BEP 5: where our DHT node listens.
    pub fn send_port(&mut self, port: u16) -> io::Result<()> {
        self.send(BtMessage::Port(Port { port }))
    }

    pub fn send_cancel(&mut self, req: Request) -> io::Result<()> {
        self.send(BtMessage::Cancel(Cancel {
            index: req.index,
            begin: req.begin,
            length: req.length,
        }))
    }

    /// BEP 6: decline a `Request`. A silent no-op if the peer never advertised Fast Extension
    /// support: the classic protocol has no "I'm declining this" message, and a plain drop
    /// is exactly what such a peer already expects.
    pub fn send_reject(&mut self, req: Request) -> io::Result<()> {
        if !self.remote_supports_fast {
            return Ok(());
        }
        self.send(BtMessage::RejectRequest(RejectRequest {
            index: req.index,
            begin: req.begin,
            length: req.length,
        }))
    }

    pub fn send_block(&mut self, piece: Piece) -> io::Result<()> {
        let length = piece.length as usize;
        self.unsent.fetch_add(length, Ordering::Relaxed);
        self.send(BtMessage::Piece(piece))?;
        self.stats.block_sent(length);
        Ok(())
    }

    /// BEP 9: reply to a ut_metadata request with one piece of the raw info dict.
    pub fn send_metadata_piece(&mut self, piece: u32, total_size: u32, data: &[u8]) -> io::Result<()> {
        let payload = build_ut_metadata_data_message(piece, total_size, data);
        self.send_extended(Extension::UtMetadata, payload)
    }

    /// BEP 11 (PEX)
    pub fn send_pex(&mut self, added: &[(SocketAddr, u8)]) -> io::Result<()> {
        self.send_extended(Extension::UtPex, build_pex_message(added))
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        for task in &self.io_tasks {
            task.abort();
        }
    }
}

type Sink = SplitSink<Framed<PeerStream, BtCodec>, BtMessage>;
type Source = SplitStream<Framed<PeerStream, BtCodec>>;

async fn read_loop(mut source: Source, addr: SocketAddr, conn: u64, inbox: Inbox) {
    loop {
        let msg = source.next().await;
        let last = !matches!(msg, Some(Ok(_)));
        if inbox.send(Incoming { addr, conn, msg }).await.is_err() || last {
            return;
        }
    }
}

/// Writes everything queued, then flushes once: a burst of Requests or Haves goes out in one
/// syscall rather than one each. A batch that can't be written within `WRITE_TIMEOUT` ends the
/// connection, reported to the swarm like a read error.
async fn write_loop(
    mut sink: Sink,
    mut queued: mpsc::Receiver<BtMessage>,
    unsent: Arc<AtomicUsize>,
    addr: SocketAddr,
    conn: u64,
    inbox: Inbox,
) {
    async fn timed<F: Future<Output = io::Result<()>>>(write: F) -> io::Result<()> {
        tokio::time::timeout(WRITE_TIMEOUT, write)
            .await
            .unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut, "write timed out")))
    }
    let feed = async |sink: &mut Sink, msg: BtMessage| {
        let block = match &msg {
            BtMessage::Piece(piece) => piece.data.len(),
            _ => 0,
        };
        sink.feed(msg).await?;
        unsent.fetch_sub(block, Ordering::Relaxed);
        io::Result::Ok(())
    };
    let written: io::Result<()> = async {
        while let Some(msg) = queued.recv().await {
            // one timer per batch rather than per message: a burst is hundreds of them
            timed(async {
                feed(&mut sink, msg).await?;
                while let Ok(msg) = queued.try_recv() {
                    feed(&mut sink, msg).await?;
                }
                sink.flush().await
            })
            .await?;
        }
        Ok(())
    }
    .await;
    if let Err(e) = written {
        let _ = inbox
            .send(Incoming {
                addr,
                conn,
                msg: Some(Err(e)),
            })
            .await;
    }
}

/// BEP 10 extended handshake payload: the message ids we want the remote to use for each
/// `Extension`, minus ut_pex for a BEP 27 private torrent (offering it would invite a compliant
/// peer to use PEX with us, whose peers must come only from the trackers), plus the total
/// metadata size. `yourip` (BEP 10) tells the peer where we see it from, which helps it learn
/// its own public address; `v` names this client. Keys in bencode order.
fn build_extended_handshake(
    metadata_size: u32,
    private: bool,
    upload_only: bool,
    listen_port: u16,
    yourip: IpAddr,
) -> Vec<u8> {
    let m: String = Extension::ALL
        .into_iter()
        .filter(|&ext| !(private && ext == Extension::UtPex))
        .map(|ext| format!("{}:{}i{}e", ext.name().len(), ext.name(), ext.id()))
        .collect();
    let m = format!("d{m}e");
    let upload_only = if upload_only { "11:upload_onlyi1e" } else { "" };
    let version = concat!("downloader ", env!("CARGO_PKG_VERSION"));
    let mut out = format!(
        "d1:m{m}13:metadata_sizei{metadata_size}e1:pi{listen_port}e4:reqqi{MAX_QUEUED_UPLOADS}e{upload_only}1:v{}:{version}",
        version.len()
    )
    .into_bytes();
    let ip = match yourip {
        IpAddr::V4(v4) => v4.octets().to_vec(),
        IpAddr::V6(v6) => v6.octets().to_vec(),
    };
    out.extend_from_slice(format!("6:yourip{}:", ip.len()).as_bytes());
    out.extend_from_slice(&ip);
    out.push(b'e');
    out
}

// The block a Piece, Cancel or RejectRequest is about, as the Request that asked for it.
impl From<&Piece> for Request {
    fn from(piece: &Piece) -> Self {
        Request {
            index: piece.index,
            begin: piece.begin,
            length: piece.length,
        }
    }
}

impl From<Cancel> for Request {
    fn from(cancel: Cancel) -> Self {
        Request {
            index: cancel.index,
            begin: cancel.begin,
            length: cancel.length,
        }
    }
}

impl From<RejectRequest> for Request {
    fn from(reject: RejectRequest) -> Self {
        Request {
            index: reject.index,
            begin: reject.begin,
            length: reject.length,
        }
    }
}

/// BEP 11 "added.f" bits: what a gossiping peer knows about the one it names.
pub(crate) const PEX_PREFERS_ENCRYPTION: u8 = 0x01;
pub(crate) const PEX_SEED: u8 = 0x02;
pub(crate) const PEX_UTP: u8 = 0x04;

/// BEP 11 (PEX) message: compact peer lists, split by address family the same way BEP 7 splits
/// an HTTP tracker's "peers"/"peers6", each with its "added.f" flags. No "dropped" tracking:
/// PEX is a discovery hint, not authoritative membership, and a peer we no longer know about
/// simply stops being resent next round.
fn build_pex_message(added: &[(SocketAddr, u8)]) -> Vec<u8> {
    let mut out = b"d".to_vec();
    for (key, flags_key, is_v6) in PEX_FAMILIES {
        let family: Vec<_> = added.iter().filter(|(addr, _)| addr.is_ipv6() == is_v6).collect();
        // a family's keys are left out entirely when there's nothing to say, rather than sent
        // as empty strings, which is the shape every other client produces
        if family.is_empty() {
            continue;
        }
        let addrs: Vec<u8> = family.iter().flat_map(|(addr, _)| compact_addr(addr)).collect();
        let flags: Vec<u8> = family.iter().map(|(_, flags)| *flags).collect();
        for (key, bytes) in [(key, addrs), (flags_key, flags)] {
            out.extend_from_slice(format!("{}:{key}{}:", key.len(), bytes.len()).as_bytes());
            out.extend_from_slice(&bytes);
        }
    }
    out.push(b'e');
    out
}

/// BEP 11's per-family keys, in bencode order: the compact addresses, their flags, and
/// whether they're IPv6 (18 bytes each, against IPv4's 6).
const PEX_FAMILIES: [(&str, &str, bool); 2] = [("added", "added.f", false), ("added6", "added6.f", true)];

/// BEP 11 (PEX): the "added"/"added6" compact peer lists of an incoming message, each with its
/// "added.f" flags (0 when the sender didn't say). "dropped"/"dropped6" are ignored: PEX is
/// treated purely as a discovery hint.
pub(crate) fn parse_pex_message(payload: &[u8]) -> Vec<(SocketAddr, u8)> {
    let Ok((_, dict)) = juicy_bencode::parse_bencode_dict(payload) else {
        return vec![];
    };
    let bytes = |key: &str| match dict.get(key.as_bytes()) {
        Some(BencodeItemView::ByteString(bytes)) => *bytes,
        _ => &[][..],
    };
    let mut peers = Vec::new();
    for (key, flags_key, is_v6) in PEX_FAMILIES {
        let flags = bytes(flags_key);
        let size = if is_v6 { 18 } else { 6 };
        for (i, raw) in bytes(key).chunks(size).enumerate() {
            if let Some(addr) = parse_compact_addr(raw) {
                peers.push((addr, flags.get(i).copied().unwrap_or(0)));
            }
        }
    }
    peers
}

/// BEP 9: the piece index of an incoming ut_metadata *request* (msg_type 0); `None` for
/// anything else. We always have the full metadata, so requests are the only message worth
/// handling.
pub(crate) fn parse_ut_metadata_request(payload: &[u8]) -> Option<u32> {
    let (_, dict) = juicy_bencode::parse_bencode_dict(payload).ok()?;
    let BencodeItemView::Integer(0) = dict.get(b"msg_type".as_slice())? else {
        return None;
    };
    let BencodeItemView::Integer(piece) = dict.get(b"piece".as_slice())? else {
        return None;
    };
    Some(*piece as u32)
}

/// BEP 9 ut_metadata "data" message: a bencoded prefix (`msg_type`, `piece`, `total_size`)
/// immediately followed by the raw metadata bytes for that piece -- there's no length-prefixed
/// framing between the two, the dict's own encoding is how a parser knows where it ends.
pub(crate) fn build_ut_metadata_data_message(piece: u32, total_size: u32, data: &[u8]) -> Vec<u8> {
    let mut payload = format!("d8:msg_typei1e5:piecei{piece}e10:total_sizei{total_size}ee").into_bytes();
    payload.extend_from_slice(data);
    payload
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct PeerStatistics {
    /// Number of bytes we've sent to the peer
    pub sent: usize,

    /// Number of bytes we've received from the peer
    pub received: usize,

    /// Download throughput from this peer, bytes per second, measured over the last
    /// RATE_WINDOW of deliveries. Only updated while the peer owes us blocks, so it holds the
    /// last measured value while the peer sits idle rather than decaying to zero.
    pub rx_rate: f64,

    /// Deliveries within the last RATE_WINDOW, oldest first
    arrivals: VecDeque<(Instant, usize)>,

    /// When the current run of outstanding requests began
    busy_since: Option<Instant>,

    /// how many times this peer has been chosen to request a piece
    pub picked_count: usize,
}

impl PeerStatistics {
    /// UCB's "times this arm was played" counts block requests, not pieces.
    pub fn block_requested(&mut self) {
        self.picked_count += 1;
    }

    /// The peer went from owing us nothing to owing us blocks. Throughput is measured from
    /// here, so time spent idle doesn't count against the peer.
    pub fn requests_started(&mut self, now: Instant) {
        self.busy_since = Some(now);
        self.arrivals.clear();
    }

    /// A requested block arrived. The throughput sample is bytes delivered over the window
    /// divided by the time the peer has been busy within it, not the wait for this block: with
    /// hundreds of blocks queued at a peer, each block's wait is mostly queueing behind the
    /// others and says nothing about how fast the peer sends.
    pub fn block_received(&mut self, length: usize, now: Instant) {
        self.received += length;
        self.arrivals.push_back((now, length));
        while self
            .arrivals
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > RATE_WINDOW)
        {
            self.arrivals.pop_front();
        }
        let window_start = self.busy_since.map_or(now, |since| since.max(now - RATE_WINDOW));
        let span = now.duration_since(window_start).as_secs_f64();
        if span > 0.0 {
            let bytes: usize = self.arrivals.iter().map(|(_, len)| len).sum();
            self.rx_rate = bytes as f64 / span;
        }
    }

    pub fn block_sent(&mut self, length: usize) {
        self.sent += length;
    }

    /// What to carry over to a new connection with the same peer: the measured rate and the
    /// pick count, UCB's memory of it, but not the deliveries of a connection that's gone.
    pub fn for_reconnect(&self) -> Self {
        Self {
            rx_rate: self.rx_rate,
            picked_count: self.picked_count,
            ..Self::default()
        }
    }

    /// How many block requests to keep outstanding at this peer: REQUEST_PIPELINE_TARGET
    /// worth of its measured throughput, within MIN_REQUEST_WINDOW..=MAX_REQUEST_WINDOW.
    pub fn request_window(&self) -> usize {
        let bytes = self.rx_rate * REQUEST_PIPELINE_TARGET.as_secs_f64();
        ((bytes / BLOCK_SIZE as f64) as usize).clamp(MIN_REQUEST_WINDOW, MAX_REQUEST_WINDOW)
    }

    /// The two halves of the score: the measured rate relative to the swarm's fastest, and
    /// the exploration bonus.
    pub fn ucb_terms(&self, total_picks: usize, rate_scale: f64) -> (f64, f64) {
        let t = total_picks as f64;
        let n_t = self.picked_count as f64;
        (self.rx_rate / rate_scale, (t.ln() / n_t).sqrt())
    }

    /// UCB1: the peer's download throughput plus an exploration bonus that shrinks the more
    /// often it's been picked, relative to how often *anyone* has been picked (`total_picks`,
    /// block requests to every peer so far). UCB1's bonus is sized for rewards in `0..=1`,
    /// so the rate is divided by `rate_scale`, the fastest rate seen in the swarm; added to
    /// raw bytes per second the bonus would be invisible and exploration would end with each
    /// peer's first pick. An arm never played goes first.
    pub fn score(&self, total_picks: usize, rate_scale: f64) -> f64 {
        if total_picks == 0 || self.picked_count == 0 {
            return f64::INFINITY;
        }
        let (exploit, explore) = self.ucb_terms(total_picks, rate_scale);
        exploit + explore
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn holepunch_messages_round_trip_and_junk_is_refused() {
        for msg in [
            Holepunch::Rendezvous("10.0.0.1:6881".parse().unwrap()),
            Holepunch::Connect("[2001:db8::1]:7000".parse().unwrap()),
            Holepunch::Error("10.0.0.2:1".parse().unwrap(), HolepunchError::NotConnected),
        ] {
            assert_eq!(Holepunch::decode(&msg.encode()), Some(msg));
        }
        assert_eq!(
            Holepunch::decode(&[1, 0, 1, 2, 3, 4, 0, 5]),
            Some(Holepunch::Connect("1.2.3.4:5".parse().unwrap()))
        );
        for junk in [
            &[][..],
            &[0],
            &[0, 0, 1, 2],
            &[0, 9, 1, 2, 3, 4, 0, 5],
            &[2, 0, 1, 2, 3, 4, 0, 5],
            &[7, 0, 1, 2, 3, 4, 0, 5],
        ] {
            assert_eq!(Holepunch::decode(junk), None, "{junk:?}");
        }
    }

    #[tokio::test]
    async fn their_handshake_sets_ids_upload_only_and_yourip_and_donthave_clears_a_piece() {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let tcp = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let _other_end = listener.accept().await.unwrap();
        let (inbox, _incoming) = mpsc::channel(8);
        let mut peer = Peer::new(
            PeerStream::Tcp(tcp),
            "10.0.0.1:1".parse().unwrap(),
            9,
            false,
            [0u8; 20],
            0,
            inbox,
        );

        let mut payload = b"d1:md11:ut_metadatai3e6:ut_pexi300ee11:upload_onlyi1e6:yourip4:".to_vec();
        payload.extend_from_slice(&[203, 0, 113, 9]);
        payload.push(b'e');
        peer.handle_extended_handshake(&payload);
        assert_eq!(peer.their_id(Extension::UtMetadata), Some(3));
        assert_eq!(peer.their_id(Extension::UtPex), None, "300 isn't a one-byte id");
        assert!(peer.upload_only);
        assert_eq!(peer.yourip, Some("203.0.113.9".parse().unwrap()));
        peer.handle_extended_handshake(b"d1:md11:ut_metadatai0eee");
        assert_eq!(peer.their_id(Extension::UtMetadata), None, "0 turns it off");
        assert!(peer.upload_only, "what a later handshake doesn't mention stays");

        assert!(!peer.drop_have(4), "it never had it");
        peer.apply(BtMessage::Have(Have { checked: 4 })).unwrap();
        assert!(peer.drop_have(4));
        assert!(!peer.they_have(4));
        assert!(!peer.drop_have(100), "out of range");

        // 9 pieces take two bytes; the 7 spare bits name nothing even when a peer sets them
        peer.apply(BtMessage::BitField(BitField {
            has: Box::new([0xFF, 0x7F]),
        }))
        .unwrap();
        assert!(!peer.is_seed(), "piece 8 is missing, however many spare bits are set");
        peer.apply(BtMessage::BitField(BitField {
            has: Box::new([0xFF, 0xFF]),
        }))
        .unwrap();
        assert!(peer.is_seed());
        assert_eq!(peer.pieces().count(), 9);
        assert_eq!(peer.snapshot().progress, 1.0);
        assert!(
            peer.apply(BtMessage::Have(Have { checked: 9 })).is_err(),
            "a Have past the last piece"
        );
    }

    /// BEP 5's Port comes once after the handshake; each one the swarm sees costs a DHT ping,
    /// so a peer repeating it can't have us ping over and over.
    #[tokio::test]
    async fn only_the_first_dht_port_reaches_the_swarm() {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let tcp = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let _other_end = listener.accept().await.unwrap();
        let (inbox, _incoming) = mpsc::channel(8);
        let mut peer = Peer::new(
            PeerStream::Tcp(tcp),
            "10.0.0.1:1".parse().unwrap(),
            4,
            false,
            [0u8; 20],
            0,
            inbox,
        );
        let port = || BtMessage::Port(Port { port: 6881 });
        assert!(matches!(peer.apply(port()), Ok(Some(BtMessage::Port(_)))));
        for _ in 0..3 {
            assert!(matches!(peer.apply(port()), Ok(None)));
        }
    }

    #[test]
    fn extensions_are_found_by_our_ids() {
        for ext in Extension::ALL {
            assert_eq!(Extension::from_id(ext.id()), Some(ext));
        }
        assert_eq!(Extension::from_id(0), None, "0 is the handshake itself");
        let names: Vec<_> = Extension::ALL.iter().map(|e| e.name()).collect();
        assert!(names.is_sorted(), "bencode wants the keys sorted");
    }

    #[test]
    fn extended_handshake_is_valid_bencode_with_expected_fields() {
        let payload = build_extended_handshake(12345, false, false, 6881, "203.0.113.9".parse().unwrap());
        let (remaining, dict) = juicy_bencode::parse_bencode_dict(&payload).unwrap();
        assert!(
            remaining.is_empty(),
            "handshake payload must be exactly one bencoded dict"
        );

        let BencodeItemView::Dictionary(m) = dict.get(b"m".as_slice()).unwrap() else {
            panic!("\"m\" must be a dict");
        };
        let BencodeItemView::Integer(ut_metadata_id) = m.get(b"ut_metadata".as_slice()).unwrap() else {
            panic!("\"m\".\"ut_metadata\" must be an integer");
        };
        assert_eq!(*ut_metadata_id, Extension::UtMetadata.id() as i64);

        let BencodeItemView::Integer(ut_pex_id) = m.get(b"ut_pex".as_slice()).unwrap() else {
            panic!("\"m\".\"ut_pex\" must be an integer");
        };
        assert_eq!(*ut_pex_id, Extension::UtPex.id() as i64);

        let BencodeItemView::Integer(metadata_size) = dict.get(b"metadata_size".as_slice()).unwrap() else {
            panic!("\"metadata_size\" must be an integer");
        };
        assert_eq!(*metadata_size, 12345);
    }

    #[test]
    fn private_torrent_extended_handshake_omits_ut_pex() {
        let payload = build_extended_handshake(12345, true, true, 6881, "::1".parse().unwrap());
        let (remaining, dict) = juicy_bencode::parse_bencode_dict(&payload).unwrap();
        assert!(remaining.is_empty());

        let BencodeItemView::Dictionary(m) = dict.get(b"m".as_slice()).unwrap() else {
            panic!("\"m\" must be a dict");
        };
        assert!(
            m.contains_key(b"ut_metadata".as_slice()),
            "ut_metadata is unaffected by \"private\""
        );
        assert!(
            !m.contains_key(b"ut_pex".as_slice()),
            "BEP 27: a private torrent must not offer PEX to its peers"
        );
    }

    #[test]
    fn ut_metadata_data_message_is_valid_bencode_followed_by_raw_bytes() {
        let data = [9u8, 8, 7, 6];
        let message = build_ut_metadata_data_message(3, 100, &data);

        let (remaining, dict) = juicy_bencode::parse_bencode_dict(&message).unwrap();
        // the raw metadata bytes for this piece follow the dict with no framing of their own
        assert_eq!(remaining, &data);

        let BencodeItemView::Integer(msg_type) = dict.get(b"msg_type".as_slice()).unwrap() else {
            panic!("\"msg_type\" must be an integer");
        };
        assert_eq!(*msg_type, 1);

        let BencodeItemView::Integer(piece) = dict.get(b"piece".as_slice()).unwrap() else {
            panic!("\"piece\" must be an integer");
        };
        assert_eq!(*piece, 3);

        let BencodeItemView::Integer(total_size) = dict.get(b"total_size".as_slice()).unwrap() else {
            panic!("\"total_size\" must be an integer");
        };
        assert_eq!(*total_size, 100);
    }

    #[test]
    fn ut_metadata_request_is_recognised_and_data_is_not() {
        assert_eq!(parse_ut_metadata_request(b"d8:msg_typei0e5:piecei4ee"), Some(4));
        assert_eq!(
            parse_ut_metadata_request(b"d8:msg_typei1e5:piecei4e10:total_sizei9ee"),
            None
        );
        assert_eq!(parse_ut_metadata_request(b"garbage"), None);
    }

    #[test]
    fn pex_message_round_trips_compact_v4_and_v6_peers() {
        let v4: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let v6: SocketAddr = "[fe80::1]:6882".parse().unwrap();
        let message = build_pex_message(&[(v4, PEX_UTP | PEX_SEED), (v6, 0)]);

        let (remaining, dict) = juicy_bencode::parse_bencode_dict(&message).unwrap();
        assert!(remaining.is_empty());
        let BencodeItemView::ByteString(flags) = dict.get(b"added.f".as_slice()).unwrap() else {
            panic!("\"added.f\" must be a byte string");
        };
        assert_eq!(flags, &[PEX_UTP | PEX_SEED]);

        let BencodeItemView::ByteString(added) = dict.get(b"added".as_slice()).unwrap() else {
            panic!("\"added\" must be a byte string");
        };
        assert_eq!(added.len(), 6, "one compact ipv4 peer entry is 6 bytes");
        assert_eq!(&added[..4], &[1, 2, 3, 4]);
        assert_eq!(u16::from_be_bytes([added[4], added[5]]), 6881);

        let BencodeItemView::ByteString(added6) = dict.get(b"added6".as_slice()).unwrap() else {
            panic!("\"added6\" must be a byte string");
        };
        assert_eq!(added6.len(), 18, "one compact ipv6 peer entry is 18 bytes");
        assert_eq!(u16::from_be_bytes([added6[16], added6[17]]), 6882);

        assert_eq!(parse_pex_message(&message), vec![(v4, PEX_UTP | PEX_SEED), (v6, 0)]);
        // a message without flags, as older clients send, still parses
        assert_eq!(parse_pex_message(b"d5:added6:\x01\x02\x03\x04\x1a\xe1e"), vec![(v4, 0)]);
        assert_eq!(
            parse_pex_message(b"d5:added7:\x01\x02\x03\x04\x1a\xe1\x09e"),
            vec![(v4, 0)],
            "a trailing partial entry is dropped"
        );
    }

    #[test]
    fn pex_message_omits_added6_when_there_are_no_v6_peers() {
        let v4: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let message = build_pex_message(&[(v4, 0)]);

        let (remaining, dict) = juicy_bencode::parse_bencode_dict(&message).unwrap();
        assert!(remaining.is_empty());

        assert!(dict.contains_key(b"added".as_slice()));
        assert!(
            !dict.contains_key(b"added6".as_slice()),
            "a v4-only added list shouldn't carry an empty added6 key"
        );
    }

    #[test]
    fn client_names_from_peer_ids() {
        assert_eq!(client_name(b"-qB4650-abcdefghijkl"), "qBittorrent 4.6.5.0");
        assert_eq!(client_name(b"-TR4060-abcdefghijkl"), "Transmission 4.0.6.0");
        assert_eq!(client_name(b"-ZZ0001-abcdefghijkl"), "ZZ 0.0.0.1");
        assert_eq!(client_name(b"T0345---abcdefghijkl"), "BitTornado 0345");
        assert_eq!(
            client_name(b"M7-3-1--abcdefghijkl"),
            "M7-3-1--",
            "unknown ids show their prefix"
        );
        assert_eq!(client_name(&[0xffu8; 20]), "........");
    }

    #[test]
    fn ucb_score_is_never_nan() {
        let mut stats = PeerStatistics::default();
        assert_eq!(stats.score(0, 1.0), f64::INFINITY, "an unpicked peer is picked first");
        stats.block_requested();
        // one pick, nothing delivered yet: used to be NaN, which sorted above infinity
        assert!(!stats.score(1, 1.0).is_nan());
        assert!(
            stats.score(1, 1.0) < f64::INFINITY,
            "a picked peer must lose to an unpicked one"
        );
        let now = Instant::now();
        stats.requests_started(now);
        stats.block_received(16_384, now + Duration::from_millis(100));
        assert!(
            stats.score(10, stats.rx_rate) > 1.0,
            "the exploration bonus is positive"
        );
    }

    #[test]
    fn request_window_follows_the_measured_rate_within_its_bounds() {
        let t0 = Instant::now();
        let later = t0 + Duration::from_secs(1);
        let mut stats = PeerStatistics::default();
        assert_eq!(stats.request_window(), MIN_REQUEST_WINDOW, "no measurement yet");

        stats.requests_started(t0);
        stats.block_received(10 * BLOCK_SIZE, later);
        assert_eq!(stats.request_window(), 10 * REQUEST_PIPELINE_TARGET.as_secs() as usize);

        stats.block_received(10_000 * BLOCK_SIZE, later);
        assert_eq!(stats.request_window(), MAX_REQUEST_WINDOW);
    }

    #[test]
    fn a_rarely_picked_peer_can_outscore_the_fastest_one() {
        let t0 = Instant::now();
        let later = t0 + Duration::from_secs(1);
        let mut fast = PeerStatistics::default();
        fast.requests_started(t0);
        fast.block_received(1_000_000, later);
        let mut slow = PeerStatistics::default();
        slow.requests_started(t0);
        slow.block_received(500_000, later);

        for _ in 0..5000 {
            fast.block_requested();
        }
        slow.block_requested();
        let total = 5001;

        assert!(
            slow.score(total, fast.rx_rate) > fast.score(total, fast.rx_rate),
            "half the speed but 5000 fewer picks deserves another look"
        );
        assert!(
            slow.score(total, 1.0) < fast.score(total, 1.0),
            "in raw bytes per second the bonus would never make up the difference"
        );
    }

    #[test]
    fn throughput_ignores_queue_depth_and_idle_time() {
        let t0 = Instant::now();
        let s = Duration::from_secs;

        // eight blocks asked for at once, delivered one per second: 16 KiB/s, however long
        // each individual block waited in the queue
        let mut deep = PeerStatistics::default();
        deep.requests_started(t0);
        for i in 1..=8 {
            deep.block_received(16_384, t0 + s(i));
        }
        assert_eq!(deep.rx_rate, 16_384.0);

        // the same deliveries after a long idle stretch measure the same, and the last
        // measurement survives going idle again
        let mut idle = PeerStatistics::default();
        idle.requests_started(t0 + s(600));
        for i in 1..=8 {
            idle.block_received(16_384, t0 + s(600 + i));
        }
        assert_eq!(idle.rx_rate, 16_384.0);
        assert_eq!(idle.rx_rate, deep.rx_rate);
    }

    #[test]
    fn throughput_forgets_deliveries_older_than_the_window() {
        let t0 = Instant::now();
        let mut stats = PeerStatistics::default();
        stats.requests_started(t0);
        stats.block_received(1_000_000, t0 + Duration::from_secs(1));
        assert_eq!(stats.rx_rate, 1_000_000.0);

        // a trickle after the burst has aged out of the window shows the current rate only
        let later = t0 + RATE_WINDOW + Duration::from_secs(10);
        stats.block_received(1_000, later);
        assert_eq!(stats.rx_rate, 1_000.0 / RATE_WINDOW.as_secs_f64());
    }

    #[tokio::test]
    async fn stalled_means_no_delivery_not_old_requests() {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let tcp = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let _other_end = listener.accept().await.unwrap();
        let (inbox, _incoming) = mpsc::channel(8);
        let mut peer = Peer::new(
            PeerStream::Tcp(tcp),
            "10.0.0.1:1".parse().unwrap(),
            4,
            false,
            [0u8; 20],
            0,
            inbox,
        );
        let limit = Duration::from_millis(50);

        assert!(!peer.stalled(limit), "nothing outstanding, nothing to stall");
        let first = Request {
            index: 0,
            begin: 0,
            length: 4,
        };
        let second = Request {
            index: 0,
            begin: 4,
            length: 4,
        };
        peer.request_block(first).unwrap();
        peer.request_block(second).unwrap();
        tokio::time::sleep(limit * 2).await;
        assert!(peer.stalled(limit), "two requests, no delivery");

        // one block arrives: the other request is just as old, but the peer is delivering
        peer.block_received(&Piece {
            index: 0,
            begin: 0,
            length: 4,
            data: Box::new([0; 4]),
        })
        .unwrap();
        assert!(!peer.stalled(limit));
        tokio::time::sleep(limit * 2).await;
        assert!(peer.stalled(limit), "and then it stops again");
    }
}
