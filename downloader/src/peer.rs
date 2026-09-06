use crate::settings::{BLOCK_SIZE, MAX_REQUEST_WINDOW, MIN_REQUEST_WINDOW, RATE_WINDOW, REQUEST_PIPELINE_TARGET};
use crate::stream::PeerStream;
use crate::wire::{
    BitField, BtCodec, BtMessage, Cancel, Choke, Extended, Have, HaveAll, HaveNone, Interested, KeepAlive, Piece, Port,
    RejectRequest, Request, Unchoke,
};
use futures::SinkExt;
use juicy_bencode::BencodeItemView;
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio_util::codec::Framed;

/// BEP 10: the message id we tell peers to use when sending *us* ut_metadata messages. Fixed,
/// since it's entirely our own choice -- only the id the *remote* wants is negotiated.
pub(crate) const UT_METADATA_ID: u8 = 1;
/// BEP 10: same idea as `UT_METADATA_ID`, but for BEP 11 (PEX) messages.
pub(crate) const UT_PEX_ID: u8 = 2;

/// One connected peer. Owned outright by its `TorrentSwarm`, which is the only thing that ever
/// reads from or writes to the socket, so everything here is a plain field: no channels, no
/// snapshots, no task of its own. The swarm's event loop polls every peer's socket and calls
/// the `&mut self` methods below in between.
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
    /// same for running over uTP
    pub utp: bool,
    /// the message id the remote wants us to use for ut_metadata messages, learned from
    /// their extended handshake; `None` until then (or if they don't support it)
    pub their_ut_metadata_id: Option<u8>,
    /// same as `their_ut_metadata_id`, but for BEP 11 (PEX) messages
    pub their_ut_pex_id: Option<u8>,

    pub socket: Framed<PeerStream, BtCodec>,

    /// BEP 3 bitfield layout: `ceil(num_pieces / 8)` bytes, piece 0 is the high bit of byte 0
    they_have: Box<[u8]>,
    num_pieces: usize,

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
    pub fn new(
        stream: PeerStream,
        remote_addr: SocketAddr,
        num_pieces: usize,
        remote_supports_fast: bool,
        peer_id: [u8; 20],
    ) -> Self {
        Self {
            peer_id,
            remote_addr,
            remote_supports_fast,
            encrypted: stream.is_encrypted(),
            utp: stream.is_utp(),
            their_ut_metadata_id: None,
            their_ut_pex_id: None,
            socket: Framed::new(stream, BtCodec),
            they_have: vec![0u8; num_pieces.div_ceil(8)].into(),
            num_pieces,
            // BEP 3: "At the start of the connection, both sides ... are choked."
            choked_them: true,
            choked_us: true,
            interested_them: false,
            interested_us: false,
            requested: BTreeMap::new(),
            last_progress: Instant::now(),
            last_received: Instant::now(),
            stats: PeerStatistics::default(),
        }
    }

    pub fn they_have(&self, piece: u32) -> bool {
        let index = piece / 8;
        let offset = piece % 8;
        let flag = 0x80u8 >> offset;
        (self.they_have[index as usize] & flag) != 0
    }

    /// Is the peer ready for more requests?
    pub fn ready(&self) -> bool {
        !self.choked_us
    }

    pub fn request_window(&self) -> usize {
        self.stats.request_window()
    }

    pub fn snapshot(&self) -> PeerSnapshot {
        let have = self.they_have.iter().map(|b| b.count_ones() as usize).sum::<usize>();
        PeerSnapshot {
            addr: self.remote_addr,
            client: client_name(&self.peer_id),
            progress: if self.num_pieces == 0 {
                0.0
            } else {
                (have as f32 / self.num_pieces as f32).clamp(0.0, 1.0)
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
        }
    }

    /// Applies a message that only touches this connection's own state. Anything else (data
    /// requests, blocks, extension payloads) is the swarm's business and is returned as
    /// `Ok(Some(msg))` for it to handle.
    pub fn apply(&mut self, msg: BtMessage) -> Result<Option<BtMessage>, ProtocolViolation> {
        self.last_received = Instant::now();
        match msg {
            BtMessage::KeepAlive(_) | BtMessage::Cancel(_) => {}
            BtMessage::Choke(_) => self.choked_us = true,
            BtMessage::Unchoke(_) => self.choked_us = false,
            // whether to unchoke in return is the choking algorithm's call, not an
            // automatic grant for declaring interest
            BtMessage::Interested(_) => self.interested_us = true,
            BtMessage::NotInterested(_) => self.interested_us = false,
            BtMessage::Have(have) => {
                if have.checked as usize >= self.num_pieces {
                    return Err(ProtocolViolation(format!(
                        "Have for out-of-range piece {}",
                        have.checked
                    )));
                }
                self.they_have[(have.checked / 8) as usize] |= 0x80u8 >> (have.checked % 8);
            }
            BtMessage::BitField(bit_field) => {
                if bit_field.has.len() != self.num_pieces.div_ceil(8) {
                    return Err(ProtocolViolation(format!(
                        "bitfield of length {} for {} pieces",
                        bit_field.has.len(),
                        self.num_pieces
                    )));
                }
                self.they_have = bit_field.has;
            }
            BtMessage::HaveAll(_) => self.they_have = vec![0xFFu8; self.num_pieces.div_ceil(8)].into(),
            BtMessage::HaveNone(_) => self.they_have = vec![0u8; self.num_pieces.div_ceil(8)].into(),
            // BEP 6: both are advisory-only, and we don't implement request-while-choked
            BtMessage::SuggestPiece(_) | BtMessage::AllowedFast(_) => {}
            BtMessage::Extended(ext) if ext.ext_id == 0 => self.handle_extended_handshake(&ext.payload),
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
        let Some(BencodeItemView::Dictionary(m)) = dict.get(b"m".as_slice()) else {
            return;
        };
        if let Some(BencodeItemView::Integer(id)) = m.get(b"ut_metadata".as_slice()) {
            self.their_ut_metadata_id = Some(*id as u8);
        }
        if let Some(BencodeItemView::Integer(id)) = m.get(b"ut_pex".as_slice()) {
            self.their_ut_pex_id = Some(*id as u8);
        }
    }

    /// Records a block that answers one of our requests. `None` if we never asked for it (or
    /// gave up waiting), in which case the caller should ignore the data.
    pub fn block_received(&mut self, piece: &Piece) -> Option<()> {
        self.requested.remove(&Request {
            index: piece.index,
            begin: piece.begin,
            length: piece.length,
        })?;
        self.stats.block_received(piece.length as usize, Instant::now());
        self.last_progress = Instant::now();
        Some(())
    }

    /// Whether this peer owes us blocks and hasn't delivered any for `limit`. Measured from
    /// its last delivery rather than from each request: a whole piece is requested at once,
    /// so "block older than the limit" would condemn any peer slower than piece size over
    /// limit, however steadily it's sending.
    pub fn stalled(&self, limit: Duration) -> bool {
        !self.requested.is_empty() && self.last_progress.elapsed() > limit
    }

    pub async fn send_extended_handshake(&mut self, metadata_size: u32, private: bool) -> io::Result<()> {
        let payload = build_extended_handshake(metadata_size, private);
        self.socket
            .send(BtMessage::Extended(Extended {
                ext_id: 0,
                payload: payload.into_boxed_slice(),
            }))
            .await
    }

    pub async fn send_keepalive(&mut self) -> io::Result<()> {
        self.socket.send(BtMessage::KeepAlive(KeepAlive)).await
    }

    pub async fn request_block(&mut self, req: Request) -> io::Result<()> {
        self.stats.block_requested();
        if self.requested.is_empty() {
            self.last_progress = Instant::now();
            self.stats.requests_started(self.last_progress);
        }
        self.requested.insert(req, Instant::now());
        self.socket.send(BtMessage::Request(req)).await
    }

    pub async fn unchoke(&mut self) -> io::Result<()> {
        self.socket.send(BtMessage::Unchoke(Unchoke)).await?;
        self.choked_them = false;
        Ok(())
    }

    pub async fn choke(&mut self) -> io::Result<()> {
        self.socket.send(BtMessage::Choke(Choke)).await?;
        self.choked_them = true;
        Ok(())
    }

    pub async fn show_interest(&mut self) -> io::Result<()> {
        self.socket.send(BtMessage::Interested(Interested)).await?;
        self.interested_them = true;
        Ok(())
    }

    pub async fn send_bitfield(&mut self, bit_field: BitField) -> io::Result<()> {
        self.socket.send(BtMessage::BitField(bit_field)).await
    }

    /// BEP 6: sent in place of `BitField` when we have every piece.
    pub async fn send_have_all(&mut self) -> io::Result<()> {
        self.socket.send(BtMessage::HaveAll(HaveAll)).await
    }

    /// BEP 6: sent in place of `BitField` when we have no pieces at all.
    pub async fn send_have_none(&mut self) -> io::Result<()> {
        self.socket.send(BtMessage::HaveNone(HaveNone)).await
    }

    /// BEP 3: `Have` isn't the piece's data and isn't subject to choking, so it goes out
    /// regardless of choke/interest state.
    pub async fn send_have(&mut self, index: u32) -> io::Result<()> {
        self.socket.send(BtMessage::Have(Have { checked: index })).await
    }

    /// BEP 6: decline a `Request`. A silent no-op if the peer never advertised Fast Extension
    /// support -- the classic protocol has no "I'm declining this" message, and a plain drop
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
    pub async fn send_port(&mut self, port: u16) -> io::Result<()> {
        self.socket.send(BtMessage::Port(Port { port })).await
    }

    pub async fn send_cancel(&mut self, req: Request) -> io::Result<()> {
        self.socket
            .send(BtMessage::Cancel(Cancel {
                index: req.index,
                begin: req.begin,
                length: req.length,
            }))
            .await
    }

    /// is exactly what such a peer already expects.
    pub async fn send_reject(&mut self, req: Request) -> io::Result<()> {
        if !self.remote_supports_fast {
            return Ok(());
        }
        self.socket
            .send(BtMessage::RejectRequest(RejectRequest {
                index: req.index,
                begin: req.begin,
                length: req.length,
            }))
            .await
    }

    pub async fn send_block(&mut self, piece: Piece) -> io::Result<()> {
        let length = piece.length as usize;
        self.socket.send(BtMessage::Piece(piece)).await?;
        self.stats.block_sent(length);
        Ok(())
    }

    /// BEP 9: reply to a ut_metadata request with one piece of the raw info dict. A silent
    /// no-op if the peer never declared ut_metadata support -- there's no sane id to send on.
    pub async fn send_metadata_piece(&mut self, piece: u32, total_size: u32, data: &[u8]) -> io::Result<()> {
        let Some(their_id) = self.their_ut_metadata_id else {
            return Ok(());
        };
        self.socket
            .send(BtMessage::Extended(Extended {
                ext_id: their_id,
                payload: build_ut_metadata_data_message(piece, total_size, data).into_boxed_slice(),
            }))
            .await
    }

    /// BEP 11 (PEX): silent no-op if the peer never declared ut_pex support, same reasoning as
    /// `send_metadata_piece`.
    pub async fn send_pex(&mut self, added: &[SocketAddr]) -> io::Result<()> {
        let Some(their_id) = self.their_ut_pex_id else {
            return Ok(());
        };
        self.socket
            .send(BtMessage::Extended(Extended {
                ext_id: their_id,
                payload: build_pex_message(added).into_boxed_slice(),
            }))
            .await
    }
}

/// BEP 10 extended handshake payload: declares the message ids we want the remote to use for
/// ut_metadata and (unless this is a BEP 27 private torrent) ut_pex, plus the total metadata
/// size. BEP 27: a private torrent's peers must come only from its trackers -- not omitting
/// "ut_pex" here would invite a compliant peer to use PEX with us, defeating the point of the
/// flag even if we ourselves never act on what we'd receive.
fn build_extended_handshake(metadata_size: u32, private: bool) -> Vec<u8> {
    let m = if private {
        format!("d11:ut_metadatai{UT_METADATA_ID}ee")
    } else {
        format!("d11:ut_metadatai{UT_METADATA_ID}e6:ut_pexi{UT_PEX_ID}ee")
    };
    format!("d1:m{m}13:metadata_sizei{metadata_size}ee").into_bytes()
}

/// BEP 11 (PEX) message: compact peer lists, split by address family the same way BEP 7 splits
/// an HTTP tracker's "peers"/"peers6". Only "added"/"added6" are sent (no "added.f" flags, no
/// "dropped" tracking) -- PEX is a discovery hint, not authoritative membership, and a peer we
/// no longer know about simply stops being resent next round.
fn build_pex_message(added: &[SocketAddr]) -> Vec<u8> {
    let mut added4 = Vec::new();
    let mut added6 = Vec::new();
    for addr in added {
        match addr {
            SocketAddr::V4(a) => {
                added4.extend_from_slice(&a.ip().octets());
                added4.extend_from_slice(&a.port().to_be_bytes());
            }
            SocketAddr::V6(a) => {
                added6.extend_from_slice(&a.ip().octets());
                added6.extend_from_slice(&a.port().to_be_bytes());
            }
        }
    }

    let mut out = b"d".to_vec();
    // omit a family's key entirely when there's nothing to say, rather than sending an empty
    // byte string -- keeps a v4-only or v6-only round the same shape a peer would expect from
    // any other client, instead of a payload no one else generates
    if !added4.is_empty() {
        out.extend_from_slice(format!("5:added{}:", added4.len()).as_bytes());
        out.extend_from_slice(&added4);
    }
    if !added6.is_empty() {
        out.extend_from_slice(format!("6:added6{}:", added6.len()).as_bytes());
        out.extend_from_slice(&added6);
    }
    out.push(b'e');
    out
}

/// BEP 11 (PEX): the "added"/"added6" compact peer lists of an incoming message.
/// "added.f"/"dropped"/"dropped6" are ignored -- PEX is treated purely as a discovery hint.
pub(crate) fn parse_pex_message(payload: &[u8]) -> Vec<SocketAddr> {
    let Ok((_, dict)) = juicy_bencode::parse_bencode_dict(payload) else {
        return vec![];
    };

    let mut peers = Vec::new();
    if let Some(BencodeItemView::ByteString(bytes)) = dict.get(b"added".as_slice()) {
        for chunk in bytes.chunks_exact(6) {
            let ip = std::net::Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            peers.push(SocketAddr::from((ip, port)));
        }
    }
    if let Some(BencodeItemView::ByteString(bytes)) = dict.get(b"added6".as_slice()) {
        for chunk in bytes.chunks_exact(18) {
            let ip = std::net::Ipv6Addr::from(<[u8; 16]>::try_from(&chunk[..16]).unwrap());
            let port = u16::from_be_bytes([chunk[16], chunk[17]]);
            peers.push(SocketAddr::from((ip, port)));
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

    /// UCB1: the peer's download throughput plus an exploration bonus that shrinks the more
    /// often it's been picked, relative to how often *anyone* has been picked (`total_picks`,
    /// block requests to every peer so far). UCB1's bonus is sized for rewards in `0..=1`,
    /// so the rate is divided by `rate_scale`, the fastest rate seen in the swarm; added to
    /// raw bytes per second the bonus would be invisible and exploration would end with each
    /// peer's first pick.
    pub fn rx_speed_ucb(&self, total_picks: usize, rate_scale: f64) -> f64 {
        let c = 1f64;
        let t = total_picks as f64;
        let n_t = self.picked_count as f64;
        self.rx_rate / rate_scale + c * (t.ln() / n_t).sqrt()
    }

    pub fn score(&self, total_picks: usize, rate_scale: f64) -> f64 {
        // In UCB, when an arm hasn't been played yet, it should be picked first, we just assign an
        // infinite score to peers who haven't been requested yet
        if total_picks == 0 || self.picked_count == 0 {
            f64::INFINITY
        } else {
            self.rx_speed_ucb(total_picks, rate_scale)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn extended_handshake_is_valid_bencode_with_expected_fields() {
        let payload = build_extended_handshake(12345, false);
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
        assert_eq!(*ut_metadata_id, UT_METADATA_ID as i64);

        let BencodeItemView::Integer(ut_pex_id) = m.get(b"ut_pex".as_slice()).unwrap() else {
            panic!("\"m\".\"ut_pex\" must be an integer");
        };
        assert_eq!(*ut_pex_id, UT_PEX_ID as i64);

        let BencodeItemView::Integer(metadata_size) = dict.get(b"metadata_size".as_slice()).unwrap() else {
            panic!("\"metadata_size\" must be an integer");
        };
        assert_eq!(*metadata_size, 12345);
    }

    #[test]
    fn private_torrent_extended_handshake_omits_ut_pex() {
        let payload = build_extended_handshake(12345, true);
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
        let message = build_pex_message(&[v4, v6]);

        let (remaining, dict) = juicy_bencode::parse_bencode_dict(&message).unwrap();
        assert!(remaining.is_empty());

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

        assert_eq!(parse_pex_message(&message), vec![v4, v6]);
    }

    #[test]
    fn pex_message_omits_added6_when_there_are_no_v6_peers() {
        let v4: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let message = build_pex_message(&[v4]);

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
        let mut peer = Peer::new(PeerStream::Tcp(tcp), "10.0.0.1:1".parse().unwrap(), 4, false, [0u8; 20]);
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
        peer.request_block(first).await.unwrap();
        peer.request_block(second).await.unwrap();
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
