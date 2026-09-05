use crate::settings::{KEEPALIVE_INTERVAL, PEER_TIMEOUT};
use crate::torrent::Torrent;
use crate::torrent_swarm::{TorrentSwarmCommand, TorrentSwarmStats};
use crate::wire::{
    BitField, BtDecoder, BtEncoder, BtMessage, Choke, Extended, Have, Interested, KeepAlive, NotInterested, Piece,
    Request, Unchoke,
};
use derive_more::{Eq, PartialEq};
use futures::SinkExt;
use futures::StreamExt;
use juicy_bencode::BencodeItemView;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::interval;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::info;

use derive_more::{Display, Error};

/// BEP 10: the message id we tell peers to use when sending *us* ut_metadata messages. Fixed,
/// since it's entirely our own choice -- only the id the *remote* wants is negotiated.
const UT_METADATA_ID: u8 = 1;
/// BEP 10: same idea as `UT_METADATA_ID`, but for BEP 11 (PEX) messages.
const UT_PEX_ID: u8 = 2;

/// It's not great that this is pub(crate) instead of fully private
#[derive(Debug)]
pub(crate) enum PeerCommands {
    UnchokePeer,
    ChokePeer,
    FancyPeer,
    RequestDataFromPeer {
        req: Request,
        syn: oneshot::Sender<Box<[u8]>>,
    },
    SendWeHave(u32),
    BitField(BitField),
    /// BEP 6: sent in place of `BitField` when we have every piece.
    SendHaveAll,
    /// BEP 6: sent in place of `BitField` when we have no pieces at all.
    SendHaveNone,
    SendData(Piece),
    /// BEP 6: we're declining a `Request`; tells the requester to stop waiting on it now
    /// instead of idling out its own timeout. A no-op if the remote never advertised Fast
    /// Extension support (there's no obligation, and nothing on the classic wire protocol to
    /// send instead -- a plain drop is what a non-fast peer already expects).
    RejectRequest(Request),
    /// BEP 9: reply to a ut_metadata request with one piece (up to 16KiB) of the raw info dict.
    SendMetadataPiece {
        piece: u32,
        total_size: u32,
        data: Box<[u8]>,
    },
    /// BEP 11 (PEX): tell this peer about other peers we know of.
    SendPex { added: Vec<SocketAddr> },
}

pub enum PeerEvent {
    Requested(Request),
    /// The connection closed, either because the peer went silent past PEER_TIMEOUT or
    /// because the socket/command channel closed outright. Lets the swarm drop this peer
    /// from `active_peers` instead of holding a dead handle forever.
    Disconnected,
    /// BEP 9: the peer asked (via ut_metadata) for one piece of our info dict. TorrentSwarm
    /// has the actual bytes (`Arc<Torrent>::raw_info`); this connection only knows how to
    /// frame the reply once it's given the data.
    MetadataRequested { piece: u32 },
    /// BEP 11 (PEX): the peer sent us its view of other peers in the swarm.
    PexReceived(Vec<SocketAddr>),
}

// TODO: Should refactor the design so that a peer connection and handle can be constructed even
// when the TCP stream is not yet established, so we can queue up messages before the connection is
// establlished. This is needed as we can have piece completion messages can need to be sent but
// the connection isn't established yet.

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerHandle {
    // TODO: i guess it's possible for multiple connections per peer, but within one download this shouldn't be true
    pub remote_peer_id: [u8; 20],

    pub remote_addr: SocketAddr,

    /// BEP 6: whether the remote advertised Fast Extension support in its handshake. A fixed
    /// fact about the connection (unlike ut_metadata/ut_pex ids, not renegotiated later), so
    /// it's exposed here the same way `remote_addr`/`remote_peer_id` are.
    pub remote_supports_fast: bool,

    #[eq(skip)]
    pub(crate) peer_tx: mpsc::Sender<PeerCommands>,

    #[eq(skip)]
    state: watch::Receiver<PeerState>,

    #[eq(skip)]
    pub(crate) stats: watch::Receiver<PeerStatistics>,
}

#[derive(Debug, Clone, Copy, Display, Error)]
pub struct PeerDied;

impl PartialOrd for PeerHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PeerHandle {
    // every lookup into `TorrentSwarm::active_peers` searches by `remote_addr` (there's only
    // ever one connection per address), so the sort key here must match, not `remote_peer_id`
    fn cmp(&self, other: &Self) -> Ordering {
        self.remote_addr.cmp(&other.remote_addr)
    }
}

impl PeerHandle {
    pub(crate) fn new(
        tcp_stream: TcpStream,
        remote_peer_id: [u8; 20],
        event_tx: mpsc::Sender<TorrentSwarmCommand>,
        torrent_status: watch::Receiver<TorrentSwarmStats>,
        torrent: &Torrent,
        remote_supports_extensions: bool,
        remote_supports_fast: bool,
    ) -> Self {
        // `to_canonical()` collapses an IPv4-mapped IPv6 address (`::ffff:a.b.c.d`, what a
        // v4 peer looks like when accepted on a dual-stack `[::]` listener) down to plain
        // `a.b.c.d`. Without this, the same peer gets a different `remote_addr` depending on
        // whether we dialed it (plain v4) or it dialed us (v4-mapped-in-v6) -- and `active_peers`
        // is a sorted vec keyed on `remote_addr`, so that peer could occupy two entries, dedup
        // against a tracker-discovered address could fail, and `Disconnected` could remove the
        // wrong one (or none).
        let remote_addr = tcp_stream.peer_addr().unwrap();
        let remote_addr = SocketAddr::new(remote_addr.ip().to_canonical(), remote_addr.port());
        let (reader, writer) = tcp_stream.into_split();
        let (commands_tx, commands_rx) = mpsc::channel(1024);

        // Create watch channel for this peer's statistics
        let initial_stats = PeerStatistics::default();
        let (stats_tx, stats_rx) = watch::channel(initial_stats);

        let num_pieces = torrent.pieces.len();
        let state = PeerState {
            // BEP 3 bitfields are `ceil(num_pieces / 8)` bytes, not one byte per piece
            they_have: vec![0u8; num_pieces.div_ceil(8)].into(),
            // BEP 3: "At the start of the connection, both sides ... are choked." Starting
            // these `false` meant we'd request from a peer before it ever unchoked us, and
            // serve any peer's requests before the choking algorithm's first round ran.
            choked_us: true,
            choked_them: true,
            interested_them: false,
            interested_us: false,
        };
        let (state_tx, state_rx) = watch::channel(state.clone());

        let peer = PeerConnection {
            commands: commands_rx,
            events: event_tx,

            state: state.clone(),
            state_tx,
            num_pieces,
            metadata_size: torrent.metadata_size(),
            their_ut_metadata_id: None,
            their_ut_pex_id: None,
            remote_supports_fast,

            remote_addr,
            reader: FramedRead::new(reader, BtDecoder),
            writer: FramedWrite::new(writer, BtEncoder),
            requested: Default::default(),
            last_received: Instant::now(),

            stats: Default::default(),
            stats_tx,

            torrent_stat: torrent_status,
        };

        tokio::spawn(peer_ev_loop(peer, remote_supports_extensions));

        Self {
            remote_peer_id,
            peer_tx: commands_tx,
            stats: stats_rx,
            state: state_rx.clone(),
            remote_addr,
            remote_supports_fast,
        }
    }

    // TODO: this one probably has a more complicated error scenario
    pub async fn request_data_from_peer(&self, req: Request) -> anyhow::Result<Box<[u8]>> {
        let (syn, ack) = oneshot::channel();

        self.peer_tx
            .send(PeerCommands::RequestDataFromPeer { req, syn })
            .await?;
        let data = ack.await?;
        Ok(data)
    }

    pub async fn fancy_peer(&self) -> Result<(), PeerDied> {
        self.peer_tx.send(PeerCommands::FancyPeer).await.map_err(|_| PeerDied)?;
        Ok(())
    }

    pub fn try_fancy_peer(&self) -> Result<(), PeerDied> {
        self.peer_tx.try_send(PeerCommands::FancyPeer).map_err(|_| PeerDied)?;
        Ok(())
    }

    /// Tell the peer that we now have a particular piece, note as with all functions on the
    /// handle, this only sends a message to the message channel linking the peer, the completion
    /// of this funciton doesn't mean the message has been copied to kernel network buffer
    pub async fn send_we_have(&self, piece: u32) -> Result<(), PeerDied> {
        self.peer_tx
            .send(PeerCommands::SendWeHave(piece))
            .await
            .map_err(|_| PeerDied)?;
        Ok(())
    }

    pub async fn bit_field(&self, bit_field: BitField) -> Result<(), PeerDied> {
        self.peer_tx
            .send(PeerCommands::BitField(bit_field))
            .await
            .map_err(|_| PeerDied)?;
        Ok(())
    }

    /// Try to send the bitfield message without blocking/awaiting
    pub fn try_bitfield(&self, bit_field: BitField) -> Result<(), PeerDied> {
        self.peer_tx
            .try_send(PeerCommands::BitField(bit_field))
            .map_err(|_| PeerDied)?;
        Ok(())
    }

    /// BEP 6: like `try_bitfield`, but for the "we have everything" shorthand.
    pub fn try_have_all(&self) -> Result<(), PeerDied> {
        self.peer_tx.try_send(PeerCommands::SendHaveAll).map_err(|_| PeerDied)?;
        Ok(())
    }

    /// BEP 6: like `try_bitfield`, but for the "we have nothing" shorthand.
    pub fn try_have_none(&self) -> Result<(), PeerDied> {
        self.peer_tx.try_send(PeerCommands::SendHaveNone).map_err(|_| PeerDied)?;
        Ok(())
    }

    /// BEP 6: decline a `Request` this peer made. A no-op (from the wire's perspective) unless
    /// the peer advertised Fast Extension support -- see `PeerConnection::send_reject`.
    pub async fn send_reject(&self, req: Request) -> Result<(), PeerDied> {
        self.peer_tx.send(PeerCommands::RejectRequest(req)).await.map_err(|_| PeerDied)?;
        Ok(())
    }

    pub async fn unchoke_peer(&self) -> Result<(), PeerDied> {
        self.peer_tx
            .send(PeerCommands::UnchokePeer)
            .await
            .map_err(|_| PeerDied)?;
        Ok(())
    }

    pub fn try_unchoke_peer(&self) -> Result<(), PeerDied> {
        self.peer_tx.try_send(PeerCommands::UnchokePeer).map_err(|_| PeerDied)?;
        Ok(())
    }

    pub async fn choke_peer(&self) -> Result<(), PeerDied> {
        self.peer_tx.send(PeerCommands::ChokePeer).await.map_err(|_| PeerDied)?;
        Ok(())
    }

    pub async fn send_data(&self, piece: Piece) -> Result<(), PeerDied> {
        self.peer_tx
            .send(PeerCommands::SendData(piece))
            .await
            .map_err(|_| PeerDied)?;
        Ok(())
    }

    pub async fn send_metadata_piece(&self, piece: u32, total_size: u32, data: Box<[u8]>) -> Result<(), PeerDied> {
        self.peer_tx
            .send(PeerCommands::SendMetadataPiece { piece, total_size, data })
            .await
            .map_err(|_| PeerDied)?;
        Ok(())
    }

    /// BEP 11 (PEX): tell this peer about other peers we know of.
    pub async fn send_pex(&self, added: Vec<SocketAddr>) -> Result<(), PeerDied> {
        self.peer_tx.send(PeerCommands::SendPex { added }).await.map_err(|_| PeerDied)?;
        Ok(())
    }

    pub fn state(&self) -> PeerState {
        self.state.borrow().clone()
    }

    pub fn stats(&self) -> PeerStatistics {
        self.stats.borrow().clone()
    }

    pub fn ready(&self) -> bool {
        self.state().ready()
    }
}

/// Represents an active connection to a peer in the BitTorrent network
#[derive(Debug)]
struct PeerConnection {
    commands: Receiver<PeerCommands>,
    // we never ask for anything from the torrent swarm, only posting events, they handle want to do with
    // us using the channel above
    events: mpsc::Sender<TorrentSwarmCommand>,

    state: PeerState,
    state_tx: watch::Sender<PeerState>,
    num_pieces: usize,
    /// total size of the bencoded info dict, advertised in our BEP 10 extended handshake
    metadata_size: u32,
    /// the message id the remote wants us to use for ut_metadata messages, learned from
    /// their extended handshake; `None` until then (or if they don't support it)
    their_ut_metadata_id: Option<u8>,
    /// same as `their_ut_metadata_id`, but for BEP 11 (PEX) messages
    their_ut_pex_id: Option<u8>,
    /// BEP 6: whether the remote advertised Fast Extension support in its handshake. Unlike
    /// `their_ut_metadata_id`/`their_ut_pex_id`, this is known at construction time (it's a
    /// handshake reserved-bit, not something negotiated via a later extended handshake).
    remote_supports_fast: bool,

    remote_addr: SocketAddr,
    reader: FramedRead<OwnedReadHalf, BtDecoder>,
    writer: FramedWrite<OwnedWriteHalf, BtEncoder>,

    requested: BTreeMap<Request, (oneshot::Sender<Box<[u8]>>, Instant)>,
    /// bumped on every inbound message (including keep-alives); a peer that sends nothing for
    /// PEER_TIMEOUT is considered dead
    last_received: Instant,

    torrent_stat: watch::Receiver<TorrentSwarmStats>,

    stats: PeerStatistics,
    stats_tx: watch::Sender<PeerStatistics>,
    ///// if the connection is closed, then the peer should be disposed off when able
    // connection_closed: bool,
}

/// BEP 10 extended handshake payload: declares the message ids we want the remote to use for
/// ut_metadata and ut_pex, plus the total metadata size.
fn build_extended_handshake(metadata_size: u32) -> Vec<u8> {
    format!(
        "d1:md11:ut_metadatai{UT_METADATA_ID}e6:ut_pexi{UT_PEX_ID}ee13:metadata_sizei{metadata_size}ee"
    )
    .into_bytes()
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

/// BEP 9 ut_metadata "data" message: a bencoded prefix (`msg_type`, `piece`, `total_size`)
/// immediately followed by the raw metadata bytes for that piece -- there's no length-prefixed
/// framing between the two, the dict's own encoding is how a parser knows where it ends.
fn build_ut_metadata_data_message(piece: u32, total_size: u32, data: &[u8]) -> Vec<u8> {
    let mut payload = format!("d8:msg_typei1e5:piecei{piece}e10:total_sizei{total_size}ee").into_bytes();
    payload.extend_from_slice(data);
    payload
}

async fn peer_ev_loop(mut peer: PeerConnection, remote_supports_extensions: bool) {
    if remote_supports_extensions {
        // BEP 10: send our extended handshake first, declaring the ut_metadata message id
        // we want the remote to use when sending *us* ut_metadata messages
        let payload = build_extended_handshake(peer.metadata_size);
        let _ = peer
            .writer
            .send(BtMessage::Extended(Extended {
                ext_id: 0,
                payload: payload.into_boxed_slice(),
            }))
            .await;
    }

    let mut keepalive_ticker = interval(KEEPALIVE_INTERVAL);
    keepalive_ticker.tick().await; // the first tick fires immediately; skip it

    loop {
        tokio::select! {
            // Handle commands from the peer handle
            Some(command) = peer.commands.recv() => { peer.process_command(command).await; }

            // Process incoming BitTorrent protocol messages
            Some(Ok(msg)) = peer.reader.next() => { peer.process_message(msg).await }

            // BEP 3: send a keep-alive periodically, and consider the peer dead if it hasn't
            // sent us anything (not even its own keep-alives) in a while
            _ = keepalive_ticker.tick() => {
                if peer.last_received.elapsed() > PEER_TIMEOUT {
                    info!("{} timed out (no messages for {:?}), disconnecting", peer.remote_addr, peer.last_received.elapsed());
                    break;
                }
                if peer.writer.send(BtMessage::KeepAlive(KeepAlive)).await.is_err() {
                    break;
                }
            }

            // Exit loop if all channels are closed
            else => break,
        }
    }

    peer.emit_event(PeerEvent::Disconnected).await;
}

impl PeerConnection {
    async fn emit_event(&mut self, event: PeerEvent) {
        // the swarm task may already be gone (e.g. mid-shutdown); that's not this
        // connection's problem to panic over
        let _ = self
            .events
            .send(TorrentSwarmCommand::ProcessPeerEvent {
                from: self.remote_addr,
                event,
            })
            .await;
    }

    /// BEP 10: the extended handshake dict looks like `{"m": {"ut_metadata": <their id>, ...},
    /// "metadata_size": <n>, ...}`. We only care about the id they want for ut_metadata; a
    /// malformed or unsupported payload is just ignored, not a protocol violation worth
    /// disconnecting over (unlike Have/BitField, this is optional and best-effort).
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

    /// BEP 11 (PEX): decode "added"/"added6" compact peer lists and hand the addresses to the
    /// swarm, which dials any we're not already connected to. "added.f"/"dropped"/"dropped6"
    /// are ignored -- PEX is treated purely as a discovery hint here.
    async fn handle_pex_message(&mut self, payload: &[u8]) {
        let Ok((_, dict)) = juicy_bencode::parse_bencode_dict(payload) else {
            return;
        };

        let mut peers = Vec::new();
        if let Some(BencodeItemView::ByteString(bytes)) = dict.get(b"added".as_slice()) {
            for chunk in bytes.chunks(6) {
                if chunk.len() != 6 {
                    break;
                }
                let ip = std::net::Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
                let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                peers.push(SocketAddr::from((ip, port)));
            }
        }
        if let Some(BencodeItemView::ByteString(bytes)) = dict.get(b"added6".as_slice()) {
            for chunk in bytes.chunks(18) {
                if chunk.len() != 18 {
                    break;
                }
                let ip = std::net::Ipv6Addr::from(<[u8; 16]>::try_from(&chunk[..16]).unwrap());
                let port = u16::from_be_bytes([chunk[16], chunk[17]]);
                peers.push(SocketAddr::from((ip, port)));
            }
        }

        if !peers.is_empty() {
            self.emit_event(PeerEvent::PexReceived(peers)).await;
        }
    }

    /// BEP 9: we only ever have the full metadata (we start from a .torrent file, not a magnet
    /// link), so the only incoming message worth handling is a request (msg_type 0) for one
    /// piece; the swarm has the actual bytes and builds the reply.
    async fn handle_ut_metadata_message(&mut self, payload: &[u8]) {
        // We reply using `their_ut_metadata_id`, negotiated via the extended handshake. Without
        // it, any reply we build is unaddressable and silently dropped by `send_metadata_piece`
        // -- so skip the parse/emit/swarm round-trip entirely rather than doing it for nothing
        // (a peer that never sent a handshake could otherwise trigger this in a loop for free).
        if self.their_ut_metadata_id.is_none() {
            return;
        }
        let Ok((_, dict)) = juicy_bencode::parse_bencode_dict(payload) else {
            return;
        };
        let Some(BencodeItemView::Integer(msg_type)) = dict.get(b"msg_type".as_slice()) else {
            return;
        };
        let Some(BencodeItemView::Integer(piece)) = dict.get(b"piece".as_slice()) else {
            return;
        };

        if *msg_type == 0 {
            self.emit_event(PeerEvent::MetadataRequested { piece: *piece as u32 }).await;
        }
    }

    pub async fn request_data_from_peer(
        &mut self,
        req: Request,
        syn: oneshot::Sender<Box<[u8]>>,
    ) -> anyhow::Result<()> {
        self.stats.picked_count += 1;
        self.publish_stat();

        self.requested.insert(req, (syn, Instant::now()));
        self.writer.send(BtMessage::Request(req)).await?;

        Ok(())
    }

    pub async fn unchoke_peer(&mut self) -> io::Result<()> {
        let unchoke = Unchoke;
        self.writer.send(BtMessage::Unchoke(unchoke)).await?;
        self.state.choked_them = false;

        Ok(())
    }

    pub async fn choke_peer(&mut self) -> io::Result<()> {
        let choke = Choke;
        self.writer.send(BtMessage::Choke(choke)).await?;
        self.state.choked_them = true;

        Ok(())
    }

    pub async fn fancy_peer(&mut self) -> io::Result<()> {
        let interested = Interested;
        self.writer.send(BtMessage::Interested(interested)).await?;
        self.state.interested_them = true;

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn unfancy_peer(&mut self) -> io::Result<()> {
        let not_interested = NotInterested;
        self.writer.send(BtMessage::NotInterested(not_interested)).await?;
        self.state.interested_them = false;

        Ok(())
    }

    pub async fn send_bitfield(&mut self, bit_field: BitField) -> io::Result<()> {
        self.writer.send(BtMessage::BitField(bit_field)).await?;
        Ok(())
    }

    /// BEP 6: sent in place of `BitField` when we have every piece.
    async fn send_have_all(&mut self) -> io::Result<()> {
        self.writer.send(BtMessage::HaveAll(crate::wire::HaveAll)).await
    }

    /// BEP 6: sent in place of `BitField` when we have no pieces at all.
    async fn send_have_none(&mut self) -> io::Result<()> {
        self.writer.send(BtMessage::HaveNone(crate::wire::HaveNone)).await
    }

    /// BEP 6: decline a `Request`. A silent no-op if the peer never advertised Fast Extension
    /// support -- the classic protocol has no "I'm declining this" message, a plain drop (what
    /// happens if this is a no-op) is exactly what such a peer already expects.
    async fn send_reject(&mut self, req: Request) -> anyhow::Result<()> {
        if !self.remote_supports_fast {
            return Ok(());
        }
        self.writer
            .send(BtMessage::RejectRequest(crate::wire::RejectRequest {
                index: req.index,
                begin: req.begin,
                length: req.length,
            }))
            .await?;
        Ok(())
    }

    /// BEP 3: `Have` announces possession of a piece; it isn't the piece's data and isn't
    /// subject to choking, so it must go out regardless of choke/interest state. This used to
    /// be gated on `!interested_us || choked_them`, which was masked back when every peer was
    /// unchoked unconditionally -- once the choking algorithm (see run_choking_algorithm) started
    /// actually choking most peers, that gate silently dropped piece announcements to almost
    /// everyone, starving their rarest-first availability data (and any future PEX/BEP 11 use)
    /// for no spec reason at all.
    pub async fn send_we_have(&mut self, index: u32) -> io::Result<()> {
        let have = Have { checked: index };
        self.writer.send(BtMessage::Have(have)).await?;

        Ok(())
    }

    pub async fn send_data(&mut self, piece: Piece) -> anyhow::Result<()> {
        let length = piece.length.clone();
        self.writer.send(BtMessage::Piece(piece)).await?;
        self.stats.sent += length as usize;
        self.publish_stat();

        Ok(())
    }

    /// BEP 9: reply to a ut_metadata request with one piece of the raw info dict. A silent
    /// no-op if the peer never declared ut_metadata support -- there's no sane id to send on.
    async fn send_metadata_piece(&mut self, piece: u32, total_size: u32, data: Box<[u8]>) -> anyhow::Result<()> {
        let Some(their_id) = self.their_ut_metadata_id else {
            return Ok(());
        };

        let payload = build_ut_metadata_data_message(piece, total_size, &data);

        self.writer
            .send(BtMessage::Extended(Extended {
                ext_id: their_id,
                payload: payload.into_boxed_slice(),
            }))
            .await?;
        Ok(())
    }

    /// BEP 11 (PEX): silent no-op if the peer never declared ut_pex support, same reasoning as
    /// `send_metadata_piece` -- there's no id to address a reply to.
    async fn send_pex(&mut self, added: &[SocketAddr]) -> anyhow::Result<()> {
        let Some(their_id) = self.their_ut_pex_id else {
            return Ok(());
        };

        let payload = build_pex_message(added);

        self.writer
            .send(BtMessage::Extended(Extended {
                ext_id: their_id,
                payload: payload.into_boxed_slice(),
            }))
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn process_command(&mut self, command: PeerCommands) {
        info!("Handling one {:?} command", command);
        match command {
            PeerCommands::UnchokePeer => self.unchoke_peer().await.unwrap(),
            PeerCommands::ChokePeer => self.choke_peer().await.unwrap(),
            PeerCommands::RequestDataFromPeer { req, syn } => self.request_data_from_peer(req, syn).await.unwrap(),
            PeerCommands::FancyPeer => self.fancy_peer().await.unwrap(),
            PeerCommands::SendWeHave(piece) => self.send_we_have(piece).await.unwrap(),
            PeerCommands::BitField(bitfield) => self.send_bitfield(bitfield).await.unwrap(),
            PeerCommands::SendHaveAll => self.send_have_all().await.unwrap(),
            PeerCommands::SendHaveNone => self.send_have_none().await.unwrap(),
            PeerCommands::RejectRequest(req) => self.send_reject(req).await.unwrap(),
            PeerCommands::SendData(piece) => self.send_data(piece).await.unwrap(),
            PeerCommands::SendMetadataPiece { piece, total_size, data } => {
                self.send_metadata_piece(piece, total_size, data).await.unwrap()
            }
            PeerCommands::SendPex { added } => self.send_pex(&added).await.unwrap(),
        }
        // unlike process_message, nothing else publishes state after a command runs --
        // without this, choke/unchoke/interested changes made here are invisible to
        // PeerHandle::state() (and so to the choking algorithm) forever
        let _ = self.state_tx.send(self.state.clone());
    }

    #[tracing::instrument(skip(self))]
    pub async fn process_message(&mut self, msg: BtMessage) {
        info!("Handling one {:?} BitTorrent message", msg);
        self.last_received = Instant::now();
        match msg {
            BtMessage::KeepAlive(_) => return,
            BtMessage::Choke(_) => self.state.choked_us = true,
            BtMessage::Unchoke(_) => self.state.choked_us = false,
            BtMessage::Interested(_) => {
                // whether to unchoke is the choking algorithm's call (run periodically by
                // TorrentSwarm), not an automatic grant for declaring interest -- doing it
                // here would bypass tit-for-tat ranking entirely
                self.state.interested_us = true;
            }
            BtMessage::NotInterested(_) => self.state.interested_us = false,
            BtMessage::Have(have) => {
                if have.checked as usize >= self.num_pieces {
                    tracing::warn!("{} sent Have for out-of-range piece {}", self.remote_addr, have.checked);
                    let _ = self.writer.close().await;
                    return;
                }

                let index = have.checked / 8;
                let offset = have.checked % 8;

                // BEP 3 bitfields are MSB-first: piece 0 is the high bit of byte 0
                let flag = 0x80u8 >> offset;
                self.state.they_have[index as usize] |= flag;
            }
            BtMessage::BitField(bit_field) => {
                // BEP 3: a bitfield of the wrong length is a protocol violation; drop the
                // connection rather than risk an out-of-bounds index later
                if bit_field.has.len() != self.num_pieces.div_ceil(8) {
                    tracing::warn!(
                        "{} sent a bitfield of length {} for {} pieces",
                        self.remote_addr,
                        bit_field.has.len(),
                        self.num_pieces
                    );
                    let _ = self.writer.close().await;
                    return;
                }
                self.state.they_have = bit_field.has;
            }
            BtMessage::HaveAll(_) => {
                self.state.they_have = vec![0xFFu8; self.num_pieces.div_ceil(8)].into();
            }
            BtMessage::HaveNone(_) => {
                self.state.they_have = vec![0u8; self.num_pieces.div_ceil(8)].into();
            }
            BtMessage::SuggestPiece(_) | BtMessage::AllowedFast(_) => {
                // BEP 6: both are advisory-only; acting on either is optional for the receiver
                // and we don't implement request-while-choked, so there's nothing to do here
                // beyond having parsed them instead of treating them as Unknown.
            }
            BtMessage::RejectRequest(reject) => {
                // BEP 6: cancels a request we made, the same way a `Piece` fulfills one --
                // dropping the oneshot sender fails the waiting `.await` immediately instead of
                // leaving it to idle out BLOCK_REQUEST_TIMEOUT. A reject for a request we have
                // no record of (already fulfilled, or the peer never had this data) is just
                // ignored, not a protocol violation worth disconnecting over.
                self.requested.remove(&Request {
                    index: reject.index,
                    begin: reject.begin,
                    length: reject.length,
                });
            }
            BtMessage::Request(request) => {
                // BEP 3: a choked peer isn't entitled to any data, full stop -- serving them
                // anyway (which is what happened here before) bypasses the choking algorithm
                // entirely. BEP 6 turns "ignore it" into "must say so": once Fast Extension is
                // negotiated, a declined request needs an explicit RejectRequest rather than a
                // silent drop, which `send_reject` already no-ops on its own if it isn't.
                if self.state.choked_them {
                    let _ = self.send_reject(request).await;
                    return;
                }
                self.events
                    .send(TorrentSwarmCommand::ProcessPeerEvent {
                        from: self.remote_addr,
                        event: PeerEvent::Requested(request),
                    })
                    .await
                    .expect("They kill us and not the other way around, TorrentSwarm outlives us");
            }
            BtMessage::Piece(piece) => {
                if !self.requested.contains_key(&Request {
                    index: piece.index,
                    begin: piece.begin,
                    length: piece.length,
                }) {
                    let _ = self.writer.close().await;
                    // TODO: let the handle know in someway
                    return;
                }

                let (syn, requested_time) = self
                    .requested
                    .remove(&Request {
                        index: piece.index,
                        begin: piece.begin,
                        length: piece.length,
                    })
                    .unwrap();

                self.stats.received += piece.length as usize;

                let speed = (piece.length as f64) / (Instant::now() - requested_time).as_secs_f64();
                self.update_speed_estimation(speed);
                self.publish_stat();
                let _ = syn.send(piece.data);
            }
            BtMessage::Cancel(_cancel) => return,
            BtMessage::Extended(ext) => {
                if ext.ext_id == 0 {
                    self.handle_extended_handshake(&ext.payload);
                } else if ext.ext_id == UT_METADATA_ID {
                    self.handle_ut_metadata_message(&ext.payload).await;
                } else if ext.ext_id == UT_PEX_ID {
                    self.handle_pex_message(&ext.payload).await;
                } else {
                    tracing::debug!("{} sent an unsupported extended message id {}", self.remote_addr, ext.ext_id);
                }
            }
            BtMessage::Unknown(msg_type, _) => {
                tracing::warn!("Unsupported message type {msg_type}");
            }
        }
        let _ = self.state_tx.send(self.state.clone());
    }

    fn publish_stat(&self) {
        // TODO: don't only publish every .5 second or something
        let _ = self.stats_tx.send(self.stats);
    }

    fn update_speed_estimation(&mut self, sampled_speed: f64) {
        // online average update formula
        self.stats.mean_rx_cnt += 1;
        self.stats.mean_rx = self.stats.mean_rx + (sampled_speed - self.stats.mean_rx) / self.stats.mean_rx_cnt as f64;
    }
}

#[derive(Clone, Debug)]
pub struct PeerState {
    // TODO: it's only used when selecting peers, probably shouldn't be here
    they_have: Box<[u8]>,

    /// We choked the peer, i.e. we won't send data until we unchoke them
    pub choked_them: bool,

    /// The peer choked us, i.e. they won't send data until they unchoke us
    pub choked_us: bool,

    /// We are interested in them, i.e. they have something we want
    pub interested_them: bool,

    /// They are interested in us, i.e. they want something from us
    pub interested_us: bool,
}

impl PeerState {
    pub fn they_have(&self, piece: u32) -> bool {
        let index = piece / 8;
        let offset = piece % 8;

        // BEP 3 bitfields are MSB-first: piece 0 is the high bit of byte 0
        let flag = 0x80u8 >> offset;
        (self.they_have[index as usize] & flag) != 0
    }

    /// Is the peer ready for more requests?
    pub fn ready(&self) -> bool {
        !self.choked_us
    }
}

#[derive(Clone, Debug, Copy, PartialEq, Default)]
pub struct PeerStatistics {
    /// Number of bytes we've sent to the peer
    pub sent: usize,

    /// Number of bytes we've received from the peer
    pub received: usize,

    /// Mean download speed from this peer
    pub mean_rx: f64,

    /// how many times have the download speed been sampled, mostly used in online averaging
    /// calculation
    pub mean_rx_cnt: usize,

    /// how many times this peer has been chosen to request a piece
    pub picked_count: usize,
}

impl PeerStatistics {
    /// Calculate the peer's download's upper confidence bound based on how many pieces have been
    /// requested
    pub fn rx_speed_ucb(&self, total_piece_requested: usize) -> f64 {
        // Upper Confidence Bound
        let c = 1f64;
        let t = total_piece_requested as f64;
        let n_t = self.picked_count as f64;
        self.mean_rx + c * (t.ln() / n_t).sqrt()
    }

    // In UCB, when an arm hasn't been played yet, it should be picked first, instead of doing an
    // if check every time we choose a peer, we just assign infinite score to peers who haven't
    // been requested yet
    pub fn score(&self, total_piece_requested: usize) -> f64 {
        if self.picked_count == 0 {
            f64::INFINITY
        } else {
            self.rx_speed_ucb(total_piece_requested)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn extended_handshake_is_valid_bencode_with_expected_fields() {
        let payload = build_extended_handshake(12345);
        let (remaining, dict) = juicy_bencode::parse_bencode_dict(&payload).unwrap();
        assert!(remaining.is_empty(), "handshake payload must be exactly one bencoded dict");

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
    fn pex_message_is_valid_bencode_with_compact_v4_and_v6_peers() {
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
}
