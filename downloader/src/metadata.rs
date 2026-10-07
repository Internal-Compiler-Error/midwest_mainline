//! Fetching a torrent's metadata from peers over BEP 9 (`ut_metadata`) -- the *consuming* side.
//!
//! This is what makes a magnet link usable: a magnet gives us an info hash and a tracker list
//! and nothing else, so before any of the normal download machinery can run (it all needs a
//! parsed `Torrent`: piece count, piece length, file list) we have to go get the info dict from
//! a peer that already has it.
//!
//! The flow is: announce to the magnet's trackers with the bare info hash -> connect to peers
//! they return -> BEP 10 extended handshake -> ask for the info dict in 16KiB pieces -> verify
//! the reassembled bytes hash to the info hash we asked for -> build a `Torrent`.
//!
//! Peers come from the magnet's trackers and from the DHT, which is how a magnet with no
//! trackers at all still resolves.

use crate::announcer::Announcing;
use crate::announcer::spawn_announcers;
use crate::defs::Identity;
use crate::dht::DhtWatch;
use crate::events::{Event, EventBus};
use crate::magnet::MagnetLink;
use crate::settings::METADATA_PIECE_SIZE;
use crate::stream::DialHints;
use crate::torrent::{Torrent, parse_torrent};
use crate::torrent_swarm::{SwarmEvent, TorrentSwarmStats};
use crate::utp::UtpWatch;
use crate::wire::{BtCodec, BtMessage, Extended, V2Support};
use anyhow::{Context, bail, ensure};
use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use futures::{SinkExt, StreamExt};
use juicy_bencode::BencodeItemView;
use librqbit_utp::UtpSocketUdp;
use midwest_mainline::types::InfoHash;
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::collections::{BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::{debug, info};

/// The ut_metadata message id we advertise for peers to send *us* ut_metadata messages on.
/// Our own choice, same as `peer::UT_METADATA_ID`; only the remote's id is negotiated.
const UT_METADATA_ID: u8 = 1;
/// Largest metadata we'll fetch. An info dict is mostly piece hashes, and 8 MiB is ~400k
/// pieces, more than torrent makers produce; a peer claiming more is lying or broken.
const MAX_METADATA_SIZE: usize = 8 * 1024 * 1024;

/// How long a single peer gets to complete the whole exchange (handshake and every metadata
/// piece) once connected, before we give up on it and let another peer try. Connecting itself
/// is bounded separately by `CONNECT_TIMEOUT`.
const PER_PEER_TIMEOUT: Duration = Duration::from_secs(30);

/// How long to keep going with no new peer turning up. Measured from the last peer discovery
/// rather than from the start: the DHT can take a while to come up and finish its first
/// lookup, and a fresh batch of peers deserves the full wait.
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// The most a fetch may take in total, however many fresh addresses keep arriving: a tracker
/// handing out a rotating list of dead peers must not keep it alive forever.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// How many peers to have metadata fetches in flight against at once. Metadata is tiny and any
/// one peer can serve all of it; what this has to absorb is that most tracker-supplied
/// addresses are unreachable and each costs a `CONNECT_TIMEOUT` to find out.
const MAX_CONCURRENT_FETCHES: usize = 32;

/// Resolves a magnet link into a full `Torrent` by fetching its metadata from peers.
#[tracing::instrument(
    name = "metadata",
    skip_all,
    fields(info_hash = %magnet.info_hash, from = tracing::field::Empty, bytes = tracing::field::Empty, tried = tracing::field::Empty)
)]
pub async fn fetch(
    magnet: &MagnetLink,
    identity: Arc<Identity>,
    shutdown: CancellationToken,
    dht: DhtWatch,
    utp: UtpWatch,
    bus: EventBus,
) -> anyhow::Result<Fetched> {
    // a watch whose sender is gone is a client with no DHT, now or ever (`Dht::none`, or a
    // node that failed to start); with no trackers or `x.pe` peers either there's nowhere to
    // find a peer
    if magnet.trackers.is_empty() && magnet.peers.is_empty() && dht.has_changed().is_err() {
        bail!(
            "magnet URI has no trackers (`tr=`) or peers (`x.pe=`) and the DHT is off, so there's no way to find peers"
        );
    }
    let (event_tx, mut event_rx) = mpsc::channel(256);
    // a child token so an outer shutdown still reaches the announcers, while this fetch ending
    // (however it ends) stops them without cancelling anything the caller still needs
    let announcers = shutdown.child_token();
    let _stop_announcers = announcers.clone().drop_guard();
    spawn_announcers(Announcing {
        trackers: magnet.trackers.clone(),
        // whether it's private (BEP 27) is in the metadata this fetches
        private: false,
        info_hash: magnet.info_hash,
        identity: identity.clone(),
        stats: watch::channel(placeholder_stats()).1,
        // announcers hold weak senders (see `TorrentSwarmHandle`); `event_tx` itself lives in
        // this frame, so the channel stays open exactly as long as this fetch does
        events: event_tx.downgrade(),
        shutdown: announcers,
        dht,
        bus: bus.clone(),
        // trackers' word on our address before there's a client to tell; it isn't kept
        external: Default::default(),
        v2: magnet.hybrid_v2_hash(),
    });

    let started = Instant::now();
    let give_up = started + OVERALL_TIMEOUT;
    let mut idle_deadline = started + IDLE_TIMEOUT;
    // dropped with this frame, which aborts what's still running: connections the swarm
    // will want to make itself, or that nobody wants after a cancel
    let mut fetches = JoinSet::new();
    let mut tried: BTreeSet<SocketAddr> = BTreeSet::new();
    // Peers heard of but not dialled yet, for want of a free slot. A tracker typically returns
    // dozens at once, and if the first few are dead the rest are what's left to try until its
    // next announce, an interval (often 30 minutes) away. A magnet's own x.pe peers go first:
    // they need no tracker or DHT to find.
    let mut pending: VecDeque<SocketAddr> = magnet.peers.iter().copied().filter(|p| tried.insert(*p)).collect();

    let (from, raw_info) = loop {
        while fetches.len() < MAX_CONCURRENT_FETCHES
            && let Some(peer) = pending.pop_front()
        {
            fetches.spawn(fetch_from(peer, magnet, identity.clone(), utp.borrow().clone()));
        }

        tokio::select! {
            _ = tokio::time::sleep_until(idle_deadline.min(give_up)) => {
                bail!(
                    "no metadata for {:?} after {}s (tried {} peers)",
                    magnet.display_name.as_deref().unwrap_or("magnet"),
                    started.elapsed().as_secs(),
                    tried.len(),
                );
            }
            _ = shutdown.cancelled() => bail!("cancelled while fetching metadata"),

            Some(event) = event_rx.recv() => {
                let SwarmEvent::PeersDiscovered(peers, _) = event else {
                    continue;
                };
                let before = pending.len();
                pending.extend(peers.into_iter().filter(|peer| tried.insert(*peer)));
                if pending.len() > before {
                    idle_deadline = Instant::now() + IDLE_TIMEOUT;
                }
            }

            Some(Ok((peer, result))) = fetches.join_next() => {
                match result {
                    Ok(raw_info) => break (peer, raw_info),
                    Err(e) => debug!("metadata fetch from a peer failed: {e:#}"),
                }
            }
        }
    };

    info!(
        "fetched {} bytes of metadata from {from}, building torrent",
        raw_info.len()
    );
    let span = tracing::Span::current();
    span.record("from", from.to_string());
    span.record("bytes", raw_info.len());
    span.record("tried", tried.len());
    bus.emit(Event::MetadataFetched {
        info_hash: magnet.info_hash,
        from,
        bytes: raw_info.len(),
        took_ms: started.elapsed().as_millis() as u64,
    });
    if let Some(v2) = magnet.info_hash_v2 {
        ensure!(
            Sha256::digest(&raw_info)[..] == v2,
            "{from} served metadata that doesn't match the magnet's v2 info hash"
        );
    }
    let torrent_file = build_torrent_file(&raw_info, &magnet.trackers);
    let mut torrent = parse_torrent(&torrent_file).context("metadata fetched from peers didn't parse as a torrent")?;
    torrent.web_seeds = magnet.web_seeds.clone();
    Ok(Fetched {
        torrent,
        peers: tried.into_iter().collect(),
    })
}

/// What the pre-metadata announces report: progress we can't know yet. BEP 3 wants the bytes
/// still needed, which only the metadata would tell; real clients send a small placeholder,
/// which only has to be non-zero so we aren't mistaken for a seed.
fn placeholder_stats() -> TorrentSwarmStats {
    TorrentSwarmStats {
        uploaded: 0,
        downloaded: 0,
        wasted: 0,
        left: METADATA_PIECE_SIZE,
        written: 0,
        verified: BitVec::<u8, Msb0>::new().into_boxed_bitslice(),
        wanted: BitVec::<u8, Msb0>::new().into_boxed_bitslice(),
        completed: false,
        storage_error: None,
    }
}

/// Fetches the metadata from `peer` within PER_PEER_TIMEOUT.
fn fetch_from(
    peer: SocketAddr,
    magnet: &MagnetLink,
    identity: Arc<Identity>,
    utp: Option<Arc<UtpSocketUdp>>,
) -> impl Future<Output = (SocketAddr, anyhow::Result<Vec<u8>>)> + use<> {
    let info_hash = magnet.info_hash;
    let v2 = magnet.v2_support();
    let span =
        tracing::info_span!("metadata.peer", info_hash = %info_hash, peer = %peer, outcome = tracing::field::Empty);
    async move {
        let result = tokio::time::timeout(PER_PEER_TIMEOUT, fetch_from_peer(peer, info_hash, v2, identity, utp))
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("timed out")));
        let outcome = match &result {
            Ok(raw) => format!("{} bytes", raw.len()),
            Err(e) => format!("{e:#}"),
        };
        tracing::Span::current().record("outcome", outcome);
        (peer, result)
    }
    .instrument(span)
}

/// A torrent built from fetched metadata, and every peer the fetch heard of on the way: the
/// swarm that starts next would otherwise wait for its own announces to find them again.
pub struct Fetched {
    pub torrent: Torrent,
    pub peers: Vec<SocketAddr>,
}

/// Runs the whole BEP 9 exchange against one peer, returning the verified raw info dict.
async fn fetch_from_peer(
    addr: SocketAddr,
    info_hash: InfoHash,
    v2: V2Support,
    identity: Arc<Identity>,
    utp: Option<Arc<UtpSocketUdp>>,
) -> anyhow::Result<Vec<u8>> {
    let (stream, handshake) =
        crate::stream::connect(addr, &info_hash, v2, &identity, utp.as_ref(), DialHints::default())
            .await
            .with_context(|| format!("connect to {addr}"))?;
    ensure!(
        handshake.supports_extensions(),
        "{addr} doesn't support the extension protocol, so it can't serve metadata"
    );

    let (mut writer, mut reader) = Framed::new(stream, BtCodec).split();

    // BEP 10: declare which id we want their ut_metadata messages on. We advertise no
    // `metadata_size` because we don't have the metadata -- that's the whole point.
    writer
        .send(BtMessage::Extended(Extended {
            ext_id: 0,
            payload: format!("d1:md11:ut_metadatai{UT_METADATA_ID}eee")
                .into_bytes()
                .into_boxed_slice(),
        }))
        .await?;

    // Wait for their extended handshake, which tells us the id to address ut_metadata requests
    // to and how big the metadata is. Anything else on the wire before then is ignored -- a
    // peer is free to send us bitfield/have/choke first, and none of it matters here.
    let (their_id, metadata_size) = loop {
        let Some(msg) = reader.next().await else {
            bail!("{addr} disconnected before sending an extended handshake");
        };
        if let BtMessage::Extended(ext) = msg?
            && ext.ext_id == 0
        {
            break parse_their_handshake(&ext.payload)?;
        }
    };

    ensure!(
        metadata_size <= MAX_METADATA_SIZE,
        "{addr} advertised an implausible metadata size of {metadata_size} bytes"
    );
    let mut metadata = Assembly::new(metadata_size);

    // Ask for everything up front rather than one at a time: metadata is at most a few
    // hundred KiB, and a round trip per 16KiB piece would dominate the transfer.
    for piece in 0..metadata.pieces() {
        writer
            .send(BtMessage::Extended(Extended {
                ext_id: their_id,
                payload: format!("d8:msg_typei0e5:piecei{piece}ee")
                    .into_bytes()
                    .into_boxed_slice(),
            }))
            .await?;
    }

    while metadata.missing() > 0 {
        let Some(msg) = reader.next().await else {
            bail!(
                "{addr} disconnected with {} metadata pieces still missing",
                metadata.missing()
            );
        };
        let BtMessage::Extended(ext) = msg? else { continue };
        if ext.ext_id != UT_METADATA_ID {
            continue;
        }
        match parse_data_message(&ext.payload) {
            Ok(UtMetadata::Data(piece, data)) => metadata.add(piece, data).with_context(|| format!("from {addr}"))?,
            Ok(UtMetadata::Reject) => bail!("{addr} rejected a metadata request"),
            // a request for our metadata (we have none to serve), or an unknown msg_type
            Ok(UtMetadata::Other) => continue,
            Err(e) => bail!("{addr} sent a malformed ut_metadata message: {e:#}"),
        }
    }
    let buffer = metadata.buffer;

    // The whole reason this is safe to accept from an untrusted peer: the bytes have to hash
    // to the info hash we asked for, otherwise they're someone else's (or fabricated) metadata.
    // (a v2-only swarm goes by the truncated SHA-256 hash; `fetch` checks all of it)
    ensure!(
        Sha1::digest(&buffer).as_slice() == info_hash.0 || Sha256::digest(&buffer)[..20] == info_hash.0,
        "{addr} served metadata that doesn't match the requested info hash"
    );

    Ok(buffer)
}

/// Metadata of a known size, as its pieces arrive in any order.
struct Assembly {
    size: usize,
    /// sized on the first piece that arrives, so a peer that only claims a size costs nothing
    buffer: Vec<u8>,
    have: Vec<bool>,
}

impl Assembly {
    fn new(size: usize) -> Self {
        Assembly {
            size,
            buffer: Vec::new(),
            have: vec![false; size.div_ceil(METADATA_PIECE_SIZE)],
        }
    }

    fn pieces(&self) -> usize {
        self.have.len()
    }

    fn missing(&self) -> usize {
        self.have.iter().filter(|got| !**got).count()
    }

    fn add(&mut self, piece: usize, data: &[u8]) -> anyhow::Result<()> {
        ensure!(piece < self.pieces(), "out-of-range metadata piece {piece}");
        let start = piece * METADATA_PIECE_SIZE;
        let end = (start + METADATA_PIECE_SIZE).min(self.size);
        ensure!(
            data.len() == end - start,
            "metadata piece {piece} has {} bytes, expected {}",
            data.len(),
            end - start
        );
        self.buffer.resize(self.size, 0);
        self.buffer[start..end].copy_from_slice(data);
        self.have[piece] = true;
        Ok(())
    }
}

/// Pulls `m.ut_metadata` and `metadata_size` out of a peer's BEP 10 extended handshake.
fn parse_their_handshake(payload: &[u8]) -> anyhow::Result<(u8, usize)> {
    let Ok((_, dict)) = juicy_bencode::parse_bencode_dict(payload) else {
        bail!("extended handshake isn't a bencoded dict");
    };
    let Some(BencodeItemView::Dictionary(m)) = dict.get(b"m".as_slice()) else {
        bail!("extended handshake has no `m` dict");
    };
    let Some(BencodeItemView::Integer(id)) = m.get(b"ut_metadata".as_slice()) else {
        bail!("peer doesn't advertise ut_metadata, so it can't serve metadata");
    };
    ensure!(
        *id > 0 && *id <= u8::MAX as i64,
        "peer advertised an invalid ut_metadata id {id}"
    );

    let Some(BencodeItemView::Integer(size)) = dict.get(b"metadata_size".as_slice()) else {
        bail!("peer advertises ut_metadata but no metadata_size");
    };
    ensure!(*size > 0, "peer advertised a non-positive metadata_size {size}");

    Ok((*id as u8, *size as usize))
}

enum UtMetadata<'a> {
    /// msg_type 1: a piece index and its raw bytes
    Data(usize, &'a [u8]),
    /// msg_type 2: a normal "I don't have it" answer rather than a protocol error
    Reject,
    /// a request (msg_type 0) or a msg_type BEP 9 doesn't define
    Other,
}

/// Splits a ut_metadata `data` message into its piece index and raw bytes.
fn parse_data_message(payload: &[u8]) -> anyhow::Result<UtMetadata<'_>> {
    let Ok((rest, dict)) = juicy_bencode::parse_bencode_dict(payload) else {
        bail!("not a bencoded dict");
    };
    let Some(BencodeItemView::Integer(msg_type)) = dict.get(b"msg_type".as_slice()) else {
        bail!("no msg_type");
    };
    match *msg_type {
        1 => {}
        2 => return Ok(UtMetadata::Reject),
        _ => return Ok(UtMetadata::Other),
    }
    let Some(BencodeItemView::Integer(piece)) = dict.get(b"piece".as_slice()) else {
        bail!("data message with no piece index");
    };
    ensure!(*piece >= 0, "negative piece index {piece}");

    // BEP 9: the raw metadata bytes follow the bencoded dict directly, with no framing of
    // their own -- whatever the dict didn't consume is the payload.
    Ok(UtMetadata::Data(*piece as usize, rest))
}

/// Wraps a raw info dict back up into a complete `.torrent` file so the existing
/// `parse_torrent` can be reused verbatim.
///
/// The info dict is embedded byte-for-byte, so the info hash `parse_torrent` recomputes over it
/// is necessarily the same one we just verified against. Keys are emitted in ascending order
/// ("announce" < "announce-list" < "info") as bencode requires.
pub(crate) fn build_torrent_file(raw_info: &[u8], trackers: &[String]) -> Vec<u8> {
    build_torrent_file_with(raw_info, trackers, None)
}

/// `build_torrent_file`, with a BEP 52 `piece layers` dict (bencoded) next to the info dict.
pub(crate) fn build_torrent_file_with(raw_info: &[u8], trackers: &[String], piece_layers: Option<&[u8]>) -> Vec<u8> {
    fn bencode_str(bytes: &[u8]) -> Vec<u8> {
        let mut out = format!("{}:", bytes.len()).into_bytes();
        out.extend_from_slice(bytes);
        out
    }

    let mut out = vec![b'd'];

    if let Some(primary) = trackers.first() {
        out.extend_from_slice(&bencode_str(b"announce"));
        out.extend_from_slice(&bencode_str(primary.as_bytes()));

        // one tier per tracker, matching how the magnet listed them
        out.extend_from_slice(&bencode_str(b"announce-list"));
        out.push(b'l');
        for tracker in trackers {
            out.push(b'l');
            out.extend_from_slice(&bencode_str(tracker.as_bytes()));
            out.push(b'e');
        }
        out.push(b'e');
    }

    out.extend_from_slice(&bencode_str(b"info"));
    out.extend_from_slice(raw_info);
    if let Some(layers) = piece_layers {
        out.extend_from_slice(&bencode_str(b"piece layers"));
        out.extend_from_slice(layers);
    }
    out.push(b'e');
    out
}

#[cfg(test)]
mod test {
    fn test_identity() -> Identity {
        Identity {
            peer_id: [9u8; 20],
            serving: "127.0.0.1:0".parse().unwrap(),
            dht: false,
            encryption: crate::config::Encryption::Disabled,
        }
    }

    use super::*;
    use tokio_util::codec::{FramedRead, FramedWrite};

    fn bencode_str(bytes: &[u8]) -> Vec<u8> {
        let mut out = format!("{}:", bytes.len()).into_bytes();
        out.extend_from_slice(bytes);
        out
    }

    fn sample_info_dict() -> Vec<u8> {
        let mut info = vec![b'd'];
        info.extend_from_slice(&bencode_str(b"length"));
        info.extend_from_slice(b"i12e");
        info.extend_from_slice(&bencode_str(b"name"));
        info.extend_from_slice(&bencode_str(b"hello.txt"));
        info.extend_from_slice(&bencode_str(b"piece length"));
        info.extend_from_slice(b"i6e");
        info.extend_from_slice(&bencode_str(b"pieces"));
        info.extend_from_slice(&bencode_str(&[7u8; 40])); // two 20-byte hashes
        info.push(b'e');
        info
    }

    /// The round trip that actually matters: bytes fetched over the wire must rebuild into a
    /// torrent whose info hash equals the one we asked the peer for.
    #[test]
    fn rebuilt_torrent_preserves_the_info_hash() {
        let raw_info = sample_info_dict();
        let expected_hash = Sha1::digest(&raw_info);

        let trackers = vec!["http://a.test/announce".to_string(), "udp://b.test:6969".to_string()];
        let file = build_torrent_file(&raw_info, &trackers);
        let torrent = parse_torrent(&file).unwrap();

        assert_eq!(torrent.info_hash.0, expected_hash.as_slice());
        assert_eq!(torrent.raw_info, raw_info, "info dict must survive byte-for-byte");
        assert_eq!(torrent.total_size, 12);
        assert_eq!(torrent.piece_size, 6);
        assert_eq!(torrent.pieces.len(), 2);
        assert_eq!(torrent.all_trackers(), trackers);
    }

    #[test]
    fn parses_a_peers_extended_handshake() {
        let payload = b"d1:md11:ut_metadatai3ee13:metadata_sizei1234ee";
        let (id, size) = parse_their_handshake(payload).unwrap();
        assert_eq!(id, 3);
        assert_eq!(size, 1234);
    }

    #[test]
    fn rejects_handshakes_that_cant_serve_metadata() {
        // advertises the extension protocol but not ut_metadata
        assert!(parse_their_handshake(b"d1:md6:ut_pexi2eee").is_err());
        // ut_metadata but no size
        assert!(parse_their_handshake(b"d1:md11:ut_metadatai3eee").is_err());
        assert!(parse_their_handshake(b"not bencode").is_err());
    }

    #[test]
    fn splits_a_data_message_into_index_and_raw_bytes() {
        let mut msg = b"d8:msg_typei1e5:piecei2e10:total_sizei99ee".to_vec();
        msg.extend_from_slice(b"RAWBYTES");

        let UtMetadata::Data(piece, data) = parse_data_message(&msg).unwrap() else {
            panic!("not parsed as data");
        };
        assert_eq!(piece, 2);
        assert_eq!(data, b"RAWBYTES");
    }

    #[test]
    fn metadata_assembles_from_pieces_in_any_order() {
        let size = METADATA_PIECE_SIZE + 10;
        let mut metadata = Assembly::new(size);
        assert_eq!((metadata.pieces(), metadata.missing()), (2, 2));
        metadata.add(1, &[7; 10]).unwrap();
        assert!(metadata.add(1, &[7; 11]).is_err(), "the last piece is short");
        assert!(metadata.add(2, &[7; 10]).is_err(), "out of range");
        assert!(metadata.add(0, &[1; 10]).is_err(), "a full piece is full");
        assert_eq!(metadata.missing(), 1);
        metadata.add(0, &[1; METADATA_PIECE_SIZE]).unwrap();
        assert_eq!(metadata.missing(), 0);
        assert_eq!(metadata.buffer.len(), size);
        assert_eq!(metadata.buffer[METADATA_PIECE_SIZE..], [7; 10]);
    }

    /// A reject is a peer legitimately saying "I don't have that", not a malformed message.
    #[test]
    fn reject_message_is_not_an_error() {
        let msg = b"d8:msg_typei2e5:piecei0ee";
        assert!(matches!(parse_data_message(msg).unwrap(), UtMetadata::Reject));
        // and a peer asking us for metadata while we fetch it isn't a reason to give up on it
        let msg = b"d8:msg_typei0e5:piecei0ee";
        assert!(matches!(parse_data_message(msg).unwrap(), UtMetadata::Other));
    }

    /// Serves `raw_info` as metadata to every peer that connects, using this crate's real
    /// serializer, advertising ut_metadata as `their_id` and hanging up on a request sent to
    /// any other id. Returns the address to point a fetcher at.
    async fn spawn_metadata_peer(raw_info: Vec<u8>, their_id: u8) -> SocketAddr {
        use crate::peer::build_ut_metadata_data_message;
        use crate::wire::{read_handshake, send_handshake};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                let served = raw_info.clone();
                tokio::spawn(async move {
                    let Ok(hs) = read_handshake(&mut tcp).await else { return };
                    if send_handshake(&mut tcp, &hs.info_hash, &test_identity(), V2Support::None)
                        .await
                        .is_err()
                    {
                        return;
                    }

                    let (reader, writer) = tcp.into_split();
                    let mut reader = FramedRead::new(reader, BtCodec);
                    let mut writer = FramedWrite::new(writer, BtCodec);

                    let handshake = format!("d1:md11:ut_metadatai{their_id}ee13:metadata_sizei{}ee", served.len());
                    if writer
                        .send(BtMessage::Extended(Extended {
                            ext_id: 0,
                            payload: handshake.into_bytes().into_boxed_slice(),
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }

                    while let Some(Ok(msg)) = reader.next().await {
                        let BtMessage::Extended(ext) = msg else { continue };
                        // ext_id 0 is the fetcher's own extended handshake
                        if ext.ext_id == 0 {
                            continue;
                        }
                        if ext.ext_id != their_id {
                            return;
                        }
                        let Ok((_, dict)) = juicy_bencode::parse_bencode_dict(&ext.payload) else {
                            continue;
                        };
                        let Some(BencodeItemView::Integer(piece)) = dict.get(b"piece".as_slice()) else {
                            continue;
                        };

                        let start = *piece as usize * METADATA_PIECE_SIZE;
                        let end = (start + METADATA_PIECE_SIZE).min(served.len());
                        let payload =
                            build_ut_metadata_data_message(*piece as u32, served.len() as u32, &served[start..end]);
                        let _ = writer
                            .send(BtMessage::Extended(Extended {
                                // the id the fetcher declared for its ut_metadata messages
                                ext_id: UT_METADATA_ID,
                                payload: payload.into_boxed_slice(),
                            }))
                            .await;
                    }
                });
            }
        });

        addr
    }

    /// End-to-end over a real loopback socket: handshake, extension negotiation, multi-piece
    /// reassembly, and the info-hash check in one go. The peer advertises ut_metadata id 5, not
    /// the 1 we advertise, so this fails if the fetcher ever assumes a fixed id instead of
    /// using the negotiated one.
    #[tokio::test]
    async fn fetches_multi_piece_metadata_from_a_real_peer() {
        // big enough to span three 16KiB pieces, so reassembly and the short final piece both
        // get exercised rather than fitting in a single message
        let mut raw_info = sample_info_dict();
        let padding_needed = METADATA_PIECE_SIZE * 2 + 1234 - raw_info.len();
        raw_info.pop(); // the trailing 'e'
        raw_info.extend_from_slice(&bencode_str(b"padding"));
        raw_info.extend_from_slice(&bencode_str(&vec![b'x'; padding_needed - 20]));
        raw_info.push(b'e');
        assert!(
            raw_info.len() > METADATA_PIECE_SIZE * 2,
            "want a genuinely multi-piece fetch"
        );
        let info_hash = InfoHash::from_bytes(Sha1::digest(&raw_info).as_slice());

        let addr = spawn_metadata_peer(raw_info.clone(), 5).await;
        let fetched = fetch_from_peer(addr, info_hash, V2Support::None, Arc::new(test_identity()), None)
            .await
            .unwrap();
        assert_eq!(fetched, raw_info, "fetched metadata must match byte-for-byte");
    }

    /// The security property that makes it safe to take metadata from an untrusted peer: bytes
    /// that don't hash to the requested info hash must be rejected, not handed back.
    #[tokio::test]
    async fn rejects_metadata_that_doesnt_match_the_info_hash() {
        let honest = sample_info_dict();
        let real_hash = InfoHash::from_bytes(Sha1::digest(&honest).as_slice());
        // what the peer actually serves: same length, different bytes
        let mut tampered = honest.clone();
        let last = tampered.len() - 2;
        tampered[last] ^= 0xff;

        let addr = spawn_metadata_peer(tampered, UT_METADATA_ID).await;
        let err = fetch_from_peer(addr, real_hash, V2Support::None, Arc::new(test_identity()), None)
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("doesn't match the requested info hash"),
            "expected an info-hash mismatch, got: {err:#}"
        );
    }

    /// A barely-HTTP tracker that answers every announce with a compact peer list containing
    /// exactly `peers`, in order.
    async fn spawn_http_tracker_with(peers: Vec<SocketAddr>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let compact: Vec<u8> = peers.iter().flat_map(midwest_mainline::message::compact_addr).collect();

        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                let compact = compact.clone();
                tokio::spawn(async move {
                    // read just enough to get past the request head; we don't care what it says
                    let mut buf = [0u8; 4096];
                    let _ = tcp.read(&mut buf).await;

                    let mut body = format!("d8:intervali1800e5:peers{}:", compact.len()).into_bytes();
                    body.extend_from_slice(&compact);
                    body.push(b'e');

                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = tcp.write_all(head.as_bytes()).await;
                    let _ = tcp.write_all(&body).await;
                    let _ = tcp.flush().await;
                });
            }
        });

        format!("http://{addr}/announce")
    }

    /// Resolves a magnet for `raw_info` that names `tracker`, with no DHT.
    async fn resolve(raw_info: &[u8], tracker: String, within: Duration) -> Torrent {
        let magnet = MagnetLink {
            info_hash: InfoHash::from_bytes(Sha1::digest(raw_info).as_slice()),
            info_hash_v2: None,
            display_name: Some("hello".to_string()),
            trackers: vec![tracker],
            web_seeds: vec![],
            peers: vec![],
            select_only: None,
            feed: None,
        };
        let fetching = fetch(
            &magnet,
            Arc::new(test_identity()),
            CancellationToken::new(),
            crate::dht::Dht::none(),
            crate::utp::none(),
            EventBus::new(),
        );
        tokio::time::timeout(within, fetching)
            .await
            .expect("magnet resolution timed out")
            .expect("magnet resolution failed")
            .torrent
    }

    /// The whole magnet path end to end: announce to a tracker, take the peer it returns, fetch
    /// the metadata from that peer over BEP 9, verify it, and build a usable `Torrent` -- with
    /// no DHT anywhere, which is the point.
    #[tokio::test]
    async fn resolves_a_magnet_link_into_a_torrent_via_a_tracker() {
        let raw_info = sample_info_dict();
        let peer = spawn_metadata_peer(raw_info.clone(), UT_METADATA_ID).await;
        let tracker = spawn_http_tracker_with(vec![peer]).await;

        let torrent = resolve(&raw_info, tracker, Duration::from_secs(20)).await;
        assert_eq!(torrent.info_hash.0, Sha1::digest(&raw_info).as_slice());
        assert_eq!(torrent.raw_info, raw_info);
        assert_eq!(torrent.total_size, 12);
        assert_eq!(torrent.files.len(), 1);
    }

    /// A fetch that ends takes its peer connections with it, rather than leaving them to run
    /// out PER_PEER_TIMEOUT against peers the swarm is about to dial, or after a cancel.
    #[tokio::test]
    async fn an_ended_fetch_hangs_up_on_its_peers() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let magnet = MagnetLink {
            info_hash: InfoHash::from_bytes(&[4; 20]),
            info_hash_v2: None,
            display_name: None,
            trackers: vec![],
            web_seeds: vec![],
            peers: vec![peer],
            select_only: None,
            feed: None,
        };
        let shutdown = CancellationToken::new();
        let fetching = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                fetch(
                    &magnet,
                    Arc::new(test_identity()),
                    shutdown,
                    crate::dht::Dht::none(),
                    crate::utp::none(),
                    EventBus::new(),
                )
                .await
            }
        });
        // a peer that takes the connection and never answers the handshake
        let (mut accepted, _) = listener.accept().await.unwrap();
        shutdown.cancel();
        assert!(fetching.await.unwrap().is_err());
        let mut rest = vec![];
        let hung_up = tokio::time::timeout(Duration::from_secs(5), accepted.read_to_end(&mut rest)).await;
        assert!(hung_up.is_ok(), "the connection outlived the fetch");
    }

    /// More peers than MAX_CONCURRENT_FETCHES from one announce, every one before the last a
    /// closed port: the overflow must be queued and dialled as slots free up, not dropped
    /// until the tracker's next announce, which is far past any timeout.
    #[tokio::test]
    async fn works_through_more_dead_peers_than_the_concurrency_limit() {
        use tokio::net::TcpListener;

        let raw_info = sample_info_dict();
        // bind then immediately drop, so the addresses are real but refuse connections
        let mut peers = Vec::new();
        for _ in 0..MAX_CONCURRENT_FETCHES + 4 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            peers.push(listener.local_addr().unwrap());
        }
        peers.push(spawn_metadata_peer(raw_info.clone(), UT_METADATA_ID).await);
        let tracker = spawn_http_tracker_with(peers).await;

        let torrent = resolve(&raw_info, tracker, Duration::from_secs(25)).await;
        assert_eq!(torrent.raw_info, raw_info);
    }
}
