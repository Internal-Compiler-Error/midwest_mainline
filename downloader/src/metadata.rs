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
use crate::wire::{BtCodec, BtMessage, Extended};
use anyhow::{Context, bail, ensure};
use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use futures::{SinkExt, StreamExt};
use juicy_bencode::BencodeItemView;
use librqbit_utp::UtpSocketUdp;
use midwest_mainline::types::InfoHash;
use sha1::{Digest, Sha1};
use std::collections::{BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// The ut_metadata message id we advertise for peers to send *us* ut_metadata messages on.
/// Our own choice, same as `peer::UT_METADATA_ID`; only the remote's id is negotiated.
const UT_METADATA_ID: u8 = 1;

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

/// BEP 3 wants the number of bytes still needed, which is unknowable before we have the
/// metadata that would tell us. Real clients send a small placeholder here for the
/// pre-metadata announce; the value only has to be non-zero so we aren't mistaken for a seed.
const UNKNOWN_BYTES_LEFT: usize = METADATA_PIECE_SIZE;

/// Resolves a magnet link into a full `Torrent` by fetching its metadata from peers.
pub async fn fetch(
    magnet: &MagnetLink,
    identity: Arc<Identity>,
    shutdown: CancellationToken,
    dht: DhtWatch,
    utp: UtpWatch,
    bus: EventBus,
) -> anyhow::Result<Fetched> {
    // a watch whose sender is gone is a client with no DHT, now or ever (`Dht::none`, or a
    // node that failed to start); with no trackers either there's nowhere to find a peer
    if magnet.trackers.is_empty() && dht.has_changed().is_err() {
        bail!("magnet URI has no trackers (`tr=`) and the DHT is off, so there's no way to find peers");
    }
    // The announcers report progress they can't possibly know yet; they only need something
    // shaped like stats to read `left`/`uploaded`/`downloaded` out of.
    let placeholder_stats = TorrentSwarmStats {
        uploaded: 0,
        downloaded: 0,
        wasted: 0,
        left: UNKNOWN_BYTES_LEFT,
        written: 0,
        verified: BitVec::<u8, Msb0>::new().into_boxed_bitslice(),
        wanted: BitVec::<u8, Msb0>::new().into_boxed_bitslice(),
        completed: false,
    };
    let (_stat_tx, stat_rx) = watch::channel(placeholder_stats);
    let (event_tx, mut event_rx) = mpsc::channel(256);

    /// Stops the metadata phase's announcers however this function exits -- success, error, or
    /// early `bail!`. Without it they'd keep announcing to the trackers forever, since they
    /// outlive this call and only ever stop on a cancelled token.
    struct StopAnnouncersOnDrop(CancellationToken);
    impl Drop for StopAnnouncersOnDrop {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }
    // a child token so an outer shutdown still propagates down, but finishing here doesn't
    // cancel anything the caller still needs
    let announcer_shutdown = StopAnnouncersOnDrop(shutdown.child_token());

    spawn_announcers(Announcing {
        trackers: magnet.trackers.clone(),
        info_hash: magnet.info_hash,
        identity: identity.clone(),
        stats: stat_rx,
        // announcers hold weak senders (see `TorrentSwarmHandle`); `event_tx` itself lives in
        // this frame, so the channel stays open exactly as long as this fetch does
        events: event_tx.downgrade(),
        shutdown: announcer_shutdown.0.clone(),
        dht,
        bus: bus.clone(),
    });

    let started = tokio::time::Instant::now();
    let (result_tx, mut result_rx) = mpsc::channel::<(SocketAddr, anyhow::Result<Vec<u8>>)>(MAX_CONCURRENT_FETCHES);
    let mut tried: BTreeSet<SocketAddr> = BTreeSet::new();
    // Peers discovered but not yet dialled, because the concurrency limit was already reached.
    // These have to be queued rather than dropped: a tracker typically returns dozens of peers
    // in one go, and if the first few happen to be dead, dropping the rest would leave us idle
    // until the *next* announce -- which is an interval (often 30 minutes) away, i.e. well past
    // any timeout here.
    let mut pending: VecDeque<SocketAddr> = VecDeque::new();
    let mut in_flight = 0usize;
    let mut deadline = tokio::time::Instant::now() + IDLE_TIMEOUT;
    let give_up = tokio::time::Instant::now() + OVERALL_TIMEOUT;

    let (from, raw_info) = loop {
        // top up to the concurrency limit from whatever's queued
        while in_flight < MAX_CONCURRENT_FETCHES {
            let Some(peer) = pending.pop_front() else { break };
            in_flight += 1;
            let info_hash = magnet.info_hash;
            let identity = identity.clone();
            let result_tx = result_tx.clone();
            let utp = utp.borrow().clone();
            tokio::spawn(async move {
                let result = tokio::time::timeout(PER_PEER_TIMEOUT, fetch_from_peer(peer, info_hash, identity, utp))
                    .await
                    .unwrap_or_else(|_| Err(anyhow::anyhow!("timed out")));
                let _ = result_tx.send((peer, result)).await;
            });
        }

        tokio::select! {
            _ = tokio::time::sleep_until(deadline.min(give_up)) => {
                bail!(
                    "no metadata for {:?} after {}s (tried {} peers)",
                    magnet.display_name.as_deref().unwrap_or("magnet"),
                    tokio::time::Instant::now().duration_since(give_up - OVERALL_TIMEOUT).as_secs(),
                    tried.len(),
                );
            }
            _ = shutdown.cancelled() => bail!("cancelled while fetching metadata"),

            Some(event) = event_rx.recv() => {
                let SwarmEvent::PeersDiscovered(peers, _) = event else {
                    continue;
                };
                // queue every peer we haven't already tried; the loop head dials as many as
                // the concurrency limit allows and keeps the rest for when a slot frees up
                let before = pending.len();
                pending.extend(peers.into_iter().filter(|peer| tried.insert(*peer)));
                if pending.len() > before {
                    deadline = tokio::time::Instant::now() + IDLE_TIMEOUT;
                }
            }

            Some((peer, result)) = result_rx.recv() => {
                in_flight = in_flight.saturating_sub(1);
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
    bus.emit(Event::MetadataFetched {
        info_hash: magnet.info_hash,
        from,
        bytes: raw_info.len(),
        took_ms: started.elapsed().as_millis() as u64,
    });
    let torrent_file = build_torrent_file(&raw_info, &magnet.trackers);
    let torrent = parse_torrent(&torrent_file).context("metadata fetched from peers didn't parse as a torrent")?;
    Ok(Fetched {
        torrent,
        peers: tried.into_iter().collect(),
    })
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
    identity: Arc<Identity>,
    utp: Option<Arc<UtpSocketUdp>>,
) -> anyhow::Result<Vec<u8>> {
    let (stream, handshake) = crate::stream::connect(addr, &info_hash, &identity, utp.as_ref(), DialHints::default())
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

    ensure!(metadata_size > 0, "{addr} advertised a zero-length metadata size");
    ensure!(
        metadata_size <= 32 * 1024 * 1024,
        "{addr} advertised an implausible metadata size of {metadata_size} bytes"
    );

    let total_pieces = metadata_size.div_ceil(METADATA_PIECE_SIZE);
    let mut buffer = vec![0u8; metadata_size];
    let mut have: Vec<bool> = vec![false; total_pieces];

    // Ask for everything up front rather than one at a time: metadata is at most a few
    // hundred KiB, and a round trip per 16KiB piece would dominate the transfer.
    for piece in 0..total_pieces {
        writer
            .send(BtMessage::Extended(Extended {
                ext_id: their_id,
                payload: format!("d8:msg_typei0e5:piecei{piece}ee")
                    .into_bytes()
                    .into_boxed_slice(),
            }))
            .await?;
    }

    while have.iter().any(|got| !got) {
        let Some(msg) = reader.next().await else {
            bail!(
                "{addr} disconnected with {} metadata pieces still missing",
                have.iter().filter(|g| !**g).count()
            );
        };
        let BtMessage::Extended(ext) = msg? else { continue };
        if ext.ext_id != UT_METADATA_ID {
            continue;
        }

        let (piece, data) = match parse_data_message(&ext.payload) {
            Ok(Some(parsed)) => parsed,
            // a `reject` (msg_type 2), or a message we don't care about
            Ok(None) => bail!("{addr} rejected a metadata request"),
            Err(e) => bail!("{addr} sent a malformed ut_metadata message: {e:#}"),
        };

        ensure!(piece < total_pieces, "{addr} sent out-of-range metadata piece {piece}");
        let start = piece * METADATA_PIECE_SIZE;
        let end = (start + METADATA_PIECE_SIZE).min(metadata_size);
        ensure!(
            data.len() == end - start,
            "{addr} sent metadata piece {piece} with {} bytes, expected {}",
            data.len(),
            end - start
        );

        buffer[start..end].copy_from_slice(data);
        have[piece] = true;
    }

    // The whole reason this is safe to accept from an untrusted peer: the bytes have to hash
    // to the info hash we asked for, otherwise they're someone else's (or fabricated) metadata.
    let digest = Sha1::digest(&buffer);
    ensure!(
        digest.as_slice() == info_hash.0,
        "{addr} served metadata that doesn't match the requested info hash"
    );

    Ok(buffer)
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

/// Splits a ut_metadata `data` message into its piece index and raw bytes.
///
/// Returns `Ok(None)` for a `reject` (msg_type 2), which is a normal "I don't have it" answer
/// rather than a protocol error.
fn parse_data_message(payload: &[u8]) -> anyhow::Result<Option<(usize, &[u8])>> {
    let Ok((rest, dict)) = juicy_bencode::parse_bencode_dict(payload) else {
        bail!("not a bencoded dict");
    };
    let Some(BencodeItemView::Integer(msg_type)) = dict.get(b"msg_type".as_slice()) else {
        bail!("no msg_type");
    };
    // 0 = request (we're not serving here), 1 = data, 2 = reject
    if *msg_type != 1 {
        return Ok(None);
    }
    let Some(BencodeItemView::Integer(piece)) = dict.get(b"piece".as_slice()) else {
        bail!("data message with no piece index");
    };
    ensure!(*piece >= 0, "negative piece index {piece}");

    // BEP 9: the raw metadata bytes follow the bencoded dict directly, with no framing of
    // their own -- whatever the dict didn't consume is the payload.
    Ok(Some((*piece as usize, rest)))
}

/// Wraps a raw info dict back up into a complete `.torrent` file so the existing
/// `parse_torrent` can be reused verbatim.
///
/// The info dict is embedded byte-for-byte, so the info hash `parse_torrent` recomputes over it
/// is necessarily the same one we just verified against. Keys are emitted in ascending order
/// ("announce" < "announce-list" < "info") as bencode requires.
pub(crate) fn build_torrent_file(raw_info: &[u8], trackers: &[String]) -> Vec<u8> {
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
    use crate::wire::{BtDecoder, BtEncoder};
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

        let (piece, data) = parse_data_message(&msg).unwrap().unwrap();
        assert_eq!(piece, 2);
        assert_eq!(data, b"RAWBYTES");
    }

    /// A reject is a peer legitimately saying "I don't have that", not a malformed message.
    #[test]
    fn reject_message_is_not_an_error() {
        let msg = b"d8:msg_typei2e5:piecei0ee";
        assert!(parse_data_message(msg).unwrap().is_none());
    }

    /// End-to-end over a real loopback socket: a peer serving metadata (using this crate's own
    /// `build_ut_metadata_data_message`, the same serializer the real serve path uses) against
    /// `fetch_from_peer`. Exercises handshake, extension negotiation, multi-piece reassembly,
    /// and the info-hash check in one go.
    ///
    /// The mock deliberately advertises ut_metadata id **5**, not the id we advertise (1), so
    /// this fails if the fetcher ever assumes a fixed id instead of using the negotiated one.
    #[tokio::test]
    async fn fetches_multi_piece_metadata_from_a_real_peer() {
        use crate::peer::build_ut_metadata_data_message;
        use crate::wire::{read_handshake, send_handshake};
        use tokio::net::TcpListener;

        const THEIR_UT_METADATA_ID: u8 = 5;

        // big enough to span three 16KiB pieces, so reassembly and the short final piece both
        // get exercised rather than fitting in a single message
        let mut raw_info = sample_info_dict();
        let padding_needed = METADATA_PIECE_SIZE * 2 + 1234 - raw_info.len();
        let mut padded = raw_info[..raw_info.len() - 1].to_vec(); // drop trailing 'e'
        padded.extend_from_slice(&bencode_str(b"padding"));
        padded.extend_from_slice(&bencode_str(&vec![b'x'; padding_needed - 20]));
        padded.push(b'e');
        raw_info = padded;

        let info_hash = InfoHash::from_bytes(Sha1::digest(&raw_info).as_slice());
        let metadata_size = raw_info.len();
        assert!(
            metadata_size > METADATA_PIECE_SIZE * 2,
            "want a genuinely multi-piece fetch"
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let served = raw_info.clone();
        let server = tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let their_handshake = read_handshake(&mut tcp).await.unwrap();
            send_handshake(&mut tcp, &their_handshake.info_hash, &test_identity())
                .await
                .unwrap();

            let (reader, writer) = tcp.into_split();
            let mut reader = FramedRead::new(reader, BtDecoder);
            let mut writer = FramedWrite::new(writer, BtEncoder);

            // our extended handshake: note the non-default ut_metadata id
            writer
                .send(BtMessage::Extended(Extended {
                    ext_id: 0,
                    payload: format!(
                        "d1:md11:ut_metadatai{THEIR_UT_METADATA_ID}ee13:metadata_sizei{}ee",
                        served.len()
                    )
                    .into_bytes()
                    .into_boxed_slice(),
                }))
                .await
                .unwrap();

            let total_pieces = served.len().div_ceil(METADATA_PIECE_SIZE);
            let mut answered = 0;
            while answered < total_pieces {
                let Some(Ok(BtMessage::Extended(ext))) = reader.next().await else {
                    continue;
                };
                // ext_id 0 is the fetcher's own extended handshake, not a metadata request
                if ext.ext_id == 0 {
                    continue;
                }
                assert_eq!(
                    ext.ext_id, THEIR_UT_METADATA_ID,
                    "fetcher must use the id we advertised"
                );

                let (_, dict) = juicy_bencode::parse_bencode_dict(&ext.payload).unwrap();
                let Some(BencodeItemView::Integer(piece)) = dict.get(b"piece".as_slice()) else {
                    panic!("request had no piece index")
                };
                let piece = *piece as usize;

                let start = piece * METADATA_PIECE_SIZE;
                let end = (start + METADATA_PIECE_SIZE).min(served.len());
                let payload = build_ut_metadata_data_message(piece as u32, served.len() as u32, &served[start..end]);

                writer
                    .send(BtMessage::Extended(Extended {
                        // reply on the id *they* advertised, which the fetcher declares as 1
                        ext_id: UT_METADATA_ID,
                        payload: payload.into_boxed_slice(),
                    }))
                    .await
                    .unwrap();
                answered += 1;
            }
        });

        let fetched = fetch_from_peer(addr, info_hash, Arc::new(test_identity()), None)
            .await
            .unwrap();
        assert_eq!(fetched, raw_info, "fetched metadata must match byte-for-byte");
        server.await.unwrap();
    }

    /// The security property that makes it safe to take metadata from an untrusted peer: bytes
    /// that don't hash to the requested info hash must be rejected, not handed back.
    #[tokio::test]
    async fn rejects_metadata_that_doesnt_match_the_info_hash() {
        use crate::peer::build_ut_metadata_data_message;
        use crate::wire::{read_handshake, send_handshake};
        use tokio::net::TcpListener;

        let honest = sample_info_dict();
        let real_hash = InfoHash::from_bytes(Sha1::digest(&honest).as_slice());

        // what the peer actually serves: same length, different bytes
        let mut tampered = honest.clone();
        let last = tampered.len() - 2;
        tampered[last] ^= 0xff;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let hs = read_handshake(&mut tcp).await.unwrap();
            send_handshake(&mut tcp, &hs.info_hash, &test_identity()).await.unwrap();

            let (reader, writer) = tcp.into_split();
            let mut reader = FramedRead::new(reader, BtDecoder);
            let mut writer = FramedWrite::new(writer, BtEncoder);

            writer
                .send(BtMessage::Extended(Extended {
                    ext_id: 0,
                    payload: format!("d1:md11:ut_metadatai1ee13:metadata_sizei{}ee", tampered.len())
                        .into_bytes()
                        .into_boxed_slice(),
                }))
                .await
                .unwrap();

            while let Some(Ok(msg)) = reader.next().await {
                if let BtMessage::Extended(_) = msg {
                    let payload = build_ut_metadata_data_message(0, tampered.len() as u32, &tampered);
                    let _ = writer
                        .send(BtMessage::Extended(Extended {
                            ext_id: UT_METADATA_ID,
                            payload: payload.into_boxed_slice(),
                        }))
                        .await;
                }
            }
        });

        let err = fetch_from_peer(addr, real_hash, Arc::new(test_identity()), None)
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("doesn't match the requested info hash"),
            "expected an info-hash mismatch, got: {err:#}"
        );
    }

    /// Serves metadata for `raw_info` to one peer, using this crate's real serializer. Returns
    /// the address to point a fetcher at.
    async fn spawn_metadata_peer(raw_info: Vec<u8>) -> SocketAddr {
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
                    if send_handshake(&mut tcp, &hs.info_hash, &test_identity()).await.is_err() {
                        return;
                    }

                    let (reader, writer) = tcp.into_split();
                    let mut reader = FramedRead::new(reader, BtDecoder);
                    let mut writer = FramedWrite::new(writer, BtEncoder);

                    let handshake = format!("d1:md11:ut_metadatai1ee13:metadata_sizei{}ee", served.len());
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
                        if ext.ext_id == 0 {
                            continue;
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

    /// A barely-HTTP tracker that answers every announce with a compact peer list containing
    /// exactly `peers`, in order.
    async fn spawn_http_tracker_with(peers: Vec<SocketAddr>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mut compact = Vec::new();
        for peer in peers {
            let SocketAddr::V4(peer_v4) = peer else {
                panic!("test peer must be v4")
            };
            compact.extend_from_slice(&peer_v4.ip().octets());
            compact.extend_from_slice(&peer_v4.port().to_be_bytes());
        }

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

    /// The whole magnet path end to end: announce to a tracker, take the peer it returns, fetch
    /// the metadata from that peer over BEP 9, verify it, and build a usable `Torrent` -- with
    /// no DHT anywhere, which is the point.
    #[tokio::test]
    async fn resolves_a_magnet_link_into_a_torrent_via_a_tracker() {
        let raw_info = sample_info_dict();
        let info_hash = InfoHash::from_bytes(Sha1::digest(&raw_info).as_slice());

        let peer_addr = spawn_metadata_peer(raw_info.clone()).await;
        let tracker_url = spawn_http_tracker_with(vec![peer_addr]).await;

        let magnet = MagnetLink {
            info_hash,
            display_name: Some("hello".to_string()),
            trackers: vec![tracker_url],
        };
        let identity = Arc::new(Identity {
            peer_id: *b"-TEST01-000000000000",
            serving: "127.0.0.1:6881".parse().unwrap(),
            dht: false,
            encryption: crate::config::Encryption::Disabled,
        });

        let torrent = tokio::time::timeout(
            Duration::from_secs(20),
            fetch(
                &magnet,
                identity,
                CancellationToken::new(),
                crate::dht::Dht::none(),
                crate::utp::none(),
                EventBus::new(),
            ),
        )
        .await
        .expect("magnet resolution timed out")
        .expect("magnet resolution failed")
        .torrent;

        assert_eq!(torrent.info_hash, info_hash);
        assert_eq!(torrent.raw_info, raw_info);
        assert_eq!(torrent.total_size, 12);
        assert_eq!(torrent.files.len(), 1);
    }

    /// Regression: a tracker returning more peers than `MAX_CONCURRENT_FETCHES` used to have
    /// the overflow silently dropped, so if the first batch of peers were all dead we'd sit
    /// idle until the *next* announce -- an interval (often 30 minutes) later, i.e. far past
    /// any timeout. Here every peer before the last is a closed port, so this only passes if
    /// the overflow is queued and dialled as slots free up.
    #[tokio::test]
    async fn works_through_more_dead_peers_than_the_concurrency_limit() {
        use tokio::net::TcpListener;

        let raw_info = sample_info_dict();
        let info_hash = InfoHash::from_bytes(Sha1::digest(&raw_info).as_slice());

        // bind then immediately drop, so the addresses are real but refuse connections
        let dead_count = MAX_CONCURRENT_FETCHES + 4;
        let mut peers = Vec::new();
        for _ in 0..dead_count {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            peers.push(listener.local_addr().unwrap());
            drop(listener);
        }
        // the one live peer sits at the very end of the list
        let live = spawn_metadata_peer(raw_info.clone()).await;
        peers.push(live);

        let tracker_url = spawn_http_tracker_with(peers).await;
        let magnet = MagnetLink {
            info_hash,
            display_name: None,
            trackers: vec![tracker_url],
        };
        let identity = Arc::new(Identity {
            peer_id: *b"-TEST01-000000000000",
            serving: "127.0.0.1:6881".parse().unwrap(),
            dht: false,
            encryption: crate::config::Encryption::Disabled,
        });

        let torrent = tokio::time::timeout(
            Duration::from_secs(25),
            fetch(
                &magnet,
                identity,
                CancellationToken::new(),
                crate::dht::Dht::none(),
                crate::utp::none(),
                EventBus::new(),
            ),
        )
        .await
        .expect("should reach the live peer well before the timeout")
        .expect("magnet resolution failed")
        .torrent;

        assert_eq!(torrent.info_hash, info_hash);
    }
}
