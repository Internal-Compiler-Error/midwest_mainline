//! A swarm over a small torrent and fake peers on localhost sockets, for the tests of every
//! part of the swarm.

pub(super) use super::{ConnectedPeer, Shared, SwarmEvent, TorrentSwarm, TorrentSwarmHandle, TorrentSwarmStats};
pub(super) use crate::defs::Identity;
pub(super) use crate::events::{EventBus, PeerSource};
pub(super) use crate::external::ExternalAddress;
pub(super) use crate::limiter::RateLimiter;
pub(super) use crate::metadata::build_torrent_file;
pub(super) use crate::settings::{CHOKING_ROUND_INTERVAL, MIN_REQUEST_WINDOW};
pub(super) use crate::storage::TorrentStorage;
pub(super) use crate::stream::PeerStream;
pub(super) use crate::torrent::parse_torrent;
pub(super) use crate::wire::{BitField, BtCodec, BtMessage, Piece, Request};
pub(super) use bitvec::prelude::*;
pub(super) use futures::SinkExt;
pub(super) use futures::StreamExt;
pub(super) use sha1::{Digest, Sha1};
pub(super) use std::collections::{BTreeMap, BTreeSet};
pub(super) use std::net::{Ipv4Addr, SocketAddrV4};
pub(super) use std::path::PathBuf;
pub(super) use std::sync::Arc;
pub(super) use std::time::Duration;
pub(super) use tokio::net::TcpListener;
pub(super) use tokio::sync::watch;
pub(super) use tokio_util::codec::Framed;

pub(super) type Wire = Framed<tokio::net::TcpStream, BtCodec>;

/// 2 full blocks and a short one
pub(super) const PIECE: usize = 40_000;
/// 3 pieces, the last one short
pub(super) const TOTAL: usize = 100_000;

pub(super) fn content() -> Vec<u8> {
    (0..TOTAL).map(|i| (i * 31 % 253) as u8).collect()
}

/// A swarm for a single-file torrent of `content()`, its target file in a scratch dir, and
/// no announcers (the only tracker URL has a scheme no announcer handles).
pub(super) fn swarm(name: &str) -> (TorrentSwarm, TorrentSwarmHandle, PathBuf) {
    swarm_with(name, false)
}

/// `seeding`: the file already holds `content()` and every piece counts as verified.
pub(super) fn swarm_with(name: &str, seeding: bool) -> (TorrentSwarm, TorrentSwarmHandle, PathBuf) {
    swarm_with_settings(name, seeding, crate::config::Settings::default())
}

pub(super) fn swarm_with_settings(
    name: &str,
    seeding: bool,
    settings: crate::config::Settings,
) -> (TorrentSwarm, TorrentSwarmHandle, PathBuf) {
    swarm_with_web_seeds(name, seeding, settings, vec![])
}

pub(super) fn swarm_with_web_seeds(
    name: &str,
    seeding: bool,
    settings: crate::config::Settings,
    web_seeds: Vec<String>,
) -> (TorrentSwarm, TorrentSwarmHandle, PathBuf) {
    let mut torrent = single_file_torrent();
    let dir = std::env::temp_dir().join(format!("downloader-swarm-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("swarm.bin");
    torrent.files[0].path = path.clone();
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
        std::fs::write(&path, content()).unwrap();
    }

    let torrent = Arc::new(torrent);
    let storage = Arc::new(TorrentStorage::new(torrent.clone(), vec![Some(file)]));
    let verified = bitvec![u8, Msb0; seeding as u8; 3].into_boxed_bitslice();
    let (settings_tx, settings) = watch::channel(settings);
    std::mem::forget(settings_tx);
    let (swarm, handle) = TorrentSwarm::new(torrent, storage, verified, shared(settings));
    (swarm, handle, path)
}

/// `content()` as a single-file torrent of 3 pieces, with a tracker URL whose scheme no
/// announcer handles.
pub(super) fn single_file_torrent() -> crate::torrent::Torrent {
    let pieces: Vec<u8> = content().chunks(PIECE).flat_map(|c| Sha1::digest(c).to_vec()).collect();
    let mut info = format!(
        "d6:lengthi{TOTAL}e4:name9:swarm.bin12:piece lengthi{PIECE}e6:pieces{}:",
        pieces.len()
    )
    .into_bytes();
    info.extend_from_slice(&pieces);
    info.push(b'e');
    parse_torrent(&build_torrent_file(&info, &["wss://unused.test/announce".to_string()])).unwrap()
}

/// `content()` as two files over three pieces: `a` is pieces 0 and 1, `b` is pieces 1 and 2.
/// Returns the directory the files are in.
pub(super) fn two_file_swarm(name: &str) -> (TorrentSwarm, TorrentSwarmHandle, PathBuf) {
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

    let dir = std::env::temp_dir().join(format!("downloader-swarm-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut handles = vec![];
    for crate::TorrentFile { len: size, path, .. } in &mut torrent.files {
        *path = dir.join(path.file_name().unwrap());
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.set_len(*size).unwrap();
        handles.push(Some(file));
    }
    let torrent = Arc::new(torrent);
    let storage = Arc::new(TorrentStorage::new(torrent.clone(), handles));
    let verified = bitvec![u8, Msb0; 0; 3].into_boxed_bitslice();
    let shared = shared(crate::bt_client::default_settings());
    let (swarm, handle) = TorrentSwarm::new(torrent, storage, verified, shared);
    (swarm, handle, dir)
}

/// The client-wide services, as a client without DHT or uTP has them.
pub(super) fn shared(settings: crate::config::SettingsWatch) -> Shared {
    Shared {
        events: EventBus::new(),
        id: Arc::new(Identity {
            peer_id: *b"-DL0100-swarm-test..",
            serving: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into(),
            dht: false,
            encryption: crate::config::Encryption::Prefer,
        }),
        dht: crate::dht::Dht::none(),
        utp: crate::utp::none(),
        settings: settings.clone(),
        limiter: Arc::new(RateLimiter::new(settings)),
        external: ExternalAddress::default(),
        shutdown: tokio_util::sync::CancellationToken::new(),
    }
}

/// Connects a fake remote peer to the swarm: the swarm gets one end of a localhost socket
/// (as if it had just completed a handshake), the test keeps the other.
pub(super) async fn fake_peer(handle: &TorrentSwarmHandle, pretend_addr: &str) -> Wire {
    fake_peer_with(handle, pretend_addr, false).await
}

pub(super) async fn fake_peer_with(handle: &TorrentSwarmHandle, pretend_addr: &str, fast: bool) -> Wire {
    let (connected, theirs) = fake_connection(pretend_addr, fast).await;
    handle.peer_connected(connected).await;
    theirs
}

/// One end of a localhost socket as if it had just completed a handshake with
/// `pretend_addr`, for handing to a swarm, and the other end as the remote peer.
pub(super) async fn fake_connection(pretend_addr: &str, fast: bool) -> (ConnectedPeer, Wire) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let ours = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (theirs, _) = listener.accept().await.unwrap();
    let connected = ConnectedPeer {
        stream: PeerStream::Tcp(ours),
        dialed: false,
        remote_addr: pretend_addr.parse().unwrap(),
        remote_supports_extensions: false,
        remote_supports_fast: fast,
        remote_supports_dht: false,
        remote_supports_v2: false,
        peer_id: *b"-TS0001-fake-peer-id",
    };
    (connected, Framed::new(theirs, BtCodec))
}

/// The fake peer's side of the opening exchange: it expects our BitField and Interested,
/// then declares it has everything and unchokes us.
pub(super) async fn open_as_seeder(peer: &mut Wire) {
    open_with(peer, 0xFF).await;
}

/// Like `open_as_seeder`, but the peer declares only the pieces set in `bitfield`.
pub(super) async fn open_with(peer: &mut Wire, bitfield: u8) {
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

pub(super) fn block(req: Request) -> BtMessage {
    let start = req.index as usize * PIECE + req.begin as usize;
    BtMessage::Piece(Piece {
        index: req.index,
        begin: req.begin,
        length: req.length,
        data: Box::from(&content()[start..start + req.length as usize]),
    })
}

pub(super) async fn wait_until_complete(stats: &mut watch::Receiver<TorrentSwarmStats>) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !stats.borrow_and_update().completed {
            stats.changed().await.unwrap();
        }
    })
    .await
    .expect("download didn't complete in time");
}
