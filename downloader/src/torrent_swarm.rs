use crate::defs::Identity;
use crate::peer::{Peer, ProtocolViolation, UT_METADATA_ID, UT_PEX_ID, parse_pex_message, parse_ut_metadata_request};
use crate::settings::{
    BLOCK_REQUEST_TIMEOUT, BLOCK_SIZE, CHOKING_ROUND_INTERVAL, KEEPALIVE_INTERVAL, MAX_UNCHOKED_PEERS,
    METADATA_PIECE_SIZE, OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS, PEER_TIMEOUT, PEX_INTERVAL, PEX_MAX_ADDED_PEERS,
};
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use crate::wire::{BitField, BtMessage, Piece, Request, shake_hands};
use anyhow::{self, Context, bail};
use bitvec::prelude::*;
use futures::StreamExt;
use futures::future::select_all;
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use rand::Rng;
use rand::seq::IndexedRandom;
use reqwest::Client;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpStream, UdpSocket, lookup_host};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, Sleep, interval, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use url::{Url, form_urlencoded};
use zerocopy::network_endian::{I32, I64, U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[allow(unused_imports)]
use derive_more::{Eq, PartialEq};

/// Handles announcements to a single tracker server
///
/// Keyed on a bare `InfoHash` rather than a whole `Torrent`: announcing only ever needs the
/// hash, and a magnet link's pre-metadata phase (see `metadata::fetch`) has nothing else to
/// give -- that's the whole reason it can reuse these announcers.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpAnnouncer {
    tracker: Url,
    info_hash: InfoHash,

    #[eq(skip)]
    identity: Arc<Identity>,
    #[eq(skip)]
    next_ready: Instant,
    #[eq(skip)]
    swarm_stat: watch::Receiver<TorrentSwarmStats>,
    #[eq(skip)]
    events: mpsc::Sender<SwarmEvent>,
    #[eq(skip)]
    sent_started: bool,
    #[eq(skip)]
    sent_completed: bool,
    #[eq(skip)]
    shutdown: CancellationToken,
}

/// Spawns a tracker announcer task per usable URL in `trackers`, reporting discovered peers to
/// `events`. Split out so a magnet link's pre-metadata phase can announce with nothing but an
/// info hash (see `metadata::fetch`) -- at that point there is no `Torrent` and no
/// `TorrentSwarm` to hang the announcers off.
pub(crate) fn spawn_announcers(
    trackers: &[String],
    info_hash: InfoHash,
    identity: Arc<Identity>,
    stat_rx: watch::Receiver<TorrentSwarmStats>,
    events: mpsc::Sender<SwarmEvent>,
    shutdown: CancellationToken,
) {
    for url in trackers.iter().filter_map(|t| Url::parse(t).ok()) {
        match url.scheme() {
            "http" | "https" => {
                let announcer = HttpAnnouncer::new(
                    url,
                    info_hash,
                    identity.clone(),
                    stat_rx.clone(),
                    events.clone(),
                    shutdown.clone(),
                );
                tokio::spawn(announcer.ev_loop());
            }
            "udp" => {
                let announcer = UdpAnnouncer::new(
                    url,
                    info_hash,
                    identity.clone(),
                    stat_rx.clone(),
                    events.clone(),
                    shutdown.clone(),
                );
                tokio::spawn(announcer.ev_loop());
            }
            scheme => warn!("ignoring tracker with unsupported scheme {scheme:?}"),
        }
    }
}

/// The tracker `event` parameter (BEP 3 for HTTP, BEP 15 for UDP). The first announce to a
/// tracker must be Started; a single announce reporting Completed should follow the download
/// finishing; Stopped is a courtesy announce on graceful shutdown so the tracker can drop us
/// immediately instead of waiting out the interval. Anything else is a regular periodic announce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnnounceEvent {
    Regular,
    Started,
    Completed,
    Stopped,
}

impl AnnounceEvent {
    fn http_str(self) -> Option<&'static str> {
        match self {
            AnnounceEvent::Regular => None,
            AnnounceEvent::Started => Some("started"),
            AnnounceEvent::Completed => Some("completed"),
            AnnounceEvent::Stopped => Some("stopped"),
        }
    }

    /// BEP 15's UDP tracker protocol encodes the same event as an int, with this (not the
    /// obvious 0/1/2/3-in-declaration-order) mapping.
    fn udp_code(self) -> i32 {
        match self {
            AnnounceEvent::Regular => Event::None as i32,
            AnnounceEvent::Completed => Event::Completed as i32,
            AnnounceEvent::Started => Event::Started as i32,
            AnnounceEvent::Stopped => Event::Stopped as i32,
        }
    }
}

impl HttpAnnouncer {
    fn new(
        tracker: Url,
        info_hash: InfoHash,
        identity: Arc<Identity>,
        swarm_stat: watch::Receiver<TorrentSwarmStats>,
        events: mpsc::Sender<SwarmEvent>,
        shutdown: CancellationToken,
    ) -> Self {
        debug_assert!({ tracker.scheme() == "http" || tracker.scheme() == "https" });
        let already_complete = swarm_stat.borrow().completed;

        HttpAnnouncer {
            tracker,
            info_hash,
            identity,
            next_ready: Instant::now() + Duration::from_millis(10),
            events,
            sent_started: false,
            // a torrent that was already complete when resumed must not announce
            // event=completed again (BEP 3)
            sent_completed: already_complete,
            swarm_stat,
            shutdown,
        }
    }

    /// The future resolves whenever the next round of announce is ready to be performed
    fn ready(&self) -> Sleep {
        sleep_until(self.next_ready)
    }

    /// Perform a single announce and return the interval and discovered peers
    #[tracing::instrument(skip(self))]
    async fn announce(&mut self, event: AnnounceEvent) -> anyhow::Result<Vec<SocketAddr>> {
        // percent encode info_hash and peer_id
        let info_hash_encoded: String = form_urlencoded::byte_serialize(&self.info_hash.0).collect();
        let peer_id_encoded: String = form_urlencoded::byte_serialize(&self.identity.peer_id).collect();

        let swarm_stat = self.swarm_stat.borrow().clone();

        // Build the announce URL
        let url = format!(
            "{tracker_url}?info_hash={info_hash}&peer_id={peer_id}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&compact=1{event}",
            tracker_url = self.tracker,
            info_hash = info_hash_encoded,
            peer_id = peer_id_encoded,
            port = self.identity.serving.port(),
            uploaded = swarm_stat.uploaded,
            downloaded = swarm_stat.downloaded,
            left = swarm_stat.left,
            event = event.http_str().map(|e| format!("&event={e}")).unwrap_or_default(),
        );

        let url = Url::parse(&url).unwrap();

        info!("Annoucing to {url}");

        // Send the GET request
        let client = Client::new();
        let response = client
            .get(url)
            .send()
            .await
            .with_context(|| format!("Failed to send http announce to {}", self.tracker))?;
        info!("Tracker [{}] responded with {}", self.tracker, response.status());

        let bytes = response
            .bytes()
            .await
            .with_context(|| format!("Failed to read the full range of bytes from {}", self.tracker))?;
        let bytes: &[u8] = &bytes;

        info!(
            "Tracker [{}] responded with {}",
            self.tracker,
            &String::from_utf8_lossy(bytes)
        );

        // Parse the bencoded response
        let parsed = juicy_bencode::parse_bencode_dict(bytes);
        let Ok((_remaining, mut dict)) = parsed else {
            bail!("Tracker [{}] responded with invalid bencoded content", self.tracker);
        };
        info!("Parsed bencode from tracker [{}] as {:?}", self.tracker, &dict);

        let mut peers = vec![];
        let Some(BencodeItemView::Integer(interval)) = dict.remove(b"interval".as_slice()) else {
            bail!(
                "Tracker [{}] responed with an non-integer as its announce interval",
                self.tracker
            );
        };

        if let Some(BencodeItemView::ByteString(peer_bytes)) = dict.remove(b"peers".as_slice()) {
            // Each peer is 6 bytes: 4 bytes IP, 2 bytes port
            for chunk in peer_bytes.chunks(6) {
                if chunk.len() != 6 {
                    break;
                }

                let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
                let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                peers.push(SocketAddr::V4(SocketAddrV4::new(ip, port)));
            }
        }

        // BEP 7: IPv6 peers come back separately under "peers6", same compact encoding but
        // 18 bytes each (16 bytes IP, 2 bytes port).
        if let Some(BencodeItemView::ByteString(peer_bytes)) = dict.remove(b"peers6".as_slice()) {
            for chunk in peer_bytes.chunks(18) {
                if chunk.len() != 18 {
                    break;
                }

                let ip = Ipv6Addr::from(<[u8; 16]>::try_from(&chunk[..16]).unwrap());
                let port = u16::from_be_bytes([chunk[16], chunk[17]]);
                peers.push(SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0)));
            }
        }

        self.next_ready = Instant::now() + Duration::from_secs(interval as u64);
        Ok(peers)
    }

    async fn ev_loop(mut self) {
        loop {
            tokio::select! {
                _ = self.ready() => {
                    // BEP 3: the first announce to a tracker must carry event=started
                    let event = if self.sent_started { AnnounceEvent::Regular } else { AnnounceEvent::Started };
                    match self.announce(event).await {
                        Ok(result) => {
                            self.sent_started = true;
                            let _ = self
                                .events
                                .send(SwarmEvent::PeersDiscovered(result))
                                .await;
                        }
                        Err(e) => {
                            tracing::error!("{:?}", e);
                            // TODO: use exponential backoff
                            self.next_ready = Instant::now() + Duration::from_mins(1);
                        }
                    }
                }
                // BEP 3: a single announce reporting event=completed should follow the
                // download finishing; watch the shared stats for that transition rather
                // than waiting for the next periodic announce, which could be minutes away
                Ok(()) = self.swarm_stat.changed(), if !self.sent_completed => {
                    if self.swarm_stat.borrow().completed && self.announce(AnnounceEvent::Completed).await.is_ok() {
                        self.sent_started = true;
                        self.sent_completed = true;
                    }
                }
                // BEP 3: send a courtesy event=stopped on graceful shutdown so the tracker
                // drops us immediately instead of waiting out the interval; best-effort,
                // since we're on our way out regardless of whether it succeeds
                _ = self.shutdown.cancelled() => {
                    let _ = tokio::time::timeout(Duration::from_secs(5), self.announce(AnnounceEvent::Stopped)).await;
                    break;
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UdpAnnouncer {
    tracker: Url,
    info_hash: InfoHash,

    #[eq(skip)]
    identity: Arc<Identity>,
    #[eq(skip)]
    next_ready: Instant,
    #[eq(skip)]
    swarm_stat: watch::Receiver<TorrentSwarmStats>,

    connection_id: i64,

    // TODO: Maybe this should be a weak sender?
    #[eq(skip)]
    event: mpsc::Sender<SwarmEvent>,

    #[eq(skip)]
    sent_started: bool,
    #[eq(skip)]
    sent_completed: bool,
    #[eq(skip)]
    shutdown: CancellationToken,
}

#[repr(i32)]
enum Action {
    Connect = 0,
    Announce = 1,
    #[allow(dead_code)]
    Scrape = 2,
    Error = 3,
}

#[repr(i32)]
enum Event {
    None = 0,
    Completed = 1,
    Started = 2,
    Stopped = 3,
}

impl UdpAnnouncer {
    fn new(
        tracker_url: Url,
        info_hash: InfoHash,
        identity: Arc<Identity>,
        swarm_stat: watch::Receiver<TorrentSwarmStats>,
        event: mpsc::Sender<SwarmEvent>,
        shutdown: CancellationToken,
    ) -> Self {
        debug_assert!(tracker_url.scheme() == "udp");
        let already_complete = swarm_stat.borrow().completed;
        UdpAnnouncer {
            tracker: tracker_url,
            info_hash,
            identity,
            next_ready: Instant::now() + Duration::from_millis(10),
            connection_id: 0, // sentinel, meaning we haven't got an id connetion yet because we
            // haven't done anything
            event,
            sent_started: false,
            // a torrent that was already complete when resumed must not announce
            // event=completed again (BEP 3)
            sent_completed: already_complete,
            swarm_stat,
            shutdown,
        }
    }

    /// The future resolves whenever the next round of announce is ready to be performed
    fn ready(&self) -> Sleep {
        sleep_until(self.next_ready)
    }

    /// Resolves the tracker's host to any address family; which one we get back determines
    /// which family we bind/connect our own UDP socket as (see `ev_loop`) and how we parse the
    /// peer list in the announce response (see `announce`).
    #[tracing::instrument(skip(self))]
    async fn resolve(&self) -> anyhow::Result<Option<SocketAddr>> {
        let host_name = self.tracker.host_str().unwrap();
        let host_port = self.tracker.port().unwrap();
        let query = format!("{}:{}", host_name, host_port);
        info!("Resolving {}", query);
        let mut addresses: Vec<_> = lookup_host(&query)
            .await
            .inspect_err(|e| info!("{:?}", e))
            .with_context(|| format!("Failed to resolve {}", &query))?
            .collect();

        info!("Looking up {} came back with {:?}", query, &addresses);

        Ok(addresses.pop())
    }

    #[tracing::instrument(skip(self))]
    async fn connect(&mut self, socket: &mut UdpSocket) -> anyhow::Result<()> {
        macro_rules! udp_log {
            ($level:ident, $fmt:literal $(, $args:expr)* $(,)?) => {
                $level!(
                    "Tracker [{}] on {}: {}",
                    self.tracker,
                    socket.peer_addr().unwrap(),
                    format_args!($fmt $(, $args)*)
                )
            };
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable)]
        #[repr(C)]
        struct Connect {
            connection_id: I64,
            action: I32,
            transaction_id: I32,
        }

        let transaction_id = rand::rng().random::<i32>();

        let connect = Connect {
            connection_id: 0x41727101980.into(),
            action: (Action::Connect as i32).into(),
            transaction_id: transaction_id.into(),
        };

        udp_log!(info, "Sending tracker connect to tracker");
        socket.send(connect.as_bytes()).await.with_context(|| {
            format!(
                "Failed to send connect packet to tracker [{}] on {}",
                self.tracker,
                socket.peer_addr().expect("Socket is already connected when passed")
            )
        })?;

        #[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable)]
        #[repr(C)]
        struct Response {
            action: I32,
            transaction_id: I32,
            connection_id: I64,
        }
        let mut response = Response::default();

        socket
            .recv(response.as_mut_bytes())
            .await
            .with_context(|| format!("Invalid response from tracker [{}]", self.tracker))?;

        self.connection_id = response.connection_id.into();

        let action: i32 = response.action.into();
        let txn_id: i32 = response.transaction_id.into();

        if transaction_id != txn_id {
            bail!("tracker transaction id didn't match our transaction_id");
        }
        if action == Action::Error as i32 {
            bail!("server errored on connect");
        }
        if action != 0 {
            bail!("server responsed with an action different than connection whilst we attempted to connect");
        }

        udp_log!(info, "Connection success");
        Ok(())
    }

    /// Perform a single announce and return the interval and discovered peers
    #[tracing::instrument(skip(self))]
    async fn announce(&mut self, socket: &mut UdpSocket, event: AnnounceEvent) -> anyhow::Result<Vec<SocketAddr>> {
        macro_rules! udp_log {
            ($level:ident, $fmt:literal $(, $args:expr)* $(,)?) => {
                $level!(
                    "Tracker [{}] on {}: {}",
                    self.tracker,
                    socket.peer_addr().unwrap(),
                    format_args!($fmt $(, $args)*)
                )
            };
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable)]
        #[repr(C)]
        struct Announce {
            connection_id: I64,
            action: I32,
            transaction_id: I32,
            info_hash: [u8; 20],
            peer_id: [u8; 20],
            downloaded: I64,
            left: I64,
            uploaded: I64,
            event: I32,
            ip: U32,
            key: U32,
            num_want: I32,
            port: U16,
            extensions: U16,
        }

        let transaction_id = rand::rng().random::<i32>();
        let key = rand::rng().random::<u32>();

        let swarm_stat = { self.swarm_stat.borrow().clone() };
        let downloaded: i64 = swarm_stat.downloaded.try_into().unwrap();
        let uploaded: i64 = swarm_stat.uploaded.try_into().unwrap();
        let left: i64 = swarm_stat.left.try_into().unwrap();

        let announce = Announce {
            connection_id: self.connection_id.into(),
            action: (Action::Announce as i32).into(),
            transaction_id: transaction_id.into(),
            info_hash: self.info_hash.0,
            peer_id: self.identity.peer_id,
            downloaded: downloaded.into(),
            left: left.into(),
            uploaded: uploaded.into(),
            event: event.udp_code().into(),
            ip: 0.into(), // i.e. let the tracker infer from the source packet
            key: key.into(),
            num_want: (-1).into(), // -1 is the default
            port: self.identity.serving.port().into(),
            extensions: 0.into(), // bitfield, i.e. 0 means no extensions
        };
        socket.send(announce.as_bytes()).await.with_context(|| {
            format!(
                "Failed to send announce packet to tracker [{}] on {}",
                self.tracker,
                socket.peer_addr().expect("Socket is connected")
            )
        })?;

        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Immutable, Default, KnownLayout, Unaligned,
        )]
        #[repr(C)]
        struct AnnounceResponseHeader {
            action: I32,
            transaction_id: I32,
            interval: I32, // in seconds
            leechers: I32,
            seeders: I32,
        }

        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable, KnownLayout, Unaligned,
        )]
        #[repr(C)]
        struct Peer {
            ip: I32,
            port: U16,
        }

        impl From<Peer> for SocketAddr {
            fn from(value: Peer) -> SocketAddr {
                let ip = Ipv4Addr::from_octets(value.ip.as_bytes().try_into().unwrap());
                let port = u16::from_be_bytes(value.port.as_bytes().try_into().unwrap());
                SocketAddr::V4(SocketAddrV4::new(ip, port))
            }
        }

        // The classic BEP 15 wire format only ever defined a 4-byte-IP peer entry; there's no
        // separate field distinguishing v4 from v6 the way BEP 7's "peers"/"peers6" split does
        // for HTTP trackers. Common tracker software instead just returns 18-byte (16 IP + 2
        // port) entries when the announce itself arrived over an IPv6 socket -- so which layout
        // to expect is determined by which family we connected to the tracker as, not by
        // anything in the response itself.
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable, KnownLayout, Unaligned,
        )]
        #[repr(C)]
        struct Peer6 {
            ip: [u8; 16],
            port: U16,
        }

        impl From<Peer6> for SocketAddr {
            fn from(value: Peer6) -> SocketAddr {
                let ip = Ipv6Addr::from(value.ip);
                let port = u16::from_be_bytes(value.port.as_bytes().try_into().unwrap());
                SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0))
            }
        }

        let is_v6 = matches!(socket.peer_addr(), Ok(SocketAddr::V6(_)));

        let mut buf = [0u8; 1500];
        let read_size = socket.recv(&mut buf).await.with_context(|| {
            format!(
                "Failed to read announce response packet to tracker [{}] on {}",
                self.tracker,
                socket.peer_addr().expect("Socket is connected")
            )
        })?;
        let buf = &buf[..read_size];

        // construct the response from raw bytes
        let header_size = size_of::<AnnounceResponseHeader>();
        if buf.len() < header_size {
            bail!(
                "announce response from tracker [{}] is shorter than a header",
                self.tracker
            );
        }
        let header =
            AnnounceResponseHeader::ref_from_bytes(&buf[..header_size]).expect("header alignment should be good");
        let peer_bytes = &buf[header_size..];

        let peers: Vec<SocketAddr> = if is_v6 {
            let peer_size = size_of::<Peer6>();
            if peer_bytes.len() % peer_size != 0 {
                bail!("trailing content is not a multiple of peer size");
            }
            let peers = <[Peer6]>::ref_from_bytes(peer_bytes).expect("shit should work");
            peers.iter().copied().map(SocketAddr::from).collect()
        } else {
            let peer_size = size_of::<Peer>();
            if peer_bytes.len() % peer_size != 0 {
                bail!("trailing content is not a multiple of peer size");
            }
            let peers = <[Peer]>::ref_from_bytes(peer_bytes).expect("shit should work");
            peers.iter().copied().map(SocketAddr::from).collect()
        };

        // verify the response is valid
        let action: i32 = header.action.into();
        if action == Action::Error as i32 {
            bail!("server errored on connect");
        }
        if action != Action::Announce as i32 {
            bail!("server responsed with an action different than connection whilst we attempted to announce");
        }

        let txn_id = header.transaction_id.into();
        if transaction_id != txn_id {
            bail!("tracker transaction id didn't match our transaction id");
        }

        // use the response

        // why the type casting insanity? because the protocol in their infinite wisdom decided using signed for interval was a good idea
        self.next_ready = Instant::now() + Duration::from_secs(i32::from(header.interval).try_into().unwrap());
        udp_log!(info, "Announce success, got {:?}", &peers);
        Ok(peers)
    }

    #[tracing::instrument(skip(self))]
    async fn ev_loop(mut self) -> anyhow::Result<()> {
        let tracker_addr = self.resolve().await.inspect_err(|e| warn!("{:?}", e))?;

        let Some(tracker_addr) = tracker_addr else {
            bail!("Tracker [{}] did not resolve to any address", self.tracker);
        };

        info!("Tracker [{}] resolved as {}", self.tracker, tracker_addr);

        // bind our own socket in the same family as the tracker resolved to
        let our_socket: SocketAddr = match tracker_addr {
            SocketAddr::V4(_) => SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into(),
            SocketAddr::V6(_) => SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0).into(),
        };
        info!("Binding to socket");
        let mut socket = UdpSocket::bind(our_socket)
            .await
            .with_context(|| format!("Failed to bind a udp socket on {our_socket}"))
            .inspect_err(|e| warn!("{:?}", e))?;

        info!("\"Connecting\" to {}", tracker_addr);
        socket
            .connect(tracker_addr)
            .await
            .with_context(|| format!("Failed to connect to addr: {}", tracker_addr))
            .inspect_err(|e| warn!("{:?}", e))?;

        self.connect(&mut socket).await.inspect_err(|e| warn!("{:?}", e))?;

        loop {
            tokio::select! {
                _ = self.ready() => {
                    // TODO: should retry instead of stopping at first failure
                    // BEP 15: the first announce to a tracker must carry event=started
                    let event = if self.sent_started { AnnounceEvent::Regular } else { AnnounceEvent::Started };
                    let peers = self
                        .announce(&mut socket, event)
                        .await
                        .with_context(|| format!("Tracker [{}] announce failed", self.tracker))?;
                    self.sent_started = true;

                    let _ = self
                        .event
                        .send(SwarmEvent::PeersDiscovered(peers))
                        .await;
                }
                // BEP 15: a single announce reporting event=completed should follow the
                // download finishing, rather than waiting for the next periodic announce
                Ok(()) = self.swarm_stat.changed(), if !self.sent_completed => {
                    if self.swarm_stat.borrow().completed
                        && self.announce(&mut socket, AnnounceEvent::Completed).await.is_ok()
                    {
                        self.sent_started = true;
                        self.sent_completed = true;
                    }
                }
                // BEP 15: send a courtesy event=stopped on graceful shutdown so the tracker
                // drops us immediately instead of waiting out the interval; best-effort
                _ = self.shutdown.cancelled() => {
                    let _ = tokio::time::timeout(
                        Duration::from_secs(5),
                        self.announce(&mut socket, AnnounceEvent::Stopped),
                    )
                    .await;
                    break;
                }
            }
        }

        Ok(())
    }
}

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

#[derive(Debug, Clone)]
pub struct TorrentSwarmHandle {
    tx: mpsc::Sender<SwarmEvent>,
}

impl TorrentSwarmHandle {
    /// Hands a freshly handshaken socket to the swarm, which owns it from here on. Used by both
    /// the inbound listener (`BtClient::accept_incoming`) and the swarm's own dial tasks.
    pub(crate) async fn peer_connected(&self, connected: ConnectedPeer) {
        let _ = self.tx.send(SwarmEvent::PeerConnected(connected)).await;
    }

    async fn dial_failed(&self, addr: SocketAddr) {
        let _ = self.tx.send(SwarmEvent::DialFailed(addr)).await;
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
}

/// How many pieces to keep in flight across all peers.
const MAX_INFLIGHT_PIECES: usize = 100;

pub struct TorrentSwarm {
    /// sorted by `remote_addr`; there's only ever one connection per address
    peers: Vec<Peer>,
    /// addresses with a dial in progress, so the same peer isn't dialed twice
    dialing: BTreeSet<SocketAddr>,
    /// rotates the order peers' sockets are polled in, so a chatty peer at the front of the
    /// list can't starve the rest (`select_all` returns the first ready future in order)
    poll_offset: usize,

    torrent: Arc<Torrent>,
    storage: Arc<TorrentStorage>,

    id: Arc<Identity>,

    http_announcers: Vec<HttpAnnouncer>,
    udp_announcers: Vec<UdpAnnouncer>,

    events_rx: mpsc::Receiver<SwarmEvent>,
    /// cloned into every task that reports back to the swarm (announcers, dials, the listener)
    events_tx: mpsc::Sender<SwarmEvent>,

    /// pieces neither verified nor in flight
    missing: Vec<u32>,
    in_flight: BTreeMap<u32, InFlight>,
    /// pieces completed in this session, UCB's `t`
    pieces_done: usize,

    stat: TorrentSwarmStats,
    stat_snapshot_tx: watch::Sender<TorrentSwarmStats>,
    /// Cloned out via `subscribe_stats` before `work_loop(self)` takes ownership of the swarm
    /// -- the clone stays valid (and keeps updating) after that, since it's independent of
    /// `TorrentSwarm` itself, just backed by the same channel `stat_snapshot_tx` publishes to.
    stat_snapshot_rx: watch::Receiver<TorrentSwarmStats>,

    shutdown: CancellationToken,
}

impl TorrentSwarm {
    pub fn new(
        torrent: Arc<Torrent>,
        storage: Arc<TorrentStorage>,
        id: Arc<Identity>,
        shutdown: CancellationToken,
    ) -> TorrentSwarm {
        let verified = bitvec![u8, Msb0; 0; torrent.pieces.len()].into_boxed_bitslice();
        Self::new_with_verified(torrent, storage, id, shutdown, verified)
    }

    /// Like `new`, but starting from pieces that are already on disk and hash-verified (a
    /// resumed download). Those pieces won't be requested again and count as had for `left`.
    pub fn new_with_verified(
        torrent: Arc<Torrent>,
        storage: Arc<TorrentStorage>,
        id: Arc<Identity>,
        shutdown: CancellationToken,
        verified: BitBox<u8, Msb0>,
    ) -> TorrentSwarm {
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

        let trackers = torrent.all_trackers();
        let trackers = trackers.into_iter().map(|s| Url::parse(&s)).filter_map(Result::ok);

        let (events_tx, events_rx) = mpsc::channel(512);

        let http_announcers: Vec<_> = trackers
            .clone()
            .filter(|t| t.scheme() == "http" || t.scheme() == "https")
            .map(|t| {
                HttpAnnouncer::new(
                    t,
                    torrent.info_hash,
                    id.clone(),
                    stat_rx.clone(),
                    events_tx.clone(),
                    shutdown.clone(),
                )
            })
            .collect();
        let udp_announcers: Vec<_> = trackers
            .filter(|t| t.scheme() == "udp")
            .map(|t| {
                UdpAnnouncer::new(
                    t,
                    torrent.info_hash,
                    id.clone(),
                    stat_rx.clone(),
                    events_tx.clone(),
                    shutdown.clone(),
                )
            })
            .collect();

        TorrentSwarm {
            peers: vec![],
            dialing: BTreeSet::new(),
            poll_offset: 0,
            torrent,
            storage,
            id,
            http_announcers,
            udp_announcers,
            events_rx,
            events_tx,
            missing,
            in_flight: BTreeMap::new(),
            pieces_done: 0,
            stat,
            stat_snapshot_tx: stat_tx,
            stat_snapshot_rx: stat_rx,
            shutdown,
        }
    }

    pub fn make_handle(&self) -> TorrentSwarmHandle {
        TorrentSwarmHandle {
            tx: self.events_tx.clone(),
        }
    }

    /// A live view of this torrent's aggregate progress (uploaded/downloaded/left/written/
    /// verified/completed). Must be called before `work_loop(self)` takes ownership of the
    /// swarm (e.g. right after `TorrentSwarm::new`, which is what `BtClient::add_torrent` does)
    /// -- the returned receiver keeps working after that, since it only depends on the
    /// underlying channel, not on `TorrentSwarm` itself still being reachable.
    pub(crate) fn subscribe_stats(&self) -> watch::Receiver<TorrentSwarmStats> {
        self.stat_snapshot_rx.clone()
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
    pub(crate) async fn work_loop(mut self) {
        let http_announcers = mem::take(&mut self.http_announcers);
        for http_announcer in http_announcers {
            tokio::spawn(http_announcer.ev_loop());
        }
        let udp_announcers = mem::take(&mut self.udp_announcers);
        for udp_announcer in udp_announcers {
            tokio::spawn(udp_announcer.ev_loop());
        }

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
                Some(event) = self.events_rx.recv() => self.process_event(event).await,
                _ = housekeeping_ticker.tick() => self.housekeeping().await,
                _ = keepalive_ticker.tick() => {
                    self.broadcast(|peer| Box::pin(peer.send_keepalive())).await;
                }
                _ = choking_ticker.tick() => {
                    choking_round += 1;
                    self.run_choking_algorithm(choking_round).await;
                }
                _ = pex_ticker.tick() => self.run_pex_round().await,
                _ = self.shutdown.cancelled() => break,
            }
        }
    }

    async fn process_event(&mut self, event: SwarmEvent) {
        match event {
            SwarmEvent::PeersDiscovered(peers) => self.connect_to_discovered_peers(peers),
            SwarmEvent::PeerConnected(connected) => self.add_peer(connected).await,
            SwarmEvent::DialFailed(addr) => {
                self.dialing.remove(&addr);
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
            // a peer that accepted a request and then just goes quiet (as opposed to
            // disconnecting outright) would otherwise hold its piece's slot forever
            stalled.extend(
                peer.requested
                    .iter()
                    .filter(|(_, at)| at.elapsed() > BLOCK_REQUEST_TIMEOUT)
                    .map(|(req, _)| req.index),
            );
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
            info!("piece {piece} timed out, will retry");
            self.fail_piece(piece);
        }

        self.schedule().await;
        self.publish_stats();
    }

    fn peer_index(&self, addr: SocketAddr) -> Option<usize> {
        self.peers.binary_search_by_key(&addr, |p| p.remote_addr).ok()
    }

    /// Removes a peer and puts whatever it was downloading for us back up for grabs.
    fn drop_peer(&mut self, idx: usize) {
        let peer = self.peers.remove(idx);
        let theirs: Vec<u32> = self
            .in_flight
            .iter()
            .filter(|(_, f)| f.peer == peer.remote_addr)
            .map(|(piece, _)| *piece)
            .collect();
        for piece in theirs {
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
        let msg = match peer.apply(msg) {
            Ok(None) => {
                // a Have/BitField/Unchoke may have just made a piece requestable
                self.schedule().await;
                return;
            }
            Ok(Some(msg)) => msg,
            Err(ProtocolViolation(what)) => {
                warn!("{} sent {what}, disconnecting", peer.remote_addr);
                self.drop_peer(idx);
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

        // never serve a piece we haven't hash-verified, and never serve more than the peer
        // actually asked for
        let verified = self.stat.verified.get(request.index as usize).is_some_and(|b| *b);
        let data = if verified {
            self.storage.read_piece(request.index).ok()
        } else {
            None
        };
        let begin = request.begin as usize;
        let end = data
            .as_ref()
            .map(|d| begin.saturating_add(request.length as usize).min(d.len()));

        let sent = match data.zip(end).filter(|&(_, end)| begin < end) {
            Some((data, end)) => {
                self.stat.uploaded += (end - begin) as u64;
                peer.send_block(Piece {
                    index: request.index,
                    begin: request.begin,
                    length: (end - begin) as u32,
                    data: Box::from(&data[begin..end]),
                })
                .await
            }
            None => peer.send_reject(request).await,
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
            return;
        }

        let piece = block.index;
        let buf = self.in_flight.remove(&piece).expect("checked above").buf;
        if let Err(e) = self.storage.write_piece(piece, buf.into_boxed_slice()) {
            warn!("couldn't write piece {piece}: {e:#}");
            self.missing.push(piece);
            return;
        }
        if !self.verify_hash(piece) {
            info!("piece {piece} failed hash verification, will retry");
            self.missing.push(piece);
            self.schedule().await;
            return;
        }

        info!("piece {piece} is completed");
        self.pieces_done += 1;
        self.stat.written += self.torrent.nth_piece_size(piece).expect("piece index in range");
        self.stat.left = self.torrent.total_size as usize - self.stat.written;
        self.stat.completed = self.stat.all_verified();
        // announcers watch this to send a prompt event=completed rather than waiting for
        // their next periodic announce, which could be minutes away
        self.publish_stats();

        self.broadcast(move |peer| Box::pin(peer.send_have(piece))).await;
        self.schedule().await;
    }

    /// Keeps up to `MAX_INFLIGHT_PIECES` pieces on the wire. Rarest-first picks *which piece*
    /// to go after next; UCB scoring separately picks *which peer* to ask -- the two compose
    /// rather than compete.
    async fn schedule(&mut self) {
        while self.in_flight.len() < MAX_INFLIGHT_PIECES {
            let Some(piece) = self.rarest_piece() else {
                break;
            };
            let Some(idx) = self.best_peer(piece) else {
                break;
            };

            let size = self.torrent.nth_piece_size(piece).expect("piece index in range");
            let blocks: Vec<Request> = (0..size)
                .step_by(BLOCK_SIZE)
                .map(|begin| Request {
                    index: piece,
                    begin: begin as u32,
                    length: (size - begin).min(BLOCK_SIZE) as u32,
                })
                .collect();
            self.missing.retain(|p| *p != piece);
            self.in_flight.insert(
                piece,
                InFlight {
                    peer: self.peers[idx].remote_addr,
                    buf: vec![0u8; size],
                    blocks_left: blocks.len(),
                },
            );
            info!("requesting piece {piece} from {}", self.peers[idx].remote_addr);
            for req in blocks {
                if self.peers[idx].request_block(req).await.is_err() {
                    self.drop_peer(idx);
                    return;
                }
            }
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

    /// UCB peer selection: of the peers that have `piece` and aren't choking us, the one with
    /// the highest upper confidence bound on its download speed.
    fn best_peer(&self, piece: u32) -> Option<usize> {
        self.peers
            .iter()
            .enumerate()
            .filter(|(_, p)| p.ready() && p.they_have(piece))
            .map(|(idx, p)| (idx, p.stats.score(self.pieces_done)))
            .max_by(|(_, l), (_, r)| l.total_cmp(r))
            .map(|(idx, _)| idx)
    }

    fn verify_hash(&mut self, piece: u32) -> bool {
        // a read failure means we can't confirm the piece, so treat it as unverified and let
        // it be retried -- never panic, this runs on the swarm's own event loop
        let Ok(written_data) = self.storage.read_piece(piece) else {
            warn!("couldn't read piece {piece} back off disk to verify it");
            return false;
        };

        let valid_piece = self.torrent.valid_piece(piece, &written_data);
        if valid_piece {
            self.stat.verified.set(piece as usize, true);
        }
        valid_piece
    }

    /// Takes ownership of a handshaken socket. Sends our side of the opening exchange (BEP 10
    /// extended handshake, then BitField/HaveAll/HaveNone, then Interested) before the peer
    /// joins `peers`, so nothing else can be written to it first.
    async fn add_peer(&mut self, connected: ConnectedPeer) {
        // `to_canonical()` collapses an IPv4-mapped IPv6 address (`::ffff:a.b.c.d`, what a v4
        // peer looks like when accepted on a dual-stack `[::]` listener) down to plain
        // `a.b.c.d`, so the same peer gets the same `remote_addr` whether we dialed it or it
        // dialed us
        let remote_addr = SocketAddr::new(connected.remote_addr.ip().to_canonical(), connected.remote_addr.port());
        self.dialing.remove(&remote_addr);
        let Err(insert_at) = self.peers.binary_search_by_key(&remote_addr, |p| p.remote_addr) else {
            info!("{remote_addr} is already connected, dropping the duplicate");
            return;
        };

        let mut peer = Peer::new(
            connected.tcp,
            remote_addr,
            self.torrent.pieces.len(),
            connected.remote_supports_fast,
        );
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
            return;
        }

        info!("{remote_addr} connected, {} peers now", self.peers.len() + 1);
        self.peers.insert(insert_at, peer);
    }

    /// Dials every address in `peers` we're not already connected to or dialing. Shared by
    /// tracker-discovered peers and BEP 11 (PEX) peers -- both are just addresses.
    fn connect_to_discovered_peers(&mut self, peers: Vec<SocketAddr>) {
        for addr in peers {
            if self.peer_index(addr).is_some() || !self.dialing.insert(addr) {
                continue;
            }
            let handle = self.make_handle();
            let torrent = self.torrent.clone();
            let our_id = self.id.clone();
            tokio::spawn(async move {
                match dial(addr, &torrent, &our_id).await {
                    Ok(connected) => handle.peer_connected(connected).await,
                    Err(e) => {
                        info!("Failed to connect to {addr}: {e:?}");
                        handle.dial_failed(addr).await;
                    }
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
            .map(|p| (p.remote_addr, p.stats.mean_rx))
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
    let mut tcp = TcpStream::connect(addr)
        .await
        .with_context(|| format!("Failed to established tcp stream with {addr}"))?;
    let handshake = shake_hands(&mut tcp, &torrent.info_hash, &our_id.peer_id)
        .await
        .with_context(|| format!("Failed to complete handshake with {addr}"))?;
    info!("Peer connection to {addr} established");
    Ok(ConnectedPeer {
        tcp,
        remote_addr: addr,
        remote_supports_extensions: crate::wire::supports_extensions(&handshake.extensions),
        remote_supports_fast: crate::wire::supports_fast_extension(&handshake.extensions),
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::metadata::build_torrent_file;
    use crate::torrent::parse_torrent;
    use crate::wire::BtCodec;
    use futures::SinkExt;
    use sha1::{Digest, Sha1};
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
    fn swarm(name: &str) -> (TorrentSwarm, PathBuf) {
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

        let torrent = Arc::new(torrent);
        let storage = Arc::new(TorrentStorage::new(torrent.clone(), vec![file]));
        let id = Arc::new(Identity {
            peer_id: *b"-DL0100-swarm-test..",
            serving: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into(),
        });
        (TorrentSwarm::new(torrent, storage, id, CancellationToken::new()), path)
    }

    /// Connects a fake remote peer to the swarm: the swarm gets one end of a localhost socket
    /// (as if it had just completed a handshake), the test keeps the other.
    async fn fake_peer(handle: &TorrentSwarmHandle, pretend_addr: &str) -> Framed<TcpStream, BtCodec> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let ours = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (theirs, _) = listener.accept().await.unwrap();
        handle
            .peer_connected(ConnectedPeer {
                tcp: ours,
                remote_addr: pretend_addr.parse().unwrap(),
                remote_supports_extensions: false,
                remote_supports_fast: false,
            })
            .await;
        Framed::new(theirs, BtCodec)
    }

    /// The fake peer's side of the opening exchange: it expects our BitField and Interested,
    /// then declares it has everything and unchokes us.
    async fn open_as_seeder(peer: &mut Framed<TcpStream, BtCodec>) {
        let Some(Ok(BtMessage::BitField(_))) = peer.next().await else {
            panic!("expected our bitfield first");
        };
        let Some(Ok(BtMessage::Interested(_))) = peer.next().await else {
            panic!("expected Interested after the bitfield");
        };
        peer.send(BtMessage::BitField(BitField {
            has: vec![0xFF; 1].into(),
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
        let (swarm, path) = swarm("happy");
        let handle = swarm.make_handle();
        let mut stats = swarm.subscribe_stats();
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

    /// A peer that vanishes mid-piece must not keep that piece's slot: another peer has to be
    /// asked for it. With only 3 pieces and room for 100 in flight, all of them are in flight
    /// with the first peer when it hangs up.
    #[tokio::test]
    async fn pieces_abandoned_by_a_dead_peer_are_requested_from_another() {
        let (swarm, path) = swarm("dead");
        let handle = swarm.make_handle();
        let mut stats = swarm.subscribe_stats();
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
        let (swarm, path) = swarm("choked");
        let handle = swarm.make_handle();
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
}
