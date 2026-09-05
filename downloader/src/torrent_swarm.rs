use crate::defs::Identity;
use crate::download::{Download, DownloadEvent};
use crate::peer::{PeerCommands, PeerEvent, PeerHandle};
use crate::settings::{
    CHOKING_ROUND_INTERVAL, MAX_UNCHOKED_PEERS, METADATA_PIECE_SIZE, OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS, PEX_INTERVAL,
    PEX_MAX_ADDED_PEERS,
};
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use midwest_mainline::types::InfoHash;
use crate::wire::{BitField, Piece, shake_hands};
use anyhow::{self, Context, bail};
use bitvec::boxed::BitBox;
use bitvec::order::Msb0;
use juicy_bencode::BencodeItemView;
use rand::Rng;
use rand::seq::IndexedRandom;
use reqwest::Client;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpStream, UdpSocket, lookup_host};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, Sleep, interval, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use url::{Url, form_urlencoded};
use zerocopy::network_endian::{I32, I64, U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use futures::future::join_all;

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
    events: mpsc::Sender<TorrentSwarmCommand>,
    #[eq(skip)]
    sent_started: bool,
    #[eq(skip)]
    sent_completed: bool,
    #[eq(skip)]
    shutdown: CancellationToken,
}

pub enum AnnouncerEvent {
    DiscoveredPeers(Vec<SocketAddr>),
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
    events: mpsc::Sender<TorrentSwarmCommand>,
    shutdown: CancellationToken,
) {
    for url in trackers.iter().filter_map(|t| Url::parse(t).ok()) {
        match url.scheme() {
            "http" | "https" => {
                let announcer =
                    HttpAnnouncer::new(url, info_hash, identity.clone(), stat_rx.clone(), events.clone(), shutdown.clone());
                tokio::spawn(announcer.ev_loop());
            }
            "udp" => {
                let announcer =
                    UdpAnnouncer::new(url, info_hash, identity.clone(), stat_rx.clone(), events.clone(), shutdown.clone());
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
        events: mpsc::Sender<TorrentSwarmCommand>,
        shutdown: CancellationToken,
    ) -> Self {
        debug_assert!({ tracker.scheme() == "http" || tracker.scheme() == "https" });

        HttpAnnouncer {
            tracker,
            info_hash,
            identity,
            next_ready: Instant::now() + Duration::from_millis(10),
            swarm_stat,
            events,
            sent_started: false,
            sent_completed: false,
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
        info!(
            "Parsed bencode from tracker [{}] as {:?}",
            self.tracker, &dict
        );

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
                                .send(TorrentSwarmCommand::ProcessAnnounceEvent(
                                    AnnouncerEvent::DiscoveredPeers(result),
                                ))
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
    event: mpsc::Sender<TorrentSwarmCommand>,

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
        event: mpsc::Sender<TorrentSwarmCommand>,
        shutdown: CancellationToken,
    ) -> Self {
        debug_assert!(tracker_url.scheme() == "udp");
        UdpAnnouncer {
            tracker: tracker_url,
            info_hash,
            identity,
            next_ready: Instant::now() + Duration::from_millis(10),
            swarm_stat,
            connection_id: 0, // sentinel, meaning we haven't got an id connetion yet because we
            // haven't done anything
            event,
            sent_started: false,
            sent_completed: false,
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
            bail!("announce response from tracker [{}] is shorter than a header", self.tracker);
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
                        .send(TorrentSwarmCommand::ProcessAnnounceEvent(
                            AnnouncerEvent::DiscoveredPeers(peers),
                        ))
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

#[derive(Clone, Debug)]
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
    tx: mpsc::Sender<TorrentSwarmCommand>,
}

/// Builds `PeerHandle`s for a specific torrent's swarm without holding a reference into
/// `TorrentSwarm` itself -- everything it needs (an `Arc<Torrent>`, a command-channel sender)
/// is cheap to clone and hands off safely across tasks. Used by the inbound connection
/// listener, which accepts a socket before it knows which swarm it belongs to.
#[derive(Clone)]
pub(crate) struct PeerFactory {
    torrent: Arc<Torrent>,
    event_tx: mpsc::Sender<TorrentSwarmCommand>,
}

impl PeerFactory {
    pub(crate) fn accept(
        &self,
        tcp_stream: TcpStream,
        remote_peer_id: [u8; 20],
        remote_supports_extensions: bool,
        remote_supports_fast: bool,
    ) -> PeerHandle {
        PeerHandle::new(
            tcp_stream,
            remote_peer_id,
            self.event_tx.clone(),
            &self.torrent,
            remote_supports_extensions,
            remote_supports_fast,
        )
    }
}

impl TorrentSwarmHandle {
    pub async fn add_initialized_peer(&self, peer: PeerHandle) {
        let _ = self
            .tx
            .send(TorrentSwarmCommand::SelfCommand(
                TorrentSwarmSelfCommand::HandleNewPeerConnection(peer),
            ))
            .await;
    }

    pub async fn handle_discovered_peers(&self, peers: Vec<SocketAddr>) {
        let _ = self
            .tx
            .send(TorrentSwarmCommand::SelfCommand(
                TorrentSwarmSelfCommand::HandleNewDiscoveredPeers(peers),
            ))
            .await;
    }

    async fn dial_failed(&self, addr: SocketAddr) {
        let _ = self
            .tx
            .send(TorrentSwarmCommand::SelfCommand(TorrentSwarmSelfCommand::DialFailed(addr)))
            .await;
    }
}

pub(crate) enum TorrentSwarmCommand {
    ProcessPeerEvent { from: SocketAddr, event: PeerEvent },
    ProcessDownloadEvent(DownloadEvent),
    ProcessAnnounceEvent(AnnouncerEvent),
    SelfCommand(TorrentSwarmSelfCommand),
}

pub(crate) enum TorrentSwarmSelfCommand {
    HandleNewPeerConnection(PeerHandle),
    HandleNewDiscoveredPeers(Vec<SocketAddr>),
    /// `Download` runs as its own task and has no direct (aliasing-unsafe) access to
    /// `TorrentSwarm`'s peer list, so it asks for a pick over the command channel instead.
    ChooseBestPeer {
        piece: u32,
        total_piece_requested: usize,
        resp: oneshot::Sender<Option<PeerHandle>>,
    },
    QueryAllVerified { resp: oneshot::Sender<bool> },
    /// Rarest-first piece selection: pick whichever of `candidates` the fewest active peers
    /// have (ties broken randomly), so swarm-wide availability stays balanced. This decides
    /// *which piece* to request next; `ChooseBestPeer`'s UCB scoring separately decides *which
    /// peer* to request it from -- the two compose rather than compete.
    PickRarestPiece {
        candidates: Vec<u32>,
        resp: oneshot::Sender<Option<u32>>,
    },
    /// A dial spawned by `connect_to_discovered_peers` failed. Without this, a `PendingPeer`
    /// entry for an address that never connects would sit in `pending_peers` forever (it's
    /// only ever removed on success, in `HandleNewPeerConnection`) -- and since dedup against
    /// re-discovering the same address checks `pending_peers`, that address could never be
    /// retried. Worse now that BEP 11 (PEX) feeds this same path every round: an address a
    /// peer keeps re-gossiping needs to actually leave the list on failure or it never clears.
    DialFailed(SocketAddr),
}

pub struct TorrentSwarm {
    active_peers: Vec<PeerHandle>,
    pending_peers: Vec<PendingPeer>,

    torrent: Arc<Torrent>,
    storage: Arc<TorrentStorage>,

    id: Arc<Identity>,

    http_announcers: Vec<HttpAnnouncer>,
    udp_announcers: Vec<UdpAnnouncer>,

    inbound_msgs: mpsc::Receiver<TorrentSwarmCommand>,
    /// we keep a sender so we can clone it and give it to objects that generate on run time who need it
    outbound_msgs: mpsc::Sender<TorrentSwarmCommand>,

    stat: TorrentSwarmStats,
    stat_snapshot_tx: watch::Sender<TorrentSwarmStats>,
    /// Cloned out via `subscribe_stats` before `work_loop(self)` takes ownership of the swarm
    /// -- the clone stays valid (and keeps updating) after that, since it's independent of
    /// `TorrentSwarm` itself, just backed by the same channel `stat_snapshot_tx` publishes to.
    stat_snapshot_rx: watch::Receiver<TorrentSwarmStats>,

    shutdown: CancellationToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingPeer {
    pub socket_addr: SocketAddr,
    /// It's possible that we would have completed a piece *after* we've started to connect and sent a bitfield but *before* the connection is established
    /// they need to be informed
    pub pending_messages: Vec<u32>,
}

impl PartialOrd for PendingPeer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

impl Ord for PendingPeer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.socket_addr.cmp(&other.socket_addr)
    }
}

impl TorrentSwarm {
    pub fn new(
        torrent: Arc<Torrent>,
        storage: Arc<TorrentStorage>,
        id: Arc<Identity>,
        shutdown: CancellationToken,
    ) -> TorrentSwarm {
        let verified = vec![false; torrent.pieces.len()];
        let verified: BitBox<u8, Msb0> = BitBox::from_iter(verified.iter());
        // TODO: this only works for fresh downloads
        // TODO: verified is not updated
        let stat = TorrentSwarmStats {
            uploaded: 0,
            downloaded: 0,
            left: torrent.total_size as usize,
            written: 0,
            verified,
            completed: false,
        };
        let (stat_tx, stat_rx) = watch::channel(stat.clone());

        let trackers = torrent.all_trackers();
        let trackers = trackers.into_iter().map(|s| Url::parse(&s)).filter_map(Result::ok);

        let (command_tx, command_rx) = mpsc::channel(512);

        let http_announcers: Vec<_> = trackers
            .clone()
            .filter(|t| t.scheme() == "http" || t.scheme() == "https")
            .map(|t| {
                HttpAnnouncer::new(
                    t,
                    torrent.info_hash,
                    id.clone(),
                    stat_rx.clone(),
                    command_tx.clone(),
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
                    command_tx.clone(),
                    shutdown.clone(),
                )
            })
            .collect();

        TorrentSwarm {
            active_peers: vec![],
            pending_peers: vec![],
            torrent,
            storage,
            id,
            http_announcers: http_announcers,
            udp_announcers: udp_announcers,
            stat,
            stat_snapshot_tx: stat_tx,
            stat_snapshot_rx: stat_rx,
            inbound_msgs: command_rx,
            outbound_msgs: command_tx,
            shutdown,
        }
    }

    pub fn aggregate_peer_stats(&mut self) {
        let mut uploaded = 0;
        let mut downloaded = 0;
        for p in self.active_peers.iter() {
            let snapshot = p.stats.borrow();
            uploaded += snapshot.sent;
            downloaded += snapshot.received;
        }

        self.stat.downloaded = downloaded as u64;
        self.stat.uploaded = uploaded as u64;
        let _ = self.stat_snapshot_tx.send(self.stat.clone());
    }

    pub fn make_handle(&self) -> TorrentSwarmHandle {
        TorrentSwarmHandle {
            tx: self.outbound_msgs.clone(),
        }
    }

    /// A cloneable sender onto this swarm's command queue, for tasks (like `Download`) that
    /// need to talk to the swarm without holding a reference into it.
    pub(crate) fn command_sender(&self) -> mpsc::Sender<TorrentSwarmCommand> {
        self.outbound_msgs.clone()
    }

    /// A cloneable factory for building `PeerHandle`s for this swarm's torrent, for the
    /// inbound connection listener (which accepts a socket before it knows which swarm it's for).
    pub(crate) fn peer_factory(&self) -> PeerFactory {
        PeerFactory {
            torrent: self.torrent.clone(),
            event_tx: self.outbound_msgs.clone(),
        }
    }

    pub(crate) async fn work_loop(mut self) {
        let mut aggregate_ticker = interval(Duration::from_mins(1));
        let mut choking_ticker = interval(CHOKING_ROUND_INTERVAL);
        let mut choking_round: u64 = 0;
        let mut pex_ticker = interval(PEX_INTERVAL);

        // `Download` only holds owned/shared (Arc, Sender) state -- it talks to the swarm
        // over the command channel rather than borrowing it, so it can run as its own task
        // instead of needing to be polled alongside `&mut self` here.
        let download = Download::new(&self);
        tokio::spawn(async move {
            download.download_loop().await;
            info!("download ended");
        });

        // TODO: probably store the join handles so they can be aborted when necessary
        let http_announcers = mem::take(&mut self.http_announcers);
        for http_announcer in http_announcers {
            tokio::spawn(http_announcer.ev_loop());
        }

        let udp_announcers = mem::take(&mut self.udp_announcers);
        for udp_announcer in udp_announcers {
            tokio::spawn(udp_announcer.ev_loop());
        }

        loop {
            tokio::select! {
                _ = aggregate_ticker.tick() => {
                    self.aggregate_peer_stats();
                }
                _ = choking_ticker.tick() => {
                    choking_round += 1;
                    self.run_choking_algorithm(choking_round).await;
                }
                _ = pex_ticker.tick() => {
                    self.run_pex_round().await;
                }
                Some(command) = self.inbound_msgs.recv() => self.process_command(command).await,
                // without this, this loop (and so the `JoinHandle` `BtClient::work` awaits for
                // this torrent) never ends on its own -- `accept_incoming` and the announcer
                // loops already check `shutdown.cancelled()`, this one just never did
                _ = self.shutdown.cancelled() => break,
            }
        }
    }

    pub fn best_peer(&self, piece: u32, total_piece_requested: usize) -> Option<PeerHandle> {
        info!(
            "Active peers: {}, pending peers: {}",
            self.active_peers.len(),
            self.pending_peers.len()
        );

        let candidates: Vec<_> = self
            .active_peers
            .iter()
            .filter(|p| {
                let state = p.state();
                state.ready() && state.they_have(piece)
            })
            .map(|h| {
                let stat = h.stats().score(total_piece_requested);
                (h, stat)
            })
            .collect();

        // Select best peer by UCB
        candidates
            .into_iter()
            .max_by(|(_, lscore), (_, rscore)| lscore.total_cmp(rscore))
            .map(|(handle, _)| handle.clone())
    }

    /// Rarest-first piece selection: among `candidates`, pick the one held by the fewest
    /// active peers (ties broken randomly, so many peers starting at once don't all pile
    /// onto the same single rarest piece). Returns `None` if none of the candidates are
    /// available from any connected peer.
    fn rarest_piece(&self, candidates: &[u32]) -> Option<u32> {
        let availability = |piece: u32| self.active_peers.iter().filter(|p| p.state().they_have(piece)).count();

        let mut by_availability: Vec<(u32, usize)> =
            candidates.iter().map(|&p| (p, availability(p))).filter(|&(_, count)| count > 0).collect();
        let rarest_count = by_availability.iter().map(|&(_, count)| count).min()?;
        by_availability.retain(|&(_, count)| count == rarest_count);

        by_availability.choose(&mut rand::rng()).map(|&(piece, _)| piece)
    }

    async fn process_command(&mut self, command: TorrentSwarmCommand) {
        match command {
            TorrentSwarmCommand::ProcessPeerEvent { from, event } => self.process_peer_event(from, event).await,
            TorrentSwarmCommand::ProcessDownloadEvent(e) => self.process_download_event(e).await,
            TorrentSwarmCommand::ProcessAnnounceEvent(e) => self.process_announce_event(e).await,
            TorrentSwarmCommand::SelfCommand(command) => {
                let _ = self.process_self_commands(command).await;
            }
        };
    }

    async fn process_announce_event(&mut self, event: AnnouncerEvent) {
        match event {
            AnnouncerEvent::DiscoveredPeers(mut socket_addr_v4s) => {
                // only keep the peers that we're not already connected
                socket_addr_v4s.retain(|s| {
                    self.active_peers
                        .binary_search_by_key(s, |handle| handle.remote_addr)
                        .is_err()
                });

                let mut pending_peers: Vec<_> = socket_addr_v4s
                    .into_iter()
                    .map(|p| PendingPeer {
                        socket_addr: p,
                        pending_messages: vec![],
                    })
                    .collect();

                for peer in &pending_peers {
                    let moi = self.make_handle();

                    let connect = self.connect_peer(peer.socket_addr.clone());
                    let peer_addr = peer.socket_addr;
                    tokio::spawn(async move {
                        let connection = connect
                            .await
                            .inspect_err(|e| info!("Failed to connect to peer on {}, error {:?}", peer_addr, e))?;
                        moi.add_initialized_peer(connection).await;

                        anyhow::Ok(())
                    });
                }

                self.pending_peers.append(&mut pending_peers);
                self.pending_peers.sort_unstable();
            }
        }
    }

    async fn process_peer_event(&mut self, from: SocketAddr, event: PeerEvent) {
        match event {
            PeerEvent::Requested(request) => {
                // the peer may have been dropped between emitting this event and it being
                // processed here; that's a normal race, not an invariant violation. Looked up
                // up front (rather than only on the success path) since BEP 6 (Fast Extension)
                // requires telling the peer about every decline, not only serving successes.
                let Ok(peer_idx) = self.active_peers.binary_search_by(|h| h.remote_addr.cmp(&from)) else {
                    return;
                };

                // never serve a piece we haven't hash-verified, and never serve more than
                // the peer actually asked for
                let verified = self.stat.verified.get(request.index as usize).is_some_and(|b| *b);
                let data = if verified { self.storage.read_piece(request.index).ok() } else { None };
                let begin = request.begin as usize;
                let end = data.as_ref().map(|d| begin.saturating_add(request.length as usize).min(d.len()));

                let Some((data, end)) = data.zip(end).filter(|&(_, end)| begin < end) else {
                    let _ = self.active_peers[peer_idx].send_reject(request).await;
                    return;
                };

                let resp = Piece {
                    index: request.index,
                    begin: request.begin,
                    length: (end - begin) as u32,
                    data: Box::from(&data[begin..end]),
                };
                let _ = self.active_peers[peer_idx].send_data(resp).await;
            }
            PeerEvent::Disconnected => {
                if let Ok(idx) = self.active_peers.binary_search_by(|h| h.remote_addr.cmp(&from)) {
                    self.active_peers.remove(idx);
                    info!("{} disconnected, removed from active peers", from);
                }
            }
            PeerEvent::MetadataRequested { piece } => {
                // BEP 9: we always have the full metadata (started from a .torrent file, not
                // a magnet link), so we can serve any in-range piece unconditionally
                let start = piece as usize * METADATA_PIECE_SIZE;
                if start >= self.torrent.raw_info.len() {
                    return;
                }
                let end = (start + METADATA_PIECE_SIZE).min(self.torrent.raw_info.len());
                let data: Box<[u8]> = Box::from(&self.torrent.raw_info[start..end]);
                let total_size = self.torrent.metadata_size();

                let Ok(peer_idx) = self.active_peers.binary_search_by(|h| h.remote_addr.cmp(&from)) else {
                    return;
                };
                let _ = self.active_peers[peer_idx].send_metadata_piece(piece, total_size, data).await;
            }
            PeerEvent::PexReceived(peers) => {
                // BEP 27: don't act on PEX for a private torrent even if some peer sends it
                // anyway (we don't advertise ut_pex when private, so a compliant peer won't).
                if self.torrent.private {
                    return;
                }
                self.connect_to_discovered_peers(peers).await;
            }
        }
    }

    async fn process_download_event(&mut self, event: DownloadEvent) {
        match event {
            DownloadEvent::PieceCompleted { piece, resp } => {
                let valid = self.verify_hash(piece);
                let _ = resp.send(valid);
                if !valid {
                    return;
                }

                self.stat.written += self
                    .torrent
                    .nth_piece_size(piece)
                    .expect("we control download task, it's not malicious");
                self.stat.left = self.torrent.total_size as usize - self.stat.written;
                self.stat.completed = self.stat.all_verified();
                // announcers watch this to send a prompt event=completed rather than
                // waiting for their next periodic announce, which could be minutes away
                let _ = self.stat_snapshot_tx.send(self.stat.clone());

                let mut work = vec![];
                for p in self.active_peers.iter() {
                    work.push(async move {
                        let _ = p.send_we_have(piece).await;
                    })
                }

                for p in &mut self.pending_peers {
                    p.pending_messages.push(piece);
                }

                // TODO: maybe we should just spawn each tasks?
                join_all(work).await;
            }
        }
    }

    async fn process_self_commands(&mut self, command: TorrentSwarmSelfCommand) -> anyhow::Result<()> {
        match command {
            TorrentSwarmSelfCommand::HandleNewPeerConnection(peer) => {
                // NOTE:Most of below is beyond stupid, if we got here, it means the tcp stream has
                // already been established, so we can queue up the bitfields with no problems.
                // Then we can immediately put it to the active peers list
                info!("New peer established");

                // TODO: they shouldnt need to be dedup twice since a well formed peer connection only comes back
                //       when we dont have it
                self.active_peers.sort_unstable();
                let insertion_idx = self.active_peers.partition_point(|p| p < &peer);
                if insertion_idx == self.active_peers.len() || self.active_peers[insertion_idx] != peer {
                    // very important, not async, this ensures once
                    self.initialize_peer(&peer)?;
                    let remote_addr = peer.remote_addr;
                    // a connection we dialed out ourselves has a pending-peer entry with any
                    // Have messages queued up while it was connecting; an inbound connection
                    // (accepted by the listener) has none -- it gets caught up by the
                    // initial bitfield `initialize_peer` just sent instead
                    let pending_peer = self
                        .pending_peers
                        .binary_search_by_key(&remote_addr, |p| p.socket_addr)
                        .ok()
                        .map(|idx| self.pending_peers.remove(idx));
                    let readied_peer = peer.clone();

                    tokio::spawn(async move {
                        let Some(pending_peer) = pending_peer else {
                            return;
                        };

                        // From tokio's doc on sync::mpsc::Sender::send:
                        // ---
                        // This channel uses a queue to ensure that calls to send and reserve
                        // complete in the order they were requested. Cancelling a call to send
                        // makes you lose your place in the queue.
                        // ---
                        //
                        // We need this so even if the peer is placed onto the active list before
                        // all the messages have been drained, calls for any send_we_have will
                        // complete *after* we've sent all the messages below.
                        let permits = readied_peer
                            .peer_tx
                            .reserve_many(pending_peer.pending_messages.len())
                            .await
                            .expect("Read the comments above, solving this case is too complicated, if this encourtered in the real world, then we should just refactor peer connection instead");

                        debug_assert!(permits.len() == pending_peer.pending_messages.len());

                        for (piece, permit) in pending_peer.pending_messages.into_iter().zip(permits) {
                            // it's very important that this is *not* async, as the TorrentSwarm
                            // processes events one by one, all the messages will have been sent
                            // before any other messages (FUCK is this actually
                            // true???????????????????)
                            permit.send(PeerCommands::SendWeHave(piece));
                        }
                    });

                    self.active_peers.insert(insertion_idx, peer);
                }
            }
            TorrentSwarmSelfCommand::HandleNewDiscoveredPeers(peers) => {
                self.connect_to_discovered_peers(peers).await;
            }
            TorrentSwarmSelfCommand::ChooseBestPeer {
                piece,
                total_piece_requested,
                resp,
            } => {
                let _ = resp.send(self.best_peer(piece, total_piece_requested));
            }
            TorrentSwarmSelfCommand::QueryAllVerified { resp } => {
                let _ = resp.send(self.all_verified());
            }
            TorrentSwarmSelfCommand::PickRarestPiece { candidates, resp } => {
                let _ = resp.send(self.rarest_piece(&candidates));
            }
            TorrentSwarmSelfCommand::DialFailed(addr) => {
                if let Ok(idx) = self.pending_peers.binary_search_by_key(&addr, |p| p.socket_addr) {
                    self.pending_peers.remove(idx);
                }
            }
        }

        Ok(())
    }

    /// A live view of this torrent's aggregate progress (uploaded/downloaded/left/written/
    /// verified/completed). Must be called before `work_loop(self)` takes ownership of the
    /// swarm (e.g. right after `TorrentSwarm::new`, which is what `BtClient::add_torrent` does)
    /// -- the returned receiver keeps working after that, since it only depends on the
    /// underlying channel, not on `TorrentSwarm` itself still being reachable.
    pub(crate) fn subscribe_stats(&self) -> watch::Receiver<TorrentSwarmStats> {
        self.stat_snapshot_rx.clone()
    }

    pub fn verified_cnt(&self) -> usize {
        self.stat.verified_cnt()
    }

    pub fn all_verified(&self) -> bool {
        self.stat.all_verified()
    }

    fn verify_hash(&mut self, piece: u32) -> bool {
        let written_data = self.storage.read_piece(piece).unwrap();

        let valid_piece = self.torrent.valid_piece(piece, &written_data);
        if valid_piece {
            self.stat.verified.set(piece as usize, true);
        }
        valid_piece
    }

    pub fn torrent(&self) -> &Arc<Torrent> {
        &self.torrent
    }

    pub fn storage(&self) -> &Arc<TorrentStorage> {
        &self.storage
    }

    pub fn connect_peer(&self, remote_addr: SocketAddr) -> impl Future<Output = anyhow::Result<PeerHandle>> + use<> {
        let torrent = self.torrent.clone();
        let our_id = self.id.clone();
        let event_tx = self.outbound_msgs.clone();

        async move {
            let mut tcp = TcpStream::connect(remote_addr)
                .await
                .with_context(|| format!("Failed to established tcp stream with {}", remote_addr))?;
            let handshake = shake_hands(&mut tcp, &torrent.info_hash, &our_id.peer_id)
                .await
                .with_context(|| format!("Failed to complete handshake with {}", remote_addr))?;
            info!("Peer connection to {} established", remote_addr);

            let handle = PeerHandle::new(
                tcp,
                handshake.peer_id,
                event_tx,
                &torrent,
                crate::wire::supports_extensions(&handshake.extensions),
                crate::wire::supports_fast_extension(&handshake.extensions),
            );
            Ok(handle)
        }
    }

    /// Dials every not-already-connected address in `peers` and queues each as a `PendingPeer`
    /// (so any `Have`s completed while the dial is in flight get delivered once it lands).
    /// Shared by tracker-discovered peers and BEP 11 (PEX) peers -- both are just addresses.
    async fn connect_to_discovered_peers(&mut self, mut peers: Vec<SocketAddr>) {
        peers.retain(|s| {
            self.active_peers.binary_search_by_key(s, |handle| handle.remote_addr).is_err()
                && self.pending_peers.binary_search_by_key(s, |p| p.socket_addr).is_err()
        });

        let mut pending_peers: Vec<_> = peers
            .into_iter()
            .map(|p| PendingPeer {
                socket_addr: p,
                pending_messages: vec![],
            })
            .collect();

        for peer in &pending_peers {
            let moi = self.make_handle();
            let peer_addr = peer.socket_addr;

            let connect = self.connect_peer(peer_addr);
            tokio::spawn(async move {
                let Ok(connection) = connect.await.inspect_err(|e| info!("Failed to connect to {peer_addr}: {e:?}"))
                else {
                    // without this, an address that never connects sits in `pending_peers`
                    // forever -- it's only ever removed on success -- and the dedup above
                    // would then refuse to ever retry it
                    moi.dial_failed(peer_addr).await;
                    return;
                };
                info!("Connection to peer success");
                moi.add_initialized_peer(connection).await;
            });
        }

        self.pending_peers.append(&mut pending_peers);
        self.pending_peers.sort_unstable();
    }

    fn initialize_peer(&self, peer: &PeerHandle) -> anyhow::Result<()> {
        // BEP 6: a peer that advertised Fast Extension support accepts HaveAll/HaveNone in
        // place of a BitField for the "I have everything"/"I have nothing" cases -- smaller
        // than sending a full bitfield, and the actual reason those two messages exist. A
        // partial bitfield still just goes out as BitField either way.
        if peer.remote_supports_fast && self.stat.all_verified() {
            peer.try_have_all()?;
        } else if peer.remote_supports_fast && self.stat.verified_cnt() == 0 {
            peer.try_have_none()?;
        } else {
            let has = Box::from(self.stat.verified.clone().as_raw_slice());
            peer.try_bitfield(BitField { has })?;
        }
        peer.try_fancy_peer()?;
        // BEP 3: connections start choked; whether to unchoke is the choking algorithm's
        // call (run periodically in work_loop), not an automatic grant on connect

        Ok(())
    }

    /// Tit-for-tat unchoking, run periodically. Ranks interested peers (those who want to
    /// download from us) by the download rate they've been giving us -- reciprocation is the
    /// point -- and unchokes the top MAX_UNCHOKED_PEERS. Every OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS
    /// rounds, one additional peer is unchoked at random so a new or under-rated peer gets a
    /// chance to prove itself instead of the same top N being unchoked forever.
    async fn run_choking_algorithm(&mut self, round: u64) {
        let mut interested: Vec<PeerHandle> = self
            .active_peers
            .iter()
            .filter(|p| p.state().interested_us)
            .cloned()
            .collect();
        interested.sort_by(|a, b| b.stats().mean_rx.total_cmp(&a.stats().mean_rx));

        let mut to_unchoke: Vec<SocketAddr> =
            interested.iter().take(MAX_UNCHOKED_PEERS).map(|p| p.remote_addr).collect();

        if round % OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS == 0 {
            let candidates: Vec<_> = interested
                .iter()
                .filter(|p| !to_unchoke.contains(&p.remote_addr))
                .collect();
            if let Some(pick) = candidates.choose(&mut rand::rng()) {
                to_unchoke.push(pick.remote_addr);
            }
        }

        for peer in &self.active_peers {
            let should_unchoke = to_unchoke.contains(&peer.remote_addr);
            let currently_choked = peer.state().choked_them;
            if should_unchoke && currently_choked {
                let _ = peer.unchoke_peer().await;
            } else if !should_unchoke && !currently_choked {
                let _ = peer.choke_peer().await;
            }
        }
    }

    /// BEP 11 (PEX): tell each active peer about every *other* active peer we know of. No
    /// per-peer diffing against what we've told them before ("added"/"dropped" bookkeeping) --
    /// we just resend the current full membership every round, which is redundant but simple
    /// and spec-legal (PEX is a discovery hint, not an authoritative membership feed).
    async fn run_pex_round(&mut self) {
        // BEP 27: a private torrent's peers must come only from its trackers.
        if self.torrent.private {
            return;
        }

        let all_addrs: Vec<SocketAddr> = self.active_peers.iter().map(|p| p.remote_addr).collect();

        for peer in &self.active_peers {
            // BEP 11 recommends capping a single PEX message at roughly 50 added peers
            let added: Vec<SocketAddr> = all_addrs
                .iter()
                .copied()
                .filter(|a| *a != peer.remote_addr)
                .take(PEX_MAX_ADDED_PEERS)
                .collect();
            if added.is_empty() {
                continue;
            }
            let _ = peer.send_pex(added).await;
        }
    }
}
