//! Tracker announcers (BEP 3 over HTTP, BEP 15 over UDP). One task per tracker URL, reporting
//! the peers it learns about as `SwarmEvent::PeersDiscovered` and reading the swarm's progress
//! (uploaded/downloaded/left/completed) off its stats watch. Keyed on a bare `InfoHash` rather
//! than a `Torrent`: a magnet link's pre-metadata phase (see `metadata::fetch`) announces with
//! nothing else to give, which is why these don't live inside `TorrentSwarm`.

use crate::defs::Identity;
use crate::dht::DhtWatch;
use crate::settings::DHT_ANNOUNCE_INTERVAL;
use crate::torrent_swarm::{SwarmEvent, TorrentSwarmStats};
use anyhow::{self, Context, bail};
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use rand::Rng;
use reqwest::Client;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{UdpSocket, lookup_host};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, Sleep, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use url::{Url, form_urlencoded};
use zerocopy::network_endian::{I32, I64, U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[allow(unused_imports)]
use derive_more::{Eq, PartialEq};

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
    events: mpsc::WeakSender<SwarmEvent>,
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
    events: mpsc::WeakSender<SwarmEvent>,
    shutdown: CancellationToken,
    dht: DhtWatch,
) {
    tokio::spawn(dht_announcer(
        info_hash,
        identity.serving.port(),
        dht,
        events.clone(),
        shutdown.clone(),
    ));
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

/// BEP 5 as a peer source: every DHT_ANNOUNCE_INTERVAL, look the info hash up, hand whatever
/// peers come back to the swarm, and announce our port to the nodes that issued tokens.
/// Waits for the node to come up first, and does nothing at all if it never does.
async fn dht_announcer(
    info_hash: InfoHash,
    tcp_port: u16,
    mut dht: DhtWatch,
    events: mpsc::WeakSender<SwarmEvent>,
    shutdown: CancellationToken,
) {
    let handle = loop {
        if let Some(handle) = dht.borrow().clone() {
            break handle;
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            changed = dht.changed() => if changed.is_err() { return },
        }
    };
    let port = Some(tcp_port);

    loop {
        let lookup = tokio::select! {
            _ = shutdown.cancelled() => return,
            lookup = handle.client.get_peers(info_hash) => lookup,
        };
        match lookup {
            Ok(result) => {
                info!(
                    "DHT lookup found {} peers, {} nodes accept our announce",
                    result.peers.len(),
                    result.announce_candidates.len()
                );
                let Some(events) = events.upgrade() else { return };
                let peers = result.peers.into_iter().map(SocketAddr::V4).collect();
                if events.send(SwarmEvent::PeersDiscovered(peers)).await.is_err() {
                    return;
                }
                for (node, token) in result.announce_candidates {
                    if let Err(e) = handle
                        .client
                        .announce_peers(node.end_point(), info_hash, port, token)
                        .await
                    {
                        tracing::debug!("announce to DHT node {} failed: {e:#}", node.end_point());
                    }
                }
            }
            Err(e) => warn!("DHT lookup for {info_hash:?} failed: {e:#}"),
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(DHT_ANNOUNCE_INTERVAL) => {}
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
        events: mpsc::WeakSender<SwarmEvent>,
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
                            if let Some(events) = self.events.upgrade() {
                                let _ = events.send(SwarmEvent::PeersDiscovered(result)).await;
                            }
                        }
                        Err(e) => {
                            warn!("{e:#}");
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

    #[eq(skip)]
    event: mpsc::WeakSender<SwarmEvent>,

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
        event: mpsc::WeakSender<SwarmEvent>,
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
            .with_context(|| format!("Failed to resolve {}", query))?
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

        let txn_id: i32 = header.transaction_id.into();
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
        let tracker_addr = self.resolve().await.inspect_err(|e| warn!("{e:#}"))?;

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
            .inspect_err(|e| warn!("{e:#}"))?;

        info!("\"Connecting\" to {}", tracker_addr);
        socket
            .connect(tracker_addr)
            .await
            .with_context(|| format!("Failed to connect to addr: {}", tracker_addr))
            .inspect_err(|e| warn!("{e:#}"))?;

        self.connect(&mut socket).await.inspect_err(|e| warn!("{e:#}"))?;

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

                    if let Some(events) = self.event.upgrade() {
                        let _ = events.send(SwarmEvent::PeersDiscovered(peers)).await;
                    }
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
