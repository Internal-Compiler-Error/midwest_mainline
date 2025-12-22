use crate::defs::Identity;
use crate::download::{Download, DownloadEvent};
use crate::peer::{PeerCommands, PeerEvent, PeerHandle, PeerStatistics};
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use crate::wire::{BitField, BtMessage, Piece, shake_hands};
use anyhow::{self, Context, bail};
use bitvec::boxed::BitBox;
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use juicy_bencode::{BencodeDictDisplay, BencodeItemView};
use rand::Rng;
use reqwest::Client;
use std::any::Any;
use std::mem;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::pin;
use std::slice::from_raw_parts;
use std::sync::Arc;
use std::time::Duration;
use tokio::io;
use tokio::net::{TcpStream, UdpSocket, lookup_host};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, Sleep, interval, sleep, sleep_until};
use tracing::{info, warn};
use url::{Url, form_urlencoded};
use zerocopy::network_endian::{I32, I64, U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use futures::future::join_all;

#[allow(unused_imports)]
use derive_more::{Eq, PartialEq};

/// Handles announcements to a single tracker server
#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpAnnouncer {
    tracker: Url,
    torrent: Arc<Torrent>,

    #[eq(skip)]
    identity: Arc<Identity>,
    #[eq(skip)]
    next_ready: Instant,
    #[eq(skip)]
    swarm_stat: watch::Receiver<TorrentSwarmStats>,
    #[eq(skip)]
    events: mpsc::Sender<TorrentSwarmCommand>,
}

pub enum AnnouncerEvent {
    DiscoveredPeers(Vec<SocketAddrV4>),
}

impl HttpAnnouncer {
    fn new(
        tracker: Url,
        torrent: Arc<Torrent>,
        identity: Arc<Identity>,
        swarm_stat: watch::Receiver<TorrentSwarmStats>,
        events: mpsc::Sender<TorrentSwarmCommand>,
    ) -> Self {
        debug_assert!({ tracker.scheme() == "http" || tracker.scheme() == "https" });

        HttpAnnouncer {
            tracker,
            torrent,
            identity,
            next_ready: Instant::now() + Duration::from_millis(10),
            swarm_stat,
            events,
        }
    }

    /// The future resolves whenever the next round of announce is ready to be performed
    fn ready(&self) -> Sleep {
        sleep_until(self.next_ready)
    }

    /// Perform a single announce and return the interval and discovered peers
    #[tracing::instrument(skip(self))]
    async fn announce(&mut self) -> anyhow::Result<Vec<SocketAddrV4>> {
        // percent encode info_hash and peer_id
        let info_hash_encoded: String = form_urlencoded::byte_serialize(&self.torrent.info_hash.0).collect();
        let peer_id_encoded: String = form_urlencoded::byte_serialize(&self.identity.peer_id).collect();

        let swarm_stat = self.swarm_stat.borrow().clone();

        // Build the announce URL
        let url = format!(
            "{tracker_url}?info_hash={info_hash}&peer_id={peer_id}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&compact=1",
            tracker_url = self.tracker,
            info_hash = info_hash_encoded,
            peer_id = peer_id_encoded,
            port = self.identity.serving.port(),
            uploaded = swarm_stat.uploaded,
            downloaded = swarm_stat.downloaded,
            left = swarm_stat.left,
        );

        // fn percent_encode(bytes: &[u8]) -> String {
        //     bytes.iter().map(|b| format!("%{:02X}", b)).collect()
        // }

        // debug_assert!(percent_encode(&self.torrent.info_hash.0).len() == 60);

        // let mut url = self.tracker.clone();
        // url.query_pairs_mut()
        //     .encoding_override(None)
        //     .append_pair("info_hash", &percent_encode(&self.torrent.info_hash.0))
        //     // .append_pair("info_hash", &info_hash_encoded)
        //     // .append_pair("peer_id", &peer_id_encoded)
        //     .append_pair("peer_id", &percent_encode(&self.identity.peer_id))
        //     .append_pair("port", &self.identity.serving.port().to_string())
        //     .append_pair("uploaded", &swarm_stat.uploaded.to_string())
        //     .append_pair("downloaded", &swarm_stat.downloaded.to_string())
        //     .append_pair("left", &swarm_stat.left.to_string())
        //     .append_pair("compact", &1.to_string());
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
            "Parsed bencode from tracker [{}] as {}",
            self.tracker,
            BencodeDictDisplay(&dict)
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
                peers.push(SocketAddrV4::new(ip, port));
            }
        }

        self.next_ready = Instant::now() + Duration::from_secs(interval as u64);
        Ok(peers)
    }

    async fn ev_loop(mut self) {
        // TODO: make a way for it to die gracefully
        loop {
            self.ready().await;
            match self.announce().await {
                Ok(result) => {
                    let _ = self
                        .events
                        .send(TorrentSwarmCommand::ProcessAnnounceEvent(
                            AnnouncerEvent::DiscoveredPeers(result),
                        ))
                        .await;
                }
                Err(e) => {
                    tracing::error!("{e}");
                    // TODO: use exponential backoff
                    self.next_ready = Instant::now() + Duration::from_mins(1);
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
struct UdpAnnouncer {
    tracker: Url,
    torrent: Arc<Torrent>,

    #[eq(skip)]
    identity: Arc<Identity>,
    #[eq(skip)]
    next_ready: Instant,
    #[eq(skip)]
    swarm_stat: watch::Receiver<TorrentSwarmStats>,

    connection_id: i64,

    // TODO: Maybe this should be a weak sender?
    #[eq(skip)]
    swarm: TorrentSwarmHandle,
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
    #[allow(dead_code)]
    Completed = 1,
    #[allow(dead_code)]
    Started = 2,
    #[allow(dead_code)]
    Stopped = 3,
}

#[allow(dead_code)]
impl UdpAnnouncer {
    fn new(
        tracker_url: Url,
        torrent: Arc<Torrent>,
        identity: Arc<Identity>,
        swarm_stat: watch::Receiver<TorrentSwarmStats>,
        swarm: TorrentSwarmHandle,
    ) -> Self {
        debug_assert!(tracker_url.scheme() == "udp");
        UdpAnnouncer {
            tracker: tracker_url,
            torrent,
            identity,
            next_ready: Instant::now() + Duration::from_millis(10),
            swarm_stat,
            connection_id: 0, // sentinel, meaning we haven't got an id connetion yet because we
            // haven't done anything
            swarm,
        }
    }

    /// The future resolves whenever the next round of announce is ready to be performed
    fn ready(&self) -> Sleep {
        sleep_until(self.next_ready)
    }

    #[tracing::instrument(skip(self))]
    async fn resolve_v4(&self) -> anyhow::Result<Option<SocketAddr>> {
        let host_name = self.tracker.host_str().unwrap();
        let host_port = self.tracker.port().unwrap();
        let query = format!("{}:{}", host_name, host_port);
        info!("Resolving {}", query);
        let addresses: Vec<_> = lookup_host(&query)
            .await
            .inspect_err(|e| info!("{e}"))
            .with_context(|| format!("Failed to resolve {}", &query))?
            .collect();

        info!("Looking up {} came back with {:?}", query, &addresses);

        let mut addresses: Vec<_> = addresses
            .into_iter()
            .filter_map(|a| match a {
                SocketAddr::V4(_) => Some(a),
                SocketAddr::V6(_) => None,
            })
            .collect();

        Ok(addresses.pop())
    }

    #[tracing::instrument(skip(self))]
    async fn connect(&mut self, socket: &mut UdpSocket) -> anyhow::Result<()> {
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

        info!("Sending tracker connect to tracker [{}]", self.tracker);
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
            info!("tracker transaction id didn't match our transaction_id");
            bail!("tracker transaction id didn't match our transaction_id");
        }
        if action == Action::Error as i32 {
            info!("server errored on connect");
            bail!("server errored on connect");
        }
        if action != 0 {
            info!("server responsed with an action different than connection whilst we attempted to connect");
            bail!("server responsed with an action different than connection whilst we attempted to connect");
        }

        info!("Connect success!");

        Ok(())
    }

    /// Perform a single announce and return the interval and discovered peers
    #[tracing::instrument]
    async fn announce(&mut self, socket: &mut UdpSocket) -> anyhow::Result<Vec<SocketAddrV4>> {
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
            info_hash: self.torrent.info_hash.0,
            peer_id: self.identity.peer_id,
            downloaded: downloaded.into(),
            left: left.into(),
            uploaded: uploaded.into(),
            event: (Event::None as i32).into(),
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

        #[derive(PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
        #[repr(C)]
        struct AnnounceResponse {
            header: AnnounceResponseHeader,
            peers: [Peer],
        }

        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable, KnownLayout, Unaligned,
        )]
        #[repr(C)]
        struct Peer {
            ip: I32,
            port: U16,
        }

        impl From<Peer> for SocketAddrV4 {
            fn from(value: Peer) -> SocketAddrV4 {
                let ip = Ipv4Addr::from_octets(value.ip.as_bytes().try_into().unwrap());
                let port = u16::from_be_bytes(value.port.as_bytes().try_into().unwrap());
                SocketAddrV4::new(ip, port)
            }
        }

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
        let header =
            AnnounceResponseHeader::ref_from_bytes(&buf[..header_size]).expect("header alignment should be good");

        let peer_size = size_of::<Peer>();
        let peer_bytes = buf.len() - header_size;
        if peer_bytes % peer_size != 0 {
            bail!("trailing content is a multiple of peer size");
        }
        let peers = <[Peer]>::ref_from_bytes(&buf[header_size..]).expect("shit should work");

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
        let peers: Vec<SocketAddrV4> = peers.iter().copied().map(SocketAddrV4::from).collect();
        info!("Announce success! Got: {:?}", &peers);
        Ok(peers)
    }

    #[tracing::instrument(skip(self))]
    async fn ev_loop(mut self) -> anyhow::Result<()> {
        let tracker_addr = self
            .resolve_v4()
            .await
            .with_context(|| format!("Failed to resolve {}", self.tracker))
            .inspect_err(|e| warn!("{e}"))?;

        let Some(tracker_addr) = tracker_addr else {
            bail!("Tracker [{}] has no ipv4", self.tracker);
        };

        info!("Tracker [{}] resolved as {}", self.tracker, tracker_addr);

        let our_socket = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0);
        info!("Binding to socket");
        let mut socket = UdpSocket::bind(our_socket)
            .await
            .with_context(|| format!("Failed bind to 0.0.0.0 as an udp socket"))
            .inspect_err(|e| warn!("{e}"))?;

        info!("\"Connecting\" to {}", tracker_addr);
        socket
            .connect(tracker_addr)
            .await
            .inspect_err(|e| warn!("{e}"))
            .with_context(|| format!("Failed to connect to addr: {}", tracker_addr))
            .inspect_err(|e| warn!("{e}"))?;

        self.connect(&mut socket).await.inspect_err(|e| warn!("{e}"))?;

        loop {
            self.ready().await;
            // TODO: should retry instead of stopping at first failure
            let peers = self
                .announce(&mut socket)
                .await
                .with_context(|| format!("Tracker [{}] announce failed", self.tracker))?;

            self.swarm.handle_discovered_peers(peers).await;
        }
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
    pub verified: BitBox<u8>,

    pub completed: bool,
}

impl TorrentSwarmStats {
    fn verified_cnt(&self) -> usize {
        self.verified.count_ones()
    }

    fn all_verified(&self) -> bool {
        self.verified.iter().all(|v| *v)
    }
}

#[derive(Debug, Clone)]
pub struct TorrentSwarmHandle {
    tx: mpsc::Sender<TorrentSwarmCommand>,
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

    pub async fn handle_discovered_peers(&self, peers: Vec<SocketAddrV4>) {
        let _ = self
            .tx
            .send(TorrentSwarmCommand::SelfCommand(
                TorrentSwarmSelfCommand::HandleNewDiscoveredPeers(peers),
            ))
            .await;
    }
}

pub enum TorrentSwarmCommand {
    ProcessPeerEvent { from: SocketAddrV4, event: PeerEvent },
    ProcessDownloadEvent(DownloadEvent),
    ProcessAnnounceEvent(AnnouncerEvent),
    SelfCommand(TorrentSwarmSelfCommand),
}

enum TorrentSwarmSelfCommand {
    HandleNewPeerConnection(PeerHandle),
    HandleNewDiscoveredPeers(Vec<SocketAddrV4>),
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
    stat_snapshot_rx: watch::Receiver<TorrentSwarmStats>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingPeer {
    pub socket_addr: SocketAddrV4,
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
    pub fn new(torrent: Arc<Torrent>, storage: Arc<TorrentStorage>, id: Arc<Identity>) -> TorrentSwarm {
        let verified = vec![false; torrent.pieces.len()];
        let verified = BitBox::from_iter(verified.iter());
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
            .map(|t| HttpAnnouncer::new(t, torrent.clone(), id.clone(), stat_rx.clone(), command_tx.clone()))
            .collect();
        let udp_announcers: Vec<_> = trackers
            .filter(|t| t.scheme() == "udp")
            .map(|t| UdpAnnouncer::new(t, torrent.clone(), id.clone(), stat_rx.clone()))
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

    pub(crate) async fn work_loop(mut self) {
        let mut aggregate_ticker = interval(Duration::from_mins(1));

        // Get a raw pointer to self to bypass borrow checker
        let self_ptr: *const TorrentSwarm = &self;

        // SAFETY: This is safe because:
        // 1. Download::choose() only reads peer_handles and doesn't hold references across await points
        // 2. tokio::select! branches are mutually exclusive - only one executes at a time
        // 3. When download is polled (branch 3), it doesn't hold any references while other branches execute
        // 4. Mutable accesses to announcers and peer_handles in branches 1 and 2 don't overlap with
        //    download's immutable access to peer_handles in branch 3
        let download = unsafe { Download::new(&*self_ptr) };
        let mut download = pin!(download.download_loop());
        let mut download_done = false;

        // TODO: probably store the join handles so they can be aborted when necessary
        // let http_announcers = mem::take(&mut self.http_announcers);
        // for http_announcer in http_announcers {
        //     tokio::spawn(http_announcer.ev_loop());
        // }

        let udp_announcers = mem::take(&mut self.udp_announcers);
        for udp_announcer in udp_announcers {
            tokio::spawn(udp_announcer.ev_loop());
        }

        loop {
            tokio::select! {
                _ = aggregate_ticker.tick() => {
                    self.aggregate_peer_stats();
                }
                _ = &mut download, if !download_done => {
                    download_done = true;
                },
                Some(command) = self.inbound_msgs.recv() => self.process_command(command).await,
            }
        }
    }

    pub fn best_peer(&self, piece: u32, total_piece_requested: usize) -> Option<PeerHandle> {
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
                // remove all the peers we already have
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
                    tokio::spawn(async move {
                        let connection = connect.await?;
                        moi.add_initialized_peer(connection).await;

                        anyhow::Ok(())
                    });
                }

                self.pending_peers.append(&mut pending_peers);
                self.pending_peers.sort_unstable();
            }
        }
    }

    async fn process_peer_event(&mut self, from: SocketAddrV4, event: PeerEvent) {
        match event {
            PeerEvent::Requested(request) => {
                let data = self.storage.read_piece(request.index);
                if data.is_err() {
                    return;
                }

                let data = data.unwrap();
                let (_head, tail) = data.split_at(request.begin as usize);
                let tail: Box<[u8]> = tail.into();

                let resp = Piece {
                    index: request.index,
                    begin: request.begin,
                    length: (data.len() - request.begin as usize) as u32,
                    data: tail,
                };
                let peer_idx = self
                    .active_peers
                    .binary_search_by(|h| h.remote_addr.cmp(&from))
                    .expect("Only us remove peers, how could it be gone");
                let _ = self.active_peers[peer_idx].send_data(resp).await;
            }
        }
    }

    async fn process_download_event(&mut self, event: DownloadEvent) {
        match event {
            DownloadEvent::PieceCompleted(piece) => {
                let valid = self.verify_hash(piece);
                if !valid {
                    return;
                }

                self.stat.written += self
                    .torrent
                    .nth_piece_size(piece)
                    .expect("we control download task, it's not malicious");
                self.stat.left = self.torrent.total_size as usize - self.stat.written;

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
                // TODO: they shouldnt need to be dedup twice since a well formed peer connection only comes back
                //       when we dont have it
                self.active_peers.sort_unstable();
                let insertion_idx = self.active_peers.partition_point(|p| p < &peer);
                if insertion_idx == self.active_peers.len() || self.active_peers[insertion_idx] != peer {
                    // very important, not async, this ensures once
                    self.initialize_peer(&peer)?;
                    let remote_addr = peer.remote_addr;
                    let peer_idx = self
                        .pending_peers
                        .binary_search_by_key(&remote_addr, |p| p.socket_addr)
                        .expect("a connection can only be made when it has been placed onto the pending list");
                    let pending_peer = self.pending_peers.remove(peer_idx);
                    let readied_peer = peer.clone();

                    tokio::spawn(async move {
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
            TorrentSwarmSelfCommand::HandleNewDiscoveredPeers(mut socket_addr_v4s) => {
                // remove all the peers we already have
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
                    tokio::spawn(async move {
                        let connection = connect.await?;
                        moi.add_initialized_peer(connection).await;

                        anyhow::Ok(())
                    });
                }

                self.pending_peers.append(&mut pending_peers);
                self.pending_peers.sort_unstable();
            }
        }

        Ok(())
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

    pub fn connect_peer(&self, remote_addr: SocketAddrV4) -> impl Future<Output = anyhow::Result<PeerHandle>> + use<> {
        let torrent = self.torrent.clone();
        let our_id = self.id.clone();
        let event_tx = self.outbound_msgs.clone();
        let stat_snapshot_rx = self.stat_snapshot_rx.clone();

        async move {
            let mut tcp = TcpStream::connect(remote_addr).await?;
            let handshake = shake_hands(&mut tcp, &torrent.info_hash, &our_id.peer_id).await?;
            let handle = PeerHandle::new(tcp, handshake.peer_id, event_tx, stat_snapshot_rx, &torrent);
            Ok(handle)
        }
    }

    fn initialize_peer(&self, peer: &PeerHandle) -> anyhow::Result<()> {
        let has = Box::from(self.stat.verified.clone().as_raw_slice());

        peer.try_bitfield(BitField { has })?;
        peer.try_unchoke_peer()?;
        peer.try_fancy_peer()?;

        Ok(())
    }
}
