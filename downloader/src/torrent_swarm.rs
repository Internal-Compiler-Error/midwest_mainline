use crate::defs::Identity;
use crate::download::{Download, DownloadEvent};
use crate::peer::{PeerEvent, PeerHandle, PeerStatistics};
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use crate::wire::{BitField, Piece, shake_hands};
use anyhow::{self, bail};
use bitvec::boxed::BitBox;
use futures::StreamExt;
use juicy_bencode::BencodeItemView;
use rand::Rng;
use reqwest::Client;
use std::any::Any;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::pin;
use std::slice::from_raw_parts;
use std::sync::Arc;
use std::time::Duration;
use tokio::io;
use tokio::net::{TcpStream, UdpSocket, lookup_host};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, Sleep, interval, sleep_until};
use url::{Url, form_urlencoded};
use zerocopy::network_endian::{I32, I64, U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use futures::future::join_all;

#[allow(unused_imports)]
use derive_more::{Eq, PartialEq};

/// Handles announcements to a single tracker server
#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpAnnouncer {
    tracker_url: String,
    torrent: Arc<Torrent>,

    #[eq(skip)]
    identity: Arc<Identity>,
    #[eq(skip)]
    next_ready: Instant,
    #[eq(skip)]
    swarm_stat: watch::Receiver<TorrentSwarmStats>,
}

// enum AnnouncerEvent {
//     DiscoveredPeers(Vec<SocketAddrV4>),
// }

impl HttpAnnouncer {
    fn new(
        tracker_url: String,
        torrent: Arc<Torrent>,
        identity: Arc<Identity>,
        swarm_stat: watch::Receiver<TorrentSwarmStats>,
    ) -> Self {
        HttpAnnouncer {
            tracker_url,
            torrent,
            identity,
            next_ready: Instant::now() + Duration::from_millis(10),
            swarm_stat,
        }
    }

    /// The future resolves whenever the next round of announce is ready to be performed
    fn ready(&self) -> Sleep {
        sleep_until(self.next_ready)
    }

    /// Perform a single announce and return the interval and discovered peers
    async fn announce(&mut self) -> anyhow::Result<Vec<SocketAddrV4>> {
        // URL-encode info_hash and peer_id
        let info_hash_encoded: String = form_urlencoded::byte_serialize(&self.torrent.info_hash.0).collect();
        let peer_id_encoded: String = form_urlencoded::byte_serialize(&self.identity.peer_id).collect();

        let swarm_stat = self.swarm_stat.borrow().clone();

        // Build the announce URL
        let url = format!(
            "{tracker_url}?info_hash={info_hash}&peer_id={peer_id}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&compact=1",
            tracker_url = self.tracker_url,
            info_hash = info_hash_encoded,
            peer_id = peer_id_encoded,
            port = self.identity.serving.port(),
            uploaded = swarm_stat.uploaded,
            downloaded = swarm_stat.downloaded,
            left = swarm_stat.left,
        );

        // Send the GET request
        let client = Client::new();
        let response = client.get(&url).send().await?;
        let bytes = response.bytes().await?;
        let bytes: &[u8] = &bytes;

        // Parse the bencoded response
        let parsed = juicy_bencode::parse_bencode_dict(bytes);
        let Ok((_remaining, mut dict)) = parsed else {
            bail!("invalid bencoded content returned");
        };

        let mut peers = vec![];
        let Some(BencodeItemView::Integer(interval)) = dict.remove(b"interval".as_slice()) else {
            bail!("response interval must be a number");
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
struct UdpAnnouncer {
    tracker_url: String,
    torrent: Arc<Torrent>,

    #[eq(skip)]
    identity: Arc<Identity>,
    #[eq(skip)]
    next_ready: Instant,
    #[eq(skip)]
    swarm_stat: watch::Receiver<TorrentSwarmStats>,

    connection_id: i64,
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
    // fn new(
    //     tracker_url: String,
    //     torrent: Arc<Torrent>,
    //     identity: Arc<Identity>,
    //     swarm_stat: watch::Receiver<TorrentSwarmStats>,
    // ) -> Self {
    //     UdpAnnouncer {
    //         tracker_url,
    //         torrent,
    //         identity,
    //         next_ready: Instant::now() + Duration::from_millis(10),
    //         swarm_stat,
    //     }
    // }

    /// The future resolves whenever the next round of announce is ready to be performed
    fn ready(&self) -> Sleep {
        sleep_until(self.next_ready)
    }

    async fn resolve(&self) -> io::Result<SocketAddr> {
        let url = Url::parse(&self.tracker_url).expect("validation should be done in new()");
        debug_assert!(url.scheme() == "udp");

        let mut addresses = lookup_host(url.host_str().expect("validation should be done in new()")).await?;

        Ok(addresses.next().unwrap())
    }

    async fn connect(&mut self, socket: UdpSocket) -> anyhow::Result<()> {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable)]
        #[repr(C)]
        struct Connect {
            connection_id: I64,
            action: I32,
            transaction_id: I32,
        }

        let mut rng = rand::rng();
        let transaction_id = rng.random::<i32>();

        let connect = Connect {
            connection_id: 0x41727101980.into(),
            action: (Action::Connect as i32).into(),
            transaction_id: transaction_id.into(),
        };

        socket.send(connect.as_bytes()).await?;

        #[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable)]
        #[repr(C)]
        struct Response {
            action: I32,
            transaction_id: I32,
            connection_id: I64,
        }
        let mut response = Response::default();

        socket.recv(response.as_mut_bytes()).await?;

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

        Ok(())
    }

    /// Perform a single announce and return the interval and discovered peers
    async fn announce(&mut self, socket: UdpSocket) -> anyhow::Result<Vec<SocketAddrV4>> {
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
        socket.send(announce.as_bytes()).await?;

        #[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Immutable, Default, KnownLayout)]
        #[repr(C)]
        struct AnnounceResponseHeader {
            action: I32,
            transaction_id: I32,
            interval: I32, // in seconds
            leechers: I32,
            seeders: I32,
        }

        #[derive(PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout)]
        #[repr(C)]
        struct AnnounceResponse {
            header: AnnounceResponseHeader,
            peers: [Peer],
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable, KnownLayout)]
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
        let read_size = socket.recv(&mut buf).await?;
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
        let peers = <[Peer]>::ref_from_bytes(&buf[..header_size]).expect("shit should work");

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
        Ok(peers)
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
}

pub enum TorrentSwarmCommand {
    ProcessPeerEvent { from: SocketAddrV4, event: PeerEvent },
    ProcessDownloadEvent(DownloadEvent),
    SelfCommand(TorrentSwarmSelfCommand),
}

enum TorrentSwarmSelfCommand {
    HandleNewPeerConnection(PeerHandle),
}

pub struct TorrentSwarm {
    peer_handles: Vec<PeerHandle>,
    torrent: Arc<Torrent>,
    storage: Arc<TorrentStorage>,

    id: Arc<Identity>,

    announcers: Vec<HttpAnnouncer>,

    inbound_msgs: mpsc::Receiver<TorrentSwarmCommand>,
    /// we keep a sender so we can clone it and give it to objects that generate on run time who need it
    outbound_msgs: mpsc::Sender<TorrentSwarmCommand>,

    stat: TorrentSwarmStats,
    stat_snapshot_tx: watch::Sender<TorrentSwarmStats>,
    stat_snapshot_rx: watch::Receiver<TorrentSwarmStats>,
}

impl TorrentSwarm {
    pub fn new(torrent: Arc<Torrent>, storage: Arc<TorrentStorage>, id: Arc<Identity>) -> TorrentSwarm {
        // Create shared peer handles
        let peer_handles = Vec::<PeerHandle>::new();

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

        // Create announcer with access to peer handles and storage stats
        // For now, use the primary tracker (first tracker in first tier)
        // TODO: Support multiple trackers from announce_tiers
        let tracker_url = torrent
            .primary_tracker()
            .expect("torrent must have at least one tracker")
            .to_string();
        let announcer = HttpAnnouncer::new(tracker_url, torrent.clone(), id.clone(), stat_rx.clone());

        let (command_tx, command_rx) = mpsc::channel(512);

        TorrentSwarm {
            peer_handles,
            torrent,
            storage,
            id,
            announcers: vec![announcer],
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
        for p in self.peer_handles.iter() {
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
        let self_handle = self.make_handle();

        let mut aggregate_ticker = interval(Duration::from_mins(5));

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

        loop {
            // TODO: support multiple announcers
            let announcer_ready = self.announcers[0].ready();
            tokio::select! {
                _ = aggregate_ticker.tick() => {
                    self.aggregate_peer_stats();
                }
                _ = announcer_ready => {
                    let mut new_peers = self.announcers[0].announce().await.unwrap();

                    // don't needlessly connect to peers we already have
                    self.peer_handles.sort_unstable();
                    new_peers.retain(|p|{
                        self.peer_handles.binary_search_by(|h| h.remote_addr.cmp(p)).is_err()
                    });

                    for new_peer in new_peers {
                        let self_handle = self_handle.clone();
                        let has = Box::from(self.stat.verified.clone().as_raw_slice());
                        let bit_field = BitField { has };

                        let connect_peer = self.connect_peer(new_peer);
                        let _ = tokio::spawn(async move {
                            let handle = connect_peer.await?;

                            handle.bit_field(bit_field).await.unwrap();
                            handle.unchoke_peer().await.unwrap();
                            handle.fancy_peer().await.unwrap();


                            self_handle.add_initialized_peer(handle).await;
                            anyhow::Ok(())
                        });
                    }
                },
                _ = &mut download, if !download_done => {
                    download_done = true;
                },
                Some(command) = self.inbound_msgs.recv() => self.process_command(command).await,
            }
        }
    }

    pub fn best_peer(&self, piece: u32, total_piece_requested: usize) -> Option<PeerHandle> {
        let candidates: Vec<_> = self
            .peer_handles
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
            TorrentSwarmCommand::SelfCommand(command) => self.process_self_commands(command).await,
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
                    .peer_handles
                    .binary_search_by(|h| h.remote_addr.cmp(&from))
                    .expect("Only us remove peers, how could it be gone");
                let _ = self.peer_handles[peer_idx].send_data(resp).await;
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
                for p in self.peer_handles.iter() {
                    work.push(async move {
                        let _ = p.send_we_have(piece).await;
                    })
                }

                join_all(work).await;
            }
        }
    }

    async fn process_self_commands(&mut self, command: TorrentSwarmSelfCommand) {
        match command {
            TorrentSwarmSelfCommand::HandleNewPeerConnection(peer) => {
                // TODO: they shouldnt need to be dedup twice since a well formed peer connection only comes back
                //       when we dont have it
                self.peer_handles.sort_unstable();
                let insertion_idx = self.peer_handles.partition_point(|p| p < &peer);
                if insertion_idx == self.peer_handles.len() || self.peer_handles[insertion_idx] != peer {
                    self.peer_handles.insert(insertion_idx, peer);
                }
            }
        }
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

    pub fn initialize_peer(&self, peer: PeerHandle) -> impl Future<Output = anyhow::Result<()>> + use<> {
        // the connection between us and the peer can be bad, so we want the future to be able to
        // be spawned as a task to not block others
        let has = Box::from(self.stat.verified.clone().as_raw_slice());
        async move {
            peer.bit_field(BitField { has }).await.unwrap();
            peer.unchoke_peer().await.unwrap();
            peer.fancy_peer().await.unwrap();

            Ok(())
        }
    }
}
