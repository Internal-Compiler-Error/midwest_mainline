use crate::defs::Identity;
use crate::peer::download::{Download, DownloadEvent};
use crate::peer::wire::{shake_hands, BitField, BtDecoder, BtEncoder, Piece};
use crate::peer::{peer_ev_loop, PeerConnection, PeerEvent, PeerHandle, PeerState, PeerStatistics};
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use anyhow::bail;
use bitvec::boxed::BitBox;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use juicy_bencode::BencodeItemView;
use reqwest::Client;
use std::collections::BTreeMap;
use std::mem::zeroed;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::pin::pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{interval, sleep_until, Instant, Sleep};
use tokio_util::codec::{FramedRead, FramedWrite};
use url::form_urlencoded;

use derive_more::{PartialEq, Eq};
use futures::future::join_all;

/// Handles announcements to a single tracker server
#[derive(Debug, Clone, PartialEq, Eq)]
struct Announcer {
    tracker_url: String,
    torrent: Arc<Torrent>,

    #[eq(skip)] identity: Arc<Identity>,
    #[eq(skip)] next_ready: Instant,
    #[eq(skip)] swarm_stat: watch::Receiver<TorrentSwarmStats>,
}

enum AnnouncerEvent {
    DiscoveredPeers(Vec<SocketAddrV4>)
}

impl Announcer {
    fn new(
        tracker_url: String,
        torrent: Arc<Torrent>,
        identity: Arc<Identity>,
        swarm_stat: watch::Receiver<TorrentSwarmStats>,
    ) -> Self {
        Announcer {
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

        // Parse the bencoded response
        let (_, dict) = juicy_bencode::parse_bencode_dict(&mut bytes.as_ref()).unwrap();

        let mut peers = vec![];
        let Some(BencodeItemView::Integer(interval)) = dict.get(b"interval".as_slice()) else {
            bail!("response interval must be a number");
        };

        if let Some(BencodeItemView::ByteString(peer_bytes)) = dict.get(b"peers".as_slice()) {
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

        self.next_ready = Instant::now() + Duration::from_secs(*interval as u64);
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
    pub async fn add_initialized_peer(&self, peer: PeerHandle)  {
        let _ = self.tx.send(TorrentSwarmCommand::SelfCommand(
            TorrentSwarmSelfCommand::HandleNewPeerConnection(peer),
        )).await;
    }
}

pub enum TorrentSwarmCommand {
    ProcessPeerEvent{ from: SocketAddrV4, event: PeerEvent },
    ProcessDownloadEvent(DownloadEvent),
    SelfCommand(TorrentSwarmSelfCommand),
}

enum TorrentSwarmSelfCommand {
    HandleNewPeerConnection(PeerHandle),
}

// TODO: move this into peer mod so it doesn't need to be pub
pub struct TorrentSwarm {
    peer_handles: Vec<PeerHandle>,
    torrent: Arc<Torrent>,
    storage: Arc<TorrentStorage>,

    id: Arc<Identity>,

    announcers: Vec<Announcer>,

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
        };
        let (stat_tx, stat_rx) = watch::channel(stat.clone());

        // Create announcer with access to peer handles and storage stats
        // For now, use the primary tracker (first tracker in first tier)
        // TODO: Support multiple trackers from announce_tiers
        let tracker_url = torrent
            .primary_tracker()
            .expect("torrent must have at least one tracker")
            .to_string();
        let announcer = Announcer::new(tracker_url, torrent.clone(), id.clone(), stat_rx.clone());

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
            uploaded += snapshot.tcp_info.tcpi_bytes_sent;
            downloaded += snapshot.tcp_info.tcpi_bytes_received;
        }

        self.stat.downloaded = downloaded;
        self.stat.uploaded = uploaded;
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

    pub fn choose(&self, piece: u32) -> Option<PeerHandle> {
        // Read all peer stats from watch receivers (non-blocking)
        let candidates: Vec<(PeerHandle, PeerStatistics)> = self
            .peer_handles
            .iter()
            .filter_map(|h| {
                let state = h.state.read().unwrap();
                if !state.ready() || !state.they_have(piece) {
                    return None;
                }
                let stat = *h.stats.borrow();
                Some((h.clone(), stat))
            })
            .collect();

        // Select best peer by UCB
        candidates
            .into_iter()
            .max_by(|(_, a), (_, b)| a.ucb.total_cmp(&b.ucb))
            .map(|(handle, _)| handle)
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
                let peer_idx =  self.peer_handles.binary_search_by(|h| {
                    h.remote_addr.cmp(&from)
                }).expect("Only us remove peers, how could it be gone");
                self.peer_handles[peer_idx].send_data(resp).await;
            },
            PeerEvent::UpdateStatsReady => {
                let peer_idx =  self.peer_handles.binary_search_by(|h| {
                    h.remote_addr.cmp(&from)
                }).expect("Only us remove peers, how could it be gone");

                // TODO: TorrentSwarm should just contain all these sorts of stats instead of letting storage manage itself
                let verified_count = self.verified_cnt();
                self.peer_handles[peer_idx].update_stats(verified_count).await;
            }
        }
    }

    async fn process_download_event(&mut self, event: DownloadEvent) {
        match event {
            DownloadEvent::PieceCompleted(piece) => {
                let valid = self.verify_hash(piece);
                if !valid { return; }

                self.stat.written += (self.torrent.nth_piece_size(piece).expect("we control download task, it's not malicious"));
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
           TorrentSwarmSelfCommand::HandleNewPeerConnection(peer)  => {
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
            let (reader, writer) = tcp.into_split();
            let (commands_tx, commands_rx) = mpsc::channel(1024);

            // Create watch channel for this peer's statistics
            let initial_stats = PeerStatistics {
                tcp_info: unsafe { zeroed() },
                mean_rx: 0.0,
                mean_rx_cnt: 0,
                mean_rx_last_checked: std::time::Instant::now(),
                ucb: 0.0,
                picked_count: 0,
            };
            let (stats_tx, stats_rx) = watch::channel(initial_stats);

            let state = Arc::new(RwLock::new(PeerState {
                they_have: vec![0u8; torrent.pieces.len()].into(),
                choked_us: false,
                choked_them: false,
                interested_them: false,
                interested_us: false,
                requested: BTreeMap::new(),
            }));

            let peer = PeerConnection {
                commands: commands_rx,
                events: event_tx,
                state: state.clone(),
                remote_addr,
                reader: FramedRead::new(reader, BtDecoder),
                writer: FramedWrite::new(writer, BtEncoder),
                requested: Default::default(),
                stat: PeerStatistics {
                tcp_info: unsafe { zeroed() },
                mean_rx: 0.0,
                mean_rx_cnt: 0,
                mean_rx_last_checked: std::time::Instant::now(),
                ucb: 0.0,
                picked_count: 0,
            },

                torrent_stat: stat_snapshot_rx,
                stats_tx,
            };

            let handle = PeerHandle {
                id: handshake.peer_id,
                peer_tx: commands_tx,
                state,
                stats: stats_rx,
                remote_addr,
            };
            tokio::spawn(peer_ev_loop(peer));

            Ok(handle)
        }
    }

    pub fn initialize_peer(&self, peer: PeerHandle) -> impl Future<Output = anyhow::Result<()>> + use<> {
        // the connection between us and the peer can be bad, so we want the future to be able to
        // be spawned as a task to not block others
        let has = Box::from(self.stat.verified.clone().as_raw_slice());
        async move {
            peer.bit_field(BitField {
                has
            }).await.unwrap();
            peer.unchoke_peer().await.unwrap();
            peer.fancy_peer().await.unwrap();

            Ok(())
        }
    }
}

// async fn try_connect(
//     id: Arc<Identity>,
//     storage: Arc<TorrentStorage>,
//     torrent: Arc<Torrent>,
//     newly_discovered: Vec<SocketAddrV4>,
//     new_connction_tx: mpsc::Sender<PeerHandle>,
// ) {
//     let work = FuturesUnordered::new();
//     for new_peer in newly_discovered {
//         work.push(connect_peer(
//             new_peer.clone(),
//             id.peer_id,
//             torrent.clone(),
//             storage.clone(),
//         ));
//     }
//
//     let new_peers: Vec<_> = work.collect().await;
//     for new_peer in new_peers.into_iter().filter_map(Result::ok) {
//         new_peer.bit_field().await.unwrap();
//         new_peer.unchoke_peer().await.unwrap();
//         new_peer.fancy_peer().await.unwrap();
//         let _ = new_connction_tx.send(new_peer).await;
//     }
// }