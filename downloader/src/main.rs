mod defs;
mod peer;
mod settings;
mod storage;
mod sys_tcp;
mod torrent;

use anyhow::bail;
use futures::StreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use rand::prelude::*;
use reqwest::Client;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::pin::pin;
use std::time::Duration;
use std::{env, fs};
use std::{fs::File, sync::Arc};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, Sleep, sleep_until};
use url::form_urlencoded;

use crate::peer::PeerStatistics;
use crate::peer::download::Download;
use crate::storage::TorrentStorage;
use peer::{PeerHandle, connect_peer};
use torrent::{Torrent, parse_torrent};

struct Identity {
    peer_id: [u8; 20],
    serving: SocketAddrV4,
}

/// Handles announcements to a single tracker server
struct Announcer {
    tracker_url: String,
    torrent: Arc<Torrent>,
    identity: Arc<Identity>,
    next_ready: Instant,
    swarm_stat: watch::Receiver<TorrentSwarmStats>,
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

        // self.torrent_swarm_controller.add_peer(peers).await;
        Ok(peers)
    }
}

#[derive(Clone, Debug, Copy)]
struct TorrentSwarmStats {
    pub uploaded: usize,
    pub downloaded: usize,
    pub left: usize,
}

// TODO: move this into peer mod so it doesn't need to be pub
pub struct TorrentSwarm {
    peer_handles: Vec<PeerHandle>,
    torrent: Arc<Torrent>,
    storage: Arc<TorrentStorage>,

    id: Arc<Identity>,

    announcers: Vec<Announcer>,

    stat: TorrentSwarmStats,
    stat_snapshot_syn: watch::Sender<TorrentSwarmStats>,
}

impl TorrentSwarm {
    pub fn new(torrent: Arc<Torrent>, storage: Arc<TorrentStorage>, id: Arc<Identity>) -> TorrentSwarm {
        // Create shared peer handles
        let peer_handles = Vec::<PeerHandle>::new();

        // TODO: this only works for fresh downloads
        let stat = TorrentSwarmStats {
            uploaded: 0,
            downloaded: 0,
            left: torrent.total_size as usize,
        };
        let (stat_syn, stat_ack) = watch::channel(stat);

        // Create announcer with access to peer handles and storage stats
        // For now, use the primary tracker (first tracker in first tier)
        // TODO: Support multiple trackers from announce_tiers
        let tracker_url = torrent
            .primary_tracker()
            .expect("torrent must have at least one tracker")
            .to_string();
        let announcer = Announcer::new(tracker_url, torrent.clone(), id.clone(), stat_ack);

        // Spawn the announcer loop
        // let announcers = vec![tokio::spawn(announcer_ev_loop(announcer))];

        TorrentSwarm {
            peer_handles,
            torrent,
            storage,
            id,
            announcers: vec![announcer],
            stat,
            stat_snapshot_syn: stat_syn,
        }
    }

    async fn work_loop(&mut self) {
        let (new_connection_tx, mut new_connection_rx) = mpsc::channel(200);

        // Get a raw pointer to self to bypass borrow checker
        let self_ptr: *const TorrentSwarm = self;

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
                _ = announcer_ready => {
                    let new_peers = self.announcers[0].announce().await.unwrap();

                    // successful connections are pushed to the new_connection_rx channel
                    let _ = tokio::spawn(try_connect(self.id.clone(), self.storage.clone(), self.torrent.clone(), new_peers, new_connection_tx.clone())).await;
                },
                Some(connection) = new_connection_rx.recv() => {
                    let insertion_idx = self.peer_handles.partition_point(|p| p < &connection);
                    if insertion_idx == self.peer_handles.len() || self.peer_handles[insertion_idx] != connection {
                        self.peer_handles.insert(insertion_idx, connection);
                    }
                },
                _ = &mut download, if !download_done => {
                    download_done = true;
                }
            }
        }
    }

    fn choose(&self, piece: u32) -> Option<PeerHandle> {
        // Read all peer stats from watch receivers (non-blocking)
        let candidates: Vec<(PeerHandle, PeerStatistics)> = self
            .peer_handles
            .iter()
            .filter_map(|h| {
                let state = h.state.read().unwrap();
                if !state.ready() || !state.they_have(piece) {
                    return None;
                }
                let stat = *h.stats_rx.borrow();
                Some((h.clone(), stat))
            })
            .collect();

        // Select best peer by UCB
        candidates
            .into_iter()
            .max_by(|(_, a), (_, b)| a.ucb.total_cmp(&b.ucb))
            .map(|(handle, _)| handle)
    }
}

async fn try_connect(
    id: Arc<Identity>,
    storage: Arc<TorrentStorage>,
    torrent: Arc<Torrent>,
    newly_discovered: Vec<SocketAddrV4>,
    new_connction_tx: mpsc::Sender<PeerHandle>,
) {
    let work = FuturesUnordered::new();
    for new_peer in newly_discovered {
        work.push(connect_peer(
            new_peer.clone(),
            id.peer_id,
            torrent.clone(),
            storage.clone(),
        ));
    }

    let new_peers: Vec<_> = work.collect().await;
    for new_peer in new_peers.into_iter().filter_map(Result::ok) {
        let _ = new_connction_tx.send(new_peer).await;
    }
}

// async fn download_loop(&self) {
// let mut missing_pieces: Vec<u32> = (0..self.torrent.pieces.len()).map(|p| p.try_into().unwrap()).collect();
// missing_pieces.shuffle(&mut rand::rng());
// let mut in_flight = HashSet::new();
// let mut completion_rx = self.piece_completed_rx.lock().await;
//
// loop {
//     if missing_pieces.is_empty() {
//         break;
//     }
//
//     if self.storage.all_verified() {
//         break;
//     }
//
//     // Find a piece to request that's not already in-flight
//     let piece_to_request = missing_pieces.iter().find(|&&p| !in_flight.contains(&p)).copied();
//
//     if let Some(piece) = piece_to_request {
//         // We have a piece to request, select between requesting and receiving completions
//         tokio::select! {
//             // Handle piece completions
//             Some(completed_piece) = completion_rx.recv() => {
//                 in_flight.remove(&completed_piece);
//                 missing_pieces.retain(|&p| p != completed_piece);
//             }
//
//             // Request a new piece
//             _ = async {
//                 let peer_handles = self.peer_handles.read().await;
//                 if let Some(peer) = choose(&peer_handles, piece).await {
//                     drop(peer_handles); // Release lock before awaiting
//                     if peer.request_piece_from_peer(piece).await.is_ok() {
//                         in_flight.insert(piece);
//                     }
//                 } else {
//                     // No peer available, wait a bit
//                     time::sleep(Duration::from_millis(100)).await;
//                 }
//             } => {}
//         }
//     } else {
//         // All wanted pieces are in-flight, just wait for completions
//         if let Some(completed_piece) = completion_rx.recv().await {
//             in_flight.remove(&completed_piece);
//             missing_pieces.retain(|&p| p != completed_piece);
//         } else {
//             // Channel closed, break
//             break;
//         }
//     }
// }
// }

struct BtClient {
    id: Arc<Identity>,
    swarms: HashMap<InfoHash, TorrentSwarm>,
}

impl BtClient {
    fn new(id: Identity) -> Self {
        BtClient {
            id: Arc::new(id),
            swarms: HashMap::new(),
        }
    }

    fn add_task(&mut self, mut torrent: Torrent) -> anyhow::Result<()> {
        if self.swarms.contains_key(&torrent.info_hash) {
            bail!("task with this info hash already exists");
        }

        let mut files = vec![];
        for (_size, file) in torrent.files.iter_mut() {
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            files.push(File::create(&file)?);
        }

        let torrent = Arc::new(torrent);
        let storage = TorrentStorage::new(torrent.clone(), files);
        let storage = Arc::new(storage);

        let task = TorrentSwarm::new(torrent.clone(), storage, self.id.clone());

        self.swarms.insert(torrent.info_hash, task);
        Ok(())
    }

    async fn work(&mut self) -> anyhow::Result<()> {
        // let mut vec = vec![];
        // for (info_hash, share) in self.swarms.iter_mut() {
        // vec.push(share.work_loop());
        // }

        // for handle in vec {
        //     handle.await;
        // }

        // join_all(vec).await;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<_>>();

    let meta_file = PathBuf::from(&args[1]);
    let meta_bytes = std::fs::read(&meta_file).unwrap();
    let torrent = parse_torrent(&meta_bytes).unwrap();

    let mut client = BtClient::new(Identity {
        peer_id: [0u8; 20],
        serving: SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 6881),
    });

    client.add_task(torrent).unwrap();
    client.work().await?;

    Ok(())
}
