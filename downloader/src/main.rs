mod peer;
mod settings;
mod storage;
mod sys_tcp;
mod torrent;
mod defs;

use anyhow::bail;
use futures::StreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use rand::prelude::*;
use reqwest::Client;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::time::Duration;
use std::{env, fs};
use std::{fs::File, sync::Arc};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::time;
use tokio::time::sleep;
use url::form_urlencoded;

use peer::{PeerHandle, PeerStatistics, create_peer_connection, peer_loop};
use storage::{StorageStats, TorrentStorage, TorrentStorageHandle, file_loop};
use torrent::{Torrent, parse_torrent};
use crate::defs::{AMutex, ARwLock};

struct Identity {
    peer_id: [u8; 20],
    serving: SocketAddrV4,
}

/// Handles announcements to a single tracker server
struct Announcer {
    tracker_url: String,
    torrent: Arc<Torrent>,
    identity: Arc<Identity>,
    interval: Duration,
    peer_handles: Arc<tokio::sync::RwLock<Vec<PeerHandle>>>,
    storage_rx: watch::Receiver<StorageStats>,
}

impl Announcer {
    fn new(
        tracker_url: String,
        torrent: Arc<Torrent>,
        identity: Arc<Identity>,
        peer_handles: Arc<tokio::sync::RwLock<Vec<PeerHandle>>>,
        storage_rx: watch::Receiver<StorageStats>,
    ) -> Self {
        Announcer {
            tracker_url,
            torrent,
            identity,
            interval: Duration::from_secs(0),
            peer_handles,
            storage_rx,
        }
    }

    /// Perform a single announce and return the interval and discovered peers
    async fn announce(&self) -> anyhow::Result<(Duration, Vec<SocketAddrV4>)> {
        // Read peer stats from watch receivers (non-blocking)
        let peer_handles = self.peer_handles.read().await;
        let (total_tx, total_rx) =
            peer_handles
                .iter()
                .map(|h| *h.stats_rx.borrow())
                .fold((0u64, 0u64), |(tx, rx), stat| {
                    (
                        tx + stat.tcp_info.tcpi_bytes_acked,
                        rx + stat.tcp_info.tcpi_bytes_received,
                    )
                });

        // Read storage stats (non-blocking)
        let storage_stats = *self.storage_rx.borrow();

        // URL-encode info_hash and peer_id
        let info_hash_encoded: String = form_urlencoded::byte_serialize(&self.torrent.info_hash.0).collect();
        let peer_id_encoded: String = form_urlencoded::byte_serialize(&self.identity.peer_id).collect();

        // Build the announce URL
        let url = format!(
            "{tracker_url}?info_hash={info_hash}&peer_id={peer_id}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&compact=1",
            tracker_url = self.tracker_url,
            info_hash = info_hash_encoded,
            peer_id = peer_id_encoded,
            port = self.identity.serving.port(),
            uploaded = total_tx,
            downloaded = total_rx,
            left = storage_stats.remaining_bytes,
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

        Ok((Duration::from_secs(*interval as u64), peers))
    }
}

/// Run the announce loop, sending discovered peers through the channel
async fn run_announcer_loop(
    mut announcer: Announcer,
    discovered_peers_tx: Sender<Vec<SocketAddrV4>>,
) -> anyhow::Result<()> {
    loop {
        sleep(announcer.interval).await;

        match announcer.announce().await {
            Ok((new_interval, peers)) => {
                announcer.interval = new_interval;
                if !peers.is_empty() {
                    let _ = discovered_peers_tx.send(peers).await;
                }
            }
            Err(e) => {
                tracing::warn!("Announce failed: {}", e);
                // Wait a bit before retrying
                sleep(Duration::from_secs(30)).await;
            }
        }
    }
}

struct TorrentSwarm {
    peer_handles: Arc<ARwLock<Vec<PeerHandle>>>,
    torrent: Arc<Torrent>,
    storage: TorrentStorageHandle,

    id: Arc<Identity>,

    /// Receives notifications when pieces are completed
    piece_completed_rx: AMutex<Receiver<u32>>,

    /// Receives newly discovered peers from the announcer
    discovered_peers_rx: AMutex<Receiver<Vec<SocketAddrV4>>>,
}

impl TorrentSwarm {
    pub fn new(
        torrent: Arc<Torrent>,
        sharing: TorrentStorageHandle,
        piece_completed_rx: Receiver<u32>,
        storage_stats_rx: watch::Receiver<StorageStats>,
        id: Arc<Identity>,
    ) -> TorrentSwarm {
        let (discovered_peers_tx, discovered_peers_rx) = mpsc::channel(100);

        // Create shared peer handles
        let peer_handles = Arc::new(ARwLock::new(Vec::<PeerHandle>::new()));

        // Create announcer with access to peer handles and storage stats
        // For now, use the primary tracker (first tracker in first tier)
        // TODO: Support multiple trackers from announce_tiers
        let tracker_url = torrent
            .primary_tracker()
            .expect("torrent must have at least one tracker")
            .to_string();
        let announcer = Announcer::new(
            tracker_url,
            torrent.clone(),
            id.clone(),
            peer_handles.clone(),
            storage_stats_rx,
        );

        // Spawn the announcer loop (no callback needed!)
        tokio::spawn(run_announcer_loop(announcer, discovered_peers_tx));

        TorrentSwarm {
            peer_handles,
            torrent,
            storage: sharing,
            id,
            piece_completed_rx: tokio::sync::Mutex::new(piece_completed_rx),
            discovered_peers_rx: tokio::sync::Mutex::new(discovered_peers_rx),
        }
    }

    async fn try_add_peer(&self, peer: SocketAddrV4) -> anyhow::Result<()> {
        let (peer_conn, handle) = create_peer_connection(
            peer,
            &self.torrent.info_hash,
            &self.id.peer_id,
            self.torrent.clone(), 
            self.storage.clone(),
        )
        .await?;

        // Add the peer handle to the shared list
        self.peer_handles.write().await.push(handle);

        tokio::spawn(peer_loop(peer_conn));
        Ok(())
    }

    async fn work_loop(&mut self) {
        tokio::select! {
            _ = self.download_loop() => {},
            _ = self.handle_discovered_peers() => {},
        }
    }

    async fn download_loop(&self) {
        let mut missing_pieces: Vec<u32> = (0..self.torrent.pieces.len()).map(|p| p.try_into().unwrap()).collect();
        missing_pieces.shuffle(&mut rand::rng());
        let mut in_flight = HashSet::new();
        let mut completion_rx = self.piece_completed_rx.lock().await;

        loop {
            if missing_pieces.is_empty() {
                break;
            }

            if self.storage.all_verified().await {
                break;
            }

            // Find a piece to request that's not already in-flight
            let piece_to_request = missing_pieces.iter().find(|&&p| !in_flight.contains(&p)).copied();

            if let Some(piece) = piece_to_request {
                // We have a piece to request, select between requesting and receiving completions
                tokio::select! {
                    // Handle piece completions
                    Some(completed_piece) = completion_rx.recv() => {
                        in_flight.remove(&completed_piece);
                        missing_pieces.retain(|&p| p != completed_piece);
                    }

                    // Request a new piece
                    _ = async {
                        let peer_handles = self.peer_handles.read().await;
                        if let Some(peer) = choose(&peer_handles, piece).await {
                            drop(peer_handles); // Release lock before awaiting
                            if peer.request_piece_from_peer(piece).await.is_ok() {
                                in_flight.insert(piece);
                            }
                        } else {
                            // No peer available, wait a bit
                            time::sleep(Duration::from_millis(100)).await;
                        }
                    } => {}
                }
            } else {
                // All wanted pieces are in-flight, just wait for completions
                if let Some(completed_piece) = completion_rx.recv().await {
                    in_flight.remove(&completed_piece);
                    missing_pieces.retain(|&p| p != completed_piece);
                } else {
                    // Channel closed, break
                    break;
                }
            }
        }
    }

    async fn handle_discovered_peers(&self) -> anyhow::Result<()> {
        let mut discovered_peers_rx = self.discovered_peers_rx.lock().await;

        while let Some(peers) = discovered_peers_rx.recv().await {
            let mut add_peers: FuturesUnordered<_> = peers.into_iter().map(|p| self.try_add_peer(p)).collect();
            while let Some(_) = add_peers.next().await {}
        }

        Ok(())
    }
}

async fn choose(peers: &Vec<PeerHandle>, piece: u32) -> Option<PeerHandle> {
    // Read all peer stats from watch receivers (non-blocking)
    let candidates: Vec<(PeerHandle, PeerStatistics)> = peers
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

struct BtClient {
    id: Arc<Identity>,
    tasks: HashMap<InfoHash, TorrentSwarm>,
}

impl BtClient {
    fn new(id: Identity) -> Self {
        BtClient {
            id: Arc::new(id),
            tasks: HashMap::new(),
        }
    }

    fn add_task(&mut self, mut torrent: Torrent) -> anyhow::Result<()> {
        if self.tasks.contains_key(&torrent.info_hash) {
            bail!("task with this info hash already exists");
        }

        let mut files = vec![];
        for (_size, file) in torrent.files.iter_mut() {
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            files.push(File::create(&file)?);
        }

        let torrent = Arc::new(torrent);
        let (storage, handle, piece_completed_rx, storage_stats_rx) =
            TorrentStorage::new_with_handle(torrent.clone(), files);
        tokio::spawn(file_loop(storage));

        let task = TorrentSwarm::new(
            torrent.clone(),
            handle,
            piece_completed_rx,
            storage_stats_rx,
            self.id.clone(),
        );

        self.tasks.insert(torrent.info_hash, task);
        Ok(())
    }

    async fn work(&mut self) -> anyhow::Result<()> {
        let mut vec = vec![];
        for (info_hash, share) in self.tasks.iter_mut() {
            vec.push(share.work_loop());
        }

        // for handle in vec {
        //     handle.await;
        // }

        join_all(vec).await;
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
