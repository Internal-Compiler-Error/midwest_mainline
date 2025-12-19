mod defs;
mod peer;
mod settings;
mod storage;
mod sys_tcp;
mod torrent;

use anyhow::bail;
use futures::future::join_all;
use futures::StreamExt;
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use rand::prelude::*;
use reqwest::Client;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::time::Duration;
use std::{env, fs};
use std::{fs::File, sync::Arc};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{interval, Interval, MissedTickBehavior};
use url::form_urlencoded;

use crate::storage::TorrentStorage;
use peer::{connect_peer, PeerHandle};
use torrent::{parse_torrent, Torrent};

struct Identity {
    peer_id: [u8; 20],
    serving: SocketAddrV4,
}

/// Handles announcements to a single tracker server
struct Announcer {
    tracker_url: String,
    torrent: Arc<Torrent>,
    identity: Arc<Identity>,
    interval: Interval,
    swarm_stat: watch::Receiver<TorrentSwarmStats>,
    torrent_swarm_controller: TorrentSwarmController,
}

impl Announcer {
    fn new(
        tracker_url: String,
        torrent: Arc<Torrent>,
        identity: Arc<Identity>,
        swarm_stat: watch::Receiver<TorrentSwarmStats>,
        torrent_swarm_controller: TorrentSwarmController,
    ) -> Self {
        Announcer {
            tracker_url,
            torrent,
            identity,
            interval: interval(Duration::from_secs(10)),
            swarm_stat,
            torrent_swarm_controller,
        }
    }

    /// Perform a single announce and return the interval and discovered peers
    async fn announce(
        &mut self,
    ) -> anyhow::Result<()> {
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

        self.interval = tokio::time::interval(Duration::from_secs(*interval as u64));
        self.interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        self.torrent_swarm_controller.add_peer(peers).await;
        Ok(())
    }
}

async fn announcer_ev_loop(
    mut announcer: Announcer,
) -> anyhow::Result<()> {
    loop {
        announcer.interval.tick().await;
        let _ = announcer.announce().await; // TODO: log on error
    }
}


#[derive(Clone, Debug, Copy)]
struct TorrentSwarmStats {
    pub uploaded: usize,
    pub downloaded: usize,
    pub left: usize,
}

enum TorrentSwarmCommands {
    NewPeers(Vec<SocketAddrV4>),
    NewConnection(Vec<PeerHandle>),
}

struct TorrentSwarm {
    peer_handles: Vec<PeerHandle>,
    torrent: Arc<Torrent>,
    storage: Arc<TorrentStorage>,

    id: Arc<Identity>,

    pending_ops: mpsc::Receiver<TorrentSwarmCommands>,
    loopback_thing: mpsc::Sender<TorrentSwarmCommands>,

    announcers: Vec<JoinHandle<anyhow::Result<()>>>,

    stat: TorrentSwarmStats,
    stat_snapshot_syn: watch::Sender<TorrentSwarmStats>,
}

#[derive(Debug, Clone)]
struct TorrentSwarmController {
    tx: mpsc::Sender<TorrentSwarmCommands>,
}

impl TorrentSwarmController {
    pub async fn add_peer_connection(&self, peer: Vec<PeerHandle>) -> anyhow::Result<()> {
        self.tx.send(TorrentSwarmCommands::NewConnection(peer)).await?;
        Ok(())
    }

    pub async fn add_peer(&self, peer: Vec<SocketAddrV4>) -> anyhow::Result<()> {
        self.tx.send(TorrentSwarmCommands::NewPeers(peer)).await?;
        Ok(())
    }
}

impl TorrentSwarm {
    pub fn new(
        torrent: Arc<Torrent>,
        storage: Arc<TorrentStorage>,
        id: Arc<Identity>,
    ) -> TorrentSwarm {
        // Create shared peer handles
        let peer_handles = Vec::<PeerHandle>::new();

        // TODO: this only works for fresh downloads
        let stat = TorrentSwarmStats {
            uploaded: 0,
            downloaded: 0,
            left: torrent.total_size as usize,
        };
        let (stat_syn, stat_ack) = watch::channel(stat);


        // TODO: incomplete
        let (syn, ack) = mpsc::channel(100);

        let controller = TorrentSwarmController {
            tx: syn.clone(),
        };

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
            stat_ack,
            controller,
        );

        // Spawn the announcer loop
        let announcers = vec![tokio::spawn(announcer_ev_loop(announcer))];


        TorrentSwarm {
            peer_handles,
            torrent,
            storage,
            id,
            pending_ops: ack,
            loopback_thing: syn,
            announcers,
            stat,
            stat_snapshot_syn: stat_syn,
        }
    }

    pub fn make_remote(&self) -> TorrentSwarmController {
        TorrentSwarmController {
            tx: self.loopback_thing.clone(),
        }
    }

    async fn process_command(&mut self, command: TorrentSwarmCommands) -> anyhow::Result<()> {
        match command {
            TorrentSwarmCommands::NewPeers(peers) => {
                for p in peers {
                    let remote = self.make_remote();
                    let peer_id = self.id.peer_id.clone();
                    let torrent = self.torrent.clone();
                    let storage = self.storage.clone();

                    tokio::spawn(async move {
                        let peer = connect_peer(
                            p,
                            peer_id,
                            torrent,
                            storage,
                        ).await.unwrap();
                        remote.add_peer_connection(vec![peer]).await;
                    });
                }
            }
        TorrentSwarmCommands::NewConnection(connection) => {
            self.peer_handles.extend(connection);
            self.peer_handles.sort_unstable();
            self.peer_handles.dedup();
            todo!("inform the download task")
        }
    }

    Ok(())
}


async fn work_loop(&mut self) {
    // tokio::select! {
    // _ = self.download_loop() => {},
    // Some(peers) = self.discovered_peers_rx.recv().await {
    //
    // },
    // }
}

async fn download_loop(&self) {
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
}
}
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
        let storage =
            TorrentStorage::new(torrent.clone(), files);
        let storage = Arc::new(storage);

        let task = TorrentSwarm::new(
            torrent.clone(),
            storage,
            self.id.clone(),
        );

        self.swarms.insert(torrent.info_hash, task);
        Ok(())
    }

    async fn work(&mut self) -> anyhow::Result<()> {
        let mut vec = vec![];
        for (info_hash, share) in self.swarms.iter_mut() {
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
