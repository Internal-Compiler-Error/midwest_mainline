use crate::peer::{PeerHandle, PeerStatistics, peer_ev_loop};
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use rand::seq::SliceRandom;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;

enum Commands {
    NewPeers(Vec<PeerHandle>),
    PieceCompleted(u32),
    RequestPiece(u32),
    Die,
}

struct DownloadInner {
    commands: mpsc::Receiver<Commands>,

    storage: Arc<TorrentStorage>,
    peers: Vec<PeerHandle>,

    inflight: HashSet<u32>,
    missing: Vec<u32>,

    max_inflight: usize,
}

impl DownloadInner {
    pub async fn ev_loop(mut self) {
        self.missing.shuffle(&mut rand::rng());

        loop {}
        while let Some(command) = self.commands.recv().await {
            self.process_command(command).await;
        }
    }

    async fn request_more(&mut self) {
        while self.inflight.len() < self.max_inflight {
            if let Some(piece_to_request) = self.missing.pop() {
                self.request_piece(piece_to_request).await;
            }
        }
    }

    async fn process_command(&mut self, command: Commands) {
        match command {
            Commands::NewPeers(peers) => {
                self.peers.extend(peers);
                self.peers.sort_unstable();
                self.peers.dedup()
            }
            Commands::PieceCompleted(piece) => {
                self.inflight.remove(&piece);
                self.missing.retain(|&x| x != piece); // make a data structure that supports efficient random sampling and deletion if this is slow
            }
            Commands::RequestPiece(piece) => {
                self.request_piece(piece).await;
            }
            Commands::Die => {
                self.commands.close();
            }
        }
    }

    async fn request_piece(&mut self, piece: u32) {
        if let Some(peer) = choose(&self.peers, piece) {
            if peer.request_piece_from_peer(piece).await.is_ok() {
                self.inflight.insert(piece);
            }
        } else {
            // No peer available
        }
    }
}

#[derive(Debug, Clone)]
pub struct Download {
    inner: mpsc::Sender<Commands>,
}

impl Download {
    pub fn new(storage: Arc<TorrentStorage>, torrent: Arc<Torrent>, peers: Vec<PeerHandle>) -> Self {
        let (syn, ack) = mpsc::channel(100);
        let inner = DownloadInner {
            commands: ack,
            storage,
            peers,
            inflight: Default::default(),
            missing: vec![],
            max_inflight: 0,
        };

        tokio::spawn(inner.ev_loop());
        Self { inner: syn }
    }

    pub async fn new_peers(&self, peers: Vec<PeerHandle>) {
        self.inner
            .send(Commands::NewPeers(peers))
            .await
            .expect("it's not dead yet");
    }

    pub async fn piece_completed(&self, piece: u32) {
        self.inner
            .send(Commands::PieceCompleted(piece))
            .await
            .expect("it's not dead yet");
    }

    pub fn dead(&self) -> bool {
        self.inner.is_closed()
    }
}

impl Drop for Download {
    fn drop(&mut self) {
        let _ = self.inner.try_send(Commands::Die);
    }
}

// async fn download_loop(&self) {
//     let mut missing_pieces: Vec<u32> = (0..self.torrent.pieces.len()).map(|p| p.try_into().unwrap()).collect();
//     missing_pieces.shuffle(&mut rand::rng());
//     let mut in_flight = HashSet::new();
//     let mut completion_rx = self.piece_completed_rx.lock().await;
//
//     loop {
//         if missing_pieces.is_empty() {
//             break;
//         }
//
//         if self.storage.all_verified() {
//             break;
//         }
//
//         // Find a piece to request that's not already in-flight
//         let piece_to_request = missing_pieces.iter().find(|&&p| !in_flight.contains(&p)).copied();
//
//         if let Some(piece) = piece_to_request {
//             // We have a piece to request, select between requesting and receiving completions
//             tokio::select! {
//                 // Handle piece completions
//                 Some(completed_piece) = completion_rx.recv() => {
//                     in_flight.remove(&completed_piece);
//                     missing_pieces.retain(|&p| p != completed_piece);
//                 }
//
//                 // Request a new piece
//                 _ = async {
//                     let peer_handles = self.peer_handles.read().await;
//                     if let Some(peer) = choose(&peer_handles, piece) {
//                         drop(peer_handles); // Release lock before awaiting
//                         if peer.request_piece_from_peer(piece).await.is_ok() {
//                             in_flight.insert(piece);
//                         }
//                     } else {
//                         // No peer available, wait a bit
//                         time::sleep(Duration::from_millis(100)).await;
//                     }
//                 } => {}
//             }
//         } else {
//             // All wanted pieces are in-flight, just wait for completions
//             if let Some(completed_piece) = completion_rx.recv().await {
//                 in_flight.remove(&completed_piece);
//                 missing_pieces.retain(|&p| p != completed_piece);
//             } else {
//                 // Channel closed, break
//                 break;
//             }
//         }
//     }
// }
fn choose(peers: &Vec<PeerHandle>, piece: u32) -> Option<PeerHandle> {
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
