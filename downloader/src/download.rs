use crate::peer::PeerHandle;
use crate::settings::BLOCK_SIZE;
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use crate::torrent_swarm::{TorrentSwarm, TorrentSwarmCommand, TorrentSwarmSelfCommand};
use crate::wire::Request;
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use rand::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::sleep;
use tracing::{info, trace};

pub enum DownloadEvent {
    PieceCompleted(u32),
}

/// Runs as its own task alongside `TorrentSwarm`. It only holds owned/shared state (Arcs, a
/// command-channel sender) rather than a reference into `TorrentSwarm`, so it can't alias with
/// the `&mut self` methods the swarm's own event loop uses concurrently.
pub struct Download {
    torrent: Arc<Torrent>,
    storage: Arc<TorrentStorage>,
    max_inflight: usize,
    command_tx: mpsc::Sender<TorrentSwarmCommand>,
}

impl Download {
    pub fn new(torrent_swarm: &TorrentSwarm) -> Download {
        Self {
            torrent: torrent_swarm.torrent().clone(),
            storage: torrent_swarm.storage().clone(),
            max_inflight: 100, // TODO: should be configurable
            command_tx: torrent_swarm.command_sender(),
        }
    }

    /// Asks the swarm to pick the best peer to request `piece` from.
    async fn best_peer(&self, piece: u32, total_piece_requested: usize) -> Option<PeerHandle> {
        let (resp, rx) = oneshot::channel();
        self.command_tx
            .send(TorrentSwarmCommand::SelfCommand(TorrentSwarmSelfCommand::ChooseBestPeer {
                piece,
                total_piece_requested,
                resp,
            }))
            .await
            .ok()?;
        rx.await.ok().flatten()
    }

    /// Asks the swarm whether every piece has been hash-verified. If the swarm is gone,
    /// treat that as "done" so this task doesn't spin forever.
    async fn all_verified(&self) -> bool {
        let (resp, rx) = oneshot::channel();
        if self
            .command_tx
            .send(TorrentSwarmCommand::SelfCommand(TorrentSwarmSelfCommand::QueryAllVerified { resp }))
            .await
            .is_err()
        {
            return true;
        }
        rx.await.unwrap_or(true)
    }
}

impl Download {
    #[tracing::instrument(skip(self))]
    pub async fn download_loop(&self) {
        // the number of pieces downloaded *in* this session, already download pieces don't count
        let mut downloaded = 0;
        let mut missing_pieces: Vec<u32> = (0..self.torrent.pieces.len()).map(|p| p.try_into().unwrap()).collect();
        missing_pieces.shuffle(&mut rand::rng());
        let mut in_flight = HashSet::new();

        let mut piece_completed = FuturesUnordered::new();
        let unblocked = Notify::new();
        unblocked.notify_one();
        loop {
            if missing_pieces.is_empty() {
                break;
            }

            if self.all_verified().await {
                break;
            }

            tokio::select! {
                Some(piece) = piece_completed.next() => {
                    info!("piece {} is completed", piece);
                    downloaded += 1;

                    in_flight.remove(&piece);
                    missing_pieces.retain(|missing| *missing != piece);

                    if in_flight.len() <= self.max_inflight {
                        unblocked.notify_one();
                    }
                }
                // TODO: suprious wakeups?
                _ = unblocked.notified() => {
                    let piece_to_request = missing_pieces.iter().find(|&&p| !in_flight.contains(&p)).copied();
                    let Some(piece) = piece_to_request else {
                        continue;
                    };

                    if let Some(peer) = self.best_peer(piece, downloaded).await {
                        in_flight.insert(piece);

                        piece_completed.push(async move {
                            info!("Started downloading piece {}", piece);
                            self.download_piece(piece, peer.clone()).await.expect("Oh this is wrong for sure, download can absolutely fail");

                            piece
                        });

                    } else {
                        trace!("No available peers, sleep for 1000ms");
                        sleep(Duration::from_millis(1000)).await;
                        unblocked.notify_one();
                    }
                }

            }
        }
    }

    async fn download_piece(&self, piece: u32, peer: PeerHandle) -> anyhow::Result<()> {
        let piece_size = self.torrent.nth_piece_size(piece).expect("piece index in range");
        let mut buf = vec![0u8; piece_size];
        let disjoint_sections = buf.chunks_mut(BLOCK_SIZE).enumerate();

        let mut download_blocks = vec![];
        for (idx, section) in disjoint_sections {
            let idx = idx as u32;
            let peer = peer.clone();
            download_blocks.push(async move {
                self.download_block(
                    Request {
                        index: piece,
                        begin: idx * BLOCK_SIZE as u32,
                        length: section.len() as u32,
                    },
                    peer,
                    section,
                )
                .await
                .expect("implement retries?");
            });
        }

        // TODO: need to ensure they all succeeded
        let _: () = join_all(download_blocks).await.into_iter().collect();
        self.storage.write_piece(piece, buf.into_boxed_slice())?;

        // hand off to the swarm: it hash-verifies, updates the verified bitset, and
        // broadcasts Have to every active peer
        let _ = self
            .command_tx
            .send(TorrentSwarmCommand::ProcessDownloadEvent(DownloadEvent::PieceCompleted(piece)))
            .await;

        Ok(())
    }

    async fn download_block(&self, req: Request, peer: PeerHandle, buffer: &mut [u8]) -> anyhow::Result<()> {
        let data = peer.request_data_from_peer(req).await?;
        buffer.copy_from_slice(&data);

        Ok(())
    }
}
