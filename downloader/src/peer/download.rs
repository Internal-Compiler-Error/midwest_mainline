use crate::TorrentSwarm;
use crate::peer::PeerHandle;
use crate::peer::wire::Request;
use crate::settings::BLOCK_SIZE;
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use rand::seq::SliceRandom;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::sleep;

pub struct Download<'a> {
    torrent: Arc<Torrent>,
    storage: Arc<TorrentStorage>,
    max_inflight: usize,
    torrent_swarm: &'a TorrentSwarm,
}

impl<'a> Download<'a> {
    pub fn new(torrent_swarm: &'a TorrentSwarm) -> Download<'a> {
        Self {
            torrent: torrent_swarm.torrent.clone(),
            storage: torrent_swarm.storage.clone(),
            max_inflight: 100, // TODO: should be configurable
            torrent_swarm,
        }
    }
}

impl Download<'_> {
    // pub fn new(torrent_swarm: &'_ TorrentSwarm) -> Download<'_> {
    //     Self {
    //         torrent: torrent_swarm.torrent.clone(),
    //         storage: torrent_swarm.storage.clone(),
    //         max_inflight: 100, // TODO: should be configurable
    //         torrent_swarm,
    //     }
    // }

    pub async fn download_loop(&self) {
        let mut missing_pieces: Vec<u32> = (0..self.torrent.pieces.len()).map(|p| p.try_into().unwrap()).collect();
        missing_pieces.shuffle(&mut rand::rng());
        let mut in_flight = HashSet::new();

        let mut work = FuturesUnordered::new();
        let unblocked = Notify::new();
        unblocked.notify_one();
        loop {
            if missing_pieces.is_empty() {
                break;
            }

            if self.storage.all_verified() {
                break;
            }

            tokio::select! {
                Some(piece) = work.next() => {
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

                    if let Some(peer) = self.torrent_swarm.choose(piece) {
                        in_flight.insert(piece);
                        work.push(async move {
                            let piece = piece;
                            self.download_piece(piece, peer).await.expect("Oh this is wrong for sure, download can absolutely fail");
                            piece

                        });
                    } else {

                        sleep(Duration::from_millis(100)).await;
                        unblocked.notify_one();
                    }
                }

            // // Find a piece to request that's not already in-flight
            // let piece_to_request = missing_pieces.iter().find(|&&p| !in_flight.contains(&p)).copied();
            //
            // if let Some(piece) = piece_to_request {
            //     if let Some(peer) = self.torrent_swarm.choose(piece) {
            //         in_flight.insert(piece);
            //         work.push(async move {
            //             let piece = piece;
            //             // let unblocked = unblocked.clone();
            //             self.download_piece(piece, peer).await;
            //             piece
            //             // in_flight.remove(&piece);
            //             // missing_pieces.retain(|missing| *missing != piece);
            //         });
            //     } else {
            //         // No peer available, wait a bit
            //         time::sleep(Duration::from_millis(100)).await;
            //     }
            }
        }
    }

    async fn download_piece(&self, piece: u32, peer: PeerHandle) -> anyhow::Result<()> {
        // if self.inflight.contains(&piece) {
        //     return Err(bail!("requested piece is already in flight"));
        // }

        let mut buf = vec![0u8; self.torrent.piece_size as usize];
        let disjoint_sections = buf.chunks_mut(BLOCK_SIZE).enumerate();

        let mut download_blocks = vec![];
        for (idx, section) in disjoint_sections {
            let idx = idx as u32;
            let peer = peer.clone();
            download_blocks.push(async move {
                self.download_block(
                    Request {
                        index: idx,
                        begin: idx * self.torrent.piece_size,
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

        Ok(())
    }

    async fn download_block(&self, req: Request, peer: PeerHandle, buffer: &mut [u8]) -> anyhow::Result<()> {
        let data = peer.request_data_from_peer(req).await?;
        // let (written, buf) = self.block_buf.entry(req.index as usize).or_insert_with(|| {
        //     (
        //         0,
        //         vec![
        //             0u8;
        //             self.torrent
        //                 .nth_piece_size(req.index)
        //                 .expect("people won't send us pieces with invalid index")
        //         ],
        //     )
        // });

        // let length: usize = req.length.try_into().unwrap();
        // let begin: usize = req.begin.try_into().unwrap();

        // *written += length;
        // buf[begin..begin + length].copy_from_slice(&data);
        buffer.copy_from_slice(&data);

        Ok(())
    }
}
