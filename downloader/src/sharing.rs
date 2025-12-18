use crate::Torrent;
use bitvec::prelude::*;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::{fs::File};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot};
use tracing::{error, warn};

#[derive(Debug)]
pub enum FileCommands {
    ReadPiece {
        piece: u32,
        res: oneshot::Sender<anyhow::Result<Box<[u8]>>>,
    },
    WritePiece {
        piece: u32,
        data: Box<[u8]>,
        res: oneshot::Sender<anyhow::Result<()>>,
    },
    AllVerified {
        res: oneshot::Sender<bool>,
    },
    RemainingBytes {
        res: oneshot::Sender<usize>,
    },
    Verified {
        res: oneshot::Sender<BitBox<u8>>,
    },
    VerifiedCnt {
        res: oneshot::Sender<usize>,
    }
}

pub struct SharedFile {
    pub torrent: Arc<Torrent>,
    pub files: Vec<File>,

    /// for each file at `i`, offset[i] contains the offset of the file into the conceptual one
    /// giant file
    offsets: Vec<usize>,

    /// if a piece is verified, it also implies it has been written
    pub verified: BitBox<u8>,

    /// number of pieces that are verified
    pub verified_cnt: usize,

    written: usize,

    pending_ops: Receiver<FileCommands>,
}

#[derive(Debug, Clone)]
pub struct SharedFileHandle {
    tx: Sender<FileCommands>,
}

impl SharedFileHandle {
    pub async fn write_piece(&self, piece: u32, complete_piece: Box<[u8]>) -> anyhow::Result<()> {
        let (syn, ack) = oneshot::channel();
        self.tx
            .send(FileCommands::WritePiece {
                piece,
                data: complete_piece,
                res: syn,
            })
            .await;
        let res = ack.await??;
        Ok(res)
    }

    pub async fn read_piece(&self, piece: u32) -> anyhow::Result<Box<[u8]>> {
        let (syn, ack) = oneshot::channel();

        self.tx.send(FileCommands::ReadPiece { piece, res: syn }).await;
        let res = ack.await??;
        Ok(res)
    }

    pub async fn all_verified(&self) -> bool {
        let (syn, ack) = oneshot::channel();

        self.tx.send(FileCommands::AllVerified { res: syn }).await;
        ack.await.unwrap()
    }

    pub async fn remaining_bytes(&self) -> usize {
        let (syn, ack) = oneshot::channel();

        self.tx.send(FileCommands::RemainingBytes { res: syn }).await;
        ack.await.unwrap()
    }

    pub async fn verified(&self) -> BitBox<u8> {
        let (syn, ack) = oneshot::channel();

        self.tx.send(FileCommands::Verified { res: syn }).await;
        ack.await.unwrap()
    }

    pub async fn verified_cnt(&self) -> usize {
        let (syn, ack) = oneshot::channel();

        self.tx.send(FileCommands::VerifiedCnt { res: syn }).await;
        ack.await.unwrap()
    }
}

async fn file_loop(mut f: SharedFile) {
    while let Some(command) = f.pending_ops.recv().await {
        match command {
            FileCommands::ReadPiece { piece, res } => {
                let has_block = f.verified.get(piece as usize);
                if has_block.is_none() {
                    todo!()
                    // res.send(anyhow!("Don't have block"));
                } else {
                    let mut buf = vec![0u8; f.torrent.piece_size as usize];
                    let mut read = 0;
                    for (file, interval) in f.file_segments(piece) {
                        let len = interval.len();
                        let dst = &mut buf[read..read + len];
                        file.read_exact_at(dst, interval.start as u64).unwrap();

                        read += len;
                    }

                    res.send(Ok(buf.into_boxed_slice()));
                }
            }
            FileCommands::WritePiece { piece, data, res } => {
                let already_has = f.verified.get(piece as usize);
                if already_has.is_none() {
                    error!("a pending write has a piece out of bounds");
                }
                if *already_has.unwrap() {
                    continue;
                }

                if !f.torrent.valid_piece(piece, &data) {
                    warn!("a piece was received that failed to verify");
                    continue;
                }

                res.send(f.write_piece(piece, data));
            }
            FileCommands::AllVerified { res } => {
                res.send(f.all_verified());
            }
            FileCommands::RemainingBytes { res } => {
                res.send(f.remaining_bytes());
            },
            FileCommands::Verified { res } => {
                res.send(f.verified.clone());
            },
            FileCommands::VerifiedCnt { res } => {
                res.send(f.verified_cnt);
            }
        }
    }
}

impl SharedFile {
    pub fn new(torrent: Arc<Torrent>, files: Vec<File>, queue: Receiver<FileCommands>) -> SharedFile {
        let piece_count = torrent.pieces.len();
        let stupid = vec![false; piece_count as usize];
        let verified = BitBox::from_iter(stupid.iter());

        // prefix sum
        let offsets = files
            .iter()
            .scan(0, |acc, file| {
                let start = *acc;
                // TODO: this assumes the files already exist and are the correct size
                *acc += file.metadata().unwrap().len() as usize;
                Some(start)
            })
            .collect::<Vec<_>>();

        SharedFile {
            torrent,
            files,
            offsets,
            verified,

            verified_cnt: 0,
            pending_ops: queue,
            written: 0,
        }
    }

    // pub fn flush_blocks(&mut self) -> anyhow::Result<()> {
    //     while let Some((piece_idx, data)) = self.pending_ops.blocking_recv() {
    //         let already_has = self.verified.get(piece_idx as usize);
    //         if already_has.is_none() {
    //             error!("a pending write has a piece out of bounds");
    //         }
    //         if *already_has.unwrap() {
    //             continue;
    //         }
    //
    //         if !self.torrent.valid_piece(piece_idx, &data) {
    //             warn!("a piece was received that failed to verify");
    //             continue;
    //         }
    //
    //         self.write_piece(piece_idx, data)?;
    //     }
    //
    //     Ok(())
    // }

    fn all_verified(&self) -> bool {
        self.verified.iter().all(|f| *f)
    }

    pub fn verified(&self, piece: u32) -> bool {
        self.verified[piece as usize]
    }

    pub fn verified_cnt(&self) -> usize {
        self.verified_cnt
    }

    fn files_responsible(&self, piece: u32) -> Range<usize> {
        // TODO: last piece has a different size
        assert!(piece as usize != self.torrent.pieces.len() - 1);

        let piece_start = (piece * self.torrent.piece_size) as usize;
        let piece_end = piece_start + self.torrent.piece_size as usize;

        let first = self
            .offsets
            .partition_point(|&off| off <= piece_start)
            .saturating_sub(1);
        let last = self.offsets.partition_point(|&off| off < piece_end);

        first..last
    }

    fn write_piece(&mut self, piece: u32, complete_piece: Box<[u8]>) -> anyhow::Result<()> {
        let segments = self.file_segments(piece);
        let mut written = 0;
        for (file, range) in segments {
            let size = range.end - range.start;
            file.write_all_at(&complete_piece[written..size], range.start.try_into().unwrap())?;
            written += size;
        }

        self.written += written;
        Ok(())
    }

    /// Find the file(s) and their corresponding range that this piece should be written to
    fn file_segments(&self, piece_idx: u32) -> Vec<(&File, Range<usize>)> {
        let mut ret = vec![];

        // [piece_begin, piece_end) is where the data should go if all the files were to be
        // concatenated
        let piece_start = (piece_idx * self.torrent.piece_size) as usize;
        let piece_end = piece_start + self.torrent.piece_size as usize;

        let responsible_files = self.files_responsible(piece_idx);

        for f in responsible_files {
            // find out, conceptually, where does file `f` lie in one giant file
            let f_range_start = self.offsets[f];
            let f_range_end = if f + 1 < self.offsets.len() {
                self.offsets[f + 1]
            } else {
                self.torrent.total_size.try_into().unwrap()
            };

            // the overlapped region between total file range and the range of the piece is where
            // we can safety write to
            let overlap_start = piece_start.max(f_range_start);
            let overlap_end = piece_end.min(f_range_end);

            if overlap_start < overlap_end {
                // when actually writing to the files, the offsets are obviously with respect to
                // the actual file itself rather than the one giant conceptual file, convert them
                // back
                let local_off_begin = overlap_start - f_range_start;
                let local_off_end = overlap_end - f_range_start;

                ret.push((&self.files[f], local_off_begin..local_off_end));
            }
        }

        ret
    }

    pub fn verify_hash(&mut self, piece: u32) {
        let mut buf = vec![0u8; self.torrent.piece_size as usize];
        let mut wrote = 0;

        let segments = self.file_segments(piece);
        for (file, range) in segments {
            let local_off = range.start;
            let local_len = range.end - range.start;

            if wrote + local_len > buf.len() {
                panic!("wrote more than piece length");
            }

            let dst_slice = &mut buf[wrote..wrote + local_len];
            file.read_exact_at(dst_slice, local_off as u64).unwrap();

            wrote += local_len;
        }

        let valid_piece = self.torrent.valid_piece(piece, &buf);

        if valid_piece {
            self.verified.set(piece as usize, true);
            self.verified_cnt += 1;
        } else {
            warn!("hash failed");
        }
    }

    fn remaining_bytes(&self) -> usize {
        (self.torrent.total_size - (self.written as u64)) as usize
    }
}
