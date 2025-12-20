use crate::torrent::Torrent;
use anyhow::anyhow;
use bitvec::prelude::*;
use std::fs::File;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::sync::{Arc, RwLock};

///// Storage statistics shared via watch channel
// #[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
// struct StorageStats {
//     pub remaining_bytes: usize,
//
//     /// which pieces have been verified, if a piece is verified, it also implies it has been written
//     // pub verified: BitBox<u8>,
//
//     written: usize,
// }


/// Manages file I/O operations for torrent pieces
#[derive(Debug)]
pub struct TorrentStorage {
    torrent: Arc<Torrent>,
    files: Vec<File>,

    /// for each file at `i`, offset[i] contains the offset of the file into the conceptual one
    /// giant file
    offsets: Vec<usize>,

    // stat: RwLock<StorageStats>,
}


impl TorrentStorage {
    pub fn new(
        torrent: Arc<Torrent>,
        files: Vec<File>,
    ) -> TorrentStorage {
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

        // let internal_state = StorageStats {
        //     remaining_bytes: 0,
        //     // verified,
        //     written: 0,
        // };

        TorrentStorage {
            torrent,
            files,
            offsets,
            // stat: RwLock::new(internal_state),
        }
    }

    // pub fn verified(&self) -> BitBox<u8> {
    //     self.stat.read().expect("lock poisoning is dumb").verified.clone()
    // }
    //
    // pub fn all_verified(&self) -> bool {
    //     self.stat.read().expect("lock poisoning is dumb").verified.iter().all(|v| *v)
    // }
    //
    // pub fn verified_cnt(&self) -> usize {
    //     self.stat.read().expect("lock poisoning is dumb").verified.count_ones()
    // }

    fn files_responsible(&self, piece: u32) -> Range<usize> {
        // TODO: last piece has a different size
        assert_ne!(piece as usize, self.torrent.pieces.len() - 1);

        let piece_start = (piece * self.torrent.piece_size) as usize;
        let piece_end = piece_start + self.torrent.piece_size as usize;

        let first = self
            .offsets
            .partition_point(|&off| off <= piece_start)
            .saturating_sub(1);
        let last = self.offsets.partition_point(|&off| off < piece_end);

        first..last
    }

    pub fn write_piece(&self, piece: u32, complete_piece: Box<[u8]>) -> anyhow::Result<()> {
        let segments = self.file_segments(piece);
        let mut written = 0;
        for (file, range) in segments {
            let size = range.end - range.start;
            file.write_all_at(&complete_piece[written..size], range.start.try_into().unwrap())?;
            written += size;
        }
        // if !self.verify_hash(piece) {
        //     panic!("Disk content is inconsistent");
        // }

        {
            // let mut stat = self.stat.write().expect("lock poisoning is dumb");
            // stat.written += written;
            // stat.remaining_bytes = (self.torrent.total_size - (stat.written as u64)) as usize

        }

        Ok(())
    }

    pub fn read_piece(&self, piece: u32) -> anyhow::Result<Box<[u8]>> {
        // let stat = self.stat.read().expect("lock poisoning is stupid");
        // let has_block = stat.verified.get(piece as usize);

        let mut buf = vec![0u8; self.torrent.piece_size as usize];
        let mut read = 0;
        for (file, interval) in self.file_segments(piece) {
            let len = interval.len();
            let dst = &mut buf[read..read + len];
            file.read_exact_at(dst, interval.start as u64).unwrap();

            read += len;
        }

        Ok(buf.into_boxed_slice())
        //
        // if has_block.is_none() {
        //     Err(anyhow!("Don't have piece"))
        // } else {
        //     let mut buf = vec![0u8; self.torrent.piece_size as usize];
        //     let mut read = 0;
        //     for (file, interval) in self.file_segments(piece) {
        //         let len = interval.len();
        //         let dst = &mut buf[read..read + len];
        //         file.read_exact_at(dst, interval.start as u64).unwrap();
        //
        //         read += len;
        //     }
        //
        //     Ok(buf.into_boxed_slice())
        // }
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

}
