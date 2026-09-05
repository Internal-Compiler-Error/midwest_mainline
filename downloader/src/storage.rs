use crate::torrent::Torrent;
use anyhow::anyhow;
use std::fs::File;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

/// Manages file I/O operations for torrent pieces
#[derive(Debug)]
pub struct TorrentStorage {
    torrent: Arc<Torrent>,
    files: Vec<File>,

    /// for each file at `i`, offset[i] contains the offset of the file into the conceptual one
    /// giant file
    offsets: Vec<usize>,
}

impl TorrentStorage {
    pub fn new(torrent: Arc<Torrent>, files: Vec<File>) -> TorrentStorage {
        // prefix sum
        let offsets = files
            .iter()
            .scan(0, |acc, file| {
                let start = *acc;
                // relies on each file already being sized to its expected length (see
                // `BtClient::add_torrent`, which `set_len`s every file right after creating it)
                *acc += file.metadata().unwrap().len() as usize;
                Some(start)
            })
            .collect::<Vec<_>>();

        TorrentStorage {
            torrent,
            files,
            offsets,
        }
    }

    fn files_responsible(&self, piece: u32) -> Range<usize> {
        let piece_start = (piece as usize) * (self.torrent.piece_size as usize);
        let piece_size = self.torrent.nth_piece_size(piece).expect("piece index in range");
        let piece_end = piece_start + piece_size;

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
            file.write_all_at(&complete_piece[written..written + size], range.start.try_into().unwrap())?;
            written += size;
        }

        Ok(())
    }

    pub fn read_piece(&self, piece: u32) -> anyhow::Result<Box<[u8]>> {
        let piece_size = self.torrent.nth_piece_size(piece).ok_or_else(|| anyhow!("piece index out of range"))?;
        let mut buf = vec![0u8; piece_size];
        let mut read = 0;
        for (file, interval) in self.file_segments(piece) {
            let len = interval.len();
            let dst = &mut buf[read..read + len];
            // propagate rather than unwrap: this runs inside the swarm's event loop, so a
            // panic here takes the whole torrent down with it
            file.read_exact_at(dst, interval.start as u64)?;

            read += len;
        }

        Ok(buf.into_boxed_slice())
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
