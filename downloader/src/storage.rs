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

    /// The byte range of `piece` in the conceptual single file all the torrent's files form.
    fn piece_range(&self, piece: u32) -> anyhow::Result<Range<usize>> {
        let piece_size = self
            .torrent
            .nth_piece_size(piece)
            .ok_or_else(|| anyhow!("piece index out of range"))?;
        let start = piece as usize * self.torrent.piece_size as usize;
        Ok(start..start + piece_size)
    }

    pub fn write_piece(&self, piece: u32, complete_piece: Box<[u8]>) -> anyhow::Result<()> {
        let mut written = 0;
        for (file, range) in self.file_segments(self.piece_range(piece)?) {
            let size = range.end - range.start;
            file.write_all_at(
                &complete_piece[written..written + size],
                range.start.try_into().unwrap(),
            )?;
            written += size;
        }

        Ok(())
    }

    pub fn read_piece(&self, piece: u32) -> anyhow::Result<Box<[u8]>> {
        self.read_range(self.piece_range(piece)?)
    }

    /// One block of a piece, as a peer would request it: `begin` and `length` are relative to
    /// the piece. Reads only those bytes. A request reaching past the end of the piece is an
    /// error rather than being clamped -- BEP 3 has no notion of a short block on the wire.
    pub fn read_block(&self, piece: u32, begin: u32, length: u32) -> anyhow::Result<Box<[u8]>> {
        let piece_range = self.piece_range(piece)?;
        let start = piece_range.start + begin as usize;
        let end = start + length as usize;
        if end > piece_range.end {
            anyhow::bail!("block {begin}+{length} runs past the end of piece {piece}");
        }
        self.read_range(start..end)
    }

    fn read_range(&self, range: Range<usize>) -> anyhow::Result<Box<[u8]>> {
        let mut buf = vec![0u8; range.len()];
        let mut read = 0;
        for (file, interval) in self.file_segments(range) {
            let len = interval.len();
            file.read_exact_at(&mut buf[read..read + len], interval.start as u64)?;
            read += len;
        }
        Ok(buf.into_boxed_slice())
    }

    /// Maps a byte range of the conceptual single file onto the actual files it spans, with
    /// each file's part expressed as a range within that file.
    fn file_segments(&self, range: Range<usize>) -> Vec<(&File, Range<usize>)> {
        let first = self
            .offsets
            .partition_point(|&off| off <= range.start)
            .saturating_sub(1);
        let last = self.offsets.partition_point(|&off| off < range.end);

        let mut ret = vec![];
        for f in first..last {
            let f_start = self.offsets[f];
            let f_end = if f + 1 < self.offsets.len() {
                self.offsets[f + 1]
            } else {
                self.torrent.total_size.try_into().unwrap()
            };

            let overlap_start = range.start.max(f_start);
            let overlap_end = range.end.min(f_end);
            if overlap_start < overlap_end {
                ret.push((&self.files[f], overlap_start - f_start..overlap_end - f_start));
            }
        }
        ret
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::metadata::build_torrent_file;
    use crate::torrent::parse_torrent;

    /// A three-file torrent (sizes 10, 25, 15 = 50 bytes, 16-byte pieces) whose files hold
    /// the bytes 0..50 in order, so any range read must come back as that arithmetic sequence.
    fn storage(name: &str) -> TorrentStorage {
        let sizes = [10usize, 25, 15];
        let total: usize = sizes.iter().sum();
        let content: Vec<u8> = (0..total as u8).collect();
        let pieces: Vec<u8> = content
            .chunks(16)
            .flat_map(|c| <sha1::Sha1 as sha1::Digest>::digest(c).to_vec())
            .collect();

        let mut info = b"d5:filesl".to_vec();
        for (i, size) in sizes.iter().enumerate() {
            info.extend_from_slice(
                format!("d6:lengthi{size}e4:pathl{}:f{i}.binee", format!("f{i}.bin").len()).as_bytes(),
            );
        }
        info.extend_from_slice(format!("e4:name7:storage12:piece lengthi16e6:pieces{}:", pieces.len()).as_bytes());
        info.extend_from_slice(&pieces);
        info.push(b'e');
        let mut torrent = parse_torrent(&build_torrent_file(&info, &["wss://unused.test".to_string()])).unwrap();

        let dir = std::env::temp_dir().join(format!("downloader-storage-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut files = vec![];
        let mut offset = 0;
        for (i, (size, path)) in torrent.files.iter_mut().enumerate() {
            *path = dir.join(format!("f{i}.bin"));
            std::fs::write(&*path, &content[offset..offset + *size as usize]).unwrap();
            offset += *size as usize;
            files.push(File::options().read(true).write(true).open(&*path).unwrap());
        }
        TorrentStorage::new(Arc::new(torrent), files)
    }

    #[test]
    fn reads_blocks_across_file_boundaries() {
        let storage = storage("blocks");
        // piece 0 is bytes 0..16: 10 from the first file, 6 from the second
        assert_eq!(
            &*storage.read_block(0, 0, 16).unwrap(),
            &(0..16).collect::<Vec<u8>>()[..]
        );
        assert_eq!(&*storage.read_block(0, 8, 4).unwrap(), &[8, 9, 10, 11]);
        // piece 2 is bytes 32..48, entirely inside the second file until 35, then the third
        assert_eq!(&*storage.read_block(2, 2, 4).unwrap(), &[34, 35, 36, 37]);
        // the last piece is 2 bytes
        assert_eq!(&*storage.read_block(3, 0, 2).unwrap(), &[48, 49]);
        assert_eq!(&*storage.read_piece(3).unwrap(), &[48, 49]);
    }

    #[test]
    fn rejects_blocks_past_the_end_of_a_piece() {
        let storage = storage("bounds");
        assert!(storage.read_block(0, 8, 9).is_err());
        assert!(storage.read_block(3, 0, 3).is_err(), "the last piece is only 2 bytes");
        assert!(storage.read_block(4, 0, 1).is_err(), "there is no piece 4");
        assert!(
            storage.read_block(0, 16, 0).is_ok(),
            "an empty block at the very end is in range"
        );
    }
}
