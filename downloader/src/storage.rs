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
    /// one per entry of the torrent's files; `None` for padding and symlinks, which have no
    /// bytes on disk: padding reads as zeros and is never written
    files: Vec<Option<File>>,
}

impl TorrentStorage {
    pub fn new(torrent: Arc<Torrent>, files: Vec<Option<File>>) -> TorrentStorage {
        debug_assert_eq!(files.len(), torrent.files.len());
        TorrentStorage { torrent, files }
    }

    /// The byte range of `piece` in the conceptual single file all the torrent's files form.
    fn piece_range(&self, piece: u32) -> anyhow::Result<Range<u64>> {
        let piece_size = self
            .torrent
            .nth_piece_size(piece)
            .ok_or_else(|| anyhow!("piece index out of range"))?;
        let start = piece as u64 * self.torrent.piece_size as u64;
        Ok(start..start + piece_size as u64)
    }

    /// Writes a whole piece; `data` must be exactly the piece's size.
    pub fn write_piece(&self, piece: u32, data: &[u8]) -> anyhow::Result<()> {
        let range = self.piece_range(piece)?;
        if data.len() as u64 != range.end - range.start {
            anyhow::bail!(
                "{} bytes for piece {piece}, which is {}",
                data.len(),
                range.end - range.start
            );
        }
        let mut at = 0;
        for (file, within) in self.torrent.file_segments(range) {
            let len = (within.end - within.start) as usize;
            if let Some(file) = &self.files[file] {
                file.write_all_at(&data[at..at + len], within.start)?;
            }
            at += len;
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
        let start = piece_range.start + begin as u64;
        let end = start + length as u64;
        if end > piece_range.end {
            anyhow::bail!("block {begin}+{length} runs past the end of piece {piece}");
        }
        self.read_range(start..end)
    }

    fn read_range(&self, range: Range<u64>) -> anyhow::Result<Box<[u8]>> {
        let mut buf = vec![0u8; (range.end - range.start) as usize];
        let mut at = 0;
        for (file, within) in self.torrent.file_segments(range) {
            let len = (within.end - within.start) as usize;
            if let Some(file) = &self.files[file] {
                file.read_exact_at(&mut buf[at..at + len], within.start)?;
            }
            at += len;
        }
        Ok(buf.into_boxed_slice())
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
        for (i, crate::TorrentFile { len: size, path, .. }) in torrent.files.iter_mut().enumerate() {
            *path = dir.join(format!("f{i}.bin"));
            std::fs::write(&*path, &content[offset..offset + *size as usize]).unwrap();
            offset += *size as usize;
            files.push(Some(File::options().read(true).write(true).open(&*path).unwrap()));
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
            storage.write_piece(3, &[0; 3]).is_err(),
            "more than the last piece holds"
        );
        assert!(storage.write_piece(0, &[0; 15]).is_err(), "less than a whole piece");
        assert!(
            storage.read_block(0, 16, 0).is_ok(),
            "an empty block at the very end is in range"
        );
    }

    #[test]
    fn writes_pieces_past_4_gib() {
        const MIB: u64 = 1 << 20;
        let sizes = [MIB, 5 << 30, MIB];
        let mut info = b"d5:filesl".to_vec();
        for (i, size) in sizes.iter().enumerate() {
            info.extend_from_slice(format!("d6:lengthi{size}e4:pathl6:f{i}.binee").as_bytes());
        }
        info.extend_from_slice(format!("e4:name3:big12:piece lengthi{}e6:pieces{}:", 4 * MIB, 1281 * 20).as_bytes());
        info.extend_from_slice(&[0; 1281 * 20]);
        info.push(b'e');
        let mut torrent = parse_torrent(&build_torrent_file(&info, &[])).unwrap();

        let dir = std::env::temp_dir().join(format!("downloader-storage-big-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut files = vec![];
        for (i, crate::TorrentFile { len: size, path, .. }) in torrent.files.iter_mut().enumerate() {
            *path = dir.join(format!("f{i}.bin"));
            let file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&*path)
                .unwrap();
            // sparse, so this costs no disk space
            file.set_len(*size).unwrap();
            files.push(Some(file));
        }
        let last = dir.join("f2.bin");
        let storage = TorrentStorage::new(Arc::new(torrent), files);

        // the last piece starts at 5 GiB: the final MiB of f1, then all of f2
        let piece: Vec<u8> = (0..2 * MIB).map(|i| (i / MIB) as u8 + 1).collect();
        storage.write_piece(1280, &piece).unwrap();
        assert_eq!(std::fs::read(&last).unwrap(), vec![2; MIB as usize]);
        assert_eq!(&*storage.read_block(1280, MIB as u32 - 1, 2).unwrap(), &[1, 2]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// BEP 47: `a` (10 bytes), 6 bytes of padding, `b` (10 bytes), 16-byte pieces. The
    /// padding has no file: it reads as zeros and writes go nowhere.
    #[test]
    fn padding_is_zeros_and_never_written() {
        let info = b"d5:filesld6:lengthi10e4:pathl1:aeed4:attr1:p6:lengthi6e4:pathl4:.pad1:6eed6:lengthi10e4:pathl1:beee4:name1:t12:piece lengthi16e6:pieces40:0123456789012345678901234567890123456789e";
        let mut torrent = parse_torrent(&build_torrent_file(info, &[])).unwrap();
        let dir = std::env::temp_dir().join(format!("downloader-storage-pad-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut files = vec![];
        for (i, crate::TorrentFile { len: size, path, .. }) in torrent.files.iter_mut().enumerate() {
            *path = dir.join(format!("f{i}"));
            if i == 1 {
                files.push(None);
                continue;
            }
            let file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&*path)
                .unwrap();
            file.set_len(*size).unwrap();
            files.push(Some(file));
        }
        let storage = TorrentStorage::new(Arc::new(torrent), files);
        storage.write_piece(0, &[7; 16]).unwrap();
        storage.write_piece(1, &[9; 10]).unwrap();
        assert_eq!(std::fs::read(dir.join("f0")).unwrap(), [7; 10]);
        assert!(!dir.join("f1").exists());
        assert_eq!(std::fs::read(dir.join("f2")).unwrap(), [9; 10]);
        assert_eq!(&*storage.read_block(0, 8, 8).unwrap(), &[7, 7, 0, 0, 0, 0, 0, 0]);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
