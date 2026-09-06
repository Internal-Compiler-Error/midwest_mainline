//! Re-hashing a torrent's files on disk, for "force recheck": what a client does when the
//! resume data can't be trusted (a crash mid-write, files copied in from elsewhere).

use crate::torrent::Torrent;
use bitvec::prelude::*;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

/// Which pieces under `root` hash correctly. A file that's missing or too short fails every
/// piece touching it. `progress` gets the count of pieces checked so far, now and then.
pub fn check_files(torrent: &Torrent, root: &Path, mut progress: impl FnMut(usize)) -> BitBox<u8, Msb0> {
    // opened once, read-only; `None` for a file that isn't there
    let files: Vec<(u64, Option<File>)> = torrent
        .files
        .iter()
        .map(|(size, path)| (*size as u64, File::open(root.join(path)).ok()))
        .collect();
    let mut offsets = Vec::with_capacity(files.len());
    let mut total = 0u64;
    for (size, _) in &files {
        offsets.push(total);
        total += size;
    }

    let mut verified = bitvec![u8, Msb0; 0; torrent.pieces.len()].into_boxed_bitslice();
    let mut buf = vec![0u8; torrent.piece_size as usize];
    for piece in 0..torrent.pieces.len() {
        let len = torrent.nth_piece_size(piece as u32).unwrap();
        let start = piece as u64 * torrent.piece_size as u64;
        if read_at(&files, &offsets, start, &mut buf[..len]) {
            verified.set(piece, torrent.valid_piece(piece as u32, &buf[..len]));
        }
        if piece % 64 == 63 {
            progress(piece + 1);
        }
    }
    progress(torrent.pieces.len());
    verified
}

/// Fills `buf` from the torrent's files as one long stream starting at `start`; false if any
/// of it is missing.
fn read_at(files: &[(u64, Option<File>)], offsets: &[u64], start: u64, buf: &mut [u8]) -> bool {
    let end = start + buf.len() as u64;
    let mut filled = 0usize;
    for (i, (size, file)) in files.iter().enumerate() {
        let file_start = offsets[i];
        let file_end = file_start + size;
        if file_end <= start || file_start >= end {
            continue;
        }
        let Some(file) = file else { return false };
        let from = start.max(file_start);
        let to = end.min(file_end);
        let chunk = &mut buf[filled..filled + (to - from) as usize];
        if file.read_exact_at(chunk, from - file_start).is_err() {
            return false;
        }
        filled += chunk.len();
    }
    filled == buf.len()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::torrent::parse_torrent;
    use sha1::{Digest, Sha1};
    use std::path::PathBuf;

    /// Two files, 8-byte pieces: the second piece straddles the files.
    fn torrent() -> (Torrent, Vec<u8>, Vec<u8>) {
        let a: Vec<u8> = (0u8..12).collect();
        let b: Vec<u8> = (100u8..110).collect();
        let all: Vec<u8> = a.iter().chain(&b).copied().collect();
        let pieces: Vec<u8> = all.chunks(8).flat_map(|c| Sha1::digest(c).to_vec()).collect();
        let mut info = format!(
            "d5:filesld6:lengthi12e4:pathl1:aeed6:lengthi10e4:pathl1:beee4:name5:check12:piece lengthi8e6:pieces{}:",
            pieces.len()
        )
        .into_bytes();
        info.extend_from_slice(&pieces);
        info.push(b'e');
        let file = crate::metadata::build_torrent_file(&info, &["http://x/ann".to_string()]);
        (parse_torrent(&file).unwrap(), a, b)
    }

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("downloader-check-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("check")).unwrap();
        dir
    }

    #[test]
    fn good_bad_and_missing_pieces_are_told_apart() {
        let (torrent, a, mut b) = torrent();
        let dir = scratch("mixed");
        // piece 2 lives entirely in b: corrupt it; pieces 0 and 1 are fine
        b[9] ^= 0xff;
        std::fs::write(dir.join("check/a"), &a).unwrap();
        std::fs::write(dir.join("check/b"), &b).unwrap();
        let mut reported = vec![];
        let verified = check_files(&torrent, &dir, |n| reported.push(n));
        assert_eq!(verified.iter().by_vals().collect::<Vec<_>>(), [true, true, false]);
        assert_eq!(reported.last(), Some(&3));

        // b gone: the straddling piece 1 and piece 2 fail, piece 0 still passes
        std::fs::remove_file(dir.join("check/b")).unwrap();
        let verified = check_files(&torrent, &dir, |_| {});
        assert_eq!(verified.iter().by_vals().collect::<Vec<_>>(), [true, false, false]);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
