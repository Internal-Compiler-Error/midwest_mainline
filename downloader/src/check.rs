//! Re-hashing a torrent's files on disk, for "force recheck": what a client does when the
//! resume data can't be trusted (a crash mid-write, files copied in from elsewhere).

use crate::torrent::Torrent;
use bitvec::prelude::*;
use rayon::prelude::*;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Which pieces under `root` hash correctly. A file that's missing or too short fails every
/// piece touching it; padding reads as the zeros it stands for. `progress` gets the count of
/// pieces checked so far, now and then.
pub fn check_files(torrent: &Torrent, root: &Path, mut progress: impl FnMut(usize)) -> BitBox<u8, Msb0> {
    // opened once, read-only
    let files: Vec<Slot> = torrent
        .files
        .iter()
        .map(|file| match &file.attr {
            attr if attr.pad => Slot::Zeros,
            attr if attr.symlink.is_some() => Slot::Missing,
            _ => File::open(root.join(&file.path)).map_or(Slot::Missing, Slot::File),
        })
        .collect();

    // every core hashes, each with its own buffer; the caller's thread only reports progress,
    // so `progress` needn't be thread-safe
    let n = torrent.num_pieces();
    let checked = AtomicUsize::new(0);
    let good: Vec<bool> = std::thread::scope(|scope| {
        let work = scope.spawn(|| {
            (0..n)
                .into_par_iter()
                .map_init(
                    || vec![0u8; torrent.piece_size as usize],
                    |buf, piece| {
                        let len = torrent.nth_piece_size(piece as u32).unwrap();
                        let start = piece as u64 * torrent.piece_size as u64;
                        let ok = read_at(torrent, &files, start, &mut buf[..len])
                            && torrent.valid_piece(piece as u32, &buf[..len]);
                        checked.fetch_add(1, Ordering::Relaxed);
                        ok
                    },
                )
                .collect()
        });
        while !work.is_finished() {
            progress(checked.load(Ordering::Relaxed));
            std::thread::sleep(Duration::from_millis(100));
        }
        work.join().expect("hashing panicked")
    });
    progress(n);
    good.into_iter().collect::<BitVec<u8, Msb0>>().into_boxed_bitslice()
}

enum Slot {
    File(File),
    Zeros,
    Missing,
}

/// Fills `buf` from the torrent's files as one long stream starting at `start`; false if any
/// of it is missing.
fn read_at(torrent: &Torrent, files: &[Slot], start: u64, buf: &mut [u8]) -> bool {
    let mut filled = 0;
    for (file, within) in torrent.file_segments(start..start + buf.len() as u64) {
        let chunk = &mut buf[filled..filled + (within.end - within.start) as usize];
        match &files[file] {
            Slot::File(f) if f.read_exact_at(chunk, within.start).is_ok() => {}
            Slot::Zeros => chunk.fill(0),
            _ => return false,
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

    /// v2-only (Merkle trees, synthesised padding) and hybrid (SHA-1 over a stream with BEP 47
    /// padding files that aren't on disk) both recheck.
    #[test]
    fn rechecks_v2_and_hybrid_torrents() {
        use crate::torrent::fixtures::{self, sorted};
        const P: usize = 16384;
        let data = |len: usize, seed: usize| -> Vec<u8> { (0..len).map(|i| ((i * 5 + seed) % 241) as u8).collect() };
        let files = sorted(&[(&["x"][..], data(20_000, 1)), (&["y"][..], data(40_000, 2))]);
        for hybrid in [false, true] {
            let torrent = parse_torrent(&fixtures::torrent_file("t", &files, P, hybrid)).unwrap();
            assert_eq!(torrent.v2_only(), !hybrid);
            let dir = scratch(if hybrid { "hybrid" } else { "v2" });
            std::fs::create_dir_all(dir.join("t")).unwrap();
            std::fs::write(dir.join("t/x"), &files[0].1).unwrap();
            std::fs::write(dir.join("t/y"), &files[1].1).unwrap();
            assert!(check_files(&torrent, &dir, |_| {}).all(), "hybrid {hybrid}");

            // y's middle piece is wrong
            let mut y = files[1].1.clone();
            y[P + 5] ^= 1;
            std::fs::write(dir.join("t/y"), &y).unwrap();
            let verified = check_files(&torrent, &dir, |_| {});
            assert_eq!(
                verified.iter().by_vals().collect::<Vec<_>>(),
                [true, true, true, false, true]
            );
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
