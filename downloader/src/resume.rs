//! Resume files: enough to pick a download back up after the process exits.
//!
//! One file per torrent, `<info hash, hex>.resume`, holding the raw info dict, the tracker
//! list, the download root, and the verified-piece bitfield. The info dict is stored verbatim so a torrent that
//! came from a magnet link resumes without going back to the network for metadata.
//!
//! The library owns the format and does the reading and writing; where the files live, and
//! finding them again, is the caller's job. See [`ResumeData::write`] and
//! [`ResumeData::read`].
//!
//! Only the bitfield is trusted for progress. A bit is set only after the piece was written
//! and hash-verified, so the file is always a subset of what's on disk -- provided the target
//! files are still the size they were, which `BtClient::add_torrent_resumed` checks.

use crate::metadata::build_torrent_file;
use crate::torrent::{Torrent, parse_torrent};
use crate::torrent_swarm::TorrentSwarmStats;
use anyhow::{Context, bail};
use bitvec::prelude::*;
use juicy_bencode::{BencodeItemView, parse_bencode_dict};
use midwest_mainline::types::InfoHash;
use sha1::{Digest, Sha1};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const VERSION: i64 = 2;
pub const EXTENSION: &str = "resume";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeData {
    pub raw_info: Vec<u8>,
    pub trackers: Vec<String>,
    /// the directory the torrent's files live under, as given to `BtClient::add_torrent`
    pub root: PathBuf,
    /// same layout as `TorrentSwarmStats::verified` (piece 0 is the high bit of byte 0)
    pub verified: BitBox<u8, Msb0>,
    /// the user paused it; resuming the session leaves it paused rather than starting it
    pub paused: bool,
    /// indices into the torrent's files of the ones the user doesn't want
    pub skip: Vec<u32>,
    /// bytes uploaded over the torrent's whole life, for the seeding ratio
    pub uploaded: u64,
}

impl ResumeData {
    pub fn from_torrent(torrent: &Torrent, root: &Path, verified: &BitSlice<u8, Msb0>) -> Self {
        Self {
            raw_info: torrent.raw_info.clone(),
            trackers: torrent.all_trackers(),
            // stored absolute: a resume file is read back from whatever directory the process
            // happens to start in later, not necessarily where it was written
            root: std::path::absolute(root).unwrap_or_else(|_| root.to_path_buf()),
            verified: verified.to_bitvec().into_boxed_bitslice(),
            paused: false,
            skip: vec![],
            uploaded: 0,
        }
    }

    /// One flag per file from `skip`.
    pub fn selected(&self, files: usize) -> Vec<bool> {
        (0..files).map(|i| !self.skip.contains(&(i as u32))).collect()
    }

    pub fn info_hash(&self) -> InfoHash {
        InfoHash::from_bytes(Sha1::digest(&self.raw_info).as_slice())
    }

    /// The file name a resume file for this torrent is written under.
    pub fn file_name(info_hash: &InfoHash) -> String {
        let hex: String = info_hash.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
        format!("{hex}.{EXTENSION}")
    }

    pub fn to_torrent(&self) -> anyhow::Result<Torrent> {
        parse_torrent(&build_torrent_file(&self.raw_info, &self.trackers))
    }

    pub fn encode(&self) -> Vec<u8> {
        fn bstr(bytes: &[u8]) -> Vec<u8> {
            let mut out = format!("{}:", bytes.len()).into_bytes();
            out.extend_from_slice(bytes);
            out
        }

        // keys in ascending order, as bencode requires
        let mut out = vec![b'd'];
        out.extend_from_slice(&bstr(b"info"));
        out.extend_from_slice(&self.raw_info);
        if self.paused {
            out.extend_from_slice(&bstr(b"paused"));
            out.extend_from_slice(b"i1e");
        }
        out.extend_from_slice(&bstr(b"root"));
        out.extend_from_slice(&bstr(self.root.as_os_str().as_encoded_bytes()));
        if !self.skip.is_empty() {
            out.extend_from_slice(&bstr(b"skip"));
            out.push(b'l');
            for i in &self.skip {
                out.extend_from_slice(format!("i{i}e").as_bytes());
            }
            out.push(b'e');
        }
        out.extend_from_slice(&bstr(b"trackers"));
        out.push(b'l');
        for t in &self.trackers {
            out.extend_from_slice(&bstr(t.as_bytes()));
        }
        out.push(b'e');
        if self.uploaded > 0 {
            out.extend_from_slice(&bstr(b"uploaded"));
            out.extend_from_slice(format!("i{}e", self.uploaded).as_bytes());
        }
        out.extend_from_slice(&bstr(b"verified"));
        out.extend_from_slice(&bstr(self.verified.as_raw_slice()));
        out.extend_from_slice(&bstr(b"version"));
        out.extend_from_slice(format!("i{VERSION}e").as_bytes());
        out.push(b'e');
        out
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let (_, mut dict) = parse_bencode_dict(bytes).map_err(|e| anyhow::anyhow!("not a bencoded dict: {e:?}"))?;

        match dict.remove(b"version".as_slice()) {
            Some(BencodeItemView::Integer(VERSION)) => {}
            other => bail!("unsupported resume file version {other:?}"),
        }

        let Some(BencodeItemView::List(trackers)) = dict.remove(b"trackers".as_slice()) else {
            bail!("missing 'trackers' list");
        };
        let trackers = trackers
            .into_iter()
            .map(|t| match t {
                BencodeItemView::ByteString(t) => Ok(String::from_utf8(t.to_vec())?),
                _ => bail!("tracker must be a string"),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let Some(BencodeItemView::ByteString(verified)) = dict.remove(b"verified".as_slice()) else {
            bail!("missing 'verified' bitfield");
        };
        let Some(BencodeItemView::ByteString(root)) = dict.remove(b"root".as_slice()) else {
            bail!("missing 'root' path");
        };
        let root = PathBuf::from(String::from_utf8(root.to_vec()).context("'root' is not utf-8")?);
        let paused = matches!(dict.remove(b"paused".as_slice()), Some(BencodeItemView::Integer(1)));
        let uploaded = match dict.remove(b"uploaded".as_slice()) {
            Some(BencodeItemView::Integer(n)) => u64::try_from(n).unwrap_or(0),
            _ => 0,
        };
        let skip: Vec<u32> = match dict.remove(b"skip".as_slice()) {
            Some(BencodeItemView::List(items)) => items
                .into_iter()
                .filter_map(|i| match i {
                    BencodeItemView::Integer(i) => u32::try_from(i).ok(),
                    _ => None,
                })
                .collect(),
            _ => vec![],
        };

        let raw_info = raw_info_bytes(bytes)?;

        let torrent = parse_torrent(&build_torrent_file(&raw_info, &trackers))
            .context("resume file's info dict didn't parse as a torrent")?;
        let pieces = torrent.pieces.len();
        if verified.len() != pieces.div_ceil(8) {
            bail!(
                "verified bitfield is {} bytes, expected {} for {pieces} pieces",
                verified.len(),
                pieces.div_ceil(8)
            );
        }
        let mut verified: BitVec<u8, Msb0> = BitVec::from_slice(verified);
        if verified[pieces..].any() {
            bail!("verified bitfield has bits set past the last piece");
        }
        verified.truncate(pieces);

        Ok(Self {
            raw_info,
            trackers,
            root,
            verified: verified.into_boxed_bitslice(),
            paused,
            skip,
            uploaded,
        })
    }

    /// Atomically writes this to `path`: a crash mid-write leaves the previous file intact
    /// rather than a truncated one that would parse as "nothing verified".
    pub fn write(&self, path: &Path) -> anyhow::Result<()> {
        let tmp = path.with_extension(format!("{EXTENSION}.tmp"));
        std::fs::write(&tmp, self.encode()).with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, path).with_context(|| format!("renaming into {}", path.display()))
    }

    /// Reads and validates a resume file. The file name is not consulted: the info hash comes
    /// from the info dict inside, so a renamed file still resumes the right torrent.
    pub fn read(path: &Path) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        Self::decode(&bytes).with_context(|| format!("parsing {}", path.display()))
    }
}

/// The file indices a selection leaves out, as the resume file stores them.
pub fn skipped(selected: &[bool]) -> Vec<u32> {
    selected
        .iter()
        .enumerate()
        .filter(|(_, s)| !**s)
        .map(|(i, _)| i as u32)
        .collect()
}

/// Keeps `dir/<info hash>.resume` up to date with `stats` until `shutdown` fires, then writes
/// it one last time. Meant to be spawned alongside `BtClient::work`.
///
/// The first write happens immediately, before any piece is verified: for a magnet-sourced
/// torrent that's what makes the metadata survive a restart. Writes are then rate-limited to
/// one per `MIN_INTERVAL`, since `stats` changes on every verified piece.
pub async fn keep_saving(
    torrent: Arc<Torrent>,
    root: PathBuf,
    mut stats: watch::Receiver<TorrentSwarmStats>,
    selected: watch::Receiver<Vec<bool>>,
    uploaded_before: u64,
    dir: PathBuf,
    shutdown: CancellationToken,
) {
    const MIN_INTERVAL: Duration = Duration::from_secs(5);

    let path = dir.join(ResumeData::file_name(&torrent.info_hash));
    let save = |stats: &watch::Receiver<TorrentSwarmStats>| {
        let stats = stats.borrow();
        let mut data = ResumeData::from_torrent(&torrent, &root, &stats.verified);
        data.skip = skipped(&selected.borrow());
        data.uploaded = uploaded_before + stats.uploaded;
        let written = std::fs::create_dir_all(&dir)
            .map_err(anyhow::Error::from)
            .and_then(|()| data.write(&path));
        if let Err(e) = written {
            tracing::warn!("couldn't write resume file {}: {e:#}", path.display());
        }
    };

    save(&stats);
    loop {
        tokio::select! {
            changed = stats.changed() => {
                if changed.is_err() {
                    break;
                }
                // debounce: wait for the interval, absorbing further changes meanwhile, but
                // still stop promptly on shutdown
                tokio::select! {
                    _ = tokio::time::sleep(MIN_INTERVAL) => {}
                    _ = shutdown.cancelled() => break,
                }
                stats.mark_unchanged();
                save(&stats);
            }
            _ = shutdown.cancelled() => break,
        }
    }
    save(&stats);
}

/// What a front end needs to list a resume file without loading the whole thing into a client.
#[derive(Debug, Clone, PartialEq)]
pub struct ResumeSummary {
    pub path: PathBuf,
    pub name: String,
    pub root: PathBuf,
    pub info_hash: InfoHash,
    pub verified_pieces: usize,
    pub total_pieces: usize,
    pub total_size: u64,
    pub paused: bool,
}

impl ResumeSummary {
    pub fn read(path: &Path) -> anyhow::Result<Self> {
        let data = ResumeData::read(path)?;
        let torrent = data.to_torrent()?;
        Ok(Self {
            path: path.to_path_buf(),
            name: torrent.name.clone(),
            root: data.root,
            info_hash: torrent.info_hash,
            verified_pieces: data.verified.count_ones(),
            total_pieces: data.verified.len(),
            paused: data.paused,
            total_size: torrent.total_size,
        })
    }

    pub fn fraction(&self) -> f32 {
        if self.total_pieces == 0 {
            return 0.0;
        }
        self.verified_pieces as f32 / self.total_pieces as f32
    }
}

/// Every `*.resume` in `dir` that parses, sorted by name. Unparseable files are skipped with a
/// warning rather than failing the listing, since one bad file shouldn't hide the rest.
pub fn list_resume_files(dir: &Path) -> Vec<ResumeSummary> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return vec![];
    };
    let mut found: Vec<ResumeSummary> = entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == EXTENSION))
        .filter_map(|p| match ResumeSummary::read(&p) {
            Ok(summary) => Some(summary),
            Err(e) => {
                tracing::warn!("skipping {}: {e:#}", p.display());
                None
            }
        })
        .collect();
    found.sort_by(|a, b| a.name.cmp(&b.name));
    found
}

/// The bencoded bytes of the top-level "info" value, verbatim, so the info hash recomputed
/// from them is the one the file was written for. `parse_bencode_dict` only hands back a parsed
/// view, so this walks the outer dict with bendy, which can return the raw span.
fn raw_info_bytes(input: &[u8]) -> anyhow::Result<Vec<u8>> {
    use bendy::decoding::{Decoder, Object};
    let mut decoder = Decoder::new(input);
    let Some(Object::Dict(mut dict)) = decoder.next_object().map_err(|e| anyhow::anyhow!("{e}"))? else {
        bail!("not a bencoded dict");
    };
    while let Some((key, val)) = dict.next_pair().map_err(|e| anyhow::anyhow!("{e}"))? {
        if key == b"info" {
            let Object::Dict(info) = val else {
                bail!("'info' must be a dict");
            };
            return Ok(info.into_raw().map_err(|e| anyhow::anyhow!("{e}"))?.to_vec());
        }
    }
    bail!("missing 'info' dict")
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::BtClient;
    use crate::defs::Identity;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn bstr(bytes: &[u8]) -> Vec<u8> {
        let mut out = format!("{}:", bytes.len()).into_bytes();
        out.extend_from_slice(bytes);
        out
    }

    /// A single-file torrent whose last piece is short, with the piece hashes actually
    /// matching `content` so the on-disk checks below have something real to compare.
    fn test_torrent(content: &[u8], piece_length: usize, trackers: &[&str]) -> Torrent {
        let pieces: Vec<u8> = content
            .chunks(piece_length)
            .flat_map(|c| Sha1::digest(c).to_vec())
            .collect();
        let mut info = vec![b'd'];
        info.extend_from_slice(&bstr(b"length"));
        info.extend_from_slice(format!("i{}e", content.len()).as_bytes());
        info.extend_from_slice(&bstr(b"name"));
        info.extend_from_slice(&bstr(b"resume-test.bin"));
        info.extend_from_slice(&bstr(b"piece length"));
        info.extend_from_slice(format!("i{piece_length}e").as_bytes());
        info.extend_from_slice(&bstr(b"pieces"));
        info.extend_from_slice(&bstr(&pieces));
        info.push(b'e');
        let trackers: Vec<String> = trackers.iter().map(|t| t.to_string()).collect();
        parse_torrent(&build_torrent_file(&info, &trackers)).unwrap()
    }

    fn content(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 7 % 251) as u8).collect()
    }

    fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("downloader-resume-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn identity() -> Identity {
        Identity {
            peer_id: *b"-DL0100-resume-test.",
            serving: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into(),
            dht: false,
        }
    }

    #[test]
    fn round_trips_through_bencode() {
        // 5 pieces: 4 full plus a short one, so the bitfield has padding bits
        let torrent = test_torrent(
            &content(4 * 16 + 5),
            16,
            &["udp://a.test:1/announce", "http://b.test/announce"],
        );
        let mut verified = bitvec![u8, Msb0; 0; 5];
        verified.set(0, true);
        verified.set(4, true);

        let data = ResumeData::from_torrent(&torrent, Path::new("/downloads"), &verified);
        let decoded = ResumeData::decode(&data.encode()).unwrap();

        assert_eq!(decoded, data);
        assert_eq!(decoded.info_hash(), torrent.info_hash);
        assert_eq!(decoded.trackers, torrent.all_trackers());
        assert_eq!(decoded.root, Path::new("/downloads"));
        let relative = ResumeData::from_torrent(&torrent, Path::new("music/here"), &verified);
        assert!(
            relative.root.is_absolute(),
            "a relative root is stored resolved: {:?}",
            relative.root
        );
        assert!(relative.root.ends_with("music/here"));
        assert_eq!(decoded.verified.len(), 5);
        assert_eq!(decoded.verified.count_ones(), 2);
        assert_eq!(decoded.to_torrent().unwrap(), torrent);
        assert!(!decoded.paused, "the flag is absent unless set");

        let paused = ResumeData { paused: true, ..data };
        assert!(ResumeData::decode(&paused.encode()).unwrap().paused);

        let skipping = ResumeData {
            skip: vec![0, 2],
            uploaded: 123_456,
            ..paused
        };
        let decoded = ResumeData::decode(&skipping.encode()).unwrap();
        assert_eq!(decoded.skip, vec![0, 2]);
        assert_eq!(decoded.selected(4), vec![false, true, false, true]);
        assert_eq!(decoded.uploaded, 123_456);
    }

    #[test]
    fn rejects_a_bitfield_of_the_wrong_length() {
        let torrent = test_torrent(&content(100), 16, &["udp://a.test:1"]);
        let mut data = ResumeData::from_torrent(&torrent, Path::new("/downloads"), &bitvec![u8, Msb0; 0; 7]);
        // 7 pieces fit in one byte; hand it two
        data.verified = bitvec![u8, Msb0; 0; 16].into_boxed_bitslice();
        let err = ResumeData::decode(&data.encode()).unwrap_err();
        assert!(err.to_string().contains("expected 1"), "{err:#}");
    }

    #[test]
    fn rejects_bits_set_past_the_last_piece() {
        let torrent = test_torrent(&content(100), 16, &["udp://a.test:1"]);
        let mut padded = bitvec![u8, Msb0; 0; 8];
        padded.set(7, true); // 7 pieces, so bit 7 is padding
        let mut data = ResumeData::from_torrent(&torrent, Path::new("/downloads"), &bitvec![u8, Msb0; 0; 7]);
        data.verified = padded.into_boxed_bitslice();
        assert!(ResumeData::decode(&data.encode()).is_err());
    }

    #[test]
    fn rejects_a_truncated_file() {
        let torrent = test_torrent(&content(100), 16, &["udp://a.test:1"]);
        let bytes = ResumeData::from_torrent(&torrent, Path::new("/downloads"), &bitvec![u8, Msb0; 1; 7]).encode();
        for cut in [1, bytes.len() / 2, bytes.len() - 1] {
            assert!(
                ResumeData::decode(&bytes[..cut]).is_err(),
                "accepted a file cut at {cut}"
            );
        }
    }

    #[test]
    fn write_then_read_and_list() {
        let dir = scratch_dir("list");
        let torrent = test_torrent(&content(100), 16, &["udp://a.test:1"]);
        let mut verified = bitvec![u8, Msb0; 0; 7];
        verified.set(3, true);
        let data = ResumeData::from_torrent(&torrent, Path::new("/downloads"), &verified);
        let path = dir.join(ResumeData::file_name(&torrent.info_hash));
        data.write(&path).unwrap();
        assert!(path.file_name().unwrap().to_str().unwrap().ends_with(".resume"));
        assert!(!dir.join("x.resume.tmp").exists());

        assert_eq!(ResumeData::read(&path).unwrap(), data);

        // junk alongside it is skipped, not fatal
        std::fs::write(dir.join("junk.resume"), b"not bencode").unwrap();
        std::fs::write(dir.join("unrelated.txt"), b"ignored").unwrap();

        let listed = list_resume_files(&dir);
        assert_eq!(listed.len(), 1);
        let summary = &listed[0];
        assert_eq!(summary.path, path);
        assert_eq!(summary.name, "resume-test.bin");
        assert_eq!(summary.root, Path::new("/downloads"));
        assert_eq!(summary.info_hash, torrent.info_hash);
        assert_eq!((summary.verified_pieces, summary.total_pieces), (1, 7));
        assert_eq!(summary.total_size, 100);

        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The test that matters: a fresh client lays the file down, some pieces get "downloaded",
    /// the swarm's bitfield is saved, and a second client rebuilt from that file must (a) not
    /// touch the bytes already there and (b) report the right amount left.
    #[tokio::test]
    async fn resumed_client_keeps_the_bytes_and_counts_them() {
        let dir = scratch_dir("client");
        let bytes = content(4 * 16 + 5);
        let torrent = test_torrent(&bytes, 16, &["udp://a.test:1"]);
        assert_eq!(
            torrent.files[0].1,
            Path::new("resume-test.bin"),
            "paths are relative to the root"
        );

        let fresh = BtClient::new(identity(), crate::dht::Dht::none());
        fresh.add_torrent(torrent.clone(), &dir).unwrap();
        assert_eq!(fresh.stats(&torrent).unwrap().borrow().left, bytes.len());
        drop(fresh);

        // pieces 1 and 4 (the short one) arrive
        let mut on_disk = vec![0u8; bytes.len()];
        on_disk[16..32].copy_from_slice(&bytes[16..32]);
        on_disk[64..].copy_from_slice(&bytes[64..]);
        std::fs::write(dir.join("resume-test.bin"), &on_disk).unwrap();
        let mut verified = bitvec![u8, Msb0; 0; 5];
        verified.set(1, true);
        verified.set(4, true);

        let path = dir.join(ResumeData::file_name(&torrent.info_hash));
        ResumeData::from_torrent(&torrent, &dir, &verified)
            .write(&path)
            .unwrap();

        let data = ResumeData::read(&path).unwrap();
        let rebuilt = data.to_torrent().unwrap();
        assert_eq!(data.root, dir);
        let resumed = BtClient::new(identity(), crate::dht::Dht::none());
        resumed
            .add_torrent_resumed(rebuilt.clone(), &data.root, data.verified)
            .unwrap();

        let stats = resumed.stats(&rebuilt).unwrap().borrow().clone();
        assert_eq!(stats.written, 16 + 5);
        assert_eq!(stats.left, bytes.len() - 21);
        assert!(!stats.completed);
        assert_eq!(stats.verified.count_ones(), 2);
        assert_eq!(
            std::fs::read(dir.join("resume-test.bin")).unwrap(),
            on_disk,
            "resume truncated the file"
        );

        // fully verified resumes as complete, and stays that way
        let done = BtClient::new(identity(), crate::dht::Dht::none());
        done.add_torrent_resumed(rebuilt.clone(), &dir, bitvec![u8, Msb0; 1; 5].into_boxed_bitslice())
            .unwrap();
        let stats = done.stats(&rebuilt).unwrap().borrow().clone();
        assert!(stats.completed);
        assert_eq!(stats.left, 0);

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn resume_refuses_missing_or_wrong_sized_files() {
        let dir = scratch_dir("refuse");
        let torrent = test_torrent(&content(100), 16, &["udp://a.test:1"]);
        let some = bitvec![u8, Msb0; 0; 7].into_boxed_bitslice();

        let client = BtClient::new(identity(), crate::dht::Dht::none());
        let err = client
            .add_torrent_resumed(torrent.clone(), &dir, some.clone())
            .unwrap_err();
        assert!(err.to_string().contains("opening"), "{err:#}");
        assert!(!dir.join("resume-test.bin").exists(), "resume must not create the file");

        std::fs::write(dir.join("resume-test.bin"), b"short").unwrap();
        let client = BtClient::new(identity(), crate::dht::Dht::none());
        let err = client
            .add_torrent_resumed(torrent.clone(), &dir, some.clone())
            .unwrap_err();
        assert!(err.to_string().contains("5 bytes on disk"), "{err:#}");
        assert_eq!(std::fs::read(dir.join("resume-test.bin")).unwrap(), b"short");

        let client = BtClient::new(identity(), crate::dht::Dht::none());
        let err = client
            .add_torrent_resumed(torrent, &dir, bitvec![u8, Msb0; 0; 8].into_boxed_bitslice())
            .unwrap_err();
        assert!(err.to_string().contains("covers 8 pieces"), "{err:#}");

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn keep_saving_writes_first_and_last() {
        let dir = scratch_dir("saver");
        let torrent = Arc::new(test_torrent(&content(100), 16, &["udp://a.test:1"]));
        let stats = TorrentSwarmStats {
            uploaded: 0,
            downloaded: 0,
            wasted: 0,
            left: 100,
            written: 0,
            verified: bitvec![u8, Msb0; 0; 7].into_boxed_bitslice(),
            wanted: bitvec![u8, Msb0; 1; 7].into_boxed_bitslice(),
            completed: false,
        };
        let (tx, rx) = watch::channel(stats.clone());
        let (_selected_tx, selected_rx) = watch::channel(vec![true]);
        let shutdown = CancellationToken::new();
        let saver = tokio::spawn(keep_saving(
            torrent.clone(),
            dir.clone(),
            rx,
            selected_rx,
            0,
            dir.join("nested"),
            shutdown.clone(),
        ));

        let path = dir.join("nested").join(ResumeData::file_name(&torrent.info_hash));
        tokio::time::timeout(Duration::from_secs(2), async {
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("no initial write");
        assert_eq!(ResumeData::read(&path).unwrap().verified.count_ones(), 0);

        // a change right before shutdown, well inside the debounce window, still lands
        let mut later = stats.clone();
        later.verified.set(2, true);
        tx.send(later).unwrap();
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), saver)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ResumeData::read(&path).unwrap().verified.count_ones(), 1);

        std::fs::remove_dir_all(dir).unwrap();
    }
}
