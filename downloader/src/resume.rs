//! Resume files: enough to pick a download back up after the process exits.
//!
//! One file per torrent, `<info hash, hex>.resume`, holding the raw info dict, the tracker
//! list, the download root, and the verified-piece bitfield. The info dict is stored verbatim
//! so a torrent that came from a magnet link resumes without going back to the network for
//! metadata.
//!
//! The library owns the format and does the reading and writing; where the files live, and
//! finding them again, is the caller's job. See [`ResumeData::write`] and
//! [`ResumeData::read`].
//!
//! Only the bitfield is trusted for progress. A bit is set only after the piece was written
//! and hash-verified, so the file is always a subset of what's on disk -- provided the target
//! files are still the size they were, which `BtClient::add_torrent_resumed` checks.

use crate::feed::{Feed, FeedKey};
use crate::metadata::build_torrent_file_with;
use crate::torrent::{Torrent, parse_torrent, swarm_info_hash};
use crate::torrent_swarm::TorrentSwarmStats;
use anyhow::{Context, bail};
use bitvec::prelude::*;
use juicy_bencode::{BencodeItemView, parse_bencode_dict};
use midwest_mainline::types::InfoHash;
use std::collections::BTreeMap;
use std::io::Write;
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
    pub modes: Modes,
    /// BEP 19 web seeds; a magnet's `ws=` ones exist nowhere else
    pub web_seeds: Vec<String>,
    /// BEP 52 piece layers (a bencoded dict), which the info dict doesn't hold
    pub piece_layers: Option<Vec<u8>>,
    /// BEP 46: the DHT key the torrent updates through, and where in its history it is
    pub feed: Option<Feed>,
}

/// The per-torrent switches the user flips, which live in the resume file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Modes {
    /// pieces are fetched in order rather than rarest first
    pub sequential: bool,
    /// BEP 16: seeding shows peers a piece at a time
    pub super_seed: bool,
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
            modes: Modes::default(),
            skip: vec![],
            uploaded: 0,
            web_seeds: torrent.web_seeds.clone(),
            piece_layers: torrent.piece_layers_bencoded(),
            feed: None,
        }
    }

    /// One flag per file from `skip`.
    pub fn selected(&self, files: usize) -> Vec<bool> {
        (0..files).map(|i| !self.skip.contains(&(i as u32))).collect()
    }

    pub fn info_hash(&self) -> InfoHash {
        swarm_info_hash(&self.raw_info)
    }

    /// The file name a resume file for this torrent is written under.
    pub fn file_name(info_hash: &InfoHash) -> String {
        format!("{info_hash}.{EXTENSION}")
    }

    pub fn to_torrent(&self) -> anyhow::Result<Torrent> {
        let mut torrent = parse_torrent(&self.torrent_file())?;
        torrent.web_seeds = self.web_seeds.clone();
        Ok(torrent)
    }

    fn torrent_file(&self) -> Vec<u8> {
        build_torrent_file_with(&self.raw_info, &self.trackers, self.piece_layers.as_deref())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut dict = DictWriter::new();
        if let Some(feed) = &self.feed {
            let mut btpk = DictWriter::new();
            btpk.bytes(b"k", &feed.key.public);
            if !feed.key.salt.is_empty() {
                btpk.bytes(b"s", &feed.key.salt);
            }
            if let Some(seq) = feed.seq {
                btpk.int(b"seq", seq);
            }
            if let Some(seq) = feed.superseded {
                btpk.int(b"superseded", seq);
            }
            dict.raw(b"btpk", &btpk.finish());
        }
        dict.raw(b"info", &self.raw_info);
        dict.flag(b"paused", self.paused);
        if let Some(layers) = &self.piece_layers {
            dict.raw(b"piece layers", layers);
        }
        dict.bytes(b"root", self.root.as_os_str().as_encoded_bytes());
        dict.flag(b"sequential", self.modes.sequential);
        if !self.skip.is_empty() {
            dict.int_list(b"skip", self.skip.iter().map(|&i| i64::from(i)));
        }
        dict.flag(b"super seed", self.modes.super_seed);
        dict.bytes_list(b"trackers", self.trackers.iter().map(String::as_bytes));
        if self.uploaded > 0 {
            dict.int(b"uploaded", self.uploaded);
        }
        if !self.web_seeds.is_empty() {
            dict.bytes_list(b"url-list", self.web_seeds.iter().map(String::as_bytes));
        }
        dict.bytes(b"verified", self.verified.as_raw_slice());
        dict.int(b"version", VERSION);
        dict.finish()
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
        let mut flag = |key: &[u8]| matches!(dict.remove(key), Some(BencodeItemView::Integer(1)));
        let paused = flag(b"paused");
        let modes = Modes {
            sequential: flag(b"sequential"),
            super_seed: flag(b"super seed"),
        };
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

        let web_seeds = match dict.remove(b"url-list".as_slice()) {
            Some(BencodeItemView::List(urls)) => crate::torrent::web_seed_urls(urls.iter().filter_map(|u| match u {
                BencodeItemView::ByteString(u) => Some(*u),
                _ => None,
            })),
            _ => vec![],
        };

        let feed = match dict.remove(b"btpk".as_slice()) {
            Some(BencodeItemView::Dictionary(feed)) => Some(decode_feed(&feed)?),
            Some(_) => bail!("'btpk' must be a dict"),
            None => None,
        };

        let raw_info = raw_dict(bytes, b"info")?.context("missing 'info' dict")?;
        let piece_layers = raw_dict(bytes, b"piece layers")?;

        let torrent = parse_torrent(&build_torrent_file_with(&raw_info, &trackers, piece_layers.as_deref()))
            .context("resume file's info dict didn't parse as a torrent")?;
        let pieces = torrent.num_pieces();
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
            modes,
            web_seeds,
            piece_layers,
            feed,
        })
    }

    /// Atomically and durably writes this to `path`: a crash mid-write leaves the previous file
    /// intact rather than a truncated one that would parse as "nothing verified", and once this
    /// returns the new file survives power loss. It says nothing about the torrent's data; see
    /// [`save_durably`] for writing a bitfield whose pieces may not have reached the disk yet.
    pub fn write(&self, path: &Path) -> anyhow::Result<()> {
        let tmp = path.with_extension(format!("{EXTENSION}.tmp"));
        replace_file(path, &tmp, &self.encode()).with_context(|| format!("writing {}", path.display()))
    }

    /// Reads and validates a resume file. The file name is not consulted: the info hash comes
    /// from the info dict inside, so a renamed file still resumes the right torrent.
    pub fn read(path: &Path) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        Self::decode(&bytes).with_context(|| format!("parsing {}", path.display()))
    }
}

/// Writes a bencoded dict. Keys must come in ascending order, as bencode requires.
struct DictWriter {
    out: Vec<u8>,
    last_key: Vec<u8>,
}

impl DictWriter {
    fn new() -> Self {
        Self {
            out: vec![b'd'],
            last_key: vec![],
        }
    }

    fn key(&mut self, key: &[u8]) {
        debug_assert!(
            self.out.len() == 1 || key > self.last_key.as_slice(),
            "keys out of order"
        );
        self.last_key = key.to_vec();
        self.string(key);
    }

    fn string(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
        self.out.extend_from_slice(bytes);
    }

    fn integer(&mut self, n: impl std::fmt::Display) {
        self.out.extend_from_slice(format!("i{n}e").as_bytes());
    }

    fn bytes(&mut self, key: &[u8], value: &[u8]) {
        self.key(key);
        self.string(value);
    }

    fn int(&mut self, key: &[u8], value: impl std::fmt::Display) {
        self.key(key);
        self.integer(value);
    }

    /// `1` when set; left out when not, which reads back as unset
    fn flag(&mut self, key: &[u8], on: bool) {
        if on {
            self.int(key, 1);
        }
    }

    /// A value that is bencoded already.
    fn raw(&mut self, key: &[u8], value: &[u8]) {
        self.key(key);
        self.out.extend_from_slice(value);
    }

    fn bytes_list<'a>(&mut self, key: &[u8], items: impl IntoIterator<Item = &'a [u8]>) {
        self.key(key);
        self.out.push(b'l');
        items.into_iter().for_each(|item| self.string(item));
        self.out.push(b'e');
    }

    fn int_list(&mut self, key: &[u8], items: impl IntoIterator<Item = i64>) {
        self.key(key);
        self.out.push(b'l');
        items.into_iter().for_each(|item| self.integer(item));
        self.out.push(b'e');
    }

    fn finish(mut self) -> Vec<u8> {
        self.out.push(b'e');
        self.out
    }
}

fn decode_feed(dict: &BTreeMap<&[u8], BencodeItemView>) -> anyhow::Result<Feed> {
    let Some(BencodeItemView::ByteString(public)) = dict.get(b"k".as_slice()) else {
        bail!("'btpk' has no key");
    };
    let salt = match dict.get(b"s".as_slice()) {
        Some(BencodeItemView::ByteString(salt)) => salt.to_vec(),
        _ => vec![],
    };
    let int = |key: &[u8]| match dict.get(key) {
        Some(BencodeItemView::Integer(n)) => Some(*n),
        _ => None,
    };
    Ok(Feed {
        key: FeedKey {
            public: (*public).try_into().context("'btpk' key must be 32 bytes")?,
            salt,
        },
        seq: int(b"seq"),
        superseded: int(b"superseded"),
    })
}

/// Replaces `path` with `bytes` by way of `tmp`, so that a crash leaves either the old file or
/// the new one, and the new one survives power loss once this returns. On macOS std's
/// `sync_all` is `F_FULLFSYNC`, which also flushes the drive's own cache; a plain `fsync`
/// there doesn't.
pub(crate) fn replace_file(path: &Path, tmp: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = std::fs::File::create(tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(tmp, path)?;
    // the rename itself lives in the directory; some file systems refuse to sync one, and the
    // file is complete either way
    if let Some(dir) = path.parent()
        && let Ok(dir) = std::fs::File::open(if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        })
    {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Flushes to disk the data of every file holding a byte of a piece in `pieces`. The files are
/// opened afresh, which is enough: a sync flushes the file, not just one descriptor's writes.
fn sync_pieces(torrent: &Torrent, root: &Path, pieces: &BitSlice<u8, Msb0>) -> std::io::Result<()> {
    for (index, (_, relative)) in torrent.files.iter().enumerate() {
        if torrent.attrs[index].virtual_file() {
            continue;
        }
        let range = torrent.pieces_of_file(index);
        if pieces[range.start as usize..range.end as usize].any() {
            std::fs::File::open(root.join(relative))?.sync_data()?;
        }
    }
    Ok(())
}

/// Writes `data` to `path` without ever claiming a piece that a power loss could take back:
/// pieces verified since `persisted` (the bitfield the file held so far) have their files
/// flushed first. If that fails they're left out this time, to be tried again with the next
/// write. Returns the bitfield the file now holds. Blocking.
pub fn save_durably(
    torrent: &Torrent,
    path: &Path,
    mut data: ResumeData,
    persisted: &BitSlice<u8, Msb0>,
) -> anyhow::Result<BitBox<u8, Msb0>> {
    let new: BitVec<u8, Msb0> = data
        .verified
        .iter()
        .by_vals()
        .enumerate()
        .map(|(piece, verified)| verified && !persisted.get(piece).is_some_and(|had| *had))
        .collect();
    if new.any()
        && let Err(e) = sync_pieces(torrent, &data.root, &new)
    {
        tracing::warn!(
            "couldn't flush {}'s data, its newest pieces aren't saved yet: {e}",
            torrent.name
        );
        for piece in new.iter_ones() {
            data.verified.set(piece, false);
        }
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    data.write(path)?;
    Ok(data.verified)
}

/// `save_durably` on the blocking pool. Returns the bitfield the file holds afterwards:
/// `persisted` still if the write failed, which is logged.
async fn save_in_background(
    torrent: &Arc<Torrent>,
    path: &Path,
    data: ResumeData,
    persisted: BitBox<u8, Msb0>,
) -> BitBox<u8, Msb0> {
    let written = {
        let (torrent, path, had) = (torrent.clone(), path.to_path_buf(), persisted.clone());
        tokio::task::spawn_blocking(move || save_durably(&torrent, &path, data, &had)).await
    };
    match written {
        Ok(Ok(now)) => return now,
        Ok(Err(e)) => tracing::warn!("couldn't write resume file {}: {e:#}", path.display()),
        Err(e) => tracing::warn!("writing resume file {} failed: {e}", path.display()),
    }
    persisted
}

/// The file indices a selection leaves out, as the resume file stores them.
fn skipped(selected: &[bool]) -> Vec<u32> {
    selected
        .iter()
        .enumerate()
        .filter(|(_, s)| !**s)
        .map(|(i, _)| i as u32)
        .collect()
}

/// Keeps `dir/<info hash>.resume` up to date with `stats` until `shutdown` fires, then writes
/// it one last time. Meant to be spawned alongside the torrent's swarm.
///
/// The first write happens immediately, before any piece is verified: for a magnet-sourced
/// torrent that's what makes the metadata survive a restart. After that a change to the
/// bitfield or the user's choices is written once things have been quiet for `SETTLE`, while
/// the upload counter, which moves all the time on a seeding torrent, only makes it into the
/// file every `COUNTERS`. The files are written off the async workers, and durably (see
/// `save_durably`). Returns the bitfield the file holds at the end.
pub(crate) async fn keep_saving(
    torrent: Arc<Torrent>,
    root: PathBuf,
    stats: watch::Receiver<TorrentSwarmStats>,
    inputs: ResumeInputs,
    dir: PathBuf,
    shutdown: CancellationToken,
) -> BitBox<u8, Msb0> {
    const SETTLE: Duration = Duration::from_secs(5);
    const COUNTERS: Duration = Duration::from_secs(60);
    save_every(torrent, root, stats, inputs, dir, shutdown, (SETTLE, COUNTERS)).await
}

/// Writes `dir/<info hash>.resume` once, marked paused, for a torrent stopped at `verified`.
/// Returns the bitfield the file holds afterwards.
pub(crate) async fn save_paused(
    torrent: &Arc<Torrent>,
    root: &Path,
    dir: &Path,
    inputs: ResumeInputs,
    verified: &BitSlice<u8, Msb0>,
) -> BitBox<u8, Msb0> {
    let mut data = inputs.snapshot(verified, 0).to_data(torrent, root);
    data.paused = true;
    let path = dir.join(ResumeData::file_name(&torrent.info_hash));
    save_in_background(torrent, &path, data, inputs.persisted).await
}

/// What a resume file holds that changes while the torrent runs.
#[derive(Clone, PartialEq)]
struct Saved {
    verified: BitBox<u8, Msb0>,
    skip: Vec<u32>,
    modes: Modes,
    uploaded: u64,
    feed: Option<Feed>,
}

impl Saved {
    /// Worth a write straight away, rather than only with the next round of counters.
    fn differs_beyond_counters(&self, other: &Saved) -> bool {
        (&self.verified, &self.skip, self.modes, &self.feed) != (&other.verified, &other.skip, other.modes, &other.feed)
    }

    fn to_data(&self, torrent: &Torrent, root: &Path) -> ResumeData {
        ResumeData {
            skip: self.skip.clone(),
            modes: self.modes,
            uploaded: self.uploaded,
            feed: self.feed.clone(),
            ..ResumeData::from_torrent(torrent, root, &self.verified)
        }
    }
}

async fn save_every(
    torrent: Arc<Torrent>,
    root: PathBuf,
    mut stats: watch::Receiver<TorrentSwarmStats>,
    mut inputs: ResumeInputs,
    dir: PathBuf,
    shutdown: CancellationToken,
    (settle, counters): (Duration, Duration),
) -> BitBox<u8, Msb0> {
    let path = dir.join(ResumeData::file_name(&torrent.info_hash));
    let snapshot = |stats: &watch::Receiver<TorrentSwarmStats>, inputs: &ResumeInputs| {
        let stats = stats.borrow();
        inputs.snapshot(&stats.verified, stats.uploaded)
    };
    let mut persisted = inputs.persisted.clone();
    let mut last = snapshot(&stats, &inputs);
    persisted = save_in_background(&torrent, &path, last.to_data(&torrent, &root), persisted).await;
    let mut last_write = tokio::time::Instant::now();
    // set while only the counters have changed since the last write
    let mut counters_due: Option<tokio::time::Instant> = None;
    loop {
        tokio::select! {
            changed = stats.changed() => if changed.is_err() { break },
            changed = inputs.selected.changed() => if changed.is_err() { break },
            changed = inputs.modes.changed() => if changed.is_err() { break },
            // a feed nobody can change any more is just one that stays put
            Ok(()) = inputs.feed.changed() => {}
            _ = tokio::time::sleep_until(counters_due.unwrap_or_else(tokio::time::Instant::now)),
                if counters_due.is_some() => {}
            _ = shutdown.cancelled() => break,
        }
        // let a burst of changes settle, but still stop promptly on shutdown
        tokio::select! {
            _ = tokio::time::sleep(settle) => {}
            _ = shutdown.cancelled() => break,
        }
        stats.mark_unchanged();
        inputs.selected.mark_unchanged();
        inputs.modes.mark_unchanged();
        inputs.feed.mark_unchanged();
        let now = snapshot(&stats, &inputs);
        if now.differs_beyond_counters(&last) || (now.uploaded != last.uploaded && last_write.elapsed() >= counters) {
            persisted = save_in_background(&torrent, &path, now.to_data(&torrent, &root), persisted).await;
            last = now;
            last_write = tokio::time::Instant::now();
            counters_due = None;
        } else if now.uploaded != last.uploaded {
            counters_due = Some(last_write + counters);
        }
    }
    let now = snapshot(&stats, &inputs);
    if now != last {
        persisted = save_in_background(&torrent, &path, now.to_data(&torrent, &root), persisted).await;
    }
    persisted
}

/// What goes into the resume file besides the torrent and its progress: the user's choices,
/// read live, the upload count from before this run, and the bitfield the file already holds.
pub(crate) struct ResumeInputs {
    pub selected: watch::Receiver<Vec<bool>>,
    pub modes: watch::Receiver<Modes>,
    pub uploaded_before: u64,
    /// pieces already claimed by the resume file on disk, so known to be on disk themselves;
    /// anything verified beyond these gets its data flushed before the file claims it
    pub persisted: BitBox<u8, Msb0>,
    /// BEP 46, see `ResumeData::feed`
    pub feed: watch::Receiver<Option<Feed>>,
}

impl ResumeInputs {
    /// The file's contents now, with `uploaded` this run's upload count.
    fn snapshot(&self, verified: &BitSlice<u8, Msb0>, uploaded: u64) -> Saved {
        Saved {
            verified: verified.to_bitvec().into_boxed_bitslice(),
            skip: skipped(&self.selected.borrow()),
            modes: *self.modes.borrow(),
            uploaded: self.uploaded_before + uploaded,
            feed: self.feed.borrow().clone(),
        }
    }
}
/// What a front end needs to list a resume file without loading the whole thing into a client.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ResumeSummary {
    #[serde(serialize_with = "display_path")]
    pub path: PathBuf,
    pub name: String,
    #[serde(serialize_with = "display_path")]
    pub root: PathBuf,
    #[serde(serialize_with = "display")]
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

/// Lossily where a path isn't UTF-8, rather than failing the whole listing.
fn display_path<S: serde::Serializer>(path: &Path, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(&path.display())
}

fn display<S: serde::Serializer>(value: &impl std::fmt::Display, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(value)
}

/// Every `*.resume` in `dir` that parses, sorted by name. Unparseable files are skipped with a
/// warning rather than failing the listing, since one bad file shouldn't hide the rest; see
/// `scan_resume_files` for those.
pub fn list_resume_files(dir: &Path) -> Vec<ResumeSummary> {
    let mut found: Vec<ResumeSummary> = scan_resume_files(dir)
        .into_iter()
        .filter_map(|(p, read)| match read {
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

/// Every `*.resume` in `dir`, with its summary or why it couldn't be read, sorted by path.
pub fn scan_resume_files(dir: &Path) -> Vec<(PathBuf, anyhow::Result<ResumeSummary>)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return vec![];
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == EXTENSION))
        .collect();
    paths.sort();
    paths
        .into_iter()
        .map(|p| {
            let read = ResumeSummary::read(&p);
            (p, read)
        })
        .collect()
}

/// The bencoded bytes of the top-level dict `key` ("info", "piece layers"), verbatim, so the
/// info hash recomputed from them is the one the file was written for. `parse_bencode_dict`
/// only hands back a parsed view, so this walks the outer dict with bendy, which can return the
/// raw span.
fn raw_dict(input: &[u8], key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
    use bendy::decoding::{Decoder, Object};
    let mut decoder = Decoder::new(input);
    let Some(Object::Dict(mut dict)) = decoder.next_object().map_err(|e| anyhow::anyhow!("{e}"))? else {
        bail!("not a bencoded dict");
    };
    while let Some((k, val)) = dict.next_pair().map_err(|e| anyhow::anyhow!("{e}"))? {
        if k == key {
            let Object::Dict(value) = val else {
                bail!("{:?} must be a dict", String::from_utf8_lossy(key));
            };
            return Ok(Some(value.into_raw().map_err(|e| anyhow::anyhow!("{e}"))?.to_vec()));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::BtClient;
    use crate::defs::Identity;
    use crate::metadata::build_torrent_file;
    use sha1::{Digest, Sha1};
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
            encryption: crate::config::Encryption::Disabled,
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

        let following = ResumeData {
            feed: Some(Feed {
                key: FeedKey {
                    public: [9; 32],
                    salt: b"salt".to_vec(),
                },
                seq: Some(41),
                superseded: Some(42),
            }),
            ..skipping.clone()
        };
        assert_eq!(ResumeData::decode(&following.encode()).unwrap(), following);
        let unsalted = ResumeData {
            feed: Some(Feed {
                key: FeedKey {
                    public: [9; 32],
                    salt: vec![],
                },
                seq: None,
                superseded: None,
            }),
            ..skipping.clone()
        };
        let encoded = unsalted.encode();
        assert!(midwest_mainline::dht::item::is_canonical_bencode(&encoded));
        assert_eq!(ResumeData::decode(&encoded).unwrap(), unsalted);

        let mut seeded = torrent.clone();
        seeded.web_seeds = vec!["https://m.test/pub/".to_string()];
        let data = ResumeData::from_torrent(&seeded, Path::new("/downloads"), &verified);
        let decoded = ResumeData::decode(&data.encode()).unwrap();
        assert_eq!(
            decoded.to_torrent().unwrap(),
            seeded,
            "a magnet's ws= seeds survive a restart"
        );
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

    fn no_progress() -> TorrentSwarmStats {
        TorrentSwarmStats {
            uploaded: 0,
            downloaded: 0,
            wasted: 0,
            left: 100,
            written: 0,
            verified: bitvec![u8, Msb0; 0; 7].into_boxed_bitslice(),
            wanted: bitvec![u8, Msb0; 1; 7].into_boxed_bitslice(),
            completed: false,
            storage_error: None,
        }
    }

    /// Waits for what's in the resume file at `path` to satisfy `pred`.
    async fn file_comes_to(path: &Path, pred: impl Fn(&ResumeData) -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ResumeData::read(path).is_ok_and(|data| pred(&data)) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the file never got there: {:?}", ResumeData::read(path).ok()));
    }

    /// The bitfield and the user's choices are written once they settle; the upload counter
    /// alone only every so often, or along with something that matters more.
    #[tokio::test]
    async fn counters_alone_are_written_lazily() {
        let dir = scratch_dir("lazy");
        let torrent = Arc::new(test_torrent(&content(100), 16, &["udp://a.test:1"]));
        std::fs::write(dir.join("resume-test.bin"), content(100)).unwrap();
        let (tx, rx) = watch::channel(no_progress());
        let (selected_tx, selected_rx) = watch::channel(vec![true]);
        let (_modes_tx, modes_rx) = watch::channel(Modes::default());
        let shutdown = CancellationToken::new();
        let saver = tokio::spawn(save_every(
            torrent.clone(),
            dir.clone(),
            rx,
            ResumeInputs {
                selected: selected_rx,
                modes: modes_rx,
                uploaded_before: 1000,
                persisted: bitvec![u8, Msb0; 0; 7].into_boxed_bitslice(),
                feed: watch::channel(None).1,
            },
            dir.clone(),
            shutdown.clone(),
            (Duration::from_millis(20), Duration::from_millis(500)),
        ));
        let path = dir.join(ResumeData::file_name(&torrent.info_hash));
        file_comes_to(&path, |data| data.uploaded == 1000).await;

        tx.send_modify(|stats| stats.uploaded = 10);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            ResumeData::read(&path).unwrap().uploaded,
            1000,
            "too soon for the counters"
        );

        tx.send_modify(|stats| stats.verified.set(1, true));
        file_comes_to(&path, |data| data.verified.count_ones() == 1).await;
        assert_eq!(ResumeData::read(&path).unwrap().uploaded, 1010, "they come along");

        tx.send_modify(|stats| stats.uploaded = 20);
        selected_tx.send(vec![false]).unwrap();
        file_comes_to(&path, |data| data.skip == [0]).await;
        tx.send_modify(|stats| stats.uploaded = 30);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(ResumeData::read(&path).unwrap().uploaded, 1020);
        // nothing changes from here on, and the counters still make it in time
        file_comes_to(&path, |data| data.uploaded == 1030).await;

        shutdown.cancel();
        assert_eq!(saver.await.unwrap().count_ones(), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A piece whose file can't be flushed isn't claimed, so a power cut can't leave the
    /// resume file promising data the disk never got; it's claimed once the flush works.
    #[test]
    fn unflushed_pieces_are_not_claimed() {
        let dir = scratch_dir("durable");
        let torrent = test_torrent(&content(100), 16, &["udp://a.test:1"]);
        let path = dir.join(ResumeData::file_name(&torrent.info_hash));
        let mut verified = bitvec![u8, Msb0; 0; 7];
        verified.set(2, true);
        let data = ResumeData::from_torrent(&torrent, &dir, &verified);
        let none = bitvec![u8, Msb0; 0; 7];

        // no data file at all to flush
        let written = save_durably(&torrent, &path, data.clone(), &none).unwrap();
        assert_eq!(written.count_ones(), 0);
        assert_eq!(ResumeData::read(&path).unwrap().verified.count_ones(), 0);
        // what the file already claimed needs no flush and stays claimed
        assert_eq!(
            save_durably(&torrent, &path, data.clone(), &verified).unwrap(),
            verified
        );

        std::fs::write(dir.join("resume-test.bin"), content(100)).unwrap();
        let written = save_durably(&torrent, &path, data, &none).unwrap();
        assert_eq!(written, verified);
        assert_eq!(ResumeData::read(&path).unwrap().verified, written);
        assert!(
            !dir.join(format!("{}.tmp", path.file_name().unwrap().display()))
                .exists()
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn keep_saving_writes_first_and_last() {
        let dir = scratch_dir("saver");
        let torrent = Arc::new(test_torrent(&content(100), 16, &["udp://a.test:1"]));
        std::fs::write(dir.join("resume-test.bin"), content(100)).unwrap();
        let stats = no_progress();
        let (tx, rx) = watch::channel(stats.clone());
        let (_selected_tx, selected_rx) = watch::channel(vec![true]);
        let (_modes_tx, modes_rx) = watch::channel(Modes::default());
        let shutdown = CancellationToken::new();
        let saver = tokio::spawn(keep_saving(
            torrent.clone(),
            dir.clone(),
            rx,
            ResumeInputs {
                selected: selected_rx,
                modes: modes_rx,
                uploaded_before: 0,
                persisted: bitvec![u8, Msb0; 0; 7].into_boxed_bitslice(),
                feed: watch::channel(None).1,
            },
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
        let persisted = tokio::time::timeout(Duration::from_secs(2), saver)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ResumeData::read(&path).unwrap().verified.count_ones(), 1);
        assert_eq!(persisted, ResumeData::read(&path).unwrap().verified);

        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A v2 torrent's piece layers aren't in the info dict, so the resume file keeps them --
    /// including ones that arrived from peers -- and it goes by the truncated v2 hash.
    #[test]
    fn keeps_a_v2_torrents_piece_layers() {
        use crate::torrent::fixtures::{self, sorted};
        const P: usize = 16384;
        let files = sorted(&[(&["big"][..], vec![5; 3 * P + 7]), (&["small"][..], vec![6; 10])]);
        let info = fixtures::info("v2", &files, P, false);
        let torrent = parse_torrent(&build_torrent_file(&info, &[])).unwrap();
        assert_eq!(torrent.missing_layers(), [0]);
        let verified = bitvec![u8, Msb0; 0; torrent.num_pieces()];

        let without = ResumeData::from_torrent(&torrent, Path::new("/downloads"), &verified);
        assert_eq!(without.piece_layers, None);
        assert_eq!(without.info_hash(), torrent.info_hash);
        assert_eq!(
            ResumeData::decode(&without.encode())
                .unwrap()
                .to_torrent()
                .unwrap()
                .missing_layers(),
            [0]
        );

        assert!(torrent.set_layer(0, fixtures::layer(&files[0].1, P)));
        let with = ResumeData::from_torrent(&torrent, Path::new("/downloads"), &verified);
        let decoded = ResumeData::decode(&with.encode()).unwrap();
        assert_eq!(decoded, with);
        let rebuilt = decoded.to_torrent().unwrap();
        assert!(rebuilt.missing_layers().is_empty());
        assert_eq!(rebuilt.info_hash, torrent.info_hash);
    }
}
