//! A multi-torrent download session, with all the orchestration a UI would otherwise have to
//! do itself: owning a runtime, resolving sources (file or magnet), driving the client, saving
//! resume data, tracking transfer rates, and cleaning up on removal.
//!
//! The point is that a front end shouldn't need to know about tokio, channels, or the shape of
//! the client at all. It calls [`Session::add`] with whatever the user typed, calls
//! [`Session::torrents`] whenever it wants to draw, and renders the [`TorrentState`]s it gets
//! back.

use crate::defs::Identity;
use crate::resume::{ResumeData, keep_saving};
use crate::torrent::Torrent;
use crate::torrent_swarm::TorrentSwarmStats;
use crate::{BtClient, load_source};
use bitvec::prelude::*;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

/// Identifies a torrent within one session, from `add`/`resume` until `remove`.
pub type TorrentId = u64;

/// Everything a front end needs to render one torrent, with no channels or futures in sight.
#[derive(Debug, Clone, PartialEq)]
pub enum TorrentState {
    /// Resolving a source into a torrent. For a magnet this means announcing to its trackers
    /// and fetching metadata from a peer, which can take a while; `elapsed` is for showing
    /// that something is still happening.
    Resolving {
        source: String,
        elapsed: Duration,
    },
    Downloading(Progress),
    Failed {
        source: String,
        error: String,
    },
}

/// A flat snapshot of download progress, in the units a UI wants to display.
#[derive(Debug, Clone, PartialEq)]
pub struct Progress {
    pub name: String,
    /// the directory the files are under
    pub root: String,
    pub files: Vec<String>,
    pub total_size: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub left: u64,
    pub verified_pieces: usize,
    pub total_pieces: usize,
    pub completed: bool,
    pub download_bps: f64,
    pub upload_bps: f64,
}

impl Progress {
    /// Fraction of pieces verified, in 0.0..=1.0. Zero-piece torrents can't happen from a valid
    /// file, but clamping keeps this total for a UI that divides by it.
    pub fn fraction(&self) -> f32 {
        if self.total_pieces == 0 {
            return 0.0;
        }
        (self.verified_pieces as f32 / self.total_pieces as f32).clamp(0.0, 1.0)
    }
}

/// What a torrent's task publishes; `Session::torrents` turns this into a `TorrentState`.
#[derive(Clone)]
enum Phase {
    Resolving {
        started: Instant,
    },
    Downloading {
        torrent: Arc<Torrent>,
        root: PathBuf,
        stats: watch::Receiver<TorrentSwarmStats>,
    },
    Failed {
        error: String,
    },
}

/// The session's side of one torrent; the task that actually runs it holds the other side.
struct Entry {
    source: String,
    phase: watch::Receiver<Phase>,
    /// tells the task to take the torrent out of the client and delete everything it wrote
    remove: Option<oneshot::Sender<()>>,
    rates: Rates,
}

pub struct Session {
    /// `Option` only so `shutdown` can take it and stop it with a bounded timeout; a plain drop
    /// waits on in-flight tasks indefinitely, which is how a UI ends up unquittable.
    rt: Option<Runtime>,
    handle: Handle,
    identity: Arc<Identity>,
    client: BtClient,
    /// every torrent's task is a child of this; cancelling it stops them all, files kept
    shutdown: CancellationToken,
    torrents: BTreeMap<TorrentId, Entry>,
    next_id: TorrentId,
    /// where resume files are written; none means progress isn't persisted
    resume_dir: Option<PathBuf>,
}

impl Session {
    /// Creates a session with its own tokio runtime, listening on `port` for inbound peers.
    pub fn new(peer_id: [u8; 20], port: u16) -> anyhow::Result<Self> {
        let rt = Runtime::new()?;
        let identity = Identity {
            peer_id,
            serving: std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, port).into(),
        };
        let shutdown = CancellationToken::new();
        // the client starts its listener on whatever runtime is current
        let client = {
            let _on_runtime = rt.enter();
            BtClient::new_with_shutdown(identity, shutdown.clone())
        };
        Ok(Self {
            handle: rt.handle().clone(),
            rt: Some(rt),
            identity: Arc::new(identity),
            client,
            shutdown,
            torrents: BTreeMap::new(),
            next_id: 1,
            resume_dir: None,
        })
    }

    /// Persist progress to `dir/<info hash>.resume` for every torrent from now on. Finding
    /// those files again and handing them to [`Session::resume`] is the caller's job; see
    /// `list_resume_files`.
    pub fn set_resume_dir(&mut self, dir: impl Into<PathBuf>) {
        self.resume_dir = Some(dir.into());
    }

    /// Starts downloading `source`, which may be a path to a `.torrent` or a magnet URI, into
    /// `root` (see `BtClient::add_torrent` for the layout under it). Returns immediately;
    /// watch [`Session::torrents`] for what happens next. Adding a torrent that's already in
    /// the session shows up as a failed entry, not a second copy.
    pub fn add(&mut self, source: impl Into<String>, root: impl Into<PathBuf>) -> TorrentId {
        let source = source.into();
        let root = root.into();
        let identity = self.identity.clone();
        self.launch(source.clone(), |cancel| async move {
            let torrent = load_source(&source, identity, cancel).await?;
            let nothing = bitvec![u8, Msb0; 0; torrent.pieces.len()].into_boxed_bitslice();
            Ok((torrent, root, nothing, false))
        })
    }

    /// Picks a download back up from a resume file (see `ResumeData`), in the root it was
    /// started in.
    pub fn resume(&mut self, path: impl AsRef<Path>) -> TorrentId {
        let path = path.as_ref().to_path_buf();
        self.launch(path.display().to_string(), |_cancel| async move {
            let data = ResumeData::read(&path)?;
            Ok((data.to_torrent()?, data.root, data.verified, true))
        })
    }

    /// Removes a torrent: its connections close, and its resume file and every file it
    /// downloaded are deleted. The entry is gone from [`Session::torrents`] immediately; the
    /// deletion itself finishes in the background. Unknown ids are ignored.
    pub fn remove(&mut self, id: TorrentId) {
        if let Some(mut entry) = self.torrents.remove(&id) {
            if let Some(remove) = entry.remove.take() {
                let _ = remove.send(());
            }
        }
    }

    /// Shared tail of `add`/`resume`: `resolve` produces the torrent, where its files go,
    /// which pieces are already had, and whether the target files are expected to exist. The
    /// task it spawns owns the torrent for the rest of its life, including its removal.
    fn launch<F, Fut>(&mut self, source: String, resolve: F) -> TorrentId
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<(Torrent, PathBuf, BitBox<u8, Msb0>, bool)>> + Send,
    {
        let id = self.next_id;
        self.next_id += 1;

        let cancel = self.shutdown.child_token();
        let (phase_tx, phase_rx) = watch::channel(Phase::Resolving {
            started: Instant::now(),
        });
        let (remove_tx, mut remove_rx) = oneshot::channel::<()>();
        self.torrents.insert(
            id,
            Entry {
                source,
                phase: phase_rx,
                remove: Some(remove_tx),
                rates: Rates::new(),
            },
        );

        let client = self.client.clone();
        let resume_dir = self.resume_dir.clone();
        self.handle.spawn(async move {
            let resolved = tokio::select! {
                resolved = resolve(cancel.clone()) => resolved,
                // removed while still resolving: nothing was written yet
                _ = &mut remove_rx => return,
            };
            let (torrent, root, verified, resumed) = match resolved {
                Ok(resolved) => resolved,
                Err(e) => {
                    let _ = phase_tx.send(Phase::Failed {
                        error: format!("{e:#}"),
                    });
                    return;
                }
            };

            let added = if resumed {
                client.add_torrent_resumed(torrent.clone(), &root, verified)
            } else {
                client.add_torrent(torrent.clone(), &root)
            };
            if let Err(e) = added {
                let _ = phase_tx.send(Phase::Failed {
                    error: format!("{e:#}"),
                });
                return;
            }
            let Some(stats) = client.stats(&torrent) else {
                let _ = phase_tx.send(Phase::Failed {
                    error: "torrent was added but reported no stats".to_string(),
                });
                return;
            };

            let torrent = Arc::new(torrent);
            let stop_saving = cancel.child_token();
            let saver = resume_dir.as_ref().map(|dir| {
                tokio::spawn(keep_saving(
                    torrent.clone(),
                    root.clone(),
                    stats.clone(),
                    dir.clone(),
                    stop_saving.clone(),
                ))
            });
            let _ = phase_tx.send(Phase::Downloading {
                torrent: torrent.clone(),
                root: root.clone(),
                stats,
            });

            let removed = tokio::select! {
                _ = cancel.cancelled() => false,
                _ = &mut remove_rx => true,
            };

            // either way the torrent leaves the client; the saver gets its final write in
            // before anything is deleted, or the deletion would race it
            client.remove_torrent(&torrent.info_hash);
            stop_saving.cancel();
            if let Some(saver) = saver {
                let _ = saver.await;
            }
            if removed {
                if let Some(dir) = resume_dir {
                    let _ = std::fs::remove_file(dir.join(ResumeData::file_name(&torrent.info_hash)));
                }
                let data = root.join(torrent.top_level());
                let deleted = if data.is_dir() {
                    std::fs::remove_dir_all(&data)
                } else {
                    std::fs::remove_file(&data)
                };
                if let Err(e) = deleted {
                    tracing::warn!("couldn't delete {}: {e}", data.display());
                }
            }
        });
        id
    }

    /// The current state of every torrent, ready to render. Cheap enough to call every frame.
    pub fn torrents(&mut self) -> Vec<(TorrentId, TorrentState)> {
        self.torrents
            .iter_mut()
            .map(|(id, entry)| (*id, entry.state()))
            .collect()
    }

    /// Stops everything, keeping files and resume data, and shuts the runtime down with a
    /// bounded wait so a slow or unreachable tracker can't stall process exit.
    pub fn shutdown(&mut self) {
        self.shutdown.cancel();
        self.torrents.clear();
        if let Some(rt) = self.rt.take() {
            rt.shutdown_timeout(Duration::from_millis(500));
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Entry {
    fn state(&mut self) -> TorrentState {
        match self.phase.borrow_and_update().clone() {
            Phase::Failed { error } => TorrentState::Failed {
                source: self.source.clone(),
                error,
            },
            Phase::Resolving { started } => TorrentState::Resolving {
                source: self.source.clone(),
                elapsed: started.elapsed(),
            },
            Phase::Downloading { torrent, root, stats } => {
                let stats = stats.borrow().clone();
                self.rates.update(&stats);
                TorrentState::Downloading(Progress {
                    name: torrent.name.clone(),
                    root: root.display().to_string(),
                    files: torrent.files.iter().map(|(_, p)| p.display().to_string()).collect(),
                    total_size: torrent.total_size,
                    downloaded: stats.downloaded,
                    uploaded: stats.uploaded,
                    left: stats.left as u64,
                    verified_pieces: stats.verified_cnt(),
                    total_pieces: stats.total_pieces(),
                    completed: stats.completed,
                    download_bps: self.rates.download_bps,
                    upload_bps: self.rates.upload_bps,
                })
            }
        }
    }
}

/// Rolling transfer-rate estimate.
struct Rates {
    last_sample: Instant,
    last_downloaded: u64,
    last_uploaded: u64,
    download_bps: f64,
    upload_bps: f64,
}

impl Rates {
    fn new() -> Self {
        Self {
            last_sample: Instant::now(),
            last_downloaded: 0,
            last_uploaded: 0,
            download_bps: 0.0,
            upload_bps: 0.0,
        }
    }

    /// Recomputes at most every 500ms. A UI calls this once per frame (~60/s), and a window
    /// that short would divide a handful of bytes by ~16ms and produce a wildly jumpy figure.
    fn update(&mut self, stats: &TorrentSwarmStats) {
        let elapsed = self.last_sample.elapsed();
        if elapsed < Duration::from_millis(500) {
            return;
        }
        self.download_bps = stats.downloaded.saturating_sub(self.last_downloaded) as f64 / elapsed.as_secs_f64();
        self.upload_bps = stats.uploaded.saturating_sub(self.last_uploaded) as f64 / elapsed.as_secs_f64();
        self.last_downloaded = stats.downloaded;
        self.last_uploaded = stats.uploaded;
        self.last_sample = Instant::now();
    }
}

/// Formats a byte count for display, e.g. `1.50 MiB`.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{size:.2} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn formats_byte_counts() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(999), "999 B");
        assert_eq!(human_bytes(1024), "1.00 KiB");
        assert_eq!(human_bytes(1536), "1.50 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.00 MiB");
    }

    #[test]
    fn fraction_is_clamped_and_total() {
        let mut p = Progress {
            name: String::new(),
            root: String::new(),
            files: vec![],
            total_size: 0,
            downloaded: 0,
            uploaded: 0,
            left: 0,
            verified_pieces: 0,
            total_pieces: 0,
            completed: false,
            download_bps: 0.0,
            upload_bps: 0.0,
        };
        // a zero-piece torrent must not divide by zero
        assert_eq!(p.fraction(), 0.0);

        p.total_pieces = 4;
        p.verified_pieces = 1;
        assert_eq!(p.fraction(), 0.25);

        p.verified_pieces = 4;
        assert_eq!(p.fraction(), 1.0);
    }

    /// Hand-builds a tiny single-file `.torrent` (no peers will ever have it, which is fine:
    /// these tests are about the session's bookkeeping, not transfer).
    fn write_torrent_file(dir: &Path) -> PathBuf {
        use sha1::Digest;
        let content = [7u8; 40];
        let pieces: Vec<u8> = content
            .chunks(16)
            .flat_map(|c| sha1::Sha1::digest(c).to_vec())
            .collect();
        let mut info = b"d6:lengthi40e4:name11:session.bin12:piece lengthi16e6:pieces60:".to_vec();
        info.extend_from_slice(&pieces);
        info.push(b'e');
        let file = crate::metadata::build_torrent_file(&info, &["wss://unused.test/announce".to_string()]);
        let path = dir.join("session.torrent");
        std::fs::write(&path, file).unwrap();
        path
    }

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("downloader-session-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Polls `torrents()` until `pred` holds for the entry with `id`, or gives up.
    fn wait_for(session: &mut Session, id: TorrentId, pred: impl Fn(Option<&TorrentState>) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let torrents = session.torrents();
            let state = torrents.iter().find(|(i, _)| *i == id).map(|(_, s)| s);
            if pred(state) {
                return;
            }
            assert!(Instant::now() < deadline, "timed out waiting; last state: {state:?}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn add_lays_files_down_and_remove_deletes_them_and_the_resume_file() {
        let dir = scratch("remove");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");
        let resume_dir = dir.join("resume");

        let mut session = Session::new(*b"-DL0100-session-tst.", 0).unwrap();
        session.set_resume_dir(&resume_dir);
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));

        let data = root.join("session.bin");
        assert_eq!(
            std::fs::metadata(&data).unwrap().len(),
            40,
            "target file is created and sized"
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while std::fs::read_dir(&resume_dir).map(|d| d.count()).unwrap_or(0) == 0 {
            assert!(Instant::now() < deadline, "resume file was never written");
            std::thread::sleep(Duration::from_millis(20));
        }

        // a second add of the same torrent is refused, not duplicated, and leaves the first alone
        let dup = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, dup, |s| matches!(s, Some(TorrentState::Failed { .. })));
        let Some((_, TorrentState::Failed { error, .. })) = session.torrents().into_iter().find(|(i, _)| *i == dup)
        else {
            panic!()
        };
        assert!(error.contains("already added"), "{error}");
        assert_eq!(std::fs::metadata(&data).unwrap().len(), 40);
        session.remove(dup);

        session.remove(id);
        wait_for(&mut session, id, |s| s.is_none());
        let deadline = Instant::now() + Duration::from_secs(5);
        while data.exists() || std::fs::read_dir(&resume_dir).map(|d| d.count()).unwrap_or(0) > 0 {
            assert!(
                Instant::now() < deadline,
                "data or resume file still there after remove"
            );
            std::thread::sleep(Duration::from_millis(20));
        }

        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn shutdown_keeps_files_and_resume_data() {
        let dir = scratch("shutdown");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");
        let resume_dir = dir.join("resume");

        let mut session = Session::new(*b"-DL0100-session-tst.", 0).unwrap();
        session.set_resume_dir(&resume_dir);
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.shutdown();

        assert!(root.join("session.bin").exists());
        assert_eq!(std::fs::read_dir(&resume_dir).unwrap().count(), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
