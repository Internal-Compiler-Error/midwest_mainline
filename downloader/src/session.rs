//! A multi-torrent download session, with all the orchestration a UI would otherwise have to
//! do itself: owning a runtime, resolving sources (file or magnet), driving the client, saving
//! resume data, tracking transfer rates, and cleaning up on removal.
//!
//! The point is that a front end shouldn't need to know about tokio, channels, or the shape of
//! the client at all. It calls [`Session::add`] with whatever the user typed, calls
//! [`Session::torrents`] whenever it wants to draw, and renders the [`TorrentState`]s it gets
//! back.

use crate::defs::Identity;
use crate::dht::Dht;
use crate::resume::{ResumeData, ResumeSummary, keep_saving, list_resume_files};
use crate::torrent::Torrent;
use crate::torrent_swarm::TorrentSwarmStats;
use crate::{BtClient, load_source};
use bitvec::prelude::*;
use midwest_mainline::types::InfoHash;
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{mpsc, watch};
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
    /// Stopped by the user: no connections, files and progress kept, ready to `unpause`
    Paused(Progress),
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
    /// bytes received that we already had: endgame races, and stray blocks
    pub wasted: u64,
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

/// What `launch` needs to start a torrent once its source has been resolved.
struct Resolved {
    torrent: Torrent,
    root: PathBuf,
    verified: BitBox<u8, Msb0>,
    /// the files are expected to exist already (checked, not created)
    resumed: bool,
    /// start paused rather than downloading
    paused: bool,
    /// peers already known for it, handed to the swarm right away
    peers: Vec<std::net::SocketAddr>,
}

/// The task that owns one torrent for its whole life in the session: resolves the source,
/// puts the torrent in the client and takes it out again on pause, unpause, remove, and
/// shutdown, and keeps the resume file current in between.
struct TorrentTask {
    client: BtClient,
    resume_dir: PathBuf,
    cancel: CancellationToken,
    phase: watch::Sender<Phase>,
    commands: mpsc::UnboundedReceiver<Command>,
}

/// What ended a running or paused stretch of a torrent's life.
enum Stop {
    Pause,
    Unpause,
    Remove { delete_files: bool },
    Shutdown,
}

impl TorrentTask {
    async fn run<F, Fut>(mut self, resolve: F)
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = anyhow::Result<Resolved>>,
    {
        let resolved = tokio::select! {
            resolved = resolve(self.cancel.clone()) => resolved,
            _ = self.cancel.cancelled() => return,
            // removed while still resolving: nothing was written yet
            _ = self.commands.recv() => return,
        };
        let Resolved {
            torrent,
            root,
            mut verified,
            mut resumed,
            mut paused,
            mut peers,
        } = match resolved {
            Ok(resolved) => resolved,
            Err(e) => {
                let _ = self.phase.send(Phase::Failed {
                    error: format!("{e:#}"),
                });
                return;
            }
        };
        let torrent = Arc::new(torrent);

        let stop = loop {
            let stop = if paused {
                self.wait_while_paused(&torrent, &root, &verified).await
            } else {
                self.run_until_stopped(&torrent, &root, &mut verified, &mut resumed, &mut peers)
                    .await
            };
            match stop {
                Ok(Stop::Pause) => paused = true,
                Ok(Stop::Unpause) => paused = false,
                Ok(stop) => break stop,
                Err(e) => {
                    let _ = self.phase.send(Phase::Failed {
                        error: format!("{e:#}"),
                    });
                    return;
                }
            }
        };

        if let Stop::Remove { delete_files } = stop {
            let _ = std::fs::remove_file(self.resume_dir.join(ResumeData::file_name(&torrent.info_hash)));
            if delete_files {
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
        }
    }

    /// Puts the torrent in the client and keeps its resume file current until something
    /// stops it, then takes it out again. `verified` is updated to what's on disk by then.
    async fn run_until_stopped(
        &mut self,
        torrent: &Arc<Torrent>,
        root: &Path,
        verified: &mut BitBox<u8, Msb0>,
        resumed: &mut bool,
        peers: &mut Vec<std::net::SocketAddr>,
    ) -> anyhow::Result<Stop> {
        if *resumed {
            self.client
                .add_torrent_resumed((**torrent).clone(), root, verified.clone())?;
        } else {
            self.client.add_torrent((**torrent).clone(), root)?;
        }
        // whatever happens next, the files exist
        *resumed = true;
        self.client.add_peers(&torrent.info_hash, std::mem::take(peers));
        let stats = self
            .client
            .stats(torrent)
            .ok_or_else(|| anyhow::anyhow!("torrent was added but reported no stats"))?;
        let stop_saving = self.cancel.child_token();
        let saver = tokio::spawn(keep_saving(
            torrent.clone(),
            root.to_path_buf(),
            stats.clone(),
            self.resume_dir.clone(),
            stop_saving.clone(),
        ));
        let _ = self.phase.send(Phase::Downloading {
            torrent: torrent.clone(),
            root: root.to_path_buf(),
            stats: stats.clone(),
        });

        let stop = loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break Stop::Shutdown,
                command = self.commands.recv() => match command {
                    Some(Command::Pause) => break Stop::Pause,
                    Some(Command::Remove { delete_files }) => break Stop::Remove { delete_files },
                    Some(Command::Unpause) => {}
                    None => break Stop::Shutdown,
                },
            }
        };
        // the torrent leaves the client; the saver gets its final write in before anything
        // is deleted, or the deletion would race it
        self.client.remove_torrent(&torrent.info_hash);
        stop_saving.cancel();
        let _ = saver.await;
        *verified = stats.borrow().verified.clone();
        if let Stop::Pause = stop {
            let _ = self.phase.send(Phase::Paused {
                torrent: torrent.clone(),
                root: root.to_path_buf(),
                stats: stats.borrow().clone(),
            });
        }
        Ok(stop)
    }

    /// Marks the resume file paused, so a restart brings the torrent back paused, and waits
    /// to be unpaused or removed.
    async fn wait_while_paused(
        &mut self,
        torrent: &Arc<Torrent>,
        root: &Path,
        verified: &BitBox<u8, Msb0>,
    ) -> anyhow::Result<Stop> {
        let _ = self.phase.send(Phase::Paused {
            torrent: torrent.clone(),
            root: root.to_path_buf(),
            stats: TorrentSwarmStats::for_verified(torrent, verified.clone()),
        });
        let mut data = ResumeData::from_torrent(torrent, root, verified);
        data.paused = true;
        if let Err(e) = data.write(&self.resume_dir.join(ResumeData::file_name(&torrent.info_hash))) {
            tracing::warn!("couldn't mark {} paused in its resume file: {e:#}", torrent.name);
        }
        Ok(loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break Stop::Shutdown,
                command = self.commands.recv() => match command {
                    Some(Command::Unpause) => break Stop::Unpause,
                    Some(Command::Remove { delete_files }) => break Stop::Remove { delete_files },
                    Some(Command::Pause) => {}
                    None => break Stop::Shutdown,
                },
            }
        })
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
    Paused {
        torrent: Arc<Torrent>,
        root: PathBuf,
        /// the last stats before the swarm was stopped
        stats: TorrentSwarmStats,
    },
    Failed {
        error: String,
    },
}

/// What a front end can do to a torrent once it's running; the torrent's task carries it out.
enum Command {
    Pause,
    Unpause,
    Remove { delete_files: bool },
}

/// The session's side of one torrent; the task that actually runs it holds the other side.
struct Entry {
    source: String,
    /// known up front for a resume file or a magnet, so `resumable` can leave running
    /// torrents out; a `.torrent` file's is only known once it has been parsed
    info_hash: Option<InfoHash>,
    phase: watch::Receiver<Phase>,
    commands: mpsc::UnboundedSender<Command>,
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
    /// where resume files are written, `<data dir>/resume`
    resume_dir: PathBuf,
    /// the DHT node, kept so it lives as long as the session; none if it was turned off
    _dht: Option<Dht>,
}

/// What a session needs to start.
pub struct SessionConfig {
    pub peer_id: [u8; 20],
    /// TCP port for inbound peers, and the DHT node's UDP port
    pub port: u16,
    /// where the session keeps its own files: resume data and the DHT database. See
    /// `paths::data_dir` for the usual answer.
    pub data_dir: PathBuf,
    /// whether to run a DHT node; off means peers come from trackers and PEX only
    pub dht: bool,
}

impl Session {
    /// Creates a session with its own tokio runtime. Progress goes to `<data dir>/resume/<info
    /// hash>.resume` for every torrent; finding those files again and handing them to
    /// [`Session::resume`] is the caller's job, see `Session::resume_dir` and
    /// `list_resume_files`.
    pub fn new(config: SessionConfig) -> anyhow::Result<Self> {
        let resume_dir = config.data_dir.join("resume");
        std::fs::create_dir_all(&resume_dir)?;
        let rt = Runtime::new()?;
        let identity = Identity {
            peer_id: config.peer_id,
            serving: std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, config.port).into(),
        };
        let shutdown = CancellationToken::new();
        // the client and the DHT node start on whatever runtime is current
        let (client, dht) = {
            let _on_runtime = rt.enter();
            let dht = config
                .dht
                .then(|| Dht::start(config.data_dir.join("dht.db"), config.port));
            let watch = dht.as_ref().map_or_else(Dht::none, Dht::watch);
            (BtClient::new_with_shutdown(identity, shutdown.clone(), watch), dht)
        };
        Ok(Self {
            handle: rt.handle().clone(),
            rt: Some(rt),
            identity: Arc::new(identity),
            client,
            shutdown,
            torrents: BTreeMap::new(),
            next_id: 1,
            resume_dir,
            _dht: dht,
        })
    }

    /// Where this session writes resume files.
    pub fn resume_dir(&self) -> &Path {
        &self.resume_dir
    }

    /// Starts downloading `source`, which may be a path to a `.torrent` or a magnet URI, into
    /// `root` (see `BtClient::add_torrent` for the layout under it). Returns immediately;
    /// watch [`Session::torrents`] for what happens next. Adding a torrent that's already in
    /// the session shows up as a failed entry, not a second copy.
    pub fn add(&mut self, source: impl Into<String>, root: impl Into<PathBuf>) -> TorrentId {
        let source = source.into();
        let root = root.into();
        let identity = self.identity.clone();
        let dht = self.client.dht();
        let info_hash = crate::magnet::parse_magnet(&source).ok().map(|m| m.info_hash);
        self.launch(source.clone(), info_hash, |cancel| async move {
            let loaded = load_source(&source, identity, cancel, dht).await?;
            let nothing = bitvec![u8, Msb0; 0; loaded.torrent.pieces.len()].into_boxed_bitslice();
            Ok(Resolved {
                torrent: loaded.torrent,
                root,
                verified: nothing,
                resumed: false,
                paused: false,
                peers: loaded.peers,
            })
        })
    }

    /// Picks a download back up from a resume file (see `ResumeData`), in the root it was
    /// started in. A torrent that was paused when its file was last written comes back paused.
    pub fn resume(&mut self, path: impl AsRef<Path>) -> TorrentId {
        let path = path.as_ref().to_path_buf();
        let info_hash = ResumeSummary::read(&path).ok().map(|s| s.info_hash);
        self.launch(path.display().to_string(), info_hash, |_cancel| async move {
            let data = ResumeData::read(&path)?;
            Ok(Resolved {
                torrent: data.to_torrent()?,
                root: data.root,
                verified: data.verified,
                resumed: true,
                paused: data.paused,
                peers: vec![],
            })
        })
    }

    /// Resumes every torrent that has a resume file in this session's resume dir and isn't
    /// already in the session. What a client does at startup.
    pub fn resume_all(&mut self) -> Vec<TorrentId> {
        let files = self.resumable();
        files.into_iter().map(|f| self.resume(f.path)).collect()
    }

    /// The resume files in this session's resume dir for torrents it isn't running.
    pub fn resumable(&self) -> Vec<ResumeSummary> {
        let running: HashSet<InfoHash> = self.torrents.values().filter_map(Entry::info_hash).collect();
        list_resume_files(&self.resume_dir)
            .into_iter()
            .filter(|f| !running.contains(&f.info_hash))
            .collect()
    }

    /// Stops a torrent's connections and announces, keeping its files and progress. Only a
    /// torrent that's downloading or seeding can be paused; anything else is left alone.
    pub fn pause(&mut self, id: TorrentId) {
        self.command(id, Command::Pause);
    }

    pub fn unpause(&mut self, id: TorrentId) {
        self.command(id, Command::Unpause);
    }

    /// Removes a torrent: its connections close and its resume file is deleted, and with
    /// `delete_files` so is everything it downloaded. The entry is gone from
    /// [`Session::torrents`] immediately; the deletion itself finishes in the background.
    /// Unknown ids are ignored.
    pub fn remove(&mut self, id: TorrentId, delete_files: bool) {
        if let Some(entry) = self.torrents.remove(&id) {
            let _ = entry.commands.send(Command::Remove { delete_files });
        }
    }

    fn command(&mut self, id: TorrentId, command: Command) {
        if let Some(entry) = self.torrents.get(&id) {
            let _ = entry.commands.send(command);
        }
    }

    /// Shared tail of `add`/`resume`: `resolve` produces the torrent, where its files go,
    /// which pieces are already had, and whether the target files are expected to exist. The
    /// task it spawns owns the torrent for the rest of its life, including its removal.
    fn launch<F, Fut>(&mut self, source: String, info_hash: Option<InfoHash>, resolve: F) -> TorrentId
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<Resolved>> + Send + 'static,
    {
        let id = self.next_id;
        self.next_id += 1;

        let cancel = self.shutdown.child_token();
        let (phase_tx, phase_rx) = watch::channel(Phase::Resolving {
            started: Instant::now(),
        });
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        self.torrents.insert(
            id,
            Entry {
                source,
                info_hash,
                phase: phase_rx,
                commands: commands_tx,
                rates: Rates::new(),
            },
        );

        let task = TorrentTask {
            client: self.client.clone(),
            resume_dir: self.resume_dir.clone(),
            cancel,
            phase: phase_tx,
            commands: commands_rx,
        };
        self.handle.spawn(task.run(resolve));
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
    /// Known up front for a magnet or a resume file, and for anything else once it's running.
    fn info_hash(&self) -> Option<InfoHash> {
        self.info_hash.or_else(|| match &*self.phase.borrow() {
            Phase::Downloading { torrent, .. } | Phase::Paused { torrent, .. } => Some(torrent.info_hash),
            _ => None,
        })
    }

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
                TorrentState::Downloading(progress(&torrent, &root, &stats, &self.rates))
            }
            Phase::Paused { torrent, root, stats } => {
                self.rates = Rates::new();
                TorrentState::Paused(progress(&torrent, &root, &stats, &self.rates))
            }
        }
    }
}

fn progress(torrent: &Torrent, root: &Path, stats: &TorrentSwarmStats, rates: &Rates) -> Progress {
    Progress {
        name: torrent.name.clone(),
        root: root.display().to_string(),
        files: torrent.files.iter().map(|(_, p)| p.display().to_string()).collect(),
        total_size: torrent.total_size,
        downloaded: stats.downloaded,
        wasted: stats.wasted,
        uploaded: stats.uploaded,
        left: stats.left as u64,
        verified_pieces: stats.verified_cnt(),
        total_pieces: stats.total_pieces(),
        completed: stats.completed,
        download_bps: rates.download_bps,
        upload_bps: rates.upload_bps,
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
            wasted: 0,
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

    /// Any free port, no DHT node: the tests must not touch the network.
    fn test_config(dir: &Path) -> SessionConfig {
        SessionConfig {
            peer_id: *b"-DL0100-session-tst.",
            port: 0,
            data_dir: dir.to_path_buf(),
            dht: false,
        }
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

        let mut session = Session::new(test_config(&dir)).unwrap();
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
        session.remove(dup, true);

        session.remove(id, true);
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

    /// Pausing takes the torrent out of the client but keeps its files; the resume file says
    /// paused, so a fresh session brings it back paused, and unpausing starts it again.
    #[test]
    fn pause_survives_a_restart_and_unpause_starts_again() {
        let dir = scratch("pause");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));

        session.pause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));
        assert!(root.join("session.bin").exists(), "pausing keeps the files");
        let Some((_, TorrentState::Paused(progress))) = session.torrents().into_iter().find(|(i, _)| *i == id) else {
            panic!()
        };
        assert_eq!((progress.total_pieces, progress.download_bps), (3, 0.0));
        let files = session.resumable();
        assert!(files.is_empty(), "a paused torrent is still in the session: {files:?}");
        session.shutdown();

        let mut session = Session::new(test_config(&dir)).unwrap();
        let files = session.resumable();
        assert_eq!(files.len(), 1);
        assert!(files[0].paused);
        let ids = session.resume_all();
        assert_eq!(ids.len(), 1);
        wait_for(&mut session, ids[0], |s| matches!(s, Some(TorrentState::Paused(_))));
        assert!(session.resumable().is_empty(), "resumed torrents aren't offered again");

        session.unpause(ids[0]);
        wait_for(&mut session, ids[0], |s| {
            matches!(s, Some(TorrentState::Downloading(_)))
        });
        session.shutdown();

        let session = Session::new(test_config(&dir)).unwrap();
        assert!(!session.resumable()[0].paused, "unpausing clears the flag");
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Removing without deleting files leaves the data behind and takes only the resume file.
    #[test]
    fn remove_can_keep_the_files() {
        let dir = scratch("keep");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");
        let resume_dir = dir.join("resume");

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.remove(id, false);
        wait_for(&mut session, id, |s| s.is_none());
        let deadline = Instant::now() + Duration::from_secs(5);
        while std::fs::read_dir(&resume_dir).map(|d| d.count()).unwrap_or(0) > 0 {
            assert!(Instant::now() < deadline, "resume file still there after remove");
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(root.join("session.bin").exists(), "the data stays");
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn shutdown_keeps_files_and_resume_data() {
        let dir = scratch("shutdown");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");
        let resume_dir = dir.join("resume");

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.shutdown();

        assert!(root.join("session.bin").exists());
        assert_eq!(std::fs::read_dir(&resume_dir).unwrap().count(), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
