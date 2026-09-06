//! A multi-torrent download session, with all the orchestration a UI would otherwise have to
//! do itself: owning a runtime, resolving sources (file or magnet), driving the client, saving
//! resume data, tracking transfer rates, and cleaning up on removal.
//!
//! The point is that a front end shouldn't need to know about tokio, channels, or the shape of
//! the client at all. It calls [`Session::add`] with whatever the user typed, calls
//! [`Session::torrents`] whenever it wants to draw, and renders the [`TorrentState`]s it gets
//! back.

use crate::config::{Settings, SettingsWatch};
use crate::defs::Identity;
use crate::dht::Dht;
use crate::peer::PeerSnapshot;
use crate::portmap::MappingState;
use crate::resume::{ResumeData, ResumeInputs, ResumeSummary, keep_saving, list_resume_files};
use crate::torrent::Torrent;
use crate::torrent_swarm::TorrentSwarmStats;
use crate::{BtClient, load_source};
use bitvec::prelude::*;
use midwest_mainline::types::InfoHash;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
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
    /// Waiting for one of `Settings::max_active_downloads` slots; starts by itself
    Queued(Progress),
    /// Re-hashing the files on disk (`recheck`); goes back to downloading or paused after
    Checking {
        name: String,
        checked_pieces: usize,
        total_pieces: usize,
    },
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
    pub files: Vec<FileInfo>,
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
    /// connected peers; empty while paused
    pub peers: Vec<PeerInfo>,
    /// pieces are fetched in order, see `Session::set_sequential`
    pub sequential: bool,
}

impl Progress {
    /// Uploaded over the torrent's whole life as a share of its size, what the seeding
    /// ratio limit compares against.
    pub fn ratio(&self) -> f64 {
        if self.total_size == 0 {
            0.0
        } else {
            self.uploaded as f64 / self.total_size as f64
        }
    }
}

/// One of a torrent's files.
#[derive(Debug, Clone, PartialEq)]
pub struct FileInfo {
    pub path: String,
    pub size: u64,
    /// whether the user wants it downloaded; see `Session::select_files`
    pub selected: bool,
}

/// One connected peer, ready to render.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerInfo {
    pub addr: String,
    pub client: String,
    /// the share of the torrent the peer has, in 0.0..=1.0
    pub progress: f32,
    pub downloaded: u64,
    pub uploaded: u64,
    pub download_bps: f64,
    pub upload_bps: f64,
    /// the usual client shorthand: `D`/`d` we download from it (`d`: want to, but choked),
    /// `U`/`u` it downloads from us (`u`: wants to, but we choke it)
    pub flags: String,
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
    /// one flag per file
    selected: Vec<bool>,
    /// bytes uploaded in earlier sessions
    uploaded: u64,
    sequential: bool,
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
    /// the file selection, read by the resume saver and shown in the phase
    selected: watch::Sender<Vec<bool>>,
    /// same for the sequential switch
    sequential: watch::Sender<bool>,
    /// the session's active-download slots, see `Settings::max_active_downloads`
    slots: Arc<Semaphore>,
    /// known before resolving for a magnet or a resume file; names the resume file to
    /// delete if removal comes before the torrent is known
    info_hash: Option<InfoHash>,
    settings: SettingsWatch,
    /// bytes uploaded in earlier stretches of this torrent's life, including earlier
    /// sessions (from the resume file); the running swarm's own count is added on top
    uploaded_before: u64,
}

/// What ended a running or paused stretch of a torrent's life.
enum Stop {
    Pause,
    Unpause,
    Recheck,
    Remove { delete_files: bool },
    Shutdown,
}

impl TorrentTask {
    async fn run<F, Fut>(mut self, resolve: F)
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = anyhow::Result<Resolved>>,
    {
        let mut resolve = std::pin::pin!(resolve(self.cancel.clone()));
        // a pause asked for while still resolving applies once resolved
        let mut pause_asked = false;
        let resolved = loop {
            tokio::select! {
                resolved = &mut resolve => break resolved,
                _ = self.cancel.cancelled() => return,
                command = self.commands.recv() => match command {
                    // removed while still resolving: nothing was written yet
                    Some(Command::Remove { .. }) | None => return,
                    Some(Command::Pause) => pause_asked = true,
                    Some(Command::Unpause) => pause_asked = false,
                    Some(Command::SelectFiles(_) | Command::Recheck | Command::Sequential(_)) => {}
                },
            }
        };
        let Resolved {
            torrent,
            root,
            mut verified,
            mut resumed,
            mut paused,
            mut peers,
            selected,
            uploaded,
            sequential,
        } = match resolved {
            Ok(resolved) => resolved,
            Err(e) => {
                self.fail(format!("{e:#}"), None).await;
                return;
            }
        };
        let torrent = Arc::new(torrent);
        let _ = self.selected.send(selected);
        let _ = self.sequential.send(sequential);
        self.uploaded_before = uploaded;
        paused |= pause_asked;

        let mut last_stats = None;
        let stop = loop {
            let stop = if paused {
                self.wait_while_paused(&torrent, &root, &verified, last_stats.take())
                    .await
            } else {
                self.run_until_stopped(&torrent, &root, &mut verified, &mut resumed, &mut peers)
                    .await
            };
            match stop {
                Ok((Stop::Pause, stats)) => {
                    paused = true;
                    last_stats = stats;
                }
                Ok((Stop::Unpause, _)) => paused = false,
                // back to whichever of the two it was in, with what the disk really holds
                Ok((Stop::Recheck, _)) => match self.check(&torrent, &root, &mut verified).await {
                    Ok(()) => resumed = true,
                    Err(stop) => break stop,
                },
                Ok((stop, _)) => break stop,
                Err(e) => {
                    self.fail(format!("{e:#}"), Some((&torrent, &root))).await;
                    return;
                }
            }
        };

        if let Stop::Remove { delete_files } = stop {
            self.remove_files(&torrent, &root, delete_files);
        }
    }

    /// Re-hashes the files off the runtime, publishing progress meanwhile. Only removal and
    /// shutdown interrupt it; the hashing itself runs to its end regardless.
    async fn check(
        &mut self,
        torrent: &Arc<Torrent>,
        root: &Path,
        verified: &mut BitBox<u8, Msb0>,
    ) -> Result<(), Stop> {
        let (progress, checked) = watch::channel(0);
        let _ = self.phase.send(Phase::Checking {
            torrent: torrent.clone(),
            checked,
        });
        let hashing = {
            let torrent = torrent.clone();
            let root = root.to_path_buf();
            tokio::task::spawn_blocking(move || {
                crate::check::check_files(&torrent, &root, |n| {
                    let _ = progress.send(n);
                })
            })
        };
        tokio::pin!(hashing);
        loop {
            tokio::select! {
                result = &mut hashing => {
                    match result {
                        Ok(bits) => {
                            tracing::info!("{}: {} of {} pieces are on disk", torrent.name, bits.count_ones(), bits.len());
                            *verified = bits;
                        }
                        Err(e) => tracing::warn!("rechecking {} failed: {e}", torrent.name),
                    }
                    return Ok(());
                }
                _ = self.cancel.cancelled() => return Err(Stop::Shutdown),
                command = self.commands.recv() => match command {
                    Some(Command::Remove { delete_files }) => return Err(Stop::Remove { delete_files }),
                    None => return Err(Stop::Shutdown),
                    Some(_) => {}
                },
            }
        }
    }

    /// Publishes the failure and stays around so the torrent can still be removed, and its
    /// resume file (and data, if asked) with it; a task that simply returned here would leave
    /// the entry unremovable and the resume file to resurrect it at the next start.
    async fn fail(&mut self, error: String, torrent: Option<(&Arc<Torrent>, &Path)>) {
        let _ = self.phase.send(Phase::Failed { error });
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                command = self.commands.recv() => match command {
                    Some(Command::Remove { delete_files }) => {
                        if let Some((torrent, root)) = torrent {
                            self.remove_files(torrent, root, delete_files);
                        } else if let Some(info_hash) = self.info_hash {
                            let _ = std::fs::remove_file(self.resume_dir.join(ResumeData::file_name(&info_hash)));
                        }
                        return;
                    }
                    None => return,
                    Some(_) => {}
                },
            }
        }
    }

    fn remove_files(&self, torrent: &Torrent, root: &Path, delete_files: bool) {
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

    /// Puts the torrent in the client and keeps its resume file current until something
    /// stops it, then takes it out again. `verified` is updated to what's on disk by then.
    async fn run_until_stopped(
        &mut self,
        torrent: &Arc<Torrent>,
        root: &Path,
        verified: &mut BitBox<u8, Msb0>,
        resumed: &mut bool,
        peers: &mut Vec<std::net::SocketAddr>,
    ) -> anyhow::Result<(Stop, Option<TorrentSwarmStats>)> {
        // held while downloading; released on completion so seeding never counts
        let mut slot = match verified.all() {
            true => None,
            false => match self.wait_for_slot(torrent, root, verified).await {
                Ok(permit) => Some(permit),
                Err(stop) => return Ok((stop, None)),
            },
        };
        if *resumed {
            self.client
                .add_torrent_resumed((**torrent).clone(), root, verified.clone())?;
        } else {
            self.client.add_torrent((**torrent).clone(), root)?;
        }
        // whatever happens next, the files exist
        *resumed = true;
        self.client.add_peers(&torrent.info_hash, std::mem::take(peers));
        if self.selected.borrow().iter().any(|s| !s) {
            self.client
                .select_files(&torrent.info_hash, self.selected.borrow().clone());
        }
        if *self.sequential.borrow() {
            self.client.set_sequential(&torrent.info_hash, true);
        }
        let stats = self
            .client
            .stats(torrent)
            .ok_or_else(|| anyhow::anyhow!("torrent was added but reported no stats"))?;
        let peers = self.client.peers(torrent).unwrap_or_else(|| watch::channel(vec![]).1);
        let stop_saving = self.cancel.child_token();
        let saver = tokio::spawn(keep_saving(
            torrent.clone(),
            root.to_path_buf(),
            stats.clone(),
            ResumeInputs {
                selected: self.selected.subscribe(),
                sequential: self.sequential.subscribe(),
                uploaded_before: self.uploaded_before,
            },
            self.resume_dir.clone(),
            stop_saving.clone(),
        ));
        let _ = self.phase.send(Phase::Downloading {
            torrent: torrent.clone(),
            root: root.to_path_buf(),
            stats: stats.clone(),
            peers,
            selected: self.selected.subscribe(),
            sequential: self.sequential.subscribe(),
            uploaded_before: self.uploaded_before,
        });

        let mut ratio_stats = stats.clone();
        let stop = loop {
            if ratio_stats.borrow().completed {
                drop(slot.take());
            }
            // a torrent that comes back already over its ratio stops before waiting for
            // anything to change
            if self.seeded_enough(torrent, &ratio_stats.borrow_and_update()) {
                break Stop::Pause;
            }
            tokio::select! {
                _ = self.cancel.cancelled() => break Stop::Shutdown,
                command = self.commands.recv() => match command {
                    Some(Command::Pause) => break Stop::Pause,
                    Some(Command::Recheck) => break Stop::Recheck,
                    Some(Command::Remove { delete_files }) => break Stop::Remove { delete_files },
                    Some(Command::SelectFiles(selected)) => {
                        self.client.select_files(&torrent.info_hash, selected.clone());
                        let _ = self.selected.send(selected);
                    }
                    Some(Command::Sequential(on)) => {
                        self.client.set_sequential(&torrent.info_hash, on);
                        let _ = self.sequential.send(on);
                    }
                    Some(Command::Unpause) => {}
                    None => break Stop::Shutdown,
                },
                // the stats change on every verified piece and uploaded block, the settings
                // when the user edits the limit; either can be what tips the ratio over
                changed = ratio_stats.changed() => {
                    if changed.is_err() {
                        break Stop::Shutdown;
                    }
                    if self.seeded_enough(torrent, &ratio_stats.borrow()) {
                        break Stop::Pause;
                    }
                }
                _ = self.settings.changed() => {
                    if self.seeded_enough(torrent, &stats.borrow()) {
                        break Stop::Pause;
                    }
                }
            }
        };
        // the torrent leaves the client; the saver gets its final write in before anything
        // is deleted, or the deletion would race it
        self.client.remove_torrent(&torrent.info_hash);
        stop_saving.cancel();
        let _ = saver.await;
        let last = stats.borrow().clone();
        *verified = last.verified.clone();
        self.uploaded_before += last.uploaded;
        Ok((stop, Some(last)))
    }

    /// Waits for an active-download slot, showing the torrent as queued meanwhile and still
    /// taking the commands that make sense for one that isn't running.
    async fn wait_for_slot(
        &mut self,
        torrent: &Arc<Torrent>,
        root: &Path,
        verified: &BitBox<u8, Msb0>,
    ) -> Result<OwnedSemaphorePermit, Stop> {
        let slots = self.slots.clone();
        let acquire = slots.acquire_owned();
        tokio::pin!(acquire);
        loop {
            // published on entry and again after a selection change, like the paused phase
            let wanted = torrent.wanted_pieces(&self.selected.borrow());
            let _ = self.phase.send(Phase::Queued {
                torrent: torrent.clone(),
                root: root.to_path_buf(),
                stats: TorrentSwarmStats::for_verified(torrent, verified.clone(), wanted),
                selected: self.selected.subscribe(),
                sequential: self.sequential.subscribe(),
                uploaded_before: self.uploaded_before,
            });
            tokio::select! {
                permit = &mut acquire => return Ok(permit.expect("the slots are never closed")),
                _ = self.cancel.cancelled() => return Err(Stop::Shutdown),
                command = self.commands.recv() => match command {
                    Some(Command::Pause) => return Err(Stop::Pause),
                    Some(Command::Recheck) => return Err(Stop::Recheck),
                    Some(Command::Remove { delete_files }) => return Err(Stop::Remove { delete_files }),
                    Some(Command::SelectFiles(selected)) => {
                        let _ = self.selected.send(selected);
                    }
                    Some(Command::Sequential(on)) => {
                        let _ = self.sequential.send(on);
                    }
                    Some(Command::Unpause) => {}
                    None => return Err(Stop::Shutdown),
                },
            }
        }
    }

    /// Complete, with a ratio limit set, and uploaded that many times its size over its life.
    fn seeded_enough(&self, torrent: &Torrent, stats: &TorrentSwarmStats) -> bool {
        let limit = self.settings.borrow().seed_ratio_limit;
        if !stats.completed || limit <= 0.0 || torrent.total_size == 0 {
            return false;
        }
        let uploaded = self.uploaded_before + stats.uploaded;
        let reached = uploaded as f64 / torrent.total_size as f64 >= limit;
        if reached {
            tracing::info!("{} seeded to ratio {limit}, stopping", torrent.name);
        }
        reached
    }

    /// Marks the resume file paused, so a restart brings the torrent back paused, and waits
    /// to be unpaused or removed.
    async fn wait_while_paused(
        &mut self,
        torrent: &Arc<Torrent>,
        root: &Path,
        verified: &BitBox<u8, Msb0>,
        last_stats: Option<TorrentSwarmStats>,
    ) -> anyhow::Result<(Stop, Option<TorrentSwarmStats>)> {
        // the counters of the stretch that just ended, if there was one; a torrent that
        // started paused has none
        // the uploaded count of a stretch that just ended is already folded into
        // `uploaded_before`, so the snapshot shown while paused must not add it again
        let mut stats = last_stats.unwrap_or_else(|| {
            let wanted = torrent.wanted_pieces(&self.selected.borrow());
            TorrentSwarmStats::for_verified(torrent, verified.clone(), wanted)
        });
        stats.uploaded = 0;
        let _ = self.phase.send(Phase::Paused {
            torrent: torrent.clone(),
            root: root.to_path_buf(),
            stats: stats.clone(),
            selected: self.selected.subscribe(),
            sequential: self.sequential.subscribe(),
            uploaded_before: self.uploaded_before,
        });
        let mut data = ResumeData::from_torrent(torrent, root, verified);
        data.paused = true;
        data.skip = crate::resume::skipped(&self.selected.borrow());
        data.sequential = *self.sequential.borrow();
        data.uploaded = self.uploaded_before;
        if let Err(e) = data.write(&self.resume_dir.join(ResumeData::file_name(&torrent.info_hash))) {
            tracing::warn!("couldn't mark {} paused in its resume file: {e:#}", torrent.name);
        }
        let stop = loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break Stop::Shutdown,
                command = self.commands.recv() => match command {
                    Some(Command::Unpause) => break Stop::Unpause,
                    Some(Command::Recheck) => break Stop::Recheck,
                    Some(Command::Remove { delete_files }) => break Stop::Remove { delete_files },
                    Some(Command::SelectFiles(selected)) => {
                        let _ = self.selected.send(selected);
                        // the paused resume file and the phase should say so too
                        break Stop::Pause;
                    }
                    Some(Command::Sequential(on)) => {
                        let _ = self.sequential.send(on);
                        break Stop::Pause;
                    }
                    Some(Command::Pause) => {}
                    None => break Stop::Shutdown,
                },
            }
        };
        Ok((stop, Some(stats)))
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
        peers: watch::Receiver<Vec<PeerSnapshot>>,
        selected: watch::Receiver<Vec<bool>>,
        sequential: watch::Receiver<bool>,
        uploaded_before: u64,
    },
    Paused {
        torrent: Arc<Torrent>,
        root: PathBuf,
        /// the last stats before the swarm was stopped
        stats: TorrentSwarmStats,
        selected: watch::Receiver<Vec<bool>>,
        sequential: watch::Receiver<bool>,
        uploaded_before: u64,
    },
    Queued {
        torrent: Arc<Torrent>,
        root: PathBuf,
        stats: TorrentSwarmStats,
        selected: watch::Receiver<Vec<bool>>,
        sequential: watch::Receiver<bool>,
        uploaded_before: u64,
    },
    Checking {
        torrent: Arc<Torrent>,
        /// pieces hashed so far
        checked: watch::Receiver<usize>,
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
    SelectFiles(Vec<bool>),
    Recheck,
    Sequential(bool),
}

/// The whole session at a glance, for a status bar.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionStatus {
    /// summed over every torrent
    pub download_bps: f64,
    pub upload_bps: f64,
    /// nodes in the DHT routing table; `None` with no node (off, or not up yet)
    pub dht_nodes: Option<usize>,
    pub listen_port: u16,
    pub port_mapping: MappingState,
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
    /// per-peer rate samples, dropped for peers that went away
    peer_rates: HashMap<std::net::SocketAddr, Rates>,
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
    /// active-download slots, `Settings::max_active_downloads` of them
    slots: Arc<Semaphore>,
    data_dir: PathBuf,
    settings: watch::Sender<Settings>,
}

/// What a session needs to start.
pub struct SessionConfig {
    pub peer_id: [u8; 20],
    /// where the session keeps its own files: resume data, the DHT database, and the
    /// settings. See `paths::data_dir` for the usual answer.
    pub data_dir: PathBuf,
    /// the settings to start with; `Settings::load(&data_dir)` for what the user saved
    pub settings: Settings,
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
        let port = config.settings.listen_port;
        let identity = Identity {
            peer_id: config.peer_id,
            serving: std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, port).into(),
            dht: config.settings.dht,
            encryption: config.settings.encryption,
        };
        let shutdown = CancellationToken::new();
        let (settings_tx, settings_rx) = watch::channel(config.settings.clone());
        // the client and the DHT node start on whatever runtime is current
        let (client, dht) = {
            let _on_runtime = rt.enter();
            let dht = config
                .settings
                .dht
                .then(|| Dht::start(config.data_dir.join("dht.db"), config.settings.dht_port()));
            let watch = dht.as_ref().map_or_else(Dht::none, Dht::watch);
            (
                BtClient::new_with_shutdown(identity, shutdown.clone(), watch, settings_rx),
                dht,
            )
        };
        Ok(Self {
            slots: Arc::new(Semaphore::new(slot_count(&config.settings))),
            handle: rt.handle().clone(),
            rt: Some(rt),
            identity: Arc::new(identity),
            client,
            shutdown,
            torrents: BTreeMap::new(),
            next_id: 1,
            resume_dir,
            _dht: dht,
            data_dir: config.data_dir,
            settings: settings_tx,
        })
    }

    pub fn settings(&self) -> Settings {
        self.settings.borrow().clone()
    }

    /// Saves and applies new settings. The connection cap and rate limits take effect at
    /// once; the listen port and the DHT switch need a restart, which is the caller's to
    /// arrange.
    pub fn update_settings(&mut self, settings: Settings) -> anyhow::Result<()> {
        settings.save(&self.data_dir)?;
        let before = slot_count(&self.settings.borrow());
        let after = slot_count(&settings);
        if after > before {
            self.slots.add_permits(after - before);
        } else if after < before {
            // taken out of circulation as they free up: a running download keeps its place
            let slots = self.slots.clone();
            self.handle.spawn(async move {
                if let Ok(permits) = slots.acquire_many_owned((before - after) as u32).await {
                    permits.forget();
                }
            });
        }
        let _ = self.settings.send(settings);
        Ok(())
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
        let utp = self.client.utp();
        let info_hash = crate::magnet::parse_magnet(&source).ok().map(|m| m.info_hash);
        self.launch(source.clone(), info_hash, |cancel| async move {
            let loaded = load_source(&source, identity, cancel, dht, utp).await?;
            let nothing = bitvec![u8, Msb0; 0; loaded.torrent.pieces.len()].into_boxed_bitslice();
            Ok(Resolved {
                selected: vec![true; loaded.torrent.files.len()],
                torrent: loaded.torrent,
                root,
                verified: nothing,
                resumed: false,
                paused: false,
                peers: loaded.peers,
                uploaded: 0,
                sequential: false,
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
            let torrent = data.to_torrent()?;
            Ok(Resolved {
                selected: data.selected(torrent.files.len()),
                torrent,
                root: data.root,
                verified: data.verified,
                resumed: true,
                paused: data.paused,
                peers: vec![],
                uploaded: data.uploaded,
                sequential: data.sequential,
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

    /// Re-hashes the torrent's files and continues from what's actually on disk, in the
    /// state (downloading or paused) it was in. For when the resume data can't be trusted.
    pub fn recheck(&mut self, id: TorrentId) {
        self.command(id, Command::Recheck);
    }

    /// Downloads only the selected files from now on: one flag per file in the order
    /// `Progress::files` lists them. Pieces shared with a selected file are still fetched.
    pub fn select_files(&mut self, id: TorrentId, selected: Vec<bool>) {
        self.command(id, Command::SelectFiles(selected));
    }

    /// Fetches pieces in order rather than rarest first, so a file can be played while it
    /// downloads. Remembered across restarts.
    pub fn set_sequential(&mut self, id: TorrentId, on: bool) {
        self.command(id, Command::Sequential(on));
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
                peer_rates: HashMap::new(),
            },
        );

        let task = TorrentTask {
            client: self.client.clone(),
            resume_dir: self.resume_dir.clone(),
            cancel,
            phase: phase_tx,
            commands: commands_rx,
            selected: watch::channel(vec![]).0,
            sequential: watch::channel(false).0,
            slots: self.slots.clone(),
            info_hash,
            settings: self.settings.subscribe(),
            uploaded_before: 0,
        };
        self.handle.spawn(task.run(resolve));
        id
    }

    /// Totals and the network's state; call after `torrents`, which is what refreshes the
    /// rates.
    pub fn status(&self) -> SessionStatus {
        let (download_bps, upload_bps) = self
            .torrents
            .values()
            .map(|e| (e.rates.download_bps, e.rates.upload_bps))
            .fold((0.0, 0.0), |(d, u), (dd, uu)| (d + dd, u + uu));
        SessionStatus {
            download_bps,
            upload_bps,
            dht_nodes: self.client.dht().borrow().as_ref().map(|dht| dht.node_count()),
            listen_port: self.identity.serving.port(),
            port_mapping: self.client.port_mapping().borrow().clone(),
        }
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
            Phase::Downloading { torrent, .. }
            | Phase::Paused { torrent, .. }
            | Phase::Queued { torrent, .. }
            | Phase::Checking { torrent, .. } => Some(torrent.info_hash),
            _ => None,
        })
    }

    fn state(&mut self) -> TorrentState {
        let phase = self.phase.borrow_and_update().clone();
        let queued = matches!(phase, Phase::Queued { .. });
        match phase {
            Phase::Failed { error } => TorrentState::Failed {
                source: self.source.clone(),
                error,
            },
            Phase::Resolving { started } => TorrentState::Resolving {
                source: self.source.clone(),
                elapsed: started.elapsed(),
            },
            Phase::Checking { torrent, checked } => TorrentState::Checking {
                name: torrent.name.clone(),
                checked_pieces: *checked.borrow(),
                total_pieces: torrent.pieces.len(),
            },
            Phase::Downloading {
                torrent,
                root,
                stats,
                peers,
                selected,
                sequential,
                uploaded_before,
            } => {
                let stats = stats.borrow().clone();
                self.rates.update(stats.downloaded, stats.uploaded);
                let mut progress = progress(
                    &torrent,
                    &root,
                    &stats,
                    &selected.borrow(),
                    *sequential.borrow(),
                    &self.rates,
                );
                progress.uploaded += uploaded_before;
                progress.peers = self.peers(&peers.borrow());
                TorrentState::Downloading(progress)
            }
            Phase::Paused {
                torrent,
                root,
                stats,
                selected,
                sequential,
                uploaded_before,
            }
            | Phase::Queued {
                torrent,
                root,
                stats,
                selected,
                sequential,
                uploaded_before,
            } => {
                self.rates = Rates::new();
                self.peer_rates.clear();
                let mut progress = progress(
                    &torrent,
                    &root,
                    &stats,
                    &selected.borrow(),
                    *sequential.borrow(),
                    &self.rates,
                );
                progress.uploaded += uploaded_before;
                if queued {
                    TorrentState::Queued(progress)
                } else {
                    TorrentState::Paused(progress)
                }
            }
        }
    }

    fn peers(&mut self, snapshots: &[PeerSnapshot]) -> Vec<PeerInfo> {
        self.peer_rates
            .retain(|addr, _| snapshots.iter().any(|p| p.addr == *addr));
        snapshots
            .iter()
            .map(|p| {
                let rates = self.peer_rates.entry(p.addr).or_insert_with(Rates::new);
                rates.update(p.downloaded, p.uploaded);
                let mut flags = String::new();
                if p.interested_them {
                    flags.push(if p.choked_us { 'd' } else { 'D' });
                }
                if p.interested_us {
                    flags.push(if p.choked_them { 'u' } else { 'U' });
                }
                if p.encrypted {
                    flags.push('E');
                }
                if p.utp {
                    flags.push('T');
                }
                PeerInfo {
                    addr: p.addr.to_string(),
                    client: p.client.clone(),
                    progress: p.progress,
                    downloaded: p.downloaded,
                    uploaded: p.uploaded,
                    download_bps: rates.download_bps,
                    upload_bps: rates.upload_bps,
                    flags,
                }
            })
            .collect()
    }
}

fn progress(
    torrent: &Torrent,
    root: &Path,
    stats: &TorrentSwarmStats,
    selected: &[bool],
    sequential: bool,
    rates: &Rates,
) -> Progress {
    Progress {
        name: torrent.name.clone(),
        root: root.display().to_string(),
        files: torrent
            .files
            .iter()
            .enumerate()
            .map(|(i, (size, p))| FileInfo {
                path: p.display().to_string(),
                size: *size as u64,
                selected: selected.get(i).copied().unwrap_or(true),
            })
            .collect(),
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
        peers: vec![],
        sequential,
    }
}

/// `max_active_downloads` as semaphore permits; 0 means no limit, which is a count no session
/// reaches (and small enough to shrink from with `acquire_many`).
fn slot_count(settings: &Settings) -> usize {
    match settings.max_active_downloads {
        0 => 1 << 20,
        n => n,
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
    fn update(&mut self, downloaded: u64, uploaded: u64) {
        let elapsed = self.last_sample.elapsed();
        if elapsed < Duration::from_millis(500) {
            return;
        }
        self.download_bps = downloaded.saturating_sub(self.last_downloaded) as f64 / elapsed.as_secs_f64();
        self.upload_bps = uploaded.saturating_sub(self.last_uploaded) as f64 / elapsed.as_secs_f64();
        self.last_downloaded = downloaded;
        self.last_uploaded = uploaded;
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
            peers: vec![],
            sequential: false,
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
        write_torrent_named(dir, "session", 7)
    }

    /// A `.torrent` for `<name>.bin`, 40 bytes of `fill` in 16-byte pieces.
    fn write_torrent_named(dir: &Path, name: &str, fill: u8) -> PathBuf {
        use sha1::Digest;
        let content = [fill; 40];
        let pieces: Vec<u8> = content
            .chunks(16)
            .flat_map(|c| sha1::Sha1::digest(c).to_vec())
            .collect();
        let mut info = format!(
            "d6:lengthi40e4:name{}:{name}.bin12:piece lengthi16e6:pieces60:",
            name.len() + 4
        )
        .into_bytes();
        info.extend_from_slice(&pieces);
        info.push(b'e');
        let file = crate::metadata::build_torrent_file(&info, &["wss://unused.test/announce".to_string()]);
        let path = dir.join(format!("{name}.torrent"));
        std::fs::write(&path, file).unwrap();
        path
    }

    /// With one slot the second torrent waits; pausing the first lets it in, and the first
    /// then queues behind it on unpause. Raising the limit lets both run.
    #[test]
    fn downloads_beyond_the_limit_queue_up() {
        let dir = scratch("queue");
        let root = dir.join("downloads");
        let mut config = test_config(&dir);
        config.settings.max_active_downloads = 1;
        let mut session = Session::new(config).unwrap();

        let a = session.add(write_torrent_named(&dir, "a", 1).display().to_string(), &root);
        wait_for(&mut session, a, |s| matches!(s, Some(TorrentState::Downloading(_))));
        let b = session.add(write_torrent_named(&dir, "b", 2).display().to_string(), &root);
        wait_for(&mut session, b, |s| matches!(s, Some(TorrentState::Queued(_))));

        session.pause(a);
        wait_for(&mut session, b, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.unpause(a);
        wait_for(&mut session, a, |s| matches!(s, Some(TorrentState::Queued(_))));

        let mut settings = session.settings();
        settings.max_active_downloads = 2;
        session.update_settings(settings).unwrap();
        wait_for(&mut session, a, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Any free port, no DHT node: the tests must not touch the network.
    fn test_config(dir: &Path) -> SessionConfig {
        SessionConfig {
            peer_id: *b"-DL0100-session-tst.",
            data_dir: dir.to_path_buf(),
            settings: Settings {
                listen_port: 0,
                dht: false,
                ..Settings::default()
            },
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
        assert_eq!(
            progress.files.len(),
            1,
            "the paused snapshot is the real one, not a blank"
        );
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

    /// With the DHT off and an ephemeral port there's nothing to map, and the status says so.
    #[test]
    fn status_reports_the_network_state() {
        let dir = scratch("status");
        let mut session = Session::new(test_config(&dir)).unwrap();
        session.torrents();
        let status = session.status();
        assert_eq!(status.dht_nodes, None);
        assert_eq!(status.port_mapping, MappingState::Off);
        assert_eq!((status.download_bps, status.upload_bps), (0.0, 0.0));
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The torrent starts with nothing verified; writing the real content to its file and
    /// rechecking finds every piece, and the torrent goes back to being paused, complete.
    #[test]
    fn recheck_finds_what_is_on_disk() {
        let dir = scratch("recheck");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.pause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));

        std::fs::write(root.join("session.bin"), [7u8; 40]).unwrap();
        session.recheck(id);
        wait_for(
            &mut session,
            id,
            |s| matches!(s, Some(TorrentState::Paused(p)) if p.completed),
        );
        session.shutdown();

        let session = Session::new(test_config(&dir)).unwrap();
        assert_eq!(
            session.resumable()[0].verified_pieces,
            3,
            "the resume file has the new bitfield"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A torrent that failed still takes commands: removing it deletes its resume file (and
    /// data if asked) instead of leaving a file that brings it back at the next start.
    #[test]
    fn a_failed_torrent_can_still_be_removed() {
        let dir = scratch("failed-remove");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");
        let resume_dir = dir.join("resume");

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.pause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));
        // the data vanishes under a paused torrent; unpausing can't open it
        std::fs::remove_dir_all(&root).unwrap();
        session.unpause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Failed { .. })));
        assert_eq!(std::fs::read_dir(&resume_dir).unwrap().count(), 1);

        session.remove(id, true);
        wait_for(&mut session, id, |s| s.is_none());
        let deadline = Instant::now() + Duration::from_secs(5);
        while std::fs::read_dir(&resume_dir).map(|d| d.count()).unwrap_or(0) > 0 {
            assert!(
                Instant::now() < deadline,
                "the failed torrent's resume file survived removal"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A pause asked for while a torrent is still resolving doesn't kill the resolve; it
    /// applies once resolved.
    #[test]
    fn pause_while_resolving_applies_afterwards() {
        let dir = scratch("pause-resolving");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), &root);
        // straight away, before the task has had a chance to parse the file
        session.pause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));
        session.unpause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A complete torrent whose lifetime upload total is over the ratio limit stops seeding
    /// as soon as it starts; raising the limit lets it seed again.
    #[test]
    fn seeding_stops_at_the_ratio_limit() {
        let dir = scratch("ratio");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("session.bin"), [7u8; 40]).unwrap();
        let torrent = crate::parse_torrent(&std::fs::read(&torrent_file).unwrap()).unwrap();
        let mut data = ResumeData::from_torrent(&torrent, &root, &bitvec![u8, Msb0; 1; 3]);
        data.uploaded = 80;
        let resume_dir = dir.join("resume");
        std::fs::create_dir_all(&resume_dir).unwrap();
        let path = resume_dir.join(ResumeData::file_name(&torrent.info_hash));
        data.write(&path).unwrap();

        let mut config = test_config(&dir);
        config.settings.seed_ratio_limit = 1.5;
        let mut session = Session::new(config).unwrap();
        let id = session.resume(&path);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));
        let Some((_, TorrentState::Paused(progress))) = session.torrents().into_iter().find(|(i, _)| *i == id) else {
            panic!()
        };
        assert_eq!((progress.uploaded, progress.ratio()), (80, 2.0));

        let mut settings = session.settings();
        settings.seed_ratio_limit = 3.0;
        session.update_settings(settings).unwrap();
        session.unpause(id);
        wait_for(
            &mut session,
            id,
            |s| matches!(s, Some(TorrentState::Downloading(p)) if p.completed),
        );
        session.shutdown();
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
