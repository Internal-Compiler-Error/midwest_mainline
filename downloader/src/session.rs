//! A multi-torrent download session, with all the orchestration a UI would otherwise have to
//! do itself: owning a runtime, resolving sources (file or magnet), driving the client, saving
//! resume data, tracking transfer rates, and cleaning up on removal.
//!
//! The point is that a front end shouldn't need to know about tokio, channels, or the shape of
//! the client at all. It calls [`Session::add`] with whatever the user typed, calls
//! [`Session::torrents`] whenever it wants to draw, and renders the [`TorrentState`]s it gets
//! back.

use crate::announcer::{TrackerState, TrackerStatus};
use crate::config::{Settings, SettingsWatch};
use crate::defs::Identity;
use crate::dht::{Dht, DhtWatch};
use crate::events::{Event, EventBus, Events};
use crate::feed::{Feed, FeedKey, Found};
use crate::peer::PeerSnapshot;
use crate::portmap::MappingState;
use crate::resume::{
    Modes, ResumeData, ResumeInputs, ResumeSummary, keep_saving, list_resume_files, scan_resume_files,
};
use crate::torrent::Torrent;
use crate::torrent_swarm::TorrentSwarmStats;
use crate::utp::UtpWatch;
use crate::{BtClient, Loaded, load_source};
use anyhow::Context;
use bitvec::prelude::*;
use midwest_mainline::types::InfoHash;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{File, TryLockError};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Identifies a torrent within one session, from `add`/`resume` until `remove`.
pub type TorrentId = u64;

/// Everything a front end needs to render one torrent, with no channels or futures in sight.
/// Serializes as JSON a web front end can take as it is: tagged by `kind`, `elapsed` in
/// milliseconds.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TorrentState {
    /// Resolving a source into a torrent. For a magnet this means announcing to its trackers
    /// and fetching metadata from a peer, which can take a while; `elapsed` is for showing
    /// that something is still happening.
    Resolving {
        source: String,
        #[serde(rename = "elapsed_ms", serialize_with = "json::millis")]
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
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Progress {
    /// 40 hex digits, what the event bus names the torrent by
    pub info_hash: String,
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
    /// BEP 16, see `Session::set_super_seed`
    pub super_seed: bool,
    /// the trackers and the DHT; empty while paused
    pub trackers: Vec<TrackerInfo>,
    /// BEP 46: the DHT key it updates through, if it does
    pub feed: Option<Feed>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TrackerInfo {
    /// the announce URL, or "DHT"
    pub url: String,
    /// as `state` ("pending", "working" or "failed") and, when failed, `error`
    #[serde(flatten, serialize_with = "json::tracker_state")]
    pub state: TrackerState,
    /// peers the last announce returned
    pub peers: usize,
    pub next_announce_secs: Option<u64>,
    /// the swarm's size as this tracker counts it; `None` where it hasn't said
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    /// times the torrent has been downloaded to completion
    pub downloaded: Option<u32>,
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
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FileInfo {
    pub path: String,
    pub size: u64,
    /// whether the user wants it downloaded; see `Session::select_files`
    pub selected: bool,
    /// BEP 47 padding: part of the piece stream, never on disk, nothing to show
    pub pad: bool,
}

/// One connected peer, ready to render.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PeerInfo {
    pub addr: String,
    pub client: String,
    /// the share of the torrent the peer has, in 0.0..=1.0
    pub progress: f32,
    pub downloaded: u64,
    pub uploaded: u64,
    pub download_bps: f64,
    pub upload_bps: f64,
    pub choked_us: bool,
    pub choked_them: bool,
    pub interested_us: bool,
    pub interested_them: bool,
    pub encrypted: bool,
    pub utp: bool,
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
    /// it comes from a resume file: its files are expected to exist already (checked, not
    /// created), and it has been away a while, so a key it follows is polled soon
    resumed: bool,
    /// start paused rather than downloading
    paused: bool,
    /// peers already known for it, handed to the swarm right away
    peers: Vec<std::net::SocketAddr>,
    /// one flag per file
    selected: Vec<bool>,
    /// bytes uploaded in earlier sessions
    uploaded: u64,
    modes: Modes,
    /// BEP 46: the key it follows
    feed: Option<Feed>,
}

impl Resolved {
    /// A torrent new to the session: nothing verified, every file selected.
    fn new(loaded: Loaded, root: PathBuf) -> Self {
        let Loaded { torrent, peers } = loaded;
        Self {
            verified: bitvec![u8, Msb0; 0; torrent.num_pieces()].into_boxed_bitslice(),
            selected: vec![true; torrent.files.len()],
            torrent,
            root,
            resumed: false,
            paused: false,
            peers,
            uploaded: 0,
            modes: Modes::default(),
            feed: None,
        }
    }

    /// Takes a switch flipped before the torrent was known. A file selection can only fit by
    /// chance, since the files weren't known either.
    fn switch(&mut self, switch: Switch) {
        match switch {
            Switch::Files(selected) => {
                if selected.len() == self.selected.len() {
                    self.selected = selected;
                }
            }
            Switch::Sequential(on) => self.modes.sequential = on,
            Switch::SuperSeed(on) => self.modes.super_seed = on,
        }
    }

    /// Picks a torrent back up from its resume file. Blocking.
    fn read(path: &Path) -> anyhow::Result<Self> {
        let data = ResumeData::read(path)?;
        let torrent = data.to_torrent()?;
        Ok(Self {
            selected: data.selected,
            torrent,
            root: data.root,
            verified: data.verified,
            resumed: true,
            paused: data.paused,
            peers: vec![],
            uploaded: data.uploaded,
            modes: data.modes,
            feed: data.feed,
        })
    }
}

/// What resolving a source needs from the session, for a torrent's task to take along.
#[derive(Clone)]
struct Loader {
    identity: Arc<Identity>,
    dht: DhtWatch,
    utp: UtpWatch,
    bus: EventBus,
}

impl Loader {
    async fn load(&self, source: &str, cancel: CancellationToken) -> anyhow::Result<Loaded> {
        let Self {
            identity,
            dht,
            utp,
            bus,
        } = self.clone();
        load_source(source, identity, cancel, dht, utp, bus).await
    }
}

/// What changes what a torrent is doing; the user's, except `Shutdown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Pause,
    Unpause,
    Recheck,
    Remove { delete_files: bool },
    Shutdown,
}

/// A per-torrent choice that holds whatever the torrent is doing.
enum Switch {
    Files(Vec<bool>),
    Sequential(bool),
    SuperSeed(bool),
}

/// What a front end can do to a torrent; the torrent's task carries it out.
enum Command {
    Act(Action),
    Set(Switch),
}

/// A torrent's task's side of the user: the commands its entry sends, the switches they
/// set, and the session's shutdown.
struct Controls {
    commands: mpsc::UnboundedReceiver<Command>,
    cancel: CancellationToken,
    /// the file selection, read by the resume saver and shown in the phase
    selected: watch::Sender<Vec<bool>>,
    modes: watch::Sender<Modes>,
}

impl Controls {
    /// The next command; `Shutdown` once the session is going away.
    async fn recv(&mut self) -> Command {
        tokio::select! {
            _ = self.cancel.cancelled() => Command::Act(Action::Shutdown),
            command = self.commands.recv() => command.unwrap_or(Command::Act(Action::Shutdown)),
        }
    }

    /// The next action. A switch that comes first is recorded, and passed on to the swarm
    /// `running` names, if any; that returns `None`, for a caller that shows the switches.
    async fn next(&mut self, running: Option<(&BtClient, &InfoHash)>) -> Option<Action> {
        match self.recv().await {
            Command::Act(action) => Some(action),
            Command::Set(switch) => {
                self.set(switch, running);
                None
            }
        }
    }

    fn set(&self, switch: Switch, running: Option<(&BtClient, &InfoHash)>) {
        if let Some((client, info_hash)) = running {
            match &switch {
                Switch::Files(selected) => client.select_files(info_hash, selected.clone()),
                Switch::Sequential(on) => client.set_sequential(info_hash, *on),
                Switch::SuperSeed(on) => client.set_super_seed(info_hash, *on),
            }
        }
        match switch {
            Switch::Files(selected) => {
                self.selected.send_replace(selected);
            }
            Switch::Sequential(on) => self.modes.send_modify(|m| m.sequential = on),
            Switch::SuperSeed(on) => self.modes.send_modify(|m| m.super_seed = on),
        }
    }
}

/// The task that owns one torrent for its whole life in the session: resolves the source,
/// puts the torrent in the client and takes it out again on pause, unpause, remove, and
/// shutdown, and keeps the resume file current in between.
struct TorrentTask {
    /// its entry's
    id: TorrentId,
    client: BtClient,
    bus: EventBus,
    /// what the user gave, for naming a torrent that failed before it was known
    source: String,
    resume_dir: PathBuf,
    phase: watch::Sender<Phase>,
    controls: Controls,
    /// the session's active-download slots, see `Settings::max_active_downloads`
    slots: Arc<Slots>,
    /// known before resolving for a magnet or a resume file; names the resume file to
    /// delete if removal comes before the torrent is known
    info_hash: Option<InfoHash>,
    /// the resume file this came from, if it did: what a failed torrent is retried from
    /// when its info hash isn't known (the file doesn't parse)
    resume_path: Option<PathBuf>,
    /// the info hashes the session's entries have to themselves, see `Claim`
    claimed: Claimed,
    settings: SettingsWatch,
    /// bytes uploaded in earlier stretches of this torrent's life, including earlier
    /// sessions (from the resume file); the running swarm's own count is added on top
    uploaded_before: u64,
    /// the bitfield the resume file on disk holds, see `ResumeInputs::persisted`
    persisted: BitBox<u8, Msb0>,
    /// BEP 46, read by the resume saver and the session's entry
    feed: watch::Sender<Option<Feed>>,
    /// where a followed key's newer version goes, for the session to add
    updates: mpsc::UnboundedSender<FeedUpdate>,
}

/// A newer version of a torrent that follows a DHT key (BEP 46), for the session to add.
struct FeedUpdate {
    previous: Arc<Torrent>,
    root: PathBuf,
    key: FeedKey,
    found: Found,
    /// the predecessor's feed, marked superseded only once the update has resolved: until
    /// then the predecessor is still the follower, so quitting early loses nothing
    previous_feed: watch::Sender<Option<Feed>>,
    /// the predecessor's file selection and modes, carried over
    selected: Vec<bool>,
    modes: Modes,
}

type Claimed = Arc<Claims>;

/// Who has which info hash to themselves, see `Claim`.
#[derive(Default)]
struct Claims {
    /// the entry holding each, and whether it has been removed and is only cleaning up
    held: std::sync::Mutex<HashMap<InfoHash, (TorrentId, bool)>>,
    released: Notify,
}

impl Claims {
    /// The entry is gone from the session; one that wants its torrent waits for it to finish.
    fn leaving(&self, id: TorrentId) {
        for (holder, leaving) in self.held.lock().unwrap().values_mut() {
            if *holder == id {
                *leaving = true;
            }
        }
    }
}

/// An info hash one entry has to itself: a second entry for the same torrent fails rather
/// than sharing (or truncating) its files and resume file. Let go when the entry's task ends.
struct Claim {
    claimed: Claimed,
    info_hash: InfoHash,
}

impl Claim {
    /// `None` if another entry holds it; one that was removed is waited out, so its files and
    /// resume file are dealt with before this entry touches them.
    async fn take(claimed: &Claimed, id: TorrentId, info_hash: InfoHash) -> Option<Self> {
        use std::collections::hash_map::Entry;
        loop {
            // registered before looking, so a release between the look and the wait isn't missed
            let released = claimed.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            match claimed.held.lock().unwrap().entry(info_hash) {
                Entry::Vacant(vacant) => {
                    vacant.insert((id, false));
                    return Some(Self {
                        claimed: claimed.clone(),
                        info_hash,
                    });
                }
                Entry::Occupied(held) if !held.get().1 => return None,
                Entry::Occupied(_) => {}
            }
            released.await;
        }
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.claimed.held.lock().unwrap().remove(&self.info_hash);
        self.claimed.released.notify_waiters();
    }
}

/// What of a failed torrent belongs to its entry, to clean up on removal and retry from.
enum Owned {
    /// it duplicates another entry's torrent, and everything on disk is that one's
    Nothing,
    Torrent {
        torrent: Arc<Torrent>,
        root: PathBuf,
    },
    /// it failed before the torrent was known; see `TorrentTask::info_hash` and `resume_path`
    Unresolved,
}

impl TorrentTask {
    async fn run<F, Fut>(mut self, resolve: F)
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = anyhow::Result<Resolved>>,
    {
        // a magnet or resume file names its torrent up front, so a duplicate fails at once
        let mut claim = None;
        if let Some(info_hash) = self.info_hash {
            claim = Claim::take(&self.claimed, self.id, info_hash).await;
            if claim.is_none() {
                self.info_hash = None;
                self.fail("this torrent is already added".to_string(), &Owned::Nothing)
                    .await;
                return;
            }
        }
        let mut resolve = std::pin::pin!(resolve(self.controls.cancel.clone()));
        // a pause or unpause asked for while still resolving, and the switches flipped, apply
        // once resolved, over what the resume file says
        let mut pause_asked = None;
        let mut switched = vec![];
        let resolved = loop {
            tokio::select! {
                resolved = &mut resolve => break resolved,
                command = self.controls.recv() => match command {
                    // a resume file is read in a moment, and has to go with the torrent;
                    // anything else has written nothing yet
                    Command::Act(Action::Remove { delete_files }) if self.resume_path.is_some() => {
                        tokio::select! {
                            resolved = &mut resolve => match resolved {
                                Ok(resolved) => self.remove_files(&resolved.torrent, &resolved.root, delete_files).await,
                                Err(_) => self.remove_unresolved(),
                            },
                            _ = self.controls.cancel.cancelled() => {}
                        }
                        return;
                    }
                    Command::Act(Action::Remove { .. } | Action::Shutdown) => return,
                    Command::Act(Action::Pause) => pause_asked = Some(true),
                    Command::Act(Action::Unpause) => pause_asked = Some(false),
                    Command::Set(switch) => switched.push(switch),
                    // there's nothing on disk to check yet
                    Command::Act(Action::Recheck) => {}
                },
            }
        };
        let mut resolved = resolved.map(|mut resolved| {
            resolved.paused = pause_asked.unwrap_or(resolved.paused);
            switched.into_iter().for_each(|switch| resolved.switch(switch));
            resolved
        });

        let mut recheck_first = false;
        loop {
            let (error, owned) = match resolved {
                Err(e) => (format!("{e:#}"), Owned::Unresolved),
                Ok(resolved) => {
                    if claim.is_none() {
                        claim = Claim::take(&self.claimed, self.id, resolved.torrent.info_hash).await;
                    }
                    if claim.is_none() {
                        self.info_hash = None;
                        (format!("{} is already added", resolved.torrent.name), Owned::Nothing)
                    } else {
                        match self.drive(resolved, recheck_first).await {
                            Some(failed) => failed,
                            None => return,
                        }
                    }
                }
            };
            let Some((path, recheck)) = self.fail(error, &owned).await else {
                return;
            };
            recheck_first = recheck;
            resolved = self.reload(path, recheck).await;
        }
    }

    /// Rereads a failed torrent's resume file to try it again. A retry by recheck leaves it
    /// paused if the file says so; one by unpause starts it.
    async fn reload(&mut self, path: PathBuf, recheck: bool) -> anyhow::Result<Resolved> {
        let _ = self.phase.send(Phase::Resolving {
            started: Instant::now(),
        });
        tracing::info!("retrying {}", path.display());
        let mut resolved = tokio::task::spawn_blocking(move || Resolved::read(&path)).await??;
        resolved.paused &= recheck;
        Ok(resolved)
    }

    /// Runs a resolved torrent until it's removed or the session shuts down, or until it
    /// fails, which is returned along with what of it is this entry's.
    async fn drive(&mut self, resolved: Resolved, recheck_first: bool) -> Option<(String, Owned)> {
        let Resolved {
            torrent,
            root,
            mut verified,
            mut resumed,
            mut paused,
            mut peers,
            selected,
            uploaded,
            modes,
            feed,
        } = resolved;
        let torrent = Arc::new(torrent);
        self.feed.send_replace(feed);
        let _follower = self.follow(&torrent, &root, resumed);
        self.persisted = match resumed {
            true => verified.clone(),
            false => bitvec![u8, Msb0; 0; verified.len()].into_boxed_bitslice(),
        };
        self.bus.emit(Event::TorrentResolved {
            info_hash: torrent.info_hash,
            name: torrent.name.clone(),
            size: torrent.total_size,
            pieces: torrent.num_pieces(),
            piece_size: torrent.piece_size,
            files: torrent.files.len(),
        });
        self.controls.selected.send_replace(selected);
        self.controls.modes.send_replace(modes);
        self.uploaded_before = uploaded;

        // the counters of the last running stretch, for the paused snapshot
        let mut last_stats = None;
        // a newly added torrent whose files are already there (its creator seeding it, a
        // download moved over from another client, an update starting from its
        // predecessor's) starts with a check, not from nothing
        let mut check = recheck_first
            || (!resumed && {
                let (torrent, root) = (torrent.clone(), root.clone());
                tokio::task::spawn_blocking(move || has_data_on_disk(&torrent, &root))
                    .await
                    .unwrap_or(false)
            });
        // a check just measured what's on disk, so whatever is missing can be laid down
        let mut checked = false;
        let action = loop {
            let action = if std::mem::take(&mut check) {
                // back to whichever of running or paused it was in, with what the disk holds
                match self.check(&torrent, &root, &mut verified).await {
                    Ok(pause_asked) => {
                        resumed = true;
                        checked = true;
                        paused = pause_asked.unwrap_or(paused);
                        continue;
                    }
                    Err(action) => action,
                }
            } else if paused {
                self.wait_while_paused(&torrent, &root, &verified, last_stats.take())
                    .await
            } else {
                match self
                    .run_until_stopped(&torrent, &root, &mut verified, (&mut resumed, &mut checked), &mut peers)
                    .await
                {
                    Ok((action, stats)) => {
                        if action == Action::Pause {
                            last_stats = stats;
                        }
                        action
                    }
                    Err(e) => return Some((format!("{e:#}"), Owned::Torrent { torrent, root })),
                }
            };
            match action {
                Action::Pause => paused = true,
                Action::Unpause => paused = false,
                Action::Recheck => check = true,
                Action::Remove { .. } | Action::Shutdown => break action,
            }
        };

        if let Action::Remove { delete_files } = action {
            self.remove_files(&torrent, &root, delete_files).await;
            self.bus.emit(Event::TorrentRemoved {
                info_hash: torrent.info_hash,
                deleted_files: delete_files,
            });
        }
        None
    }

    /// What the session shows of a known torrent whatever it's doing.
    fn shown(&self, torrent: &Arc<Torrent>, root: &Path) -> Shown {
        Shown {
            torrent: torrent.clone(),
            root: root.to_path_buf(),
            selected: self.controls.selected.subscribe(),
            modes: self.controls.modes.subscribe(),
            feed: self.feed.subscribe(),
            uploaded_before: self.uploaded_before,
        }
    }

    fn resume_inputs(&self) -> ResumeInputs {
        ResumeInputs {
            selected: self.controls.selected.subscribe(),
            modes: self.controls.modes.subscribe(),
            uploaded_before: self.uploaded_before,
            persisted: self.persisted.clone(),
            feed: self.feed.subscribe(),
        }
    }

    /// Re-hashes the files off the runtime, publishing progress meanwhile. Only removal and
    /// shutdown interrupt it; the hashing itself runs to its end regardless. A pause or
    /// unpause asked for meanwhile is returned for the caller to apply afterwards; the
    /// switches are recorded for when the torrent restarts.
    async fn check(
        &mut self,
        torrent: &Arc<Torrent>,
        root: &Path,
        verified: &mut BitBox<u8, Msb0>,
    ) -> Result<Option<bool>, Action> {
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
        let mut pause_asked = None;
        loop {
            tokio::select! {
                result = &mut hashing => {
                    match result {
                        Ok(bits) => {
                            tracing::info!("{}: {} of {} pieces are on disk", torrent.name, bits.count_ones(), bits.len());
                            self.bus.emit(Event::TorrentChecked {
                                info_hash: torrent.info_hash,
                                good: bits.count_ones(),
                                pieces: bits.len(),
                            });
                            *verified = bits;
                            // a piece found bad and fetched again must have its new data
                            // flushed before the resume file claims it once more
                            self.persisted &= verified.clone();
                        }
                        Err(e) => tracing::warn!("rechecking {} failed: {e}", torrent.name),
                    }
                    return Ok(pause_asked);
                }
                action = self.controls.next(None) => match action {
                    Some(Action::Pause) => pause_asked = Some(true),
                    Some(Action::Unpause) => pause_asked = Some(false),
                    Some(Action::Recheck) | None => {}
                    Some(action) => return Err(action),
                },
            }
        }
    }

    /// Publishes the failure and stays around so the torrent can still be removed, and its
    /// resume file (and data, if asked) with it; a task that simply returned here would leave
    /// the entry unremovable and the resume file to resurrect it at the next start.
    ///
    /// With a resume file to go back to, unpause or recheck tries again (say the drive the
    /// files are on wasn't mounted yet): returns the file, and whether it was a recheck.
    async fn fail(&mut self, error: String, owned: &Owned) -> Option<(PathBuf, bool)> {
        tracing::warn!("{}: {error}", self.source);
        self.bus.emit(Event::TorrentFailed {
            source: self.source.clone(),
            error: error.clone(),
        });
        let holds = match owned {
            Owned::Nothing => None,
            Owned::Torrent { torrent, .. } => Some(torrent.info_hash),
            Owned::Unresolved => self.info_hash,
        };
        let _ = self.phase.send(Phase::Failed { error, holds });
        let retry_from = match owned {
            Owned::Nothing => None,
            Owned::Torrent { torrent, .. } => Some(self.resume_dir.join(ResumeData::file_name(&torrent.info_hash))),
            Owned::Unresolved => self.resume_path.clone().or_else(|| {
                self.info_hash
                    .map(|info_hash| self.resume_dir.join(ResumeData::file_name(&info_hash)))
            }),
        };
        loop {
            match self.controls.next(None).await {
                Some(Action::Remove { delete_files }) => {
                    match owned {
                        Owned::Nothing => {}
                        Owned::Torrent { torrent, root } => self.remove_files(torrent, root, delete_files).await,
                        Owned::Unresolved => self.remove_unresolved(),
                    }
                    return None;
                }
                Some(Action::Shutdown) => return None,
                Some(action @ (Action::Unpause | Action::Recheck)) => {
                    if let Some(path) = retry_from.as_ref().filter(|path| path.exists()) {
                        return Some((path.clone(), action == Action::Recheck));
                    }
                }
                Some(Action::Pause) | None => {}
            }
        }
    }

    /// The resume file of a torrent that failed before it was known goes with it. One that
    /// doesn't even parse is moved aside instead, out of the way of the next start but kept
    /// for whoever wants to look at it.
    fn remove_unresolved(&self) {
        if let Some(info_hash) = self.info_hash {
            let _ = std::fs::remove_file(self.resume_dir.join(ResumeData::file_name(&info_hash)));
        } else if let Some(path) = self.resume_path.as_ref().filter(|path| path.exists()) {
            let aside = path.with_extension(format!("{}.bad", crate::resume::EXTENSION));
            match std::fs::rename(path, &aside) {
                Ok(()) => tracing::info!("moved {} aside to {}", path.display(), aside.display()),
                Err(e) => tracing::warn!("couldn't move {} aside: {e}", path.display()),
            }
        }
    }

    /// The resume file, and with `delete_files` the data, which can be a big tree and is
    /// deleted on the blocking pool.
    async fn remove_files(&self, torrent: &Torrent, root: &Path, delete_files: bool) {
        let _ = std::fs::remove_file(self.resume_dir.join(ResumeData::file_name(&torrent.info_hash)));
        if delete_files {
            let data = root.join(torrent.top_level());
            let deleted = tokio::task::spawn_blocking(move || {
                let deleted = if data.is_dir() {
                    std::fs::remove_dir_all(&data)
                } else {
                    std::fs::remove_file(&data)
                };
                if let Err(e) = deleted {
                    tracing::warn!("couldn't delete {}: {e}", data.display());
                }
            });
            let _ = deleted.await;
        }
    }

    /// Puts the torrent in the client and keeps its resume file current until something
    /// stops it, then takes it out again. `verified` is updated to what's on disk by then.
    /// Returns the last stats of the stretch, if it got as far as running.
    async fn run_until_stopped(
        &mut self,
        torrent: &Arc<Torrent>,
        root: &Path,
        verified: &mut BitBox<u8, Msb0>,
        (resumed, checked): (&mut bool, &mut bool),
        peers: &mut Vec<std::net::SocketAddr>,
    ) -> anyhow::Result<(Action, Option<TorrentSwarmStats>)> {
        // held while downloading; released on completion so seeding never counts
        let mut slot = match verified.all() {
            true => None,
            false => match self.wait_for_slot(torrent, root, verified).await {
                Ok(permit) => Some(permit),
                Err(action) => return Ok((action, None)),
            },
        };
        // one paused before it ever ran has no files to pick up, and one just checked has had
        // what's missing measured: either is laid down like a new torrent, provided its root is
        // there (and not on a drive that isn't mounted)
        let adding = {
            let (client, torrent, root) = (self.client.clone(), (**torrent).clone(), root.to_path_buf());
            let (resumed, checked, verified) = (*resumed, *checked, verified.clone());
            tokio::task::spawn_blocking(move || {
                let nothing_to_pick_up = (checked || verified.not_any()) && root.is_dir();
                if resumed && !nothing_to_pick_up {
                    client.add_torrent_resumed(torrent, &root, verified)
                } else {
                    client.add_torrent_checked(torrent, &root, verified)
                }
            })
        };
        adding.await??;
        // whatever happens next, the files exist
        (*resumed, *checked) = (true, false);
        let info_hash = &torrent.info_hash;
        self.client.add_peers(info_hash, std::mem::take(peers));
        let selected = self.controls.selected.borrow().clone();
        if selected.iter().any(|s| !s) {
            self.client.select_files(info_hash, selected);
        }
        let modes = *self.controls.modes.borrow();
        if modes.sequential {
            self.client.set_sequential(info_hash, true);
        }
        if modes.super_seed {
            self.client.set_super_seed(info_hash, true);
        }
        let Some(stats) = self.client.stats(torrent) else {
            self.client.remove_torrent(info_hash);
            anyhow::bail!("torrent was added but reported no stats");
        };
        let peers = self.client.peers(torrent).unwrap_or_else(|| watch::channel(vec![]).1);
        let trackers = self
            .client
            .trackers(torrent)
            .unwrap_or_else(|| watch::channel(vec![]).1);
        let stop_saving = self.controls.cancel.child_token();
        let saver = tokio::spawn(keep_saving(
            torrent.clone(),
            root.to_path_buf(),
            stats.clone(),
            self.resume_inputs(),
            self.resume_dir.clone(),
            stop_saving.clone(),
        ));
        let _ = self.phase.send(Phase::Downloading {
            shown: self.shown(torrent, root),
            stats: stats.clone(),
            peers,
            trackers,
        });
        self.bus.emit(Event::TorrentStarted {
            info_hash: *info_hash,
            verified: verified.count_ones(),
            pieces: verified.len(),
        });

        let mut ratio_stats = stats.clone();
        // one that starts complete finished some other time
        let mut completion_told = ratio_stats.borrow().completed;
        let action = loop {
            // the swarm stopped itself: Unpause retries once the disk has room again
            if let Some(e) = ratio_stats.borrow().storage_error.clone() {
                break Err(anyhow::anyhow!("couldn't write the files: {e}"));
            }
            if ratio_stats.borrow().completed {
                drop(slot.take());
                if !completion_told {
                    completion_told = true;
                    self.bus.emit(Event::TorrentCompleted { info_hash: *info_hash });
                }
            }
            // a torrent that comes back already over its ratio stops before waiting for
            // anything to change
            if self.seeded_enough(torrent, &ratio_stats.borrow_and_update()) {
                break Ok(Action::Pause);
            }
            tokio::select! {
                action = self.controls.next(Some((&self.client, info_hash))) => match action {
                    Some(Action::Unpause) | None => {}
                    Some(action) => break Ok(action),
                },
                // the stats change on every verified piece and uploaded block, the settings
                // when the user edits the limit; either can be what tips the ratio over
                changed = ratio_stats.changed() => {
                    // the swarm's end closes its stats; one the session didn't stop died
                    if changed.is_err() {
                        break match self.controls.cancel.is_cancelled() {
                            true => Ok(Action::Shutdown),
                            false => Err(anyhow::anyhow!("the torrent stopped running unexpectedly")),
                        };
                    }
                    if self.seeded_enough(torrent, &ratio_stats.borrow()) {
                        break Ok(Action::Pause);
                    }
                }
                _ = self.settings.changed() => {
                    if self.seeded_enough(torrent, &stats.borrow()) {
                        break Ok(Action::Pause);
                    }
                }
            }
        };
        // the torrent leaves the client; the saver gets its final write in before anything
        // is deleted, or the deletion would race it
        self.client.remove_torrent(info_hash);
        stop_saving.cancel();
        if let Ok(persisted) = saver.await {
            self.persisted = persisted;
        }
        let last = stats.borrow().clone();
        *verified = last.verified.clone();
        self.uploaded_before += last.uploaded;
        Ok((action?, Some(last)))
    }

    /// Waits for an active-download slot, showing the torrent as queued meanwhile.
    async fn wait_for_slot(
        &mut self,
        torrent: &Arc<Torrent>,
        root: &Path,
        verified: &BitBox<u8, Msb0>,
    ) -> Result<SlotPermit, Action> {
        let slots = self.slots.clone();
        let acquire = slots.acquire();
        tokio::pin!(acquire);
        self.bus.emit(Event::TorrentQueued {
            info_hash: torrent.info_hash,
        });
        loop {
            // again after a selection change, which changes what's wanted
            let wanted = torrent.wanted_pieces(&self.controls.selected.borrow());
            let _ = self.phase.send(Phase::Queued {
                shown: self.shown(torrent, root),
                stats: TorrentSwarmStats::for_verified(torrent, verified.clone(), wanted),
            });
            tokio::select! {
                permit = &mut acquire => return Ok(permit),
                action = self.controls.next(None) => match action {
                    Some(Action::Unpause) | None => {}
                    Some(action) => return Err(action),
                },
            }
        }
    }

    /// Polls the key the torrent follows (BEP 46), if it follows one, until it names a newer
    /// version, which goes to the session to add; once that resolves, this torrent is marked
    /// superseded and only seeds. Stops when the returned guard is dropped.
    fn follow(&self, torrent: &Arc<Torrent>, root: &Path, soon: bool) -> Option<tokio_util::sync::DropGuard> {
        let feed = self.feed.borrow().clone().filter(|f| f.superseded.is_none())?;
        let stop = CancellationToken::new();
        let (dht, feed_tx, updates, bus) = (
            self.client.dht(),
            self.feed.clone(),
            self.updates.clone(),
            self.bus.clone(),
        );
        let (torrent, root) = (torrent.clone(), root.to_path_buf());
        let (selected, modes) = (self.controls.selected.subscribe(), self.controls.modes.subscribe());
        let follow = async move {
            let set = {
                let feed_tx = feed_tx.clone();
                move |feed| {
                    feed_tx.send_replace(Some(feed));
                }
            };
            let schedule = crate::feed::Schedule::standard(!soon);
            let found = crate::feed::follow(dht, feed_tx.subscribe(), set, torrent.info_hash, schedule).await?;
            tracing::info!(
                "{}: its BEP 46 key {} is at seq {} now, naming {}; adding that",
                torrent.name,
                feed.key.public_hex(),
                found.seq,
                found.version.info_hash
            );
            bus.emit(Event::TorrentUpdateFound {
                info_hash: torrent.info_hash,
                update: found.version.info_hash,
                seq: found.seq,
            });
            let _ = updates.send(FeedUpdate {
                previous: torrent,
                root,
                key: feed.key,
                found,
                previous_feed: feed_tx,
                selected: selected.borrow().clone(),
                modes: *modes.borrow(),
            });
            Some(())
        };
        tokio::spawn(stop.clone().run_until_cancelled_owned(follow));
        Some(stop.drop_guard())
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

    /// Shows the torrent paused, with the counters of the stretch that just ended if there
    /// was one, and keeps its resume file marked paused (so a restart brings it back paused)
    /// and up to date with the switches, until another action comes.
    async fn wait_while_paused(
        &mut self,
        torrent: &Arc<Torrent>,
        root: &Path,
        verified: &BitBox<u8, Msb0>,
        last_stats: Option<TorrentSwarmStats>,
    ) -> Action {
        let mut stats = last_stats.unwrap_or_else(|| {
            let wanted = torrent.wanted_pieces(&self.controls.selected.borrow());
            TorrentSwarmStats::for_verified(torrent, verified.clone(), wanted)
        });
        // already folded into `uploaded_before`
        stats.uploaded = 0;
        self.bus.emit(Event::TorrentPaused {
            info_hash: torrent.info_hash,
        });
        let mut feed = self.feed.subscribe();
        loop {
            let _ = self.phase.send(Phase::Paused {
                shown: self.shown(torrent, root),
                stats: stats.clone(),
            });
            let saved = self.save_paused(torrent, root, verified).await;
            tokio::select! {
                action = self.controls.next(None) => match action {
                    Some(Action::Pause) | None => {}
                    Some(Action::Shutdown) if !saved => {
                        self.save_paused(torrent, root, verified).await;
                        return Action::Shutdown;
                    }
                    Some(action) => return action,
                },
                // the key moved on while paused
                Ok(()) = feed.changed() => {}
                _ = tokio::time::sleep(RESAVE), if !saved => {}
            }
        }
    }

    /// Writes the resume file marked paused; returns whether that worked.
    async fn save_paused(&mut self, torrent: &Arc<Torrent>, root: &Path, verified: &BitBox<u8, Msb0>) -> bool {
        let saved = crate::resume::save_paused(torrent, root, &self.resume_dir, self.resume_inputs(), verified).await;
        if let Some(persisted) = &saved {
            self.persisted = persisted.clone();
        }
        saved.is_some()
    }
}

/// What the session shows of a known torrent whatever it's doing. The receivers follow the
/// user's switches as they flip.
#[derive(Clone)]
struct Shown {
    torrent: Arc<Torrent>,
    root: PathBuf,
    selected: watch::Receiver<Vec<bool>>,
    modes: watch::Receiver<Modes>,
    feed: watch::Receiver<Option<Feed>>,
    /// see `TorrentTask::uploaded_before`
    uploaded_before: u64,
}

impl Shown {
    fn progress(&self, stats: &TorrentSwarmStats, rates: &Rates) -> Progress {
        let torrent = &self.torrent;
        let selected = self.selected.borrow();
        let modes = *self.modes.borrow();
        Progress {
            info_hash: torrent.info_hash.to_string(),
            name: torrent.name.clone(),
            root: self.root.display().to_string(),
            files: torrent
                .files
                .iter()
                .enumerate()
                .map(|(i, file)| FileInfo {
                    path: file.path.display().to_string(),
                    size: file.len,
                    selected: selected.get(i).copied().unwrap_or(true),
                    pad: file.attr.pad,
                })
                .collect(),
            total_size: torrent.total_size,
            downloaded: stats.downloaded,
            wasted: stats.wasted,
            uploaded: self.uploaded_before + stats.uploaded,
            left: stats.left as u64,
            verified_pieces: stats.verified_cnt(),
            total_pieces: stats.total_pieces(),
            completed: stats.completed,
            download_bps: rates.download_bps,
            upload_bps: rates.upload_bps,
            peers: vec![],
            sequential: modes.sequential,
            super_seed: modes.super_seed,
            trackers: vec![],
            feed: self.feed.borrow().clone(),
        }
    }
}

/// What a torrent's task publishes; `Session::torrents` turns this into a `TorrentState`.
#[derive(Clone)]
enum Phase {
    Resolving {
        started: Instant,
    },
    Downloading {
        shown: Shown,
        stats: watch::Receiver<TorrentSwarmStats>,
        peers: watch::Receiver<Vec<PeerSnapshot>>,
        trackers: watch::Receiver<Vec<TrackerStatus>>,
    },
    Paused {
        shown: Shown,
        /// the last stats before the swarm was stopped
        stats: TorrentSwarmStats,
    },
    Queued {
        shown: Shown,
        stats: TorrentSwarmStats,
    },
    Checking {
        torrent: Arc<Torrent>,
        /// pieces hashed so far
        checked: watch::Receiver<usize>,
    },
    Failed {
        error: String,
        /// the info hash it has to itself, if it got as far as knowing it and isn't a duplicate
        holds: Option<InfoHash>,
    },
}

impl Phase {
    fn torrent(&self) -> Option<&Arc<Torrent>> {
        match self {
            Phase::Downloading { shown, .. } | Phase::Paused { shown, .. } | Phase::Queued { shown, .. } => {
                Some(&shown.torrent)
            }
            Phase::Checking { torrent, .. } => Some(torrent),
            Phase::Resolving { .. } | Phase::Failed { .. } => None,
        }
    }
}

/// The whole session at a glance, for a status bar.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SessionStatus {
    /// summed over every torrent
    pub download_bps: f64,
    pub upload_bps: f64,
    /// nodes in the DHT routing table; `None` with no node (off, or not up yet)
    pub dht_nodes: Option<usize>,
    pub listen_port: u16,
    /// as its name alone: "off", "searching", "mapped" or "unavailable"
    #[serde(serialize_with = "json::mapping_state")]
    pub port_mapping: MappingState,
    /// our public address: what the gateway says, else what peers and trackers report
    /// (BEP 10 `yourip`, BEP 24) once two of them agree
    pub external_ip: Option<std::net::IpAddr>,
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
    dht: Option<Dht>,
    /// active-download slots, `Settings::max_active_downloads` of them
    slots: Arc<Slots>,
    claimed: Claimed,
    /// every torrent's task that may still be running, so shutdown can wait for their last
    /// resume writes
    tasks: Vec<JoinHandle<()>>,
    events: EventBus,
    data_dir: PathBuf,
    settings: watch::Sender<Settings>,
    /// the exclusive lock on `<data dir>/lock`, held until shutdown
    lock: Option<File>,
    /// newer versions of torrents that follow a DHT key, added by `torrents`
    updates: (mpsc::UnboundedSender<FeedUpdate>, mpsc::UnboundedReceiver<FeedUpdate>),
}

/// `Session::new`'s error when another session, in this process or another (the GUI and the
/// command line share a data directory), already uses the data directory. Two sessions would
/// download into the same files, write the same resume files, and share one DHT database.
#[derive(Debug)]
pub struct AlreadyRunning {
    pub data_dir: PathBuf,
}

impl std::fmt::Display for AlreadyRunning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "another downloader (the app or the command line) is already using {}; quit it first",
            self.data_dir.display()
        )
    }
}

impl std::error::Error for AlreadyRunning {}

/// Takes the exclusive advisory lock on `<data dir>/lock`, which goes with the process if it
/// dies. A file system that can't lock at all only gets a warning: refusing to start there
/// would be worse than the risk the lock guards against.
fn lock_data_dir(data_dir: &Path) -> anyhow::Result<Option<File>> {
    let path = data_dir.join("lock");
    let file = File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Err(AlreadyRunning {
            data_dir: data_dir.to_path_buf(),
        }
        .into()),
        Err(TryLockError::Error(e)) => {
            tracing::warn!("couldn't lock {}, carrying on without: {e}", path.display());
            Ok(None)
        }
    }
}

/// How long shutdown waits for the torrents' last resume writes, and then for the trackers'
/// `stopped` announces and the like.
const SAVE_BUDGET: Duration = Duration::from_secs(10);
/// How soon a paused torrent's resume write that failed is tried again.
const RESAVE: Duration = Duration::from_secs(30);
const ANNOUNCE_BUDGET: Duration = Duration::from_secs(2);

/// What a session needs to start.
pub struct SessionConfig {
    pub peer_id: [u8; 20],
    /// where the session keeps its own files: resume data, the DHT database, and the
    /// settings. See `paths::data_dir` for the usual answer.
    pub data_dir: PathBuf,
    /// the settings to start with; `Settings::load(&data_dir)` for what the user saved
    pub settings: Settings,
}

/// Every peer connection is a socket and every open file a descriptor, and with no peer cap a
/// busy session holds thousands. macOS hands an app launched from Finder a soft limit of 256,
/// so lift the soft limit to the hard one (as far as the kernel's per-process maximum allows).
fn raise_fd_limit() {
    match rlimit::increase_nofile_limit(u64::MAX) {
        Ok(n) => tracing::info!("file descriptor limit {n}"),
        Err(e) => tracing::warn!("couldn't raise the file descriptor limit: {e}"),
    }
}

impl Session {
    /// Creates a session with its own tokio runtime. Progress goes to `<data dir>/resume/<info
    /// hash>.resume` for every torrent; finding those files again and handing them to
    /// [`Session::resume`] is the caller's job, see `Session::resume_dir` and
    /// `list_resume_files`.
    ///
    /// Only one session at a time can use a data directory: another one there, in this process
    /// or another, makes this fail with [`AlreadyRunning`].
    pub fn new(config: SessionConfig) -> anyhow::Result<Self> {
        raise_fd_limit();
        let resume_dir = config.data_dir.join("resume");
        std::fs::create_dir_all(&resume_dir)?;
        let lock = lock_data_dir(&config.data_dir)?;
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
        let events = EventBus::new();
        // the client and the DHT node start on whatever runtime is current
        let (client, dht) = {
            let _on_runtime = rt.enter();
            let dht = config.settings.dht.then(|| {
                Dht::start(
                    config.data_dir.join("dht.db"),
                    config.settings.dht_port(),
                    config.settings.dht_read_only,
                    events.clone(),
                )
            });
            let watch = dht.as_ref().map_or_else(Dht::none, Dht::watch);
            (
                BtClient::new_with_shutdown(identity, shutdown.clone(), watch, settings_rx, events.clone()),
                dht,
            )
        };
        Ok(Self {
            events,
            slots: Slots::new(slot_count(&config.settings)),
            claimed: Claimed::default(),
            tasks: vec![],
            handle: rt.handle().clone(),
            rt: Some(rt),
            identity: Arc::new(identity),
            client,
            shutdown,
            torrents: BTreeMap::new(),
            next_id: 1,
            resume_dir,
            dht,
            data_dir: config.data_dir,
            settings: settings_tx,
            lock,
            updates: mpsc::unbounded_channel(),
        })
    }

    /// Everything the session does, as it happens; see `events::Event`. Subscribers get
    /// what's emitted from the moment they subscribe.
    pub fn subscribe(&self) -> Events {
        self.events.subscribe()
    }

    pub fn events(&self) -> EventBus {
        self.events.clone()
    }

    pub fn settings(&self) -> Settings {
        self.settings.borrow().clone()
    }

    /// Saves and applies new settings. The connection cap and rate limits take effect at
    /// once; others (the listen port, the DHT switch) at the next start, which is the caller's
    /// to arrange: returns whether any of those changed (see `Settings::restart_needed`).
    pub fn update_settings(&mut self, settings: Settings) -> anyhow::Result<bool> {
        settings.save(&self.data_dir)?;
        let restart = settings.restart_needed(&self.settings.borrow());
        self.slots.set_limit(slot_count(&settings));
        let _ = self.settings.send(settings);
        Ok(restart)
    }

    /// BEP 46: points `signing`'s DHT item under `salt` at `version`, so whoever follows the
    /// key moves on to it. Waits for the DHT node (up to `wait`), so it blocks; not to be called
    /// on an async runtime.
    pub fn publish_update(
        &self,
        signing: &midwest_mainline::dht::item::SigningKey,
        salt: &[u8],
        version: &crate::feed::Version,
        wait: Duration,
    ) -> anyhow::Result<crate::feed::Published> {
        let dht = self.client.dht();
        self.handle.block_on(async {
            let clients = tokio::time::timeout(wait, crate::feed::clients(dht))
                .await
                .map_err(|_| anyhow::anyhow!("the DHT node wasn't up after {}s", wait.as_secs()))??;
            crate::feed::publish(&clients, signing, salt, version).await
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
    ///
    /// A magnet for a torrent whose entry has failed is taken as asking for it again: an
    /// entry with a resume file is rechecked (downloading whatever went missing, in its own
    /// root) and its id returned; one without is dropped, its files left alone, for the new one.
    pub fn add(&mut self, source: impl Into<String>, root: impl Into<PathBuf>) -> TorrentId {
        let source = source.into();
        let root = root.into();
        if let Ok(Some(key)) = crate::magnet::parse_feed(&source) {
            return self.add_feed(source, key, root);
        }
        let magnet = crate::magnet::parse_magnet(&source).ok();
        let info_hash = magnet.as_ref().map(|m| m.info_hash);
        if let Some(info_hash) = info_hash
            && let Some(failed) = self.failed_holder(info_hash)
        {
            if self.resume_dir.join(ResumeData::file_name(&info_hash)).exists() {
                self.recheck(failed);
                return failed;
            }
            if let Some(entry) = self.torrents.remove(&failed) {
                self.claimed.leaving(failed);
                let _ = entry.commands.send(Command::Act(Action::Shutdown));
            }
        }
        let loader = self.loader();
        self.launch(source.clone(), info_hash, None, |cancel| async move {
            let mut resolved = Resolved::new(loader.load(&source, cancel).await?, root);
            if let Some(magnet) = magnet {
                resolved.selected = magnet.selection(resolved.torrent.files.len());
            }
            Ok(resolved)
        })
    }

    /// `add` for a BEP 46 magnet: the torrent is whatever the key's DHT item names (or the
    /// magnet's own `xt` while the DHT has nothing), and it follows the key from then on.
    fn add_feed(&mut self, source: String, key: FeedKey, root: PathBuf) -> TorrentId {
        let fallback = crate::magnet::parse_magnet(&source).ok().map(|m| crate::feed::Version {
            info_hash: m.info_hash,
            info_hash_v2: m.info_hash_v2,
        });
        let loader = self.loader();
        // the key decides which torrent this is, so nothing is claimed before it has
        self.launch(source.clone(), None, None, move |cancel| async move {
            let (found, version) = crate::feed::resolve(loader.dht.clone(), &key, fallback, cancel.clone()).await?;
            let magnet_uri = crate::magnet::with_version(&source, &version);
            let magnet = crate::magnet::parse_magnet(&magnet_uri)?;
            let mut resolved = Resolved::new(loader.load(&magnet_uri, cancel).await?, root);
            resolved.selected = magnet.selection(resolved.torrent.files.len());
            resolved.feed = Some(Feed {
                key,
                seq: found.map(|f| f.seq),
                superseded: None,
            });
            Ok(resolved)
        })
    }

    /// Adds the newer version a followed key named, next to its predecessor (in a directory of
    /// its own if the names clash), starting from whichever of the predecessor's files it
    /// shares. Nothing happens if the session has it already.
    fn add_update(&mut self, update: FeedUpdate) -> Option<TorrentId> {
        let FeedUpdate {
            previous,
            root,
            key,
            found,
            previous_feed,
            selected,
            modes,
        } = update;
        let supersede = move |seq| {
            previous_feed.send_modify(|feed| {
                if let Some(feed) = feed {
                    feed.superseded = Some(seq);
                }
            })
        };
        let info_hash = found.version.info_hash;
        if self.torrents.values().any(|e| e.info_hash() == Some(info_hash)) {
            tracing::info!("{info_hash}, the BEP 46 update of {}, is here already", previous.name);
            supersede(found.seq);
            return None;
        }
        let source = crate::feed::magnet_uri(
            &key,
            Some(&found.version),
            Some(&previous.name),
            &previous.all_trackers(),
        );
        let feed = Feed {
            key,
            seq: Some(found.seq),
            superseded: None,
        };
        let loader = self.loader();
        Some(
            self.launch(source.clone(), Some(info_hash), None, move |cancel| async move {
                let Loaded { torrent, peers } = loader.load(&source, cancel).await?;
                let seq = found.seq;
                // the files it shares with its predecessor are found by the check that a
                // torrent with data on disk starts with
                let (torrent, root, previous) = tokio::task::spawn_blocking(move || {
                    let root = place_update(&previous, &root, &torrent, seq)?;
                    anyhow::Ok((torrent, root, previous))
                })
                .await??;
                supersede(seq);
                Ok(Resolved {
                    selected: carried_selection(&previous, &selected, &torrent),
                    modes,
                    feed: Some(feed),
                    ..Resolved::new(Loaded { torrent, peers }, root)
                })
            }),
        )
    }

    /// Picks a download back up from a resume file (see `ResumeData`), in the root it was
    /// started in. A torrent that was paused when its file was last written comes back paused.
    /// One that fails, even for a file that doesn't parse, stays listed as failed, and unpause
    /// or recheck tries the file again.
    pub fn resume(&mut self, path: impl AsRef<Path>) -> TorrentId {
        let path = path.as_ref().to_path_buf();
        let info_hash = ResumeSummary::read(&path).ok().map(|s| s.info_hash);
        self.resume_known(path, info_hash)
    }

    /// `resume` for a file whose info hash has been read already, if it could be.
    fn resume_known(&mut self, path: PathBuf, info_hash: Option<InfoHash>) -> TorrentId {
        self.launch(
            path.display().to_string(),
            info_hash,
            Some(path.clone()),
            |_cancel| async move { tokio::task::spawn_blocking(move || Resolved::read(&path)).await? },
        )
    }

    fn loader(&self) -> Loader {
        Loader {
            identity: self.identity.clone(),
            dht: self.client.dht(),
            utp: self.client.utp(),
            bus: self.events.clone(),
        }
    }

    /// Resumes every torrent that has a resume file in this session's resume dir and isn't
    /// already in the session. What a client does at startup. A file that doesn't parse
    /// becomes a failed entry with the reason, rather than the torrent silently vanishing.
    pub fn resume_all(&mut self) -> Vec<TorrentId> {
        let sources: HashSet<String> = self.torrents.values().map(|e| e.source.clone()).collect();
        let running: HashSet<InfoHash> = self.torrents.values().filter_map(Entry::info_hash).collect();
        let (readable, broken): (Vec<_>, Vec<_>) = scan_resume_files(&self.resume_dir)
            .into_iter()
            .partition(|(_, read)| read.is_ok());
        let mut readable: Vec<ResumeSummary> = readable
            .into_iter()
            .filter_map(|(_, read)| read.ok())
            .filter(|summary| !running.contains(&summary.info_hash))
            .collect();
        readable.sort_by(|a, b| a.name.cmp(&b.name));
        let mut ids: Vec<TorrentId> = readable
            .into_iter()
            .map(|summary| self.resume_known(summary.path, Some(summary.info_hash)))
            .collect();
        for (path, _) in broken {
            if !sources.contains(&path.display().to_string()) {
                ids.push(self.resume_known(path, None));
            }
        }
        ids
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
        self.command(id, Command::Act(Action::Pause));
    }

    pub fn unpause(&mut self, id: TorrentId) {
        self.command(id, Command::Act(Action::Unpause));
    }

    /// Re-hashes the torrent's files and continues from what's actually on disk, in the
    /// state (downloading or paused) it was in. For when the resume data can't be trusted.
    pub fn recheck(&mut self, id: TorrentId) {
        self.command(id, Command::Act(Action::Recheck));
    }

    /// Downloads only the selected files from now on: one flag per file in the order
    /// `Progress::files` lists them. Pieces shared with a selected file are still fetched.
    pub fn select_files(&mut self, id: TorrentId, selected: Vec<bool>) {
        self.command(id, Command::Set(Switch::Files(selected)));
    }

    /// Fetches pieces in order rather than rarest first, so a file can be played while it
    /// downloads. Remembered across restarts.
    pub fn set_sequential(&mut self, id: TorrentId, on: bool) {
        self.command(id, Command::Set(Switch::Sequential(on)));
    }

    /// BEP 16 super-seeding: once the torrent is complete, each newly connected peer is shown
    /// one piece at a time, and the next only after it passed the last one on. For the first
    /// seeder of a torrent, it spreads the pieces with less of its own upload. Unknown ids are
    /// ignored.
    pub fn set_super_seed(&mut self, id: TorrentId, on: bool) {
        self.command(id, Command::Set(Switch::SuperSeed(on)));
    }

    /// Removes a torrent: its connections close and its resume file is deleted, and with
    /// `delete_files` so is everything it downloaded. The entry is gone from
    /// [`Session::torrents`] immediately; the deletion itself finishes in the background.
    /// Unknown ids are ignored.
    pub fn remove(&mut self, id: TorrentId, delete_files: bool) {
        if let Some(entry) = self.torrents.remove(&id) {
            self.claimed.leaving(id);
            let _ = entry.commands.send(Command::Act(Action::Remove { delete_files }));
        }
    }

    /// The failed entry that has `info_hash` to itself, if one does.
    fn failed_holder(&self, info_hash: InfoHash) -> Option<TorrentId> {
        self.torrents
            .iter()
            .find_map(|(id, entry)| match &*entry.phase.borrow() {
                Phase::Failed { holds, .. } if *holds == Some(info_hash) => Some(*id),
                _ => None,
            })
    }

    fn command(&mut self, id: TorrentId, command: Command) {
        if let Some(entry) = self.torrents.get(&id) {
            let _ = entry.commands.send(command);
        }
    }

    /// Shared tail of `add`/`resume`: `resolve` produces the torrent, where its files go,
    /// which pieces are already had, and whether the target files are expected to exist. The
    /// task it spawns owns the torrent for the rest of its life, including its removal.
    fn launch<F, Fut>(
        &mut self,
        source: String,
        info_hash: Option<InfoHash>,
        resume_path: Option<PathBuf>,
        resolve: F,
    ) -> TorrentId
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<Resolved>> + Send + 'static,
    {
        let id = self.next_id;
        self.next_id += 1;

        let (phase_tx, phase_rx) = watch::channel(Phase::Resolving {
            started: Instant::now(),
        });
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        self.torrents.insert(
            id,
            Entry {
                source: source.clone(),
                info_hash,
                phase: phase_rx,
                commands: commands_tx,
                rates: Rates::new(),
                peer_rates: HashMap::new(),
            },
        );

        let task = TorrentTask {
            id,
            client: self.client.clone(),
            bus: self.events.clone(),
            source,
            resume_dir: self.resume_dir.clone(),
            phase: phase_tx,
            controls: Controls {
                commands: commands_rx,
                cancel: self.shutdown.child_token(),
                selected: watch::channel(vec![]).0,
                modes: watch::channel(Modes::default()).0,
            },
            slots: self.slots.clone(),
            info_hash,
            resume_path,
            claimed: self.claimed.clone(),
            settings: self.settings.subscribe(),
            uploaded_before: 0,
            persisted: BitBox::default(),
            feed: watch::channel(None).0,
            updates: self.updates.0.clone(),
        };
        // a session that runs for weeks sees many torrents come and go
        self.tasks.retain(|task| !task.is_finished());
        self.tasks.push(self.handle.spawn(task.run(resolve)));
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
        let port_mapping = self.client.port_mapping().borrow().clone();
        let gateway_ip = match port_mapping {
            MappingState::Mapped { external_ip } => external_ip,
            _ => None,
        };
        SessionStatus {
            download_bps,
            upload_bps,
            dht_nodes: self.client.dht().borrow().as_ref().map(|dht| dht.node_count()),
            listen_port: self.identity.serving.port(),
            port_mapping: port_mapping.clone(),
            external_ip: gateway_ip.or_else(|| self.client.external_address()),
        }
    }

    /// The current state of every torrent, ready to render. Cheap enough to call every frame.
    ///
    /// Also where the newer versions that followed DHT keys found (BEP 46) join the session.
    pub fn torrents(&mut self) -> Vec<(TorrentId, TorrentState)> {
        while let Ok(update) = self.updates.1.try_recv() {
            self.add_update(update);
        }
        self.torrents
            .iter_mut()
            .map(|(id, entry)| (*id, entry.state()))
            .collect()
    }

    /// Stops everything, keeping files and resume data, and shuts the runtime down. Every
    /// torrent's last resume write is waited for, then the trackers get a moment for their
    /// `stopped` announces; both waits are bounded so a hung disk or an unreachable tracker
    /// can't stall process exit. Releases the data directory for the next session.
    pub fn shutdown(&mut self) {
        self.shutdown.cancel();
        self.torrents.clear();
        if let Some(rt) = self.rt.take() {
            let tasks = std::mem::take(&mut self.tasks);
            rt.block_on(async {
                if tokio::time::timeout(SAVE_BUDGET, futures::future::join_all(tasks))
                    .await
                    .is_err()
                {
                    tracing::warn!("gave up waiting for the torrents to save their progress");
                }
            });
            // the node has no goodbye to say, and its tasks would keep the wait below going
            self.dht.take();
            let metrics = rt.metrics();
            rt.block_on(async {
                let deadline = Instant::now() + ANNOUNCE_BUDGET;
                while metrics.num_alive_tasks() > 0 && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            });
            rt.shutdown_timeout(Duration::from_millis(500));
        }
        self.lock.take();
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
            Phase::Failed { holds, .. } => *holds,
            phase => phase.torrent().map(|t| t.info_hash),
        })
    }

    fn state(&mut self) -> TorrentState {
        let phase = self.phase.borrow_and_update().clone();
        match phase {
            Phase::Failed { error, .. } => TorrentState::Failed {
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
                total_pieces: torrent.num_pieces(),
            },
            Phase::Downloading {
                shown,
                stats,
                peers,
                trackers,
            } => {
                let stats = stats.borrow().clone();
                self.rates.update(stats.downloaded, stats.uploaded);
                let mut progress = shown.progress(&stats, &self.rates);
                progress.peers = self.peers(&peers.borrow());
                progress.trackers = trackers.borrow().iter().map(tracker_info).collect();
                TorrentState::Downloading(progress)
            }
            Phase::Paused { shown, stats } => TorrentState::Paused(self.idle(&shown, &stats)),
            Phase::Queued { shown, stats } => TorrentState::Queued(self.idle(&shown, &stats)),
        }
    }

    /// The progress of a torrent that isn't running, which moves nothing.
    fn idle(&mut self, shown: &Shown, stats: &TorrentSwarmStats) -> Progress {
        self.rates = Rates::new();
        self.peer_rates.clear();
        shown.progress(stats, &self.rates)
    }

    fn peers(&mut self, snapshots: &[PeerSnapshot]) -> Vec<PeerInfo> {
        self.peer_rates
            .retain(|addr, _| snapshots.iter().any(|p| p.addr == *addr));
        snapshots
            .iter()
            .map(|p| {
                let rates = self.peer_rates.entry(p.addr).or_insert_with(Rates::new);
                rates.update(p.downloaded, p.uploaded);
                PeerInfo {
                    addr: p.web_seed.clone().unwrap_or_else(|| p.addr.to_string()),
                    client: p.client.clone(),
                    progress: p.progress,
                    downloaded: p.downloaded,
                    uploaded: p.uploaded,
                    download_bps: rates.download_bps,
                    upload_bps: rates.upload_bps,
                    choked_us: p.choked_us,
                    choked_them: p.choked_them,
                    interested_us: p.interested_us,
                    interested_them: p.interested_them,
                    encrypted: p.encrypted,
                    utp: p.utp,
                }
            })
            .collect()
    }
}

fn has_data_on_disk(torrent: &Torrent, root: &Path) -> bool {
    torrent
        .files
        .iter()
        .any(|file| !file.attr.pad && std::fs::metadata(root.join(&file.path)).is_ok_and(|m| m.len() > 0))
}

/// An update's file selection: the predecessor's choice for a file at the same path, and
/// selected for one it didn't have.
fn carried_selection(previous: &Torrent, selected: &[bool], torrent: &Torrent) -> Vec<bool> {
    let before: HashMap<&Path, bool> = previous
        .files
        .iter()
        .zip(selected)
        .map(|(file, on)| (file.path.as_path(), *on))
        .collect();
    torrent
        .files
        .iter()
        .map(|file| before.get(file.path.as_path()).copied().unwrap_or(true))
        .collect()
}

/// Where a BEP 46 update of `previous` (whose files are under `root`) goes: `root` too, unless
/// that already holds something by its name (its predecessor's files, most likely, which the
/// update would write its own pieces over while they seed). Then a directory of its own
/// beside them, `<name> (seq N)`. The predecessor's files the update has too, same path and
/// size, are copied over (a clone on APFS and the like) and, if there were any, the rest laid
/// down empty, for a check to sort out what is still good.
fn place_update(previous: &Torrent, root: &Path, torrent: &Torrent, seq: i64) -> std::io::Result<PathBuf> {
    let top = torrent.top_level();
    let mut new_root = root.to_path_buf();
    let mut attempt = 1;
    while new_root.join(&top).exists() {
        let suffix = if attempt == 1 {
            format!("(seq {seq})")
        } else {
            format!("(seq {seq}, {attempt})")
        };
        new_root = root.join(format!("{} {suffix}", top.display()));
        attempt += 1;
    }

    let real = |t: &Torrent| -> HashMap<PathBuf, u64> {
        t.files
            .iter()
            .filter(|file| !file.attr.virtual_file())
            .map(|file| (file.path.clone(), file.len))
            .collect()
    };
    let before = real(previous);
    let wanted = real(torrent);
    let mut reused = false;
    for (path, size) in &wanted {
        let from = root.join(path);
        if before.get(path) == Some(size) && std::fs::metadata(&from).is_ok_and(|m| m.len() == *size) {
            let to = new_root.join(path);
            if let Some(dir) = to.parent() {
                std::fs::create_dir_all(dir)?;
            }
            std::fs::copy(&from, &to)?;
            reused = true;
        }
    }
    if reused {
        for (path, size) in &wanted {
            let to = new_root.join(path);
            if !to.exists() {
                if let Some(dir) = to.parent() {
                    std::fs::create_dir_all(dir)?;
                }
                std::fs::File::create(&to)?.set_len(*size)?;
            }
        }
        tracing::info!(
            "{}: starting from the files it shares with {}, in {}",
            torrent.name,
            previous.name,
            new_root.display()
        );
    }
    Ok(new_root)
}

fn tracker_info(status: &TrackerStatus) -> TrackerInfo {
    TrackerInfo {
        url: status.url.clone(),
        state: status.state.clone(),
        peers: status.peers,
        next_announce_secs: status
            .next_announce
            .map(|at| at.saturating_duration_since(tokio::time::Instant::now()).as_secs()),
        seeders: status.swarm.seeders,
        leechers: status.swarm.leechers,
        downloaded: status.swarm.downloaded,
    }
}

/// `max_active_downloads` as a slot count, with 0 read as no limit.
fn slot_count(settings: &Settings) -> usize {
    match settings.max_active_downloads {
        0 => usize::MAX,
        n => n,
    }
}

/// The active-download slots. Unlike a semaphore's permits the limit can drop below the
/// slots in use: a running download keeps its slot, and nobody gets a new one until enough
/// have been given back. Waiters are served in the order they came.
struct Slots {
    state: std::sync::Mutex<SlotState>,
    changed: Notify,
}

struct SlotState {
    limit: usize,
    taken: usize,
    next_ticket: u64,
    /// tickets of the tasks waiting, the lowest first in line
    waiting: BTreeSet<u64>,
}

impl Slots {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::Mutex::new(SlotState {
                limit,
                taken: 0,
                next_ticket: 0,
                waiting: BTreeSet::new(),
            }),
            changed: Notify::new(),
        })
    }

    fn set_limit(&self, limit: usize) {
        self.state.lock().unwrap().limit = limit;
        self.changed.notify_waiters();
    }

    async fn acquire(self: &Arc<Self>) -> SlotPermit {
        let mut ticket = {
            let mut state = self.state.lock().unwrap();
            let ticket = state.next_ticket;
            state.next_ticket += 1;
            state.waiting.insert(ticket);
            Ticket {
                slots: self.clone(),
                ticket,
                served: false,
            }
        };
        loop {
            // registered before looking, so a change between the look and the wait isn't missed
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap();
                if state.taken < state.limit && state.waiting.first() == Some(&ticket.ticket) {
                    state.waiting.remove(&ticket.ticket);
                    state.taken += 1;
                    drop(state);
                    ticket.served = true;
                    // the next in line may fit as well
                    self.changed.notify_waiters();
                    return SlotPermit { slots: self.clone() };
                }
            }
            changed.await;
        }
    }

    #[cfg(test)]
    fn taken(&self) -> usize {
        self.state.lock().unwrap().taken
    }
}

/// A place in the line for a slot; leaving it (the wait was abandoned) lets the next one up.
struct Ticket {
    slots: Arc<Slots>,
    ticket: u64,
    served: bool,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if self.served {
            return;
        }
        self.slots.state.lock().unwrap().waiting.remove(&self.ticket);
        self.slots.changed.notify_waiters();
    }
}

/// One active-download slot, given back on drop.
struct SlotPermit {
    slots: Arc<Slots>,
}

impl Drop for SlotPermit {
    fn drop(&mut self) {
        self.slots.state.lock().unwrap().taken -= 1;
        self.slots.changed.notify_waiters();
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

/// How the front-end types serialize where the derived shape isn't the one a UI wants.
mod json {
    use super::*;
    use serde::Serializer;
    use serde::ser::SerializeMap;

    pub fn millis<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_millis() as u64)
    }

    pub fn tracker_state<S: Serializer>(state: &TrackerState, s: S) -> Result<S::Ok, S::Error> {
        let (name, error) = match state {
            TrackerState::Pending => ("pending", None),
            TrackerState::Working => ("working", None),
            TrackerState::Failed(why) => ("failed", Some(why)),
        };
        let mut map = s.serialize_map(Some(2))?;
        map.serialize_entry("state", name)?;
        map.serialize_entry("error", &error)?;
        map.end()
    }

    pub fn mapping_state<S: Serializer>(state: &MappingState, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(match state {
            MappingState::Off => "off",
            MappingState::Searching => "searching",
            MappingState::Mapped { .. } => "mapped",
            MappingState::Unavailable => "unavailable",
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn fraction_is_clamped_and_total() {
        let mut p = Progress {
            info_hash: String::new(),
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
            super_seed: false,
            trackers: vec![],
            feed: None,
        };
        // a zero-piece torrent must not divide by zero
        assert_eq!(p.fraction(), 0.0);

        p.total_pieces = 4;
        p.verified_pieces = 1;
        assert_eq!(p.fraction(), 0.25);

        p.verified_pieces = 4;
        assert_eq!(p.fraction(), 1.0);
    }

    /// The JSON the GUI's `api.ts` reads.
    #[test]
    fn states_serialize_the_way_the_gui_reads_them() {
        fn json(value: &impl Serialize) -> serde_json::Value {
            serde_json::to_value(value).unwrap()
        }
        let resolving = TorrentState::Resolving {
            source: "s".into(),
            elapsed: Duration::from_millis(1500),
        };
        assert_eq!(
            json(&resolving),
            serde_json::json!({"kind": "resolving", "source": "s", "elapsed_ms": 1500})
        );
        let tracker = TrackerInfo {
            url: "udp://t".into(),
            state: TrackerState::Failed("timed out".into()),
            peers: 0,
            next_announce_secs: None,
            seeders: Some(3),
            leechers: None,
            downloaded: None,
        };
        let t = json(&tracker);
        assert_eq!(
            (&t["state"], &t["error"], &t["seeders"]),
            (&"failed".into(), &"timed out".into(), &3.into())
        );
        let working = json(&TrackerInfo {
            state: TrackerState::Working,
            ..tracker
        });
        assert_eq!(
            (&working["state"], &working["error"]),
            (&"working".into(), &serde_json::Value::Null)
        );
        let feed = Feed {
            key: FeedKey {
                public: [0xab; 32],
                salt: b"v".to_vec(),
            },
            seq: Some(2),
            superseded: None,
        };
        assert_eq!(
            json(&feed),
            serde_json::json!({"key": "ab".repeat(32), "salt": "76", "seq": 2, "superseded": null})
        );
        let status = SessionStatus {
            download_bps: 0.0,
            upload_bps: 0.0,
            dht_nodes: None,
            listen_port: 1,
            port_mapping: MappingState::Mapped { external_ip: None },
            external_ip: Some("203.0.113.9".parse().unwrap()),
        };
        let s = json(&status);
        assert_eq!(
            (&s["port_mapping"], &s["external_ip"]),
            (&"mapped".into(), &"203.0.113.9".into())
        );
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

    /// A pause whose resume write fails is written again, at the latest on the way out, so
    /// the torrent doesn't come back running at the next start.
    #[test]
    fn a_pause_that_could_not_be_saved_is_saved_on_shutdown() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch("pause-unsaved");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");
        let resume_dir = dir.join("resume");

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        let deadline = Instant::now() + Duration::from_secs(5);
        while std::fs::read_dir(&resume_dir).unwrap().count() == 0 {
            assert!(Instant::now() < deadline, "resume file was never written");
            std::thread::sleep(Duration::from_millis(20));
        }
        let set_mode = |mode| std::fs::set_permissions(&resume_dir, std::fs::Permissions::from_mode(mode)).unwrap();
        set_mode(0o500);
        session.pause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));
        std::thread::sleep(Duration::from_millis(200));
        set_mode(0o700);
        session.shutdown();
        assert!(list_resume_files(&resume_dir)[0].paused, "the pause was lost");
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A torrent paused before it ever ran has a resume file but no files; after a restart it
    /// still starts when unpaused. Its root has to be there, though: one on a drive that isn't
    /// mounted stays failed rather than being laid down on the wrong disk.
    #[test]
    fn a_torrent_paused_before_it_ran_starts_after_a_restart() {
        let dir = scratch("paused-unstarted");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");
        let torrent = crate::parse_torrent(&std::fs::read(&torrent_file).unwrap()).unwrap();
        let mut data = ResumeData::from_torrent(&torrent, &root, &bitvec![u8, Msb0; 0; 3]);
        data.paused = true;
        let path = dir.join("resume").join(ResumeData::file_name(&torrent.info_hash));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        data.write(&path).unwrap();

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.resume(&path);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));
        session.unpause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Failed { .. })));

        std::fs::create_dir_all(&root).unwrap();
        session.unpause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        assert_eq!(std::fs::metadata(root.join("session.bin")).unwrap().len(), 40);
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Adding the magnet of a torrent that failed asks for it again rather than adding a
    /// second failed entry: one with a resume file is rechecked, one without makes way.
    #[test]
    fn a_magnet_for_a_failed_torrent_tries_it_again() {
        let dir = scratch("readd-failed");
        let torrent_file = write_torrent_file(&dir);
        let torrent = crate::parse_torrent(&std::fs::read(&torrent_file).unwrap()).unwrap();
        let magnet = format!("magnet:?xt=urn:btih:{}", torrent.info_hash);
        let root = dir.join("downloads");
        std::fs::create_dir_all(&root).unwrap();
        let mut session = Session::new(test_config(&dir)).unwrap();
        let failed = |session: &mut Session, id| {
            wait_for(session, id, |s| matches!(s, Some(TorrentState::Failed { .. })));
        };

        // no resume file: can't even lay its files down under a root that is a file
        let blocked = dir.join("blocked");
        std::fs::write(&blocked, b"").unwrap();
        let first = session.add(torrent_file.display().to_string(), &blocked);
        failed(&mut session, first);
        let second = session.add(&magnet, &root);
        assert_ne!(second, first);
        wait_for(&mut session, first, |s| s.is_none());
        wait_for(&mut session, second, |s| {
            matches!(s, Some(TorrentState::Resolving { .. }))
        });
        session.remove(second, false);

        // with one, its data deleted
        let data = ResumeData::from_torrent(&torrent, &root, &bitvec![u8, Msb0; 1; 3]);
        let path = dir.join("resume").join(ResumeData::file_name(&torrent.info_hash));
        data.write(&path).unwrap();
        let third = session.resume(&path);
        failed(&mut session, third);
        assert_eq!(session.add(&magnet, &root), third);
        wait_for(&mut session, third, |s| matches!(s, Some(TorrentState::Downloading(_))));
        assert_eq!(session.torrents().len(), 1);
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A torrent whose data was deleted (its resume file kept) fails saying a recheck brings
    /// it back, and one does: the files are laid down again and it downloads from what's left.
    #[test]
    fn a_recheck_downloads_deleted_data_again() {
        let dir = scratch("deleted-data");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");
        std::fs::create_dir_all(&root).unwrap();
        let torrent = crate::parse_torrent(&std::fs::read(&torrent_file).unwrap()).unwrap();
        let data = ResumeData::from_torrent(&torrent, &root, &bitvec![u8, Msb0; 1; 3]);
        let path = dir.join("resume").join(ResumeData::file_name(&torrent.info_hash));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        data.write(&path).unwrap();

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.resume(&path);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Failed { .. })));
        let Some((_, TorrentState::Failed { error, .. })) = session.torrents().into_iter().find(|(i, _)| *i == id)
        else {
            panic!()
        };
        assert!(error.contains("a recheck downloads"), "{error}");
        session.recheck(id);
        wait_for(
            &mut session,
            id,
            |s| matches!(s, Some(TorrentState::Downloading(p)) if p.verified_pieces == 0),
        );
        assert_eq!(std::fs::metadata(root.join("session.bin")).unwrap().len(), 40);

        // only some of it lost: what survived is kept
        session.pause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));
        std::fs::write(root.join("session.bin"), [7u8; 16]).unwrap();
        session.recheck(id);
        wait_for(
            &mut session,
            id,
            |s| matches!(s, Some(TorrentState::Paused(p)) if p.verified_pieces == 1),
        );
        session.unpause(id);
        wait_for(
            &mut session,
            id,
            |s| matches!(s, Some(TorrentState::Downloading(p)) if p.verified_pieces == 1),
        );
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// An unpause that comes while a paused torrent's resume file is being read starts it.
    #[test]
    fn unpause_while_resolving_wins_over_the_resume_file() {
        let dir = scratch("unpause-resolving");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.pause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));
        session.shutdown();

        let mut session = Session::new(test_config(&dir)).unwrap();
        let ids = session.resume_all();
        session.unpause(ids[0]);
        wait_for(&mut session, ids[0], |s| {
            matches!(s, Some(TorrentState::Downloading(_)))
        });
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

    /// Removed before its resume file has even been read: the file goes anyway, and the data
    /// with it if asked, or the torrent would be back at the next start.
    #[test]
    fn removal_while_reading_the_resume_file_takes_it() {
        let dir = scratch("remove-resuming");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");
        let resume_dir = dir.join("resume");

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.shutdown();

        let mut session = Session::new(test_config(&dir)).unwrap();
        let ids = session.resume_all();
        session.remove(ids[0], true);
        let deadline = Instant::now() + Duration::from_secs(5);
        while root.join("session.bin").exists() || std::fs::read_dir(&resume_dir).unwrap().count() > 0 {
            assert!(
                Instant::now() < deadline,
                "the resume file or the data survived removal"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Removing a torrent and adding it straight back (to download it afresh, say) gets a new
    /// entry once the old one has cleaned up, not "already added".
    #[test]
    fn a_removed_torrent_can_be_added_again_at_once() {
        let dir = scratch("readd");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.remove(id, true);
        let again = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, again, |s| {
            matches!(s, Some(TorrentState::Downloading(_) | TorrentState::Failed { .. }))
        });
        let Some((_, state)) = session.torrents().into_iter().find(|(i, _)| *i == again) else {
            panic!()
        };
        assert!(matches!(state, TorrentState::Downloading(_)), "{state:?}");
        // a duplicate of one that stays is still refused
        let dup = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, dup, |s| matches!(s, Some(TorrentState::Failed { .. })));
        session.remove(dup, false);
        let third = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, third, |s| matches!(s, Some(TorrentState::Failed { .. })));
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

    /// Shrinking the limit below what's taken keeps the running ones going and lets nobody
    /// new in until it's back under; growing it again, or lifting it, adds exactly that much.
    #[tokio::test]
    async fn slots_follow_the_limit_both_ways() {
        let slots = Slots::new(2);
        let a = slots.acquire().await;
        let b = slots.acquire().await;
        let try_acquire = |slots: &Arc<Slots>| {
            let slots = slots.clone();
            tokio::spawn(async move { slots.acquire().await })
        };
        let settle = || tokio::time::sleep(Duration::from_millis(20));

        slots.set_limit(1);
        let c = try_acquire(&slots);
        drop(a);
        settle().await;
        assert!(!c.is_finished(), "one taken, one allowed");
        // flapping the limit must not leak or swallow slots
        for _ in 0..5 {
            slots.set_limit(usize::MAX);
            slots.set_limit(1);
        }
        drop(b);
        let c = c.await.unwrap();
        assert_eq!(slots.taken(), 1);

        let d = try_acquire(&slots);
        let e = try_acquire(&slots);
        settle().await;
        assert!(!d.is_finished() && !e.is_finished());
        slots.set_limit(2);
        let d = d.await.unwrap();
        settle().await;
        assert!(!e.is_finished(), "first come first served, and only one more fits");
        slots.set_limit(usize::MAX);
        let e = e.await.unwrap();
        assert_eq!(slots.taken(), 3);

        slots.set_limit(1);
        drop((c, d, e));
        assert_eq!(slots.taken(), 0);
        let f = slots.acquire().await;
        let g = try_acquire(&slots);
        let h = try_acquire(&slots);
        settle().await;
        // the one first in line giving up lets the next one have its turn
        g.abort();
        drop(f);
        drop(h.await.unwrap());
        assert_eq!(slots.taken(), 0);
    }

    /// One data directory, one session: the second one is refused while the first runs, and
    /// gets in once it has shut down.
    #[test]
    fn a_second_session_on_the_same_data_dir_is_refused() {
        let dir = scratch("lock");
        let mut first = Session::new(test_config(&dir)).unwrap();
        let Err(e) = Session::new(test_config(&dir)) else {
            panic!("two sessions on one data dir");
        };
        assert!(e.is::<AlreadyRunning>(), "{e:#}");
        assert!(e.to_string().contains("already using"), "{e}");
        first.shutdown();
        Session::new(test_config(&dir)).unwrap().shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A torrent whose files are missing at startup (a drive not mounted yet) fails, keeps
    /// its resume file, and starts once the files are back and it's unpaused.
    #[test]
    fn a_failed_torrent_is_retried_from_its_resume_file() {
        let dir = scratch("retry");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");
        let elsewhere = dir.join("unmounted");

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.pause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));
        session.shutdown();
        std::fs::rename(&root, &elsewhere).unwrap();

        let mut session = Session::new(test_config(&dir)).unwrap();
        let ids = session.resume_all();
        wait_for(&mut session, ids[0], |s| matches!(s, Some(TorrentState::Paused(_))));
        session.unpause(ids[0]);
        wait_for(&mut session, ids[0], |s| matches!(s, Some(TorrentState::Failed { .. })));
        // nothing to retry with yet: it fails again, and the resume file is still there
        session.unpause(ids[0]);
        wait_for(&mut session, ids[0], |s| matches!(s, Some(TorrentState::Failed { .. })));
        assert_eq!(std::fs::read_dir(dir.join("resume")).unwrap().count(), 1);

        std::fs::rename(&elsewhere, &root).unwrap();
        session.unpause(ids[0]);
        wait_for(&mut session, ids[0], |s| {
            matches!(s, Some(TorrentState::Downloading(_)))
        });
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// What the resume file says about sequential mode (and the file selection) comes back
    /// with the torrent. These go through `watch` senders that have no receiver yet at that
    /// point, where a plain `send` silently drops the value.
    #[test]
    fn a_resumed_torrent_keeps_its_modes() {
        let dir = scratch("sequential");
        let torrent_file = write_torrent_file(&dir);
        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), dir.join("downloads"));
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.set_sequential(id, true);
        session.set_super_seed(id, true);
        wait_for(
            &mut session,
            id,
            |s| matches!(s, Some(TorrentState::Downloading(p)) if p.sequential && p.super_seed),
        );
        session.pause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));
        session.shutdown();

        let mut session = Session::new(test_config(&dir)).unwrap();
        let ids = session.resume_all();
        wait_for(
            &mut session,
            ids[0],
            |s| matches!(s, Some(TorrentState::Paused(p)) if p.sequential && p.super_seed),
        );
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Switches flipped while paused show at once and reach the resume file, which stays
    /// marked paused.
    #[test]
    fn switches_flipped_while_paused_are_saved() {
        let dir = scratch("paused-switches");
        let torrent_file = write_torrent_file(&dir);
        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), dir.join("downloads"));
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.pause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));

        session.set_sequential(id, true);
        session.select_files(id, vec![false]);
        wait_for(
            &mut session,
            id,
            |s| matches!(s, Some(TorrentState::Paused(p)) if p.sequential && !p.files[0].selected),
        );
        let path = list_resume_files(&dir.join("resume"))[0].path.clone();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !ResumeData::read(&path).is_ok_and(|d| d.paused && d.modes.sequential && d.selected == [false]) {
            assert!(Instant::now() < deadline, "{:?}", ResumeData::read(&path));
            std::thread::sleep(Duration::from_millis(20));
        }
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The DHT key a torrent follows (BEP 46) is in its resume file, and still is after a
    /// restart, a pause, and a shutdown; Progress shows it.
    #[test]
    fn a_followed_key_survives_a_restart() {
        let dir = scratch("feed");
        let torrent_file = write_torrent_file(&dir);
        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), dir.join("downloads"));
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.shutdown();

        let path = list_resume_files(&dir.join("resume"))[0].path.clone();
        let feed = Feed {
            key: FeedKey {
                public: [4; 32],
                salt: b"x".to_vec(),
            },
            seq: Some(7),
            superseded: None,
        };
        let mut data = ResumeData::read(&path).unwrap();
        data.feed = Some(feed.clone());
        data.write(&path).unwrap();

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.resume_all()[0];
        wait_for(
            &mut session,
            id,
            |s| matches!(s, Some(TorrentState::Downloading(p)) if p.feed.as_ref() == Some(&feed)),
        );
        session.pause(id);
        wait_for(
            &mut session,
            id,
            |s| matches!(s, Some(TorrentState::Paused(p)) if p.feed.as_ref() == Some(&feed)),
        );
        session.shutdown();
        assert_eq!(ResumeData::read(&path).unwrap().feed, Some(feed));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_key_only_magnet_needs_the_dht() {
        let dir = scratch("feed-no-dht");
        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(
            format!("magnet:?xs=urn:btpk:{}", "ab".repeat(32)),
            dir.join("downloads"),
        );
        wait_for(
            &mut session,
            id,
            |s| matches!(s, Some(TorrentState::Failed { error, .. }) if error.contains("needs the DHT")),
        );
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A newer version a key names joins the session as an entry of its own, once; its
    /// predecessor is superseded only once the newer one has resolved
    #[test]
    fn an_update_is_added_unless_already_there() {
        let dir = scratch("update");
        let torrent_file = write_torrent_file(&dir);
        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), dir.join("downloads"));
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        let previous = Arc::new(crate::parse_torrent(&std::fs::read(&torrent_file).unwrap()).unwrap());
        let signing = midwest_mainline::dht::item::SigningKey::from_bytes(&[1; 32]);
        let update = |info_hash: InfoHash, previous_feed: watch::Sender<Option<Feed>>| {
            let version = crate::feed::Version::v1(info_hash);
            let value = crate::feed::item_value(&version);
            FeedUpdate {
                previous: previous.clone(),
                root: dir.join("downloads"),
                key: FeedKey::of(&signing, b""),
                found: Found {
                    seq: 2,
                    version,
                    item: midwest_mainline::dht::item::MutableItem {
                        key: signing.verifying_key().to_bytes(),
                        salt: vec![],
                        seq: 2,
                        sig: midwest_mainline::dht::item::sign(&signing, b"", 2, &value),
                        value,
                    },
                },
                previous_feed,
                selected: vec![true],
                modes: Modes::default(),
            }
        };
        let following = || {
            watch::channel(Some(Feed {
                key: FeedKey::of(&signing, b""),
                seq: Some(1),
                superseded: None,
            }))
            .0
        };
        let here_already = following();
        session
            .updates
            .0
            .send(update(previous.info_hash, here_already.clone()))
            .unwrap();
        assert_eq!(session.torrents().len(), 1, "the session has that one already");
        assert_eq!(here_already.borrow().as_ref().unwrap().superseded, Some(2));

        let unresolved = following();
        session
            .updates
            .0
            .send(update(InfoHash([5; 20]), unresolved.clone()))
            .unwrap();
        let torrents = session.torrents();
        assert_eq!(torrents.len(), 2);
        let (_, added) = &torrents[1];
        let source = match added {
            TorrentState::Resolving { source, .. } | TorrentState::Failed { source, .. } => source,
            other => panic!("{other:?}"),
        };
        assert!(source.contains(&format!("xt=urn:btih:{}", "05".repeat(20))), "{source}");
        assert!(source.contains("xs=urn:btpk:"), "{source}");
        std::thread::sleep(Duration::from_millis(300));
        assert!(matches!(session.torrents()[1].1, TorrentState::Resolving { .. }));
        assert_eq!(
            unresolved.borrow().as_ref().unwrap().superseded,
            None,
            "while the update resolves, its predecessor is still the follower"
        );
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_update_keeps_its_predecessors_choice_of_files() {
        let mut previous =
            crate::parse_torrent(&std::fs::read(write_torrent_file(&scratch("carry"))).unwrap()).unwrap();
        let mut next = previous.clone();
        let template = previous.files[0].clone();
        let file = |path: &str| {
            let mut file = template.clone();
            file.path = PathBuf::from(path);
            file
        };
        previous.files = vec![file("a"), file("b"), file("c")];
        next.files = vec![file("c"), file("new"), file("a")];
        assert_eq!(
            carried_selection(&previous, &[true, true, false], &next),
            [false, true, true]
        );
    }

    /// An update whose name is taken goes beside it, starting from the files it shares
    #[test]
    fn an_update_starts_from_its_predecessors_files() {
        use crate::torrent::fixtures;
        let dir = scratch("place-update");
        let torrent = |files: &[(&[&str], Vec<u8>)]| {
            let info = fixtures::info("pkg", &fixtures::sorted(files), 16384, false);
            crate::parse_torrent(&crate::metadata::build_torrent_file(&info, &[])).unwrap()
        };
        let old = torrent(&[(&["same"], vec![1; 10]), (&["gone"], vec![2; 5])]);
        let new = torrent(&[(&["same"], vec![1; 10]), (&["added"], vec![3; 7])]);
        for file in &old.files {
            std::fs::create_dir_all(dir.join(&file.path).parent().unwrap()).unwrap();
            std::fs::write(dir.join(&file.path), vec![9; file.len as usize]).unwrap();
        }

        let root = place_update(&old, &dir, &new, 2).unwrap();
        assert!(has_data_on_disk(&new, &root), "so it starts with a check");
        assert_eq!(root, dir.join("pkg (seq 2)"));
        assert_eq!(
            std::fs::read(root.join("pkg/same")).unwrap(),
            [9; 10],
            "copied as it was"
        );
        assert_eq!(std::fs::metadata(root.join("pkg/added")).unwrap().len(), 7);
        assert!(!root.join("pkg/gone").exists());
        assert_eq!(
            std::fs::read(dir.join("pkg/same")).unwrap(),
            [9; 10],
            "the old one untouched"
        );

        let again = place_update(&old, &dir, &new, 2).unwrap();
        assert_eq!(again, dir.join("pkg (seq 2, 2)"), "never on top of anything");

        let unrelated = crate::parse_torrent(&crate::metadata::build_torrent_file(
            &fixtures::info("other", &[(&["f"], vec![1; 3])], 16384, false),
            &[],
        ))
        .unwrap();
        assert_eq!(place_update(&old, &dir, &unrelated, 3).unwrap(), dir);
        assert!(!has_data_on_disk(&unrelated, &dir), "nothing laid down for it");
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A resume file that doesn't parse shows up as a failed entry saying why, instead of
    /// the torrent quietly vanishing; removing it moves the file aside rather than deleting it.
    #[test]
    fn a_broken_resume_file_is_listed_as_failed() {
        let dir = scratch("broken");
        let resume_dir = dir.join("resume");
        std::fs::create_dir_all(&resume_dir).unwrap();
        let broken = resume_dir.join("0123.resume");
        std::fs::write(&broken, b"d4:junke").unwrap();

        let mut session = Session::new(test_config(&dir)).unwrap();
        let ids = session.resume_all();
        assert_eq!(ids.len(), 1);
        wait_for(&mut session, ids[0], |s| matches!(s, Some(TorrentState::Failed { .. })));
        let Some((_, TorrentState::Failed { error, source })) = session.torrents().into_iter().next() else {
            panic!()
        };
        assert!(error.contains("0123.resume"), "{error}");
        assert_eq!(source, broken.display().to_string());
        assert!(session.resume_all().is_empty(), "listed once");

        session.remove(ids[0], true);
        let deadline = Instant::now() + Duration::from_secs(5);
        while broken.exists() {
            assert!(Instant::now() < deadline, "the broken file wasn't moved aside");
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(std::fs::read(resume_dir.join("0123.resume.bad")).unwrap(), b"d4:junke");
        session.shutdown();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Adding a torrent the session has paused is refused like adding a running one, and
    /// removing the refused entry leaves the paused one's files and resume file alone.
    #[test]
    fn a_duplicate_of_a_paused_torrent_is_refused() {
        let dir = scratch("dup-paused");
        let torrent_file = write_torrent_file(&dir);
        let root = dir.join("downloads");

        let mut session = Session::new(test_config(&dir)).unwrap();
        let id = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Downloading(_))));
        session.pause(id);
        wait_for(&mut session, id, |s| matches!(s, Some(TorrentState::Paused(_))));
        std::fs::write(root.join("session.bin"), [7u8; 40]).unwrap();

        let dup = session.add(torrent_file.display().to_string(), &root);
        wait_for(&mut session, dup, |s| matches!(s, Some(TorrentState::Failed { .. })));
        session.remove(dup, true);
        wait_for(&mut session, dup, |s| s.is_none());
        session.recheck(id);
        wait_for(
            &mut session,
            id,
            |s| matches!(s, Some(TorrentState::Paused(p)) if p.completed),
        );
        session.shutdown();
        assert_eq!(Session::new(test_config(&dir)).unwrap().resumable().len(), 1);
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
