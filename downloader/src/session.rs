//! A single-torrent download session, with all the orchestration a UI would otherwise have to
//! do itself: owning a runtime, resolving a source (file or magnet), starting the client, and
//! tracking transfer rates.
//!
//! The point is that a front end shouldn't need to know about tokio, channels, or the shape of
//! the client at all. It calls [`Session::start`] with whatever the user typed, calls
//! [`Session::state`] whenever it wants to draw, and renders the [`SessionState`] it gets back.

use crate::defs::Identity;
use crate::resume::{ResumeData, keep_saving};
use crate::torrent::Torrent;
use crate::torrent_swarm::TorrentSwarmStats;
use crate::{BtClient, load_source};
use bitvec::prelude::*;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Everything a front end needs to render, with no channels or futures in sight.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionState {
    Idle,
    /// Resolving a source into a torrent. For a magnet this means announcing to its trackers
    /// and fetching metadata from a peer, which can take a while; `elapsed` is for showing
    /// that something is still happening.
    Resolving {
        source: String,
        elapsed: Duration,
    },
    Downloading(Progress),
    Failed {
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

/// What the background task publishes; `Session::state` turns this into a `SessionState`.
#[derive(Clone)]
enum Phase {
    Idle,
    Resolving {
        source: String,
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

pub struct Session {
    /// `Option` only so `shutdown` can take it and stop it with a bounded timeout; a plain drop
    /// waits on in-flight tasks indefinitely, which is how a UI ends up unquittable.
    rt: Option<Runtime>,
    handle: Handle,
    identity: Arc<Identity>,
    phase_tx: watch::Sender<Phase>,
    phase_rx: watch::Receiver<Phase>,
    /// cancels whatever is currently running (a resolve, a download, or both)
    current: Option<CancellationToken>,
    rates: Rates,
    /// where resume files are written; none means progress isn't persisted
    resume_dir: Option<PathBuf>,
}

impl Session {
    /// Creates a session with its own tokio runtime, listening on `port` for inbound peers.
    pub fn new(peer_id: [u8; 20], port: u16) -> anyhow::Result<Self> {
        let rt = Runtime::new()?;
        let (phase_tx, phase_rx) = watch::channel(Phase::Idle);
        Ok(Self {
            handle: rt.handle().clone(),
            rt: Some(rt),
            identity: Arc::new(Identity {
                peer_id,
                serving: std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, port).into(),
            }),
            phase_tx,
            phase_rx,
            current: None,
            rates: Rates::new(),
            resume_dir: None,
        })
    }

    /// Persist progress to `dir/<info hash>.resume` for every download from now on. Finding
    /// those files again and handing them to [`Session::resume`] is the caller's job; see
    /// `list_resume_files`.
    pub fn set_resume_dir(&mut self, dir: impl Into<PathBuf>) {
        self.resume_dir = Some(dir.into());
    }

    /// Starts downloading `source`, which may be a path to a `.torrent` or a magnet URI, into
    /// `root` (see `BtClient::add_torrent` for the layout under it). Replaces whatever was
    /// running before. Returns immediately; watch [`Session::state`] for what happens next.
    pub fn start(&mut self, source: impl Into<String>, root: impl Into<PathBuf>) {
        let source = source.into();
        let root = root.into();
        let identity = self.identity.clone();
        self.launch(source.clone(), |cancel| async move {
            let torrent = load_source(&source, identity, cancel).await?;
            let nothing = bitvec![u8, Msb0; 0; torrent.pieces.len()].into_boxed_bitslice();
            Ok((torrent, root, nothing, false))
        });
    }

    /// Picks a download back up from a resume file (see `ResumeData`), in the root it was
    /// started in. Replaces whatever was running before, like [`Session::start`].
    pub fn resume(&mut self, path: impl AsRef<Path>) {
        let path = path.as_ref().to_path_buf();
        self.launch(path.display().to_string(), |_cancel| async move {
            let data = ResumeData::read(&path)?;
            Ok((data.to_torrent()?, data.root, data.verified, true))
        });
    }

    /// Shared tail of `start`/`resume`: `resolve` produces the torrent, where its files go,
    /// which pieces are already had, and whether the target files are expected to exist.
    fn launch<F, Fut>(&mut self, source: String, resolve: F)
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<(Torrent, PathBuf, BitBox<u8, Msb0>, bool)>> + Send,
    {
        self.stop();
        self.rates = Rates::new();

        let cancel = CancellationToken::new();
        self.current = Some(cancel.clone());
        let _ = self.phase_tx.send(Phase::Resolving {
            source,
            started: Instant::now(),
        });

        let identity = self.identity.clone();
        let phase_tx = self.phase_tx.clone();
        let resume_dir = self.resume_dir.clone();
        self.handle.spawn(async move {
            let fail = |e: anyhow::Error| {
                let _ = phase_tx.send(Phase::Failed {
                    error: format!("{e:#}"),
                });
            };
            let (torrent, root, verified, resumed) = match resolve(cancel.clone()).await {
                Ok(resolved) => resolved,
                Err(e) => return fail(e),
            };

            // share the session's token so `Session::stop` shuts the client down too
            let mut client = BtClient::new_with_shutdown(*identity, cancel.clone());
            let added = if resumed {
                client.add_torrent_resumed(torrent.clone(), &root, verified)
            } else {
                client.add_torrent(torrent.clone(), &root)
            };
            if let Err(e) = added {
                return fail(e);
            }
            let Some(stats) = client.stats(&torrent) else {
                return fail(anyhow::anyhow!("torrent was added but reported no stats"));
            };

            let torrent = Arc::new(torrent);
            if let Some(dir) = resume_dir {
                tokio::spawn(keep_saving(
                    torrent.clone(),
                    root.clone(),
                    stats.clone(),
                    dir,
                    cancel.clone(),
                ));
            }
            let _ = phase_tx.send(Phase::Downloading { torrent, root, stats });
            let _ = client.work().await;
        });
    }

    /// Stops any running resolve/download. Safe to call when nothing is running.
    pub fn stop(&mut self) {
        if let Some(cancel) = self.current.take() {
            cancel.cancel();
        }
        let _ = self.phase_tx.send(Phase::Idle);
    }

    /// The current state, ready to render. Cheap enough to call every frame.
    pub fn state(&mut self) -> SessionState {
        let phase = self.phase_rx.borrow_and_update().clone();
        match phase {
            Phase::Idle => SessionState::Idle,
            Phase::Failed { error } => SessionState::Failed { error },
            Phase::Resolving { source, started } => SessionState::Resolving {
                source,
                elapsed: started.elapsed(),
            },
            Phase::Downloading { torrent, root, stats } => {
                let stats = stats.borrow().clone();
                self.rates.update(&stats);
                SessionState::Downloading(Progress {
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

    /// Stops everything and shuts the runtime down with a bounded wait, so a slow or
    /// unreachable tracker can't stall process exit.
    pub fn shutdown(&mut self) {
        self.stop();
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
}
