//! The command-line client: one torrent, on the same `Session` the GUI uses, with the user's
//! settings from the data directory. Progress goes to the log every few seconds; the process
//! exits once the download is complete (or keeps seeding with `--seed`) and on Ctrl-C, saving
//! resume data either way, so running the same command again carries on where it stopped.

use downloader::{
    ResumeData, Session, SessionConfig, Settings, Telemetry, TorrentState, data_dir, is_magnet_uri, parse_magnet,
    parse_torrent, random_peer_id,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing_subscriber::Layer;

const USAGE: &str = "usage: downloader [--seed] <path-to-.torrent | magnet-uri> [download-dir]
       downloader [--seed] <path-to-.resume>

  --seed   keep uploading after the download completes, until Ctrl-C or the seed ratio limit";

const REPORT_EVERY: Duration = Duration::from_secs(5);

fn main() -> anyhow::Result<()> {
    // RUST_LOG picks the console's verbosity, e.g. `RUST_LOG=info,downloader::metadata=debug`;
    // OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 also sends the traces to a collector
    let telemetry = Telemetry::install(tracing_subscriber::fmt::layer().pretty().boxed())?;

    let mut seed = false;
    let mut positional = Vec::new();
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--seed" => seed = true,
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            _ => positional.push(arg),
        }
    }
    let Some(source) = positional.first().cloned() else {
        eprintln!("{USAGE}");
        std::process::exit(2);
    };

    let interrupted = Arc::new(AtomicBool::new(false));
    ctrlc::set_handler({
        let interrupted = interrupted.clone();
        move || interrupted.store(true, Ordering::SeqCst)
    })?;

    let data_dir = data_dir();
    let settings = Settings::load(&data_dir);
    let root = positional
        .get(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| settings.download_dir.clone());
    let mut session = match Session::new(SessionConfig {
        peer_id: random_peer_id(),
        data_dir,
        settings,
    }) {
        Ok(session) => session,
        Err(e) if e.is::<downloader::session::AlreadyRunning>() => {
            eprintln!("downloader: {e}");
            std::process::exit(1);
        }
        Err(e) => return Err(e),
    };

    let id = match existing_resume_file(&session, &source) {
        Some(path) => {
            tracing::info!("carrying on from {}", path.display());
            session.resume(path)
        }
        None => session.add(source, root),
    };

    let mut last_report = Instant::now();
    let mut announced_metadata = false;
    let status = loop {
        std::thread::sleep(Duration::from_millis(250));
        if interrupted.load(Ordering::SeqCst) {
            tracing::info!("interrupted, saving progress and stopping");
            break 130;
        }
        let Some((_, state)) = session.torrents().into_iter().find(|(i, _)| *i == id) else {
            break 1;
        };
        match state {
            TorrentState::Failed { error, .. } => {
                tracing::error!("{error}");
                break 1;
            }
            TorrentState::Resolving { elapsed, .. } if last_report.elapsed() >= REPORT_EVERY => {
                tracing::info!("resolving, {}s so far", elapsed.as_secs());
                last_report = Instant::now();
            }
            TorrentState::Downloading(p) | TorrentState::Paused(p) | TorrentState::Queued(p) => {
                if !announced_metadata {
                    tracing::info!("got metadata for {} files, downloading into {}", p.files.len(), p.root);
                    announced_metadata = true;
                }
                if p.completed && !seed {
                    tracing::info!("{} is complete in {}", p.name, p.root);
                    break 0;
                }
                if last_report.elapsed() >= REPORT_EVERY {
                    tracing::info!(
                        "{:.1}%  {}/{} pieces  down {}  up {}  {} peers{}",
                        p.fraction() * 100.0,
                        p.verified_pieces,
                        p.total_pieces,
                        rate(p.download_bps),
                        rate(p.upload_bps),
                        p.peers.len(),
                        if p.completed { "  seeding" } else { "" }
                    );
                    last_report = Instant::now();
                }
            }
            _ => {}
        }
    };

    // stops the swarms (resume data is written on the way out) and the runtime
    session.shutdown();
    // exit skips destructors, and this one flushes the last traces
    drop(telemetry);
    std::process::exit(status);
}

/// The resume file a previous run of the same source left behind, so it continues rather
/// than starting over. A `.resume` path is its own answer.
fn existing_resume_file(session: &Session, source: &str) -> Option<PathBuf> {
    let path = Path::new(source);
    if path.extension().is_some_and(|ext| ext == downloader::resume::EXTENSION) {
        return Some(path.to_path_buf());
    }
    let info_hash = if is_magnet_uri(source) {
        parse_magnet(source).ok()?.info_hash
    } else {
        parse_torrent(&std::fs::read(path).ok()?).ok()?.info_hash
    };
    let candidate = session.resume_dir().join(ResumeData::file_name(&info_hash));
    candidate.exists().then_some(candidate)
}

fn rate(bps: f64) -> String {
    if bps >= 1024.0 * 1024.0 {
        format!("{:.1} MiB/s", bps / (1024.0 * 1024.0))
    } else {
        format!("{:.0} KiB/s", bps / 1024.0)
    }
}
