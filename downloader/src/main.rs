//! The command-line client: one torrent, on the same `Session` the GUI uses, with the user's
//! settings from the data directory. Progress goes to the log every few seconds; the process
//! exits once the download is complete (or keeps seeding with `--seed`) and on Ctrl-C, saving
//! resume data either way, so running the same command again carries on where it stopped.

use anyhow::Context;
use downloader::feed::{FeedKey, Version};
use downloader::{
    ResumeData, Session, SessionConfig, Settings, Telemetry, TorrentState, data_dir, is_magnet_uri, parse_magnet,
    parse_torrent, random_peer_id,
};
use midwest_mainline::dht::item::SigningKey;
use midwest_mainline::types::InfoHash;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing_subscriber::Layer;

const USAGE: &str = "usage: downloader [--seed] [--super-seed] <path-to-.torrent | magnet-uri> [download-dir]
       downloader [--seed] [--super-seed] <path-to-.resume>
       downloader publish <key-file> <path-to-.torrent | info-hash> [--salt <text>]

  --seed         keep uploading after the download completes, until Ctrl-C or the seed ratio limit
  --super-seed   as the first seeder, show each peer a piece at a time (BEP 16); implies --seed
  publish        point a BEP 46 key at a torrent, so the clients following it update to that one;
                 the key file holds the secret key in hex and is made if it doesn't exist. Prints
                 the magnet link that follows the key";

const REPORT_EVERY: Duration = Duration::from_secs(5);

fn main() -> anyhow::Result<()> {
    // RUST_LOG picks the console's verbosity, e.g. `RUST_LOG=info,downloader::metadata=debug`;
    // OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 also sends the traces to a collector
    let telemetry = Telemetry::install(tracing_subscriber::fmt::layer().pretty().boxed())?;

    if std::env::args().nth(1).as_deref() == Some("publish") {
        let status = match publish(std::env::args().skip(2).collect()) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("downloader publish: {e:#}");
                1
            }
        };
        drop(telemetry);
        std::process::exit(status);
    }

    let mut seed = false;
    let mut super_seed = false;
    let mut positional = Vec::new();
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--seed" => seed = true,
            "--super-seed" => (seed, super_seed) = (true, true),
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
    if super_seed {
        session.set_super_seed(id, true);
    }

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

/// `downloader publish`: signs and puts the BEP 46 item for a torrent
fn publish(args: Vec<String>) -> anyhow::Result<()> {
    let mut salt = vec![];
    let mut positional = vec![];
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--salt" => salt = args.next().context("--salt needs a value")?.into_bytes(),
            _ => positional.push(arg),
        }
    }
    let [key_file, target] = positional.as_slice() else {
        anyhow::bail!("{USAGE}");
    };
    let signing = signing_key(Path::new(key_file))?;
    let (version, name) = match target.len() {
        40 if !Path::new(target).exists() => {
            let mut hash = [0u8; 20];
            for (i, byte) in hash.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&target[i * 2..i * 2 + 2], 16).context("not a hex info hash")?;
            }
            (Version::v1(InfoHash(hash)), None)
        }
        _ => {
            let torrent = parse_torrent(&std::fs::read(target).with_context(|| format!("reading {target}"))?)?;
            let version = match &torrent.v2 {
                // BEP 46 has 20 bytes; a v2-only torrent has no v1 hash to give
                Some(v2) if torrent.pieces.is_empty() => Version {
                    info_hash: torrent.info_hash,
                    info_hash_v2: Some(v2.info_hash),
                },
                _ => Version::v1(torrent.info_hash),
            };
            (version, Some(torrent.name))
        }
    };

    let data_dir = data_dir();
    let session = Session::new(SessionConfig {
        peer_id: random_peer_id(),
        settings: Settings::load(&data_dir),
        data_dir,
    })?;
    let published = session.publish_update(&signing, &salt, &version, Duration::from_secs(90))?;
    let key = FeedKey::of(&signing, &salt);
    println!(
        "{} at seq {}, stored by {} DHT nodes",
        version.info_hash, published.seq, published.stored
    );
    println!("{}", downloader::feed::magnet_uri(&key, None, name.as_deref(), &[]));
    Ok(())
}

/// The ed25519 secret key in `path` (64 hex digits), or a new one written there
fn signing_key(path: &Path) -> anyhow::Result<SigningKey> {
    let mut seed = [0u8; 32];
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let text = text.trim();
            anyhow::ensure!(text.len() == 64, "{} must hold 64 hex digits", path.display());
            for (i, byte) in seed.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
                    .with_context(|| format!("{} isn't hex", path.display()))?;
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            seed = rand::random();
            let hex: String = seed.iter().map(|b| format!("{b:02x}")).collect();
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
            std::io::Write::write_all(&mut options.open(path)?, format!("{hex}\n").as_bytes())?;
            eprintln!("made a new key in {}", path.display());
        }
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    }
    Ok(SigningKey::from_bytes(&seed))
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
