//! The command-line client: one torrent, on the same `Session` the GUI uses, with the user's
//! settings from the data directory. Progress goes to the log every few seconds; the process
//! exits once the download is complete (or keeps seeding with `--seed`) and on Ctrl-C, saving
//! resume data either way, so running the same command again carries on where it stopped.

use anyhow::Context;
use downloader::feed::{FeedKey, Version};
use downloader::{
    ResumeData, Session, SessionConfig, Settings, Telemetry, TorrentId, TorrentState, data_dir, is_magnet_uri,
    parse_magnet, parse_torrent, random_peer_id,
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

/// What the command line asks for.
#[derive(Debug, PartialEq)]
enum Cli {
    Help,
    Download {
        source: String,
        /// the settings' download directory if not given
        dir: Option<PathBuf>,
        seed: bool,
        super_seed: bool,
    },
    Publish {
        key_file: PathBuf,
        /// a `.torrent` path or 40 hex digits
        target: String,
        salt: Vec<u8>,
    },
}

impl Cli {
    /// From the arguments after the program name; an error is a usage mistake.
    fn parse(args: impl IntoIterator<Item = String>) -> anyhow::Result<Self> {
        let mut args = args.into_iter().peekable();
        let publish = args.next_if(|arg| arg == "publish").is_some();
        let (mut seed, mut super_seed, mut salt) = (false, false, vec![]);
        let mut positional = vec![];
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => return Ok(Cli::Help),
                "--seed" if !publish => seed = true,
                "--super-seed" if !publish => (seed, super_seed) = (true, true),
                "--salt" if publish => salt = args.next().context("--salt needs a value")?.into_bytes(),
                _ if arg.starts_with("--") => anyhow::bail!("unknown option {arg}"),
                _ => positional.push(arg),
            }
        }
        let mut positional = positional.into_iter();
        match (publish, positional.next(), positional.next(), positional.next()) {
            (false, Some(source), dir, None) => Ok(Cli::Download {
                source,
                dir: dir.map(PathBuf::from),
                seed,
                super_seed,
            }),
            (true, Some(key_file), Some(target), None) => Ok(Cli::Publish {
                key_file: key_file.into(),
                target,
                salt,
            }),
            _ => anyhow::bail!("wrong number of arguments"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    // RUST_LOG picks the console's verbosity, e.g. `RUST_LOG=info,downloader::metadata=debug`;
    // OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 also sends the traces to a collector
    let telemetry = Telemetry::install(tracing_subscriber::fmt::layer().pretty().boxed())?;

    let status = match Cli::parse(std::env::args().skip(1)) {
        Ok(Cli::Help) => {
            println!("{USAGE}");
            0
        }
        Ok(Cli::Download {
            source,
            dir,
            seed,
            super_seed,
        }) => download(source, dir, seed, super_seed)?,
        Ok(Cli::Publish { key_file, target, salt }) => match publish(&key_file, &target, &salt) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("downloader publish: {e:#}");
                1
            }
        },
        Err(e) => {
            eprintln!("downloader: {e}\n{USAGE}");
            2
        }
    };
    // exit skips destructors, and this one flushes the last traces
    drop(telemetry);
    std::process::exit(status);
}

/// Downloads `source` until it's complete (and, with `seed`, after), or until Ctrl-C.
/// Returns the exit status.
fn download(source: String, dir: Option<PathBuf>, seed: bool, super_seed: bool) -> anyhow::Result<i32> {
    let interrupted = Arc::new(AtomicBool::new(false));
    ctrlc::set_handler({
        let interrupted = interrupted.clone();
        move || interrupted.store(true, Ordering::SeqCst)
    })?;

    let data_dir = data_dir();
    let settings = Settings::load(&data_dir);
    let root = dir.unwrap_or_else(|| settings.download_dir.clone());
    let mut session = match Session::new(SessionConfig {
        peer_id: random_peer_id(),
        data_dir,
        settings,
    }) {
        Ok(session) => session,
        Err(e) if e.is::<downloader::session::AlreadyRunning>() => {
            eprintln!("downloader: {e}");
            return Ok(1);
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
    let status = report_until_done(&mut session, id, seed, &interrupted);
    // stops the swarms (resume data is written on the way out) and the runtime
    session.shutdown();
    Ok(status)
}

/// Logs the torrent's progress every few seconds until it's done with, and returns the exit
/// status: 0 complete, 1 failed, 130 interrupted.
fn report_until_done(session: &mut Session, id: TorrentId, seed: bool, interrupted: &AtomicBool) -> i32 {
    let mut last_report = Instant::now();
    let mut announced_metadata = false;
    loop {
        std::thread::sleep(Duration::from_millis(250));
        if interrupted.load(Ordering::SeqCst) {
            tracing::info!("interrupted, saving progress and stopping");
            return 130;
        }
        let Some((_, state)) = session.torrents().into_iter().find(|(i, _)| *i == id) else {
            return 1;
        };
        let due = last_report.elapsed() >= REPORT_EVERY;
        match state {
            TorrentState::Failed { error, .. } => {
                tracing::error!("{error}");
                return 1;
            }
            TorrentState::Resolving { elapsed, .. } if due => {
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
                    return 0;
                }
                if due {
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
    }
}

/// `downloader publish`: signs and puts the BEP 46 item for a torrent
fn publish(key_file: &Path, target: &str, salt: &[u8]) -> anyhow::Result<()> {
    let signing = signing_key(key_file)?;
    let (version, name) = match parse_info_hash(target) {
        Some(info_hash) if !Path::new(target).exists() => (Version::v1(info_hash), None),
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
    let published = session.publish_update(&signing, salt, &version, Duration::from_secs(90))?;
    let key = FeedKey::of(&signing, salt);
    println!(
        "{} at seq {}, stored by {} DHT nodes",
        version.info_hash, published.seq, published.stored
    );
    println!("{}", downloader::feed::magnet_uri(&key, None, name.as_deref(), &[]));
    Ok(())
}

/// 40 hex digits as an info hash.
fn parse_info_hash(text: &str) -> Option<InfoHash> {
    InfoHash::try_from_bytes(&hex::decode(text).ok()?)
}

/// The ed25519 secret key in `path` (64 hex digits), or a new one written there
fn signing_key(path: &Path) -> anyhow::Result<SigningKey> {
    let seed: [u8; 32] = match std::fs::read_to_string(path) {
        Ok(text) => hex::decode(text.trim())
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .with_context(|| format!("{} must hold 64 hex digits", path.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let seed: [u8; 32] = rand::random();
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
            std::io::Write::write_all(&mut options.open(path)?, format!("{}\n", hex::encode(seed)).as_bytes())?;
            eprintln!("made a new key in {}", path.display());
            seed
        }
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
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

#[cfg(test)]
mod test {
    use super::*;

    fn parse(args: &[&str]) -> anyhow::Result<Cli> {
        Cli::parse(args.iter().map(|a| a.to_string()))
    }

    #[test]
    fn arguments() {
        assert_eq!(
            parse(&["--super-seed", "x.torrent", "out"]).unwrap(),
            Cli::Download {
                source: "x.torrent".into(),
                dir: Some("out".into()),
                seed: true,
                super_seed: true,
            }
        );
        assert_eq!(
            parse(&["publish", "key", "x.torrent", "--salt", "v1"]).unwrap(),
            Cli::Publish {
                key_file: "key".into(),
                target: "x.torrent".into(),
                salt: b"v1".to_vec(),
            }
        );
        assert_eq!(parse(&["x", "--help"]).unwrap(), Cli::Help);
        for wrong in [
            &[][..],
            &["a", "b", "c"],
            &["--sed", "x.torrent"],
            &["publish", "key"],
            &["publish", "key", "x", "--salt"],
            &["publish", "--seed", "key", "x"],
        ] {
            assert!(parse(wrong).is_err(), "{wrong:?}");
        }
    }

    #[test]
    fn info_hashes_in_hex() {
        assert_eq!(parse_info_hash(&"ab".repeat(20)), Some(InfoHash([0xab; 20])));
        assert_eq!(parse_info_hash(&"ab".repeat(19)), None);
        // 40 bytes, but not 40 hex digits; slicing it by byte would split the 'é'
        assert_eq!(parse_info_hash(&format!("é{}", "a".repeat(38))), None);
    }
}
