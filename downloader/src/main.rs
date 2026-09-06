use downloader::{BtClient, Dht, Identity, ResumeData, data_dir, keep_saving, load_source};
use std::env;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

fn random_idv4(external_ip: &Ipv4Addr, rand: u8) -> [u8; 20] {
    let mut rng = rand::rng();
    let r = rand & 0x07;
    let mut id = [0u8; 20];
    let mut ip = external_ip.octets();
    let mask = [0x03, 0x0f, 0x3f, 0xff];

    for (ip, mask) in ip.iter_mut().zip(mask.iter()) {
        *ip &= mask;
    }

    ip[0] |= r << 5;
    let crc = crc32c::crc32c(&ip);

    id[0] = (crc >> 24) as u8;
    id[1] = (crc >> 16) as u8;
    id[2] = (((crc >> 8) & 0xf8) as u8) | (rand::Rng::random::<u8>(&mut rng) & 0x7);

    rand::RngCore::fill_bytes(&mut rng, &mut id[3..19]);

    id[19] = rand;

    id
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // RUST_LOG picks the verbosity, e.g. `RUST_LOG=info,downloader::metadata=debug`
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .pretty()
        .init();

    let args = env::args().collect::<Vec<_>>();
    let source = args.get(1).cloned().unwrap_or_else(|| {
        eprintln!(
            "usage: downloader <path-to-.torrent | magnet-uri> [download-dir]\n       downloader <path-to-.resume>"
        );
        std::process::exit(2);
    });

    let public_ip = Ipv4Addr::from_str("99.226.33.190")?;
    let identity = Identity {
        peer_id: random_idv4(&public_ip, 3),
        serving: SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 6881).into(),
        dht: true,
    };

    // resume files and the DHT database live in the data dir (see `paths::data_dir`)
    let data_dir = data_dir();
    std::fs::create_dir_all(data_dir.join("resume"))?;
    let dht = Dht::start(data_dir.join("dht.db"), 6881);
    let client = BtClient::new(identity, dht.watch());
    let (torrent, root) = if Path::new(&source)
        .extension()
        .is_some_and(|ext| ext == downloader::resume::EXTENSION)
    {
        let data = ResumeData::read(Path::new(&source))?;
        let torrent = data.to_torrent()?;
        tracing::info!(
            "resuming {} in {} with {}/{} pieces already verified",
            torrent.name,
            data.root.display(),
            data.verified.count_ones(),
            data.verified.len()
        );
        client.add_torrent_resumed(torrent.clone(), &data.root, data.verified)?;
        (torrent, data.root)
    } else {
        let root = PathBuf::from(args.get(2).map(String::as_str).unwrap_or("."));
        // A magnet has to fetch its metadata off the network before there's anything to
        // download, so this can run for a while (or fail) where a .torrent returns
        // immediately -- long enough that Ctrl+C has to work during it, not just once the
        // download proper has started.
        if downloader::is_magnet_uri(&source) {
            tracing::info!("resolving magnet link, fetching metadata from peers...");
        }
        let resolving = CancellationToken::new();
        let loaded = tokio::select! {
            resolved = load_source(&source, Arc::new(identity), resolving.clone(), client.dht()) => resolved?,
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("interrupted while resolving, stopping tracker announces...");
                resolving.cancel();
                return Ok(());
            }
        };
        let torrent = loaded.torrent;
        tracing::info!(
            "got metadata for {} files, downloading into {}",
            torrent.files.len(),
            root.display()
        );
        client.add_torrent(torrent.clone(), &root)?;
        client.add_peers(&torrent.info_hash, loaded.peers);
        (torrent, root)
    };

    let resume_dir = data_dir.join("resume");
    tracing::info!(
        "progress is saved to {}",
        resume_dir.join(ResumeData::file_name(&torrent.info_hash)).display()
    );
    let stats = client.stats(&torrent).expect("torrent was just added");
    let (_all_files, all_files) = tokio::sync::watch::channel(vec![true; torrent.files.len()]);
    tokio::spawn(keep_saving(
        Arc::new(torrent),
        root,
        stats,
        all_files,
        0,
        resume_dir,
        client.shutdown_token(),
    ));

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down, sending a farewell announce to trackers...");
    client.shutdown_token().cancel();
    // dropping the client stops the swarm; give the announcer tasks a moment to get their
    // event=stopped out before the runtime drops them on exit
    drop(client);
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    Ok(())
}
