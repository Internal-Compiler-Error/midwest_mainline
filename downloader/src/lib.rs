mod announcer;
mod bt_client;
pub mod config;
mod defs;
pub mod dht;
mod limiter;
pub mod logs;
pub mod magnet;
pub mod metadata;
pub mod paths;
mod peer;
pub mod resume;
pub mod session;
mod settings;
mod storage;
mod torrent;
pub mod torrent_swarm;
mod wire;

pub use bt_client::BtClient;
pub use config::{Settings, SettingsWatch};
pub use defs::Identity;
pub use dht::{Dht, DhtWatch};
pub use logs::LogBuffer;
pub use magnet::{MagnetLink, is_magnet_uri, parse_magnet};
pub use paths::{data_dir, default_download_dir};
pub use resume::{ResumeData, ResumeSummary, keep_saving, list_resume_files};
pub use session::{PeerInfo, Progress, Session, SessionConfig, TorrentId, TorrentState, human_bytes};
pub use torrent::{Torrent, parse_torrent};
pub use torrent_swarm::TorrentSwarmStats;

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Resolves whatever the user handed us -- a path to a `.torrent` file, or a magnet URI -- into
/// a `Torrent` ready for `BtClient::add_torrent`.
///
/// A `.torrent` returns basically immediately. A magnet has to go find its metadata on the
/// network first (announce to the magnet's trackers, then fetch the info dict from a peer over
/// BEP 9), so it can take a while and needs a working network; see `metadata::fetch`.
pub async fn load_source(
    source: &str,
    identity: Arc<Identity>,
    shutdown: CancellationToken,
    dht: DhtWatch,
) -> anyhow::Result<Loaded> {
    if is_magnet_uri(source) {
        let magnet = parse_magnet(source)?;
        let fetched = metadata::fetch(&magnet, identity, shutdown, dht).await?;
        Ok(Loaded {
            torrent: fetched.torrent,
            peers: fetched.peers,
        })
    } else {
        let bytes = std::fs::read(source)?;
        Ok(Loaded {
            torrent: parse_torrent(&bytes)?,
            peers: vec![],
        })
    }
}

/// What `load_source` produces: the torrent, and any peers already known for it (a magnet's
/// metadata fetch meets plenty), to give the swarm a head start via `BtClient::add_peers`.
pub struct Loaded {
    pub torrent: Torrent,
    pub peers: Vec<std::net::SocketAddr>,
}
