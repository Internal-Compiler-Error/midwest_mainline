mod bt_client;
mod defs;
pub mod download;
pub mod magnet;
pub mod metadata;
mod peer;
pub mod resume;
pub mod session;
mod settings;
mod storage;
mod torrent;
pub mod torrent_swarm;
mod wire;

pub use bt_client::BtClient;
pub use defs::Identity;
pub use magnet::{MagnetLink, is_magnet_uri, parse_magnet};
pub use resume::{ResumeData, ResumeSummary, keep_saving, list_resume_files};
pub use session::{Progress, Session, SessionState, human_bytes};
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
) -> anyhow::Result<Torrent> {
    if is_magnet_uri(source) {
        let magnet = parse_magnet(source)?;
        metadata::fetch(&magnet, identity, shutdown).await
    } else {
        let bytes = std::fs::read(source)?;
        parse_torrent(&bytes)
    }
}
