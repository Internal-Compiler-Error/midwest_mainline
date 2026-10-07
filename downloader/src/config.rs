//! User settings, kept in `<data dir>/settings.json`. Missing keys take their defaults, so
//! a file from an older version still loads and new settings appear with sensible values.

use crate::paths::default_download_dir;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::sync::watch;

/// Whether connections use MSE (see `mse`). Inbound: `Disabled` refuses encrypted peers,
/// `Require` refuses plaintext ones. Outbound: `Prefer` tries encryption first and falls back
/// to plaintext on a fresh connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Encryption {
    Disabled,
    #[default]
    Prefer,
    Require,
}

pub const FILE_NAME: &str = "settings.json";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    /// port for inbound peers, TCP and uTP alike; the DHT node takes the next one up (see
    /// `dht_port`). Takes effect at the next start.
    pub listen_port: u16,
    /// where a new torrent goes unless the user picks somewhere else
    pub download_dir: PathBuf,
    /// run a DHT node; off means peers come from trackers and PEX only. Next start.
    pub dht: bool,
    /// BEP 43: the DHT node asks but never answers, and other nodes leave it out of their
    /// routing tables; for a host that can't take inbound UDP or is on a metered link. Next
    /// start.
    pub dht_read_only: bool,
    /// connections per torrent, inbound and outbound together, 0 for no limit; applies live
    pub max_peers_per_torrent: usize,
    /// bytes per second across all torrents, 0 for no limit; applies live
    pub download_limit: u64,
    pub upload_limit: u64,
    /// stop seeding once uploaded / torrent size reaches this, 0 to seed forever; applies
    /// live. The upload total survives restarts, so a torrent that seeded 1.5 last week and
    /// comes back with a limit of 2 keeps going until it has uploaded 2 sizes in all.
    pub seed_ratio_limit: f64,
    /// MSE protocol encryption policy. Next start.
    pub encryption: Encryption,
    /// accept and dial peers over uTP as well as TCP. Next start.
    pub utp: bool,
    /// ask the router to forward our ports (NAT-PMP, PCP, or UPnP). Next start.
    pub port_mapping: bool,
    /// torrents downloading at once, 0 for no limit; the rest wait their turn (seeding
    /// doesn't count). Applies live, though a running download keeps its place.
    pub max_active_downloads: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            listen_port: 6881,
            download_dir: default_download_dir(),
            dht: true,
            dht_read_only: false,
            max_peers_per_torrent: 0,
            download_limit: 0,
            upload_limit: 0,
            seed_ratio_limit: 0.0,
            encryption: Encryption::Prefer,
            utp: true,
            port_mapping: true,
            max_active_downloads: 0,
        }
    }
}

impl Settings {
    /// The DHT node's UDP port: uTP has the listen port's number, and a node's port is its own
    /// business (BEP 5 carries it in the Port message), so the next one up keeps it stable
    /// across restarts and one thing to forward.
    pub fn dht_port(&self) -> u16 {
        if self.listen_port == u16::MAX {
            self.listen_port - 1
        } else {
            self.listen_port + 1
        }
    }

    /// `max_peers_per_torrent` with 0 read as no limit
    pub fn peer_cap(&self) -> usize {
        match self.max_peers_per_torrent {
            0 => usize::MAX,
            n => n,
        }
    }

    /// The settings in `data_dir`, or the defaults if there's no file yet. A file that
    /// doesn't parse is treated as absent rather than blocking startup, and moved aside to
    /// `settings.json.bad` so the next save doesn't destroy what the user had.
    pub fn load(data_dir: &Path) -> Self {
        let path = data_dir.join(FILE_NAME);
        match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                let bad = path.with_extension("json.bad");
                match std::fs::rename(&path, &bad) {
                    Ok(()) => tracing::warn!(
                        "{} doesn't parse ({e}); using the defaults, the old file is kept as {}",
                        path.display(),
                        bad.display()
                    ),
                    Err(rename) => tracing::warn!(
                        "{} doesn't parse ({e}), using the defaults; couldn't move it aside: {rename}",
                        path.display()
                    ),
                }
                Self::default()
            }),
            Err(_) => Self::default(),
        }
    }

    pub fn save(&self, data_dir: &Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join(FILE_NAME);
        let tmp = path.with_extension("json.tmp");
        crate::resume::replace_file(&path, &tmp, &serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }
}

/// How the live settings reach the parts that apply them (swarms read the connection cap
/// and the rate limits from it).
pub type SettingsWatch = watch::Receiver<Settings>;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn missing_file_is_defaults_and_a_partial_file_fills_in() {
        let dir = std::env::temp_dir().join(format!("downloader-settings-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(Settings::load(&dir), Settings::default());

        std::fs::write(dir.join(FILE_NAME), r#"{"listen_port": 7000}"#).unwrap();
        let loaded = Settings::load(&dir);
        assert_eq!(loaded.listen_port, 7000);
        assert_eq!(loaded.max_peers_per_torrent, Settings::default().max_peers_per_torrent);

        let mut changed = loaded.clone();
        changed.upload_limit = 12_345;
        changed.save(&dir).unwrap();
        assert_eq!(Settings::load(&dir), changed);

        std::fs::write(dir.join(FILE_NAME), "not json").unwrap();
        assert_eq!(Settings::load(&dir), Settings::default(), "garbage is ignored");
        assert!(!dir.join(FILE_NAME).exists());
        assert_eq!(
            std::fs::read(dir.join("settings.json.bad")).unwrap(),
            b"not json",
            "but kept where the next save won't overwrite it"
        );
        changed.save(&dir).unwrap();
        assert_eq!(Settings::load(&dir), changed);
        assert!(!dir.join("settings.json.tmp").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
