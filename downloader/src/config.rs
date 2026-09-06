//! User settings, kept in `<data dir>/settings.json`. Missing keys take their defaults, so
//! a file from an older version still loads and new settings appear with sensible values.

use crate::paths::default_download_dir;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::sync::watch;

pub const FILE_NAME: &str = "settings.json";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    /// TCP port for inbound peers, and the DHT node's UDP port. Takes effect at the next
    /// start.
    pub listen_port: u16,
    /// where a new torrent goes unless the user picks somewhere else
    pub download_dir: PathBuf,
    /// run a DHT node; off means peers come from trackers and PEX only. Next start.
    pub dht: bool,
    /// connections per torrent, inbound and outbound together; applies live
    pub max_peers_per_torrent: usize,
    /// bytes per second across all torrents, 0 for no limit; applies live
    pub download_limit: u64,
    pub upload_limit: u64,
    /// stop seeding once uploaded / torrent size reaches this, 0 to seed forever; applies
    /// live. The upload total survives restarts, so a torrent that seeded 1.5 last week and
    /// comes back with a limit of 2 keeps going until it has uploaded 2 sizes in all.
    pub seed_ratio_limit: f64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            listen_port: 6881,
            download_dir: default_download_dir(),
            dht: true,
            max_peers_per_torrent: 200,
            download_limit: 0,
            upload_limit: 0,
            seed_ratio_limit: 0.0,
        }
    }
}

impl Settings {
    /// The settings in `data_dir`, or the defaults if there's no file yet. A file that
    /// doesn't parse is reported and treated as absent rather than blocking startup.
    pub fn load(data_dir: &Path) -> Self {
        let path = data_dir.join(FILE_NAME);
        match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                tracing::warn!("ignoring {}: {e}", path.display());
                Self::default()
            }),
            Err(_) => Self::default(),
        }
    }

    pub fn save(&self, data_dir: &Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join(FILE_NAME);
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(&tmp, &path)?;
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
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
