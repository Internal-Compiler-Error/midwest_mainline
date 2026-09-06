//! Where the client keeps its own files. Downloads go wherever the user says per torrent;
//! this is for the rest: resume files and the DHT's node database.

use std::path::PathBuf;

/// `$DOWNLOADER_DATA_DIR`, else the platform's per-user data directory (`~/Library/Application
/// Support/downloader` on macOS, `~/.local/share/downloader` on Linux), else the current
/// directory. A GUI launched from Finder starts in `/`, so relative paths can't be the default.
pub fn data_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("DOWNLOADER_DATA_DIR") {
        return PathBuf::from(dir);
    }
    dirs::data_local_dir()
        .map(|dir| dir.join("downloader"))
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Where a new torrent goes unless the user picks somewhere else: the platform's Downloads
/// folder, else the current directory.
pub fn default_download_dir() -> PathBuf {
    dirs::download_dir()
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."))
}
