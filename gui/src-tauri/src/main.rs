//! Desktop front end for the `downloader` library: a Tauri shell around `Session`. The
//! Svelte side (`gui/src`) renders what `torrents` reports and forwards what the user does
//! to `add_torrent`/`resume_torrent`/`remove_torrent`; no download logic lives here. The one
//! thing the library leaves to the UI is *finding* resume files: `Session::resume_dir` says
//! where they are, shared with the CLI.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use downloader::{
    Encryption, FileInfo, LogBuffer, PeerInfo, Progress, Session, SessionConfig, Settings, TorrentId, TorrentState,
    data_dir,
};
use serde::Serialize;
use std::sync::Mutex;
use tauri::{Manager, RunEvent, State};

struct App {
    session: Mutex<Session>,
    logs: LogBuffer,
}

#[derive(Serialize)]
struct TorrentRow {
    id: TorrentId,
    #[serde(flatten)]
    state: StateDto,
}

/// `TorrentState` in the shape JS wants: a tag, milliseconds, and plain strings.
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StateDto {
    Resolving { source: String, elapsed_ms: u64 },
    Downloading(ProgressDto),
    Paused(ProgressDto),
    Failed { source: String, error: String },
}

/// `Progress` field for field; the library stays free of serde.
#[derive(Serialize)]
struct ProgressDto {
    name: String,
    root: String,
    files: Vec<FileDto>,
    total_size: u64,
    downloaded: u64,
    wasted: u64,
    uploaded: u64,
    left: u64,
    verified_pieces: usize,
    total_pieces: usize,
    completed: bool,
    download_bps: f64,
    upload_bps: f64,
    peers: Vec<PeerDto>,
}

#[derive(Serialize)]
struct FileDto {
    path: String,
    size: u64,
    selected: bool,
}

impl From<FileInfo> for FileDto {
    fn from(f: FileInfo) -> Self {
        Self {
            path: f.path,
            size: f.size,
            selected: f.selected,
        }
    }
}

#[derive(Serialize)]
struct PeerDto {
    addr: String,
    client: String,
    progress: f32,
    downloaded: u64,
    uploaded: u64,
    download_bps: f64,
    upload_bps: f64,
    flags: String,
}

impl From<PeerInfo> for PeerDto {
    fn from(p: PeerInfo) -> Self {
        Self {
            addr: p.addr,
            client: p.client,
            progress: p.progress,
            downloaded: p.downloaded,
            uploaded: p.uploaded,
            download_bps: p.download_bps,
            upload_bps: p.upload_bps,
            flags: p.flags,
        }
    }
}

impl From<Progress> for ProgressDto {
    fn from(p: Progress) -> Self {
        Self {
            name: p.name,
            root: p.root,
            files: p.files.into_iter().map(FileDto::from).collect(),
            total_size: p.total_size,
            downloaded: p.downloaded,
            wasted: p.wasted,
            uploaded: p.uploaded,
            left: p.left,
            verified_pieces: p.verified_pieces,
            total_pieces: p.total_pieces,
            completed: p.completed,
            download_bps: p.download_bps,
            upload_bps: p.upload_bps,
            peers: p.peers.into_iter().map(PeerDto::from).collect(),
        }
    }
}

impl From<TorrentState> for StateDto {
    fn from(state: TorrentState) -> Self {
        match state {
            TorrentState::Resolving { source, elapsed } => StateDto::Resolving {
                source,
                elapsed_ms: elapsed.as_millis() as u64,
            },
            TorrentState::Downloading(progress) => StateDto::Downloading(progress.into()),
            TorrentState::Paused(progress) => StateDto::Paused(progress.into()),
            TorrentState::Failed { source, error } => StateDto::Failed { source, error },
        }
    }
}

#[derive(Serialize)]
struct ResumableDto {
    path: String,
    name: String,
    root: String,
    verified_pieces: usize,
    total_pieces: usize,
    total_size: u64,
    paused: bool,
}

#[derive(Serialize)]
struct LogChunk {
    seen: u64,
    lines: Vec<String>,
}

#[tauri::command]
fn torrents(app: State<App>) -> Vec<TorrentRow> {
    let mut session = app.session.lock().unwrap();
    session
        .torrents()
        .into_iter()
        .map(|(id, state)| TorrentRow {
            id,
            state: state.into(),
        })
        .collect()
}

#[tauri::command]
fn add_torrent(app: State<App>, source: String, root: String) -> TorrentId {
    app.session.lock().unwrap().add(source, root)
}

#[tauri::command]
fn resume_torrent(app: State<App>, path: String) -> TorrentId {
    app.session.lock().unwrap().resume(path)
}

#[tauri::command]
fn remove_torrent(app: State<App>, id: TorrentId, delete_files: bool) {
    app.session.lock().unwrap().remove(id, delete_files);
}

#[tauri::command]
fn select_files(app: State<App>, id: TorrentId, selected: Vec<bool>) {
    app.session.lock().unwrap().select_files(id, selected);
}

#[tauri::command]
fn pause_torrent(app: State<App>, id: TorrentId) {
    app.session.lock().unwrap().pause(id);
}

#[tauri::command]
fn unpause_torrent(app: State<App>, id: TorrentId) {
    app.session.lock().unwrap().unpause(id);
}

#[tauri::command]
fn resumable(app: State<App>) -> Vec<ResumableDto> {
    app.session
        .lock()
        .unwrap()
        .resumable()
        .into_iter()
        .map(|r| ResumableDto {
            path: r.path.display().to_string(),
            name: r.name,
            root: r.root.display().to_string(),
            verified_pieces: r.verified_pieces,
            total_pieces: r.total_pieces,
            total_size: r.total_size,
            paused: r.paused,
        })
        .collect()
}

#[tauri::command]
fn logs_since(app: State<App>, seen: u64) -> LogChunk {
    let (seen, lines) = app.logs.lines_since(seen);
    LogChunk { seen, lines }
}

#[tauri::command]
fn clear_logs(app: State<App>) {
    app.logs.clear();
}

#[tauri::command]
fn default_download_dir(app: State<App>) -> String {
    app.session
        .lock()
        .unwrap()
        .settings()
        .download_dir
        .display()
        .to_string()
}

/// `Settings` field for field, in the units the dialog edits.
#[derive(Serialize, serde::Deserialize)]
struct SettingsDto {
    listen_port: u16,
    download_dir: String,
    dht: bool,
    max_peers_per_torrent: usize,
    download_limit: u64,
    upload_limit: u64,
    seed_ratio_limit: f64,
    encryption: Encryption,
    utp: bool,
}

impl From<Settings> for SettingsDto {
    fn from(s: Settings) -> Self {
        Self {
            listen_port: s.listen_port,
            download_dir: s.download_dir.display().to_string(),
            dht: s.dht,
            max_peers_per_torrent: s.max_peers_per_torrent,
            download_limit: s.download_limit,
            upload_limit: s.upload_limit,
            seed_ratio_limit: s.seed_ratio_limit,
            encryption: s.encryption,
            utp: s.utp,
        }
    }
}

impl From<SettingsDto> for Settings {
    fn from(s: SettingsDto) -> Self {
        Self {
            listen_port: s.listen_port,
            download_dir: s.download_dir.into(),
            dht: s.dht,
            max_peers_per_torrent: s.max_peers_per_torrent,
            download_limit: s.download_limit,
            upload_limit: s.upload_limit,
            seed_ratio_limit: s.seed_ratio_limit,
            encryption: s.encryption,
            utp: s.utp,
        }
    }
}

#[tauri::command]
fn settings(app: State<App>) -> SettingsDto {
    app.session.lock().unwrap().settings().into()
}

/// Returns whether a restart is needed for everything to take effect.
#[tauri::command]
fn update_settings(app: State<App>, settings: SettingsDto) -> Result<bool, String> {
    let mut session = app.session.lock().unwrap();
    let before = session.settings();
    let settings: Settings = settings.into();
    let restart = settings.listen_port != before.listen_port
        || settings.dht != before.dht
        || settings.encryption != before.encryption
        || settings.utp != before.utp;
    session.update_settings(settings).map_err(|e| format!("{e:#}"))?;
    Ok(restart)
}

/// A fully random peer id, Azureus-style ("-DL0100-" + 12 random bytes).
fn random_peer_id() -> [u8; 20] {
    let mut id = *b"-DL0100-............";
    rand::RngCore::fill_bytes(&mut rand::rng(), &mut id[8..]);
    id
}

fn main() {
    // installed before anything that logs; the console panel shows what lands here
    let logs = LogBuffer::install(5_000).expect("failed to install the log buffer");
    let data_dir = data_dir();
    let mut session = Session::new(SessionConfig {
        peer_id: random_peer_id(),
        settings: Settings::load(&data_dir),
        data_dir,
    })
    .expect("failed to start a session");
    // everything from last time comes back, paused ones paused
    session.resume_all();
    if let Some(source) = std::env::args().nth(1) {
        let root = session.settings().download_dir;
        session.add(source, root);
    }

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(App {
            session: Mutex::new(session),
            logs,
        })
        .invoke_handler(tauri::generate_handler![
            torrents,
            add_torrent,
            resume_torrent,
            remove_torrent,
            pause_torrent,
            unpause_torrent,
            select_files,
            resumable,
            logs_since,
            clear_logs,
            default_download_dir,
            settings,
            update_settings,
        ])
        .build(tauri::generate_context!())
        .expect("error while building the tauri application")
        .run(|app, event| {
            // the session owns a tokio runtime, which must be stopped from a plain thread:
            // here, on the main thread, once the last window has closed
            if let RunEvent::ExitRequested { .. } = event {
                app.state::<App>().session.lock().unwrap().shutdown();
            }
        });
}
