//! Desktop front end for the `downloader` library: a Tauri shell around `Session`. The
//! Svelte side (`gui/src`) renders what `torrents` reports and forwards what the user does
//! to `add_torrent`/`resume_torrent`/`remove_torrent`; no download logic lives here. The one
//! thing the library leaves to the UI is *finding* resume files: `Session::resume_dir` says
//! where they are, shared with the CLI.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use downloader::{LogBuffer, Progress, Session, SessionConfig, TorrentId, TorrentState, data_dir, list_resume_files};
use serde::Serialize;
use std::path::PathBuf;
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
    Failed { source: String, error: String },
}

/// `Progress` field for field; the library stays free of serde.
#[derive(Serialize)]
struct ProgressDto {
    name: String,
    root: String,
    files: Vec<String>,
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
}

impl From<Progress> for ProgressDto {
    fn from(p: Progress) -> Self {
        Self {
            name: p.name,
            root: p.root,
            files: p.files,
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
fn remove_torrent(app: State<App>, id: TorrentId) {
    app.session.lock().unwrap().remove(id);
}

#[tauri::command]
fn resumable(app: State<App>) -> Vec<ResumableDto> {
    let dir = app.session.lock().unwrap().resume_dir().to_path_buf();
    list_resume_files(&dir)
        .into_iter()
        .map(|r| ResumableDto {
            path: r.path.display().to_string(),
            name: r.name,
            root: r.root.display().to_string(),
            verified_pieces: r.verified_pieces,
            total_pieces: r.total_pieces,
            total_size: r.total_size,
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
fn default_download_dir() -> String {
    downloader::default_download_dir().display().to_string()
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
    let mut session = Session::new(SessionConfig {
        peer_id: random_peer_id(),
        port: 6881,
        data_dir: data_dir(),
        dht: true,
    })
    .expect("failed to start a session");
    if let Some(source) = std::env::args().nth(1) {
        session.add(source, default_download_dir());
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
            resumable,
            logs_since,
            clear_logs,
            default_download_dir,
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
