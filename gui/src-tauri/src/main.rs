//! Desktop front end for the `downloader` library: a Tauri shell around `Session`. The
//! Svelte side (`gui/src`) renders what `torrents` reports and forwards what the user does
//! to `add_torrent`/`resume_torrent`/`remove_torrent`; no download logic lives here. The one
//! thing the library leaves to the UI is *finding* resume files: `Session::resume_dir` says
//! where they are, shared with the CLI.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use downloader::{
    Events, LogBuffer, ResumeSummary, Session, SessionConfig, SessionStatus, Settings, Telemetry, TorrentId,
    TorrentState, TraceSnapshot, data_dir, random_peer_id,
};
use serde::Serialize;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tauri::{Emitter, Manager, RunEvent, State};
use tokio::sync::Notify;

struct App {
    session: Mutex<Session>,
    logs: LogBuffer,
    /// the tracing setup, kept for the trace recorder and so OTLP export flushes on exit
    telemetry: Telemetry,
    /// the bus subscription, taken before the first torrent starts so nothing is missed;
    /// `forward_events` takes it out
    events: Mutex<Option<Events>>,
    /// the page has its listener up (see `events_ready`); until then the forwarder holds back
    page_ready: Arc<Notify>,
}

impl App {
    fn session(&self) -> MutexGuard<'_, Session> {
        self.session.lock().unwrap()
    }
}

#[derive(Serialize)]
struct TorrentRow {
    id: TorrentId,
    #[serde(flatten)]
    state: TorrentState,
}

#[tauri::command]
fn status(app: State<App>) -> SessionStatus {
    app.session().status()
}

#[derive(Serialize)]
struct LogChunk {
    seen: u64,
    lines: Vec<String>,
}

#[tauri::command]
fn torrents(app: State<App>) -> Vec<TorrentRow> {
    app.session()
        .torrents()
        .into_iter()
        .map(|(id, state)| TorrentRow { id, state })
        .collect()
}

#[tauri::command]
fn add_torrent(app: State<App>, source: String, root: String) -> TorrentId {
    app.session().add(source, root)
}

#[tauri::command]
fn resume_torrent(app: State<App>, path: String) -> TorrentId {
    app.session().resume(path)
}

#[tauri::command]
fn remove_torrent(app: State<App>, id: TorrentId, delete_files: bool) {
    app.session().remove(id, delete_files);
}

#[tauri::command]
fn select_files(app: State<App>, id: TorrentId, selected: Vec<bool>) {
    app.session().select_files(id, selected);
}

#[tauri::command]
fn pause_torrent(app: State<App>, id: TorrentId) {
    app.session().pause(id);
}

#[tauri::command]
fn unpause_torrent(app: State<App>, id: TorrentId) {
    app.session().unpause(id);
}

#[tauri::command]
fn recheck_torrent(app: State<App>, id: TorrentId) {
    app.session().recheck(id);
}

#[tauri::command]
fn set_sequential(app: State<App>, id: TorrentId, on: bool) {
    app.session().set_sequential(id, on);
}

#[tauri::command]
fn set_super_seed(app: State<App>, id: TorrentId, on: bool) {
    app.session().set_super_seed(id, on);
}

#[tauri::command]
fn resumable(app: State<App>) -> Vec<ResumeSummary> {
    app.session().resumable()
}

#[tauri::command]
fn logs_since(app: State<App>, seen: u64) -> LogChunk {
    let (seen, lines) = app.logs.lines_since(seen);
    LogChunk { seen, lines }
}

/// The torrent's spans (see `TraceRecorder`) that finished since `since`, and its open ones.
#[tauri::command]
fn traces(app: State<App>, info_hash: String, since: u64) -> TraceSnapshot {
    app.telemetry.traces.snapshot(&info_hash, since)
}

#[tauri::command]
fn clear_logs(app: State<App>) {
    app.logs.clear();
}

/// A front-end exception, logged where they can be seen (see `gui/src/main.ts`).
#[tauri::command]
fn report_error(message: String) {
    tracing::error!("front end: {message}");
}

/// The page is listening on the `events` channel; what happened since startup can flow.
#[tauri::command]
fn events_ready(app: State<App>) {
    app.page_ready.notify_one();
}

#[tauri::command]
fn settings(app: State<App>) -> Settings {
    app.session().settings()
}

/// Returns whether a restart is needed for everything to take effect.
#[tauri::command]
fn update_settings(app: State<App>, settings: Settings) -> Result<bool, String> {
    let mut session = app.session();
    let before = session.settings();
    let restart = settings.listen_port != before.listen_port
        || settings.dht != before.dht
        || settings.dht_read_only != before.dht_read_only
        || settings.encryption != before.encryption
        || settings.utp != before.utp
        || settings.port_mapping != before.port_mapping;
    session.update_settings(settings).map_err(|e| format!("{e:#}"))?;
    Ok(restart)
}

/// How long to gather library events before handing them to the webview as one batch: a
/// busy swarm emits hundreds a second, and one IPC message per event would swamp it.
const EVENT_BATCH_EVERY: Duration = Duration::from_millis(100);

/// Pumps the library's event bus into the webview as batches on the `events` channel; the
/// Svelte side (`lib/bus.svelte.ts`) accumulates them into its charts. Nothing is sent
/// before the page says it's listening (an emit with no listener is dropped); the bus
/// buffers what happens meanwhile, including every resumed torrent's start.
fn forward_events(app: tauri::AppHandle) {
    let state = app.state::<App>();
    let Some(mut events) = state.events.lock().unwrap().take() else {
        return;
    };
    let page_ready = state.page_ready.clone();
    tauri::async_runtime::spawn(async move {
        page_ready.notified().await;
        while let Some(first) = events.next().await {
            tokio::time::sleep(EVENT_BATCH_EVERY).await;
            let mut batch = vec![first];
            batch.extend(events.drain());
            if app.emit("events", &batch).is_err() {
                break;
            }
        }
    });
}

fn main() {
    // installed before anything that logs; the console panel shows what lands here
    let logs = LogBuffer::new(5_000);
    let telemetry = Telemetry::install(Box::new(logs.layer())).expect("failed to install tracing");
    let data_dir = data_dir();
    let mut session = match Session::new(SessionConfig {
        peer_id: random_peer_id(),
        settings: Settings::load(&data_dir),
        data_dir,
    }) {
        Ok(session) => session,
        // a second copy of the app, most likely: say so in a window rather than vanish
        Err(e) => {
            rfd::MessageDialog::new()
                .set_level(rfd::MessageLevel::Error)
                .set_title("downloader")
                .set_description(format!("{e:#}"))
                .show();
            std::process::exit(1);
        }
    };
    let events = session.subscribe();
    // everything from last time comes back, paused ones paused
    session.resume_all();
    if let Some(source) = std::env::args().nth(1) {
        let root = session.settings().download_dir;
        session.add(source, root);
    }

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_notification::init())
        .manage(App {
            session: Mutex::new(session),
            logs,
            telemetry,
            events: Mutex::new(Some(events)),
            page_ready: Arc::new(Notify::new()),
        })
        .invoke_handler(tauri::generate_handler![
            torrents,
            status,
            add_torrent,
            resume_torrent,
            remove_torrent,
            pause_torrent,
            unpause_torrent,
            recheck_torrent,
            set_sequential,
            set_super_seed,
            select_files,
            resumable,
            logs_since,
            traces,
            clear_logs,
            report_error,
            events_ready,
            settings,
            update_settings,
        ])
        // the run driver opens a given bottom pane (console, insights, traces) for its
        // screenshots; it's the same remembered choice the footer buttons make
        .on_page_load(|webview, payload| {
            if payload.event() == tauri::webview::PageLoadEvent::Finished
                && let Ok(pane) = std::env::var("DOWNLOADER_PANE")
            {
                let _ = webview.eval(format!(
                    "if (localStorage.getItem('pane') !== {pane:?}) {{ localStorage.setItem('pane', {pane:?}); location.reload() }}"
                ));
            }
        })
        .setup(|app| {
            forward_events(app.handle().clone());
            // a window launched from a script opens behind whatever is in front, and WebKit
            // stops painting a fully covered window, so a screenshot of it comes out blank;
            // the run driver sets this to keep the window on top while it captures
            if std::env::var_os("DOWNLOADER_WINDOW_ON_TOP").is_some()
                && let Some(window) = app.get_webview_window("main")
            {
                let _ = window.set_always_on_top(true);
            }
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building the tauri application")
        .run(|app, event| {
            // the session owns a tokio runtime, which must be stopped from a plain thread:
            // here, on the main thread, once the last window has closed
            if let RunEvent::ExitRequested { .. } = event {
                app.state::<App>().session().shutdown();
            }
        });
}
