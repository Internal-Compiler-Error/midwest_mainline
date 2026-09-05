//! A small desktop GUI for downloading a torrent, built on top of the `downloader` library
//! crate. Pick a `.torrent` file (or pass one on the command line) and watch it go.
//!
//! Threading model: eframe owns the main thread and drives the render loop, so the tokio
//! runtime that runs the BitTorrent client lives alongside it and is only ever touched through
//! a `watch` channel. Each frame just reads the latest stats snapshot -- a non-blocking
//! `borrow()` -- which keeps the UI thread free of any awaiting or locking.

use downloader::{BtClient, Identity, Torrent, TorrentSwarmStats, parse_torrent};
use eframe::egui;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// A fully random peer id, Azureus-style ("-DL0100-" + 12 random bytes). Real BEP 42-style
/// IP-derived ids are a DHT concern (see `random_idv4` in `main.rs`); a UI has no need for that.
fn random_peer_id() -> [u8; 20] {
    let mut id = *b"-DL0100-............";
    rand::RngCore::fill_bytes(&mut rand::rng(), &mut id[8..]);
    id
}

fn main() -> eframe::Result {
    let initial = std::env::args().nth(1).map(PathBuf::from);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([640.0, 460.0])
            .with_min_inner_size([420.0, 320.0])
            .with_title("downloader"),
        ..Default::default()
    };

    eframe::run_native(
        "downloader",
        options,
        Box::new(move |_cc| Ok(Box::new(App::new(initial)))),
    )
}

/// Everything belonging to one in-progress torrent. Absent until a file is opened.
struct Active {
    torrent: Torrent,
    stats: watch::Receiver<TorrentSwarmStats>,
    shutdown: CancellationToken,
    file_names: Vec<String>,
    speed: Speed,
}

struct App {
    /// Kept alive for the whole run: dropping the runtime aborts the client's tasks. `Option`
    /// only so `on_exit` can take it and shut it down with a bounded timeout -- a plain drop
    /// waits on in-flight tasks indefinitely, which is exactly how a UI ends up unquittable.
    rt: Option<Runtime>,
    /// spawn through this rather than through `rt`, so the `Option` above stays out of the way
    handle: tokio::runtime::Handle,
    active: Option<Active>,
    error: Option<String>,
}

impl App {
    fn new(initial: Option<PathBuf>) -> Self {
        let rt = Runtime::new().expect("failed to start a tokio runtime");
        let mut app = Self {
            handle: rt.handle().clone(),
            rt: Some(rt),
            active: None,
            error: None,
        };
        if let Some(path) = initial {
            app.start(path);
        }
        app
    }

    fn start(&mut self, path: PathBuf) {
        match self.try_start(path) {
            Ok(active) => {
                self.active = Some(active);
                self.error = None;
            }
            Err(e) => self.error = Some(format!("{e:#}")),
        }
    }

    fn try_start(&mut self, path: PathBuf) -> anyhow::Result<Active> {
        // stop whatever was running first, so its trackers get a courtesy event=stopped and
        // its listening port is released before the new torrent tries to bind the same one
        if let Some(previous) = self.active.take() {
            previous.shutdown.cancel();
        }

        let meta_bytes = std::fs::read(&path)?;
        let torrent = parse_torrent(&meta_bytes)?;

        let mut client = BtClient::new(Identity {
            peer_id: random_peer_id(),
            serving: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 6881).into(),
        });
        client.add_torrent(torrent.clone())?;

        // both must be taken before `work()` consumes the client
        let stats = client.stats(&torrent).expect("torrent was just added");
        let shutdown = client.shutdown_token();
        self.handle.spawn(client.work());

        let file_names = torrent.files.iter().map(|(_, p)| p.display().to_string()).collect();
        Ok(Active {
            torrent,
            stats,
            shutdown,
            file_names,
            speed: Speed::new(),
        })
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // the client makes progress on the runtime's own threads, so nothing wakes the UI on
        // its own -- repaint on a timer to keep the numbers live
        ctx.request_repaint_after(Duration::from_millis(250));

        egui::TopBottomPanel::top("toolbar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.add_space(4.0);
                if ui.button("Open .torrent…").clicked() {
                    if let Some(path) = rfd::FileDialog::new().add_filter("torrent", &["torrent"]).pick_file() {
                        self.start(path);
                    }
                }
                if let Some(active) = &self.active {
                    ui.separator();
                    ui.label(&active.file_names[0]);
                }
            });
            ui.add_space(4.0);
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            if let Some(err) = &self.error {
                ui.colored_label(egui::Color32::from_rgb(220, 80, 80), format!("⚠ {err}"));
                ui.separator();
            }

            let Some(active) = &mut self.active else {
                ui.centered_and_justified(|ui| {
                    ui.label("Open a .torrent file to begin.");
                });
                return;
            };

            let stats = active.stats.borrow().clone();
            active.speed.update(&stats);
            draw_torrent(ui, &active.torrent, &active.file_names, &stats, &active.speed);
        });
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        // give the trackers a courtesy event=stopped on the way out, but cap how long we'll
        // wait for it -- a slow/unreachable tracker must not be able to wedge the exit
        if let Some(active) = &self.active {
            active.shutdown.cancel();
        }
        if let Some(rt) = self.rt.take() {
            rt.shutdown_timeout(Duration::from_millis(500));
        }
    }
}

fn draw_torrent(
    ui: &mut egui::Ui,
    torrent: &Torrent,
    file_names: &[String],
    stats: &TorrentSwarmStats,
    speed: &Speed,
) {
    let verified = stats.verified_cnt();
    let total_pieces = stats.total_pieces().max(1);
    let ratio = (verified as f32 / total_pieces as f32).clamp(0.0, 1.0);

    ui.horizontal(|ui| {
        ui.heading(if stats.completed { "Seeding" } else { "Downloading" });
        if stats.completed {
            ui.colored_label(egui::Color32::from_rgb(80, 190, 120), "✔ complete");
        }
    });
    ui.add_space(6.0);

    ui.add(
        egui::ProgressBar::new(ratio)
            .show_percentage()
            .desired_height(20.0)
            .fill(if stats.completed {
                egui::Color32::from_rgb(80, 190, 120)
            } else {
                egui::Color32::from_rgb(80, 150, 220)
            }),
    );
    ui.label(format!("{verified} / {total_pieces} pieces verified"));

    ui.add_space(10.0);
    egui::Grid::new("transfer").num_columns(2).spacing([16.0, 6.0]).show(ui, |ui| {
        ui.strong("Downloaded");
        ui.label(format!(
            "{}  ({}/s)",
            human_bytes(stats.downloaded),
            human_bytes(speed.download_bps as u64)
        ));
        ui.end_row();

        ui.strong("Uploaded");
        ui.label(format!(
            "{}  ({}/s)",
            human_bytes(stats.uploaded),
            human_bytes(speed.upload_bps as u64)
        ));
        ui.end_row();

        ui.strong("Remaining");
        ui.label(human_bytes(stats.left as u64));
        ui.end_row();

        ui.strong("Total size");
        ui.label(human_bytes(torrent.total_size));
        ui.end_row();
    });

    ui.add_space(10.0);
    ui.strong(format!("Files ({})", file_names.len()));
    egui::ScrollArea::vertical().show(ui, |ui| {
        for name in file_names {
            ui.label(name);
        }
    });
}

struct Speed {
    last_sample: Instant,
    last_downloaded: u64,
    last_uploaded: u64,
    download_bps: f64,
    upload_bps: f64,
}

impl Speed {
    fn new() -> Self {
        Self {
            last_sample: Instant::now(),
            last_downloaded: 0,
            last_uploaded: 0,
            download_bps: 0.0,
            upload_bps: 0.0,
        }
    }

    /// Recomputes the rolling rate at most once every 500ms. `update` is called every frame
    /// (~60/s), and a window that short would divide a handful of bytes by ~16ms and produce a
    /// wildly jumpy figure.
    fn update(&mut self, stats: &TorrentSwarmStats) {
        let elapsed = self.last_sample.elapsed();
        if elapsed < Duration::from_millis(500) {
            return;
        }
        self.download_bps = stats.downloaded.saturating_sub(self.last_downloaded) as f64 / elapsed.as_secs_f64();
        self.upload_bps = stats.uploaded.saturating_sub(self.last_uploaded) as f64 / elapsed.as_secs_f64();
        self.last_downloaded = stats.downloaded;
        self.last_uploaded = stats.uploaded;
        self.last_sample = Instant::now();
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{size:.2} {}", UNITS[unit])
    }
}
