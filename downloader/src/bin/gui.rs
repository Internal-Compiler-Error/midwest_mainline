//! Desktop front end for the `downloader` library. Takes a `.torrent` file, a magnet link, or
//! a resume file from an earlier run.
//!
//! This file deliberately contains no download logic: it forwards user input to
//! `Session::start`/`resume`/`stop`, and renders whatever `Session::state()` reports.
//! Everything else -- the runtime, resolving magnets, driving the client, transfer rates,
//! writing resume files -- lives in the library. The one thing the library leaves to the UI is
//! *finding* resume files: this one keeps them in `RESUME_DIR` and lists whatever is there.

use downloader::{Progress, ResumeSummary, Session, SessionState, human_bytes, is_magnet_uri, list_resume_files};
use eframe::egui;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Resume files go next to the downloads, and are shared with the CLI.
const RESUME_DIR: &str = "resume";

fn main() -> eframe::Result {
    let mut session = Session::new(random_peer_id(), 6881).expect("failed to start a session");
    session.set_resume_dir(RESUME_DIR);
    let download_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    if let Some(source) = std::env::args().nth(1) {
        session.start(source, download_dir.clone());
    }

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([660.0, 500.0])
            .with_min_inner_size([460.0, 360.0])
            .with_title("downloader"),
        ..Default::default()
    };

    eframe::run_native(
        "downloader",
        options,
        Box::new(move |_cc| {
            Ok(Box::new(App {
                session,
                input: String::new(),
                download_dir,
                resumable: list_resume_files(Path::new(RESUME_DIR)),
            }))
        }),
    )
}

/// A fully random peer id, Azureus-style ("-DL0100-" + 12 random bytes).
fn random_peer_id() -> [u8; 20] {
    let mut id = *b"-DL0100-............";
    rand::RngCore::fill_bytes(&mut rand::rng(), &mut id[8..]);
    id
}

struct App {
    session: Session,
    input: String,
    /// where the next torrent goes; the folder picker starts here and updates it
    download_dir: PathBuf,
    /// what's in `RESUME_DIR`; refreshed on demand, not every frame
    resumable: Vec<ResumeSummary>,
}

impl App {
    /// Asks where this torrent should go, then starts it. Cancelling the picker cancels the add.
    fn start(&mut self, source: String) {
        let picked = rfd::FileDialog::new()
            .set_title("Download into…")
            .set_directory(&self.download_dir)
            .pick_folder();
        if let Some(dir) = picked {
            self.download_dir = dir.clone();
            self.session.start(source, dir);
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // progress happens on the session's own threads, so nothing wakes the UI on its own
        ctx.request_repaint_after(Duration::from_millis(250));

        egui::TopBottomPanel::top("toolbar").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                if ui.button("Open .torrent…").clicked() {
                    if let Some(path) = rfd::FileDialog::new().add_filter("torrent", &["torrent"]).pick_file() {
                        self.start(path.display().to_string());
                    }
                }
                ui.separator();

                let ready = is_magnet_uri(&self.input);
                let field = ui.add(
                    egui::TextEdit::singleline(&mut self.input)
                        .hint_text("or paste a magnet: link")
                        .desired_width(ui.available_width() - 70.0),
                );
                let entered = field.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                if ready && (entered || ui.add_enabled(ready, egui::Button::new("Add")).clicked()) {
                    let magnet = std::mem::take(&mut self.input);
                    self.start(magnet);
                }
            });
            ui.add_space(4.0);
        });

        egui::CentralPanel::default().show(ctx, |ui| match self.session.state() {
            SessionState::Idle => {
                ui.label("Open a .torrent file, or paste a magnet link above.");
                ui.add_space(12.0);
                if let Some(path) = draw_resumable(ui, &mut self.resumable) {
                    self.session.resume(path);
                }
            }
            SessionState::Failed { error } => {
                ui.colored_label(egui::Color32::from_rgb(220, 80, 80), format!("⚠ {error}"));
            }
            SessionState::Resolving { source, elapsed } => draw_resolving(ui, &source, elapsed),
            SessionState::Downloading(progress) => draw_progress(ui, &progress),
        });
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        self.session.shutdown();
    }
}

/// Lists earlier downloads that can be picked back up; returns the one the user chose.
fn draw_resumable(ui: &mut egui::Ui, resumable: &mut Vec<ResumeSummary>) -> Option<std::path::PathBuf> {
    ui.horizontal(|ui| {
        ui.strong("Resume an earlier download");
        if ui
            .small_button("⟳")
            .on_hover_text(format!("rescan ./{RESUME_DIR}"))
            .clicked()
        {
            *resumable = list_resume_files(Path::new(RESUME_DIR));
        }
    });
    if resumable.is_empty() {
        ui.weak(format!("(nothing in ./{RESUME_DIR})"));
        return None;
    }

    let mut chosen = None;
    egui::ScrollArea::vertical().show(ui, |ui| {
        egui::Grid::new("resumable")
            .num_columns(3)
            .spacing([12.0, 6.0])
            .show(ui, |ui| {
                for entry in resumable.iter() {
                    ui.label(&entry.name).on_hover_text(entry.root.display().to_string());
                    ui.add(
                        egui::ProgressBar::new(entry.fraction())
                            .show_percentage()
                            .desired_width(120.0),
                    );
                    if ui.button("Resume").clicked() {
                        chosen = Some(entry.path.clone());
                    }
                    ui.end_row();
                }
            });
    });
    chosen
}

fn draw_resolving(ui: &mut egui::Ui, source: &str, elapsed: Duration) {
    ui.centered_and_justified(|ui| {
        ui.vertical_centered(|ui| {
            ui.add_space(40.0);
            ui.spinner();
            ui.add_space(8.0);
            ui.label(if is_magnet_uri(source) {
                "Fetching metadata from peers…"
            } else {
                "Loading…"
            });
            ui.weak(format!("{:.0}s", elapsed.as_secs_f32()));
            // with no DHT, a magnet whose trackers are all dead has no fallback
            if is_magnet_uri(source) && elapsed > Duration::from_secs(20) {
                ui.add_space(6.0);
                ui.weak("(still looking — needs a peer from one of the magnet's trackers)");
            }
        });
    });
}

fn draw_progress(ui: &mut egui::Ui, p: &Progress) {
    ui.horizontal(|ui| {
        ui.heading(if p.completed { "Seeding" } else { "Downloading" });
        if p.completed {
            ui.colored_label(egui::Color32::from_rgb(80, 190, 120), "✔ complete");
        }
    });
    ui.add_space(6.0);

    ui.add(
        egui::ProgressBar::new(p.fraction())
            .show_percentage()
            .desired_height(20.0)
            .fill(if p.completed {
                egui::Color32::from_rgb(80, 190, 120)
            } else {
                egui::Color32::from_rgb(80, 150, 220)
            }),
    );
    ui.label(format!("{} / {} pieces verified", p.verified_pieces, p.total_pieces));

    ui.add_space(10.0);
    egui::Grid::new("transfer")
        .num_columns(2)
        .spacing([16.0, 6.0])
        .show(ui, |ui| {
            for (label, value) in [
                ("Location", p.root.clone()),
                (
                    "Downloaded",
                    format!(
                        "{}  ({}/s)",
                        human_bytes(p.downloaded),
                        human_bytes(p.download_bps as u64)
                    ),
                ),
                (
                    "Uploaded",
                    format!("{}  ({}/s)", human_bytes(p.uploaded), human_bytes(p.upload_bps as u64)),
                ),
                ("Remaining", human_bytes(p.left)),
                ("Total size", human_bytes(p.total_size)),
            ] {
                ui.strong(label);
                ui.label(value);
                ui.end_row();
            }
        });

    ui.add_space(10.0);
    ui.strong(format!("Files ({})", p.files.len()));
    egui::ScrollArea::vertical().show(ui, |ui| {
        for name in &p.files {
            ui.label(name);
        }
    });
}
