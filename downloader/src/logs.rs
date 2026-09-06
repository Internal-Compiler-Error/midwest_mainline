//! A bounded, in-memory sink for `tracing` events, for a UI that wants to show them.
//!
//! The formatting happens here so the front end only ever sees finished lines; see
//! [`LogBuffer::install`].

use std::collections::VecDeque;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::Subscriber;
use tracing::field::{Field, Visit};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

/// The most recent `capacity` log lines. Cheap to clone; clones share the buffer.
#[derive(Clone)]
pub struct LogBuffer {
    lines: Arc<Mutex<VecDeque<String>>>,
    capacity: usize,
    /// lines ever pushed, so a reader can ask for "everything after the N-th"
    pushed: Arc<AtomicU64>,
    /// a copy of every line goes here when `DOWNLOADER_LOG_ADDR` is set, see `spawn_forwarder`
    forward: Option<std::sync::mpsc::Sender<String>>,
}

impl LogBuffer {
    /// Installs this buffer as the process's global `tracing` subscriber, keeping the last
    /// `capacity` events. Honours `RUST_LOG`, defaulting to `info`. Fails if something else
    /// already installed a global subscriber.
    pub fn install(capacity: usize) -> anyhow::Result<Self> {
        let buffer = Self {
            lines: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
            pushed: Arc::new(AtomicU64::new(0)),
            forward: std::env::var("DOWNLOADER_LOG_ADDR").ok().map(spawn_forwarder),
        };
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        tracing_subscriber::registry()
            .with(filter)
            .with(BufferLayer(buffer.clone()))
            .try_init()?;
        Ok(buffer)
    }

    /// A snapshot of the buffered lines, oldest first.
    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().unwrap().iter().cloned().collect()
    }

    /// The lines pushed after the first `seen`, oldest first, and the new count to pass back
    /// next time. Lines that have already fallen out of the buffer are skipped.
    pub fn lines_since(&self, seen: u64) -> (u64, Vec<String>) {
        let lines = self.lines.lock().unwrap();
        let total = self.pushed.load(Ordering::Acquire);
        let oldest = total - lines.len() as u64;
        let skip = seen.saturating_sub(oldest) as usize;
        (total, lines.iter().skip(skip).cloned().collect())
    }

    pub fn len(&self) -> usize {
        self.lines.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        self.lines.lock().unwrap().clear();
    }

    fn push(&self, line: String) {
        if let Some(forward) = &self.forward {
            let _ = forward.send(line.clone());
        }
        let mut lines = self.lines.lock().unwrap();
        if lines.len() == self.capacity {
            lines.pop_front();
        }
        lines.push_back(line);
        self.pushed.fetch_add(1, Ordering::Release);
    }
}

/// Streams log lines to a TCP listener at `addr`, one per line, so a GUI's console can be
/// watched from a terminal (`nc -l 9999`, then run the app with
/// `DOWNLOADER_LOG_ADDR=127.0.0.1:9999`). Reconnects when the listener goes away; lines
/// logged while nobody is listening are dropped, not queued.
fn spawn_forwarder(addr: String) -> std::sync::mpsc::Sender<String> {
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    std::thread::Builder::new()
        .name("log-forwarder".into())
        .spawn(move || {
            use std::io::Write;
            let mut conn: Option<std::net::TcpStream> = None;
            for line in rx {
                if conn.is_none() {
                    conn = std::net::TcpStream::connect(&addr).ok();
                }
                if let Some(stream) = &mut conn
                    && writeln!(stream, "{line}").is_err()
                {
                    conn = None;
                }
            }
        })
        .expect("spawning the log forwarder thread");
    tx
}

struct BufferLayer(LogBuffer);

impl<S: Subscriber> Layer<S> for BufferLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        let mut line = format!("{} {:>5} {}: ", clock(), meta.level(), meta.target());
        let mut fields = LineWriter {
            line: &mut line,
            message_written: false,
        };
        event.record(&mut fields);
        self.0.push(line);
    }
}

/// Appends the `message` field as-is and every other field as ` key=value`.
struct LineWriter<'a> {
    line: &'a mut String,
    message_written: bool,
}

impl Visit for LineWriter<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" && !self.message_written {
            self.line.push_str(value);
            self.message_written = true;
        } else {
            let _ = write!(self.line, " {}={value}", field.name());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" && !self.message_written {
            let _ = write!(self.line, "{value:?}");
            self.message_written = true;
        } else {
            let _ = write!(self.line, " {}={value:?}", field.name());
        }
    }
}

/// Wall-clock time of day, UTC, `HH:MM:SS`. No date: the buffer only ever spans one sitting.
fn clock() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
        % 86_400;
    format!("{:02}:{:02}:{:02}", secs / 3600, secs % 3600 / 60, secs % 60)
}

#[cfg(test)]
mod test {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt;

    #[test]
    fn formats_events_and_keeps_only_the_newest() {
        let buffer = LogBuffer {
            lines: Arc::new(Mutex::new(VecDeque::new())),
            capacity: 2,
            pushed: Arc::new(AtomicU64::new(0)),
            forward: None,
        };
        // a local subscriber rather than the global one, so tests don't fight over it
        let subscriber = tracing_subscriber::registry().with(BufferLayer(buffer.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("first");
            tracing::warn!(peer = "1.2.3.4:6881", "second {}", 2);
            tracing::info!("third");
        });

        let lines = buffer.lines();
        assert_eq!(lines.len(), 2, "capacity is enforced: {lines:?}");
        assert!(
            lines[0].contains(" WARN downloader::logs::test: second 2 peer=1.2.3.4:6881"),
            "{}",
            lines[0]
        );
        assert!(
            lines[1].ends_with(" INFO downloader::logs::test: third"),
            "{}",
            lines[1]
        );
        assert_eq!(&lines[1][2..3], ":", "starts with an HH:MM:SS clock");

        let (seen, new) = buffer.lines_since(0);
        assert_eq!(seen, 3, "three were pushed even though only two are kept");
        assert_eq!(new, lines, "asking from the start skips what fell out of the buffer");
        let (seen, new) = buffer.lines_since(2);
        assert_eq!((seen, new.len()), (3, 1), "only the third is new after the second");
        assert!(buffer.lines_since(3).1.is_empty());

        buffer.clear();
        assert!(buffer.is_empty());
    }
}
