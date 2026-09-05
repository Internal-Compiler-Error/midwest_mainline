//! A bounded, in-memory sink for `tracing` events, for a UI that wants to show them.
//!
//! The formatting happens here so the front end only ever sees finished lines; see
//! [`LogBuffer::install`].

use std::collections::VecDeque;
use std::fmt::Write;
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
}

impl LogBuffer {
    /// Installs this buffer as the process's global `tracing` subscriber, keeping the last
    /// `capacity` events. Honours `RUST_LOG`, defaulting to `info`. Fails if something else
    /// already installed a global subscriber.
    pub fn install(capacity: usize) -> anyhow::Result<Self> {
        let buffer = Self {
            lines: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
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
        let mut lines = self.lines.lock().unwrap();
        if lines.len() == self.capacity {
            lines.pop_front();
        }
        lines.push_back(line);
    }
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

        buffer.clear();
        assert!(buffer.is_empty());
    }
}
