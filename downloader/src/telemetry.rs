//! Where `tracing` goes: a console of the caller's choosing, an in-process record of every
//! torrent's spans for a UI to draw ([`TraceRecorder`]), and, when `OTEL_EXPORTER_OTLP_ENDPOINT`
//! is set, OpenTelemetry export to a collector such as Jaeger.
//!
//! The spans worth recording are the ones tagged `info_hash` (a dial, a tracker announce, a DHT
//! lookup, a peer connection, a piece; see `torrent_swarm`, `announcer`, `metadata`) and
//! whatever happens inside them. Everything else is left to the console.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

/// Finished spans kept per torrent; a 1.5 GB download makes about 3000 piece spans and a
/// few thousand dials.
const SPANS_PER_TORRENT: usize = 20_000;
/// Events kept per span, the first ones; a span that logs more is cut short.
const EVENTS_PER_SPAN: usize = 64;

pub type ConsoleLayer = Box<dyn Layer<Registry> + Send + Sync>;

/// The installed telemetry. Dropping it flushes whatever OTLP export hasn't gone out yet.
pub struct Telemetry {
    pub traces: TraceRecorder,
    otlp: Option<SdkTracerProvider>,
}

impl Telemetry {
    /// Installs the process's global `tracing` subscriber: `console` filtered by `RUST_LOG`
    /// (default `info`), the trace recorder, and OTLP export if `OTEL_EXPORTER_OTLP_ENDPOINT`
    /// is set (`OTEL_EXPORTER_OTLP_PROTOCOL` and friends are honoured; the default is
    /// http/protobuf on port 4318). Each has its own filter, so a quiet console doesn't starve
    /// the traces of the debug events inside spans.
    pub fn install(console: ConsoleLayer) -> anyhow::Result<Self> {
        let console_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let traces = TraceRecorder::new(SPANS_PER_TORRENT);
        let in_spans = || {
            Targets::new()
                .with_target("downloader", Level::DEBUG)
                .with_target("midwest_mainline", Level::WARN)
        };
        let (otlp_layer, otlp) = match otlp_provider() {
            Ok(Some(provider)) => {
                let tracer = provider.tracer("downloader");
                let layer = tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    .with_filter(in_spans());
                (Some(layer), Some(provider))
            }
            Ok(None) => (None, None),
            Err(e) => {
                eprintln!("OpenTelemetry export is off: {e:#}");
                (None, None)
            }
        };
        tracing_subscriber::registry()
            .with(console.with_filter(console_filter))
            .with(traces.layer().with_filter(in_spans()))
            .with(otlp_layer)
            .try_init()?;
        if otlp.is_some() {
            tracing::info!("exporting traces over OTLP");
        }
        Ok(Self { traces, otlp })
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Some(provider) = self.otlp.take() {
            let _ = provider.shutdown();
        }
    }
}

fn otlp_provider() -> anyhow::Result<Option<SdkTracerProvider>> {
    if std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_none()
        && std::env::var_os("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_none()
    {
        return Ok(None);
    }
    let exporter = opentelemetry_otlp::SpanExporter::builder().with_http().build()?;
    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name("downloader")
        .build();
    Ok(Some(
        SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build(),
    ))
}

/// One span of a torrent's work, open or finished.
#[derive(Debug, Clone, Serialize)]
pub struct TraceSpan {
    /// unique for the process's lifetime (tracing's own ids are reused)
    pub id: u64,
    pub parent: Option<u64>,
    pub name: &'static str,
    /// unix milliseconds
    pub start_ms: f64,
    /// `None` while the span is still open
    pub end_ms: Option<f64>,
    /// every field but `info_hash`, formatted
    pub fields: Vec<(&'static str, String)>,
    pub events: Vec<TraceEvent>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceEvent {
    pub at_ms: f64,
    pub level: &'static str,
    pub message: String,
}

/// What [`TraceRecorder::snapshot`] returns: spans finished since the caller's last look,
/// and every span still open.
#[derive(Debug, Clone, Serialize)]
pub struct TraceSnapshot {
    /// pass back as `since` next time
    pub seq: u64,
    pub finished: Vec<TraceSpan>,
    pub open: Vec<TraceSpan>,
}

/// Every torrent's spans, the open ones and the last `SPANS_PER_TORRENT` finished ones. Cheap
/// to clone; clones share the record.
#[derive(Clone)]
pub struct TraceRecorder {
    state: Arc<Mutex<State>>,
    next_id: Arc<AtomicU64>,
}

#[derive(Default)]
struct State {
    open: HashMap<u64, (Arc<str>, TraceSpan)>,
    /// per info hash, finished spans with the sequence number each finished at
    finished: HashMap<Arc<str>, VecDeque<(u64, TraceSpan)>>,
    seq: u64,
    capacity: usize,
}

/// In a tracked span's extensions: its id in the record and the torrent it belongs to.
#[derive(Clone)]
struct Tracked {
    id: u64,
    info_hash: Arc<str>,
}

impl TraceRecorder {
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                capacity,
                ..State::default()
            })),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn layer(&self) -> RecorderLayer {
        RecorderLayer(self.clone())
    }

    /// The spans of the torrent with this info hash (40 hex digits) that finished after
    /// `since`, and all its open ones.
    pub fn snapshot(&self, info_hash: &str, since: u64) -> TraceSnapshot {
        let state = self.state.lock().unwrap();
        let finished = state
            .finished
            .get(info_hash)
            .map(|spans| {
                spans
                    .iter()
                    .filter(|(seq, _)| *seq > since)
                    .map(|(_, span)| span.clone())
                    .collect()
            })
            .unwrap_or_default();
        let open = state
            .open
            .values()
            .filter(|(hash, _)| &**hash == info_hash)
            .map(|(_, span)| span.clone())
            .collect();
        TraceSnapshot {
            seq: state.seq,
            finished,
            open,
        }
    }

    /// Forgets a torrent's finished spans, for when it leaves the session.
    pub fn forget(&self, info_hash: &str) {
        self.state.lock().unwrap().finished.remove(info_hash);
    }
}

pub struct RecorderLayer(TraceRecorder);

fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
}

impl<S> Layer<S> for RecorderLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let parent = span.parent().and_then(|p| p.extensions().get::<Tracked>().cloned());
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        // a span is a torrent's if it says so, or if it's inside one that does
        let Some(info_hash) = fields
            .info_hash
            .take()
            .map(Arc::from)
            .or(parent.as_ref().map(|p| p.info_hash.clone()))
        else {
            return;
        };
        let tracked = Tracked {
            id: self.0.next_id.fetch_add(1, Ordering::Relaxed),
            info_hash: info_hash.clone(),
        };
        let record = TraceSpan {
            id: tracked.id,
            parent: parent.map(|p| p.id),
            name: attrs.metadata().name(),
            start_ms: now_ms(),
            end_ms: None,
            fields: fields.values,
            events: vec![],
        };
        self.0
            .state
            .lock()
            .unwrap()
            .open
            .insert(tracked.id, (info_hash, record));
        span.extensions_mut().insert(tracked);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(tracked) = ctx.span(id).and_then(|s| s.extensions().get::<Tracked>().cloned()) else {
            return;
        };
        let mut fields = Fields::default();
        values.record(&mut fields);
        let mut state = self.0.state.lock().unwrap();
        if let Some((_, span)) = state.open.get_mut(&tracked.id) {
            for (name, value) in fields.values {
                match span.fields.iter_mut().find(|(n, _)| *n == name) {
                    Some(slot) => slot.1 = value,
                    None => span.fields.push((name, value)),
                }
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let Some(tracked) = ctx
            .event_span(event)
            .and_then(|s| s.extensions().get::<Tracked>().cloned())
        else {
            return;
        };
        let mut fields = Fields::default();
        event.record(&mut fields);
        let mut message = fields.message.unwrap_or_default();
        for (name, value) in &fields.values {
            let _ = write!(message, " {name}={value}");
        }
        let mut state = self.0.state.lock().unwrap();
        if let Some((_, span)) = state.open.get_mut(&tracked.id)
            && span.events.len() < EVENTS_PER_SPAN
        {
            span.events.push(TraceEvent {
                at_ms: now_ms(),
                level: event.metadata().level().as_str(),
                message,
            });
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(tracked) = ctx.span(&id).and_then(|s| s.extensions().get::<Tracked>().cloned()) else {
            return;
        };
        let mut state = self.0.state.lock().unwrap();
        let Some((info_hash, mut span)) = state.open.remove(&tracked.id) else {
            return;
        };
        span.end_ms = Some(now_ms());
        state.seq += 1;
        let (seq, capacity) = (state.seq, state.capacity);
        let finished = state.finished.entry(info_hash).or_default();
        if finished.len() == capacity {
            finished.pop_front();
        }
        finished.push_back((seq, span));
    }
}

/// A span's or event's fields, formatted; `info_hash` and `message` are pulled out.
#[derive(Default)]
struct Fields {
    info_hash: Option<String>,
    message: Option<String>,
    values: Vec<(&'static str, String)>,
}

impl Fields {
    fn put(&mut self, field: &Field, value: String) {
        match field.name() {
            "info_hash" => self.info_hash = Some(value),
            "message" => self.message = Some(value),
            name => self.values.push((name, value)),
        }
    }
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.put(field, format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, value.to_string());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn spans_of_a_torrent_and_their_children_are_recorded() {
        let recorder = TraceRecorder::new(2);
        let subscriber = tracing_subscriber::registry().with(recorder.layer());
        tracing::subscriber::with_default(subscriber, || {
            let piece = tracing::info_span!("piece", info_hash = "ab", piece = 7, outcome = tracing::field::Empty);
            {
                let _in = piece.enter();
                tracing::info!(peer = "1.2.3.4:5", "block arrived");
                tracing::info_span!("piece.check").in_scope(|| {});
            }
            let open = recorder.snapshot("ab", 0);
            assert_eq!(open.open.len(), 1, "the piece is still open");
            assert_eq!(open.finished.len(), 1, "the check finished inside it");
            assert_eq!(open.finished[0].parent, Some(open.open[0].id));
            piece.record("outcome", "verified");
            drop(piece);

            tracing::info_span!("unrelated").in_scope(|| {});
            let snapshot = recorder.snapshot("ab", open.seq);
            assert!(snapshot.open.is_empty());
            let [piece] = &snapshot.finished[..] else {
                panic!("{snapshot:?}")
            };
            assert_eq!(piece.name, "piece");
            assert!(piece.end_ms.is_some());
            assert_eq!(
                piece.fields,
                [("piece", "7".to_string()), ("outcome", "verified".to_string())]
            );
            assert_eq!(piece.events[0].message, "block arrived peer=1.2.3.4:5");

            for _ in 0..3 {
                tracing::info_span!("dial", info_hash = "ab").in_scope(|| {});
            }
            assert_eq!(
                recorder.snapshot("ab", 0).finished.len(),
                2,
                "only the last `capacity` are kept"
            );
        });
    }
}
