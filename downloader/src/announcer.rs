//! Tracker announcers (BEP 3 over HTTP, BEP 15 over UDP). One task per tracker URL, reporting
//! the peers it learns about as `SwarmEvent::PeersDiscovered` and reading the swarm's progress
//! (uploaded/downloaded/left/completed) off its stats watch. Keyed on a bare `InfoHash` rather
//! than a `Torrent`: a magnet link's pre-metadata phase (see `metadata::fetch`) announces with
//! nothing else to give, which is why these don't live inside `TorrentSwarm`.

use crate::defs::Identity;
use crate::dht::DhtWatch;
use crate::events::{Event as BusEvent, EventBus, PeerSource};
use crate::settings::{
    ANNOUNCE_INTERVAL_MAX, ANNOUNCE_INTERVAL_MIN, ANNOUNCE_RETRY, ANNOUNCE_RETRY_MAX, DHT_ANNOUNCE_INTERVAL, DHT_RETRY,
    HTTP_TRACKER_TIMEOUT, TRACKER_RESPONSE_MAX, UDP_CONNECTION_ID_TTL, UDP_TRACKER_ATTEMPTS, UDP_TRACKER_TIMEOUT,
};
use crate::torrent_swarm::{SwarmEvent, TorrentSwarmStats};
use anyhow::{Context, anyhow, bail};
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use rand::RngExt;
use reqwest::Client;
use std::fmt::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::net::{UdpSocket, lookup_host};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::{debug, info, warn};
use url::{Host, Url};
use zerocopy::network_endian::{I32, I64, U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// What a tracker (or the DHT) has done for a torrent lately, for the details panel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackerStatus {
    /// the announce URL, or "DHT"
    pub url: String,
    pub state: TrackerState,
    /// peers the last successful announce returned
    pub peers: usize,
    pub next_announce: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrackerState {
    /// not announced yet
    Pending,
    Working,
    /// the last announce failed; retried with backoff
    Failed(String),
}

/// One row per announcer, written by them and read through what `spawn_announcers` returns.
type TrackerBoard = Arc<watch::Sender<Vec<TrackerStatus>>>;

fn report(board: &TrackerBoard, slot: usize, update: impl FnOnce(&mut TrackerStatus)) {
    board.send_modify(|rows| {
        if let Some(row) = rows.get_mut(slot) {
            update(row);
        }
    });
}

/// How long to wait before announcing again after `failures` consecutive failures.
fn retry_delay(failures: u32) -> Duration {
    ANNOUNCE_RETRY
        .saturating_mul(2u32.saturating_pow(failures.saturating_sub(1)))
        .min(ANNOUNCE_RETRY_MAX)
}

/// What every announcer of one torrent shares.
pub(crate) struct Announcing {
    pub trackers: Vec<String>,
    pub info_hash: InfoHash,
    pub identity: Arc<Identity>,
    /// what to tell the trackers about our progress
    pub stats: watch::Receiver<TorrentSwarmStats>,
    /// where discovered peers go
    pub events: mpsc::WeakSender<SwarmEvent>,
    pub shutdown: CancellationToken,
    pub dht: DhtWatch,
    pub bus: EventBus,
}

/// Spawns a tracker announcer task per usable URL in `trackers`, reporting discovered peers to
/// `events`. Split out so a magnet link's pre-metadata phase can announce with nothing but an
/// info hash (see `metadata::fetch`) -- at that point there is no `Torrent` and no
/// `TorrentSwarm` to hang the announcers off.
pub(crate) fn spawn_announcers(args: Announcing) -> watch::Receiver<Vec<TrackerStatus>> {
    let Announcing {
        trackers,
        info_hash,
        identity,
        events,
        shutdown,
        dht,
        bus,
        ..
    } = &args;
    let (board, statuses) = watch::channel(vec![]);
    let board = Arc::new(board);
    let mut rows = Vec::new();
    let mut slot = |url: &str| {
        rows.push(TrackerStatus {
            url: url.to_owned(),
            state: TrackerState::Pending,
            peers: 0,
            next_announce: None,
        });
        rows.len() - 1
    };
    // a watch whose sender is gone is a client with no DHT, now or ever; no row for it
    let dht_slot = dht.has_changed().is_ok().then(|| slot("DHT"));
    let usable: Vec<Url> = trackers.iter().filter_map(|t| Url::parse(t).ok()).collect();
    let slots: Vec<usize> = usable.iter().map(|url| slot(url.as_str())).collect();
    let _ = board.send(rows);

    if let Some(dht_slot) = dht_slot {
        tokio::spawn(dht_announcer(
            *info_hash,
            identity.serving.port(),
            dht.clone(),
            events.clone(),
            shutdown.clone(),
            (board.clone(), dht_slot),
            bus.clone(),
        ));
    }
    for (url, slot) in usable.into_iter().zip(slots) {
        match url.scheme() {
            "http" | "https" => {
                let announcer = HttpAnnouncer::new(url, &args, (board.clone(), slot));
                tokio::spawn(announcer.ev_loop());
            }
            "udp" if url.host().is_none() || url.port().is_none() => {
                warn!("ignoring UDP tracker {url} without a host and port");
                report(&board, slot, |row| {
                    row.state = TrackerState::Failed("a UDP tracker URL needs a host and a port".into())
                });
            }
            "udp" => {
                let announcer = UdpAnnouncer::new(url, &args, (board.clone(), slot));
                tokio::spawn(announcer.ev_loop());
            }
            scheme => {
                warn!("ignoring tracker with unsupported scheme {scheme:?}");
                report(&board, slot, |row| {
                    row.state = TrackerState::Failed(format!("unsupported scheme {scheme:?}"))
                });
            }
        }
    }
    statuses
}

/// BEP 5 as a peer source: every DHT_ANNOUNCE_INTERVAL (sooner while lookups come back empty,
/// see DHT_RETRY), look the info hash up, hand whatever
/// peers come back to the swarm, and announce our port to the nodes that issued tokens.
/// Waits for the node to come up first, and does nothing at all if it never does.
async fn dht_announcer(
    info_hash: InfoHash,
    tcp_port: u16,
    mut dht: DhtWatch,
    events: mpsc::WeakSender<SwarmEvent>,
    shutdown: CancellationToken,
    (board, slot): (TrackerBoard, usize),
    bus: EventBus,
) {
    let handle = loop {
        if let Some(handle) = dht.borrow().clone() {
            break handle;
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            changed = dht.changed() => if changed.is_err() { return },
        }
    };
    let port = Some(tcp_port);
    let mut retry = DHT_RETRY;

    loop {
        let started = Instant::now();
        // peers go to the swarm as nodes return them; waiting for the lookup to converge
        // would leave them idle for the seconds that takes
        let early = events.clone();
        let streamed = handle.client.get_peers_with(info_hash, move |peers| {
            if let Some(events) = early.upgrade() {
                let peers = peers.iter().copied().map(SocketAddr::V4).collect();
                let _ = events.try_send(SwarmEvent::PeersDiscovered(peers, PeerSource::Dht));
            }
        });
        let span = tracing::info_span!(
            "dht.lookup",
            info_hash = %info_hash,
            peers = tracing::field::Empty,
            announce_to = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        let lookup = tokio::select! {
            _ = shutdown.cancelled() => return,
            lookup = streamed.instrument(span.clone()) => lookup,
        };
        match &lookup {
            Ok(result) => {
                span.record("peers", result.peers.len());
                span.record("announce_to", result.announce_candidates.len());
            }
            Err(e) => {
                span.record("error", format!("{e:#}"));
            }
        }
        drop(span);
        let wait = match &lookup {
            Ok(result) if !result.peers.is_empty() => {
                retry = DHT_RETRY;
                DHT_ANNOUNCE_INTERVAL
            }
            _ => {
                let wait = retry;
                retry = (retry * 2).min(DHT_ANNOUNCE_INTERVAL);
                wait
            }
        };
        match lookup {
            Ok(result) => {
                info!(
                    "DHT lookup found {} peers, {} nodes accept our announce",
                    result.peers.len(),
                    result.announce_candidates.len()
                );
                bus.emit(BusEvent::DhtLookup {
                    info_hash,
                    peers: result.peers.len(),
                    took_ms: started.elapsed().as_millis() as u64,
                });
                report(&board, slot, |row| {
                    row.state = TrackerState::Working;
                    row.peers = result.peers.len();
                    row.next_announce = Some(Instant::now() + wait);
                });
                // the whole set again, in case a batch above found the queue full; the swarm
                // and the metadata fetch both skip addresses they already have
                let Some(events) = events.upgrade() else { return };
                let peers = result.peers.into_iter().map(SocketAddr::V4).collect();
                if events
                    .send(SwarmEvent::PeersDiscovered(peers, PeerSource::Dht))
                    .await
                    .is_err()
                {
                    return;
                }
                // all at once and in the background: one at a time, each dead node held the next
                // lookup back by a full request timeout
                let client = handle.client.clone();
                tokio::spawn(futures::future::join_all(result.announce_candidates.into_iter().map(
                    move |(node, token)| {
                        let client = client.clone();
                        async move {
                            if let Err(e) = client.announce_peers(node.end_point(), info_hash, port, token).await {
                                tracing::debug!("announce to DHT node {} failed: {e:#}", node.end_point());
                            }
                        }
                    },
                )));
            }
            Err(e) => {
                warn!("DHT lookup for {info_hash:?} failed: {e:#}");
                report(&board, slot, |row| {
                    row.state = TrackerState::Failed(format!("{e:#}"));
                    row.next_announce = Some(Instant::now() + wait);
                });
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

/// The tracker `event` parameter (BEP 3 for HTTP, BEP 15 for UDP). The first announce to a
/// tracker must be Started; a single announce reporting Completed should follow the download
/// finishing; Stopped is a courtesy announce on graceful shutdown so the tracker can drop us
/// immediately instead of waiting out the interval. Anything else is a regular periodic announce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnnounceEvent {
    Regular,
    Started,
    Completed,
    Stopped,
}

impl AnnounceEvent {
    fn http_str(self) -> Option<&'static str> {
        match self {
            AnnounceEvent::Regular => None,
            AnnounceEvent::Started => Some("started"),
            AnnounceEvent::Completed => Some("completed"),
            AnnounceEvent::Stopped => Some("stopped"),
        }
    }

    /// BEP 15's UDP tracker protocol encodes the same event as an int, with this (not the
    /// obvious 0/1/2/3-in-declaration-order) mapping.
    fn udp_code(self) -> i32 {
        match self {
            AnnounceEvent::Regular => Event::None as i32,
            AnnounceEvent::Completed => Event::Completed as i32,
            AnnounceEvent::Started => Event::Started as i32,
            AnnounceEvent::Stopped => Event::Stopped as i32,
        }
    }
}

/// How long the courtesy event=stopped may take on the way out.
const STOPPED_TIMEOUT: Duration = Duration::from_secs(5);

/// Room for the largest UDP datagram, so a long peer list is never silently truncated.
const UDP_MAX_DATAGRAM: usize = 64 * 1024;

/// What we tell a tracker about our progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Progress {
    uploaded: u64,
    downloaded: u64,
    left: u64,
}

/// The wait until the next regular announce, from the tracker's `interval` and, if it sent
/// one, `min interval` (in seconds; zero, negative and absurd values all happen).
fn announce_interval(interval: i64, min_interval: Option<i64>) -> Duration {
    let secs = interval.max(min_interval.unwrap_or(0)).clamp(
        ANNOUNCE_INTERVAL_MIN.as_secs() as i64,
        ANNOUNCE_INTERVAL_MAX.as_secs() as i64,
    );
    Duration::from_secs(secs as u64)
}

/// Tracker-supplied text, made safe to log and show: lossy UTF-8, cut at 200 characters.
fn preview(bytes: &[u8]) -> String {
    const MAX: usize = 200;
    let text = String::from_utf8_lossy(bytes);
    match text.char_indices().nth(MAX) {
        Some((cut, _)) => format!("{}...", &text[..cut]),
        None => text.into_owned(),
    }
}

/// What an HTTP and a UDP announcer both keep: the schedule, the BEP 3 event bookkeeping, and
/// where outcomes go.
#[derive(Debug)]
struct Tracker {
    url: Url,
    info_hash: InfoHash,
    identity: Arc<Identity>,
    next_ready: Instant,
    swarm_stat: watch::Receiver<TorrentSwarmStats>,
    events: mpsc::WeakSender<SwarmEvent>,
    sent_started: bool,
    sent_completed: bool,
    shutdown: CancellationToken,
    board: (TrackerBoard, usize),
    /// consecutive failed announces, for the retry backoff
    failures: u32,
    bus: EventBus,
}

impl Tracker {
    fn new(url: Url, shared: &Announcing, board: (TrackerBoard, usize)) -> Self {
        Tracker {
            url,
            info_hash: shared.info_hash,
            identity: shared.identity.clone(),
            next_ready: Instant::now() + Duration::from_millis(10),
            swarm_stat: shared.stats.clone(),
            events: shared.events.clone(),
            sent_started: false,
            // a torrent that was already complete when resumed must not announce
            // event=completed again (BEP 3)
            sent_completed: shared.stats.borrow().completed,
            shutdown: shared.shutdown.clone(),
            board,
            failures: 0,
            bus: shared.bus.clone(),
        }
    }

    fn progress(&self) -> Progress {
        let stats = self.swarm_stat.borrow();
        Progress {
            uploaded: stats.uploaded,
            downloaded: stats.downloaded,
            left: stats.left as u64,
        }
    }

    /// Waits until the next announce is due; false on shutdown. BEP 3: a single announce
    /// reporting event=completed should follow the download finishing, so that transition
    /// pulls the next announce forward unless we're backing off from a failure.
    async fn due(&mut self) -> bool {
        loop {
            tokio::select! {
                _ = sleep_until(self.next_ready) => return true,
                Ok(()) = self.swarm_stat.changed(), if !self.sent_completed => {
                    if self.swarm_stat.borrow().completed && self.failures == 0 {
                        self.next_ready = Instant::now();
                    }
                }
                _ = self.shutdown.cancelled() => return false,
            }
        }
    }

    /// BEP 3: the first announce must carry event=started, and one carrying event=completed
    /// follows the download finishing (standing in for started if it comes first).
    fn next_event(&self) -> AnnounceEvent {
        if !self.sent_completed && self.swarm_stat.borrow().completed {
            AnnounceEvent::Completed
        } else if !self.sent_started {
            AnnounceEvent::Started
        } else {
            AnnounceEvent::Regular
        }
    }

    /// One announce, for the traces. The URL leaves out its query, where private trackers keep
    /// the user's passkey.
    fn announce_span(&self, event: AnnounceEvent) -> tracing::Span {
        let mut url = self.url.clone();
        url.set_query(None);
        tracing::info_span!(
            "tracker.announce",
            info_hash = %self.info_hash,
            url = %url,
            event = ?event,
            peers = tracing::field::Empty,
            interval_secs = tracing::field::Empty,
            error = tracing::field::Empty,
        )
    }

    /// Books an announce's outcome: the schedule or backoff, the board row, the event bus, and
    /// the peers to the swarm.
    async fn settle(&mut self, event: AnnounceEvent, announced: anyhow::Result<(Vec<SocketAddr>, Duration)>) {
        let (board, slot) = &self.board;
        let span = tracing::Span::current();
        match &announced {
            Ok((peers, interval)) => {
                span.record("peers", peers.len());
                span.record("interval_secs", interval.as_secs());
            }
            Err(e) => {
                span.record("error", format!("{e:#}"));
            }
        }
        match announced {
            Ok((peers, interval)) => {
                self.sent_started = true;
                self.sent_completed |= event == AnnounceEvent::Completed;
                self.failures = 0;
                self.next_ready = Instant::now() + interval;
                let (count, next) = (peers.len(), self.next_ready);
                info!(
                    "Tracker [{}] returned {count} peers, next announce in {}s",
                    self.url,
                    interval.as_secs()
                );
                report(board, *slot, |row| {
                    row.state = TrackerState::Working;
                    row.peers = count;
                    row.next_announce = Some(next);
                });
                self.bus.emit(BusEvent::Announced {
                    info_hash: self.info_hash,
                    url: self.url.to_string(),
                    peers: count,
                    interval_secs: interval.as_secs(),
                });
                if let Some(events) = self.events.upgrade() {
                    let source = PeerSource::Tracker {
                        url: self.url.to_string(),
                    };
                    tokio::select! {
                        _ = events.send(SwarmEvent::PeersDiscovered(peers, source)) => {}
                        _ = self.shutdown.cancelled() => {}
                    }
                }
            }
            Err(e) => {
                warn!("Tracker [{}]: {e:#}", self.url);
                self.failures += 1;
                self.next_ready = Instant::now() + retry_delay(self.failures);
                let next = self.next_ready;
                report(board, *slot, |row| {
                    row.state = TrackerState::Failed(format!("{e:#}"));
                    row.next_announce = Some(next);
                });
                self.bus.emit(BusEvent::AnnounceFailed {
                    info_hash: self.info_hash,
                    url: self.url.to_string(),
                    error: format!("{e:#}"),
                });
            }
        }
    }
}

/// Peers asked of a tracker per announce. Left out, some trackers (Ubuntu's among them) hand
/// out a single peer; 200 is what libtorrent asks for, and peers are cheap to have on hand.
const NUMWANT: u32 = 200;

/// One shared client, so announces reuse connections; the timeout covers the whole request.
static HTTP_CLIENT: LazyLock<Client> = LazyLock::new(|| {
    Client::builder()
        .timeout(HTTP_TRACKER_TIMEOUT)
        .build()
        .expect("the tracker HTTP client should build")
});

/// BEP 3 sends the info hash and peer ID as raw bytes; everything outside the unreserved set
/// is escaped, so no tracker has to guess whether `+` means a space.
fn percent_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

/// The URL of one HTTP announce. Private trackers' announce URLs carry a passkey in their own
/// query, which our parameters join rather than replace.
fn http_announce_url(
    tracker: &Url,
    info_hash: &InfoHash,
    peer_id: &[u8; 20],
    port: u16,
    progress: Progress,
    event: AnnounceEvent,
) -> anyhow::Result<Url> {
    let mut base = tracker.clone();
    base.set_fragment(None);
    let separator = match base.query() {
        None => "?",
        Some("") => "",
        Some(_) => "&",
    };
    let Progress {
        uploaded,
        downloaded,
        left,
    } = progress;
    let url = format!(
        "{base}{separator}info_hash={}&peer_id={}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&compact=1&numwant={NUMWANT}{}",
        percent_encode(&info_hash.0),
        percent_encode(peer_id),
        event.http_str().map(|e| format!("&event={e}")).unwrap_or_default(),
    );
    Url::parse(&url).context("building the announce URL")
}

/// Reads a response body, giving up past `cap` bytes rather than buffering whatever a broken
/// tracker sends.
async fn read_capped(mut response: reqwest::Response, cap: usize) -> anyhow::Result<Vec<u8>> {
    if response.content_length().is_some_and(|len| len > cap as u64) {
        bail!("tracker response is larger than {cap} bytes");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| e.without_url())
        .context("reading the tracker's response")?
    {
        if body.len() + chunk.len() > cap {
            bail!("tracker response is larger than {cap} bytes");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// What an HTTP tracker's announce response says.
#[derive(Debug, PartialEq, Eq)]
struct HttpResponse {
    peers: Vec<SocketAddr>,
    interval: Duration,
    warning: Option<String>,
}

/// Parses an HTTP tracker's announce response; a `failure reason` is the error.
fn parse_http_response(body: &[u8]) -> anyhow::Result<HttpResponse> {
    let Ok((_remaining, mut dict)) = juicy_bencode::parse_bencode_dict(body) else {
        bail!("tracker responded with invalid bencode: {}", preview(body));
    };
    if let Some(BencodeItemView::ByteString(reason)) = dict.remove(b"failure reason".as_slice()) {
        bail!("tracker: {}", preview(reason));
    }
    let warning = match dict.remove(b"warning message".as_slice()) {
        Some(BencodeItemView::ByteString(warning)) => Some(preview(warning)),
        _ => None,
    };
    let integer = |item| match item {
        Some(BencodeItemView::Integer(i)) => Some(i),
        _ => None,
    };
    let Some(interval) = integer(dict.remove(b"interval".as_slice())) else {
        match warning {
            Some(warning) => bail!("tracker sent no interval, warning: {warning}"),
            None => bail!("tracker sent no interval"),
        }
    };
    let min_interval = integer(dict.remove(b"min interval".as_slice()));

    let mut peers = vec![];
    match dict.remove(b"peers".as_slice()) {
        // compact: 4 bytes IP, 2 bytes port each
        Some(BencodeItemView::ByteString(peer_bytes)) => {
            for &[a, b, c, d, p1, p2] in peer_bytes.as_chunks::<6>().0 {
                peers.push(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::new(a, b, c, d),
                    u16::from_be_bytes([p1, p2]),
                )));
            }
        }
        // trackers may ignore compact=1 and send the original list of dicts
        Some(BencodeItemView::List(entries)) => {
            for entry in entries {
                let BencodeItemView::Dictionary(entry) = entry else {
                    continue;
                };
                let (Some(BencodeItemView::ByteString(ip)), Some(BencodeItemView::Integer(port))) =
                    (entry.get(b"ip".as_slice()), entry.get(b"port".as_slice()))
                else {
                    continue;
                };
                let ip = std::str::from_utf8(ip).ok().and_then(|ip| ip.parse::<IpAddr>().ok());
                if let (Some(ip), Ok(port)) = (ip, u16::try_from(*port)) {
                    peers.push(SocketAddr::new(ip, port));
                }
            }
        }
        _ => {}
    }

    // BEP 7: IPv6 peers come back separately under "peers6", same compact encoding but
    // 18 bytes each (16 bytes IP, 2 bytes port).
    if let Some(BencodeItemView::ByteString(peer_bytes)) = dict.remove(b"peers6".as_slice()) {
        for chunk in peer_bytes.as_chunks::<18>().0 {
            let (ip, port) = chunk.split_first_chunk::<16>().expect("18 bytes");
            let port = u16::from_be_bytes([port[0], port[1]]);
            peers.push(SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(*ip), port, 0, 0)));
        }
    }

    Ok(HttpResponse {
        peers,
        interval: announce_interval(interval, min_interval),
        warning,
    })
}

struct HttpAnnouncer {
    tracker: Tracker,
}

impl HttpAnnouncer {
    fn new(url: Url, shared: &Announcing, board: (TrackerBoard, usize)) -> Self {
        debug_assert!({ url.scheme() == "http" || url.scheme() == "https" });
        HttpAnnouncer {
            tracker: Tracker::new(url, shared, board),
        }
    }

    /// Performs a single announce, returning the peers and the interval until the next one.
    #[tracing::instrument(skip(self))]
    async fn announce(&self, event: AnnounceEvent) -> anyhow::Result<(Vec<SocketAddr>, Duration)> {
        let tracker = &self.tracker;
        let url = http_announce_url(
            &tracker.url,
            &tracker.info_hash,
            &tracker.identity.peer_id,
            tracker.identity.serving.port(),
            tracker.progress(),
            event,
        )?;
        debug!("Announcing to {url}");

        // without_url: private trackers' URLs carry a passkey, and errors end up in the GUI
        let response = HTTP_CLIENT
            .get(url)
            .send()
            .await
            .map_err(|e| e.without_url())
            .context("sending the announce")?;
        let status = response.status();
        let body = read_capped(response, TRACKER_RESPONSE_MAX).await?;
        debug!("Tracker [{}] responded {status}: {}", tracker.url, preview(&body));

        let response = match parse_http_response(&body) {
            Ok(response) => response,
            Err(e) if !status.is_success() => return Err(e.context(format!("HTTP {status}"))),
            Err(e) => return Err(e),
        };
        if let Some(warning) = &response.warning {
            warn!("Tracker [{}] warns: {warning}", tracker.url);
        }
        Ok((response.peers, response.interval))
    }

    async fn ev_loop(mut self) {
        let shutdown = self.tracker.shutdown.clone();
        while self.tracker.due().await {
            let event = self.tracker.next_event();
            let span = self.tracker.announce_span(event);
            let announced = tokio::select! {
                announced = self.announce(event).instrument(span.clone()) => announced,
                _ = shutdown.cancelled() => break,
            };
            self.tracker.settle(event, announced).instrument(span).await;
        }
        // BEP 3: send a courtesy event=stopped on graceful shutdown so the tracker drops us
        // immediately instead of waiting out the interval; best-effort, since we're on our
        // way out regardless of whether it succeeds
        if self.tracker.sent_started {
            let _ = tokio::time::timeout(STOPPED_TIMEOUT, self.announce(AnnounceEvent::Stopped)).await;
        }
    }
}

#[repr(i32)]
enum Action {
    Connect = 0,
    Announce = 1,
    #[allow(dead_code)]
    Scrape = 2,
    Error = 3,
}

#[repr(i32)]
enum Event {
    None = 0,
    Completed = 1,
    Started = 2,
    Stopped = 3,
}

/// How long to wait for an answer to the `n`th try (from 0) of a UDP tracker request (BEP 15).
fn udp_retransmit_wait(n: u32) -> Duration {
    UDP_TRACKER_TIMEOUT * 2u32.pow(n)
}

/// Sends `request` once and waits up to `wait` for the datagram answering it, matched on the
/// transaction ID every BEP 15 response carries at bytes 4..8; anything else is a late answer
/// to an earlier request, or noise. `None` means it's time to retransmit. The tracker's error
/// action comes back as an error carrying its message.
async fn udp_exchange(
    socket: &UdpSocket,
    request: &[u8],
    transaction_id: i32,
    wait: Duration,
) -> anyhow::Result<Option<Vec<u8>>> {
    socket.send(request).await.context("sending to the tracker")?;
    let deadline = Instant::now() + wait;
    let mut buf = vec![0u8; UDP_MAX_DATAGRAM];
    loop {
        let Ok(received) = tokio::time::timeout_at(deadline, socket.recv(&mut buf)).await else {
            return Ok(None);
        };
        let len = received.context("receiving from the tracker")?;
        let reply = &buf[..len];
        if len < 8 || reply[4..8] != transaction_id.to_be_bytes() {
            continue;
        }
        if reply[..4] == (Action::Error as i32).to_be_bytes() {
            bail!("tracker: {}", preview(&reply[8..]));
        }
        buf.truncate(len);
        return Ok(Some(buf));
    }
}

/// BEP 15's connect exchange: a connection ID, and when it arrived.
async fn udp_connect(socket: &UdpSocket, attempts: u32) -> anyhow::Result<(i64, Instant)> {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable)]
    #[repr(C)]
    struct Connect {
        connection_id: I64,
        action: I32,
        transaction_id: I32,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable, KnownLayout)]
    #[repr(C)]
    struct Response {
        action: I32,
        transaction_id: I32,
        connection_id: I64,
    }

    let transaction_id = rand::rng().random::<i32>();
    let connect = Connect {
        connection_id: 0x41727101980.into(),
        action: (Action::Connect as i32).into(),
        transaction_id: transaction_id.into(),
    };
    for n in 0..attempts {
        let Some(reply) = udp_exchange(socket, connect.as_bytes(), transaction_id, udp_retransmit_wait(n)).await?
        else {
            continue;
        };
        let Ok((response, _)) = Response::read_from_prefix(&reply) else {
            bail!("tracker's connect response is too short");
        };
        if i32::from(response.action) != Action::Connect as i32 {
            bail!("tracker answered connect with action {}", i32::from(response.action));
        }
        return Ok((response.connection_id.into(), Instant::now()));
    }
    bail!("tracker didn't answer connect after {attempts} tries")
}

/// Parses a UDP tracker's announce response. The classic BEP 15 wire format only ever defined
/// a 4-byte-IP peer entry, with nothing like BEP 7's "peers"/"peers6" split; common tracker
/// software returns 18-byte (16 IP + 2 port) entries when the announce arrived over IPv6, so
/// `is_v6` is which family we reached the tracker over, not anything in the response.
fn parse_udp_announce(reply: &[u8], is_v6: bool) -> anyhow::Result<(Vec<SocketAddr>, Duration)> {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Immutable, Default, KnownLayout, Unaligned)]
    #[repr(C)]
    struct AnnounceResponseHeader {
        action: I32,
        transaction_id: I32,
        interval: I32, // in seconds
        leechers: I32,
        seeders: I32,
    }

    let Ok((header, peer_bytes)) = AnnounceResponseHeader::ref_from_prefix(reply) else {
        bail!("announce response is shorter than a header");
    };
    if i32::from(header.action) != Action::Announce as i32 {
        bail!("tracker answered announce with action {}", i32::from(header.action));
    }

    let peer_size = if is_v6 { 18 } else { 6 };
    if peer_bytes.len() % peer_size != 0 {
        bail!("announce response's peers are not a multiple of {peer_size} bytes");
    }
    let peers = peer_bytes
        .chunks_exact(peer_size)
        .map(|chunk| {
            let (ip, port) = chunk.split_at(peer_size - 2);
            let port = u16::from_be_bytes([port[0], port[1]]);
            match <[u8; 16]>::try_from(ip) {
                Ok(ip) => SocketAddr::new(Ipv6Addr::from(ip).into(), port),
                Err(_) => SocketAddr::new(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]).into(), port),
            }
        })
        .collect();
    Ok((peers, announce_interval(header.interval.get().into(), None)))
}

struct UdpAnnouncer {
    tracker: Tracker,
    /// connected to the tracker's address that answered; dropped on any failure, since the
    /// tracker may have been down or renumbered
    socket: Option<UdpSocket>,
    /// the connection ID and when it arrived
    connection: Option<(i64, Instant)>,
    /// BEP 15: lets the tracker tell us apart if our address changes; fixed for our lifetime
    key: u32,
}

impl UdpAnnouncer {
    fn new(url: Url, shared: &Announcing, board: (TrackerBoard, usize)) -> Self {
        debug_assert!(url.scheme() == "udp");
        UdpAnnouncer {
            tracker: Tracker::new(url, shared, board),
            socket: None,
            connection: None,
            key: rand::rng().random(),
        }
    }

    /// Resolves the tracker and connects to the first address that answers, IPv4 first.
    #[tracing::instrument(skip(self))]
    async fn open(&mut self) -> anyhow::Result<UdpSocket> {
        let url = &self.tracker.url;
        let port = url.port().context("tracker URL has no port")?;
        let mut addresses: Vec<SocketAddr> = match url.host().context("tracker URL has no host")? {
            Host::Ipv4(ip) => vec![SocketAddr::new(ip.into(), port)],
            Host::Ipv6(ip) => vec![SocketAddr::new(ip.into(), port)],
            Host::Domain(domain) => lookup_host((domain, port))
                .await
                .with_context(|| format!("resolving {domain}"))?
                .collect(),
        };
        addresses.sort_by_key(SocketAddr::is_ipv6);
        addresses.dedup();
        debug!("Tracker [{url}] resolved to {addresses:?}");

        let mut last_error = None;
        for (i, &address) in addresses.iter().enumerate() {
            // only the last candidate gets the whole retransmit schedule, so a dead address
            // ahead of it costs one timeout rather than all of them
            let attempts = if i + 1 == addresses.len() {
                UDP_TRACKER_ATTEMPTS
            } else {
                1
            };
            match Self::open_at(address, attempts).await {
                Ok((socket, connection)) => {
                    info!("Tracker [{url}] connected on {address}");
                    self.connection = Some(connection);
                    return Ok(socket);
                }
                Err(e) => {
                    debug!("Tracker [{url}] on {address}: {e:#}");
                    last_error = Some(e.context(format!("on {address}")));
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("tracker did not resolve to any address")))
    }

    async fn open_at(address: SocketAddr, attempts: u32) -> anyhow::Result<(UdpSocket, (i64, Instant))> {
        let ours: SocketAddr = match address {
            SocketAddr::V4(_) => SocketAddrV4::new(crate::defs::BIND_V4, 0).into(),
            SocketAddr::V6(_) => SocketAddrV6::new(crate::defs::BIND_V6, 0, 0, 0).into(),
        };
        let socket = UdpSocket::bind(ours)
            .await
            .with_context(|| format!("binding a UDP socket on {ours}"))?;
        socket.connect(address).await.context("connecting the UDP socket")?;
        let connection = udp_connect(&socket, attempts).await?;
        Ok((socket, connection))
    }

    /// Performs a single announce, returning the peers and the interval until the next one.
    #[tracing::instrument(skip(self))]
    async fn announce(&mut self, event: AnnounceEvent) -> anyhow::Result<(Vec<SocketAddr>, Duration)> {
        if self.socket.is_none() {
            self.socket = Some(self.open().await?);
        }
        let socket = self.socket.as_ref().expect("opened above");

        #[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable)]
        #[repr(C)]
        struct Announce {
            connection_id: I64,
            action: I32,
            transaction_id: I32,
            info_hash: [u8; 20],
            peer_id: [u8; 20],
            downloaded: I64,
            left: I64,
            uploaded: I64,
            event: I32,
            ip: U32,
            key: U32,
            num_want: I32,
            port: U16,
            extensions: U16,
        }

        let transaction_id = rand::rng().random::<i32>();
        let progress = self.tracker.progress();
        let signed = |n: u64| i64::try_from(n).unwrap_or(i64::MAX);
        let mut announce = Announce {
            connection_id: 0.into(),
            action: (Action::Announce as i32).into(),
            transaction_id: transaction_id.into(),
            info_hash: self.tracker.info_hash.0,
            peer_id: self.tracker.identity.peer_id,
            downloaded: signed(progress.downloaded).into(),
            left: signed(progress.left).into(),
            uploaded: signed(progress.uploaded).into(),
            event: event.udp_code().into(),
            ip: 0.into(), // i.e. let the tracker infer from the source packet
            key: self.key.into(),
            num_want: (NUMWANT as i32).into(),
            port: self.tracker.identity.serving.port().into(),
            extensions: 0.into(), // bitfield, i.e. 0 means no extensions
        };
        let is_v6 = matches!(socket.peer_addr(), Ok(SocketAddr::V6(_)));

        for n in 0..UDP_TRACKER_ATTEMPTS {
            // BEP 15: a connection ID is good for a minute after it arrived, and retransmits
            // can outlast that
            let connection_id = match self.connection {
                Some((id, at)) if at.elapsed() < UDP_CONNECTION_ID_TTL => id,
                _ => {
                    let connection = udp_connect(socket, UDP_TRACKER_ATTEMPTS).await?;
                    self.connection = Some(connection);
                    connection.0
                }
            };
            announce.connection_id = connection_id.into();
            let wait = udp_retransmit_wait(n);
            if let Some(reply) = udp_exchange(socket, announce.as_bytes(), transaction_id, wait).await? {
                let (peers, interval) = parse_udp_announce(&reply, is_v6)?;
                debug!("Tracker [{}] announce returned {peers:?}", self.tracker.url);
                return Ok((peers, interval));
            }
        }
        bail!("tracker didn't answer the announce after {UDP_TRACKER_ATTEMPTS} tries")
    }

    async fn ev_loop(mut self) {
        let shutdown = self.tracker.shutdown.clone();
        while self.tracker.due().await {
            let event = self.tracker.next_event();
            let span = self.tracker.announce_span(event);
            let announced = tokio::select! {
                announced = self.announce(event).instrument(span.clone()) => announced,
                _ = shutdown.cancelled() => break,
            };
            if announced.is_err() {
                self.socket = None;
                self.connection = None;
            }
            self.tracker.settle(event, announced).instrument(span).await;
        }
        // BEP 15: send a courtesy event=stopped on graceful shutdown so the tracker drops us
        // immediately instead of waiting out the interval; best-effort
        if self.tracker.sent_started && self.socket.is_some() {
            let _ = tokio::time::timeout(STOPPED_TIMEOUT, self.announce(AnnounceEvent::Stopped)).await;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn retry_delay_doubles_and_caps() {
        assert_eq!(retry_delay(1), ANNOUNCE_RETRY);
        assert_eq!(retry_delay(2), ANNOUNCE_RETRY * 2);
        assert_eq!(retry_delay(3), ANNOUNCE_RETRY * 4);
        assert_eq!(retry_delay(40), ANNOUNCE_RETRY_MAX);
    }

    #[test]
    fn announce_interval_is_clamped_and_honours_min_interval() {
        assert_eq!(announce_interval(1800, None), Duration::from_secs(1800));
        assert_eq!(announce_interval(0, None), ANNOUNCE_INTERVAL_MIN);
        assert_eq!(announce_interval(-5, None), ANNOUNCE_INTERVAL_MIN);
        assert_eq!(announce_interval(i64::MAX, None), ANNOUNCE_INTERVAL_MAX);
        assert_eq!(announce_interval(120, Some(900)), Duration::from_secs(900));
        assert_eq!(announce_interval(1800, Some(-1)), Duration::from_secs(1800));
        assert_eq!(announce_interval(120, Some(i64::MAX)), ANNOUNCE_INTERVAL_MAX);
    }

    fn announce_url(tracker: &str, event: AnnounceEvent) -> String {
        let mut info_hash = [b'a'; 20];
        info_hash[0] = b' ';
        info_hash[1] = 0xff;
        let progress = Progress {
            uploaded: 1,
            downloaded: 2,
            left: 3,
        };
        http_announce_url(
            &Url::parse(tracker).unwrap(),
            &InfoHash::from_bytes(&info_hash),
            &[b'-'; 20],
            6881,
            progress,
            event,
        )
        .unwrap()
        .to_string()
    }

    #[test]
    fn announce_url_joins_an_existing_query() {
        let params = format!(
            "info_hash=%20%FF{}&peer_id={}&port=6881&uploaded=1&downloaded=2&left=3&compact=1&numwant=200",
            "a".repeat(18),
            "-".repeat(20)
        );
        assert_eq!(
            announce_url("http://t.test/announce", AnnounceEvent::Regular),
            format!("http://t.test/announce?{params}")
        );
        assert_eq!(
            announce_url("https://t.test/announce?passkey=abc#frag", AnnounceEvent::Started),
            format!("https://t.test/announce?passkey=abc&{params}&event=started")
        );
        assert_eq!(
            announce_url("http://t.test/announce?", AnnounceEvent::Regular),
            format!("http://t.test/announce?{params}")
        );
    }

    #[test]
    fn http_response_failure_reason_is_the_error() {
        let e = parse_http_response(b"d14:failure reason17:unregistered hashe").unwrap_err();
        assert_eq!(format!("{e:#}"), "tracker: unregistered hash");

        let e = parse_http_response(b"d15:warning message4:slowe").unwrap_err();
        assert_eq!(format!("{e:#}"), "tracker sent no interval, warning: slow");
        assert!(parse_http_response(b"<html>").is_err());
    }

    #[test]
    fn http_response_peers_and_intervals() {
        let mut body = b"d8:intervali0e12:min intervali900e5:peers6:".to_vec();
        body.extend_from_slice(&[10, 0, 0, 1, 0x1a, 0xe1]);
        body.extend_from_slice(b"6:peers618:");
        body.extend_from_slice(&[0; 15]);
        body.extend_from_slice(&[1, 0x1a, 0xe2]);
        body.extend_from_slice(b"15:warning message2:hie");
        let response = parse_http_response(&body).unwrap();
        assert_eq!(
            response,
            HttpResponse {
                peers: vec!["10.0.0.1:6881".parse().unwrap(), "[::1]:6882".parse().unwrap()],
                interval: Duration::from_secs(900),
                warning: Some("hi".into()),
            }
        );

        let body = b"d8:intervali60e5:peersld2:ip9:127.0.0.14:porti7eed2:ip3:bad4:porti1eeee";
        let response = parse_http_response(body).unwrap();
        assert_eq!(response.peers, ["127.0.0.1:7".parse::<SocketAddr>().unwrap()]);
    }

    #[test]
    fn udp_announce_response_parsing() {
        let header = |action: i32, interval: i32| {
            [action, 7, interval, 0, 0]
                .iter()
                .flat_map(|n| n.to_be_bytes())
                .collect::<Vec<u8>>()
        };
        let mut reply = header(1, -30);
        reply.extend_from_slice(&[10, 0, 0, 1, 0x1a, 0xe1]);
        let (peers, interval) = parse_udp_announce(&reply, false).unwrap();
        assert_eq!(peers, ["10.0.0.1:6881".parse::<SocketAddr>().unwrap()]);
        assert_eq!(interval, ANNOUNCE_INTERVAL_MIN);
        assert!(parse_udp_announce(&reply, true).is_err(), "6 bytes is no IPv6 peer");
        assert!(parse_udp_announce(&header(2, 60), false).is_err());
        assert!(parse_udp_announce(&reply[..10], false).is_err());
    }

    /// An unanswered request comes back as time to retransmit; answers to other transactions
    /// are skipped; the tracker's error action carries its message.
    #[tokio::test]
    async fn udp_exchange_matches_transactions_and_times_out() {
        let tracker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.connect(tracker.local_addr().unwrap()).await.unwrap();

        let wait = Duration::from_millis(100);
        assert_eq!(udp_exchange(&socket, b"hello", 42, wait).await.unwrap(), None);
        let mut buf = [0; 64];
        tracker.recv_from(&mut buf).await.unwrap();

        let answer = tokio::spawn(async move {
            let (_, from) = tracker.recv_from(&mut buf).await.unwrap();
            let mut stale = 1i32.to_be_bytes().to_vec();
            stale.extend_from_slice(&41i32.to_be_bytes());
            tracker.send_to(&stale, from).await.unwrap();
            let mut error = 3i32.to_be_bytes().to_vec();
            error.extend_from_slice(&42i32.to_be_bytes());
            error.extend_from_slice(b"go away");
            tracker.send_to(&error, from).await.unwrap();
        });
        let e = udp_exchange(&socket, b"hello", 42, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert_eq!(format!("{e:#}"), "tracker: go away");
        answer.await.unwrap();
    }

    /// A one-shot HTTP server answering with `response`, handing back the request line.
    async fn http_server(response: Vec<u8>) -> (String, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let served = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let n = stream.read(&mut request).await.unwrap();
            let _ = stream.write_all(&response).await;
            let _ = stream.shutdown().await;
            String::from_utf8_lossy(&request[..n])
                .lines()
                .next()
                .unwrap_or_default()
                .to_owned()
        });
        (base, served)
    }

    #[tokio::test]
    async fn response_bodies_are_capped() {
        let mut response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n".to_vec();
        response.extend(std::iter::repeat_n(b'x', 64 * 1024));
        let (base, served) = http_server(response).await;
        let response = HTTP_CLIENT.get(&base).send().await.unwrap();
        let e = read_capped(response, 1024).await.unwrap_err();
        assert!(format!("{e:#}").contains("larger than 1024 bytes"), "{e:#}");
        served.await.unwrap();

        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello".to_vec();
        let (base, served) = http_server(response).await;
        let response = HTTP_CLIENT.get(&base).send().await.unwrap();
        assert_eq!(read_capped(response, 5).await.unwrap(), b"hello");
        served.await.unwrap();
    }

    fn stats(completed: bool) -> watch::Receiver<TorrentSwarmStats> {
        watch::channel(TorrentSwarmStats {
            uploaded: 0,
            downloaded: 0,
            wasted: 0,
            left: 0,
            written: 0,
            verified: bitvec::vec::BitVec::<u8, bitvec::order::Msb0>::new().into_boxed_bitslice(),
            wanted: bitvec::vec::BitVec::<u8, bitvec::order::Msb0>::new().into_boxed_bitslice(),
            completed,
            storage_error: None,
        })
        .1
    }

    fn identity() -> Arc<Identity> {
        Arc::new(Identity {
            peer_id: [1; 20],
            serving: "127.0.0.1:0".parse().unwrap(),
            dht: false,
            encryption: crate::config::Encryption::Disabled,
        })
    }

    /// A private tracker's passkey survives into the request, and its failure reason is what
    /// the announce fails with, even on a non-200 answer.
    #[tokio::test]
    async fn http_announce_reports_the_failure_reason() {
        let body = b"d14:failure reason11:bad passkeye";
        let mut response = format!(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        let (base, served) = http_server(response).await;
        let (events, _rx) = mpsc::channel(1);
        let announcer = HttpAnnouncer::new(
            Url::parse(&format!("{base}/announce?passkey=s3cret")).unwrap(),
            &announcing(&events),
            (Arc::new(watch::channel(vec![]).0), 0),
        );
        let e = announcer.announce(AnnounceEvent::Started).await.unwrap_err();
        assert_eq!(format!("{e:#}"), "HTTP 403 Forbidden: tracker: bad passkey");
        let request = served.await.unwrap();
        assert!(
            request.starts_with("GET /announce?passkey=s3cret&info_hash=%02%02"),
            "{request}"
        );
        assert!(request.contains("&event=started "), "{request}");
    }

    fn announcing(events: &mpsc::Sender<SwarmEvent>) -> Announcing {
        Announcing {
            trackers: vec![],
            info_hash: InfoHash::from_bytes(&[2; 20]),
            identity: identity(),
            stats: stats(false),
            events: events.downgrade(),
            shutdown: CancellationToken::new(),
            dht: crate::dht::Dht::none(),
            bus: EventBus::new(),
        }
    }

    /// Connect, then announce with the connection ID the tracker handed out.
    #[tokio::test]
    async fn udp_announce_against_a_fake_tracker() {
        let tracker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("udp://{}", tracker.local_addr().unwrap())).unwrap();
        let served = tokio::spawn(async move {
            let mut buf = [0; 1500];
            let (n, from) = tracker.recv_from(&mut buf).await.unwrap();
            assert_eq!(n, 16);
            let mut reply = 0i32.to_be_bytes().to_vec();
            reply.extend_from_slice(&buf[12..16]);
            reply.extend_from_slice(&77i64.to_be_bytes());
            tracker.send_to(&reply, from).await.unwrap();

            let (n, from) = tracker.recv_from(&mut buf).await.unwrap();
            assert_eq!(n, 100, "98 bytes and an empty BEP 41 option list");
            assert_eq!(buf[..8], 77i64.to_be_bytes(), "connection ID");
            assert_eq!(buf[80..84], 2i32.to_be_bytes(), "event=started");
            let mut reply = 1i32.to_be_bytes().to_vec();
            reply.extend_from_slice(&buf[12..16]);
            for n in [0i32, 0, 1] {
                reply.extend_from_slice(&n.to_be_bytes());
            }
            reply.extend_from_slice(&[10, 0, 0, 1, 0x1a, 0xe1]);
            tracker.send_to(&reply, from).await.unwrap();
        });
        let (events, _rx) = mpsc::channel(1);
        let mut announcer = UdpAnnouncer::new(url, &announcing(&events), (Arc::new(watch::channel(vec![]).0), 0));
        let (peers, interval) = announcer.announce(AnnounceEvent::Started).await.unwrap();
        assert_eq!(peers, ["10.0.0.1:6881".parse::<SocketAddr>().unwrap()]);
        assert_eq!(interval, ANNOUNCE_INTERVAL_MIN);
        served.await.unwrap();
    }

    /// One row per usable tracker, in order, plus a DHT row only when there is (or may be) a
    /// node; the rows exist before any announcer has done anything.
    #[tokio::test]
    async fn the_board_lists_every_announcer_up_front() {
        let identity = identity();
        let trackers = [
            "http://127.0.0.1:1/announce".to_string(),
            "wss://nope.test/announce".to_string(),
            "udp://127.0.0.1:1".to_string(),
            "udp://no-port.test".to_string(),
        ];
        let stats = stats(false);
        let (events, _rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let rows = spawn_announcers(Announcing {
            trackers: trackers.to_vec(),
            info_hash: InfoHash::from_bytes(&[2; 20]),
            identity: identity.clone(),
            stats: stats.clone(),
            events: events.downgrade(),
            shutdown: shutdown.clone(),
            dht: crate::dht::Dht::none(),
            bus: EventBus::new(),
        });
        let urls: Vec<String> = rows.borrow().iter().map(|r| r.url.clone()).collect();
        assert_eq!(
            urls,
            [
                "http://127.0.0.1:1/announce",
                "wss://nope.test/announce",
                "udp://127.0.0.1:1",
                "udp://no-port.test"
            ]
        );
        assert!(
            matches!(rows.borrow()[1].state, TrackerState::Failed(_)),
            "unsupported scheme"
        );
        assert!(matches!(rows.borrow()[3].state, TrackerState::Failed(_)), "no port");

        let (dht_tx, dht_rx) = watch::channel(None);
        let rows = spawn_announcers(Announcing {
            trackers: trackers[..1].to_vec(),
            info_hash: InfoHash::from_bytes(&[2; 20]),
            identity,
            stats,
            events: events.downgrade(),
            shutdown: shutdown.clone(),
            dht: dht_rx,
            bus: EventBus::new(),
        });
        let urls: Vec<String> = rows.borrow().iter().map(|r| r.url.clone()).collect();
        assert_eq!(urls, ["DHT", "http://127.0.0.1:1/announce"]);
        drop(dht_tx);
        shutdown.cancel();
    }
}
