//! What an HTTP and a UDP tracker's announcer share: the schedule and its backoff, BEP 3's
//! event bookkeeping, when to scrape, where outcomes go, and the loop that drives either.

use super::udp::UdpClient;
use super::{Announcing, Row, SwarmCounts, TrackerState, http};
use crate::events::{Event as BusEvent, PeerSource};
use crate::settings::{ANNOUNCE_INTERVAL_MAX, ANNOUNCE_INTERVAL_MIN, ANNOUNCE_RETRY, ANNOUNCE_RETRY_MAX};
use crate::torrent_swarm::SwarmEvent;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::{Instant, sleep_until};
use tracing::{Instrument, debug, info, warn};
use url::Url;

/// Peers asked of a tracker per announce. Left out, some trackers (Ubuntu's among them) hand
/// out a single peer; 200 is what libtorrent asks for, and peers are cheap to have on hand.
pub(super) const NUMWANT: u32 = 200;

/// The least time between scrapes of one tracker for one torrent; they only run when an
/// announce left the swarm's counts unsaid, and the completed count changes slowly.
pub(super) const SCRAPE_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// How long the courtesy event=stopped may take on the way out.
const STOPPED_TIMEOUT: Duration = Duration::from_secs(5);

/// What a successful announce brings back.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Announced {
    pub peers: Vec<SocketAddr>,
    pub interval: Duration,
    pub counts: SwarmCounts,
}

/// The tracker `event` parameter (BEP 3 for HTTP, BEP 15 for UDP). The first announce to a
/// tracker must be Started; a single announce reporting Completed should follow the download
/// finishing; Stopped is a courtesy announce on graceful shutdown so the tracker can drop us
/// immediately instead of waiting out the interval. Anything else is a regular periodic announce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AnnounceEvent {
    Regular,
    Started,
    Completed,
    Stopped,
}

/// What we tell a tracker about our progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Progress {
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
}

/// How long to wait before announcing again after `failures` consecutive failures.
fn retry_delay(failures: u32) -> Duration {
    ANNOUNCE_RETRY
        .saturating_mul(2u32.saturating_pow(failures.saturating_sub(1)))
        .min(ANNOUNCE_RETRY_MAX)
}

/// The wait until the next regular announce, from the tracker's `interval` and, if it sent
/// one, `min interval` (in seconds; zero, negative and absurd values all happen).
pub(super) fn announce_interval(interval: i64, min_interval: Option<i64>) -> Duration {
    let secs = interval.max(min_interval.unwrap_or(0)).clamp(
        ANNOUNCE_INTERVAL_MIN.as_secs() as i64,
        ANNOUNCE_INTERVAL_MAX.as_secs() as i64,
    );
    Duration::from_secs(secs as u64)
}

/// Tracker-supplied text, made safe to log and show: lossy UTF-8, cut at 200 characters.
pub(super) fn preview(bytes: &[u8]) -> String {
    const MAX: usize = 200;
    let text = String::from_utf8_lossy(bytes);
    match text.char_indices().nth(MAX) {
        Some((cut, _)) => format!("{}...", &text[..cut]),
        None => text.into_owned(),
    }
}

/// The protocol side of an announcer.
pub(super) enum Client {
    Http,
    Udp(UdpClient),
}

impl Client {
    pub fn udp() -> Client {
        Client::Udp(UdpClient::new())
    }

    async fn announce(&mut self, tracker: &Tracker, event: AnnounceEvent) -> anyhow::Result<Announced> {
        match self {
            Client::Http => http::announce(tracker, event).await,
            Client::Udp(udp) => udp.announce(tracker, event).await,
        }
    }

    async fn scrape(&mut self, tracker: &Tracker) -> anyhow::Result<SwarmCounts> {
        match self {
            Client::Http => http::scrape(tracker).await,
            Client::Udp(udp) => udp.scrape(tracker).await,
        }
    }

    /// After a failed announce: a UDP tracker may have been down or renumbered, so the next
    /// announce resolves and connects afresh.
    fn forget_connection(&mut self) {
        if let Client::Udp(udp) = self {
            udp.disconnect();
        }
    }

    /// Whether a stopped event can be sent without connecting first.
    fn connected(&self) -> bool {
        match self {
            Client::Http => true,
            Client::Udp(udp) => udp.is_connected(),
        }
    }
}

/// Announces to one tracker until shutdown, scraping it now and then, and says goodbye.
pub(super) async fn run(mut tracker: Tracker, mut client: Client) {
    let shutdown = tracker.args.shutdown.clone();
    while tracker.due().await {
        let event = tracker.next_event();
        let span = tracker.announce_span(event);
        let announced = tokio::select! {
            announced = client.announce(&tracker, event).instrument(span.clone()) => announced,
            _ = shutdown.cancelled() => break,
        };
        if announced.is_err() {
            client.forget_connection();
        }
        tracker.settle(event, announced).instrument(span).await;
        if tracker.wants_scrape() {
            let span = tracker.scrape_span();
            let scraped = tokio::select! {
                scraped = client.scrape(&tracker).instrument(span.clone()) => scraped,
                _ = shutdown.cancelled() => break,
            };
            span.in_scope(|| tracker.record_scrape(scraped));
        }
    }
    // best effort: we're on our way out regardless
    if tracker.sent_started && client.connected() {
        let _ = tokio::time::timeout(STOPPED_TIMEOUT, client.announce(&tracker, AnnounceEvent::Stopped)).await;
    }
}

/// One tracker's announcer, for one of the torrent's hashes.
pub(super) struct Tracker {
    pub url: Url,
    /// the torrent's, with the hash this announcer announces as its `info_hash`
    pub args: Announcing,
    row: Row,
    next_ready: Instant,
    sent_started: bool,
    sent_completed: bool,
    /// consecutive failed announces, for the retry backoff
    failures: u32,
    /// what the last successful announce said about the swarm, before any scrape filled in
    announced_counts: SwarmCounts,
    last_scrape: Option<Instant>,
}

impl Tracker {
    pub fn new(url: Url, args: Announcing, row: Row) -> Self {
        // a torrent that was already complete when resumed must not announce
        // event=completed again (BEP 3)
        let sent_completed = args.stats.borrow().completed;
        Tracker {
            url,
            args,
            row,
            next_ready: Instant::now() + Duration::from_millis(10),
            sent_started: false,
            sent_completed,
            failures: 0,
            announced_counts: SwarmCounts::default(),
            last_scrape: None,
        }
    }

    pub fn progress(&self) -> Progress {
        let stats = self.args.stats.borrow();
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
                Ok(()) = self.args.stats.changed(), if !self.sent_completed => {
                    if self.args.stats.borrow().completed && self.failures == 0 {
                        self.next_ready = Instant::now();
                    }
                }
                _ = self.args.shutdown.cancelled() => return false,
            }
        }
    }

    /// BEP 3: the first announce must carry event=started, and one carrying event=completed
    /// follows the download finishing (standing in for started if it comes first).
    fn next_event(&self) -> AnnounceEvent {
        if !self.sent_completed && self.args.stats.borrow().completed {
            AnnounceEvent::Completed
        } else if !self.sent_started {
            AnnounceEvent::Started
        } else {
            AnnounceEvent::Regular
        }
    }

    /// Whether to scrape after an announce: it succeeded but didn't say how many have
    /// completed the torrent (or anything about the swarm), and the last scrape is a while ago.
    fn wants_scrape(&self) -> bool {
        let swarm = self.announced_counts;
        let missing = swarm.downloaded.is_none() || swarm.seeders.is_none();
        self.failures == 0 && missing && self.last_scrape.is_none_or(|at| at.elapsed() >= SCRAPE_INTERVAL)
    }

    fn record_scrape(&mut self, scraped: anyhow::Result<SwarmCounts>) {
        self.last_scrape = Some(Instant::now());
        match scraped {
            Ok(counts) => {
                debug!("Tracker [{}] scrape: {counts:?}", self.url);
                tracing::Span::current().record("seeders", counts.seeders.unwrap_or(0));
                self.row.update(|row| row.swarm = row.swarm.updated(counts));
            }
            Err(e) => {
                tracing::Span::current().record("error", format!("{e:#}"));
                debug!("Tracker [{}] scrape: {e:#}", self.url);
            }
        }
    }

    /// The tracker's URL for the traces, without its query, where private trackers keep the
    /// user's passkey.
    fn traced_url(&self) -> Url {
        let mut url = self.url.clone();
        url.set_query(None);
        url
    }

    fn scrape_span(&self) -> tracing::Span {
        tracing::info_span!(
            "tracker.scrape",
            info_hash = %self.args.info_hash,
            url = %self.traced_url(),
            seeders = tracing::field::Empty,
            error = tracing::field::Empty,
        )
    }

    fn announce_span(&self, event: AnnounceEvent) -> tracing::Span {
        tracing::info_span!(
            "tracker.announce",
            info_hash = %self.args.info_hash,
            url = %self.traced_url(),
            event = ?event,
            peers = tracing::field::Empty,
            interval_secs = tracing::field::Empty,
            seeders = tracing::field::Empty,
            leechers = tracing::field::Empty,
            error = tracing::field::Empty,
        )
    }

    /// Books an announce's outcome: the schedule or backoff, the board row, the event bus, and
    /// the peers to the swarm.
    async fn settle(&mut self, event: AnnounceEvent, announced: anyhow::Result<Announced>) {
        let span = tracing::Span::current();
        match announced {
            Ok(Announced {
                peers,
                interval,
                counts,
            }) => {
                span.record("peers", peers.len());
                span.record("interval_secs", interval.as_secs());
                if let Some(n) = counts.seeders {
                    span.record("seeders", n);
                }
                if let Some(n) = counts.leechers {
                    span.record("leechers", n);
                }
                self.sent_started = true;
                self.sent_completed |= event == AnnounceEvent::Completed;
                self.failures = 0;
                self.announced_counts = counts;
                self.next_ready = Instant::now() + interval;
                let (count, next) = (peers.len(), self.next_ready);
                info!(
                    "Tracker [{}] returned {count} peers, next announce in {}s",
                    self.url,
                    interval.as_secs()
                );
                self.row.update(|row| {
                    row.state = TrackerState::Working;
                    row.peers = count;
                    row.next_announce = Some(next);
                    row.swarm = row.swarm.updated(counts);
                });
                self.args.bus.emit(BusEvent::Announced {
                    info_hash: self.args.info_hash,
                    url: self.url.to_string(),
                    peers: count,
                    interval_secs: interval.as_secs(),
                });
                if let Some(events) = self.args.events.upgrade() {
                    let source = PeerSource::Tracker {
                        url: self.url.to_string(),
                    };
                    tokio::select! {
                        _ = events.send(SwarmEvent::PeersDiscovered(peers, source)) => {}
                        _ = self.args.shutdown.cancelled() => {}
                    }
                }
            }
            Err(e) => {
                let error = format!("{e:#}");
                span.record("error", &error);
                warn!("Tracker [{}]: {error}", self.url);
                self.failures += 1;
                self.next_ready = Instant::now() + retry_delay(self.failures);
                let next = self.next_ready;
                self.row.update(|row| {
                    row.state = TrackerState::Failed(error.clone());
                    row.next_announce = Some(next);
                });
                self.args.bus.emit(BusEvent::AnnounceFailed {
                    info_hash: self.args.info_hash,
                    url: self.url.to_string(),
                    error,
                });
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::test::{announcing, row};
    use super::*;
    use anyhow::anyhow;
    use tokio::sync::mpsc;

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

    #[test]
    fn preview_cuts_long_text_on_a_character() {
        assert_eq!(preview(b"short"), "short");
        let long = "é".repeat(300);
        assert_eq!(preview(long.as_bytes()), format!("{}...", "é".repeat(200)));
    }

    /// BEP 3's events: started first, completed once when the download finishes (also for the
    /// first announce of one that finished before it), regular otherwise.
    #[tokio::test]
    async fn events_follow_bep_3() {
        let (events, _rx) = mpsc::channel(8);
        let (stats_tx, stats) = super::super::test::stats(false);
        let mut tracker = Tracker::new(
            Url::parse("http://t.test/announce").unwrap(),
            Announcing {
                stats,
                ..announcing(&events)
            },
            row(),
        );
        let ok = || {
            Ok(Announced {
                peers: vec![],
                interval: Duration::from_secs(1800),
                counts: SwarmCounts::default(),
            })
        };
        assert_eq!(tracker.next_event(), AnnounceEvent::Started);
        tracker.settle(AnnounceEvent::Started, Err(anyhow!("down"))).await;
        assert_eq!(tracker.next_event(), AnnounceEvent::Started, "until one succeeds");
        tracker.settle(AnnounceEvent::Started, ok()).await;
        assert_eq!(tracker.next_event(), AnnounceEvent::Regular);
        stats_tx.send_modify(|s| s.completed = true);
        assert_eq!(tracker.next_event(), AnnounceEvent::Completed);
        tracker.settle(AnnounceEvent::Completed, ok()).await;
        assert_eq!(tracker.next_event(), AnnounceEvent::Regular);

        let resumed_complete = Tracker::new(
            Url::parse("http://t.test/announce").unwrap(),
            Announcing {
                stats: super::super::test::stats(true).1,
                ..announcing(&events)
            },
            row(),
        );
        assert_eq!(resumed_complete.next_event(), AnnounceEvent::Started);
    }

    /// A tracker whose announces never say how many have completed (every UDP tracker) is
    /// scraped again every SCRAPE_INTERVAL, not just once: the count a scrape filled in must not
    /// pass for the announce having said it.
    #[tokio::test]
    async fn scrapes_again_while_announces_leave_counts_out() {
        let (events, _rx) = mpsc::channel(8);
        let mut tracker = Tracker::new(Url::parse("udp://t.test:1").unwrap(), announcing(&events), row());
        let announced = || Announced {
            peers: vec![],
            interval: Duration::from_secs(1800),
            counts: SwarmCounts {
                seeders: Some(3),
                leechers: Some(4),
                downloaded: None,
            },
        };
        tracker.settle(AnnounceEvent::Started, Ok(announced())).await;
        assert!(tracker.wants_scrape());
        tracker.record_scrape(Ok(SwarmCounts {
            seeders: Some(3),
            leechers: Some(4),
            downloaded: Some(50),
        }));
        assert!(!tracker.wants_scrape(), "just scraped");
        tracker.last_scrape = Instant::now().checked_sub(SCRAPE_INTERVAL);
        tracker.settle(AnnounceEvent::Regular, Ok(announced())).await;
        assert!(tracker.wants_scrape(), "the completed count would go stale");
        tracker.settle(AnnounceEvent::Regular, Err(anyhow!("down"))).await;
        assert!(!tracker.wants_scrape(), "not while the tracker fails");
    }
}
