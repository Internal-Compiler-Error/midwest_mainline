//! The peer sources that announce a torrent: trackers over HTTP (BEP 3, scraped per BEP 48) and
//! UDP (BEP 15), and the DHT (BEP 5, with BEP 33's swarm estimates). One task per source, each
//! with a row on the torrent's tracker board, reporting the peers it learns about as
//! `SwarmEvent::PeersDiscovered` and reading the swarm's progress off its stats watch. Keyed on
//! a bare `InfoHash` rather than a `Torrent`: a magnet link's pre-metadata phase (see
//! `metadata::fetch`) announces with nothing else to give, which is why these don't live
//! inside `TorrentSwarm`.

mod dht;
mod http;
mod tracker;
mod udp;

pub(crate) use http::percent_encode;

use crate::defs::Identity;
use crate::dht::DhtWatch;
use crate::events::EventBus;
use crate::external::ExternalAddress;
use crate::torrent_swarm::{SwarmEvent, TorrentSwarmStats};
use midwest_mainline::message::parse_compact_addr;
use midwest_mainline::types::InfoHash;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use tracker::{Client, Tracker};
use url::Url;

/// What a tracker (or the DHT) has done for a torrent lately, for the details panel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackerStatus {
    /// the announce URL, or "DHT"
    pub url: String,
    pub state: TrackerState,
    /// peers the last successful announce returned
    pub peers: usize,
    pub next_announce: Option<Instant>,
    /// the swarm's size as the tracker counts it, from its announce replies or a scrape
    pub swarm: SwarmCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrackerState {
    /// not announced yet
    Pending,
    Working,
    /// the last announce failed; retried with backoff
    Failed(String),
}

/// A tracker's count of a torrent's swarm (BEP 3 announce fields, BEP 48 / BEP 15 scrape).
/// Each is `None` until the tracker has said.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SwarmCounts {
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    /// how many times the torrent has been downloaded to completion
    pub downloaded: Option<u32>,
}

impl SwarmCounts {
    /// `newer`'s counts, falling back to these where it says nothing.
    fn updated(self, newer: SwarmCounts) -> SwarmCounts {
        SwarmCounts {
            seeders: newer.seeders.or(self.seeders),
            leechers: newer.leechers.or(self.leechers),
            downloaded: newer.downloaded.or(self.downloaded),
        }
    }
}

/// One announcer's row on the board that `spawn_announcers` returns.
#[derive(Debug, Clone)]
struct Row {
    board: Arc<watch::Sender<Vec<TrackerStatus>>>,
    slot: usize,
}

impl Row {
    fn update(&self, update: impl FnOnce(&mut TrackerStatus)) {
        self.board.send_modify(|rows| {
            if let Some(row) = rows.get_mut(self.slot) {
                update(row);
            }
        });
    }
}

/// What every announcer of one torrent shares.
#[derive(Clone)]
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
    /// where trackers' BEP 24 `external ip` goes
    pub external: ExternalAddress,
    /// BEP 52: a hybrid's truncated v2 hash, announced too (to the DHT and every tracker, as
    /// a swarm of its own) so v2-only peers find us
    pub v2: Option<InfoHash>,
}

/// Where one announcer gets its peers.
#[derive(Clone)]
enum Source {
    Dht,
    Http(Url),
    Udp(Url),
}

impl Source {
    /// The source a tracker URL names, or why it can't be announced to.
    fn of(url: &Url) -> Result<Source, String> {
        match url.scheme() {
            "http" | "https" => Ok(Source::Http(url.clone())),
            "udp" if url.host().is_some() && url.port().is_some() => Ok(Source::Udp(url.clone())),
            "udp" => Err("a UDP tracker URL needs a host and a port".into()),
            scheme => Err(format!("unsupported scheme {scheme:?}")),
        }
    }
}

/// Spawns an announcer per source: the DHT if the client has (or may yet have) a node, and
/// every usable URL in `trackers`; a hybrid gets two of each, one per hash. Every source has
/// a row on the returned board from the start, an unusable tracker URL a failed one.
pub(crate) fn spawn_announcers(args: Announcing) -> watch::Receiver<Vec<TrackerStatus>> {
    let per_hash = |label: &str| {
        std::iter::once((label.to_owned(), args.info_hash))
            .chain(args.v2.map(|v2| (format!("{label} (v2 hash)"), v2)))
            .collect::<Vec<_>>()
    };
    let mut planned: Vec<(String, Result<Source, String>, InfoHash)> = vec![];
    // a watch whose sender is gone is a client with no DHT, now or ever; no row for it
    if args.dht.has_changed().is_ok() {
        planned.extend(
            per_hash("DHT")
                .into_iter()
                .map(|(label, hash)| (label, Ok(Source::Dht), hash)),
        );
    }
    for url in args.trackers.iter().filter_map(|t| Url::parse(t).ok()) {
        match Source::of(&url) {
            Ok(source) => planned.extend(
                per_hash(url.as_str())
                    .into_iter()
                    .map(|(label, hash)| (label, Ok(source.clone()), hash)),
            ),
            Err(why) => {
                warn!("ignoring tracker {url}: {why}");
                planned.push((url.to_string(), Err(why), args.info_hash));
            }
        }
    }

    let rows = planned
        .iter()
        .map(|(label, source, _)| TrackerStatus {
            url: label.clone(),
            state: match source {
                Ok(_) => TrackerState::Pending,
                Err(why) => TrackerState::Failed(why.clone()),
            },
            peers: 0,
            next_announce: None,
            swarm: SwarmCounts::default(),
        })
        .collect();
    let (board, statuses) = watch::channel(rows);
    let board = Arc::new(board);
    for (slot, (_, source, info_hash)) in planned.into_iter().enumerate() {
        let Ok(source) = source else { continue };
        let row = Row {
            board: board.clone(),
            slot,
        };
        let args = Announcing {
            info_hash,
            ..args.clone()
        };
        match source {
            Source::Dht => {
                tokio::spawn(dht::announce(args, row));
            }
            Source::Http(url) => {
                tokio::spawn(tracker::run(Tracker::new(url, args, row), Client::Http));
            }
            Source::Udp(url) => {
                tokio::spawn(tracker::run(Tracker::new(url, args, row), Client::udp()));
            }
        }
    }
    statuses
}

/// Compact peer info (BEP 23, and BEP 7's IPv6 form): `size`-byte entries, 6 for IPv4 and 18
/// for IPv6. A trailing partial entry is dropped.
fn compact_peers(bytes: &[u8], size: usize) -> impl Iterator<Item = SocketAddr> + '_ {
    bytes.chunks_exact(size).filter_map(parse_compact_addr)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    pub(super) fn stats(completed: bool) -> (watch::Sender<TorrentSwarmStats>, watch::Receiver<TorrentSwarmStats>) {
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
    }

    /// Announcing a torrent with info hash `[2; 20]` and no trackers, DHT, or v2 hash.
    pub(super) fn announcing(events: &mpsc::Sender<SwarmEvent>) -> Announcing {
        Announcing {
            trackers: vec![],
            info_hash: InfoHash::from_bytes(&[2; 20]),
            identity: Arc::new(Identity {
                peer_id: [1; 20],
                serving: "127.0.0.1:0".parse().unwrap(),
                dht: false,
                encryption: crate::config::Encryption::Disabled,
            }),
            stats: stats(false).1,
            events: events.downgrade(),
            shutdown: CancellationToken::new(),
            dht: crate::dht::Dht::none(),
            bus: EventBus::new(),
            external: Default::default(),
            v2: None,
        }
    }

    /// A board of one pending row, and that row.
    pub(super) fn row() -> Row {
        let status = TrackerStatus {
            url: "t".into(),
            state: TrackerState::Pending,
            peers: 0,
            next_announce: None,
            swarm: SwarmCounts::default(),
        };
        Row {
            board: Arc::new(watch::channel(vec![status]).0),
            slot: 0,
        }
    }

    #[test]
    fn compact_peers_of_both_families() {
        let mut v4 = vec![10, 0, 0, 1, 0x1a, 0xe1];
        v4.extend_from_slice(&[1, 2, 3]);
        assert_eq!(
            compact_peers(&v4, 6).collect::<Vec<_>>(),
            ["10.0.0.1:6881".parse::<SocketAddr>().unwrap()]
        );
        let mut v6 = [0; 18];
        v6[15] = 1;
        v6[17] = 7;
        assert_eq!(
            compact_peers(&v6, 18).collect::<Vec<_>>(),
            ["[::1]:7".parse::<SocketAddr>().unwrap()]
        );
    }

    /// BEP 52: a hybrid is announced to each tracker under both hashes, a row each, and the
    /// peers of both swarms go to the one swarm.
    #[tokio::test]
    async fn a_hybrid_is_announced_under_both_hashes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/announce", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 4096];
                let n = stream.read(&mut request).await.unwrap();
                let request = String::from_utf8_lossy(&request[..n]).into_owned();
                // a peer per swarm: 10.0.0.2 for the v1 hash, 10.0.0.3 for the v2 one
                let last = if request.contains("info_hash=%03%03") { 3 } else { 2 };
                let mut body = b"d8:intervali1800e5:peers6:".to_vec();
                body.extend_from_slice(&[10, 0, 0, last, 0x1a, 0xe1]);
                body.push(b'e');
                let mut response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .into_bytes();
                response.extend_from_slice(&body);
                let _ = stream.write_all(&response).await;
            }
        });
        let (events, mut rx) = mpsc::channel(8);
        let shutdown = CancellationToken::new();
        let rows = spawn_announcers(Announcing {
            trackers: vec![url.clone(), "wss://nope.test/announce".to_string()],
            v2: Some(InfoHash::from_bytes(&[3; 20])),
            shutdown: shutdown.clone(),
            ..announcing(&events)
        });
        let urls: Vec<String> = rows.borrow().iter().map(|r| r.url.clone()).collect();
        assert_eq!(
            urls,
            [
                url.clone(),
                format!("{url} (v2 hash)"),
                "wss://nope.test/announce".to_string()
            ],
            "an unusable tracker gets one row"
        );
        let mut found = std::collections::BTreeSet::new();
        while found.len() < 2 {
            let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
                .await
                .unwrap()
                .unwrap();
            if let SwarmEvent::PeersDiscovered(peers, _) = event {
                found.extend(peers);
            }
        }
        let expected: std::collections::BTreeSet<SocketAddr> = ["10.0.0.2:6881", "10.0.0.3:6881"]
            .iter()
            .map(|a| a.parse().unwrap())
            .collect();
        assert_eq!(found, expected);
        shutdown.cancel();
    }

    /// One row per usable tracker, in order, plus a DHT row only when there is (or may be) a
    /// node; the rows exist before any announcer has done anything.
    #[tokio::test]
    async fn the_board_lists_every_announcer_up_front() {
        let trackers = [
            "http://127.0.0.1:1/announce".to_string(),
            "wss://nope.test/announce".to_string(),
            "udp://127.0.0.1:1".to_string(),
            "udp://no-port.test".to_string(),
        ];
        let (events, _rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let rows = spawn_announcers(Announcing {
            trackers: trackers.to_vec(),
            shutdown: shutdown.clone(),
            ..announcing(&events)
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
            shutdown: shutdown.clone(),
            dht: dht_rx,
            ..announcing(&events)
        });
        let urls: Vec<String> = rows.borrow().iter().map(|r| r.url.clone()).collect();
        assert_eq!(urls, ["DHT", "http://127.0.0.1:1/announce"]);
        drop(dht_tx);
        shutdown.cancel();
    }
}
