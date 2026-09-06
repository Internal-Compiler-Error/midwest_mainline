//! Everything the client does that a front end might want to see, as one ordered stream: peers
//! coming and going, every UCB pick, pieces verified, announces, port mapping, torrent
//! lifecycle. The library emits raw facts and leaves the accumulating to whoever listens,
//! so an event is what happened, never a rolling average of it.
//!
//! The bus is a `broadcast` channel: any number of subscribers, each reading at its own pace,
//! and one that falls `CAPACITY` events behind loses the oldest and is told so through
//! `Event::Lagged` rather than an error. With no subscriber at all, emitting costs a clone
//! into the ring buffer and nothing more.

use midwest_mainline::types::InfoHash;
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

use crate::portmap::MappingState;

/// Events a slow subscriber can fall behind before losing the oldest. A busy swarm emits
/// a few hundred a second (a sample per peer per second plus a pick per piece request).
const CAPACITY: usize = 16 * 1024;

/// An event with its place in the stream and when it happened (Unix milliseconds).
#[derive(Clone, Debug, Serialize)]
pub struct Stamped {
    pub seq: u64,
    pub at_ms: u64,
    #[serde(flatten)]
    pub event: Event,
}

/// Where a batch of peer addresses came from.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "via", rename_all = "snake_case")]
pub enum PeerSource {
    Tracker {
        url: String,
    },
    Dht,
    Pex {
        from: SocketAddr,
    },
    Lsd,
    /// peers the metadata fetch met, handed to the swarm when it starts
    Metadata,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    // -- the client's network
    /// an inbound listener is up
    Listening {
        transport: &'static str,
        port: u16,
    },
    PortMapping {
        state: MappingState,
    },
    DhtUp {
        port: u16,
        nodes: usize,
    },

    // -- a torrent's life
    /// a source became a torrent: parsed from a file, or fetched from peers for a magnet
    TorrentResolved {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        name: String,
        size: u64,
        pieces: usize,
        piece_size: u32,
        files: usize,
    },
    MetadataFetched {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        from: SocketAddr,
        bytes: usize,
        took_ms: u64,
    },
    /// its swarm is running, with this many pieces already on disk
    TorrentStarted {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        verified: usize,
        pieces: usize,
    },
    TorrentQueued {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
    },
    TorrentPaused {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
    },
    TorrentChecked {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        good: usize,
        pieces: usize,
    },
    TorrentCompleted {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
    },
    TorrentFailed {
        source: String,
        error: String,
    },
    TorrentRemoved {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        deleted_files: bool,
    },

    // -- peers
    PeersDiscovered {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        source: PeerSource,
        count: usize,
    },
    DialFailed {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        addr: SocketAddr,
    },
    PeerConnected {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        addr: SocketAddr,
        client: String,
        /// we called them, as opposed to accepting their connection
        dialed: bool,
        encrypted: bool,
        utp: bool,
    },
    PeerDisconnected {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        addr: SocketAddr,
        downloaded: u64,
        uploaded: u64,
        reason: &'static str,
    },
    /// a choke state flipped: `by_us` for our side of it, otherwise theirs
    ChokeChanged {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        addr: SocketAddr,
        choked: bool,
        by_us: bool,
    },
    /// UCB chose a peer to request a piece from. `exploit` is its measured rate relative to
    /// the fastest peer, `explore` the confidence bonus; the pick is the highest sum. A peer
    /// never picked before has no finite bonus (it's picked first no matter what): `None`.
    PeerPicked {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        addr: SocketAddr,
        piece: u32,
        exploit: f64,
        explore: Option<f64>,
        picked_count: usize,
        total_picks: usize,
    },
    /// one per connected peer, once a second
    PeerSample {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        addr: SocketAddr,
        rx_bps: f64,
        downloaded: u64,
        uploaded: u64,
        outstanding: usize,
        choked_us: bool,
        choked_them: bool,
    },

    // -- pieces and bytes
    /// the swarm's starting point: which pieces it already had, as the BEP 3 bitfield in hex
    PiecesKnown {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        bitfield: String,
    },
    PieceVerified {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        piece: u32,
        len: usize,
        /// who delivered blocks of it
        peers: Vec<SocketAddr>,
    },
    PieceFailed {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        piece: u32,
        peers: Vec<SocketAddr>,
    },
    BlockWasted {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        addr: SocketAddr,
        len: u32,
        why: &'static str,
    },
    /// running totals for the torrent, once a second
    Traffic {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        downloaded: u64,
        uploaded: u64,
        wasted: u64,
        peers: usize,
    },

    // -- trackers and the DHT
    Announced {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        url: String,
        peers: usize,
        interval_secs: u64,
    },
    AnnounceFailed {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        url: String,
        error: String,
    },
    DhtLookup {
        #[serde(serialize_with = "hex")]
        info_hash: InfoHash,
        peers: usize,
        took_ms: u64,
    },

    /// this subscriber fell behind and missed that many events
    Lagged {
        missed: u64,
    },
}

fn hex<S: serde::Serializer>(hash: &InfoHash, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&info_hash_hex(hash))
}

/// The 40 hex digits of an info hash, how the events name a torrent.
pub fn info_hash_hex(hash: &InfoHash) -> String {
    hash.0.iter().map(|b| format!("{b:02x}")).collect()
}

/// The sender side; clone it into whatever emits. Cheap to clone, shares the stream.
#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<Stamped>,
    seq: Arc<AtomicU64>,
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EventBus({} events so far)", self.seq.load(Ordering::Relaxed))
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            tx: broadcast::channel(CAPACITY).0,
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn emit(&self, event: Event) {
        let stamped = Stamped {
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
            at_ms: now_ms(),
            event,
        };
        tracing::trace!(target: "downloader::events", "{stamped:?}");
        // no subscriber is not an error
        let _ = self.tx.send(stamped);
    }

    pub fn subscribe(&self) -> Events {
        Events {
            rx: self.tx.subscribe(),
            last_seq: 0,
        }
    }
}

/// One subscriber's view of the stream, from the moment it subscribed.
pub struct Events {
    rx: broadcast::Receiver<Stamped>,
    /// of the last event delivered, so a synthesized `Lagged` doesn't reset the order
    last_seq: u64,
}

impl Events {
    /// The next event; `None` once the bus is gone. Falling behind yields `Event::Lagged`
    /// and then carries on from the oldest event still buffered.
    pub async fn next(&mut self) -> Option<Stamped> {
        match self.rx.recv().await {
            Ok(event) => Some(self.seen(event)),
            Err(broadcast::error::RecvError::Lagged(missed)) => Some(self.lagged(missed)),
            Err(broadcast::error::RecvError::Closed) => None,
        }
    }

    /// Whatever has arrived so far, without waiting.
    pub fn drain(&mut self) -> Vec<Stamped> {
        let mut out = Vec::new();
        loop {
            match self.rx.try_recv() {
                Ok(event) => out.push(self.seen(event)),
                Err(broadcast::error::TryRecvError::Lagged(missed)) => out.push(self.lagged(missed)),
                Err(_) => return out,
            }
        }
    }

    fn seen(&mut self, event: Stamped) -> Stamped {
        self.last_seq = event.seq;
        event
    }

    fn lagged(&self, missed: u64) -> Stamped {
        Stamped {
            seq: self.last_seq,
            at_ms: now_ms(),
            event: Event::Lagged { missed },
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(n: u8) -> InfoHash {
        InfoHash([n; 20])
    }

    #[tokio::test]
    async fn events_arrive_in_order_with_a_sequence_number() {
        let bus = EventBus::new();
        let mut events = bus.subscribe();
        bus.emit(Event::TorrentQueued { info_hash: hash(1) });
        bus.emit(Event::TorrentCompleted { info_hash: hash(1) });
        let first = events.next().await.unwrap();
        let second = events.next().await.unwrap();
        assert_eq!((first.seq, second.seq), (0, 1));
        assert!(matches!(first.event, Event::TorrentQueued { .. }));
        assert!(matches!(second.event, Event::TorrentCompleted { .. }));
        assert!(first.at_ms > 1_700_000_000_000);
    }

    #[tokio::test]
    async fn a_slow_subscriber_is_told_what_it_missed() {
        let bus = EventBus::new();
        let mut events = bus.subscribe();
        for _ in 0..CAPACITY + 10 {
            bus.emit(Event::Lagged { missed: 0 });
        }
        let first = events.next().await.unwrap();
        assert_eq!(first.event, Event::Lagged { missed: 10 });
        let rest = events.drain();
        assert_eq!(rest.len(), CAPACITY);
        // a lag notice after real events keeps their place in the order
        for _ in 0..CAPACITY + 1 {
            bus.emit(Event::Lagged { missed: 0 });
        }
        let notice = events.next().await.unwrap();
        assert_eq!(notice.event, Event::Lagged { missed: 1 });
        assert_eq!(notice.seq, rest.last().unwrap().seq);
    }

    #[test]
    fn serializes_as_tagged_json_with_hex_hashes() {
        let stamped = Stamped {
            seq: 7,
            at_ms: 1,
            event: Event::PeerConnected {
                info_hash: hash(0xab),
                addr: "10.0.0.1:6881".parse().unwrap(),
                client: "qBittorrent 5.0".into(),
                dialed: true,
                encrypted: false,
                utp: true,
            },
        };
        let json = serde_json::to_value(&stamped).unwrap();
        assert_eq!(json["kind"], "peer_connected");
        assert_eq!(json["seq"], 7);
        assert_eq!(json["info_hash"], "ab".repeat(20));
        assert_eq!(json["addr"], "10.0.0.1:6881");
        let source = serde_json::to_value(PeerSource::Tracker { url: "http://t".into() }).unwrap();
        assert_eq!(source, serde_json::json!({ "via": "tracker", "url": "http://t" }));
    }
}
