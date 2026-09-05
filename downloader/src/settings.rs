use std::time::Duration;

pub const BLOCK_SIZE: usize = 16 * 1024; // 16 KiB

/// BEP 9: metadata (the info dict) is exchanged in fixed 16KiB pieces, same size as BLOCK_SIZE
/// but conceptually distinct (one is torrent data, the other is the torrent's own metadata).
pub const METADATA_PIECE_SIZE: usize = 16 * 1024;

/// Total bytes of pieces being downloaded at once, across all peers. In bytes rather than
/// pieces because piece sizes range from 16 KiB to several MiB, and each in-flight piece is
/// held in memory until it completes. Each piece occupies one peer until it's done, so this
/// also bounds how many peers are downloading from at once.
pub const MAX_INFLIGHT_BYTES: usize = 128 * 1024 * 1024;

/// How much of a peer's measured throughput to keep requested from it: its request window
/// is this many seconds of data, in blocks (see `Peer::request_window`). The same idea as a
/// TCP congestion window sized to the bandwidth-delay product, except both terms are
/// measured directly. It must comfortably exceed the request-to-delivery latency, or the
/// measured rate can never grow the window.
pub const REQUEST_PIPELINE_TARGET: Duration = Duration::from_secs(3);

/// Floor of the request window: what a peer with no measured rate yet is asked for, and
/// enough that the queue can't empty between a delivery and the refill that follows it.
pub const MIN_REQUEST_WINDOW: usize = 4;

/// Ceiling of the request window. Remote clients cap how many requests they'll queue
/// (commonly 250-500) and reject, drop, or disconnect past it.
pub const MAX_REQUEST_WINDOW: usize = 128;

/// How long to wait for a peer's TCP connection to come up. Most addresses a tracker hands
/// out are behind NAT or gone, and the OS default (over a minute) would hold a dial slot that
/// long for each of them.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a peer that owes us blocks may go without delivering any before the pieces it's
/// working on are taken back (see `Peer::stalled`). A peer that goes silent mid-request, as
/// opposed to disconnecting outright, is the failure mode this guards against.
pub const BLOCK_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How far back a peer's download throughput is measured over. Shorter reacts faster to a
/// peer's upload slots changing hands, longer smooths out TCP burstiness.
pub const RATE_WINDOW: Duration = Duration::from_secs(10);

/// BEP 3: "it is common to send a message every two minutes to keep the connection alive".
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(100);

/// A peer that sends nothing at all -- not even a keep-alive -- for this long is treated as
/// dead. Comfortably more than KEEPALIVE_INTERVAL so a single delayed tick doesn't trip it.
pub const PEER_TIMEOUT: Duration = Duration::from_secs(220);

/// How often to re-run the choking algorithm (tit-for-tat unchoking, plus an occasional
/// optimistic unchoke). Real clients commonly use 10s.
pub const CHOKING_ROUND_INTERVAL: Duration = Duration::from_secs(10);

/// How many interested peers we keep unchoked at once based on reciprocation (the download
/// rate they've been giving us), not counting the optimistic slot.
pub const MAX_UNCHOKED_PEERS: usize = 4;

/// Every this-many choking rounds, one additional peer is unchoked at random regardless of
/// its reciprocation rate, so a new or currently-worse peer gets a chance to prove itself
/// instead of the same top N being unchoked forever.
pub const OPTIMISTIC_UNCHOKE_EVERY_N_ROUNDS: u64 = 3;

/// BEP 11 (PEX): how often to send each peer our current view of the swarm. The spec asks for
/// "not more frequently than once per minute".
pub const PEX_INTERVAL: Duration = Duration::from_secs(60);

/// BEP 11 (PEX): the spec recommends capping a single message at roughly 50 added peers.
pub const PEX_MAX_ADDED_PEERS: usize = 50;
