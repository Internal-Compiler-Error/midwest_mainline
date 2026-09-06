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

/// Bound on everything between a connection opening and the BitTorrent handshake completing
/// (an MSE exchange, possibly a plaintext retry). A peer that stalls here holds nothing
/// worth waiting for.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

/// Endgame: once every missing piece is in flight and a peer still has room, the pieces
/// furthest from done are also requested from other peers, in the opposite block order,
/// and whoever finishes second gets Cancel for the rest. This many peers may hold the
/// same piece at once.
pub const ENDGAME_RACERS: usize = 2;

/// Endgame: at most this many bytes of pieces may be raced at any time, which bounds the
/// bytes downloaded twice. A cap in pieces let a torrent with small pieces race far more of
/// itself than one with large pieces (2% waste on a 512 KiB-piece ISO against 0.2% on a
/// 4 MiB-piece video); the piece the user is waiting on always gets a racer even if it's
/// bigger than this.
pub const ENDGAME_MAX_RACED_BYTES: usize = 32 * 1024 * 1024;

/// How long to leave an address alone after a failed dial. Doubles with each consecutive
/// failure up to DIAL_BACKOFF_MAX; trackers and PEX keep handing out the same dead
/// addresses, and without this each one is redialed every time it comes around.
pub const DIAL_BACKOFF: Duration = Duration::from_secs(2 * 60);
pub const DIAL_BACKOFF_MAX: Duration = Duration::from_secs(60 * 60);

/// How long to leave a peer alone after it disconnected without a single block exchanged
/// in either direction. A connection that produced nothing is likely to do so again.
pub const FRUITLESS_PEER_COOLDOWN: Duration = Duration::from_secs(10 * 60);

/// How long a peer stays banned after sending a piece that failed its hash, or breaking the
/// protocol. Inbound connections from it are refused too. Pieces are assigned whole to one
/// peer, so a bad piece identifies its sender exactly.
pub const BAD_PEER_BAN: Duration = Duration::from_secs(2 * 60 * 60);

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

/// BEP 14: how often to tell the local network which torrents we serve. The BEP suggests
/// five minutes.
pub const LSD_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// How often to look a torrent up in the DHT and re-announce ourselves for it. Announced
/// peers expire from nodes after roughly 45 minutes, so this is comfortably inside that.
pub const DHT_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// BEP 11 (PEX): the spec recommends capping a single message at roughly 50 added peers.
pub const PEX_MAX_ADDED_PEERS: usize = 50;
