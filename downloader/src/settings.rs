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

/// Block requests a peer may have queued with us at once; advertised as `reqq` (BEP 10) and
/// enforced, since each one is a disk read and a block of memory until it's sent.
pub const MAX_QUEUED_UPLOADS: usize = 500;

/// The largest block we serve. BEP 3 clients ask for 16 KiB; some go up to 128 KiB.
pub const MAX_SERVED_BLOCK: u32 = 128 * 1024;

/// How long to wait for a peer's TCP connection to come up. Most addresses a tracker hands
/// out are behind NAT or gone, and the OS default (over a minute) would hold a dial slot that
/// long for each of them.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Happy eyeballs (RFC 8305) between TCP and uTP: how long a dial waits on the preferred
/// transport before trying the other one in parallel. A NATed or firewalled peer that only
/// one of them reaches then costs this instead of a whole `CONNECT_TIMEOUT`, while a peer that
/// answers within a typical round trip is reached over the preferred transport alone.
pub const HAPPY_EYEBALLS_DELAY: Duration = Duration::from_millis(250);

/// Bound on everything between starting to dial and the BitTorrent handshake completing: TCP
/// and uTP connects, an MSE exchange, possibly a plaintext retry. A peer that stalls here
/// holds nothing worth waiting for.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// Outbound connection attempts in flight across the whole client. Each is a half-open socket
/// for up to HANDSHAKE_TIMEOUT, and thousands at once overflow a home router's NAT table and
/// look like a SYN flood; the rest wait their turn.
pub const MAX_HALF_OPEN: usize = 256;

/// Inbound connections in their opening exchange at once (an MSE handshake costs a 768-bit
/// modular exponentiation); past this, new ones are closed straight away.
pub const MAX_INBOUND_HANDSHAKES: usize = 256;

/// uTP connections at once on our UDP socket, inbound and outbound together. librqbit-utp's
/// default of 128 would cap uTP peers client-wide far below what the peer limits allow.
pub const UTP_MAX_CONNECTIONS: usize = 4096;

/// A uTP connection's receive window, which is what bounds its throughput (one window per
/// round trip): 4 MiB is about 330 Mbit/s at 100 ms. The window is a limit, not an
/// allocation; buffered bytes are what a slow reader leaves unread. Also the cap the send
/// buffer grows to.
pub const UTP_WINDOW: usize = 4 * 1024 * 1024;

/// What the uTP UDP socket's kernel receive buffer is asked for, to absorb bursts from many
/// peers while the dispatcher catches up. Linux clamps to `net.core.rmem_max`; macOS refuses
/// anything over `kern.ipc.maxsockbuf` outright, so the largest size it takes is searched for.
pub const UTP_UDP_RECV_BUFFER: usize = 32 * 1024 * 1024;

/// How long one write to a peer may take before the connection is given up on. A peer that
/// stops reading (zero receive window) otherwise holds its writer forever.
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Messages queued for one peer's writer. Sends are queued without waiting, so a peer whose
/// queue is full isn't keeping up with what we send it and is disconnected rather than
/// buffered for without bound.
pub const PEER_OUTBOX: usize = 1024;

/// Messages from all of a torrent's peers waiting for the swarm. Readers wait when it's full,
/// which pushes back on the peers through TCP rather than piling up here.
pub const SWARM_INBOX: usize = 4096;

/// Endgame: once every missing piece is in flight, a peer with room takes on the in-flight
/// piece furthest from done, walking its blocks in the opposite order from the peer already on
/// it. The slow peer keeps only the blocks it already asked for, and a block that arrives is
/// cancelled at the other racers. This many peers may hold the same piece at once.
pub const ENDGAME_RACERS: usize = 2;

/// For the last few pieces in flight, more racers each: the whole download waits on them, and
/// with two racers that are both slow the final seconds drag.
pub const ENDGAME_LAST_PIECES: usize = 8;
pub const ENDGAME_LAST_RACERS: usize = 4;

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

/// The fewest interested peers we keep unchoked at once based on reciprocation (the download
/// rate they've been giving us), not counting the optimistic slot; a bigger swarm gets more
/// (see `torrent_swarm::upload_slots`).
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

/// After a failed announce, how long before the next try; doubles per consecutive failure up
/// to `ANNOUNCE_RETRY_MAX`.
pub const ANNOUNCE_RETRY: Duration = Duration::from_secs(60);
pub const ANNOUNCE_RETRY_MAX: Duration = Duration::from_secs(30 * 60);

/// Bounds on the wait between regular tracker announces, whatever interval the tracker asks
/// for: zero or negative would hammer it, and a huge one would starve the swarm of peers.
pub const ANNOUNCE_INTERVAL_MIN: Duration = Duration::from_secs(60);
pub const ANNOUNCE_INTERVAL_MAX: Duration = Duration::from_secs(2 * 60 * 60);

/// Bound on a whole HTTP announce, from connecting to the last byte of the response.
pub const HTTP_TRACKER_TIMEOUT: Duration = Duration::from_secs(30);

/// An announce response is a few KiB of peers; past this the tracker is broken or hostile.
pub const TRACKER_RESPONSE_MAX: usize = 2 * 1024 * 1024;

/// BEP 15: a UDP tracker request is retransmitted if no answer comes within 15 * 2^n seconds,
/// n counting tries from 0. The spec goes on to n = 8, over an hour; after these three tries
/// (15 + 30 + 60 s) the announce counts as failed and `ANNOUNCE_RETRY` takes over.
pub const UDP_TRACKER_TIMEOUT: Duration = Duration::from_secs(15);
pub const UDP_TRACKER_ATTEMPTS: u32 = 3;

/// BEP 15: a connection ID may be used for a minute after it was received.
pub const UDP_CONNECTION_ID_TTL: Duration = Duration::from_secs(60);

/// How often to look a torrent up in the DHT and re-announce ourselves for it. Announced
/// peers expire from nodes after roughly 45 minutes, so this is comfortably inside that.
pub const DHT_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// First retry after a DHT lookup that found no peers or failed, doubling up to
/// `DHT_ANNOUNCE_INTERVAL`. Lookups right after the node comes up often converge on a sparse
/// corner of a half-built routing table and come back empty; waiting the full interval then
/// leaves a trackerless magnet with nothing to do for minutes.
pub const DHT_RETRY: Duration = Duration::from_secs(5);

/// BEP 11 (PEX): the spec recommends capping a single message at roughly 50 added peers.
pub const PEX_MAX_ADDED_PEERS: usize = 50;

/// BEP 19: Range requests in flight at once per web seed. A few keep a mirror busy across
/// each request's round trip without looking like a download accelerator to it.
pub const WEB_SEED_JOBS: usize = 4;

/// A web seed's jobs together fetch about this much of its measured throughput, so a fast
/// mirror gets long runs of pieces and a slow one doesn't sit on many.
pub const WEB_SEED_RUN_TARGET: Duration = Duration::from_secs(4);

/// Requests one web seed job keeps in flight when its run spans several files.
pub const WEB_SEED_PIPELINE: usize = 4;

/// The most one web seed request fetches; it's all held in memory until verified.
pub const WEB_SEED_MAX_RUN: usize = 16 * 1024 * 1024;

/// After a failed request, a web seed rests this long, doubling per consecutive failure up
/// to WEB_SEED_BACKOFF_MAX.
pub const WEB_SEED_BACKOFF: Duration = Duration::from_secs(2);
pub const WEB_SEED_BACKOFF_MAX: Duration = Duration::from_secs(10 * 60);

pub const WEB_SEED_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// longest a web seed may go without sending a byte mid-response
pub const WEB_SEED_READ_TIMEOUT: Duration = Duration::from_secs(30);
