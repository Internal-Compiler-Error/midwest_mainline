use std::time::Duration;

pub const BLOCK_SIZE: usize = 16 * 1024; // 16 KiB

/// How long to wait for a peer that accepted a block request to actually send the block.
/// A peer that goes silent mid-request (as opposed to disconnecting outright) is the common
/// failure mode this guards against -- without it, a single unresponsive peer hangs a piece
/// (and the block-request future it's part of) forever.
pub const BLOCK_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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