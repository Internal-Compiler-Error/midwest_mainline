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