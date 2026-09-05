use std::time::Duration;

pub const BLOCK_SIZE: usize = 16 * 1024; // 16 KiB

/// How long to wait for a peer that accepted a block request to actually send the block.
/// A peer that goes silent mid-request (as opposed to disconnecting outright) is the common
/// failure mode this guards against -- without it, a single unresponsive peer hangs a piece
/// (and the block-request future it's part of) forever.
pub const BLOCK_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);