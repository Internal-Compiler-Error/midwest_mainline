//! A piece being downloaded: its buffer, which blocks are in and from whom, and how far each
//! claimant has got in requesting the rest.

use crate::settings::BLOCK_SIZE;
use crate::torrent::Torrent;
use crate::wire::Request;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

/// One peer's share of an in-flight piece: where it is in requesting the blocks.
pub(super) struct Claim {
    /// index of the next block to request
    pub(super) cursor: usize,
    /// walk the piece from the end: the second peer racing for a piece goes the other way,
    /// so the two meet in the middle and the bytes fetched twice are roughly halved
    pub(super) reverse: bool,
}

/// Who "delivered" a block of padding (BEP 47): nobody, it's zeros from the start.
pub(super) const PADDING: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0));

/// A piece we're in the middle of downloading. Normally one peer holds it; in endgame
/// (see `schedule`) several race for it, and each block records who delivered it so a
/// failed hash can still convict a lone sender.
pub(super) struct InFlight {
    pub(super) buf: Vec<u8>,
    /// per block, the peer it arrived from
    pub(super) received: Vec<Option<SocketAddr>>,
    pub(super) claims: BTreeMap<SocketAddr, Claim>,
    /// from assignment to verification, for the traces
    pub(super) span: tracing::Span,
}

impl InFlight {
    pub(super) fn new(size: usize, peer: SocketAddr) -> Self {
        let blocks = size.div_ceil(BLOCK_SIZE);
        let mut claims = BTreeMap::new();
        claims.insert(
            peer,
            Claim {
                cursor: 0,
                reverse: false,
            },
        );
        Self {
            buf: vec![0u8; size],
            received: vec![None; blocks],
            claims,
            span: tracing::Span::none(),
        }
    }

    /// Marks the blocks that are nothing but padding as in already: the buffer starts out
    /// zeroed, so there's nothing to request.
    pub(super) fn skip_padding(&mut self, torrent: &Torrent, piece: u32) {
        for pad in torrent.padding_in_piece(piece) {
            let first = pad.start.div_ceil(BLOCK_SIZE);
            let end = if pad.end == self.buf.len() {
                self.received.len()
            } else {
                pad.end / BLOCK_SIZE
            };
            for block in first..end {
                self.received[block] = Some(PADDING);
            }
        }
    }

    /// A claim on every block not in yet, all requested at once: a web seed's.
    pub(super) fn claim_rest(&mut self, source: SocketAddr) {
        let cursor = self.received.len();
        self.claims.insert(source, Claim { cursor, reverse: false });
        self.span.record("racers", self.claims.len());
    }

    /// Torrent-relative bytes from the first block not in yet to the end of the last one.
    pub(super) fn missing_span(&self, piece_offset: u64) -> Option<(u64, u64)> {
        let first = self.received.iter().position(Option::is_none)?;
        let last = self.received.iter().rposition(Option::is_none)?;
        let end = ((last + 1) * BLOCK_SIZE).min(self.buf.len());
        Some((piece_offset + (first * BLOCK_SIZE) as u64, piece_offset + end as u64))
    }

    pub(super) fn add_racer(&mut self, peer: SocketAddr) {
        let reverse = self.claims.len() % 2 == 1;
        let cursor = if reverse { self.received.len() - 1 } else { 0 };
        self.claims.insert(peer, Claim { cursor, reverse });
        self.span.record("racers", self.claims.len());
        tracing::debug!(parent: &self.span, %peer, "racer joined");
    }

    pub(super) fn request(&self, piece: u32, block: usize) -> Request {
        let begin = block * BLOCK_SIZE;
        Request {
            index: piece,
            begin: begin as u32,
            length: (self.buf.len() - begin).min(BLOCK_SIZE) as u32,
        }
    }

    /// The next block `peer` should ask for: the first one past its cursor that hasn't
    /// arrived from anyone yet.
    pub(super) fn next_request(&mut self, piece: u32, peer: SocketAddr) -> Option<Request> {
        let claim = self.claims.get_mut(&peer)?;
        loop {
            let block = claim.cursor;
            if block >= self.received.len() {
                return None;
            }
            claim.cursor = if claim.reverse {
                block.wrapping_sub(1)
            } else {
                block + 1
            };
            if self.received[block].is_none() {
                return Some(self.request(piece, block));
            }
        }
    }

    /// Blocks `peer` has still to request
    pub(super) fn unrequested_blocks(&self, peer: SocketAddr) -> usize {
        let Some(claim) = self.claims.get(&peer) else {
            return 0;
        };
        if claim.cursor >= self.received.len() {
            return 0;
        }
        let ahead = if claim.reverse {
            &self.received[..=claim.cursor]
        } else {
            &self.received[claim.cursor..]
        };
        ahead.iter().filter(|r| r.is_none()).count()
    }

    pub(super) fn blocks_left(&self) -> usize {
        self.received.iter().filter(|r| r.is_none()).count()
    }

    pub(super) fn senders(&self) -> BTreeSet<SocketAddr> {
        self.received
            .iter()
            .flatten()
            .copied()
            .filter(|&a| a != PADDING)
            .collect()
    }
}
