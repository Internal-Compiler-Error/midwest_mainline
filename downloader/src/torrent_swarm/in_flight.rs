//! The pieces being downloaded: each one's buffer, which blocks are in and from whom, and how
//! far each claimant (a peer or a web seed) has got in requesting the rest.

use crate::settings::BLOCK_SIZE;
use crate::torrent::Torrent;
use crate::wire::{BlockRef, Piece};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::net::SocketAddr;

/// One claimant's share of an in-flight piece: where it is in requesting the blocks.
struct Claim {
    /// index of the next block to request
    cursor: usize,
    /// walk the piece from the end: the second peer racing for a piece goes the other way,
    /// so the two meet in the middle and the bytes fetched twice are roughly halved
    reverse: bool,
    /// blocks the claimant rejected, to ask for again before moving on
    retry: Vec<usize>,
    /// how many of this piece's requests it has rejected
    rejects: u8,
}

impl Claim {
    fn from(cursor: usize, reverse: bool) -> Self {
        Claim {
            cursor,
            reverse,
            retry: Vec::new(),
            rejects: 0,
        }
    }
}

/// Rejects of one piece's requests after which a claimant gives the piece up: the first few
/// are usually a full request queue, not a refusal.
const MAX_REJECTS: u8 = 3;

/// What becomes of a claim when its claimant rejects one of its requests.
#[derive(Debug, PartialEq)]
pub(super) enum Rejected {
    /// the block will be asked for again
    Retry,
    /// it rejected too often; the claim should go
    GiveUp,
}

/// Who "delivered" a block of padding (BEP 47): nobody, it's zeros from the start.
const PADDING: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0));

/// A piece we're in the middle of downloading. Normally one peer holds it; in endgame
/// (see `schedule`) several race for it, and each block records who delivered it so a
/// failed hash can still convict a lone sender.
pub(super) struct InFlight {
    pub(super) buf: Vec<u8>,
    /// where the piece ends as peers see it (`Torrent::wire_piece_len`); a request never runs
    /// past it
    wire_len: usize,
    /// per block, the claimant it arrived from
    received: Vec<Option<SocketAddr>>,
    /// the blocks of `received` still to come
    left: usize,
    claims: BTreeMap<SocketAddr, Claim>,
    /// from assignment to verification, for the traces
    pub(super) span: tracing::Span,
}

/// What became of a block handed to `InFlight::store`.
#[derive(Debug, PartialEq)]
pub(super) enum Stored {
    /// it's in; this many blocks are still to come
    Added { blocks_left: usize },
    /// someone else's copy came first
    Duplicate,
    /// it covers padding, which is in from the start (a web seed's run doesn't skip it)
    Padding,
    /// it doesn't fit a block of the piece
    Malformed,
}

impl InFlight {
    /// `piece`, claimed by `claimant` to request front to back; `shown_as` names the claimant in
    /// the traces. Blocks that are nothing but padding are in already: the buffer starts out
    /// zeroed, so there's nothing to request.
    pub(super) fn new(torrent: &Torrent, piece: u32, claimant: SocketAddr, shown_as: impl Display) -> Self {
        let size = torrent.nth_piece_size(piece).expect("piece index in range");
        let mut received = vec![None; size.div_ceil(BLOCK_SIZE)];
        for pad in torrent.padding_in_piece(piece) {
            let first = pad.start.div_ceil(BLOCK_SIZE);
            let end = if pad.end == size {
                received.len()
            } else {
                pad.end / BLOCK_SIZE
            };
            // a stretch of padding inside a single block has first > end, and covers nothing
            for slot in received.iter_mut().take(end).skip(first) {
                *slot = Some(PADDING);
            }
        }
        let left = received.iter().filter(|r| r.is_none()).count();
        Self {
            buf: vec![0u8; size],
            wire_len: torrent.wire_piece_len(piece).expect("piece index in range"),
            received,
            left,
            claims: BTreeMap::from([(claimant, Claim::from(0, false))]),
            span: tracing::info_span!(
                "piece",
                info_hash = %torrent.info_hash,
                piece,
                size,
                peer = %shown_as,
                racers = 1,
                outcome = tracing::field::Empty,
            ),
        }
    }

    pub(super) fn racers(&self) -> usize {
        self.claims.len()
    }

    pub(super) fn claimed_by(&self, claimant: SocketAddr) -> bool {
        self.claims.contains_key(&claimant)
    }

    pub(super) fn claimants(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.claims.keys().copied()
    }

    /// Torrent-relative bytes from the first block not in yet to the end of the last one.
    pub(super) fn missing_span(&self, piece_offset: u64) -> Option<(u64, u64)> {
        let first = self.received.iter().position(Option::is_none)?;
        let last = self.received.iter().rposition(Option::is_none)?;
        let end = ((last + 1) * BLOCK_SIZE).min(self.buf.len());
        Some((piece_offset + (first * BLOCK_SIZE) as u64, piece_offset + end as u64))
    }

    fn request(&self, piece: u32, block: usize) -> BlockRef {
        let begin = block * BLOCK_SIZE;
        BlockRef {
            index: piece,
            begin: begin as u32,
            length: self.wire_len.saturating_sub(begin).min(BLOCK_SIZE) as u32,
        }
    }

    /// The next block `claimant` should ask for: the first one past its cursor that hasn't
    /// arrived from anyone yet.
    fn next_request(&mut self, piece: u32, claimant: SocketAddr) -> Option<BlockRef> {
        let claim = self.claims.get_mut(&claimant)?;
        while let Some(block) = claim.retry.pop() {
            if self.received[block].is_none() {
                return Some(self.request(piece, block));
            }
        }
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

    /// Blocks `claimant` has still to request.
    pub(super) fn unrequested_blocks(&self, claimant: SocketAddr) -> usize {
        let Some(claim) = self.claims.get(&claimant) else {
            return 0;
        };
        let retries = claim.retry.iter().filter(|&&b| self.received[b].is_none()).count();
        if claim.cursor >= self.received.len() {
            return retries;
        }
        let ahead = if claim.reverse {
            &self.received[..=claim.cursor]
        } else {
            &self.received[claim.cursor..]
        };
        retries + ahead.iter().filter(|r| r.is_none()).count()
    }

    /// `claimant` rejected its request for the block at `begin`.
    fn rejected(&mut self, claimant: SocketAddr, begin: u32) -> Rejected {
        let Some(claim) = self.claims.get_mut(&claimant) else {
            return Rejected::GiveUp;
        };
        claim.rejects += 1;
        if claim.rejects >= MAX_REJECTS {
            return Rejected::GiveUp;
        }
        let block = begin as usize / BLOCK_SIZE;
        if block < self.received.len() && !claim.retry.contains(&block) {
            claim.retry.push(block);
        }
        Rejected::Retry
    }

    pub(super) fn blocks_left(&self) -> usize {
        self.left
    }

    /// Puts a block from `from` in its place. Its length is the sender's word, so it's checked
    /// before anything is copied.
    pub(super) fn store(&mut self, from: SocketAddr, block: &Piece) -> Stored {
        let begin = block.begin as usize;
        let end = begin + block.data.len();
        if end > self.buf.len() || !begin.is_multiple_of(BLOCK_SIZE) {
            return Stored::Malformed;
        }
        let slot = &mut self.received[begin / BLOCK_SIZE];
        match *slot {
            Some(PADDING) => return Stored::Padding,
            Some(_) => return Stored::Duplicate,
            None => *slot = Some(from),
        }
        self.left -= 1;
        self.buf[begin..end].copy_from_slice(&block.data);
        Stored::Added { blocks_left: self.left }
    }

    /// Everyone who delivered a block of it.
    pub(super) fn senders(&self) -> BTreeSet<SocketAddr> {
        self.received
            .iter()
            .flatten()
            .copied()
            .filter(|&a| a != PADDING)
            .collect()
    }
}

/// Every in-flight piece, and per claimant the pieces it has a claim on (the inverse of each
/// piece's claims, so a peer's pieces are found without scanning everything in flight, once
/// per block). Claims change only through here, which keeps the two in step.
#[derive(Default)]
pub(super) struct InFlightPieces {
    pieces: BTreeMap<u32, InFlight>,
    holdings: BTreeMap<SocketAddr, BTreeSet<u32>>,
}

impl InFlightPieces {
    pub(super) fn len(&self) -> usize {
        self.pieces.len()
    }

    pub(super) fn contains(&self, piece: u32) -> bool {
        self.pieces.contains_key(&piece)
    }

    pub(super) fn get(&self, piece: u32) -> Option<&InFlight> {
        self.pieces.get(&piece)
    }

    pub(super) fn get_mut(&mut self, piece: u32) -> Option<&mut InFlight> {
        self.pieces.get_mut(&piece)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (u32, &InFlight)> {
        self.pieces.iter().map(|(&piece, f)| (piece, f))
    }

    /// The bytes of every piece in flight.
    pub(super) fn bytes(&self) -> usize {
        self.pieces.values().map(|f| f.buf.len()).sum()
    }

    pub(super) fn start(&mut self, piece: u32, in_flight: InFlight) {
        for claimant in in_flight.claimants() {
            self.holdings.entry(claimant).or_default().insert(piece);
        }
        self.pieces.insert(piece, in_flight);
    }

    /// An endgame racer joins `piece`, walking it the other way from the last one to join.
    pub(super) fn add_racer(&mut self, piece: u32, racer: SocketAddr) {
        let f = self.pieces.get_mut(&piece).expect("racing a piece in flight");
        let reverse = f.claims.len() % 2 == 1;
        let cursor = if reverse { f.received.len() - 1 } else { 0 };
        f.claims.insert(racer, Claim::from(cursor, reverse));
        f.span.record("racers", f.claims.len());
        tracing::debug!(parent: &f.span, peer = %racer, "racer joined");
        self.holdings.entry(racer).or_default().insert(piece);
    }

    /// A claim on every block of `piece` not in yet, all requested at once: a web seed's.
    pub(super) fn claim_rest(&mut self, piece: u32, claimant: SocketAddr) {
        let f = self.pieces.get_mut(&piece).expect("claiming a piece in flight");
        let cursor = f.received.len();
        f.claims.insert(claimant, Claim::from(cursor, false));
        f.span.record("racers", f.claims.len());
        self.holdings.entry(claimant).or_default().insert(piece);
    }

    pub(super) fn holds(&self, claimant: SocketAddr, piece: u32) -> bool {
        self.holdings.get(&claimant).is_some_and(|held| held.contains(&piece))
    }

    pub(super) fn held_by(&self, claimant: SocketAddr) -> Vec<u32> {
        self.holdings
            .get(&claimant)
            .map(|held| held.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Blocks `claimant` has still to request, across its pieces.
    pub(super) fn backlog(&self, claimant: SocketAddr) -> usize {
        self.holdings
            .get(&claimant)
            .into_iter()
            .flatten()
            .map(|piece| self.pieces[piece].unrequested_blocks(claimant))
            .sum()
    }

    /// The next block `claimant` should ask for, from the first of its pieces that has one.
    pub(super) fn next_request(&mut self, claimant: SocketAddr) -> Option<BlockRef> {
        let held = self.holdings.get(&claimant)?;
        held.iter()
            .find_map(|&piece| self.pieces.get_mut(&piece)?.next_request(piece, claimant))
    }

    /// `claimant` rejected `request`: the block is asked for again later, unless it has
    /// rejected this piece too often, when the claim should be released.
    pub(super) fn rejected(&mut self, claimant: SocketAddr, request: BlockRef) -> Rejected {
        match self.pieces.get_mut(&request.index) {
            Some(f) => f.rejected(claimant, request.begin),
            None => Rejected::GiveUp,
        }
    }

    /// Takes `claimant` off `piece`. A piece left with no claimant isn't in flight any more,
    /// and is returned.
    pub(super) fn release(&mut self, piece: u32, claimant: SocketAddr) -> Option<InFlight> {
        if let Some(held) = self.holdings.get_mut(&claimant) {
            held.remove(&piece);
            if held.is_empty() {
                self.holdings.remove(&claimant);
            }
        }
        let f = self.pieces.get_mut(&piece)?;
        f.claims.remove(&claimant);
        tracing::debug!(parent: &f.span, peer = %claimant, "claim released");
        if !f.claims.is_empty() {
            return None;
        }
        f.span.record("outcome", "released");
        self.pieces.remove(&piece)
    }

    /// `piece` is complete: it leaves, and so do its claims.
    pub(super) fn finish(&mut self, piece: u32) -> Option<InFlight> {
        let f = self.pieces.remove(&piece)?;
        for claimant in f.claimants() {
            if let Some(held) = self.holdings.get_mut(&claimant) {
                held.remove(&piece);
                if held.is_empty() {
                    self.holdings.remove(&claimant);
                }
            }
        }
        Some(f)
    }

    /// Everything in flight, which is in flight no more.
    pub(super) fn take_all(&mut self) -> BTreeMap<u32, InFlight> {
        self.holdings.clear();
        std::mem::take(&mut self.pieces)
    }
}

#[cfg(test)]
mod test {
    use super::super::test_support::{PIECE, single_file_torrent};
    use super::*;

    fn addr(n: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, n], 6881))
    }

    fn block(piece: u32, begin: usize, len: usize) -> Piece {
        Piece {
            index: piece,
            begin: begin as u32,
            data: vec![7; len].into(),
        }
    }

    #[test]
    fn racers_walk_a_piece_from_opposite_ends_and_skip_what_is_in() {
        let torrent = single_file_torrent();
        let mut pieces = InFlightPieces::default();
        pieces.start(0, InFlight::new(&torrent, 0, addr(1), addr(1)));
        pieces.add_racer(0, addr(2));
        let begins = |pieces: &mut InFlightPieces, who| {
            std::iter::from_fn(|| pieces.next_request(who))
                .map(|r| r.begin as usize)
                .collect::<Vec<_>>()
        };
        assert_eq!(pieces.get(0).unwrap().unrequested_blocks(addr(2)), 3);
        assert_eq!(begins(&mut pieces, addr(2)), [2 * BLOCK_SIZE, BLOCK_SIZE, 0]);

        let f = pieces.get_mut(0).unwrap();
        assert_eq!(
            f.store(addr(2), &block(0, 2 * BLOCK_SIZE, PIECE - 2 * BLOCK_SIZE)),
            Stored::Added { blocks_left: 2 }
        );
        assert_eq!(
            begins(&mut pieces, addr(1)),
            [0, BLOCK_SIZE],
            "the block already in isn't asked for"
        );
        assert_eq!(pieces.backlog(addr(1)), 0);
    }

    #[test]
    fn a_block_is_stored_once_and_only_where_it_fits() {
        let torrent = single_file_torrent();
        let mut f = InFlight::new(&torrent, 0, addr(1), "test");
        assert_eq!(
            f.store(addr(1), &block(0, 1, BLOCK_SIZE)),
            Stored::Malformed,
            "not on a block boundary"
        );
        assert_eq!(
            f.store(addr(1), &block(0, 2 * BLOCK_SIZE, BLOCK_SIZE)),
            Stored::Malformed,
            "past the end"
        );
        assert_eq!(
            f.store(addr(1), &block(0, 0, BLOCK_SIZE)),
            Stored::Added { blocks_left: 2 }
        );
        assert_eq!(f.store(addr(2), &block(0, 0, BLOCK_SIZE)), Stored::Duplicate);
        assert_eq!(f.senders(), BTreeSet::from([addr(1)]));
    }

    #[test]
    fn claims_and_holdings_stay_in_step() {
        let torrent = single_file_torrent();
        let mut pieces = InFlightPieces::default();
        pieces.start(0, InFlight::new(&torrent, 0, addr(1), addr(1)));
        pieces.start(1, InFlight::new(&torrent, 1, addr(1), addr(1)));
        pieces.add_racer(1, addr(2));
        assert_eq!(pieces.held_by(addr(1)), [0, 1]);
        assert!(pieces.holds(addr(2), 1));

        assert!(pieces.release(1, addr(1)).is_none(), "addr(2) still races for it");
        assert_eq!(pieces.held_by(addr(1)), [0]);
        assert!(
            pieces.release(1, addr(2)).is_some(),
            "the last claimant leaving abandons it"
        );
        assert!(!pieces.contains(1));
        assert!(pieces.held_by(addr(2)).is_empty());

        assert!(pieces.finish(0).is_some());
        assert!(pieces.held_by(addr(1)).is_empty());
        assert_eq!(pieces.len(), 0);
    }

    /// A v2-only piece that ends with its file: the block across the file's end is asked for
    /// only up to it, as a peer would refuse more.
    #[test]
    fn a_request_stops_where_a_v2_file_ends() {
        let files: &[(&[&str], Vec<u8>)] = &[(&["a"], vec![1; 40_000]), (&["b"], vec![2; 10])];
        let torrent =
            crate::parse_torrent(&crate::torrent::fixtures::torrent_file("wire", files, 32_768, false)).unwrap();
        let mut pieces = InFlightPieces::default();
        pieces.start(1, InFlight::new(&torrent, 1, addr(1), "a"));
        let first = pieces.next_request(addr(1)).unwrap();
        assert_eq!((first.begin, first.length), (0, 40_000 - 32_768));
        assert_eq!(pieces.next_request(addr(1)), None, "the rest is padding");
    }
}
