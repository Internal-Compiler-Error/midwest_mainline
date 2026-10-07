//! A parsed .torrent: its files laid end to end as one piece stream, the v1 hashes of the
//! pieces, and for v2 and hybrid torrents (BEP 52) each file's Merkle root and piece layer.

#[cfg(test)]
pub(crate) mod fixtures;
mod parse;
#[cfg(test)]
mod tests;

pub(crate) use parse::web_seed_urls;
pub use parse::{parse_torrent, swarm_info_hash};

use crate::merkle::{self, Hash};
use crate::wire::V2Support;
use bitvec::prelude::*;
use midwest_mainline::types::InfoHash;
use sha1::{Digest, Sha1};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

/// BEP 47 attributes of one file.
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct FileAttr {
    /// zeros that pad the file before it out to a piece boundary: part of the piece stream,
    /// never on disk, never requested
    pub pad: bool,
    pub executable: bool,
    pub hidden: bool,
    /// the file is a symlink to this path, relative to the torrent's top level
    pub symlink: Option<PathBuf>,
}

impl FileAttr {
    /// Nothing of it is on disk as a regular file: padding, or a symlink.
    pub fn virtual_file(&self) -> bool {
        self.pad || self.symlink.is_some()
    }
}

/// BEP 52: what a v2 (or hybrid) torrent adds.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct V2 {
    /// SHA-256 of the info dict; its first 20 bytes stand for it where only 20 fit
    pub info_hash: Hash,
    /// per entry of `Torrent::files`: its `pieces root`, `None` for padding and empty files
    pub roots: Vec<Option<Hash>>,
    /// per entry of `Torrent::files`: its piece layer, once known -- from the .torrent's
    /// `piece layers`, or from peers (BEP 52 hash requests) for a magnet. Only files of more
    /// than one piece have one; a smaller file's root is its one piece's hash. Clones share
    /// it, so a layer that arrives in the swarm's copy is in the session's for the resume file.
    layers: Arc<[OnceLock<Box<[Hash]>>]>,
    /// set once a hybrid's piece passed SHA-1 but not its v2 hash: the halves disagree, and
    /// as the v1 info hash covers the whole info dict, SHA-1 alone decides from then on.
    /// Shared by clones, like `layers`
    inconsistent: Arc<OnceLock<()>>,
}

/// One file of the piece stream.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TorrentFile {
    pub len: u64,
    /// relative to the download root, starting with the torrent's top level (see
    /// `Torrent::top_level`)
    pub path: PathBuf,
    /// the path exactly as the info dict spells it, `name` first, before it was made safe to
    /// put on disk: what a web seed's URLs are built from
    pub raw_path: Vec<String>,
    /// BEP 47
    pub attr: FileAttr,
    /// where it starts in the piece stream
    offset: u64,
}

/// A parsed .torrent.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Torrent {
    /// List of tracker tiers, where each tier contains multiple tracker URLs
    /// Trackers in the same tier should be tried in parallel
    pub announce_tiers: Vec<Vec<String>>,

    /// Number of bytes for each piece, barring the last one
    pub piece_size: u32,

    /// v1 SHA-1 hash of each piece; empty for a v2-only torrent, whose pieces are checked
    /// against `v2`'s Merkle trees
    pub pieces: Vec<[u8; 20]>,

    /// The files, which laid end to end make the piece stream, padding files included. A
    /// v2-only torrent gets padding synthesised after each file that doesn't end on a piece
    /// boundary, which is exactly BEP 52's piece layout.
    pub files: Vec<TorrentFile>,

    /// Total size of all files combined in bytes, padding included
    pub total_size: u64,

    /// Size of the last piece in bytes
    pub last_piece_size: u32,

    /// The info dict's `name`: the single file's name, or the directory everything lives under
    pub name: String,

    /// The 20-byte info hash the swarm goes by (handshakes, trackers, the DHT): the SHA-1 one
    /// for v1 and hybrid torrents, the truncated SHA-256 one for v2-only torrents
    pub info_hash: InfoHash,

    /// BEP 52 metadata, for v2 and hybrid torrents
    pub v2: Option<V2>,

    /// The bencoded "info" dict exactly as the .torrent has it, which the info hashes are
    /// taken over and BEP 9 (ut_metadata) serves to peers
    pub raw_info: Vec<u8>,

    /// BEP 27: if set, peers for this torrent must only come from the trackers named in this
    /// torrent -- no DHT, PEX or LSD.
    pub private: bool,

    /// BEP 19 web seeds: HTTP(S) URLs serving the torrent's files, from `url-list` (or a
    /// magnet's `ws=`)
    pub web_seeds: Vec<String>,

    num_pieces: usize,
}

impl Torrent {
    /// The single entry a download root gains from this torrent: the file itself for a
    /// single-file torrent, the directory holding everything for a multi-file one. Deleting
    /// `root.join(self.top_level())` removes all of the torrent's data.
    pub fn top_level(&self) -> PathBuf {
        self.files
            .first()
            .and_then(|file| file.path.components().next())
            .map(|component| PathBuf::from(component.as_os_str()))
            .unwrap_or_default()
    }

    pub fn num_pieces(&self) -> usize {
        self.num_pieces
    }

    /// Where file `index` starts in the piece stream.
    pub fn file_offset(&self, index: usize) -> u64 {
        self.files[index].offset
    }

    /// The file holding stream byte `offset` (never an empty one).
    pub fn file_at(&self, offset: u64) -> usize {
        self.files.partition_point(|f| f.offset <= offset).saturating_sub(1)
    }

    /// The v2-only torrent this is: no SHA-1 piece hashes, pieces checked by Merkle trees.
    pub fn v2_only(&self) -> bool {
        self.pieces.is_empty()
    }

    /// The truncated v2 info hash of a hybrid, the second swarm it can be found in.
    pub fn hybrid_v2_hash(&self) -> Option<InfoHash> {
        let v2 = self.v2.as_ref().filter(|_| !self.v2_only())?;
        Some(InfoHash::from_bytes(&v2.info_hash[..20]))
    }

    /// What our handshakes for this torrent say about BEP 52.
    pub(crate) fn v2_support(&self) -> V2Support {
        match (&self.v2, self.hybrid_v2_hash()) {
            (_, Some(v2)) => V2Support::Hybrid(v2),
            (Some(_), None) => V2Support::Only,
            (None, _) => V2Support::None,
        }
    }

    /// Validates that a piece matches its expected hashes: a hybrid's must match both its SHA-1
    /// hash and, once its file's piece layer is known, its Merkle hash. A v2-only piece whose
    /// file's piece layer isn't known yet can't be checked, and fails.
    pub fn valid_piece(&self, piece: u32, data: &[u8]) -> bool {
        let v2_valid = |(expected, len, leaves): (Hash, usize, usize)| {
            data.len() >= len && merkle::data_root(&data[..len], leaves) == expected
        };
        if self.v2_only() {
            return self.v2_piece_hash(piece).is_some_and(v2_valid);
        }
        if *Sha1::digest(data) != self.pieces[piece as usize] {
            return false;
        }
        // data can't be forged to pass SHA-1, so a v2 mismatch here is the torrent's fault,
        // not the peer's: no peer could ever send a piece that passes both
        if self.v2_consistent()
            && let Some(expected) = self.v2_piece_hash(piece)
            && !v2_valid(expected)
            && let Some(v2) = &self.v2
            && v2.inconsistent.set(()).is_ok()
        {
            tracing::warn!(
                "{}: piece {piece} matches its SHA-1 hash but not its v2 hash; the hybrid's halves disagree, so SHA-1 alone checks it from now on",
                self.name
            );
        }
        true
    }

    /// False once a hybrid turned out to describe different data in its two halves; its v2
    /// hashes are then neither checked nor handed to peers.
    pub fn v2_consistent(&self) -> bool {
        self.v2.as_ref().is_some_and(|v2| v2.inconsistent.get().is_none())
    }

    /// Whether `valid_piece` can tell for `piece`: always, but for a v2-only torrent from a
    /// magnet whose piece layer for that file hasn't come from a peer yet.
    pub fn can_verify(&self, piece: u32) -> bool {
        !self.v2_only() || self.v2_piece_hash(piece).is_some()
    }

    /// For a v2-only piece: the hash it must have, how many of its bytes are file data (the
    /// rest is padding), and how many leaves its tree is wide.
    fn v2_piece_hash(&self, piece: u32) -> Option<(Hash, usize, usize)> {
        let v2 = self.v2.as_ref()?;
        let start = piece as u64 * self.piece_size as u64;
        let file = self.file_at(start);
        let root = v2.roots[file]?;
        let file_end = self.files[file].offset + self.files[file].len;
        let len = (file_end - start).min(self.piece_size as u64) as usize;
        let pieces = self.pieces_of_file(file);
        if pieces.len() == 1 {
            return Some((root, len, merkle::file_leaves(self.files[file].len)));
        }
        let layer = v2.layers[file].get()?;
        Some((
            layer[(piece - pieces.start) as usize],
            len,
            self.piece_size as usize / merkle::BLOCK,
        ))
    }

    /// The files with a `pieces root` but no piece layer yet, which peers must supply.
    pub fn missing_layers(&self) -> Vec<usize> {
        let Some(v2) = &self.v2 else {
            return vec![];
        };
        (0..self.files.len())
            .filter(|&f| v2.roots[f].is_some() && self.pieces_of_file(f).len() > 1 && v2.layers[f].get().is_none())
            .collect()
    }

    /// The piece layer of `file`, if it has one and it's known.
    pub fn layer(&self, file: usize) -> Option<&[Hash]> {
        self.v2.as_ref()?.layers[file].get().map(|l| &**l)
    }

    /// Takes a piece layer for `file` from a peer; false if it doesn't roll up to the file's
    /// root (or the file needs none).
    pub fn set_layer(&self, file: usize, layer: Vec<Hash>) -> bool {
        let Some(v2) = &self.v2 else {
            return false;
        };
        let Some(root) = v2.roots[file] else {
            return false;
        };
        if layer.len() != self.pieces_of_file(file).len() || merkle::root_from_layer(&layer, self.piece_size) != root {
            return false;
        }
        let _ = v2.layers[file].set(layer.into_boxed_slice());
        true
    }

    /// The file whose `pieces root` is `root`.
    pub fn file_with_root(&self, root: &Hash) -> Option<usize> {
        self.v2.as_ref()?.roots.iter().position(|r| r.as_ref() == Some(root))
    }

    /// The known piece layers as a bencoded `piece layers` dict, for writing back next to the
    /// info dict (the info hash doesn't cover them, so they'd otherwise be lost).
    pub fn piece_layers_bencoded(&self) -> Option<Vec<u8>> {
        let v2 = self.v2.as_ref()?;
        let layers: BTreeMap<&[u8], &[Hash]> = (0..self.files.len())
            .filter_map(|f| Some((v2.roots[f].as_ref()?.as_slice(), &**v2.layers[f].get()?)))
            .collect();
        if layers.is_empty() {
            return None;
        }
        let mut out = vec![b'd'];
        for (root, layer) in layers {
            out.extend_from_slice(b"32:");
            out.extend_from_slice(root);
            out.extend_from_slice(format!("{}:", layer.len() * 32).as_bytes());
            out.extend_from_slice(layer.as_flattened());
        }
        out.push(b'e');
        Some(out)
    }

    /// The pieces holding any byte of file `index`; empty for an empty file.
    pub fn pieces_of_file(&self, index: usize) -> std::ops::Range<u32> {
        let start = self.files[index].offset;
        let end = start + self.files[index].len;
        if end == start {
            return 0..0;
        }
        let piece = self.piece_size as u64;
        (start / piece) as u32..end.div_ceil(piece) as u32
    }

    /// One bit per piece: set if the piece holds any byte of a selected file. A piece
    /// shared by a selected and an unselected file is wanted; padding wants nothing.
    pub fn wanted_pieces(&self, selected: &[bool]) -> BitBox<u8, Msb0> {
        let mut wanted = bitvec![u8, Msb0; 0; self.num_pieces];
        for (index, file) in self.files.iter().enumerate() {
            if !file.attr.pad && selected.get(index).copied().unwrap_or(true) {
                for piece in self.pieces_of_file(index) {
                    wanted.set(piece as usize, true);
                }
            }
        }
        wanted.into_boxed_bitslice()
    }

    /// The parts of `piece` that are padding, relative to the piece's start: zeros nobody
    /// needs to send.
    pub fn padding_in_piece(&self, piece: u32) -> Vec<std::ops::Range<usize>> {
        let Some(size) = self.nth_piece_size(piece) else {
            return vec![];
        };
        let start = piece as u64 * self.piece_size as u64;
        self.file_segments(start..start + size as u64)
            .filter(|(file, _)| self.files[*file].attr.pad)
            .map(|(file, within)| {
                let at = (self.files[file].offset + within.start - start) as usize;
                at..at + (within.end - within.start) as usize
            })
            .collect()
    }

    /// The pieces of the files that stream bytes `range` covers, in stream order: each one's
    /// file and the byte range within that file. Empty files are skipped, so the segments of a
    /// range inside the stream add up to it exactly.
    pub fn file_segments(
        &self,
        range: std::ops::Range<u64>,
    ) -> impl Iterator<Item = (usize, std::ops::Range<u64>)> + '_ {
        let std::ops::Range { start, end } = range;
        (self.file_at(start)..self.files.len())
            .map(|file| {
                (
                    file,
                    self.files[file].offset,
                    self.files[file].offset + self.files[file].len,
                )
            })
            .take_while(move |&(_, from, _)| from < end)
            .filter_map(move |(file, from, to)| {
                let (a, b) = (start.max(from), end.min(to));
                (a < b).then(|| (file, a - from..b - from))
            })
    }

    /// Returns the size of the ith piece in bytes
    pub fn nth_piece_size<T: Into<u64>>(&self, i: T) -> Option<usize> {
        let i = i.into();
        if i >= self.num_pieces as u64 {
            return None;
        }
        if i == self.num_pieces as u64 - 1 {
            Some(self.last_piece_size as usize)
        } else {
            Some(self.piece_size as usize)
        }
    }

    /// Returns all tracker URLs flattened from all tiers
    pub fn all_trackers(&self) -> Vec<String> {
        self.announce_tiers.iter().flatten().cloned().collect()
    }

    /// Returns the primary tracker URL (first tracker in first tier)
    pub fn primary_tracker(&self) -> Option<&str> {
        self.announce_tiers
            .first()
            .and_then(|tier| tier.first())
            .map(|s| s.as_str())
    }

    /// Total size of the bencoded "info" dict, i.e. the total metadata size BEP 9 peers need
    /// to know to request all of it.
    pub fn metadata_size(&self) -> u32 {
        self.raw_info.len() as u32
    }
}
