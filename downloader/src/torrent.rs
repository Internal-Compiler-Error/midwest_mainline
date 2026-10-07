use crate::merkle::{self, Hash};
use crate::wire::V2Support;
use anyhow::{anyhow, bail};
use bendy::decoding::Object;
use bitvec::prelude::*;
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::str;
use std::sync::{Arc, OnceLock};

/// Turns one "name"/"path" segment from a .torrent's info dict into a single, safe path
/// component. Those fields are attacker-controlled (they come from whoever made the torrent,
/// not from us) and get pushed onto a `PathBuf` that's later used to create real files on disk
/// -- without checking, a segment like ".." or an absolute path would let a torrent write
/// outside the directory it's meant to download into.
///
/// A literal `/` or `\` is substituted rather than rejected, not stripped: real-world torrent
/// names sometimes contain one for cosmetic reasons (e.g. an artist named "AC/DC"), and BEP 3
/// gives each "path" list entry as a separate segment specifically so a real directory
/// separator never needs to appear inside one -- so a `/` inside a single segment is always
/// either a harmless display quirk or someone trying to smuggle extra path segments into what's
/// supposed to be one, and rejecting a real torrent for the former isn't worth it just to catch
/// the latter, which substitution already defeats just as well as an error would.
fn safe_path_component(raw: &str) -> anyhow::Result<PathBuf> {
    let sanitized = raw.replace(['/', '\\'], "_");
    let path = Path::new(&sanitized);
    let mut components = path.components();
    let Some(Component::Normal(_)) = components.next() else {
        bail!("path component {raw:?} is not a plain name");
    };
    if components.next().is_some() {
        bail!("path component {raw:?} contains multiple segments");
    }
    Ok(path.to_path_buf())
}

/// Largest piece length we accept. BEP 3 sets no limit, but real torrents stay at or under
/// 16 MiB, and a whole piece is buffered in memory while it downloads.
const MAX_PIECE_SIZE: u32 = 64 << 20;

/// How deep a v2 `file tree` may nest; real ones are a handful of levels.
const MAX_TREE_DEPTH: usize = 64;

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
    /// From a v1 `files` entry or a v2 file's properties.
    fn parse(dict: &BTreeMap<&[u8], BencodeItemView>) -> anyhow::Result<Self> {
        let flags = match dict.get(b"attr".as_slice()) {
            Some(BencodeItemView::ByteString(flags)) => *flags,
            _ => b"",
        };
        let mut attr = FileAttr {
            pad: flags.contains(&b'p'),
            executable: flags.contains(&b'x'),
            hidden: flags.contains(&b'h'),
            symlink: None,
        };
        if flags.contains(&b'l')
            && let Some(BencodeItemView::List(target)) = dict.get(b"symlink path".as_slice())
        {
            let mut path = PathBuf::new();
            for segment in target {
                let BencodeItemView::ByteString(segment) = segment else {
                    bail!("symlink path segment needs to be a string");
                };
                path.push(safe_path_component(str::from_utf8(segment)?)?);
            }
            if path.as_os_str().is_empty() {
                bail!("symlink path is empty");
            }
            attr.symlink = Some(path);
        }
        Ok(attr)
    }

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

/// Represents a parsed torrent metadata file
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

    /// Files in the torrent: (size in bytes, path relative to the download root), always
    /// starting with `name` -- the single file's name, or the directory holding them all.
    /// Laid end to end they make the piece stream; padding files (see `attrs`) are part of
    /// it. A v2-only torrent gets padding synthesised after each file that doesn't end on a
    /// piece boundary, which is exactly BEP 52's piece layout.
    pub files: Vec<(u64, PathBuf)>,

    /// BEP 47 attributes, one per entry of `files`
    pub attrs: Vec<FileAttr>,

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

    /// The raw bencoded bytes of the "info" dict, exactly as they appeared in the .torrent
    /// file. Kept around so we can serve it to peers over BEP 9 (ut_metadata) -- we always
    /// have the full metadata already, having started from a .torrent file rather than a
    /// magnet link.
    pub raw_info: Vec<u8>,

    /// BEP 27: if set, peers for this torrent must only come from the trackers named in this
    /// torrent -- no DHT, no PEX. We don't implement DHT, but we do implement PEX, so this has
    /// to actually gate something.
    pub private: bool,

    /// BEP 19 web seeds: HTTP(S) URLs serving the torrent's files, from `url-list` (or a
    /// magnet's `ws=`)
    pub web_seeds: Vec<String>,

    /// Each file's path exactly as the info dict spells it, `name` first, before
    /// `safe_path_component` touched it: what a web seed's URLs are built from
    pub raw_paths: Vec<Vec<String>>,

    /// where each of `files` starts in the piece stream
    offsets: Vec<u64>,
    num_pieces: usize,
}

impl Torrent {
    /// The single entry a download root gains from this torrent: the file itself for a
    /// single-file torrent, the directory holding everything for a multi-file one. Deleting
    /// `root.join(self.top_level())` removes all of the torrent's data.
    pub fn top_level(&self) -> PathBuf {
        self.files
            .first()
            .and_then(|(_, path)| path.components().next())
            .map(|component| PathBuf::from(component.as_os_str()))
            .unwrap_or_default()
    }

    pub fn num_pieces(&self) -> usize {
        self.num_pieces
    }

    /// Where file `index` starts in the piece stream.
    pub fn file_offset(&self, index: usize) -> u64 {
        self.offsets[index]
    }

    /// The file holding stream byte `offset` (never an empty one).
    pub fn file_at(&self, offset: u64) -> usize {
        self.offsets.partition_point(|&o| o <= offset).saturating_sub(1)
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
        let file_end = self.offsets[file] + self.files[file].0;
        let len = (file_end - start).min(self.piece_size as u64) as usize;
        let pieces = self.pieces_of_file(file);
        if pieces.len() == 1 {
            return Some((root, len, merkle::file_leaves(self.files[file].0)));
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
        let start = self.offsets[index];
        let end = start + self.files[index].0;
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
        for (index, attr) in self.attrs.iter().enumerate() {
            if !attr.pad && selected.get(index).copied().unwrap_or(true) {
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
            .filter(|(file, _)| self.attrs[*file].pad)
            .map(|(file, within)| {
                let at = (self.offsets[file] + within.start - start) as usize;
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
            .map(|file| (file, self.offsets[file], self.offsets[file] + self.files[file].0))
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

/// The 20-byte hash a swarm knows the info dict `raw_info` by: SHA-1 for v1 and hybrid
/// torrents, truncated SHA-256 for v2-only ones (`meta version` 2 and no `pieces`).
pub fn swarm_info_hash(raw_info: &[u8]) -> InfoHash {
    let v2_only = juicy_bencode::parse_bencode_dict(raw_info).is_ok_and(|(_, info)| {
        matches!(info.get(b"meta version".as_slice()), Some(BencodeItemView::Integer(2)))
            && !info.contains_key(b"pieces".as_slice())
    });
    if v2_only {
        InfoHash::from_bytes(&Sha256::digest(raw_info)[..20])
    } else {
        InfoHash::from_bytes(Sha1::digest(raw_info).as_slice())
    }
}

/// One file of a v2 `file tree`.
struct TreeFile {
    len: u64,
    /// below the torrent's top level
    path: Vec<String>,
    root: Option<Hash>,
    attr: FileAttr,
}

/// Walks a `file tree` dict depth first, in key order (which is the piece order).
fn walk_file_tree(
    dir: &BTreeMap<&[u8], BencodeItemView>,
    path: &mut Vec<String>,
    out: &mut Vec<TreeFile>,
) -> anyhow::Result<()> {
    if path.len() >= MAX_TREE_DEPTH {
        bail!("file tree is nested too deep");
    }
    for (key, node) in dir {
        let BencodeItemView::Dictionary(node) = node else {
            bail!("file tree entry needs to be a dict");
        };
        if key.is_empty() {
            bail!("a directory of the file tree is also a file");
        }
        let name = str::from_utf8(key)?;
        safe_path_component(name)?;
        path.push(name.to_string());
        if let Some(file) = node.get(b"".as_slice()) {
            let BencodeItemView::Dictionary(props) = file else {
                bail!("file properties need to be a dict");
            };
            if node.len() != 1 {
                bail!("file {name:?} is also a directory");
            }
            let attr = FileAttr::parse(props)?;
            let len = match props.get(b"length".as_slice()) {
                Some(BencodeItemView::Integer(len)) => {
                    u64::try_from(*len).map_err(|_| anyhow!("file length {len} is negative"))?
                }
                // BEP 47: a symlink's length may be left out
                None if attr.symlink.is_some() => 0,
                _ => bail!("file length needs to be an integer"),
            };
            let root = match props.get(b"pieces root".as_slice()) {
                Some(BencodeItemView::ByteString(root)) if len > 0 => {
                    Some(Hash::try_from(*root).map_err(|_| anyhow!("pieces root needs to be 32 bytes"))?)
                }
                _ if len > 0 => bail!("file {name:?} has no pieces root"),
                _ => None,
            };
            out.push(TreeFile {
                len,
                path: path.clone(),
                root,
                attr,
            });
        } else {
            walk_file_tree(node, path, out)?;
        }
        path.pop();
    }
    Ok(())
}

/// The v1 file list: `(files, raw paths, attributes)`.
type Layout = (Vec<(u64, PathBuf)>, Vec<Vec<String>>, Vec<FileAttr>);

fn v1_files(info: &mut BTreeMap<&[u8], BencodeItemView>, name: &str, root: &Path) -> anyhow::Result<Layout> {
    let file_len = |length: i64| -> anyhow::Result<u64> {
        u64::try_from(length).map_err(|_| anyhow!("file length {length} is negative"))
    };
    let (mut files, mut raw_paths, mut attrs) = (vec![], vec![], vec![]);
    if let Some(BencodeItemView::Integer(length)) = info.remove(b"length".as_slice()) {
        files.push((file_len(length)?, root.to_path_buf()));
        raw_paths.push(vec![name.to_string()]);
        attrs.push(FileAttr::parse(info)?);
    } else if let Some(BencodeItemView::List(file_lists)) = info.remove(b"files".as_slice()) {
        for entry in file_lists.iter() {
            let BencodeItemView::Dictionary(entries) = entry else {
                bail!("file entry needs to be a dict");
            };
            let mut attr = FileAttr::parse(entries)?;
            let length = match entries.get(b"length".as_slice()) {
                Some(BencodeItemView::Integer(length)) => file_len(*length)?,
                None if attr.symlink.is_some() => 0,
                _ => bail!("file length needs to be an integer"),
            };
            let mut f = root.to_path_buf();
            let mut raw = vec![name.to_string()];
            match entries.get(b"path".as_slice()) {
                Some(BencodeItemView::List(paths)) if !paths.is_empty() => {
                    for p in paths.iter() {
                        let BencodeItemView::ByteString(p) = p else {
                            bail!("file path segment needs to be a string");
                        };
                        let p = str::from_utf8(p)?;
                        f.push(safe_path_component(p)?);
                        raw.push(p.to_string());
                    }
                }
                // BEP 47: a padding file needn't have a path
                None if attr.pad => {
                    f.push(".pad");
                    f.push(length.to_string());
                    raw.extend([".pad".to_string(), length.to_string()]);
                }
                Some(BencodeItemView::List(_)) => bail!("file path is empty"),
                _ => bail!("file path needs to be a list"),
            }
            // BitComet's padding predates BEP 47's attribute
            if raw.last().is_some_and(|last| last.starts_with("_____padding_file_")) {
                attr.pad = true;
            }
            files.push((length, f));
            raw_paths.push(raw);
            attrs.push(attr);
        }
    }
    Ok((files, raw_paths, attrs))
}

/// Lays out a v2 `file tree` as a piece stream: each file not ending on a piece boundary is
/// followed by padding, but for the last.
fn v2_layout(tree: &[TreeFile], name: &str, root: &Path, piece: u64) -> anyhow::Result<(Layout, Vec<Option<Hash>>)> {
    let (mut files, mut raw_paths, mut attrs, mut roots) = (vec![], vec![], vec![], vec![]);
    let single = tree.len() == 1 && tree[0].path.len() == 1;
    let last_data = tree.iter().rposition(|f| f.len > 0);
    for (i, file) in tree.iter().enumerate() {
        if single {
            files.push((file.len, safe_path_component(&file.path[0])?));
            raw_paths.push(file.path.clone());
        } else {
            let mut path = root.to_path_buf();
            for segment in &file.path {
                path.push(safe_path_component(segment)?);
            }
            files.push((file.len, path));
            raw_paths.push(
                [name.to_string()]
                    .into_iter()
                    .chain(file.path.iter().cloned())
                    .collect(),
            );
        }
        attrs.push(file.attr.clone());
        roots.push(file.root);
        let gap = (piece - file.len % piece) % piece;
        if gap > 0 && Some(i) != last_data {
            files.push((gap, root.join(".pad").join(gap.to_string())));
            raw_paths.push(vec![name.to_string(), ".pad".to_string(), gap.to_string()]);
            attrs.push(FileAttr {
                pad: true,
                ..Default::default()
            });
            roots.push(None);
        }
    }
    Ok(((files, raw_paths, attrs), roots))
}

/// For a hybrid: the v2 root of each v1 file, if the two describe the same files in the same
/// order and every file starts on a piece boundary, as BEP 52 requires of a hybrid.
fn hybrid_roots(v1: &Layout, tree: &[TreeFile], piece: u64) -> anyhow::Result<Vec<Option<Hash>>> {
    let (files, raw_paths, attrs) = v1;
    let mut roots = vec![None; files.len()];
    let mut tree = tree.iter();
    let mut offset = 0u64;
    for (i, (len, _)) in files.iter().enumerate() {
        if !attrs[i].pad {
            let Some(file) = tree.next() else {
                bail!("v1 lists more files than the file tree");
            };
            let same_path = raw_paths[i].len() == 1 || raw_paths[i][1..] == file.path[..];
            if file.len != *len || !same_path {
                bail!("v1 file {:?} isn't the file tree's {:?}", raw_paths[i], file.path);
            }
            if *len > 0 && !offset.is_multiple_of(piece) {
                bail!("{:?} doesn't start on a piece boundary", raw_paths[i]);
            }
            roots[i] = file.root;
        }
        offset += len;
    }
    if tree.next().is_some() {
        bail!("the file tree lists more files than v1");
    }
    Ok(roots)
}

/// Parses a torrent metadata file and returns a Torrent struct
pub fn parse_torrent(metadata_file: &[u8]) -> anyhow::Result<Torrent> {
    let (hash, raw_info) = compute_info_hash(metadata_file)?;

    let (_, mut torrent) = juicy_bencode::parse_bencode_dict(metadata_file).map_err(|_| {
        // the error type has a reference on the input, we don't want that
        anyhow!("not a valid dict")
    })?;

    let Some(BencodeItemView::Dictionary(mut info)) = torrent.remove(b"info".as_slice()) else {
        bail!("info needs to be a dict");
    };

    // Parse announce-list (optional, multi-tracker support)
    let mut announce_tiers = Vec::new();

    if let Some(BencodeItemView::List(announce_list)) = torrent.remove(b"announce-list".as_slice()) {
        // Parse announce-list: list of lists of strings
        let mut announce_list = announce_list.iter();
        while let Some(BencodeItemView::List(tier)) = announce_list.next() {
            let mut tier_urls = Vec::new();
            let mut tier = tier.iter();
            while let Some(BencodeItemView::ByteString(url)) = tier.next() {
                if let Ok(url_str) = String::from_utf8(url.to_vec()) {
                    tier_urls.push(url_str);
                }
            }
            if !tier_urls.is_empty() {
                announce_tiers.push(tier_urls);
            }
        }
    }

    // Fall back to single announce if announce-list is not present or empty. Neither is
    // required: a torrent built from a tracker-less magnet finds its peers over the DHT.
    if announce_tiers.is_empty()
        && let Some(BencodeItemView::ByteString(announce)) = torrent.remove(b"announce".as_slice())
    {
        announce_tiers.push(vec![String::from_utf8(announce.to_vec())?]);
    }

    let web_seeds = match torrent.remove(b"url-list".as_slice()) {
        Some(BencodeItemView::ByteString(url)) => web_seed_urls([url]),
        Some(BencodeItemView::List(urls)) => web_seed_urls(urls.iter().filter_map(|url| match url {
            BencodeItemView::ByteString(url) => Some(*url),
            _ => None,
        })),
        _ => vec![],
    };

    // BEP 52 says to check this before anything else, so a newer format is reported as such
    let v2 = match info.remove(b"meta version".as_slice()) {
        None => false,
        Some(BencodeItemView::Integer(2)) => true,
        Some(BencodeItemView::Integer(n)) if n > 2 => bail!("meta version {n} is newer than this client supports"),
        Some(_) => bail!("meta version needs to be 2"),
    };

    let Some(BencodeItemView::ByteString(name)) = info.remove(b"name".as_slice()) else {
        bail!("name needs to be a string");
    };
    let name = str::from_utf8(name)?.to_string();
    // paths are relative to a download root the caller chooses per torrent (see
    // `BtClient::add_torrent`); this only decides the layout under it
    let root = safe_path_component(&name)?;

    let Some(BencodeItemView::Integer(piece_len)) = info.remove(b"piece length".as_slice()) else {
        bail!("piece length needs to be an integer");
    };
    if !(1..=MAX_PIECE_SIZE as i64).contains(&piece_len) {
        bail!("piece length {piece_len} is out of range");
    }
    let piece_len = piece_len as u64;

    let pieces = match info.remove(b"pieces".as_slice()) {
        Some(BencodeItemView::ByteString(pieces)) => Some(pieces),
        None if v2 => None,
        _ => bail!("pieces needs to be a byte string"),
    };
    if let Some(pieces) = pieces
        && pieces.len() % 20 != 0
    {
        bail!("pieces is {} bytes, not a multiple of 20", pieces.len());
    }

    // BEP 27: absent means not private; some encoders write `0` explicitly rather than
    // omitting the key, so treat any non-1 value the same as absent instead of erroring.
    let private = matches!(info.remove(b"private".as_slice()), Some(BencodeItemView::Integer(1)));

    let tree = if v2 {
        if !piece_len.is_power_of_two() || piece_len < merkle::BLOCK as u64 {
            bail!("a v2 piece length must be a power of two of at least 16 KiB, not {piece_len}");
        }
        let Some(BencodeItemView::Dictionary(tree)) = info.remove(b"file tree".as_slice()) else {
            bail!("file tree needs to be a dict");
        };
        let mut files = vec![];
        walk_file_tree(&tree, &mut vec![], &mut files)?;
        Some(files)
    } else {
        None
    };

    let (layout, roots) = match (&tree, pieces.is_some()) {
        (Some(tree), false) => {
            let (layout, roots) = v2_layout(tree, &name, &root, piece_len)?;
            (layout, Some(roots))
        }
        (tree, _) => {
            let layout = v1_files(&mut info, &name, &root)?;
            // BEP 52 lets a client that finds the halves of a hybrid disagreeing carry on with
            // one of them; v1 is the one the swarm goes by
            let roots = tree
                .as_ref()
                .and_then(|tree| match hybrid_roots(&layout, tree, piece_len) {
                    Ok(roots) => Some(roots),
                    Err(e) => {
                        tracing::warn!("{name}: ignoring the v2 half of an inconsistent hybrid: {e:#}");
                        None
                    }
                });
            (layout, roots)
        }
    };
    let (files, raw_paths, attrs) = layout;
    if files.is_empty() {
        bail!("torrent has no files");
    }

    let total_size = files
        .iter()
        .try_fold(0u64, |acc, (len, _)| acc.checked_add(*len))
        .ok_or_else(|| anyhow!("total size overflows"))?;
    if total_size == 0 {
        bail!("torrent is empty");
    }
    let num_pieces = total_size.div_ceil(piece_len);
    let pieces = match pieces {
        Some(pieces) => {
            let pieces = pieces.as_chunks::<20>().0.to_vec();
            if pieces.len() as u64 != num_pieces {
                bail!(
                    "{} piece hashes for {total_size} bytes in pieces of {piece_len}",
                    pieces.len()
                );
            }
            pieces
        }
        None => vec![],
    };
    if u32::try_from(num_pieces).is_err() {
        bail!("too many pieces");
    }
    // an evenly-divisible torrent has a "remainder" of 0, but the last piece is still
    // full-sized in that case
    let last_piece_len = match total_size % piece_len {
        0 => piece_len,
        remainder => remainder,
    };
    let offsets = files
        .iter()
        .scan(0u64, |at, (len, _)| {
            let start = *at;
            *at += len;
            Some(start)
        })
        .collect();

    let mut torrent_v2 = None;
    if let Some(roots) = roots {
        torrent_v2 = Some(V2 {
            info_hash: Sha256::digest(&raw_info).into(),
            layers: (0..roots.len()).map(|_| OnceLock::new()).collect(),
            roots,
            inconsistent: Arc::default(),
        });
    }
    let info_hash = match &torrent_v2 {
        Some(v2) if pieces.is_empty() => InfoHash::from_bytes(&v2.info_hash[..20]),
        _ => hash,
    };

    let parsed = Torrent {
        announce_tiers,
        piece_size: piece_len as u32,
        pieces,
        total_size,
        files,
        attrs,
        last_piece_size: last_piece_len as u32,
        name,
        info_hash,
        v2: torrent_v2,
        raw_info,
        private,
        web_seeds,
        raw_paths,
        offsets,
        num_pieces: num_pieces as usize,
    };

    // BEP 52: outside the info dict, so not covered by the info hash, but each must roll up to
    // its file's root. A torrent rebuilt from a magnet's metadata has none, and its resume file
    // those that came before it stopped: peers send the rest.
    if parsed.v2.is_some()
        && let Some(layers) = torrent.remove(b"piece layers".as_slice())
    {
        let BencodeItemView::Dictionary(layers) = layers else {
            bail!("piece layers needs to be a dict");
        };
        for file in parsed.missing_layers() {
            let root = parsed
                .v2
                .as_ref()
                .and_then(|v2| v2.roots[file])
                .expect("missing_layers has roots");
            let layer = match layers.get(root.as_slice()) {
                Some(BencodeItemView::ByteString(layer)) => layer,
                None => continue,
                Some(_) => bail!("the piece layer of {:?} needs to be a string", parsed.files[file].1),
            };
            let (hashes, rest) = layer.as_chunks::<32>();
            if !rest.is_empty() || !parsed.set_layer(file, hashes.to_vec()) {
                bail!(
                    "the piece layer of {:?} doesn't match its pieces root",
                    parsed.files[file].1
                );
            }
        }
    }

    Ok(parsed)
}

/// The usable web seeds among `urls`: valid UTF-8 http(s) URLs, each once. Torrent makers
/// put all sorts in `url-list` (empty strings, ftp, file paths); anything else is skipped.
pub(crate) fn web_seed_urls<'a>(urls: impl IntoIterator<Item = &'a [u8]>) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    for url in urls {
        let Ok(url) = str::from_utf8(url) else {
            continue;
        };
        let url = url.trim();
        let http = url::Url::parse(url).is_ok_and(|u| matches!(u.scheme(), "http" | "https") && u.has_host());
        if http && !out.iter().any(|seen| seen == url) {
            out.push(url.to_string());
        }
    }
    out
}

/// Computes the info hash of a torrent metadata file, and also returns the raw bencoded bytes
/// of the "info" dict (needed to serve BEP 9 ut_metadata requests).
fn compute_info_hash(input: &[u8]) -> anyhow::Result<(InfoHash, Vec<u8>)> {
    let bencode = |e: bendy::decoding::Error| anyhow!("invalid bencode: {e}");
    let mut decoder = bendy::decoding::Decoder::new(input);
    let Some(Object::Dict(mut dict)) = decoder.next_object().map_err(bencode)? else {
        bail!("torrent metadata must be a dictionary")
    };

    while let Some((key, val)) = dict.next_pair().map_err(bencode)? {
        if key == b"info" {
            let Object::Dict(dict_decoder) = val else {
                bail!("info needs to be a dict");
            };
            let buf = dict_decoder.into_raw().map_err(bencode)?;
            let hash = InfoHash::from_bytes(Sha1::digest(buf).as_slice());
            return Ok((hash, buf.to_vec()));
        }
    }

    bail!("torrent metadata must contain 'info' key")
}

/// Hand-built v2 and hybrid torrents for tests, straight from BEP 52's definitions.
#[cfg(test)]
pub(crate) mod fixtures {
    use crate::merkle::{self, Hash};
    use sha1::{Digest, Sha1};
    use std::collections::BTreeMap;

    pub fn bstr(out: &mut Vec<u8>, bytes: &[u8]) {
        out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
        out.extend_from_slice(bytes);
    }

    enum Node {
        File(usize),
        Dir(BTreeMap<String, Node>),
    }

    fn encode_tree(out: &mut Vec<u8>, dir: &BTreeMap<String, Node>, files: &[(&[&str], Vec<u8>)]) {
        out.push(b'd');
        for (name, node) in dir {
            bstr(out, name.as_bytes());
            match node {
                Node::Dir(dir) => encode_tree(out, dir, files),
                Node::File(i) => {
                    let data = &files[*i].1;
                    out.extend_from_slice(format!("d0:d6:lengthi{}e", data.len()).as_bytes());
                    if !data.is_empty() {
                        bstr(out, b"pieces root");
                        bstr(out, &root(data));
                    }
                    out.extend_from_slice(b"ee");
                }
            }
        }
        out.push(b'e');
    }

    pub fn root(data: &[u8]) -> Hash {
        merkle::data_root(data, merkle::file_leaves(data.len() as u64))
    }

    pub fn layer(data: &[u8], piece: usize) -> Vec<Hash> {
        data.chunks(piece)
            .map(|c| merkle::data_root(c, piece / merkle::BLOCK))
            .collect()
    }

    /// The files in BEP 52 order (the file tree's: sorted by path), each `(path, data)`.
    pub fn sorted<'a>(files: &[(&'a [&'a str], Vec<u8>)]) -> Vec<(&'a [&'a str], Vec<u8>)> {
        let mut files = files.to_vec();
        files.sort_by(|a, b| a.0.cmp(b.0));
        files
    }

    /// The info dict of a torrent `name` holding `files` (already in file tree order), in
    /// pieces of `piece` bytes; a hybrid also gets the v1 keys, padding files included.
    pub fn info(name: &str, files: &[(&[&str], Vec<u8>)], piece: usize, hybrid: bool) -> Vec<u8> {
        let mut tree = BTreeMap::new();
        for (i, (path, _)) in files.iter().enumerate() {
            let mut dir = &mut tree;
            for segment in &path[..path.len() - 1] {
                let Node::Dir(next) = dir
                    .entry(segment.to_string())
                    .or_insert_with(|| Node::Dir(BTreeMap::new()))
                else {
                    panic!("a file and a directory share a name");
                };
                dir = next;
            }
            dir.insert(path[path.len() - 1].to_string(), Node::File(i));
        }
        let mut out = b"d".to_vec();
        bstr(&mut out, b"file tree");
        encode_tree(&mut out, &tree, files);
        let mut stream = vec![];
        if hybrid {
            bstr(&mut out, b"files");
            out.push(b'l');
            let last = files.iter().rposition(|(_, d)| !d.is_empty());
            for (i, (path, data)) in files.iter().enumerate() {
                out.extend_from_slice(format!("d6:lengthi{}e4:pathl", data.len()).as_bytes());
                for segment in *path {
                    bstr(&mut out, segment.as_bytes());
                }
                out.extend_from_slice(b"ee");
                stream.extend_from_slice(data);
                let gap = (piece - data.len() % piece) % piece;
                if gap > 0 && Some(i) != last {
                    out.extend_from_slice(format!("d4:attr1:p6:lengthi{gap}e4:pathl4:.pad").as_bytes());
                    bstr(&mut out, gap.to_string().as_bytes());
                    out.extend_from_slice(b"ee");
                    stream.resize(stream.len() + gap, 0);
                }
            }
            out.push(b'e');
        }
        out.extend_from_slice(b"12:meta versioni2e");
        bstr(&mut out, b"name");
        bstr(&mut out, name.as_bytes());
        out.extend_from_slice(format!("12:piece lengthi{piece}e").as_bytes());
        if hybrid {
            let pieces: Vec<u8> = stream.chunks(piece).flat_map(|c| Sha1::digest(c).to_vec()).collect();
            bstr(&mut out, b"pieces");
            bstr(&mut out, &pieces);
        }
        out.push(b'e');
        out
    }

    /// The `piece layers` dict for `files`.
    pub fn piece_layers(files: &[(&[&str], Vec<u8>)], piece: usize) -> Vec<u8> {
        let layers: BTreeMap<Hash, Vec<Hash>> = files
            .iter()
            .filter(|(_, d)| d.len() > piece)
            .map(|(_, d)| (root(d), layer(d, piece)))
            .collect();
        let mut out = b"d".to_vec();
        for (root, layer) in layers {
            bstr(&mut out, &root);
            bstr(&mut out, layer.as_flattened());
        }
        out.push(b'e');
        out
    }

    /// A whole .torrent: `info` plus the piece layers.
    pub fn torrent_file(name: &str, files: &[(&[&str], Vec<u8>)], piece: usize, hybrid: bool) -> Vec<u8> {
        let info = info(name, files, piece, hybrid);
        crate::metadata::build_torrent_file_with(
            &info,
            &["http://unused.test/announce".to_string()],
            Some(&piece_layers(files, piece)),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn bencode_string(bytes: &[u8]) -> Vec<u8> {
        let mut out = format!("{}:", bytes.len()).into_bytes();
        out.extend_from_slice(bytes);
        out
    }

    /// Hand-builds the bencode for a minimal single-file torrent, so parsing tests don't
    /// depend on an external `.torrent` fixture.
    fn single_file_torrent(total_size: u64, piece_length: u32) -> Vec<u8> {
        single_file_torrent_with_privacy(total_size, piece_length, false)
    }

    fn single_file_torrent_with_privacy(total_size: u64, piece_length: u32, private: bool) -> Vec<u8> {
        single_file_torrent_with_name(total_size, piece_length, private, b"test.txt")
    }

    fn single_file_torrent_with_name(total_size: u64, piece_length: u32, private: bool, name: &[u8]) -> Vec<u8> {
        let num_pieces = total_size.div_ceil(piece_length as u64) as usize;
        let pieces: Vec<u8> = (0..num_pieces)
            .flat_map(|i| {
                let mut hash = [0u8; 20];
                hash[0] = i as u8;
                hash
            })
            .collect();

        let mut info = Vec::new();
        info.extend_from_slice(b"d");
        info.extend_from_slice(&bencode_string(b"length"));
        info.extend_from_slice(format!("i{total_size}e").as_bytes());
        info.extend_from_slice(&bencode_string(b"name"));
        info.extend_from_slice(&bencode_string(name));
        info.extend_from_slice(&bencode_string(b"piece length"));
        info.extend_from_slice(format!("i{piece_length}e").as_bytes());
        info.extend_from_slice(&bencode_string(b"pieces"));
        info.extend_from_slice(&bencode_string(&pieces));
        if private {
            info.extend_from_slice(&bencode_string(b"private"));
            info.extend_from_slice(b"i1e");
        }
        info.extend_from_slice(b"e");

        let mut torrent = Vec::new();
        torrent.extend_from_slice(b"d");
        torrent.extend_from_slice(&bencode_string(b"announce"));
        torrent.extend_from_slice(&bencode_string(b"http://tracker.test/announce"));
        torrent.extend_from_slice(&bencode_string(b"info"));
        torrent.extend_from_slice(&info);
        torrent.extend_from_slice(b"e");
        torrent
    }

    /// Hand-builds a two-file torrent (5 bytes each) where the second file's "path" list is
    /// exactly `path_components`, so tests can hand it something malicious.
    fn multi_file_torrent_with_path(path_components: &[&[u8]]) -> Vec<u8> {
        let pieces = bencode_string(&[0u8; 20]);

        let mut second_path = Vec::new();
        second_path.extend_from_slice(b"l");
        for component in path_components {
            second_path.extend_from_slice(&bencode_string(component));
        }
        second_path.extend_from_slice(b"e");

        let mut info = Vec::new();
        info.extend_from_slice(b"d");
        info.extend_from_slice(&bencode_string(b"files"));
        info.extend_from_slice(b"l");
        // first file: ordinary, single-segment path
        info.extend_from_slice(b"d");
        info.extend_from_slice(&bencode_string(b"length"));
        info.extend_from_slice(b"i5e");
        info.extend_from_slice(&bencode_string(b"path"));
        info.extend_from_slice(b"l");
        info.extend_from_slice(&bencode_string(b"a.txt"));
        info.extend_from_slice(b"e");
        info.extend_from_slice(b"e");
        // second file: the one under test
        info.extend_from_slice(b"d");
        info.extend_from_slice(&bencode_string(b"length"));
        info.extend_from_slice(b"i5e");
        info.extend_from_slice(&bencode_string(b"path"));
        info.extend_from_slice(&second_path);
        info.extend_from_slice(b"e");
        info.extend_from_slice(b"e");
        info.extend_from_slice(&bencode_string(b"name"));
        info.extend_from_slice(&bencode_string(b"multi"));
        info.extend_from_slice(&bencode_string(b"piece length"));
        info.extend_from_slice(b"i10e");
        info.extend_from_slice(&bencode_string(b"pieces"));
        info.extend_from_slice(&pieces);
        info.extend_from_slice(b"e");

        let mut torrent = Vec::new();
        torrent.extend_from_slice(b"d");
        torrent.extend_from_slice(&bencode_string(b"announce"));
        torrent.extend_from_slice(&bencode_string(b"http://tracker.test/announce"));
        torrent.extend_from_slice(&bencode_string(b"info"));
        torrent.extend_from_slice(&info);
        torrent.extend_from_slice(b"e");
        torrent
    }

    /// A multi-file torrent with `files` as (length, name) and `piece_hashes` zeroed hashes.
    fn multi_file_torrent(files: &[(i64, &str)], piece_length: i64, piece_hashes: usize) -> Vec<u8> {
        let mut info = b"d5:filesl".to_vec();
        for (length, name) in files {
            info.extend_from_slice(format!("d6:lengthi{length}e4:pathl").as_bytes());
            info.extend_from_slice(&bencode_string(name.as_bytes()));
            info.extend_from_slice(b"ee");
        }
        info.extend_from_slice(format!("e4:name5:multi12:piece lengthi{piece_length}e6:pieces").as_bytes());
        info.extend_from_slice(&bencode_string(&vec![0u8; piece_hashes * 20]));
        info.push(b'e');
        crate::metadata::build_torrent_file(&info, &[])
    }

    #[test]
    fn lays_out_files_past_4_gib() {
        const MIB: u64 = 1 << 20;
        let big = 5 << 30;
        let bytes = multi_file_torrent(
            &[(MIB as i64, "a"), (big as i64, "big"), (MIB as i64, "c")],
            4 << 20,
            1281,
        );
        let torrent = parse_torrent(&bytes).unwrap();

        assert_eq!(torrent.files[1].0, big);
        assert_eq!(torrent.total_size, big + 2 * MIB);
        assert_eq!(torrent.pieces_of_file(0), 0..1);
        assert_eq!(torrent.pieces_of_file(1), 0..1281);
        // "c" starts at 5 GiB + 1 MiB: the second half of piece 1280, the last one
        assert_eq!(torrent.pieces_of_file(2), 1280..1281);
        assert_eq!(torrent.last_piece_size as u64, 2 * MIB);
        let wanted = torrent.wanted_pieces(&[false, false, true]);
        assert_eq!(wanted.count_ones(), 1);
        assert!(wanted[1280]);
    }

    #[test]
    fn rejects_malformed_info() {
        let ok = multi_file_torrent(&[(5, "a"), (5, "b")], 4, 3);
        assert!(parse_torrent(&ok).is_ok());

        let mut bad_pieces = single_file_torrent(15, 5);
        // "pieces" is the last info key; cut a byte off its hashes and its length prefix
        let at = bad_pieces.windows(4).position(|w| w == b"60:\0").unwrap();
        bad_pieces.splice(at..at + 3, *b"59:");
        bad_pieces.remove(at + 3);
        assert!(parse_torrent(&bad_pieces).is_err(), "pieces not a multiple of 20");

        assert!(
            parse_torrent(&multi_file_torrent(&[(5, "a")], 0, 0)).is_err(),
            "zero piece length"
        );
        assert!(
            parse_torrent(&multi_file_torrent(&[(5, "a")], -4, 2)).is_err(),
            "negative piece length"
        );
        assert!(
            parse_torrent(&multi_file_torrent(&[(5, "a")], 1 << 40, 1)).is_err(),
            "huge piece length"
        );
        assert!(
            parse_torrent(&multi_file_torrent(&[(-5, "a"), (10, "b")], 4, 2)).is_err(),
            "negative length"
        );
        assert!(
            parse_torrent(&multi_file_torrent(&[(5, "a"), (5, "b")], 4, 2)).is_err(),
            "too few hashes"
        );
        assert!(
            parse_torrent(&multi_file_torrent(&[(5, "a"), (5, "b")], 4, 4)).is_err(),
            "too many hashes"
        );
        assert!(parse_torrent(&multi_file_torrent(&[], 4, 0)).is_err(), "no files");
        assert!(
            parse_torrent(&multi_file_torrent(&[(0, "a")], 4, 0)).is_err(),
            "empty torrent"
        );
        let overflow = multi_file_torrent(&[(i64::MAX, "a"), (i64::MAX, "b"), (2, "c")], 1 << 20, 0);
        assert!(parse_torrent(&overflow).is_err(), "total size overflows");

        assert!(parse_torrent(b"d8:announce3:urle").is_err(), "missing info");
        assert!(parse_torrent(b"d4:infoi1ee").is_err(), "info not a dict");
        assert!(parse_torrent(b"d4:infod4:name1:aee").is_err(), "info missing keys");
        assert!(parse_torrent(b"li1ee").is_err(), "not a dict");
        assert!(parse_torrent(b"").is_err(), "empty input");
        assert!(parse_torrent(b"d4:info").is_err(), "truncated");
    }

    #[test]
    fn parses_evenly_divisible_torrent() {
        let bytes = single_file_torrent(15, 5);
        let torrent = parse_torrent(&bytes).unwrap();

        assert_eq!(torrent.total_size, 15);
        assert_eq!(torrent.piece_size, 5);
        assert_eq!(torrent.pieces.len(), 3);
        // an evenly-divisible torrent's last piece is still full-sized, not zero
        assert_eq!(torrent.last_piece_size, 5);
        assert_eq!(torrent.nth_piece_size(2u32), Some(5));
        assert_eq!(torrent.files.len(), 1);
        assert_eq!(torrent.files[0].0, 15);
        assert_eq!(torrent.primary_tracker(), Some("http://tracker.test/announce"));

        // raw_info must be exactly the bencoded "info" dict, byte for byte, since a peer
        // fetching it over BEP 9 needs to reconstruct the exact bytes info_hash was taken over
        let pieces: Vec<u8> = (0..3u8)
            .flat_map(|i| {
                let mut hash = [0u8; 20];
                hash[0] = i;
                hash
            })
            .collect();
        let mut expected_info = Vec::new();
        expected_info.extend_from_slice(b"d");
        expected_info.extend_from_slice(&bencode_string(b"length"));
        expected_info.extend_from_slice(b"i15e");
        expected_info.extend_from_slice(&bencode_string(b"name"));
        expected_info.extend_from_slice(&bencode_string(b"test.txt"));
        expected_info.extend_from_slice(&bencode_string(b"piece length"));
        expected_info.extend_from_slice(b"i5e");
        expected_info.extend_from_slice(&bencode_string(b"pieces"));
        expected_info.extend_from_slice(&bencode_string(&pieces));
        expected_info.extend_from_slice(b"e");

        assert_eq!(torrent.raw_info, expected_info);
        assert_eq!(torrent.metadata_size() as usize, torrent.raw_info.len());
    }

    #[test]
    fn parses_torrent_with_a_short_last_piece() {
        let bytes = single_file_torrent(17, 5);
        let torrent = parse_torrent(&bytes).unwrap();

        assert_eq!(torrent.pieces.len(), 4);
        assert_eq!(torrent.last_piece_size, 2);
        assert_eq!(torrent.nth_piece_size(0u32), Some(5));
        assert_eq!(torrent.nth_piece_size(3u32), Some(2));
        assert_eq!(torrent.nth_piece_size(4u32), None);
    }

    #[test]
    fn parses_url_list_as_a_string_or_a_list() {
        let with = |url_list: &str| {
            let plain = single_file_torrent(15, 5);
            let mut bytes = plain[..plain.len() - 1].to_vec();
            bytes.extend_from_slice(b"8:url-list");
            bytes.extend_from_slice(url_list.as_bytes());
            bytes.push(b'e');
            parse_torrent(&bytes).unwrap().web_seeds
        };
        assert_eq!(with("16:http://m.test/a/"), ["http://m.test/a/"]);
        assert_eq!(
            with("l16:http://m.test/a/0:13:ftp://m.test/18:https://n.test/b/c16:http://m.test/a/e"),
            ["http://m.test/a/", "https://n.test/b/c"],
            "empty, non-http and repeated entries are skipped"
        );
        assert!(parse_torrent(&single_file_torrent(15, 5)).unwrap().web_seeds.is_empty());
    }

    #[test]
    fn keeps_raw_paths_for_web_seeds() {
        let multi = parse_torrent(&multi_file_torrent_with_path(&[b"AC/DC", b"b c.txt"])).unwrap();
        assert_eq!(multi.raw_paths[1], ["multi", "AC/DC", "b c.txt"]);
        let single = parse_torrent(&single_file_torrent(15, 5)).unwrap();
        assert_eq!(single.raw_paths, [["test.txt"]]);
    }

    #[test]
    fn parses_private_flag() {
        let public = parse_torrent(&single_file_torrent(15, 5)).unwrap();
        assert!(!public.private);

        let private = parse_torrent(&single_file_torrent_with_privacy(15, 5, true)).unwrap();
        assert!(private.private);
    }

    #[test]
    fn safe_path_component_accepts_plain_names() {
        assert!(safe_path_component("a.txt").is_ok());
        assert!(safe_path_component("subdir").is_ok());
    }

    #[test]
    fn safe_path_component_rejects_traversal_primitives() {
        // ".." and "." aren't affected by slash substitution -- they're rejected because
        // `Path::components()` parses them as ParentDir/CurDir, not because of any character
        // they contain.
        assert!(safe_path_component("..").is_err());
        assert!(safe_path_component(".").is_err());
    }

    /// A `/` or `\` inside a single segment is substituted, not rejected -- real torrent names
    /// sometimes contain one for cosmetic reasons (e.g. an artist named "AC/DC"), and
    /// substitution defeats any traversal it could otherwise spell out just as well as an
    /// error would, without failing on a legitimate torrent.
    #[test]
    fn safe_path_component_substitutes_embedded_separators_instead_of_rejecting() {
        let sanitized = safe_path_component("AC/DC - album").unwrap();
        assert_eq!(sanitized.components().count(), 1, "must not become a nested path");
        assert!(!sanitized.to_string_lossy().contains('/'));

        // even an embedded absolute-looking path is neutralized into one harmless segment,
        // not resolved as an absolute path
        let sanitized = safe_path_component("/etc/passwd").unwrap();
        assert_eq!(sanitized.components().count(), 1);
        assert!(!sanitized.is_absolute());
    }

    /// A malicious torrent's "name" field is attacker-controlled; without validation it could
    /// walk the download root outside the intended directory (e.g. naming the torrent "..").
    #[test]
    fn rejects_path_traversal_in_name() {
        let bytes = single_file_torrent_with_name(15, 5, false, b"..");
        assert!(parse_torrent(&bytes).is_err());
    }

    /// Same concern as the name field, but for a multi-file torrent's "path" list: BEP 3 gives
    /// each list entry as its own segment specifically so this is the vector that matters (as
    /// opposed to a slash embedded within a single entry, which substitution already handles).
    #[test]
    fn rejects_path_traversal_in_file_path() {
        let bytes = multi_file_torrent_with_path(&[b"..", b"..", b"etc", b"passwd"]);
        assert!(parse_torrent(&bytes).is_err());

        // a well-formed nested path is still fine
        let bytes = multi_file_torrent_with_path(&[b"subdir", b"b.txt"]);
        assert!(parse_torrent(&bytes).is_ok());
    }

    const LIBTORRENT_V2: &[u8] = include_bytes!("../testdata/bittorrent-v2-test.torrent");
    const LIBTORRENT_HYBRID: &[u8] = include_bytes!("../testdata/bittorrent-v2-hybrid-test.torrent");

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// libtorrent's own v2 test torrent: parsing it checks every piece layer against its
    /// file's root, so our Merkle trees agree with libtorrent's.
    #[test]
    fn parses_libtorrents_v2_test_torrent() {
        let t = parse_torrent(LIBTORRENT_V2).unwrap();
        assert!(t.v2_only());
        let v2 = t.v2.as_ref().unwrap();
        assert_eq!(
            hex(&v2.info_hash),
            "caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e"
        );
        assert_eq!(t.info_hash.as_bytes(), &v2.info_hash[..20]);
        assert!(t.hybrid_v2_hash().is_none());
        assert!(t.missing_layers().is_empty(), "the .torrent carries every layer");
        assert_eq!(t.piece_size, 4 << 20);
        assert_eq!(t.num_pieces(), 371);
        let real: Vec<usize> = (0..t.files.len()).filter(|&f| !t.attrs[f].pad).collect();
        assert_eq!(real.len(), 11);
        for f in real.iter().copied().filter(|&f| t.files[f].0 > 0) {
            assert!(
                t.file_offset(f).is_multiple_of(t.piece_size as u64),
                "{:?}",
                t.files[f].1
            );
        }
        assert!(t.files.iter().all(|(_, p)| p.starts_with("bittorrent-v2-test")));
        assert_eq!(swarm_info_hash(&t.raw_info), t.info_hash);
        // a layer that came from peers goes back into a .torrent the same as it came
        let layers = t.piece_layers_bencoded().unwrap();
        let rebuilt = crate::metadata::build_torrent_file_with(&t.raw_info, &[], Some(&layers));
        assert!(parse_torrent(&rebuilt).unwrap().missing_layers().is_empty());
    }

    #[test]
    fn parses_libtorrents_hybrid_test_torrent() {
        let t = parse_torrent(LIBTORRENT_HYBRID).unwrap();
        assert!(!t.v2_only());
        assert_eq!(hex(t.info_hash.as_bytes()), "631a31dd0a46257d5078c0dee4e66e26f73e42ac");
        let v2 = t.v2.as_ref().expect("the halves agree");
        assert_eq!(
            hex(&v2.info_hash),
            "d8dd32ac93357c368556af3ac1d95c9d76bd0dff6fa9833ecdac3d53134efabb"
        );
        assert_eq!(hex(t.hybrid_v2_hash().unwrap().as_bytes()), hex(&v2.info_hash[..20]));
        assert_eq!(swarm_info_hash(&t.raw_info), t.info_hash, "a hybrid goes by v1");
        assert_eq!(t.files.len(), 17);
        assert_eq!(t.attrs.iter().filter(|a| a.pad).count(), 8);
        assert_eq!(t.attrs.iter().filter(|a| a.executable).count(), 3);
        for (f, attr) in t.attrs.iter().enumerate() {
            assert_eq!(v2.roots[f].is_none(), attr.pad || t.files[f].0 == 0);
        }
        // padding wants nothing; everything else is wanted
        let wanted = t.wanted_pieces(&vec![true; t.files.len()]);
        assert!(wanted.all());
    }

    use super::fixtures::{self, sorted};
    use std::ops::Range;

    fn data(len: usize, seed: usize) -> Vec<u8> {
        (0..len).map(|i| ((i * 7 + seed * 13) % 251) as u8).collect()
    }

    const P: usize = 32768;

    fn files() -> Vec<(&'static [&'static str], Vec<u8>)> {
        sorted(&[
            (&["a"], data(40_000, 1)),
            (&["b", "c"], vec![]),
            (&["d"], data(70_000, 2)),
        ])
    }

    /// The piece stream: each file padded to a piece boundary but the last.
    fn stream(files: &[(&[&str], Vec<u8>)]) -> Vec<u8> {
        let mut out = vec![];
        let last = files.iter().rposition(|(_, d)| !d.is_empty()).unwrap();
        for (i, (_, d)) in files.iter().enumerate() {
            out.extend_from_slice(d);
            if i != last {
                out.resize(out.len().next_multiple_of(P), 0);
            }
        }
        out
    }

    #[test]
    fn lays_out_a_v2_torrent_with_padding() {
        let files = files();
        let t = parse_torrent(&fixtures::torrent_file("v2", &files, P, false)).unwrap();
        assert!(t.v2_only());
        let layout: Vec<(u64, String, bool)> = t
            .files
            .iter()
            .zip(&t.attrs)
            .map(|((len, path), attr)| (*len, path.display().to_string(), attr.pad))
            .collect();
        assert_eq!(
            layout,
            [
                (40_000, "v2/a".to_string(), false),
                (25_536, "v2/.pad/25536".to_string(), true),
                (0, "v2/b/c".to_string(), false),
                (70_000, "v2/d".to_string(), false),
            ]
        );
        assert_eq!(t.total_size, 65_536 + 70_000);
        assert_eq!(t.num_pieces(), 5);
        assert_eq!(t.last_piece_size as usize, 70_000 - 2 * P);
        assert_eq!(t.pieces_of_file(3), 2..5);
        assert_eq!(
            t.padding_in_piece(1),
            [Range {
                start: 40_000 - P,
                end: P
            }]
        );
        assert!(t.padding_in_piece(2).is_empty());
        assert_eq!(t.raw_paths[2], ["v2", "b", "c"]);

        let stream = stream(&files);
        for piece in 0..5u32 {
            let start = piece as usize * P;
            let chunk = &stream[start..(start + P).min(stream.len())];
            assert!(t.valid_piece(piece, chunk), "piece {piece}");
            let mut bad = chunk.to_vec();
            bad[0] ^= 1;
            assert!(!t.valid_piece(piece, &bad), "piece {piece} corrupted");
        }
        // padding isn't hashed: what a peer sends there doesn't matter
        let mut odd = stream[P..2 * P].to_vec();
        odd[P - 1] = 0xff;
        assert!(t.valid_piece(1, &odd));
    }

    #[test]
    fn a_single_file_v2_torrent_is_named_by_its_tree() {
        let files = [(&["only.bin"][..], data(50_000, 3))];
        let t = parse_torrent(&fixtures::torrent_file("ignored", &files, P, false)).unwrap();
        assert_eq!(t.files, [(50_000, PathBuf::from("only.bin"))]);
        assert_eq!(t.top_level(), PathBuf::from("only.bin"));
        assert_eq!(t.num_pieces(), 2);
    }

    #[test]
    fn a_v2_torrent_without_piece_layers_waits_for_them() {
        let files = files();
        let info = fixtures::info("v2", &files, P, false);
        let t = parse_torrent(&crate::metadata::build_torrent_file(&info, &[])).unwrap();
        // "a" is two pieces, "d" three; both need a layer
        assert_eq!(t.missing_layers(), [0, 3]);
        assert!(!t.can_verify(0));
        assert!(!t.valid_piece(0, &stream(&files)[..P]));
        assert!(!t.set_layer(0, fixtures::layer(&files[2].1, P)), "d's layer isn't a's");
        assert!(t.set_layer(0, fixtures::layer(&files[0].1, P)));
        assert!(t.can_verify(0) && t.can_verify(1) && !t.can_verify(2));
        assert!(t.valid_piece(0, &stream(&files)[..P]));
        assert_eq!(t.missing_layers(), [3]);
        // a clone shares what's come in since
        let clone = t.clone();
        assert!(t.set_layer(3, fixtures::layer(&files[2].1, P)));
        assert!(clone.missing_layers().is_empty());
    }

    #[test]
    fn rejects_piece_layers_that_dont_match() {
        let files = files();
        let info = fixtures::info("v2", &files, P, false);
        let mut layers = fixtures::piece_layers(&files, P);
        let last = layers.len() - 2;
        layers[last] ^= 1;
        assert!(parse_torrent(&crate::metadata::build_torrent_file_with(&info, &[], Some(&layers))).is_err());
    }

    #[test]
    fn rejects_malformed_v2_info() {
        let files = files();
        let good = fixtures::info("v2", &files, P, false);
        assert!(parse_torrent(&crate::metadata::build_torrent_file(&good, &[])).is_ok());
        let with = |from: &str, to: &str| {
            let text = good.clone();
            let at = text.windows(from.len()).position(|w| w == from.as_bytes()).expect(from);
            let mut info = text[..at].to_vec();
            info.extend_from_slice(to.as_bytes());
            info.extend_from_slice(&text[at + from.len()..]);
            parse_torrent(&crate::metadata::build_torrent_file(&info, &[]))
        };
        let newer = with("12:meta versioni2e", "12:meta versioni3e").unwrap_err();
        assert!(format!("{newer:#}").contains("newer"), "{newer:#}");
        assert!(with("12:meta versioni2e", "12:meta version1:2").is_err());
        assert!(
            with("12:piece lengthi32768e", "12:piece lengthi30000e").is_err(),
            "not a power of two"
        );
        assert!(
            with("12:piece lengthi32768e", "12:piece lengthi8192e").is_err(),
            "under 16 KiB"
        );
        assert!(with("6:lengthi40000e", "6:lengthi-40000e").is_err(), "negative length");
        assert!(with("6:lengthi40000e", "6:length1:x").is_err(), "length not an integer");
        assert!(with("11:pieces root32:", "11:pieces root31:").is_err(), "short root");
        assert!(with("1:ad0:d", "1:ad1:xde0:d").is_err(), "a file with a sibling");
        assert!(with("1:ad0:d", "0:d0:d").is_err(), "an empty name");
        assert!(with("1:ad0:d", "2:..d0:d").is_err(), "a parent-directory name");
        assert!(with("1:bd1:cd", "1:bi5e1:cd").is_err(), "a directory that's not a dict");
        assert!(
            with("9:file treed", "9:file treel").is_err(),
            "a tree that's not a dict"
        );
        // a file without a root, by renaming the key
        assert!(with("11:pieces root", "11:pieces ruut").is_err());
        assert!(with("9:file tree", "9:file_tree").is_err(), "no tree");
        assert!(
            with(
                "9:file treed1:ad",
                "9:file treed0:d6:lengthi1e11:pieces root32:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaae1:ad"
            )
            .is_err(),
            "the root is a file"
        );

        let mut deep = b"d9:file tree".to_vec();
        deep.extend(std::iter::repeat_n(&b"d1:x"[..], 200).flatten());
        deep.extend_from_slice(b"d0:d6:lengthi1e11:pieces root32:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaee");
        deep.extend(std::iter::repeat_n(b'e', 201));
        deep.extend_from_slice(b"12:meta versioni2e4:name1:x12:piece lengthi16384ee");
        assert!(
            parse_torrent(&crate::metadata::build_torrent_file(&deep, &[])).is_err(),
            "too deep"
        );

        let empty = b"d9:file treede12:meta versioni2e4:name1:x12:piece lengthi16384ee";
        assert!(
            parse_torrent(&crate::metadata::build_torrent_file(empty, &[])).is_err(),
            "no files"
        );
    }

    #[test]
    fn a_hybrid_carries_both_hashes() {
        let files = files();
        let t = parse_torrent(&fixtures::torrent_file("hy", &files, P, true)).unwrap();
        assert!(!t.v2_only());
        assert_eq!(t.files.len(), 4, "the v1 list's padding");
        assert!(t.attrs[1].pad);
        let v2 = t.v2.as_ref().expect("consistent");
        assert_eq!(v2.roots[0], Some(fixtures::root(&files[0].1)));
        assert_eq!(v2.roots[3], Some(fixtures::root(&files[2].1)));
        assert_eq!(t.hybrid_v2_hash().unwrap().as_bytes(), &v2.info_hash[..20]);
        let stream = stream(&files);
        assert!(t.valid_piece(1, &stream[P..2 * P]), "checked by SHA-1");
    }

    /// A resume file keeps whichever layers had come from peers when it was written; the
    /// others are still to fetch. A wrong one is still an error.
    #[test]
    fn some_piece_layers_are_enough() {
        let files = files();
        let info = fixtures::info("v2", &files, P, false);
        let mut layers = b"d".to_vec();
        fixtures::bstr(&mut layers, &fixtures::root(&files[0].1));
        fixtures::bstr(&mut layers, fixtures::layer(&files[0].1, P).as_flattened());
        layers.push(b'e');
        let t = parse_torrent(&crate::metadata::build_torrent_file_with(&info, &[], Some(&layers))).unwrap();
        assert_eq!(
            t.missing_layers(),
            [t.file_with_root(&fixtures::root(&files[2].1)).unwrap()]
        );

        let wrong = layers.len() - 2;
        layers[wrong] ^= 1;
        assert!(parse_torrent(&crate::metadata::build_torrent_file_with(&info, &[], Some(&layers))).is_err());
    }

    /// A hybrid whose halves disagree: here the v1 hashes describe one `d` and the file tree
    /// another. Data that passes SHA-1 is good (nothing else could pass it), so the piece is
    /// taken, and the torrent's v2 hashes stop counting.
    #[test]
    fn an_inconsistent_hybrid_trusts_sha1() {
        let files = files();
        let mut other = files.clone();
        other[2].1[40_000] ^= 1;
        let v1 = fixtures::info("hy", &files, P, true);
        let v2 = fixtures::info("hy", &other, P, true);
        let pieces = |info: &[u8]| info.windows(8).position(|w| w == b"6:pieces").unwrap();
        let mut info = v2[..pieces(&v2)].to_vec();
        info.extend_from_slice(&v1[pieces(&v1)..]);
        let layers = fixtures::piece_layers(&other, P);
        let t = parse_torrent(&crate::metadata::build_torrent_file_with(&info, &[], Some(&layers))).unwrap();
        assert!(t.v2.is_some() && !t.v2_only());

        let (ours, theirs) = (stream(&files), stream(&other));
        let piece = |data: &[u8], i: usize| data[i * P..((i + 1) * P).min(data.len())].to_vec();
        for i in [0, 1, 2, 4] {
            assert!(t.valid_piece(i as u32, &piece(&ours, i)), "piece {i}");
        }
        assert!(
            !t.valid_piece(3, &piece(&theirs, 3)),
            "the Merkle tree agrees, SHA-1 doesn't"
        );
        assert!(t.v2_consistent());
        assert!(
            t.valid_piece(3, &piece(&ours, 3)),
            "SHA-1 agrees, the Merkle tree doesn't"
        );
        assert!(
            !t.v2_consistent() && !t.clone().v2_consistent(),
            "flagged, for clones too"
        );
        assert!(!t.valid_piece(3, &piece(&theirs, 3)), "still SHA-1's call");

        // without the piece layers (as from a magnet), only SHA-1 can tell
        let bare = parse_torrent(&crate::metadata::build_torrent_file(&info, &[])).unwrap();
        assert!(bare.valid_piece(3, &piece(&ours, 3)));
        assert!(!bare.valid_piece(3, &piece(&theirs, 3)));
    }

    #[test]
    fn an_inconsistent_hybrid_falls_back_to_v1() {
        let files = files();
        let info = fixtures::info("hy", &files, P, true);
        // the tree's "a" becomes "0": the names no longer agree with the v1 list
        let at = info.windows(7).position(|w| w == b"1:ad0:d").unwrap();
        let mut renamed = info.clone();
        renamed[at + 2] = b'0';
        let t = parse_torrent(&crate::metadata::build_torrent_file(&renamed, &[])).unwrap();
        assert!(t.v2.is_none());
        assert_eq!(t.info_hash.as_bytes(), Sha1::digest(&renamed).as_slice());

        // unpadded, the second file doesn't start on a piece boundary
        let tree: Vec<TreeFile> = files
            .iter()
            .filter(|(_, d)| !d.is_empty())
            .map(|(path, d)| TreeFile {
                len: d.len() as u64,
                path: path.iter().map(|s| s.to_string()).collect(),
                root: Some(fixtures::root(d)),
                attr: FileAttr::default(),
            })
            .collect();
        let unpadded: Layout = (
            vec![(40_000, "x/a".into()), (70_000, "x/d".into())],
            vec![vec!["x".into(), "a".into()], vec!["x".into(), "d".into()]],
            vec![FileAttr::default(); 2],
        );
        let err = hybrid_roots(&unpadded, &tree, P as u64).unwrap_err();
        assert!(format!("{err}").contains("piece boundary"), "{err}");
    }

    /// BEP 47 in a v1 torrent: `a`, 6 bytes of padding (without a path), then `b`, in 16-byte
    /// pieces; and BitComet's padding, which predates the attribute.
    #[test]
    fn parses_padding_and_attributes() {
        let info = b"d5:filesld4:attr1:x6:lengthi10e4:pathl1:aeed4:attr1:p6:lengthi6eed6:lengthi10e4:pathl1:beed4:attr2:hl6:lengthi0e4:pathl4:linke12:symlink pathl1:beee4:name1:t12:piece lengthi16e6:pieces40:0123456789012345678901234567890123456789e";
        let t = parse_torrent(&crate::metadata::build_torrent_file(info, &[])).unwrap();
        assert_eq!(t.files[1], (6, PathBuf::from("t/.pad/6")));
        assert!(t.attrs[0].executable && !t.attrs[0].pad);
        assert!(t.attrs[1].pad && t.attrs[1].virtual_file());
        assert!(t.attrs[3].hidden);
        assert_eq!(t.attrs[3].symlink, Some(PathBuf::from("b")));
        assert!(t.attrs[3].virtual_file());
        assert_eq!(t.padding_in_piece(0), [Range { start: 10, end: 16 }]);
        assert!(t.padding_in_piece(1).is_empty());
        // only the pieces of selected real files are wanted, never padding's
        assert_eq!(
            t.wanted_pieces(&[true, true, false, true])
                .iter()
                .by_vals()
                .collect::<Vec<_>>(),
            [true, false]
        );
        assert_eq!(
            t.wanted_pieces(&[false, true, true, true])
                .iter()
                .by_vals()
                .collect::<Vec<_>>(),
            [false, true]
        );

        let bitcomet = b"d5:filesld6:lengthi10e4:pathl1:aeed6:lengthi6e4:pathl29:_____padding_file_0_if you seeed6:lengthi10e4:pathl1:beee4:name1:t12:piece lengthi16e6:pieces40:0123456789012345678901234567890123456789e";
        let t = parse_torrent(&crate::metadata::build_torrent_file(bitcomet, &[])).unwrap();
        assert!(t.attrs[1].pad);

        let escaping =
            b"d5:filesld4:attr1:l6:lengthi0e4:pathl1:ae12:symlink pathl2:..eee4:name1:t12:piece lengthi16e6:pieces0:e";
        assert!(parse_torrent(&crate::metadata::build_torrent_file(escaping, &[])).is_err());
    }
}
