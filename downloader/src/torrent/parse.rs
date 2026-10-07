//! .torrent files into `Torrent`s: v1's file list, v2's file tree (BEP 52) and the agreement
//! of the two in a hybrid, BEP 47's padding and attributes.
//!
//! Everything in a .torrent is whoever made it's to choose, so nothing here trusts it: paths
//! are made safe, lengths and counts checked against each other, and a malformed file is an
//! error, never a panic.

use super::{FileAttr, Torrent, V2};
use crate::merkle::{self, Hash};
use anyhow::{anyhow, bail};
use bendy::decoding::Object;
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::str;
use std::sync::{Arc, OnceLock};

/// Largest piece length we accept. BEP 3 sets no limit, but real torrents stay at or under
/// 16 MiB, and a whole piece is buffered in memory while it downloads.
const MAX_PIECE_SIZE: u32 = 64 << 20;

/// How deep a v2 `file tree` may nest; real ones are a handful of levels.
const MAX_TREE_DEPTH: usize = 64;

type Dict<'a> = BTreeMap<&'a [u8], BencodeItemView<'a>>;

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
pub(super) fn safe_path_component(raw: &str) -> anyhow::Result<PathBuf> {
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

/// BEP 47 attributes, from a v1 `files` entry (or a single-file info dict) or a v2 file's
/// properties.
fn file_attr(dict: &Dict) -> anyhow::Result<FileAttr> {
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

/// A file's `length`, which BEP 47 lets a symlink leave out.
fn file_len(dict: &Dict, attr: &FileAttr) -> anyhow::Result<u64> {
    match dict.get(b"length".as_slice()) {
        Some(BencodeItemView::Integer(len)) => {
            u64::try_from(*len).map_err(|_| anyhow!("file length {len} is negative"))
        }
        None if attr.symlink.is_some() => Ok(0),
        _ => bail!("file length needs to be an integer"),
    }
}

/// The files of a torrent laid end to end as its piece stream, padding included.
#[derive(Debug, Default)]
pub(super) struct Layout {
    /// (length, path below the download root)
    pub files: Vec<(u64, PathBuf)>,
    /// each path as the info dict spells it, `name` first
    pub raw_paths: Vec<Vec<String>>,
    pub attrs: Vec<FileAttr>,
}

impl Layout {
    fn push(&mut self, len: u64, path: PathBuf, raw: Vec<String>, attr: FileAttr) {
        self.files.push((len, path));
        self.raw_paths.push(raw);
        self.attrs.push(attr);
    }

    /// Padding of `len` bytes, which a v1 list may give without a path and a v2 layout
    /// synthesises.
    fn push_padding(&mut self, len: u64, name: &str, root: &Path) {
        let attr = FileAttr {
            pad: true,
            ..Default::default()
        };
        let raw = vec![name.to_string(), ".pad".to_string(), len.to_string()];
        self.push(len, root.join(".pad").join(len.to_string()), raw, attr);
    }
}

/// The v1 file list: the single file `length` describes, or the `files` list.
fn v1_layout(info: &mut Dict, name: &str, root: &Path) -> anyhow::Result<Layout> {
    let mut layout = Layout::default();
    if let Some(BencodeItemView::Integer(length)) = info.remove(b"length".as_slice()) {
        let len = u64::try_from(length).map_err(|_| anyhow!("file length {length} is negative"))?;
        layout.push(len, root.to_path_buf(), vec![name.to_string()], file_attr(info)?);
        return Ok(layout);
    }
    let Some(BencodeItemView::List(entries)) = info.remove(b"files".as_slice()) else {
        return Ok(layout);
    };
    for entry in &entries {
        let BencodeItemView::Dictionary(entry) = entry else {
            bail!("file entry needs to be a dict");
        };
        let mut attr = file_attr(entry)?;
        let len = file_len(entry, &attr)?;
        let mut path = root.to_path_buf();
        let mut raw = vec![name.to_string()];
        match entry.get(b"path".as_slice()) {
            Some(BencodeItemView::List(segments)) if !segments.is_empty() => {
                for segment in segments {
                    let BencodeItemView::ByteString(segment) = segment else {
                        bail!("file path segment needs to be a string");
                    };
                    let segment = str::from_utf8(segment)?;
                    path.push(safe_path_component(segment)?);
                    raw.push(segment.to_string());
                }
            }
            // BEP 47: a padding file needn't have a path
            None if attr.pad => {
                layout.push_padding(len, name, root);
                continue;
            }
            Some(BencodeItemView::List(_)) => bail!("file path is empty"),
            _ => bail!("file path needs to be a list"),
        }
        // BitComet's padding predates BEP 47's attribute
        if raw.last().is_some_and(|last| last.starts_with("_____padding_file_")) {
            attr.pad = true;
        }
        layout.push(len, path, raw, attr);
    }
    Ok(layout)
}

/// One file of a v2 `file tree`.
pub(super) struct TreeFile {
    pub len: u64,
    /// below the torrent's top level
    pub path: Vec<String>,
    pub root: Option<Hash>,
    pub attr: FileAttr,
}

/// Walks a `file tree` dict depth first, in key order (which is the piece order).
fn walk_file_tree(dir: &Dict, path: &mut Vec<String>, out: &mut Vec<TreeFile>) -> anyhow::Result<()> {
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
            let attr = file_attr(props)?;
            let len = file_len(props, &attr)?;
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

/// Lays out a v2 `file tree` as a piece stream: each file not ending on a piece boundary is
/// followed by padding, but for the last. Returns each file's root alongside.
fn v2_layout(tree: &[TreeFile], name: &str, root: &Path, piece: u64) -> anyhow::Result<(Layout, Vec<Option<Hash>>)> {
    let mut layout = Layout::default();
    let mut roots = vec![];
    // a lone file at the top is the torrent itself, not a directory holding it
    let single = tree.len() == 1 && tree[0].path.len() == 1;
    let last_data = tree.iter().rposition(|f| f.len > 0);
    for (i, file) in tree.iter().enumerate() {
        if single {
            let path = safe_path_component(&file.path[0])?;
            layout.push(file.len, path, file.path.clone(), file.attr.clone());
        } else {
            let mut path = root.to_path_buf();
            for segment in &file.path {
                path.push(safe_path_component(segment)?);
            }
            let raw = [name.to_string()]
                .into_iter()
                .chain(file.path.iter().cloned())
                .collect();
            layout.push(file.len, path, raw, file.attr.clone());
        }
        roots.push(file.root);
        let gap = (piece - file.len % piece) % piece;
        if gap > 0 && Some(i) != last_data {
            layout.push_padding(gap, name, root);
            roots.push(None);
        }
    }
    Ok((layout, roots))
}

/// For a hybrid: the v2 root of each v1 file, if the two describe the same files in the same
/// order and every file starts on a piece boundary, as BEP 52 requires of a hybrid.
pub(super) fn hybrid_roots(v1: &Layout, tree: &[TreeFile], piece: u64) -> anyhow::Result<Vec<Option<Hash>>> {
    let mut roots = vec![None; v1.files.len()];
    let mut tree = tree.iter();
    let mut offset = 0u64;
    for (i, &(len, _)) in v1.files.iter().enumerate() {
        if !v1.attrs[i].pad {
            let raw = &v1.raw_paths[i];
            let Some(file) = tree.next() else {
                bail!("v1 lists more files than the file tree");
            };
            let same_path = raw.len() == 1 || raw[1..] == file.path[..];
            if file.len != len || !same_path {
                bail!("v1 file {raw:?} isn't the file tree's {:?}", file.path);
            }
            if len > 0 && !offset.is_multiple_of(piece) {
                bail!("{raw:?} doesn't start on a piece boundary");
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

/// What the info dict says, before it's laid out as a `Torrent`.
struct Info<'a> {
    name: String,
    piece_len: u64,
    /// the v1 piece hashes, which a v2-only torrent has none of
    pieces: Option<&'a [u8]>,
    private: bool,
    layout: Layout,
    /// each file's `pieces root`, for a v2 torrent or a hybrid whose halves agree
    roots: Option<Vec<Option<Hash>>>,
}

fn parse_info<'a>(mut info: Dict<'a>) -> anyhow::Result<Info<'a>> {
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
        Some(BencodeItemView::ByteString(pieces)) if pieces.len() % 20 == 0 => Some(pieces),
        Some(BencodeItemView::ByteString(pieces)) => bail!("pieces is {} bytes, not a multiple of 20", pieces.len()),
        None if v2 => None,
        _ => bail!("pieces needs to be a byte string"),
    };

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

    let (layout, roots) = match (&tree, pieces) {
        (Some(tree), None) => {
            let (layout, roots) = v2_layout(tree, &name, &root, piece_len)?;
            (layout, Some(roots))
        }
        (tree, _) => {
            let layout = v1_layout(&mut info, &name, &root)?;
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
    Ok(Info {
        name,
        piece_len,
        pieces,
        private,
        layout,
        roots,
    })
}

/// The tracker tiers of `announce-list`, or `announce` alone. Neither is required: a torrent
/// built from a tracker-less magnet finds its peers over the DHT.
fn announce_tiers(torrent: &mut Dict) -> anyhow::Result<Vec<Vec<String>>> {
    let mut tiers = vec![];
    if let Some(BencodeItemView::List(list)) = torrent.remove(b"announce-list".as_slice()) {
        for tier in list.iter().map_while(|tier| match tier {
            BencodeItemView::List(tier) => Some(tier),
            _ => None,
        }) {
            let urls: Vec<String> = tier
                .iter()
                .map_while(|url| match url {
                    BencodeItemView::ByteString(url) => Some(*url),
                    _ => None,
                })
                .filter_map(|url| String::from_utf8(url.to_vec()).ok())
                .collect();
            if !urls.is_empty() {
                tiers.push(urls);
            }
        }
    }
    if tiers.is_empty()
        && let Some(BencodeItemView::ByteString(announce)) = torrent.remove(b"announce".as_slice())
    {
        tiers.push(vec![String::from_utf8(announce.to_vec())?]);
    }
    Ok(tiers)
}

/// Parses a .torrent file.
pub fn parse_torrent(metadata_file: &[u8]) -> anyhow::Result<Torrent> {
    let (v1_hash, raw_info) = raw_info_dict(metadata_file)?;

    // the error type has a reference on the input, we don't want that
    let (_, mut torrent) = juicy_bencode::parse_bencode_dict(metadata_file).map_err(|_| anyhow!("not a valid dict"))?;
    let Some(BencodeItemView::Dictionary(info)) = torrent.remove(b"info".as_slice()) else {
        bail!("info needs to be a dict");
    };
    let announce_tiers = announce_tiers(&mut torrent)?;
    let web_seeds = match torrent.remove(b"url-list".as_slice()) {
        Some(BencodeItemView::ByteString(url)) => web_seed_urls([url]),
        Some(BencodeItemView::List(urls)) => web_seed_urls(urls.iter().filter_map(|url| match url {
            BencodeItemView::ByteString(url) => Some(*url),
            _ => None,
        })),
        _ => vec![],
    };
    let Info {
        name,
        piece_len,
        pieces,
        private,
        layout,
        roots,
    } = parse_info(info)?;

    let Layout {
        files,
        raw_paths,
        attrs,
    } = layout;
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
        Some(pieces) if (pieces.len() / 20) as u64 != num_pieces => bail!(
            "{} piece hashes for {total_size} bytes in pieces of {piece_len}",
            pieces.len() / 20
        ),
        Some(pieces) => pieces.as_chunks::<20>().0.to_vec(),
        None => vec![],
    };
    if u32::try_from(num_pieces).is_err() {
        bail!("too many pieces");
    }
    let offsets = files
        .iter()
        .scan(0u64, |at, (len, _)| {
            let start = *at;
            *at += len;
            Some(start)
        })
        .collect();

    let v2 = roots.map(|roots| V2 {
        info_hash: Sha256::digest(&raw_info).into(),
        layers: (0..roots.len()).map(|_| OnceLock::new()).collect(),
        roots,
        inconsistent: Arc::default(),
    });
    let info_hash = match &v2 {
        Some(v2) if pieces.is_empty() => InfoHash::from_bytes(&v2.info_hash[..20]),
        _ => v1_hash,
    };

    let parsed = Torrent {
        announce_tiers,
        piece_size: piece_len as u32,
        pieces,
        total_size,
        files,
        attrs,
        // an evenly-divisible torrent's last piece is a whole one, not an empty one
        last_piece_size: (total_size - (num_pieces - 1) * piece_len) as u32,
        name,
        info_hash,
        v2,
        raw_info,
        private,
        web_seeds,
        raw_paths,
        offsets,
        num_pieces: num_pieces as usize,
    };
    if parsed.v2.is_some()
        && let Some(layers) = torrent.remove(b"piece layers".as_slice())
    {
        take_piece_layers(&parsed, &layers)?;
    }
    Ok(parsed)
}

/// BEP 52's `piece layers`: outside the info dict, so not covered by the info hash, but each
/// must roll up to its file's root. A torrent rebuilt from a magnet's metadata has none, and
/// its resume file those that came before it stopped: peers send the rest.
fn take_piece_layers(torrent: &Torrent, layers: &BencodeItemView) -> anyhow::Result<()> {
    let BencodeItemView::Dictionary(layers) = layers else {
        bail!("piece layers needs to be a dict");
    };
    let roots = &torrent.v2.as_ref().expect("a v2 torrent").roots;
    for file in torrent.missing_layers() {
        let root = roots[file].expect("missing_layers has roots");
        let layer = match layers.get(root.as_slice()) {
            Some(BencodeItemView::ByteString(layer)) => layer,
            None => continue,
            Some(_) => bail!("the piece layer of {:?} needs to be a string", torrent.files[file].1),
        };
        let (hashes, rest) = layer.as_chunks::<32>();
        if !rest.is_empty() || !torrent.set_layer(file, hashes.to_vec()) {
            bail!(
                "the piece layer of {:?} doesn't match its pieces root",
                torrent.files[file].1
            );
        }
    }
    Ok(())
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

/// The raw bencoded bytes of the .torrent's "info" dict, exactly as they appear in it (the
/// info hashes are taken over them, and BEP 9 serves them), and their SHA-1.
fn raw_info_dict(input: &[u8]) -> anyhow::Result<(InfoHash, Vec<u8>)> {
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
