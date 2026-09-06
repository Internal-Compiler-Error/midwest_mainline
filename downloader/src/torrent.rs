use anyhow::{anyhow, bail};
use bendy::decoding::Object;
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use sha1::{Digest, Sha1};
use std::path::{Component, Path, PathBuf};
use std::str;

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

/// Represents a parsed torrent metadata file
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Torrent {
    /// List of tracker tiers, where each tier contains multiple tracker URLs
    /// Trackers in the same tier should be tried in parallel
    pub announce_tiers: Vec<Vec<String>>,

    /// Number of bytes for each piece, barring the last one
    pub piece_size: u32,

    /// Hash of each piece
    pub pieces: Vec<[u8; 20]>,

    /// Files in the torrent: (size in bytes, path relative to the download root), always
    /// starting with `name` -- the single file's name, or the directory holding them all
    pub files: Vec<(u32, PathBuf)>,

    /// Total size of all files combined in bytes
    pub total_size: u64,

    /// Size of the last piece in bytes
    pub last_piece_size: u32,

    /// The info dict's `name`: the single file's name, or the directory everything lives under
    pub name: String,

    /// Info hash of the torrent
    pub info_hash: InfoHash,

    /// The raw bencoded bytes of the "info" dict, exactly as they appeared in the .torrent
    /// file. Kept around so we can serve it to peers over BEP 9 (ut_metadata) -- we always
    /// have the full metadata already, having started from a .torrent file rather than a
    /// magnet link.
    pub raw_info: Vec<u8>,

    /// BEP 27: if set, peers for this torrent must only come from the trackers named in this
    /// torrent -- no DHT, no PEX. We don't implement DHT, but we do implement PEX, so this has
    /// to actually gate something.
    pub private: bool,
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

    /// Validates that a piece matches its expected hash
    pub fn valid_piece(&self, piece: u32, data: &[u8]) -> bool {
        let expected_hash = self.pieces[piece as usize];
        let got = Sha1::digest(data);
        &*got == &expected_hash
    }

    /// Returns the size of the ith piece in bytes
    pub fn nth_piece_size<T: Into<u64>>(&self, i: T) -> Option<usize> {
        let i = i.into();
        if i >= self.pieces.len().try_into().unwrap() {
            return None;
        }

        let last_piece_index = (self.pieces.len() - 1).try_into().unwrap();
        if i == last_piece_index {
            Some(self.last_piece_size.try_into().unwrap())
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

/// Parses a torrent metadata file and returns a Torrent struct
pub fn parse_torrent(metadata_file: &[u8]) -> anyhow::Result<Torrent> {
    let (hash, raw_info) = compute_info_hash(metadata_file);

    let (_, mut torrent) = juicy_bencode::parse_bencode_dict(metadata_file).map_err(|_| {
        // the error type has a reference on the input, we don't want that
        anyhow!("not a valid dict")
    })?;

    let BencodeItemView::Dictionary(mut info) = torrent.remove(b"info".as_slice()).unwrap() else {
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

    let BencodeItemView::ByteString(name) = info.remove(b"name".as_slice()).unwrap() else {
        bail!("name needs to be a string");
    };
    let name = str::from_utf8(name)?.to_string();
    // paths are relative to a download root the caller chooses per torrent (see
    // `BtClient::add_torrent`); this only decides the layout under it
    let root = safe_path_component(&name)?;

    let BencodeItemView::Integer(piece_len) = info.remove(b"piece length".as_slice()).unwrap() else {
        bail!("piece length needs to be an integer");
    };
    let piece_len: u32 = piece_len.try_into()?;

    let BencodeItemView::ByteString(pieces) = info.remove(b"pieces".as_slice()).unwrap() else {
        bail!("pieces needs to be a byte string");
    };

    // BEP 27: absent means not private; some encoders write `0` explicitly rather than
    // omitting the key, so treat any non-1 value the same as absent instead of erroring.
    let private = matches!(info.remove(b"private".as_slice()), Some(BencodeItemView::Integer(1)));

    let mut files = vec![];

    if let Some(BencodeItemView::Integer(length)) = info.remove(b"length".as_slice()) {
        files.push((length as u32, root));
    } else if let Some(BencodeItemView::List(file_lists)) = info.remove(b"files".as_slice()) {
        let mut file_lists = file_lists.iter();
        while let Some(BencodeItemView::Dictionary(entries)) = file_lists.next() {
            let BencodeItemView::Integer(length) = entries.get(b"length".as_slice()).unwrap() else {
                bail!("file length needs to be an integer");
            };
            let BencodeItemView::List(paths) = entries.get(b"path".as_slice()).unwrap() else {
                bail!("file path needs to be a list");
            };
            let mut f = root.clone();
            let mut paths = paths.iter();
            while let Some(BencodeItemView::ByteString(p)) = paths.next() {
                f.push(safe_path_component(str::from_utf8(p)?)?);
            }

            files.push((*length as u32, f));
        }
    }

    let pieces = pieces.chunks(20).map(|e| e.try_into().unwrap()).collect();
    let total_size = files.iter().map(|(len, _f)| *len as u64).sum();
    let piece_len: u64 = piece_len.try_into()?;
    // an evenly-divisible torrent has a "remainder" of 0, but the last piece is still
    // full-sized in that case
    let last_piece_len = match total_size % piece_len {
        0 => piece_len,
        remainder => remainder,
    };

    Ok(Torrent {
        announce_tiers,
        piece_size: piece_len as u32,
        pieces,
        total_size,
        files,
        last_piece_size: last_piece_len.try_into()?,
        name,
        info_hash: hash,
        raw_info,
        private,
    })
}

/// Computes the info hash of a torrent metadata file, and also returns the raw bencoded bytes
/// of the "info" dict (needed to serve BEP 9 ut_metadata requests).
fn compute_info_hash(input: &[u8]) -> (InfoHash, Vec<u8>) {
    let mut decoder = bendy::decoding::Decoder::new(input);
    let Some(Object::Dict(mut dict)) = decoder.next_object().unwrap() else {
        panic!("torrent metadata must be a dictionary")
    };

    while let Some((key, val)) = dict.next_pair().unwrap() {
        if key == b"info" {
            let buf = match val {
                Object::List(list_decoder) => list_decoder.into_raw().unwrap(),
                Object::Dict(dict_decoder) => dict_decoder.into_raw().unwrap(),
                Object::Integer(i) => i.as_bytes(),
                Object::Bytes(items) => items,
            };
            let hash = InfoHash::from_bytes(Sha1::digest(buf).as_slice());
            return (hash, buf.to_vec());
        }
    }

    panic!("torrent metadata must contain 'info' key")
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
}
