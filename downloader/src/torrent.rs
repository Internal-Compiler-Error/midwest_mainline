use anyhow::{anyhow, bail};
use bendy::decoding::Object;
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use sha1::{Digest, Sha1};
use std::path::PathBuf;
use std::str;

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

    /// Files in the torrent: (size in bytes, file path)
    pub files: Vec<(u32, PathBuf)>,

    /// Total size of all files combined in bytes
    pub total_size: u64,

    /// Size of the last piece in bytes
    pub last_piece_size: u32,

    /// Info hash of the torrent
    pub info_hash: InfoHash,
}

impl Torrent {
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
}

/// Parses a torrent metadata file and returns a Torrent struct
pub fn parse_torrent(metadata_file: &[u8]) -> anyhow::Result<Torrent> {
    let hash = compute_info_hash(metadata_file);

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

    // Fall back to single announce if announce-list is not present or empty
    if announce_tiers.is_empty() {
        if let Some(BencodeItemView::ByteString(announce)) = torrent.remove(b"announce".as_slice()) {
            let announce_str = String::from_utf8(announce.to_vec())?;
            announce_tiers.push(vec![announce_str]);
        } else {
            bail!("torrent must have either 'announce' or 'announce-list'");
        }
    }

    let BencodeItemView::ByteString(name) = info.remove(b"name".as_slice()).unwrap() else {
        bail!("name needs to be a string");
    };
    let mut root = PathBuf::new();
    // TODO: make this configurable
    root.push("./");
    root.push(str::from_utf8(name)?);

    let BencodeItemView::Integer(piece_len) = info.remove(b"piece length".as_slice()).unwrap() else {
        bail!("piece length needs to be an integer");
    };
    let piece_len: u32 = piece_len.try_into()?;

    let BencodeItemView::ByteString(pieces) = info.remove(b"pieces".as_slice()).unwrap() else {
        bail!("pieces needs to be a byte string");
    };

    let mut files = vec![];

    // TODO: we expect each `Torrent` object's `files` to contain file paths that already exists,
    // need to make sure they do
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
                f.push(str::from_utf8(p)?);
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
        info_hash: hash,
    })
}

/// Computes the info hash of a torrent metadata file
fn compute_info_hash(input: &[u8]) -> InfoHash {
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
            return InfoHash::from_bytes(Sha1::digest(buf).as_slice());
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
        info.extend_from_slice(&bencode_string(b"test.txt"));
        info.extend_from_slice(&bencode_string(b"piece length"));
        info.extend_from_slice(format!("i{piece_length}e").as_bytes());
        info.extend_from_slice(&bencode_string(b"pieces"));
        info.extend_from_slice(&bencode_string(&pieces));
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
}
