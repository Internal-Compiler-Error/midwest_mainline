//! Hand-built v2 and hybrid torrents for tests, straight from BEP 52's definitions.

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

/// The piece stream of `files` in pieces of `piece` bytes: each file padded to a piece
/// boundary but the last.
pub fn stream(files: &[(&[&str], Vec<u8>)], piece: usize) -> Vec<u8> {
    let mut out = vec![];
    let last = files.iter().rposition(|(_, d)| !d.is_empty()).unwrap();
    for (i, (_, d)) in files.iter().enumerate() {
        out.extend_from_slice(d);
        if i != last {
            out.resize(out.len().next_multiple_of(piece), 0);
        }
    }
    out
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
