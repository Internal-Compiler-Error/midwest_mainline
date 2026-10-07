use super::parse::{Layout, TreeFile, hybrid_roots, safe_path_component};
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
        parse_torrent(&multi_file_torrent(&[(5, "a"), (5, "b")], 4, 0)).is_err(),
        "no hashes isn't v2"
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

const LIBTORRENT_V2: &[u8] = include_bytes!("../../testdata/bittorrent-v2-test.torrent");
const LIBTORRENT_HYBRID: &[u8] = include_bytes!("../../testdata/bittorrent-v2-hybrid-test.torrent");

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
    let unpadded = Layout {
        files: vec![(40_000, "x/a".into()), (70_000, "x/d".into())],
        raw_paths: vec![vec!["x".into(), "a".into()], vec!["x".into(), "d".into()]],
        attrs: vec![FileAttr::default(); 2],
    };
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
