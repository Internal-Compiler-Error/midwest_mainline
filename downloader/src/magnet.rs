//! Magnet URI parsing (BEP 9's "magnet-uri" form).
//!
//! Only what's needed to start a download without a `.torrent` file: the info hash, the
//! display name, and the tracker list. A magnet with no trackers is fine: the DHT finds its
//! peers.

use anyhow::{bail, ensure};
use midwest_mainline::types::InfoHash;
use std::net::SocketAddr;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MagnetLink {
    /// what the swarm goes by: the `btih` hash, or for a v2-only magnet the `btmh` one
    /// truncated (BEP 52)
    pub info_hash: InfoHash,
    /// BEP 52 `xt=urn:btmh:1220<hex>`: the full SHA-256 info hash, which the metadata from
    /// peers is checked against
    pub info_hash_v2: Option<[u8; 32]>,
    /// the `dn` param -- a *hint* only. The real name comes from the metadata we fetch, since
    /// this one is attacker-controlled and never hash-verified.
    pub display_name: Option<String>,
    pub trackers: Vec<String>,
    /// BEP 19 `ws=` web seeds; they join once the metadata has come from peers
    pub web_seeds: Vec<String>,
    /// `x.pe=`: peers to try first, before any tracker or the DHT has answered
    pub peers: Vec<SocketAddr>,
    /// BEP 53 `so=`: the indices of the files to download; `None` for all of them
    pub select_only: Option<Vec<usize>>,
}

impl MagnetLink {
    /// The file selection `so=` asks for, for a torrent of `files` files: all of them without
    /// `so=`, or when none of its indices exists.
    pub fn selection(&self, files: usize) -> Vec<bool> {
        let picked: Vec<bool> = (0..files)
            .map(|i| self.select_only.as_ref().is_none_or(|only| only.contains(&i)))
            .collect();
        if picked.contains(&true) {
            picked
        } else {
            vec![true; files]
        }
    }
}

/// BEP 53: comma-separated indices and inclusive ranges, `0,2,4,6-8`. Malformed parts are
/// skipped; a range is capped so a hostile `0-4294967295` can't allocate the world.
fn parse_select_only(value: &str) -> Vec<usize> {
    const MAX_RANGE: usize = 1 << 20;
    let mut out = Vec::new();
    for part in value.split(',') {
        match part.split_once('-') {
            Some((from, to)) => {
                if let (Ok(from), Ok(to)) = (from.trim().parse::<usize>(), to.trim().parse::<usize>())
                    && from <= to
                {
                    out.extend(from..=to.min(from + MAX_RANGE));
                }
            }
            None => out.extend(part.trim().parse::<usize>()),
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// True if `s` looks like a magnet URI, so callers can accept either this or a file path.
pub fn is_magnet_uri(s: &str) -> bool {
    s.trim_start().to_ascii_lowercase().starts_with("magnet:")
}

pub fn parse_magnet(uri: &str) -> anyhow::Result<MagnetLink> {
    let url = Url::parse(uri.trim())?;
    ensure!(url.scheme().eq_ignore_ascii_case("magnet"), "not a magnet URI");

    let mut info_hash = None;
    let mut info_hash_v2 = None;
    let mut display_name = None;
    let mut trackers = Vec::new();
    let mut web_seeds = Vec::new();
    let mut peers = Vec::new();
    let mut select_only: Option<Vec<usize>> = None;

    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            // a hybrid's magnet has both a v1 btih and a v2 btmh; the first of each counts
            "xt" => {
                if let Some(rest) = strip_prefix_ignore_ascii_case(&value, "urn:btih:")
                    && info_hash.is_none()
                {
                    info_hash = Some(parse_info_hash(rest)?);
                } else if let Some(rest) = strip_prefix_ignore_ascii_case(&value, "urn:btmh:")
                    && info_hash_v2.is_none()
                {
                    info_hash_v2 = Some(parse_multihash(rest)?);
                }
            }
            "dn" if display_name.is_none() => display_name = Some(value.into_owned()),
            "tr" => trackers.push(value.into_owned()),
            "ws" => web_seeds.push(value.into_owned()),
            // only address literals: a hostname here would have us resolve whatever the link says
            "x.pe" => peers.extend(value.parse::<SocketAddr>()),
            "so" => select_only.get_or_insert_default().extend(parse_select_only(&value)),
            _ => {}
        }
    }

    let Some(info_hash) = info_hash.or(info_hash_v2.map(|v2| InfoHash::from_bytes(&v2[..20]))) else {
        bail!("magnet URI has no `xt=urn:btih:` or `xt=urn:btmh:` info hash");
    };

    Ok(MagnetLink {
        info_hash,
        info_hash_v2,
        display_name,
        trackers,
        web_seeds: crate::torrent::web_seed_urls(web_seeds.iter().map(|u| u.as_bytes())),
        peers,
        select_only,
    })
}

fn strip_prefix_ignore_ascii_case<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let (head, rest) = s.split_at_checked(prefix.len())?;
    head.eq_ignore_ascii_case(prefix).then_some(rest)
}

/// A btih info hash is either 40 hex characters or 32 base32 ones; both decode to the same 20
/// raw bytes.
fn parse_info_hash(raw: &str) -> anyhow::Result<InfoHash> {
    let bytes = match raw.len() {
        40 => decode_hex::<20>(raw)?,
        32 => decode_base32(raw)?,
        n => bail!("info hash must be 40 hex or 32 base32 characters, got {n}"),
    };
    Ok(InfoHash::from_bytes(&bytes))
}

/// A btmh hash is a hex multihash; BEP 52 only has SHA-256 (code 0x12, 32 bytes long).
fn parse_multihash(raw: &str) -> anyhow::Result<[u8; 32]> {
    let Some(digest) = strip_prefix_ignore_ascii_case(raw, "1220") else {
        bail!("btmh info hash must be a SHA-256 multihash (1220...)");
    };
    ensure!(
        digest.len() == 64,
        "btmh info hash must have 64 hex digits, got {}",
        digest.len()
    );
    decode_hex(digest)
}

fn decode_hex<const N: usize>(s: &str) -> anyhow::Result<[u8; N]> {
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        let hi = hex_val(s.as_bytes()[i * 2])?;
        let lo = hex_val(s.as_bytes()[i * 2 + 1])?;
        *byte = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_val(c: u8) -> anyhow::Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => bail!("invalid hex character {:?} in info hash", c as char),
    }
}

/// RFC 4648 base32 (A-Z, 2-7), no padding. 32 characters is exactly 160 bits, so this only has
/// to handle the one length an info hash can be.
fn decode_base32(s: &str) -> anyhow::Result<[u8; 20]> {
    let mut out = [0u8; 20];
    let mut acc: u64 = 0;
    let mut bits = 0u32;
    let mut written = 0usize;

    for c in s.bytes() {
        let val = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a',
            b'2'..=b'7' => c - b'2' + 26,
            _ => bail!("invalid base32 character {:?} in info hash", c as char),
        };
        acc = (acc << 5) | val as u64;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out[written] = (acc >> bits) as u8;
            written += 1;
        }
    }

    ensure!(
        written == 20,
        "base32 info hash decoded to {written} bytes, expected 20"
    );
    Ok(out)
}

#[cfg(test)]
mod test {
    #[test]
    fn select_only_and_peer_addresses() {
        let hash = "f45add9d1a5185d8588df7dd6cd89993dd0174fa";
        let magnet = parse_magnet(&format!(
            "magnet:?xt=urn:btih:{hash}&so=0,2,4-6,x,9-7&x.pe=10.0.0.1:6881&x.pe=[::1]:7000&x.pe=host.test:1"
        ))
        .unwrap();
        assert_eq!(magnet.select_only, Some(vec![0, 2, 4, 5, 6]));
        assert_eq!(
            magnet.peers,
            [
                "10.0.0.1:6881".parse::<SocketAddr>().unwrap(),
                "[::1]:7000".parse().unwrap()
            ]
        );
        assert_eq!(magnet.selection(4), [true, false, true, false]);
        let none_exist = parse_magnet(&format!("magnet:?xt=urn:btih:{hash}&so=10")).unwrap();
        assert_eq!(
            none_exist.selection(2),
            [true, true],
            "nothing valid selected means everything"
        );
        let plain = parse_magnet(&format!("magnet:?xt=urn:btih:{hash}")).unwrap();
        assert_eq!(plain.selection(2), [true, true]);
        assert_eq!(
            parse_select_only("0-4294967295").len(),
            (1 << 20) + 1,
            "ranges are capped"
        );
    }

    use super::*;

    #[test]
    fn parses_btmh_magnets() {
        let v2 = "caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e";
        let only = parse_magnet(&format!("magnet:?xt=urn:btmh:1220{v2}&dn=v2")).unwrap();
        let full: Vec<u8> = (0..32)
            .map(|i| u8::from_str_radix(&v2[i * 2..i * 2 + 2], 16).unwrap())
            .collect();
        assert_eq!(only.info_hash_v2.unwrap().as_slice(), full);
        assert_eq!(
            only.info_hash.as_bytes(),
            &full[..20],
            "the swarm goes by the truncated hash"
        );

        let hybrid = parse_magnet(&format!(
            "magnet:?xt=urn:btmh:1220{v2}&xt=urn:btih:631a31dd0a46257d5078c0dee4e66e26f73e42ac"
        ))
        .unwrap();
        assert_eq!(hybrid.info_hash.as_bytes()[0], 0x63, "a hybrid goes by its v1 hash");
        assert!(hybrid.info_hash_v2.is_some());

        assert!(
            parse_magnet(&format!("magnet:?xt=urn:btmh:1114{v2}")).is_err(),
            "not sha2-256"
        );
        assert!(parse_magnet("magnet:?xt=urn:btmh:1220abcd").is_err(), "short");
        assert!(
            parse_magnet(&format!("magnet:?xt=urn:btmh:1220{}zz", &v2[2..])).is_err(),
            "not hex"
        );
    }

    const HEX: &str = "0123456789abcdef0123456789abcdef01234567";
    /// same 20 bytes as HEX, base32-encoded (RFC 4648, padding stripped)
    const BASE32: &str = "AERUKZ4JVPG66AJDIVTYTK6N54ASGRLH";

    fn expected_bytes() -> [u8; 20] {
        let mut out = [0u8; 20];
        for i in 0..20 {
            out[i] = u8::from_str_radix(&HEX[i * 2..i * 2 + 2], 16).unwrap();
        }
        out
    }

    #[test]
    fn parses_hex_info_hash_with_trackers() {
        let uri = format!("magnet:?xt=urn:btih:{HEX}&dn=example&tr=http://a.test/announce&tr=udp://b.test:6969");
        let magnet = parse_magnet(&uri).unwrap();

        assert_eq!(magnet.info_hash.0, expected_bytes());
        assert_eq!(magnet.display_name.as_deref(), Some("example"));
        assert_eq!(magnet.trackers, ["http://a.test/announce", "udp://b.test:6969"]);
    }

    /// A base32 `xt` must produce byte-for-byte the same hash as the hex form; getting this
    /// wrong would mean handshaking against the wrong torrent entirely.
    #[test]
    fn base32_and_hex_info_hashes_agree() {
        let hex_uri = format!("magnet:?xt=urn:btih:{HEX}&tr=http://a.test/announce");
        let b32_uri = format!("magnet:?xt=urn:btih:{BASE32}&tr=http://a.test/announce");

        let from_hex = parse_magnet(&hex_uri).unwrap();
        let from_b32 = parse_magnet(&b32_uri).unwrap();
        assert_eq!(from_hex.info_hash, from_b32.info_hash);
        assert_eq!(from_b32.info_hash.0, expected_bytes());
    }

    #[test]
    fn percent_encoded_trackers_are_decoded() {
        let uri = format!("magnet:?xt=urn:btih:{HEX}&tr=udp%3A%2F%2Ftracker.test%3A6969%2Fannounce");
        let magnet = parse_magnet(&uri).unwrap();
        assert_eq!(magnet.trackers, ["udp://tracker.test:6969/announce"]);
    }

    /// A magnet with no `tr=` is a normal DHT-only magnet, not an error.
    #[test]
    fn accepts_magnet_without_trackers() {
        let uri = format!("magnet:?xt=urn:btih:{HEX}&dn=lonely");
        let magnet = parse_magnet(&uri).unwrap();
        assert!(magnet.trackers.is_empty());
    }

    #[test]
    fn rejects_missing_or_malformed_info_hash() {
        assert!(parse_magnet("magnet:?dn=nothing&tr=http://a.test/announce").is_err());
        assert!(parse_magnet("magnet:?xt=urn:btih:xyz&tr=http://a.test/announce").is_err());
    }

    #[test]
    fn parses_web_seeds() {
        let uri = format!(
            "magnet:?xt=urn:btih:{HEX}&ws=https%3A%2F%2Fmirror.test%2Fpub%2Ffile%20name.iso&ws=http://other.test/&ws=ftp://no.test/x"
        );
        let magnet = parse_magnet(&uri).unwrap();
        assert_eq!(
            magnet.web_seeds,
            ["https://mirror.test/pub/file name.iso", "http://other.test/"],
            "decoded, and only http(s)"
        );
    }

    #[test]
    fn is_magnet_uri_distinguishes_from_paths() {
        assert!(is_magnet_uri("magnet:?xt=urn:btih:abc"));
        assert!(is_magnet_uri("MAGNET:?xt=urn:btih:abc"));
        assert!(!is_magnet_uri("/home/me/file.torrent"));
        assert!(!is_magnet_uri("./relative.torrent"));
    }
}
