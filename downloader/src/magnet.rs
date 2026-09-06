//! Magnet URI parsing (BEP 9's "magnet-uri" form).
//!
//! Only what's needed to start a download without a `.torrent` file: the info hash, the
//! display name, and the tracker list. A magnet with no trackers is fine: the DHT finds its
//! peers.

use anyhow::{bail, ensure};
use midwest_mainline::types::InfoHash;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MagnetLink {
    pub info_hash: InfoHash,
    /// the `dn` param -- a *hint* only. The real name comes from the metadata we fetch, since
    /// this one is attacker-controlled and never hash-verified.
    pub display_name: Option<String>,
    pub trackers: Vec<String>,
}

/// True if `s` looks like a magnet URI, so callers can accept either this or a file path.
pub fn is_magnet_uri(s: &str) -> bool {
    s.trim_start().to_ascii_lowercase().starts_with("magnet:")
}

pub fn parse_magnet(uri: &str) -> anyhow::Result<MagnetLink> {
    let url = Url::parse(uri.trim())?;
    ensure!(url.scheme().eq_ignore_ascii_case("magnet"), "not a magnet URI");

    let mut info_hash = None;
    let mut display_name = None;
    let mut trackers = Vec::new();

    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            // there can be several `xt`s (e.g. a v1 btih alongside a v2 btmh); take the first
            // btih we understand and ignore the rest
            "xt" if info_hash.is_none() => {
                if let Some(rest) = strip_prefix_ignore_ascii_case(&value, "urn:btih:") {
                    info_hash = Some(parse_info_hash(rest)?);
                }
            }
            "dn" if display_name.is_none() => display_name = Some(value.into_owned()),
            "tr" => trackers.push(value.into_owned()),
            _ => {}
        }
    }

    let Some(info_hash) = info_hash else {
        bail!("magnet URI has no `xt=urn:btih:` info hash (v2-only `btmh` magnets aren't supported)");
    };

    Ok(MagnetLink {
        info_hash,
        display_name,
        trackers,
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
        40 => decode_hex(raw)?,
        32 => decode_base32(raw)?,
        n => bail!("info hash must be 40 hex or 32 base32 characters, got {n}"),
    };
    Ok(InfoHash::from_bytes(&bytes))
}

fn decode_hex(s: &str) -> anyhow::Result<[u8; 20]> {
    let mut out = [0u8; 20];
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
    use super::*;

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
        // v2-only magnets name a btmh multihash, which this client can't use
        let v2 = "magnet:?xt=urn:btmh:1220caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e&tr=http://a.test/announce";
        assert!(parse_magnet(v2).is_err());
    }

    #[test]
    fn is_magnet_uri_distinguishes_from_paths() {
        assert!(is_magnet_uri("magnet:?xt=urn:btih:abc"));
        assert!(is_magnet_uri("MAGNET:?xt=urn:btih:abc"));
        assert!(!is_magnet_uri("/home/me/file.torrent"));
        assert!(!is_magnet_uri("./relative.torrent"));
    }
}
