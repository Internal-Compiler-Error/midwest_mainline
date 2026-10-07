//! BEP 40 canonical peer priority: a rank both ends of a connection compute alike, so when
//! clients have to choose which connections to keep, the swarm converges on one mesh instead
//! of each side dropping a different half.

use std::net::{IpAddr, SocketAddr};

/// The priority of a connection between `a` and `b`, the same whichever side asks; higher is
/// better. Addresses of different families have no common rank: `None`.
pub fn peer_priority(a: SocketAddr, b: SocketAddr) -> Option<u32> {
    let (a, b) = (canonical(a), canonical(b));
    if a.ip() == b.ip() {
        let (lo, hi) = (a.port().min(b.port()), a.port().max(b.port()));
        let mut buf = [0u8; 4];
        buf[..2].copy_from_slice(&lo.to_be_bytes());
        buf[2..].copy_from_slice(&hi.to_be_bytes());
        return Some(crc32c::crc32c(&buf));
    }
    match (a.ip(), b.ip()) {
        (IpAddr::V4(x), IpAddr::V4(y)) => Some(masked_crc(&x.octets(), &y.octets(), 2)),
        (IpAddr::V6(x), IpAddr::V6(y)) => Some(masked_crc(&x.octets(), &y.octets(), 4)),
        _ => None,
    }
}

/// Masks both addresses by how much of a prefix they share (past `whole` bytes, one more
/// whole byte per matching byte, up to the full address), then CRC32-Cs them in order.
fn masked_crc<const N: usize>(x: &[u8; N], y: &[u8; N], whole: usize) -> u32 {
    // IPv4: different /16 masks ff.ff.55.55, same /16 ff.ff.ff.55, same /24 everything;
    // IPv6 the same over the first 8 bytes, from /32
    let keep = if x[..whole] != y[..whole] {
        whole
    } else if x[whole] != y[whole] {
        whole + 1
    } else {
        N
    };
    let mask = |ip: &[u8; N]| {
        let mut out = *ip;
        for byte in &mut out[keep..] {
            *byte &= 0x55;
        }
        out
    };
    let (x, y) = (mask(x), mask(y));
    let (lo, hi) = if x <= y { (x, y) } else { (y, x) };
    crc32c::crc32c_append(crc32c::crc32c(&lo), &hi)
}

fn canonical(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(addr.ip().to_canonical(), addr.port())
}

#[cfg(test)]
mod test {
    use super::*;

    fn at(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    /// The examples in BEP 40 itself.
    #[test]
    fn matches_the_spec_examples() {
        assert_eq!(
            peer_priority(at("123.213.32.10:0"), at("98.76.54.32:0")),
            Some(0xec2d7224)
        );
        assert_eq!(
            peer_priority(at("123.213.32.10:0"), at("123.213.32.234:0")),
            Some(0x99568189)
        );
    }

    #[test]
    fn symmetric_and_family_bound() {
        let pairs = [
            ("1.2.3.4:6881", "1.2.3.4:7000"),
            ("1.2.3.4:6881", "1.2.9.9:1"),
            ("[2001:db8::1]:1", "[2001:db8:0:1::2]:2"),
            ("[2001:db8::1]:1", "[2a00::1]:2"),
        ];
        for (a, b) in pairs {
            assert_eq!(peer_priority(at(a), at(b)), peer_priority(at(b), at(a)), "{a} {b}");
        }
        assert_eq!(peer_priority(at("1.2.3.4:1"), at("[2001:db8::1]:1")), None);
        assert_eq!(
            peer_priority(at("[::ffff:1.2.3.4]:1"), at("98.76.54.32:1")),
            peer_priority(at("1.2.3.4:1"), at("98.76.54.32:1")),
            "mapped addresses are their IPv4 selves"
        );
    }
}
