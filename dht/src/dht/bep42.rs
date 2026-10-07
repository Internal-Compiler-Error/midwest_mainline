//! BEP 42, the DHT security extension: a node id's top 21 bits are the CRC32C of the node's
//! masked IP (with 3 random bits mixed in, repeated in the id's last byte), so an id can't be
//! chosen freely to sit next to some info hash. We mint our ids this way and check others'.
//! Plenty of old clients aren't compliant, so a mismatch makes a node second choice, not an
//! outcast: it loses a full bucket to a compliant node, doesn't count towards a lookup's end
//! (as BEP 42 asks), and is announced to only when there aren't enough compliant ones.

use std::net::IpAddr;

use rand::{Rng, RngExt};

use crate::dht::routing_table::sybil_group;
use crate::types::NodeId;

/// The CRC32C of `ip` under BEP 42's mask (the first 4 bytes of an IPv4 address, the first 8
/// of an IPv6 one) with `r`'s low 3 bits in the top bits
fn crc(ip: IpAddr, r: u8) -> u32 {
    let mut masked = [0u8; 8];
    let len = match ip {
        IpAddr::V4(ip) => {
            for ((m, b), mask) in masked.iter_mut().zip(ip.octets()).zip([0x03, 0x0f, 0x3f, 0xff]) {
                *m = b & mask;
            }
            4
        }
        IpAddr::V6(ip) => {
            let mask = [0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff];
            for ((m, b), mask) in masked.iter_mut().zip(ip.octets()).zip(mask) {
                *m = b & mask;
            }
            8
        }
    };
    masked[0] |= (r & 0x07) << 5;
    crc32c::crc32c(&masked[..len])
}

/// A compliant node id for `ip`: random but for the CRC prefix, with `rand` as the last byte
pub(crate) fn mint(ip: IpAddr, rand: u8) -> NodeId {
    let mut rng = rand::rng();
    let crc = crc(ip, rand);
    let mut id = [0u8; 20];
    id[0] = (crc >> 24) as u8;
    id[1] = (crc >> 16) as u8;
    id[2] = (((crc >> 8) & 0xf8) as u8) | (rng.random::<u8>() & 0x7);
    rng.fill_bytes(&mut id[3..19]);
    id[19] = rand;
    NodeId(id)
}

/// Whether a node at `ip` may use `id`. LAN addresses (the ones BEP 42 exempts, and their
/// IPv6 counterparts) always may.
pub fn compliant(id: &NodeId, ip: IpAddr) -> bool {
    if sybil_group(&ip).is_none() {
        return true;
    }
    let crc = crc(ip, id.0[19]);
    id.0[0] == (crc >> 24) as u8 && id.0[1] == (crc >> 16) as u8 && (id.0[2] ^ (crc >> 8) as u8) & 0xf8 == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(hex: &str) -> NodeId {
        NodeId::from_bytes(&hex::decode(hex).unwrap())
    }

    #[test]
    fn bep_42s_test_vectors_check_out() {
        let vectors = [
            ("124.31.75.21", "5fbfbff10c5d6a4ec8a88e4c6ab4c28b95eee401"),
            ("21.75.31.124", "5a3ce9c14e7a08645677bbd1cfe7d8f956d53256"),
            ("65.23.51.170", "a5d43220bc8f112a3d426c84764f8c2a1150e616"),
            ("84.124.73.14", "1b0321dd1bb1fe518101ceef99462b947a01ff41"),
            ("43.213.53.83", "e56f6cbf5b7c4be0237986d5243b87aa6d51305a"),
        ];
        for (ip, hex) in vectors {
            let ip: IpAddr = ip.parse().unwrap();
            assert!(compliant(&id(hex), ip), "{ip}");
            let rand = id(hex).0[19];
            assert_eq!(&mint(ip, rand).0[..2], &id(hex).0[..2]);
        }
        // the right prefix for another address isn't
        assert!(!compliant(&id(vectors[0].1), "21.75.31.124".parse().unwrap()));
        // the random bits must match the last byte
        let mut wrong_r = id(vectors[0].1);
        wrong_r.0[19] = 2;
        assert!(!compliant(&wrong_r, vectors[0].0.parse().unwrap()));
    }

    #[test]
    fn ipv6_ids_follow_the_slash_64_and_lan_addresses_are_exempt() {
        let ip: IpAddr = "2001:470:1:2::5".parse().unwrap();
        for rand in [0u8, 7, 86, 255] {
            let id = mint(ip, rand);
            assert!(compliant(&id, ip));
            assert!(compliant(&id, "2001:470:1:2:ffff::9".parse().unwrap()));
            assert!(!compliant(&id, "2a02:752::1".parse().unwrap()));
        }
        let anything = NodeId([0x42; 20]);
        for lan in [
            "10.1.2.3",
            "172.16.0.9",
            "192.168.1.1",
            "169.254.3.3",
            "127.0.0.1",
            "::1",
            "fd00::1",
            "fe80::1",
        ] {
            assert!(compliant(&anything, lan.parse().unwrap()), "{lan}");
        }
        assert!(!compliant(&anything, "8.8.8.8".parse().unwrap()));
    }
}
