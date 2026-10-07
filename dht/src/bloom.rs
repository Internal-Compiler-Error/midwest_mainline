//! BEP 33's bloom filter of IP addresses: 2048 bits, two hash functions taken from the
//! address's SHA-1. A node answers a scrape with one of its seeds' addresses and one of the
//! other peers'; ORing the filters from several nodes and counting the bits left zero
//! estimates how many distinct addresses went in.

use std::net::IpAddr;

use sha1::{Digest, Sha1};

const BITS: usize = 2048;
pub const BLOOM_BYTES: usize = BITS / 8;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BloomFilter(pub [u8; BLOOM_BYTES]);

impl Default for BloomFilter {
    fn default() -> Self {
        Self([0; BLOOM_BYTES])
    }
}

impl std::fmt::Debug for BloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BloomFilter(~{:.0})", self.estimate())
    }
}

impl BloomFilter {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        Some(Self(bytes.try_into().ok()?))
    }

    pub fn insert(&mut self, ip: IpAddr) {
        let hash = match ip {
            IpAddr::V4(ip) => Sha1::digest(ip.octets()),
            IpAddr::V6(ip) => Sha1::digest(ip.octets()),
        };
        for index in [
            usize::from(hash[0]) | usize::from(hash[1]) << 8,
            usize::from(hash[2]) | usize::from(hash[3]) << 8,
        ] {
            let index = index % BITS;
            self.0[index / 8] |= 1 << (index % 8);
        }
    }

    /// The filter of both filters' addresses
    pub fn union(&self, other: &BloomFilter) -> BloomFilter {
        let mut both = *self;
        for (b, o) in both.0.iter_mut().zip(other.0) {
            *b |= o;
        }
        both
    }

    /// How many distinct addresses went in, roughly; BEP 33's formula
    pub fn estimate(&self) -> f64 {
        let zeros: u32 = self.0.iter().map(|b| b.count_zeros()).sum();
        if zeros == BITS as u32 {
            return 0.0;
        }
        let c = f64::from(zeros.min(BITS as u32 - 1));
        let m = BITS as f64;
        (c / m).ln() / (2.0 * (1.0 - 1.0 / m).ln())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn bep_33s_test_vector() {
        let mut filter = BloomFilter::default();
        for i in 0..=255u8 {
            filter.insert(Ipv4Addr::new(192, 0, 2, i).into());
        }
        for i in 0..1000u16 {
            let mut segments = [0x2001, 0xdb8, 0, 0, 0, 0, 0, 0];
            segments[7] = i;
            filter.insert(Ipv6Addr::from(segments).into());
        }
        let expected = hex::decode(
            "F6C3F5EAA07FFD91BDE89F777F26FB2BFF37BDB8FB2BBAA2FD3DDDE7BACFFF75\
             EE7CCBAEFE5EEDB1FBFAFF67F6ABFF5E43DDBCA3FD9B9FFDF4FFD3E9DFF12D1B\
             DF59DB53DBE9FA5B7FF3B8FDFCDE1AFB8BEDD7BE2F3EE71EBBBFE93BCDEEFE14\
             8246C2BC5DBFF7E7EFDCF24FD8DC7ADFFD8FFFDFDDFFF7A4BBEEDF5CB95CE81F\
             C7FCFF1FF4FFFFDFE5F7FDCBB7FD79B3FA1FC77BFE07FFF905B7B7FFC7FEFEFF\
             E0B8370BB0CD3F5B7F2BD93FEB4386CFDD6F7FD5BFAF2E9EBFFFFEECD67ADBF7\
             C67F17EFD5D75EBA6FFEBA7FFF47A91EB1BFBB53E8ABFB5762ABE8FF237279BF\
             EFBFEEF5FFC5FEBFDFE5ADFFADFEE1FB737FFFFBFD9F6AEFFEEE76B6FD8F72EF",
        )
        .unwrap();
        assert_eq!(filter.0.as_slice(), expected.as_slice());
        assert!((filter.estimate() - 1224.9308).abs() < 0.001, "{}", filter.estimate());
    }

    #[test]
    fn the_union_counts_each_address_once() {
        let mut a = BloomFilter::default();
        let mut b = BloomFilter::default();
        for i in 0..40u8 {
            a.insert(Ipv4Addr::new(10, 0, 0, i).into());
        }
        for i in 20..60u8 {
            b.insert(Ipv4Addr::new(10, 0, 0, i).into());
        }
        assert_eq!(BloomFilter::default().estimate(), 0.0);
        assert!((a.estimate() - 40.0).abs() < 2.0, "{}", a.estimate());
        assert!(
            (a.union(&b).estimate() - 60.0).abs() < 3.0,
            "{}",
            a.union(&b).estimate()
        );
        assert_eq!(BloomFilter::from_bytes(&[0; 3]), None);
    }
}
