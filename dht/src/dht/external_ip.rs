//! What other nodes say our external address is (the BEP 42 `ip` field they put in
//! responses). Every node that answers one of our queries gets one vote; once enough agree,
//! the winner is what the next start derives the node id from. The id can't change while the
//! node runs (every bucket in the routing table hangs off it), which is why the learning is
//! applied one start late.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Mutex;

/// agreeing voters before an address counts as known
const MIN_VOTES: usize = 5;
/// voters remembered; bootstrapping alone brings in plenty
const MAX_VOTERS: usize = 256;

#[derive(Debug, Default)]
pub(crate) struct ExternalIp {
    /// voter -> the address it saw us at
    votes: Mutex<HashMap<IpAddr, IpAddr>>,
    consensus: Mutex<Option<IpAddr>>,
}

impl ExternalIp {
    /// Starts out agreeing with `known`, the address learned last time, so votes for it
    /// aren't news.
    pub fn new(known: Option<IpAddr>) -> Self {
        Self {
            votes: Mutex::default(),
            consensus: Mutex::new(known),
        }
    }

    /// Records that `voter` sees us as `seen`. Returns the consensus when this vote changes
    /// it: the first time enough nodes agree, and again if the majority moves elsewhere.
    pub fn vote(&self, voter: IpAddr, seen: IpAddr) -> Option<IpAddr> {
        if !is_public(seen) {
            // a node on our own LAN sees the LAN address
            return None;
        }
        let mut votes = self.votes.lock().unwrap();
        if votes.len() >= MAX_VOTERS && !votes.contains_key(&voter) {
            return None;
        }
        votes.insert(voter, seen);

        let mut tally = HashMap::<IpAddr, usize>::new();
        for seen in votes.values() {
            *tally.entry(*seen).or_default() += 1;
        }
        let (winner, count) = tally.into_iter().max_by_key(|(_, count)| *count)?;
        if count < MIN_VOTES {
            return None;
        }
        let mut consensus = self.consensus.lock().unwrap();
        if *consensus == Some(winner) {
            return None;
        }
        *consensus = Some(winner);
        Some(winner)
    }

    #[cfg(test)]
    fn current(&self) -> Option<IpAddr> {
        *self.consensus.lock().unwrap()
    }
}

fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !(ip.is_unspecified()
                || ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || is_carrier_grade_nat(ip))
        }
        IpAddr::V6(ip) => is_global_v6(ip),
    }
}

/// 100.64.0.0/10, what an ISP without enough addresses hands out
fn is_carrier_grade_nat(ip: Ipv4Addr) -> bool {
    let [a, b, ..] = ip.octets();
    a == 100 && (b & 0xc0) == 64
}

/// Global unicast: not loopback or unspecified, not unique local (fc00::/7), link local
/// (fe80::/10), multicast, documentation (2001:db8::/32) or an IPv4-mapped address.
pub(crate) fn is_global_v6(ip: Ipv6Addr) -> bool {
    let s = ip.segments();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || ip.is_unique_local()
        || ip.is_unicast_link_local()
        || (s[0] == 0x2001 && s[1] == 0x0db8)
        || ip.to_ipv4_mapped().is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        Ipv4Addr::new(a, b, c, d).into()
    }

    fn voter(n: u8) -> IpAddr {
        ip(10, 0, 0, n)
    }

    #[test]
    fn ipv6_votes_count_for_global_addresses_only() {
        let ext = ExternalIp::default();
        let us: IpAddr = "2001:470:1:2::5".parse().unwrap();
        for n in 1..=MIN_VOTES as u8 {
            assert_eq!(ext.vote(voter(n), "fd00::1".parse().unwrap()), None, "unique local");
            assert_eq!(ext.vote(voter(n), "fe80::1".parse().unwrap()), None, "link local");
        }
        for n in 1..MIN_VOTES as u8 {
            assert_eq!(ext.vote(voter(n), us), None);
        }
        assert_eq!(ext.vote(voter(MIN_VOTES as u8), us), Some(us));
    }

    #[test]
    fn enough_agreeing_votes_make_a_consensus() {
        let ext = ExternalIp::default();
        let us = ip(5, 6, 7, 8);
        for n in 1..MIN_VOTES as u8 {
            assert_eq!(ext.vote(voter(n), us), None);
        }
        assert_eq!(ext.vote(voter(MIN_VOTES as u8), us), Some(us));
        // more of the same is not news
        assert_eq!(ext.vote(voter(20), us), None);
        assert_eq!(ext.current(), Some(us));
    }

    #[test]
    fn a_voter_counts_once_and_private_addresses_do_not_count() {
        let ext = ExternalIp::default();
        let us = ip(5, 6, 7, 8);
        for _ in 0..10 {
            assert_eq!(
                ext.vote(voter(1), us),
                None,
                "one voter must not reach the threshold alone"
            );
        }
        for n in 1..=10 {
            assert_eq!(ext.vote(voter(n), ip(192, 168, 1, 2)), None);
            assert_eq!(ext.vote(voter(n), ip(100, 64, 0, 1)), None);
        }
        assert_eq!(ext.current(), None);
    }

    #[test]
    fn what_was_known_already_is_not_news() {
        let us = ip(5, 6, 7, 8);
        let ext = ExternalIp::new(Some(us));
        for n in 1..=10 {
            assert_eq!(ext.vote(voter(n), us), None);
        }
        assert_eq!(ext.current(), Some(us));
    }

    #[test]
    fn the_majority_can_move() {
        let ext = ExternalIp::default();
        let before = ip(5, 6, 7, 8);
        let after = ip(9, 9, 9, 9);
        for n in 1..=5 {
            ext.vote(voter(n), before);
        }
        assert_eq!(ext.current(), Some(before));
        // the same voters change their minds, e.g. after the router got a new lease
        for n in 1..=4 {
            assert_eq!(ext.vote(voter(n), after), None, "no majority yet");
        }
        assert_eq!(ext.vote(voter(5), after), Some(after));
    }
}
