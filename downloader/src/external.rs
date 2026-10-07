//! Our public address as others see it: peers say so in their extended handshake (BEP 10
//! `yourip`), trackers in their announce replies (BEP 24 `external ip`). One voice could lie
//! or be behind a proxy, so it's a tally, and an address needs two distinct voters to count.

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

/// Voters remembered per address, so one peer reconnecting can't outvote the rest.
const VOTERS_PER_ADDRESS: usize = 64;

#[derive(Clone, Default, Debug)]
pub struct ExternalAddress {
    /// per address, who said so: a peer's address or a tracker's host
    votes: Arc<Mutex<BTreeMap<IpAddr, BTreeSet<String>>>>,
}

impl ExternalAddress {
    /// `voter` says we're at `ip`. Addresses that can't be anyone's public one are ignored.
    /// Returns the agreed address when this vote changed it.
    pub fn vote(&self, ip: IpAddr, voter: &str) -> Option<IpAddr> {
        let ip = ip.to_canonical();
        if !is_public(&ip) {
            return None;
        }
        let before = self.best();
        {
            let mut votes = self.votes.lock().unwrap();
            let voters = votes.entry(ip).or_default();
            if voters.len() < VOTERS_PER_ADDRESS {
                voters.insert(voter.to_string());
            }
        }
        let after = self.best();
        (after != before).then_some(after).flatten()
    }

    /// The address most voters agree on, IPv4 before IPv6 (an IPv6 address is usually our
    /// own interface's anyway); `None` until two have agreed.
    pub fn best(&self) -> Option<IpAddr> {
        let votes = self.votes.lock().unwrap();
        votes
            .iter()
            .filter(|(_, voters)| voters.len() >= 2)
            .max_by_key(|(ip, voters)| (ip.is_ipv4(), voters.len()))
            .map(|(ip, _)| *ip)
    }
}

fn is_public(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => !(v4.is_private() || v4.is_loopback() || v4.is_link_local() || v4.is_unspecified()),
        IpAddr::V6(v6) => {
            !(v6.is_loopback() || v6.is_unspecified() || v6.is_unicast_link_local() || v6.is_unique_local())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn two_distinct_voters_make_an_address() {
        let tally = ExternalAddress::default();
        let ours: IpAddr = "203.0.113.7".parse().unwrap();
        let peer = |n: u8| format!("198.51.100.{n}:6881");
        tally.vote(ours, &peer(1));
        tally.vote(ours, &peer(1));
        assert_eq!(tally.best(), None, "one voter twice is still one voter");
        tally.vote("10.0.0.2".parse().unwrap(), &peer(2));
        tally.vote("10.0.0.2".parse().unwrap(), &peer(3));
        assert_eq!(tally.best(), None, "private addresses don't count");
        tally.vote(ours, &peer(2));
        assert_eq!(tally.best(), Some(ours));
        let v6: IpAddr = "2001:db8::1".parse().unwrap();
        for n in 3..9 {
            tally.vote(v6, &peer(n));
        }
        assert_eq!(tally.best(), Some(ours), "IPv4 first");
    }
}
