//! Our public address as others see it: peers say so in their extended handshake (BEP 10
//! `yourip`), trackers in their announce replies (BEP 24 `external ip`). One voice could lie
//! or be behind a proxy, so it's a tally of the latest votes, and an address needs two
//! distinct voters to count.

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

/// Votes kept per address family, the latest of each voter's: enough for a clear majority, few
/// enough that an address we've moved off is soon outvoted.
const RECENT_VOTES: usize = 64;

#[derive(Clone, Default, Debug)]
pub struct ExternalAddress {
    votes: Arc<Mutex<Tally>>,
}

/// Per family, oldest first: who voted (a peer's IP or a tracker's host), and for what.
#[derive(Default, Debug)]
struct Tally {
    v4: VecDeque<(String, IpAddr)>,
    v6: VecDeque<(String, IpAddr)>,
}

impl ExternalAddress {
    /// `voter` (a peer's address, or a tracker's host) says we're at `ip`. Addresses that
    /// can't be anyone's public one are ignored. Returns the agreed address when this vote
    /// changed it.
    pub fn vote(&self, ip: IpAddr, voter: &str) -> Option<IpAddr> {
        let ip = ip.to_canonical();
        if !is_public(&ip) {
            return None;
        }
        // a peer that connects again comes from another port, but is still one voter
        let voter = match voter.parse::<SocketAddr>() {
            Ok(addr) => addr.ip().to_canonical().to_string(),
            Err(_) => voter.to_string(),
        };
        let mut tally = self.votes.lock().unwrap();
        let before = tally.best();
        let votes = match ip {
            IpAddr::V4(_) => &mut tally.v4,
            IpAddr::V6(_) => &mut tally.v6,
        };
        votes.retain(|(who, _)| *who != voter);
        if votes.len() == RECENT_VOTES {
            votes.pop_front();
        }
        votes.push_back((voter, ip));
        let after = tally.best();
        (after != before).then_some(after).flatten()
    }

    /// The address most voters agree on, IPv4 before IPv6 (an IPv6 address is usually our
    /// own interface's anyway); `None` until two have agreed.
    pub fn best(&self) -> Option<IpAddr> {
        self.votes.lock().unwrap().best()
    }
}

impl Tally {
    fn best(&self) -> Option<IpAddr> {
        agreed(&self.v4).or_else(|| agreed(&self.v6))
    }
}

/// The address with the most votes, at least two; of those tied, the one voted for last.
fn agreed(votes: &VecDeque<(String, IpAddr)>) -> Option<IpAddr> {
    let mut counts: HashMap<IpAddr, (usize, usize)> = HashMap::new();
    for (at, (_, ip)) in votes.iter().enumerate() {
        let (count, last) = counts.entry(*ip).or_default();
        *count += 1;
        *last = at;
    }
    counts
        .into_iter()
        .filter(|(_, (count, _))| *count >= 2)
        .max_by_key(|(_, rank)| *rank)
        .map(|(ip, _)| ip)
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
        tally.vote(ours, "198.51.100.1:7000");
        assert_eq!(tally.best(), None, "so is one peer from another port");
        tally.vote("10.0.0.2".parse().unwrap(), &peer(2));
        tally.vote("10.0.0.2".parse().unwrap(), &peer(3));
        assert_eq!(tally.best(), None, "private addresses don't count");
        assert_eq!(tally.vote(ours, "tracker.test"), Some(ours));
        assert_eq!(tally.best(), Some(ours));
        let v6: IpAddr = "2001:db8::1".parse().unwrap();
        for n in 3..9 {
            tally.vote(v6, &peer(n));
        }
        assert_eq!(tally.best(), Some(ours), "IPv4 first");
    }

    /// When our address changes (a new lease, a VPN coming up), the voters of the new one
    /// carry it, however many the old one had; and the tally stays its size.
    #[test]
    fn a_new_address_outvotes_the_old_one() {
        let tally = ExternalAddress::default();
        let (old, new): (IpAddr, IpAddr) = ("203.0.113.7".parse().unwrap(), "198.51.100.9".parse().unwrap());
        let peer = |n: u32| format!("{}:6881", std::net::Ipv4Addr::from(0x0a00_0000 + n));
        for n in 0..1000 {
            tally.vote(old, &peer(n));
        }
        assert_eq!(tally.best(), Some(old));
        for n in 1000..1000 + RECENT_VOTES as u32 / 2 {
            tally.vote(new, &peer(n));
        }
        assert_eq!(tally.best(), Some(new), "the latest of a tie");
        assert_eq!(tally.votes.lock().unwrap().v4.len(), RECENT_VOTES);
        // an old voter coming back with the new address replaces its old vote
        tally.vote(new, &peer(999));
        assert_eq!(
            tally
                .votes
                .lock()
                .unwrap()
                .v4
                .iter()
                .filter(|(_, ip)| *ip == old)
                .count(),
            RECENT_VOTES / 2 - 1
        );
    }
}
