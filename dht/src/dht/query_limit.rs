//! How many queries one host may send us a second. Answers can be many times a query's size
//! (both families' closest nodes, BEP 33's filters, BEP 44's values), so a sender spoofing its
//! source address could aim our answers at someone else; and every announce or put it gets in
//! is a row in the store. Past the limit its queries go unanswered for the rest of the second.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use super::routing_table::sybil_group;

/// Queries a second one host gets answered. A busy client behind one address (a carrier-grade
/// NAT, say) sends a few a second; a flood sends thousands.
const PER_SECOND: u32 = 25;
/// Hosts counted within one second; a flood from more than this many addresses is dropped
/// whole for the rest of the second rather than grow the count
const MAX_HOSTS: usize = 50_000;

#[derive(Debug)]
pub(crate) struct QueryLimit {
    second: Instant,
    /// queries this second, by host: an IPv4 address or an IPv6 /64 (see `sybil_group`)
    counts: HashMap<IpAddr, u32>,
}

impl QueryLimit {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            second: now,
            counts: HashMap::new(),
        }
    }

    /// Whether to answer a query from `ip` that came in at `now`. LAN addresses aren't limited.
    pub(crate) fn allows(&mut self, ip: IpAddr, now: Instant) -> bool {
        let Some(host) = sybil_group(&ip) else {
            return true;
        };
        if now.saturating_duration_since(self.second) >= Duration::from_secs(1) {
            self.counts.clear();
            self.second = now;
        }
        if self.counts.len() >= MAX_HOSTS && !self.counts.contains_key(&host) {
            return false;
        }
        let count = self.counts.entry(host).or_default();
        *count += 1;
        *count <= PER_SECOND
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_host_gets_its_share_a_second() {
        let start = Instant::now();
        let mut limit = QueryLimit::new(start);
        let host: IpAddr = "8.8.8.8".parse().unwrap();
        let answered = (0..100).filter(|_| limit.allows(host, start)).count();
        assert_eq!(answered, PER_SECOND as usize);
        assert!(limit.allows("9.9.9.9".parse().unwrap(), start), "others aren't held up");
        assert!(
            limit.allows(host, start + Duration::from_secs(1)),
            "and the next second is new"
        );

        // an IPv6 host is its /64
        let mut limit = QueryLimit::new(start);
        let answered = (0..100u16)
            .filter(|i| limit.allows(format!("2001:470:1:2::{i:x}").parse().unwrap(), start))
            .count();
        assert_eq!(answered, PER_SECOND as usize);

        // the LAN isn't limited
        assert!((0..100).all(|_| limit.allows("192.168.1.2".parse().unwrap(), start)));
    }
}
