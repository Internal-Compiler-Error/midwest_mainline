//! What the swarm remembers about an address between connections: dial backoff, bans, and
//! the statistics UCB built up on it.

use crate::peer::PeerStatistics;
use crate::settings::{BAD_PEER_BAN, DIAL_BACKOFF, DIAL_BACKOFF_MAX, FRUITLESS_PEER_COOLDOWN};
use crate::stream::DialHints;
use std::net::SocketAddr;
use std::time::Instant;

use super::TorrentSwarm;

/// What the swarm remembers about an address between connections to it. Trackers and PEX
/// hand out the same addresses over and over, so what happened last time decides whether
/// it's worth dialing again, and a returning peer resumes with the statistics UCB built up
/// on it rather than as a stranger to be explored from scratch.
#[derive(Default)]
pub(super) struct KnownPeer {
    pub(super) stats: PeerStatistics,
    pub(super) consecutive_dial_failures: u32,
    pub(super) dial_after: Option<Instant>,
    pub(super) banned_until: Option<Instant>,
    /// PEX flagged it uTP-capable
    pub(super) prefers_utp: bool,
    /// it refused the encrypted opening when we dialled it
    pub(super) plaintext_only: bool,
    /// the peer whose PEX told us about it: who to ask for a holepunch if dialing fails
    pub(super) via: Option<SocketAddr>,
    /// a holepunch was asked for already; one try per address
    pub(super) holepunched: bool,
}

impl KnownPeer {
    pub(super) fn dial_hints(&self) -> DialHints {
        DialHints {
            prefer_utp: self.prefers_utp,
            plaintext: self.plaintext_only,
            utp_only: false,
        }
    }
}

impl KnownPeer {
    pub(super) fn dial_failed(&mut self, now: Instant) {
        self.consecutive_dial_failures += 1;
        let backoff = DIAL_BACKOFF.saturating_mul(1 << (self.consecutive_dial_failures - 1).min(16));
        self.dial_after = Some(now + backoff.min(DIAL_BACKOFF_MAX));
    }

    pub(super) fn connected(&mut self) {
        self.consecutive_dial_failures = 0;
        self.dial_after = None;
    }

    pub(super) fn disconnected(&mut self, stats: &PeerStatistics, now: Instant) {
        if stats.received + stats.sent == 0 {
            self.dial_after = Some(now + FRUITLESS_PEER_COOLDOWN);
        }
        self.stats = stats.for_reconnect();
    }

    pub(super) fn ban(&mut self, now: Instant) {
        self.banned_until = Some(now + BAD_PEER_BAN);
    }

    pub(super) fn banned(&self, now: Instant) -> bool {
        self.banned_until.is_some_and(|until| until > now)
    }

    pub(super) fn may_dial(&self, now: Instant) -> bool {
        !self.banned(now) && !self.dial_after.is_some_and(|after| after > now)
    }
}

/// See `prune_known`. A few thousand is a busy swarm's worth over days.
pub(super) const KNOWN_PEERS_MAX: usize = 20_000;

impl TorrentSwarm {
    /// Keeps `known` from growing without bound over a long seed: past `KNOWN_PEERS_MAX`, the
    /// entries that remember nothing worth keeping go (never delivered, not banned, not
    /// connected or being dialled, not waiting out a dial backoff).
    pub(super) fn prune_known(&mut self) {
        if self.known.len() <= KNOWN_PEERS_MAX {
            return;
        }
        let now = Instant::now();
        let peers = &self.peers;
        let dialing = &self.dialing;
        self.known.retain(|addr, k| {
            k.stats.received > 0
                || k.banned(now)
                || k.dial_after.is_some_and(|after| after > now)
                || dialing.contains(addr)
                || peers.binary_search_by_key(addr, |p| p.remote_addr).is_ok()
        });
    }
}

#[cfg(test)]
mod test {
    use super::super::test_support::*;
    use super::*;

    #[test]
    fn known_peer_backs_off_failed_dials_and_forgets_on_connect() {
        let t0 = Instant::now();
        let mut k = KnownPeer::default();
        assert!(k.may_dial(t0), "nothing known against it");

        k.dial_failed(t0);
        assert!(!k.may_dial(t0 + DIAL_BACKOFF / 2));
        assert!(k.may_dial(t0 + DIAL_BACKOFF));
        k.dial_failed(t0);
        assert!(!k.may_dial(t0 + DIAL_BACKOFF), "the second failure waits twice as long");
        assert!(k.may_dial(t0 + 2 * DIAL_BACKOFF));
        for _ in 0..40 {
            k.dial_failed(t0);
        }
        assert!(k.may_dial(t0 + DIAL_BACKOFF_MAX), "the backoff is capped");

        k.connected();
        assert!(k.may_dial(t0));
    }

    #[test]
    fn known_peer_cools_down_after_a_fruitless_connection_and_bans_block_dialing() {
        let t0 = Instant::now();
        let mut k = KnownPeer::default();
        k.disconnected(&PeerStatistics::default(), t0);
        assert!(!k.may_dial(t0 + FRUITLESS_PEER_COOLDOWN / 2));
        assert!(k.may_dial(t0 + FRUITLESS_PEER_COOLDOWN));

        let mut useful = PeerStatistics::default();
        useful.block_received(16_384, t0);
        let mut k = KnownPeer::default();
        k.disconnected(&useful, t0);
        assert!(k.may_dial(t0), "a peer that delivered is welcome straight back");
        assert_eq!(k.stats.received, 0, "only the rate and pick count carry over");

        k.ban(t0);
        assert!(k.banned(t0 + BAD_PEER_BAN / 2));
        assert!(!k.may_dial(t0 + BAD_PEER_BAN / 2));
        assert!(!k.banned(t0 + BAD_PEER_BAN));
    }

    /// Trackers and PEX keep handing out the same addresses. One that connected and then hung
    /// up without a block exchanged isn't dialed again for a while; one that delivered is.
    #[tokio::test]
    async fn fruitless_peers_are_not_redialed_but_useful_ones_are() {
        let (swarm, handle, path) = swarm("redial");
        tokio::spawn(swarm.work_loop());

        let fruitless_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let fruitless_addr = fruitless_listener.local_addr().unwrap();
        let mut fruitless = fake_peer_with(&handle, &fruitless_addr.to_string(), false).await;
        open_as_seeder(&mut fruitless).await;
        let Some(Ok(BtMessage::Request(_))) = fruitless.next().await else {
            panic!("expected a request");
        };
        drop(fruitless);

        let useful_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let useful_addr = useful_listener.local_addr().unwrap();
        let mut useful = fake_peer_with(&handle, &useful_addr.to_string(), false).await;
        open_as_seeder(&mut useful).await;
        let Some(Ok(BtMessage::Request(req))) = useful.next().await else {
            panic!("expected a request");
        };
        useful.send(block(req)).await.unwrap();
        // let the swarm take the block before the socket goes away under it
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(useful);
        tokio::time::sleep(Duration::from_millis(100)).await;

        handle
            .tx
            .send(SwarmEvent::PeersDiscovered(
                vec![fruitless_addr, useful_addr],
                PeerSource::Lsd,
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), useful_listener.accept())
            .await
            .expect("a peer that delivered is dialed again")
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(300), fruitless_listener.accept())
                .await
                .is_err(),
            "a peer that delivered nothing is left alone"
        );
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
