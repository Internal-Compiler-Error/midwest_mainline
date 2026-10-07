//! Getting connected: dialing what discovery hands out, taking on handshaken sockets (making
//! room at the connection cap by BEP 40), and our side of the opening exchange.

use crate::defs::Identity;
use crate::events::{Event, PeerSource};
use crate::peer::Peer;
use crate::settings::MAX_HALF_OPEN;
use crate::stream::DialHints;
use crate::torrent::Torrent;
use crate::wire::BitField;
use anyhow::Context;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;
use tracing::info;

use super::{ConnectedPeer, SwarmEvent, TorrentSwarm, canonical};

/// Dials queued or under way per swarm, past which more addresses are passed over: a peer can
/// gossip (PEX) or introduce (holepunch) addresses as fast as it can send, and each one waiting
/// for a `HALF_OPEN` slot is a task and an entry in `dialing`. Far more than can be dialled at
/// once anyway.
pub(super) const MAX_PENDING_DIALS: usize = 1024;

/// See `MAX_HALF_OPEN`.
pub(super) static HALF_OPEN: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(MAX_HALF_OPEN);

pub(super) async fn dial(
    addr: SocketAddr,
    torrent: &Torrent,
    our_id: &Identity,
    utp: Option<Arc<librqbit_utp::UtpSocketUdp>>,
    hints: DialHints,
) -> anyhow::Result<ConnectedPeer> {
    let (stream, handshake) = tokio::time::timeout(
        crate::settings::HANDSHAKE_TIMEOUT,
        crate::stream::connect(
            addr,
            &torrent.info_hash,
            torrent.v2_support(),
            our_id,
            utp.as_ref(),
            hints,
        ),
    )
    .await
    .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into()))
    .with_context(|| format!("Failed to connect to {addr}"))?;
    info!(
        "Peer connection to {addr} established{}{}",
        if stream.is_utp() { " over uTP" } else { "" },
        if stream.is_encrypted() { " (encrypted)" } else { "" }
    );
    Ok(ConnectedPeer {
        stream,
        dialed: true,
        remote_addr: addr,
        remote_supports_extensions: handshake.supports_extensions(),
        remote_supports_fast: handshake.supports_fast_extension(),
        remote_supports_dht: handshake.supports_dht(),
        remote_supports_v2: torrent.v2_support().v2_peer(&handshake),
        peer_id: handshake.peer_id,
    })
}

impl TorrentSwarm {
    /// Our address as peers see it, for BEP 40: the agreed public IP and our listening port.
    pub(super) fn our_address(&self) -> Option<SocketAddr> {
        let ip = self.external.best()?;
        Some(SocketAddr::new(ip, self.id.serving.port()))
    }

    /// At the cap, the connection `newcomer` may replace (BEP 40): the lowest ranked of those
    /// that have never delivered a block, if `newcomer` outranks it. A peer that has delivered
    /// keeps its place: what UCB measured is better evidence than a hash.
    pub(super) fn make_room_for(&self, newcomer: SocketAddr) -> Option<usize> {
        let us = self.our_address()?;
        let rank = |addr| crate::priority::peer_priority(us, addr);
        let theirs = rank(newcomer)?;
        let (idx, lowest) = self
            .peers
            .iter()
            .enumerate()
            .filter(|(_, p)| p.stats.received == 0)
            .filter_map(|(idx, p)| Some((idx, rank(p.remote_addr)?)))
            .min_by_key(|&(_, r)| r)?;
        (theirs > lowest).then_some(idx)
    }

    /// Takes ownership of a handshaken socket. Sends our side of the opening exchange (BEP 10
    /// extended handshake, then BitField/HaveAll/HaveNone, then Interested) before the peer
    /// joins `peers`, so nothing else can be written to it first.
    pub(super) fn add_peer(&mut self, connected: ConnectedPeer) {
        let remote_addr = canonical(connected.remote_addr);
        self.dialing.remove(&remote_addr);
        if self.peer_index(remote_addr).is_some() {
            info!("{remote_addr} is already connected, dropping the duplicate");
            return;
        }
        if self.known.get(&remote_addr).is_some_and(|k| k.banned(Instant::now())) {
            info!("{remote_addr} is banned, refusing it");
            return;
        }
        if self.peers.len() >= self.settings.borrow().peer_cap() {
            match self.make_room_for(remote_addr) {
                Some(idx) => self.drop_peer(idx, "replaced by a peer of higher BEP 40 priority"),
                None => {
                    tracing::debug!("{remote_addr} refused, at the connection cap");
                    self.known.entry(remote_addr).or_default().connected();
                    return;
                }
            }
        }
        let known = self.known.entry(remote_addr).or_default();
        known.connected();
        // a dialled peer that came up plaintext under `Prefer` refused the encrypted opening
        if connected.dialed && self.id.encryption == crate::config::Encryption::Prefer {
            known.plaintext_only = !connected.stream.is_encrypted();
        }

        self.next_conn += 1;
        let (extensions, dht) = (connected.remote_supports_extensions, connected.remote_supports_dht);
        let connected = ConnectedPeer {
            remote_addr,
            ..connected
        };
        let mut peer = Peer::new(connected, self.torrent.num_pieces(), self.next_conn, self.inbox.clone());
        peer.stats = known.stats.clone();
        let opened = self.open(&mut peer, extensions, dht);
        if let Err(e) = opened {
            info!("{remote_addr} went away during the opening exchange ({e})");
            self.known
                .entry(remote_addr)
                .or_default()
                .disconnected(&peer.stats, Instant::now());
            return;
        }

        info!("{remote_addr} connected, {} peers now", self.peers.len() + 1);
        peer.span = tracing::info_span!(
            "peer",
            info_hash = %self.torrent.info_hash,
            peer = %remote_addr,
            client = %crate::peer::client_name(&peer.peer_id),
            transport = if peer.utp { "utp" } else { "tcp" },
            encrypted = peer.encrypted,
            dialed = peer.dialed,
            downloaded = tracing::field::Empty,
            uploaded = tracing::field::Empty,
            reason = tracing::field::Empty,
        );
        self.bus.emit(Event::PeerConnected {
            info_hash: self.torrent.info_hash,
            addr: remote_addr,
            client: crate::peer::client_name(&peer.peer_id),
            dialed: peer.dialed,
            encrypted: peer.encrypted,
            utp: peer.utp,
        });
        let insert_at = self.peers.partition_point(|p| p.remote_addr < remote_addr);
        self.peers.insert(insert_at, peer);
        self.reveal_next_piece(insert_at);
    }

    /// Our side of the opening exchange, in the order BEP 10 and BEP 6 expect.
    pub(super) fn open(&self, peer: &mut Peer, extensions: bool, dht: bool) -> io::Result<()> {
        if extensions {
            peer.send_extended_handshake(
                self.torrent.metadata_size(),
                self.torrent.private,
                self.partial_seed(),
                self.id.serving.port(),
            )?;
        }
        // BEP 6: a peer that advertised Fast Extension support accepts HaveAll/HaveNone in
        // place of a BitField for the "everything"/"nothing" cases
        if self.super_seeding() {
            peer.super_seed = Some(Default::default());
            if peer.remote_supports_fast {
                peer.send_have_none()?;
            } else {
                let has = vec![0u8; self.torrent.num_pieces().div_ceil(8)].into_boxed_slice();
                peer.send_bitfield(BitField { has })?;
            }
        } else if peer.remote_supports_fast && self.stat.all_verified() {
            peer.send_have_all()?;
        } else if peer.remote_supports_fast && self.stat.verified_cnt() == 0 {
            peer.send_have_none()?;
        } else {
            let has = Box::from(self.stat.verified.as_raw_slice());
            peer.send_bitfield(BitField { has })?;
        }
        // BEP 5: a peer that has a DHT node too gets told where ours listens
        let dht_port = self.dht.borrow().as_ref().map(|d| d.udp_port_for(&peer.remote_addr));
        if dht && let Some(port) = dht_port {
            peer.send_port(port)?;
        }
        // BEP 3: connections start choked; whether to unchoke is the choking algorithm's
        // call, not an automatic grant on connect
        peer.show_interest()
    }

    /// Addresses from a tracker, the DHT, LSD or the metadata fetch.
    pub(super) fn peers_discovered(&mut self, peers: Vec<SocketAddr>, source: PeerSource) {
        self.bus.emit(Event::PeersDiscovered {
            info_hash: self.torrent.info_hash,
            source,
            count: peers.len(),
        });
        self.connect_to_peers(peers.into_iter().map(|addr| (addr, false)).collect(), None);
    }

    /// A dial has failed: the address waits out a backoff, and one PEX told us about may be
    /// reachable through a holepunch.
    pub(super) fn dial_failed(&mut self, addr: SocketAddr) {
        self.bus.emit(Event::DialFailed {
            info_hash: self.torrent.info_hash,
            addr,
        });
        self.dialing.remove(&addr);
        let addr = canonical(addr);
        self.known.entry(addr).or_default().dial_failed(Instant::now());
        self.try_holepunch(addr);
    }

    /// Dials what a tracker, the DHT, LSD or PEX handed out, as far as the peer cap allows.
    /// The flag marks an address PEX said speaks uTP; it's remembered only for addresses
    /// that get dialled, so gossip about peers we never call doesn't pile up in `known`.
    pub(super) fn connect_to_peers(&mut self, mut peers: Vec<(SocketAddr, bool)>, via: Option<SocketAddr>) {
        let now = Instant::now();
        let cap = self.settings.borrow().peer_cap();
        // best BEP 40 rank first: what the cap cuts off, and what waits longest for a
        // half-open slot, is the end of the list
        if let Some(us) = self.our_address() {
            peers.sort_by_cached_key(|(addr, _)| std::cmp::Reverse(crate::priority::peer_priority(us, *addr)));
        }
        for (addr, utp_capable) in peers {
            let addr = canonical(addr);
            if !self.room_to_dial(cap) {
                break;
            }
            if !self.worth_dialing(addr, now) {
                continue;
            }
            self.dialing.insert(addr);
            let known = self.known.entry(addr).or_default();
            if utp_capable {
                known.prefers_utp = true;
            }
            if via.is_some() {
                known.via = via;
            }
            let hints = known.dial_hints();
            self.spawn_dial(addr, hints);
        }
    }

    /// Whether another dial fits under the connection cap and `MAX_PENDING_DIALS`.
    pub(super) fn room_to_dial(&self, cap: usize) -> bool {
        self.peers.len() + self.dialing.len() < cap && self.dialing.len() < MAX_PENDING_DIALS
    }

    /// Not connected or being dialled, not banned or backing off, and with a port: trackers
    /// and PEX both hand out port 0 for peers whose port they don't know.
    pub(super) fn worth_dialing(&self, addr: SocketAddr, now: Instant) -> bool {
        addr.port() != 0
            && self.known.get(&addr).is_none_or(|k| k.may_dial(now))
            && self.peer_index(addr).is_none()
            && !self.dialing.contains(&addr)
    }

    /// Dials `addr` in the background (one of the `MAX_HALF_OPEN` at a time); the outcome comes
    /// back as `PeerConnected` or `DialFailed`. The caller has put it in `dialing`.
    pub(super) fn spawn_dial(&self, addr: SocketAddr, hints: DialHints) {
        let events = self.events_tx.clone();
        let torrent = self.torrent.clone();
        let our_id = self.id.clone();
        let utp = self.utp.borrow().clone();
        tokio::spawn(async move {
            let Ok(_permit) = HALF_OPEN.acquire().await else {
                return;
            };
            // from here, not from the queueing above: a dial waiting for a slot isn't dialling
            let span = tracing::info_span!(
                "dial",
                info_hash = %torrent.info_hash,
                peer = %addr,
                holepunch = hints.utp_only,
                transport = tracing::field::Empty,
                encrypted = tracing::field::Empty,
                error = tracing::field::Empty,
            );
            let dialed = dial(addr, &torrent, &our_id, utp, hints).instrument(span.clone()).await;
            let result = match dialed {
                Ok(connected) => {
                    span.record("transport", if connected.stream.is_utp() { "utp" } else { "tcp" });
                    span.record("encrypted", connected.stream.is_encrypted());
                    SwarmEvent::PeerConnected(connected)
                }
                Err(e) => {
                    tracing::debug!("couldn't connect to {addr}: {e:#}");
                    span.record("error", format!("{e:#}"));
                    SwarmEvent::DialFailed(addr)
                }
            };
            drop(span);
            // if the swarm is gone meanwhile, the socket just drops here
            if let Some(events) = events.upgrade() {
                let _ = events.send(result).await;
            }
        });
    }
}

#[cfg(test)]
mod test {
    use super::super::test_support::*;
    use super::*;

    /// BEP 40 at the connection cap: a newcomer that outranks an idle peer takes its place;
    /// one that doesn't is turned away.
    #[tokio::test]
    async fn at_the_cap_a_higher_priority_peer_replaces_an_idle_one() {
        let settings = crate::config::Settings {
            max_peers_per_torrent: 1,
            ..Default::default()
        };
        let (swarm, handle, path) = swarm_with_settings("bep40", true, settings);
        let ours: std::net::IpAddr = "203.0.113.7".parse().unwrap();
        swarm.external.vote(ours, "a");
        swarm.external.vote(ours, "b");
        let us = SocketAddr::new(ours, 0);
        tokio::spawn(swarm.work_loop());
        let mut candidates: Vec<SocketAddr> = (1..=3)
            .map(|n| format!("198.51.{n}.{n}:6881").parse().unwrap())
            .collect();
        candidates.sort_by_key(|addr| crate::priority::peer_priority(us, *addr));
        let [low, mid, high] = candidates[..] else {
            unreachable!()
        };

        // a closed connection reads as the end of the stream, after whatever was sent first
        async fn closed(peer: &mut Wire) -> bool {
            let drained = async { while let Some(Ok(_)) = peer.next().await {} };
            tokio::time::timeout(Duration::from_secs(5), drained).await.is_ok()
        }
        let mut first = fake_peer(&handle, &mid.to_string()).await;
        let mut outranked = fake_peer(&handle, &low.to_string()).await;
        assert!(closed(&mut outranked).await, "a lower rank is refused");
        let _better = fake_peer(&handle, &high.to_string()).await;
        assert!(closed(&mut first).await, "a higher rank replaces the idle peer");
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
