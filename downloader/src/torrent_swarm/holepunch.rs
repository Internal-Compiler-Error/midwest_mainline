//! BEP 55 holepunching, as the one asking for an introduction and as the relay.

use crate::peer::{Extension, Holepunch, HolepunchError};
use std::net::SocketAddr;
use std::time::Instant;

use super::{TorrentSwarm, canonical, known::KnownPeer};

impl TorrentSwarm {
    /// BEP 55, as the initiator: a peer we couldn't dial may be behind a NAT that only lets in
    /// what it sent out to first. The peer that told us about it is connected to it, so it can
    /// tell both of us to connect at once (over uTP), which opens both NATs. Once per address.
    pub(super) fn try_holepunch(&mut self, addr: SocketAddr) {
        let Some(known) = self.known.get_mut(&addr) else {
            return;
        };
        let Some(relay) = known.via.filter(|_| !known.holepunched) else {
            return;
        };
        let Some(idx) = self
            .peer_index(relay)
            .filter(|&idx| self.peers[idx].their_id(Extension::UtHolepunch).is_some())
        else {
            return;
        };
        if self.utp.borrow().is_none() {
            return;
        }
        if let Some(known) = self.known.get_mut(&addr) {
            known.holepunched = true;
        }
        tracing::debug!("asking {relay} to introduce us to {addr} (holepunch)");
        if self.peers[idx].send_holepunch(Holepunch::Rendezvous(addr)).is_err() {
            self.drop_peer(idx, "send failed");
        }
    }

    /// BEP 55: a holepunch message from the peer at `idx`.
    pub(super) fn on_holepunch(&mut self, idx: usize, msg: Holepunch) {
        let from = self.peers[idx].remote_addr;
        match msg {
            // we're the relay: introduce the two, or say why not
            Holepunch::Rendezvous(target) => {
                let target = canonical(target);
                let error = if target == from || target == self.peers[idx].reachable_addr() {
                    Some(HolepunchError::NoSelf)
                } else {
                    match self
                        .peers
                        .iter()
                        .position(|p| p.reachable_addr() == target || p.remote_addr == target)
                    {
                        None => Some(HolepunchError::NotConnected),
                        Some(t) if self.peers[t].their_id(Extension::UtHolepunch).is_none() => {
                            Some(HolepunchError::NoSupport)
                        }
                        Some(t) => {
                            let initiator = self.peers[idx].reachable_addr();
                            tracing::debug!("introducing {from} and {target} (holepunch)");
                            let to_target = self.peers[t].send_holepunch(Holepunch::Connect(initiator));
                            let to_initiator = self.peers[idx].send_holepunch(Holepunch::Connect(target));
                            if to_target.is_err() || to_initiator.is_err() {
                                tracing::debug!("couldn't pass on a holepunch between {from} and {target}");
                            }
                            None
                        }
                    }
                };
                if let Some(error) = error {
                    let _ = self.peers[idx].send_holepunch(Holepunch::Error(target, error));
                }
            }
            // a relay introduced us: dial now, over uTP, while the other side dials us
            Holepunch::Connect(addr) => {
                let addr = canonical(addr);
                let cap = self.settings.borrow().peer_cap();
                // a dial backoff doesn't count: a peer we failed to dial is who we asked for
                let banned = self.known.get(&addr).is_some_and(|k| k.banned(Instant::now()));
                if self.utp.borrow().is_none()
                    || !self.room_to_dial(cap)
                    || banned
                    || addr.port() == 0
                    || self.peer_index(addr).is_some()
                    || self.dialing.contains(&addr)
                {
                    return;
                }
                self.dialing.insert(addr);
                let mut hints = self.known.get(&addr).map(KnownPeer::dial_hints).unwrap_or_default();
                hints.utp_only = true;
                tracing::debug!("{from} introduced us to {addr}, dialing (holepunch)");
                self.spawn_dial(addr, hints);
            }
            Holepunch::Error(addr, error) => tracing::debug!("{from} couldn't introduce us to {addr}: {error:?}"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::connect::MAX_PENDING_DIALS;
    use super::super::test_support::*;
    use super::*;

    /// BEP 55, as the relay: two peers that speak holepunch are introduced to each other on
    /// one's request, and a request for a peer we don't have gets the matching error.
    #[tokio::test]
    async fn relays_a_holepunch_between_two_peers() {
        let (swarm, handle, path) = swarm_with("holepunch", true);
        tokio::spawn(swarm.work_loop());
        let (a_addr, b_addr): (SocketAddr, SocketAddr) =
            ("10.0.0.1:6881".parse().unwrap(), "10.0.0.2:6881".parse().unwrap());
        let mut a = fake_peer(&handle, &a_addr.to_string()).await;
        let mut b = fake_peer(&handle, &b_addr.to_string()).await;
        let speaks_holepunch = BtMessage::Extended(crate::wire::Extended {
            ext_id: 0,
            payload: tokio_util::bytes::Bytes::from_static(b"d1:md12:ut_holepunchi9eee"),
        });
        for peer in [&mut a, &mut b] {
            peer.send(speaks_holepunch.clone()).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let ask = |msg: Holepunch| {
            BtMessage::Extended(crate::wire::Extended {
                ext_id: Extension::UtHolepunch.id(),
                payload: msg.encode().into(),
            })
        };
        a.send(ask(Holepunch::Rendezvous(b_addr))).await.unwrap();

        // the first holepunch message each gets, skipping the opening exchange
        async fn next_holepunch(peer: &mut Wire) -> Holepunch {
            loop {
                if let Some(Ok(BtMessage::Extended(ext))) = peer.next().await
                    && ext.ext_id == 9
                {
                    return Holepunch::decode(&ext.payload).unwrap();
                }
            }
        }
        let timeout = Duration::from_secs(5);
        assert_eq!(
            tokio::time::timeout(timeout, next_holepunch(&mut b)).await.unwrap(),
            Holepunch::Connect(a_addr)
        );
        assert_eq!(
            tokio::time::timeout(timeout, next_holepunch(&mut a)).await.unwrap(),
            Holepunch::Connect(b_addr)
        );

        let stranger: SocketAddr = "10.0.0.9:1".parse().unwrap();
        a.send(ask(Holepunch::Rendezvous(stranger))).await.unwrap();
        assert_eq!(
            tokio::time::timeout(timeout, next_holepunch(&mut a)).await.unwrap(),
            Holepunch::Error(stranger, HolepunchError::NotConnected)
        );
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// A peer can send introductions (and PEX) as fast as it likes: the dials they start stay
    /// bounded, and a banned address isn't dialled because someone introduced it.
    #[tokio::test]
    async fn introductions_and_gossip_cannot_queue_unbounded_dials() {
        let (mut swarm, _handle, path) = swarm_with("holepunch-flood", true);
        let mut utp = crate::utp::start(0, tokio_util::sync::CancellationToken::new());
        utp.wait_for(Option::is_some).await.unwrap();
        swarm.utp = utp;
        let (relay, _theirs) = fake_connection("10.0.0.1:6881", false).await;
        swarm.add_peer(relay);

        let banned: SocketAddr = "127.0.0.1:9".parse().unwrap();
        swarm.ban(banned);
        swarm.on_holepunch(0, Holepunch::Connect(banned));
        assert!(swarm.dialing.is_empty(), "a banned peer was dialled on an introduction");

        let local = |n: usize| SocketAddr::from((Ipv4Addr::LOCALHOST, 10_000 + n as u16));
        for n in 0..MAX_PENDING_DIALS {
            swarm.on_holepunch(0, Holepunch::Connect(local(n)));
        }
        let gossip: Vec<_> = (MAX_PENDING_DIALS..2 * MAX_PENDING_DIALS)
            .map(|n| (local(n), false))
            .collect();
        swarm.connect_to_peers(gossip, None);
        assert_eq!(swarm.dialing.len(), MAX_PENDING_DIALS);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
