//! What peers send: messages about the connection's own state are applied by the `Peer`, and
//! the swarm follows up on what changed; the rest go to the part of the swarm they concern.

use crate::events::Event;
use crate::peer::{Extension, Holepunch, ProtocolViolation};
use crate::wire::{BtMessage, Extended, RejectRequest, Request};
use std::net::SocketAddr;
use tracing::{info, warn};

use super::TorrentSwarm;

impl TorrentSwarm {
    pub(super) fn on_peer_message(&mut self, idx: usize, msg: BtMessage) {
        match self.apply(idx, msg) {
            Ok(None) => {}
            Ok(Some(msg)) => self.dispatch(idx, msg),
            Err(ProtocolViolation(what)) => {
                let addr = self.peers[idx].remote_addr;
                warn!("{addr} sent {what}, disconnecting");
                self.drop_peer(idx, "protocol violation");
                self.ban(addr);
            }
        }
    }

    /// Has the peer apply `msg` to its state, and follows up on what changed: piece
    /// availability, our public address, scheduling and unchoking. A message that isn't about
    /// the peer's own state is handed back.
    fn apply(&mut self, idx: usize, msg: BtMessage) -> Result<Option<BtMessage>, ProtocolViolation> {
        let peer = &mut self.peers[idx];
        let choked = matches!(msg, BtMessage::Choke(_));
        let choked_us_before = peer.choked_us;
        let became_interested = matches!(msg, BtMessage::Interested(_)) && !peer.interested_us;
        let new_piece = match &msg {
            BtMessage::Have(have) if !peer.they_have(have.checked) => Some(have.checked),
            _ => None,
        };
        // a whole bitfield replaces whatever the peer claimed before, so its old pieces stop
        // counting towards availability and the new ones start; on a violation the old one
        // stands, and dropping the peer takes it off again
        let replaces_bitfield = matches!(
            msg,
            BtMessage::BitField(_) | BtMessage::HaveAll(_) | BtMessage::HaveNone(_)
        );
        if replaces_bitfield {
            peer.pieces().for_each(|p| self.availability[p as usize] -= 1);
        }
        let applied = peer.apply(msg);
        if replaces_bitfield {
            peer.pieces().for_each(|p| self.availability[p as usize] += 1);
        }
        if let Some(ip) = peer.yourip.take()
            && let Some(agreed) = self.external.vote(ip, &peer.remote_addr.to_string())
        {
            info!("peers agree our public address is {agreed}");
        }
        if applied.as_ref().is_ok_and(Option::is_none) {
            let choke_changed = peer.choked_us != choked_us_before;
            if choke_changed {
                tracing::debug!(parent: &peer.span, choked = peer.choked_us, "choke changed by the peer");
                self.bus.emit(Event::ChokeChanged {
                    info_hash: self.torrent.info_hash,
                    addr: peer.remote_addr,
                    choked: peer.choked_us,
                    by_us: false,
                });
            }
            if let Some(piece) = new_piece {
                self.availability[piece as usize] += 1;
                self.schedule_peer(idx);
            } else if choked {
                // BEP 3: a choke discards our outstanding requests, and nothing more will be
                // asked of the peer until it unchokes, so its pieces go back on the pile now
                // for others rather than after a stall timeout
                let addr = self.peers[idx].remote_addr;
                for piece in self.in_flight.held_by(addr) {
                    self.release_claim(piece, addr);
                }
                self.schedule();
            } else if became_interested {
                self.unchoke_if_slot_free(idx);
            } else if replaces_bitfield || choke_changed {
                // an unchoke or a bitfield may have made pieces requestable
                self.schedule();
            }
            if new_piece.is_some() || replaces_bitfield {
                self.reveal_where_spread();
            }
        }
        applied
    }

    fn dispatch(&mut self, idx: usize, msg: BtMessage) {
        match msg {
            BtMessage::Port(port) => self.ping_dht_node(idx, port.port),
            BtMessage::Request(request) => self.serve_request(idx, request),
            BtMessage::Piece(piece) => self.block_arrived(idx, piece),
            BtMessage::RejectRequest(reject) => self.request_rejected(idx, reject),
            BtMessage::Extended(ext) => self.on_extended(idx, ext),
            BtMessage::HashRequest(req) => self.answer_hash_request(idx, req),
            BtMessage::Hashes(hashes) => self.hashes_arrived(idx, hashes),
            BtMessage::HashReject(_) => {
                self.layers.give_up(self.peers[idx].remote_addr);
                self.request_layers();
            }
            other => unreachable!("Peer::apply handles everything else: {other:?}"),
        }
    }

    fn on_extended(&mut self, idx: usize, ext: Extended) {
        match Extension::from_id(ext.ext_id) {
            Some(Extension::UtMetadata) => self.serve_metadata(idx, &ext.payload),
            Some(Extension::UtPex) => self.on_pex(idx, &ext.payload),
            Some(Extension::LtDonthave) => self.on_donthave(idx, &ext.payload),
            Some(Extension::UtHolepunch) => {
                if let Some(msg) = Holepunch::decode(&ext.payload) {
                    self.on_holepunch(idx, msg);
                }
            }
            None => tracing::debug!(
                "{} sent an unsupported extended message id {}",
                self.peers[idx].remote_addr,
                ext.ext_id
            ),
        }
    }

    /// BEP 5: the peer runs a DHT node there; pinging it puts it in our routing table, which
    /// is how the table fills from a swarm rather than the routers.
    fn ping_dht_node(&self, idx: usize, port: u16) {
        let addr = self.peers[idx].remote_addr;
        let client = self.dht.borrow().as_ref().and_then(|dht| dht.client_for(&addr));
        if let Some(client) = client {
            let node = SocketAddr::new(addr.ip(), port);
            tokio::spawn(async move {
                let _ = client.ping(node).await;
            });
        }
    }

    /// BEP 6: the peer is declining a request we made; the piece it belonged to goes back on
    /// the pile rather than idling out BLOCK_REQUEST_TIMEOUT.
    fn request_rejected(&mut self, idx: usize, reject: RejectRequest) {
        let req = Request::from(reject);
        let peer = &mut self.peers[idx];
        if peer.requested.remove(&req).is_some() {
            let addr = peer.remote_addr;
            tracing::debug!("{addr} rejected {req:?}, giving up piece {} there", req.index);
            self.release_claim(req.index, addr);
            self.schedule();
        }
    }

    /// BEP 54: the peer dropped a piece; it can't be given that piece any more.
    fn on_donthave(&mut self, idx: usize, payload: &[u8]) {
        let Ok(raw) = <[u8; 4]>::try_from(payload) else {
            return;
        };
        let piece = u32::from_be_bytes(raw);
        let peer = &mut self.peers[idx];
        if !peer.drop_have(piece) {
            return;
        }
        self.availability[piece as usize] -= 1;
        let addr = peer.remote_addr;
        if self.in_flight.holds(addr, piece) {
            self.release_claim(piece, addr);
            self.schedule();
        }
    }
}
