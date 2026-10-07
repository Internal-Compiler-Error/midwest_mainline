//! What peers send: state messages are applied by the `Peer` itself, the rest go to the part
//! of the swarm they concern.

use crate::events::{Event, PeerSource};
use crate::layers::{self, MAX_HASH_READS, Received};
use crate::peer::{Extension, Holepunch, PEX_UTP, ProtocolViolation, parse_pex_message, parse_ut_metadata_request};
use crate::settings::{METADATA_PIECE_SIZE, PEX_MAX_ADDED_PEERS};
use crate::wire::{BtMessage, Request};
use std::net::SocketAddr;
use tracing::{info, warn};

use super::TorrentSwarm;

impl TorrentSwarm {
    pub(super) fn on_peer_message(&mut self, idx: usize, msg: BtMessage) {
        let peer = &mut self.peers[idx];
        let choked = matches!(msg, BtMessage::Choke(_));
        let choked_us_before = peer.choked_us;
        let became_interested = matches!(msg, BtMessage::Interested(_)) && !peer.interested_us;
        let new_piece = match &msg {
            BtMessage::Have(have)
                if (have.checked as usize) < self.availability.len() && !peer.they_have(have.checked) =>
            {
                Some(have.checked)
            }
            _ => None,
        };
        // a whole bitfield replaces whatever the peer claimed before, so its old pieces stop
        // counting towards availability and the new ones start
        let replaces_bitfield = matches!(
            msg,
            BtMessage::BitField(_) | BtMessage::HaveAll(_) | BtMessage::HaveNone(_)
        );
        if replaces_bitfield {
            for piece in peer.pieces() {
                self.availability[piece as usize] -= 1;
            }
        }
        let peer = &mut self.peers[idx];
        let applied = peer.apply(msg);
        if replaces_bitfield {
            // on a violation the old bitfield stands, and dropping the peer takes it off again
            for piece in self.peers[idx].pieces() {
                self.availability[piece as usize] += 1;
            }
        }
        let peer = &mut self.peers[idx];
        if let Some(ip) = peer.yourip.take()
            && let Some(agreed) = self.external.vote(ip, &peer.remote_addr.to_string())
        {
            info!("peers agree our public address is {agreed}");
        }
        let msg = match applied {
            Ok(None) => {
                if peer.choked_us != choked_us_before {
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
                    // BEP 3: a choke discards our outstanding requests, and nothing more
                    // will be asked of the peer until it unchokes, so its pieces go back
                    // on the pile now for others rather than after a stall timeout
                    let addr = peer.remote_addr;
                    for piece in self.in_flight.held_by(addr) {
                        self.release_claim(piece, addr);
                    }
                    self.schedule();
                } else if became_interested {
                    self.unchoke_if_slot_free(idx);
                } else if replaces_bitfield || peer.choked_us != choked_us_before {
                    // an unchoke or a bitfield may have made pieces requestable
                    self.schedule();
                }
                if new_piece.is_some() || replaces_bitfield {
                    self.reveal_where_spread();
                }
                return;
            }
            Ok(Some(msg)) => msg,
            Err(ProtocolViolation(what)) => {
                warn!("{} sent {what}, disconnecting", peer.remote_addr);
                let addr = peer.remote_addr;
                self.drop_peer(idx, "protocol violation");
                self.ban(addr);
                return;
            }
        };

        match msg {
            BtMessage::Port(port) => {
                // BEP 5: the peer runs a DHT node there; pinging it puts it in our routing
                // table, which is how the table fills from a swarm rather than the routers
                let client = self
                    .dht
                    .borrow()
                    .as_ref()
                    .and_then(|dht| dht.client_for(&peer.remote_addr));
                if let Some(client) = client {
                    let node = SocketAddr::new(peer.remote_addr.ip(), port.port);
                    tokio::spawn(async move {
                        let _ = client.ping(node).await;
                    });
                }
            }
            BtMessage::Request(request) => self.serve_request(idx, request),
            BtMessage::Piece(piece) => self.block_arrived(idx, piece),
            BtMessage::RejectRequest(reject) => {
                // BEP 6: the peer is declining a request we made; the piece it belonged to
                // goes back on the pile rather than idling out BLOCK_REQUEST_TIMEOUT
                let req = Request {
                    index: reject.index,
                    begin: reject.begin,
                    length: reject.length,
                };
                if peer.requested.remove(&req).is_some() {
                    tracing::debug!(
                        "{} rejected {req:?}, giving up piece {} there",
                        peer.remote_addr,
                        req.index
                    );
                    let addr = peer.remote_addr;
                    self.release_claim(req.index, addr);
                    self.schedule();
                }
            }
            BtMessage::Extended(ext) if Extension::from_id(ext.ext_id) == Some(Extension::UtMetadata) => {
                // BEP 9: we always have the full metadata, so any in-range piece is served
                // unconditionally
                let Some(piece) = parse_ut_metadata_request(&ext.payload) else {
                    return;
                };
                let start = piece as usize * METADATA_PIECE_SIZE;
                if start >= self.torrent.raw_info.len() {
                    return;
                }
                let end = (start + METADATA_PIECE_SIZE).min(self.torrent.raw_info.len());
                let total_size = self.torrent.metadata_size();
                let data = &self.torrent.raw_info[start..end];
                if peer.send_metadata_piece(piece, total_size, data).is_err() {
                    self.drop_peer(idx, "send failed");
                }
            }
            BtMessage::Extended(ext) if Extension::from_id(ext.ext_id) == Some(Extension::UtHolepunch) => {
                if let Some(msg) = Holepunch::decode(&ext.payload) {
                    self.on_holepunch(idx, msg);
                }
            }
            BtMessage::Extended(ext) if Extension::from_id(ext.ext_id) == Some(Extension::LtDonthave) => {
                // BEP 54: the peer dropped a piece; it can't be given that piece any more
                let Ok(raw) = <[u8; 4]>::try_from(&ext.payload[..]) else {
                    return;
                };
                let piece = u32::from_be_bytes(raw);
                if peer.drop_have(piece) {
                    self.availability[piece as usize] -= 1;
                    let addr = peer.remote_addr;
                    if self.in_flight.holds(addr, piece) {
                        self.release_claim(piece, addr);
                        self.schedule();
                    }
                }
            }
            BtMessage::Extended(ext) if Extension::from_id(ext.ext_id) == Some(Extension::UtPex) => {
                // BEP 27: don't act on PEX for a private torrent even if some peer sends it
                // anyway (we don't advertise ut_pex when private, so a compliant peer won't)
                if !self.torrent.private {
                    // BEP 11 caps a message at 50 added peers; a peer sending thousands would
                    // otherwise have us dial whoever it likes
                    let gossiped: Vec<(SocketAddr, bool)> = parse_pex_message(&ext.payload)
                        .into_iter()
                        .take(PEX_MAX_ADDED_PEERS)
                        .map(|(addr, flags)| (addr, flags & PEX_UTP != 0))
                        .collect();
                    self.bus.emit(Event::PeersDiscovered {
                        info_hash: self.torrent.info_hash,
                        source: PeerSource::Pex { from: peer.remote_addr },
                        count: gossiped.len(),
                    });
                    let via = peer.remote_addr;
                    self.connect_to_peers(gossiped, Some(via));
                }
            }
            BtMessage::Extended(ext) => {
                tracing::debug!(
                    "{} sent an unsupported extended message id {}",
                    peer.remote_addr,
                    ext.ext_id
                );
            }
            BtMessage::HashRequest(req) => {
                let reply = match layers::pieces_for(&self.torrent, &req) {
                    _ if !self.torrent.v2_consistent() => BtMessage::HashReject(req),
                    Some(pieces) => {
                        let had = pieces.clone().all(|p| self.stat.verified[p as usize]);
                        if had && self.hash_reads < MAX_HASH_READS {
                            self.answer_from_data(self.peers[idx].remote_addr, req, pieces);
                            return;
                        }
                        BtMessage::HashReject(req)
                    }
                    None => match layers::answer(&self.torrent, &mut self.hash_trees, &req) {
                        Some(hashes) => BtMessage::Hashes(hashes),
                        None => BtMessage::HashReject(req),
                    },
                };
                if self.peers[idx].send(reply).is_err() {
                    self.drop_peer(idx, "send failed");
                }
            }
            BtMessage::Hashes(hashes) => {
                let addr = self.peers[idx].remote_addr;
                match self.layers.received(&self.torrent, addr, &hashes) {
                    Received::Partial => {}
                    Received::Layer(file) => {
                        info!("piece layer of {:?} in from {addr}", self.torrent.files[file].1);
                        self.schedule();
                    }
                    Received::Bad => {
                        warn!("{addr} sent piece hashes that don't add up, disconnecting");
                        self.drop_peer(idx, "bad hashes");
                        self.ban(addr);
                    }
                }
            }
            BtMessage::HashReject(_) => {
                self.layers.give_up(self.peers[idx].remote_addr);
                self.request_layers();
            }
            other => unreachable!("Peer::apply handles everything else: {other:?}"),
        }
    }
}
