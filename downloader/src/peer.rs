pub mod download;
pub mod torrent_swarm;
mod wire;

use derive_more::{Eq, PartialEq};
use futures::SinkExt;
use futures::StreamExt;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::mem::zeroed;
use std::net::SocketAddrV4;
use std::os::fd::{AsRawFd};
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;
use std::{io, mem, sync::Arc};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::codec::{FramedRead, FramedWrite};
use crate::sys_tcp;

use wire::{
    BitField, BtDecoder, BtEncoder, BtMessage, Choke, Have, Interested, NotInterested, Piece, Request, Unchoke,
};
use crate::peer::torrent_swarm::{TorrentSwarmCommand, TorrentSwarmStats};

pub enum PeerCommands {
    UnchokePeer,
    #[allow(dead_code)]
    ChokePeer,
    FancyPeer,
    RequestDataFromPeer {
        req: Request,
        syn: oneshot::Sender<Box<[u8]>>,
    },
    SendWeHave(u32),
    BitField(BitField),
    SendData(Piece),
    UpdateStats { verified: usize },
}

pub enum PeerEvent {
    Requested(Request),
    UpdateStatsReady,
}

pub async fn peer_ev_loop(mut peer: PeerConnection) {
    let mut stats_interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            // Handle periodic stats updates
            _ = stats_interval.tick() => {
                peer.emit_event(PeerEvent::UpdateStatsReady).await;
            }

            // Handle commands from the peer handle
            Some(command) = peer.commands.recv() => {
                // TODO: at least don't send the data when they're not interested or we've choked them or something
                match command {
                    PeerCommands::UnchokePeer => peer.unchoke_peer().await.unwrap(),
                    PeerCommands::ChokePeer => peer.choke_peer().await.unwrap(),
                    PeerCommands::RequestDataFromPeer{req,syn} => peer.request_data_from_peer(req,syn).await.unwrap(),
                    PeerCommands::FancyPeer => peer.fancy_peer().await.unwrap(),
                    PeerCommands::SendWeHave(piece) => peer.send_we_have(piece).await.unwrap(),
                    PeerCommands::BitField(bitfield) => peer.send_bitfield(bitfield).await.unwrap(),
                    PeerCommands::SendData(piece) => peer.send_data(piece).await.unwrap(),
                    PeerCommands::UpdateStats{verified} => peer.update_stat(verified),
                }
            }

            // Process incoming BitTorrent protocol messages
            Some(Ok(msg)) = peer.reader.next() => {
                peer.process_message(msg).await
            }

            // Exit loop if all channels are closed
            else => break,
        }
    }
}

/// Represents an active connection to a peer in the BitTorrent network
pub struct PeerConnection {
    commands: Receiver<PeerCommands>,
    // we never ask for anything from the torrent swarm, only posting events, they handle want to do with
    // us using the channel above
    events: mpsc::Sender<TorrentSwarmCommand>,

    // torrent: Arc<Torrent>,
    state: Arc<RwLock<PeerState>>,

    remote_addr: SocketAddrV4,
    reader: FramedRead<OwnedReadHalf, BtDecoder>,
    writer: FramedWrite<OwnedWriteHalf, BtEncoder>,

    requested: BTreeMap<Request, oneshot::Sender<Box<[u8]>>>,

    stat: PeerStatistics,

    torrent_stat: watch::Receiver<TorrentSwarmStats>,

    /// Watch channel to publish statistics updates
    stats_tx: watch::Sender<PeerStatistics>,
    ///// if the connection is closed, then the peer should be disposed off when able
    // connection_closed: bool,
}

#[derive(Clone, Debug)]
pub struct PeerState {
    they_have: Box<[u8]>,

    /// We choked the peer, i.e. we won't send data until we unchoke them
    pub choked_them: bool,

    /// The peer choked us, i.e. they won't send data until they unchoke us
    pub choked_us: bool,

    /// We are interested in them, i.e. they have something we want
    pub interested_them: bool,

    /// They are interested in us, i.e. they want something from us
    pub interested_us: bool,

    requested: BTreeMap<Request, Instant>,
}

impl PeerState {
    pub fn they_have(&self, piece: u32) -> bool {
        let index = piece / 8;
        let offset = piece % 8;

        let flag = 1u8 << offset;
        (self.they_have[index as usize] & flag) != 0
    }

    /// Is the peer ready for more requests?
    pub fn ready(&self) -> bool {
        self.requested.len() <= 500 && !self.choked_us
    }
}

#[derive(Clone, Debug, Copy)]
pub struct PeerStatistics {
    pub tcp_info: sys_tcp::tcp_info,
    pub mean_rx: f64,
    pub mean_rx_cnt: usize,
    pub mean_rx_last_checked: Instant,

    /// upper confidence bound
    pub ucb: f64,
    pub picked_count: usize,
}

impl PeerConnection {
    async fn emit_event(&mut self, event: PeerEvent) {
        self.events.send(TorrentSwarmCommand::ProcessPeerEvent {
            from: self.remote_addr,
            event: event,
        }).await.unwrap();
    }

    pub async fn request_data_from_peer(
        &mut self,
        req: Request,
        syn: oneshot::Sender<Box<[u8]>>,
    ) -> anyhow::Result<()> {
        self.requested.insert(req, syn);
        self.writer.send(BtMessage::Request(req)).await?;

        Ok(())
    }

    pub async fn unchoke_peer(&mut self) -> io::Result<()> {
        let unchoke = Unchoke;
        self.writer.send(BtMessage::Unchoke(unchoke)).await?;
        self.state.write().unwrap().choked_them = false;

        Ok(())
    }

    pub async fn choke_peer(&mut self) -> io::Result<()> {
        let choke = Choke;
        self.writer.send(BtMessage::Choke(choke)).await?;
        self.state.write().unwrap().choked_them = true;

        Ok(())
    }

    pub async fn fancy_peer(&mut self) -> io::Result<()> {
        let interested = Interested;
        self.writer.send(BtMessage::Interested(interested)).await?;
        self.state.write().unwrap().interested_them = true;

        Ok(())
    }

    pub async fn unfancy_peer(&mut self) -> io::Result<()> {
        let not_interested = NotInterested;
        self.writer.send(BtMessage::NotInterested(not_interested)).await?;
        self.state.write().unwrap().interested_them = false;

        Ok(())
    }

    pub async fn send_bitfield(&mut self, bit_field: BitField) -> io::Result<()> {
        self.writer.send(BtMessage::BitField(bit_field)).await?;
        Ok(())
    }

    pub async fn send_we_have(&mut self, index: u32) -> io::Result<()> {
        let connection_state = {
            self.state.read().unwrap().clone()
        };
        if !connection_state.interested_us || connection_state.choked_them {
            return Ok(());
        }

        let have = Have { checked: index };
        self.writer.send(BtMessage::Have(have)).await?;

        Ok(())
    }


    pub async fn send_data(&mut self, piece: Piece) -> anyhow::Result<()> {
        self.writer.send(BtMessage::Piece(piece)).await?;
        Ok(())
    }

    async fn process_command(&mut self, command: PeerCommands) {
        match command {
            PeerCommands::UnchokePeer => self.unchoke_peer().await.unwrap(),
            PeerCommands::ChokePeer => self.choke_peer().await.unwrap(),
            PeerCommands::RequestDataFromPeer{req,syn} => self.request_data_from_peer(req,syn).await.unwrap(),
            PeerCommands::FancyPeer => self.fancy_peer().await.unwrap(),
            PeerCommands::SendWeHave(piece) => self.send_we_have(piece).await.unwrap(),
            PeerCommands::BitField(bitfield) => self.send_bitfield(bitfield).await.unwrap(),
            PeerCommands::SendData(piece) => self.send_data(piece).await.unwrap(),
            PeerCommands::UpdateStats{verified} => self.update_stat(verified),
        }
    }

    pub async fn process_message(&mut self, msg: BtMessage) {
        match msg {
            BtMessage::KeepAlive(_) => return,
            BtMessage::Choke(_) => self.state.write().unwrap().choked_us = true,
            BtMessage::Unchoke(_) => self.state.write().unwrap().choked_us = false,
            BtMessage::Interested(_) => {
                self.state.write().unwrap().interested_us = true;
                let _ = self.unchoke_peer().await;
            }
            BtMessage::NotInterested(_) => self.state.write().unwrap().interested_us = false,
            BtMessage::Have(have) => {
                let index = have.checked / 8;
                let offset = have.checked % 8;

                let flag = 1u8 << offset;
                self.state.write().unwrap().they_have[index as usize] |= flag;
            }
            BtMessage::BitField(bit_field) => {
                self.state.write().unwrap().they_have = bit_field.has;
            }
            BtMessage::Request(request) => {
                self.events.send(TorrentSwarmCommand::ProcessPeerEvent {
                    from: self.remote_addr,
                    event: PeerEvent::Requested(request),
                }).await.expect("They kill us and not the other way around, TorrentSwarm outlives us");
            }
            BtMessage::Piece(piece) => {
                if !self.requested.contains_key(&Request {
                    index: piece.index,
                    begin: piece.begin,
                    length: piece.length,
                }) {
                    // be wary of strangers sending data you didn't ask for, ignore them
                    return;
                }

                let syn = self
                    .requested
                    .remove(&Request {
                        index: piece.index,
                        begin: piece.begin,
                        length: piece.length,
                    })
                    .unwrap();

                let _ = syn.send(piece.data);
            }
            BtMessage::Cancel(_cancel) => return,
            BtMessage::Unknown(msg_type, _) => {
                tracing::warn!("Unsupported message type {msg_type}");
            }
        }
    }
    pub fn update_stat(&mut self, verified_cnt: usize) {
        if Instant::now().duration_since(self.stat.mean_rx_last_checked) < Duration::from_secs(1) {
            return;
        }

        let fd = self.writer.get_ref().as_ref().as_raw_fd();

        unsafe {
            let mut len = mem::size_of::<sys_tcp::tcp_info>() as libc::socklen_t;
            libc::getsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_INFO,
                &mut self.stat.tcp_info as *mut _ as *mut libc::c_void,
                &mut len,
            );
        };

        // online average update formula
        self.stat.mean_rx_cnt += 1;
        self.stat.mean_rx = self.stat.mean_rx
            + (self.stat.tcp_info.tcpi_delivery_rate as f64 - self.stat.mean_rx) / self.stat.mean_rx_cnt as f64;
        self.stat.mean_rx_last_checked = Instant::now();

        // Upper Confidence Bound
        let c = 1f64;
        let t = verified_cnt as f64;
        let n_t = self.stat.picked_count as f64;
        self.stat.ucb = self.stat.mean_rx + c * (t.ln() / n_t).sqrt();

        // Publish updated statistics via watch channel
        let _ = self.stats_tx.send(self.stat);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerHandle {
    // TODO: i guess it's possible for multiple connections per peer, but within one download this shouldn't be true
    pub id: [u8; 20],

    pub remote_addr: SocketAddrV4,

    #[eq(skip)]
    peer_tx: mpsc::Sender<PeerCommands>,

    #[eq(skip)]
    pub state: Arc<RwLock<PeerState>>,

    #[eq(skip)]
    stats: watch::Receiver<PeerStatistics>,
}

impl PartialOrd for PeerHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id.partial_cmp(&other.id)
    }
}
impl Ord for PeerHandle {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PeerHandle {
    pub async fn request_data_from_peer(&self, req: Request) -> anyhow::Result<Box<[u8]>> {
        let (syn, ack) = oneshot::channel();

        self.peer_tx.send(PeerCommands::RequestDataFromPeer { req, syn }).await?;
        let data = ack.await?;
        Ok(data)
    }

    pub async fn fancy_peer(&self) -> anyhow::Result<()> {
        self.peer_tx.send(PeerCommands::FancyPeer).await?;
        Ok(())
    }

    pub async fn send_we_have(&self, piece: u32) -> anyhow::Result<()> {
        self.peer_tx.send(PeerCommands::SendWeHave(piece)).await?;
        Ok(())
    }

    pub async fn bit_field(&self, bit_field: BitField) -> anyhow::Result<()> {
        self.peer_tx.send(PeerCommands::BitField(bit_field)).await?;
        Ok(())
    }

    pub async fn unchoke_peer(&self) -> anyhow::Result<()> {
        self.peer_tx.send(PeerCommands::UnchokePeer).await?;
        Ok(())
    }

    pub async fn send_data(&self, piece: Piece) -> anyhow::Result<()> {
        self.peer_tx.send(PeerCommands::SendData(piece)).await?;
        Ok(())
    }

    pub async fn update_stats(&mut self, verified_cnt: usize) -> anyhow::Result<()> {
        let _ = self.peer_tx.send(PeerCommands::UpdateStats { verified: verified_cnt }).await?;
        Ok(())
    }
}

