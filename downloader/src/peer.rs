use crate::torrent::Torrent;
use crate::torrent_swarm::{TorrentSwarmCommand, TorrentSwarmStats};
use crate::wire::{
    BitField, BtDecoder, BtEncoder, BtMessage, Choke, Have, Interested, NotInterested, Piece, Request, Unchoke,
};
use derive_more::{Eq, PartialEq};
use futures::SinkExt;
use futures::StreamExt;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io;
use std::net::{SocketAddr, SocketAddrV4};
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::codec::{FramedRead, FramedWrite};

use derive_more::{Display, Error};

/// It's not great that this is pub(crate) instead of fully private
pub(crate) enum PeerCommands {
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
}

pub enum PeerEvent {
    Requested(Request),
}

// TODO: Should refactor the design so that a peer connection and handle can be constructed even
// when the TCP stream is not yet established, so we can queue up messages before the connection is
// establlished. This is needed as we can have piece completion messages can need to be sent but
// the connection isn't established yet.

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerHandle {
    // TODO: i guess it's possible for multiple connections per peer, but within one download this shouldn't be true
    pub remote_peer_id: [u8; 20],

    pub remote_addr: SocketAddrV4,

    #[eq(skip)]
    pub(crate) peer_tx: mpsc::Sender<PeerCommands>,

    #[eq(skip)]
    state: watch::Receiver<PeerState>,

    #[eq(skip)]
    pub(crate) stats: watch::Receiver<PeerStatistics>,
}

#[derive(Debug, Clone, Copy, Display, Error)]
pub struct PeerDied;

impl PartialOrd for PeerHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.remote_peer_id.partial_cmp(&other.remote_peer_id)
    }
}
impl Ord for PeerHandle {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PeerHandle {
    pub fn new(
        tcp_stream: TcpStream,
        remote_peer_id: [u8; 20],
        event_tx: mpsc::Sender<TorrentSwarmCommand>,
        torrent_status: watch::Receiver<TorrentSwarmStats>,
        torrent: &Torrent,
    ) -> Self {
        let remote_addr = tcp_stream.peer_addr().unwrap();
        let SocketAddr::V4(remote_addr) = remote_addr else {
            panic!("we only support ipv4");
        };
        let (reader, writer) = tcp_stream.into_split();
        let (commands_tx, commands_rx) = mpsc::channel(1024);

        // Create watch channel for this peer's statistics
        let initial_stats = PeerStatistics::default();
        let (stats_tx, stats_rx) = watch::channel(initial_stats);

        let state = PeerState {
            they_have: vec![0u8; torrent.pieces.len()].into(),
            choked_us: false,
            choked_them: false,
            interested_them: false,
            interested_us: false,
        };
        let (state_tx, state_rx) = watch::channel(state.clone());

        let peer = PeerConnection {
            commands: commands_rx,
            events: event_tx,

            state: state.clone(),
            state_tx,

            remote_addr,
            reader: FramedRead::new(reader, BtDecoder),
            writer: FramedWrite::new(writer, BtEncoder),
            requested: Default::default(),

            stats: Default::default(),
            stats_tx,

            torrent_stat: torrent_status,
        };

        tokio::spawn(peer_ev_loop(peer));

        Self {
            remote_peer_id,
            peer_tx: commands_tx,
            stats: stats_rx,
            state: state_rx.clone(),
            remote_addr,
        }
    }

    // TODO: this one probably has a more complicated error scenario
    pub async fn request_data_from_peer(&self, req: Request) -> anyhow::Result<Box<[u8]>> {
        let (syn, ack) = oneshot::channel();

        self.peer_tx
            .send(PeerCommands::RequestDataFromPeer { req, syn })
            .await?;
        let data = ack.await?;
        Ok(data)
    }

    pub async fn fancy_peer(&self) -> Result<(), PeerDied> {
        self.peer_tx.send(PeerCommands::FancyPeer).await.map_err(|_| PeerDied)?;
        Ok(())
    }

    pub fn try_fancy_peer(&self) -> Result<(), PeerDied> {
        self.peer_tx.try_send(PeerCommands::FancyPeer).map_err(|_| PeerDied)?;
        Ok(())
    }

    /// Tell the peer that we now have a particular piece, note as with all functions on the
    /// handle, this only sends a message to the message channel linking the peer, the completion
    /// of this funciton doesn't mean the message has been copied to kernel network buffer
    pub async fn send_we_have(&self, piece: u32) -> Result<(), PeerDied> {
        self.peer_tx
            .send(PeerCommands::SendWeHave(piece))
            .await
            .map_err(|_| PeerDied)?;
        Ok(())
    }

    pub async fn bit_field(&self, bit_field: BitField) -> Result<(), PeerDied> {
        self.peer_tx
            .send(PeerCommands::BitField(bit_field))
            .await
            .map_err(|_| PeerDied)?;
        Ok(())
    }

    /// Try to send the bitfield message without blocking/awaiting
    pub fn try_bitfield(&self, bit_field: BitField) -> Result<(), PeerDied> {
        self.peer_tx
            .try_send(PeerCommands::BitField(bit_field))
            .map_err(|_| PeerDied)?;
        Ok(())
    }

    pub async fn unchoke_peer(&self) -> Result<(), PeerDied> {
        self.peer_tx
            .send(PeerCommands::UnchokePeer)
            .await
            .map_err(|_| PeerDied)?;
        Ok(())
    }

    pub fn try_unchoke_peer(&self) -> Result<(), PeerDied> {
        self.peer_tx.try_send(PeerCommands::UnchokePeer).map_err(|_| PeerDied)?;
        Ok(())
    }

    pub async fn send_data(&self, piece: Piece) -> Result<(), PeerDied> {
        self.peer_tx
            .send(PeerCommands::SendData(piece))
            .await
            .map_err(|_| PeerDied)?;
        Ok(())
    }

    pub fn state(&self) -> PeerState {
        self.state.borrow().clone()
    }

    pub fn stats(&self) -> PeerStatistics {
        self.stats.borrow().clone()
    }

    pub fn ready(&self) -> bool {
        self.state().ready()
    }
}

/// Represents an active connection to a peer in the BitTorrent network
#[derive(Debug)]
struct PeerConnection {
    commands: Receiver<PeerCommands>,
    // we never ask for anything from the torrent swarm, only posting events, they handle want to do with
    // us using the channel above
    events: mpsc::Sender<TorrentSwarmCommand>,

    state: PeerState,
    state_tx: watch::Sender<PeerState>,

    remote_addr: SocketAddrV4,
    reader: FramedRead<OwnedReadHalf, BtDecoder>,
    writer: FramedWrite<OwnedWriteHalf, BtEncoder>,

    requested: BTreeMap<Request, (oneshot::Sender<Box<[u8]>>, Instant)>,

    torrent_stat: watch::Receiver<TorrentSwarmStats>,

    stats: PeerStatistics,
    stats_tx: watch::Sender<PeerStatistics>,
    ///// if the connection is closed, then the peer should be disposed off when able
    // connection_closed: bool,
}

async fn peer_ev_loop(mut peer: PeerConnection) {
    loop {
        tokio::select! {
            // Handle commands from the peer handle
            Some(command) = peer.commands.recv() => { peer.process_command(command).await; }

            // Process incoming BitTorrent protocol messages
            Some(Ok(msg)) = peer.reader.next() => { peer.process_message(msg).await }

            // Exit loop if all channels are closed
            else => break,
        }
    }
}

impl PeerConnection {
    async fn emit_event(&mut self, event: PeerEvent) {
        self.events
            .send(TorrentSwarmCommand::ProcessPeerEvent {
                from: self.remote_addr,
                event: event,
            })
            .await
            .unwrap();
    }

    pub async fn request_data_from_peer(
        &mut self,
        req: Request,
        syn: oneshot::Sender<Box<[u8]>>,
    ) -> anyhow::Result<()> {
        self.stats.picked_count += 1;
        self.publish_stat();

        self.requested.insert(req, (syn, Instant::now()));
        self.writer.send(BtMessage::Request(req)).await?;

        Ok(())
    }

    pub async fn unchoke_peer(&mut self) -> io::Result<()> {
        let unchoke = Unchoke;
        self.writer.send(BtMessage::Unchoke(unchoke)).await?;
        self.state.choked_them = false;

        Ok(())
    }

    pub async fn choke_peer(&mut self) -> io::Result<()> {
        let choke = Choke;
        self.writer.send(BtMessage::Choke(choke)).await?;
        self.state.choked_them = true;

        Ok(())
    }

    pub async fn fancy_peer(&mut self) -> io::Result<()> {
        let interested = Interested;
        self.writer.send(BtMessage::Interested(interested)).await?;
        self.state.interested_them = true;

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn unfancy_peer(&mut self) -> io::Result<()> {
        let not_interested = NotInterested;
        self.writer.send(BtMessage::NotInterested(not_interested)).await?;
        self.state.interested_them = false;

        Ok(())
    }

    pub async fn send_bitfield(&mut self, bit_field: BitField) -> io::Result<()> {
        self.writer.send(BtMessage::BitField(bit_field)).await?;
        Ok(())
    }

    pub async fn send_we_have(&mut self, index: u32) -> io::Result<()> {
        let connection_state = { self.state.clone() };
        if !connection_state.interested_us || connection_state.choked_them {
            return Ok(());
        }

        let have = Have { checked: index };
        self.writer.send(BtMessage::Have(have)).await?;

        Ok(())
    }

    pub async fn send_data(&mut self, piece: Piece) -> anyhow::Result<()> {
        let length = piece.length.clone();
        self.writer.send(BtMessage::Piece(piece)).await?;
        self.stats.sent += length as usize;
        self.publish_stat();

        Ok(())
    }

    async fn process_command(&mut self, command: PeerCommands) {
        match command {
            PeerCommands::UnchokePeer => self.unchoke_peer().await.unwrap(),
            PeerCommands::ChokePeer => self.choke_peer().await.unwrap(),
            PeerCommands::RequestDataFromPeer { req, syn } => self.request_data_from_peer(req, syn).await.unwrap(),
            PeerCommands::FancyPeer => self.fancy_peer().await.unwrap(),
            PeerCommands::SendWeHave(piece) => self.send_we_have(piece).await.unwrap(),
            PeerCommands::BitField(bitfield) => self.send_bitfield(bitfield).await.unwrap(),
            PeerCommands::SendData(piece) => self.send_data(piece).await.unwrap(),
        }
    }

    pub async fn process_message(&mut self, msg: BtMessage) {
        match msg {
            BtMessage::KeepAlive(_) => return,
            BtMessage::Choke(_) => self.state.choked_us = true,
            BtMessage::Unchoke(_) => self.state.choked_us = false,
            BtMessage::Interested(_) => {
                self.state.interested_us = true;
                let _ = self.unchoke_peer().await;
            }
            BtMessage::NotInterested(_) => self.state.interested_us = false,
            BtMessage::Have(have) => {
                let index = have.checked / 8;
                let offset = have.checked % 8;

                let flag = 1u8 << offset;
                self.state.they_have[index as usize] |= flag;
            }
            BtMessage::BitField(bit_field) => {
                self.state.they_have = bit_field.has;
            }
            BtMessage::Request(request) => {
                self.events
                    .send(TorrentSwarmCommand::ProcessPeerEvent {
                        from: self.remote_addr,
                        event: PeerEvent::Requested(request),
                    })
                    .await
                    .expect("They kill us and not the other way around, TorrentSwarm outlives us");
            }
            BtMessage::Piece(piece) => {
                if !self.requested.contains_key(&Request {
                    index: piece.index,
                    begin: piece.begin,
                    length: piece.length,
                }) {
                    let _ = self.writer.close().await;
                    // TODO: let the handle know in someway
                    return;
                }

                let (syn, requested_time) = self
                    .requested
                    .remove(&Request {
                        index: piece.index,
                        begin: piece.begin,
                        length: piece.length,
                    })
                    .unwrap();

                self.stats.received += piece.length as usize;

                let speed = (piece.length as f64) / (Instant::now() - requested_time).as_secs_f64();
                self.update_speed_estimation(speed);
                self.publish_stat();
                let _ = syn.send(piece.data);
            }
            BtMessage::Cancel(_cancel) => return,
            BtMessage::Unknown(msg_type, _) => {
                tracing::warn!("Unsupported message type {msg_type}");
            }
        }
        let _ = self.state_tx.send(self.state.clone());
    }

    fn publish_stat(&self) {
        // TODO: don't only publish every .5 second or something
        let _ = self.stats_tx.send(self.stats);
    }

    fn update_speed_estimation(&mut self, sampled_speed: f64) {
        // online average update formula
        self.stats.mean_rx_cnt += 1;
        self.stats.mean_rx = self.stats.mean_rx + (sampled_speed - self.stats.mean_rx) / self.stats.mean_rx_cnt as f64;
    }
}

#[derive(Clone, Debug)]
pub struct PeerState {
    // TODO: it's only used when selecting peers, probably shouldn't be here
    they_have: Box<[u8]>,

    /// We choked the peer, i.e. we won't send data until we unchoke them
    pub choked_them: bool,

    /// The peer choked us, i.e. they won't send data until they unchoke us
    pub choked_us: bool,

    /// We are interested in them, i.e. they have something we want
    pub interested_them: bool,

    /// They are interested in us, i.e. they want something from us
    pub interested_us: bool,
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
        !self.choked_us
    }
}

#[derive(Clone, Debug, Copy, PartialEq, Default)]
pub struct PeerStatistics {
    /// Number of bytes we've sent to the peer
    pub sent: usize,

    /// Number of bytes we've received from the peer
    pub received: usize,

    /// Mean download speed from this peer
    pub mean_rx: f64,

    /// how many times have the download speed been sampled, mostly used in online averaging
    /// calculation
    pub mean_rx_cnt: usize,

    /// how many times this peer has been chosen to request a piece
    pub picked_count: usize,
}

impl PeerStatistics {
    /// Calculate the peer's download's upper confidence bound based on how many pieces have been
    /// requested
    pub fn rx_speed_ucb(&self, total_piece_requested: usize) -> f64 {
        // Upper Confidence Bound
        let c = 1f64;
        let t = total_piece_requested as f64;
        let n_t = self.picked_count as f64;
        self.mean_rx + c * (t.ln() / n_t).sqrt()
    }

    // In UCB, when an arm hasn't been played yet, it should be picked first, instead of doing an
    // if check every time we choose a peer, we just assign infinite score to peers who haven't
    // been requested yet
    pub fn score(&self, total_piece_requested: usize) -> f64 {
        if self.picked_count == 0 {
            f64::INFINITY
        } else {
            self.rx_speed_ucb(total_piece_requested)
        }
    }
}
