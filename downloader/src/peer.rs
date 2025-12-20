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
use std::os::fd::AsRawFd;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;
use std::{io, mem, sync::Arc};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::storage::TorrentStorage;
use crate::sys_tcp;
use crate::torrent::Torrent;

use wire::{
    BitField, BtDecoder, BtEncoder, BtMessage, Choke, Have, Interested, NotInterested, Piece, Request, Unchoke,
    shake_hands,
};

pub enum PeerCommands {
    UnchokePeer,
    #[allow(dead_code)]
    ChokePeer,
    FancyPeer,
    RequestDataFromPeer {
        req: Request,
        syn: oneshot::Sender<Box<[u8]>>,
    },
    WeHave(u32),
    BitField,
}

pub async fn peer_ev_loop(mut peer: PeerConnection) {
    let mut stats_interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            // Handle periodic stats updates
            _ = stats_interval.tick() => {
                peer.update_stat().await;
            }

            // Handle commands from the peer handle
            Some(command) = peer.commands.recv() => {
                match command {
                    PeerCommands::UnchokePeer => peer.unchoke_peer().await.unwrap(),
                    PeerCommands::ChokePeer => peer.choke_peer().await.unwrap(),
                    PeerCommands::RequestDataFromPeer{req,syn} => peer.request_data_from_peer(req,syn).await.unwrap(),
                    PeerCommands::FancyPeer => peer.fancy_peer().await.unwrap(),
                    PeerCommands::WeHave(piece) => peer.we_have(piece).await.unwrap(),
                    PeerCommands::BitField => peer.send_bitfield().await.unwrap(),
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

    // torrent: Arc<Torrent>,
    state: Arc<RwLock<PeerState>>,

    // remote_addr: SocketAddrV4,
    reader: FramedRead<OwnedReadHalf, BtDecoder>,
    writer: FramedWrite<OwnedWriteHalf, BtEncoder>,
    storage: Arc<TorrentStorage>,

    requested: BTreeMap<Request, oneshot::Sender<Box<[u8]>>>,

    stat: PeerStatistics,

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
    fn new(
        commands: Receiver<PeerCommands>,
        // torrent: Arc<Torrent>,
        state: Arc<RwLock<PeerState>>,
        reader: FramedRead<OwnedReadHalf, BtDecoder>,
        writer: FramedWrite<OwnedWriteHalf, BtEncoder>,
        storage: Arc<TorrentStorage>,
        stats_tx: watch::Sender<PeerStatistics>,
        // remote_addr: SocketAddrV4,
    ) -> Self {
        let initial_stats = PeerStatistics {
            tcp_info: unsafe { zeroed() },
            mean_rx: 0.0,
            mean_rx_cnt: 0,
            mean_rx_last_checked: Instant::now(),
            ucb: 0.0,
            picked_count: 0,
        };

        PeerConnection {
            commands,
            // torrent,
            state,
            reader,
            writer,
            storage,
            requested: Default::default(),
            stat: initial_stats,
            stats_tx,
            // connection_closed: false,
            // remote_addr,
        }
    }

    pub fn share_states(&self) -> Arc<RwLock<PeerState>> {
        Arc::clone(&self.state)
    }

    //
    // pub fn prune_expired_reqs(&mut self) {
    //     let now = Instant::now();
    //     self.requested
    //         .retain(|_k, v| now.duration_since(*v) <= Duration::from_secs(120));
    // }

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

    pub async fn send_bitfield(&mut self) -> io::Result<()> {
        let bit_field = BitField {
            has: self.storage.verified().as_raw_slice().into(),
        };
        self.writer.send(BtMessage::BitField(bit_field)).await?;

        Ok(())
    }

    pub async fn we_have(&mut self, index: u32) -> io::Result<()> {
        let have = Have { checked: index };
        self.writer.send(BtMessage::Have(have)).await?;

        Ok(())
    }

    pub async fn update_stat(&mut self) {
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
        let t = self.storage.verified_cnt() as f64;
        let n_t = self.stat.picked_count as f64;
        self.stat.ucb = self.stat.mean_rx + c * (t.ln() / n_t).sqrt();

        // Publish updated statistics via watch channel
        let _ = self.stats_tx.send(self.stat);
    }

    /// Reads and processes one incoming BitTorrent protocol message from this peer
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
                let data = self.storage.read_piece(request.index);
                if data.is_err() {
                    return;
                }

                let data = data.unwrap();
                let (_head, tail) = data.split_at(request.begin as usize);
                let tail: Box<[u8]> = tail.into();

                let resp = BtMessage::Piece(Piece {
                    index: request.index,
                    begin: request.begin,
                    length: (data.len() - request.begin as usize) as u32,
                    data: tail,
                });
                self.writer.send(resp).await.unwrap();
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerHandle {
    // TODO: i guess it's possible for multiple connections per peer, but within one download this shouldn't be true
    pub id: [u8; 20],

    pub remote_addr: SocketAddrV4,

    #[eq(skip)]
    ctrl: mpsc::Sender<PeerCommands>,

    #[eq(skip)]
    pub state: Arc<RwLock<PeerState>>,

    #[eq(skip)]
    pub stats_rx: watch::Receiver<PeerStatistics>,
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

        self.ctrl.send(PeerCommands::RequestDataFromPeer { req, syn }).await?;
        let data = ack.await?;
        Ok(data)
    }

    pub async fn fancy_peer(&self) -> anyhow::Result<()> {
        self.ctrl.send(PeerCommands::FancyPeer).await?;
        Ok(())
    }

    pub async fn we_have(&self, piece: u32) -> anyhow::Result<()> {
        self.ctrl.send(PeerCommands::WeHave(piece)).await?;
        Ok(())
    }

    pub async fn bit_field(&self) -> anyhow::Result<()> {
        self.ctrl.send(PeerCommands::BitField).await?;
        Ok(())
    }

    pub async fn unchoke_peer(&self) -> anyhow::Result<()> {
        self.ctrl.send(PeerCommands::UnchokePeer).await?;
        Ok(())
    }
}

pub async fn connect_peer(
    remote_addr: SocketAddrV4,
    peer_id: [u8; 20],
    torrent: Arc<Torrent>,
    storage: Arc<TorrentStorage>,
) -> anyhow::Result<PeerHandle> {
    let mut tcp = TcpStream::connect(remote_addr).await?;
    let _ = shake_hands(&mut tcp, &torrent.info_hash, &peer_id).await?;
    let (reader, writer) = tcp.into_split();
    let (commands_tx, commands_rx) = tokio::sync::mpsc::channel(1024);

    // Create watch channel for this peer's statistics
    let initial_stats = PeerStatistics {
        tcp_info: unsafe { zeroed() },
        mean_rx: 0.0,
        mean_rx_cnt: 0,
        mean_rx_last_checked: Instant::now(),
        ucb: 0.0,
        picked_count: 0,
    };
    let (stats_tx, stats_rx) = watch::channel(initial_stats);

    let state = Arc::new(RwLock::new(PeerState {
        they_have: vec![0u8; torrent.pieces.len()].into(),
        choked_us: false,
        choked_them: false,
        interested_them: false,
        interested_us: false,
        requested: BTreeMap::new(),
    }));

    let peer = PeerConnection::new(
        commands_rx,
        // torrent,
        state.clone(),
        FramedRead::new(reader, BtDecoder),
        FramedWrite::new(writer, BtEncoder),
        storage,
        stats_tx,
        // remote_addr,
    );

    let handle = PeerHandle {
        id: peer_id.clone(),
        ctrl: commands_tx,
        state,
        stats_rx,
        remote_addr,
    };
    tokio::spawn(peer_ev_loop(peer));

    Ok(handle)
}
