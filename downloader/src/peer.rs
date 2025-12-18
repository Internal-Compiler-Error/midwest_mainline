mod wire;

use futures::SinkExt;
use futures::StreamExt;
use midwest_mainline::types::InfoHash;
use std::collections::BTreeMap;
use std::mem::zeroed;
use std::os::fd::AsRawFd;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;
use std::{io, mem, sync::Arc};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::storage::TorrentStorageHandle;
use crate::sys_tcp;
use crate::torrent::Torrent;

use wire::{
    BtDecoder, BtEncoder, BtMessage, BitField, Choke, Have, Interested, NotInterested, Piece,
    Request, Unchoke, shake_hands,
};

pub enum PeerCommands {
    UnchokePeer,
    ChokePeer,
    RequestPieceFromPeer(u32),
}

pub async fn peer_loop(peer: PeerConnection) {
    let PeerConnection {
        mut commands,
        torrent,
        state,
        reader,
        writer,
        sharing,
        requested,
        block_buf,
        stat,
        stats_tx,
        connection_closed,
    } = peer;

    // Reconstruct peer without commands channel to avoid borrow conflicts
    let mut peer = PeerConnection {
        commands: tokio::sync::mpsc::channel(1).1, // Dummy receiver, not used
        torrent,
        state,
        reader,
        writer,
        sharing,
        requested,
        block_buf,
        stat,
        stats_tx,
        connection_closed,
    };

    let mut stats_interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            // Handle periodic stats updates
            _ = stats_interval.tick() => {
                peer.update_stat().await;
            }

            // Handle commands from the peer handle
            Some(command) = commands.recv() => {
                match command {
                    PeerCommands::UnchokePeer => peer.unchoke_peer().await.unwrap(),
                    PeerCommands::ChokePeer => peer.choke_peer().await.unwrap(),
                    PeerCommands::RequestPieceFromPeer(piece) => peer.request_piece_from_peer(piece).await.unwrap(),
                }
            }

            // Process incoming BitTorrent protocol messages
            _ = peer.process_message() => {
                // Continue processing messages
            }

            // Exit loop if all channels are closed
            else => break,
        }
    }
}

/// Represents an active connection to a peer in the BitTorrent network
pub struct PeerConnection {
    commands: Receiver<PeerCommands>,

    torrent: Arc<Torrent>,

    state: Arc<RwLock<PeerState>>,

    reader: FramedRead<OwnedReadHalf, BtDecoder>,
    writer: FramedWrite<OwnedWriteHalf, BtEncoder>,
    sharing: TorrentStorageHandle,

    requested: BTreeMap<Request, Instant>,

    /// a place to hold blocks before they form a complete piece and ready to be written to disk
    block_buf: BTreeMap<usize, (usize, Vec<u8>)>, // piece num -> (written, buffer)

    stat: PeerStatistics,

    /// Watch channel to publish statistics updates
    stats_tx: watch::Sender<PeerStatistics>,

    /// if the connection is closed, then the peer should be disposed off when able
    connection_closed: bool,
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
    pub fn new(
        commands: Receiver<PeerCommands>,
        torrent: Arc<Torrent>,
        state: Arc<RwLock<PeerState>>,
        reader: FramedRead<OwnedReadHalf, BtDecoder>,
        writer: FramedWrite<OwnedWriteHalf, BtEncoder>,
        sharing: TorrentStorageHandle,
        stats_tx: watch::Sender<PeerStatistics>,
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
            torrent,
            state,
            reader,
            writer,
            sharing,
            requested: Default::default(),
            block_buf: BTreeMap::new(),
            stat: initial_stats,
            stats_tx,
            connection_closed: false,
        }
    }

    pub fn share_states(&self) -> Arc<RwLock<PeerState>> {
        Arc::clone(&self.state)
    }

    pub fn prune_expired_reqs(&mut self) {
        let now = Instant::now();
        self.requested
            .retain(|_k, v| now.duration_since(*v) <= Duration::from_secs(120));
    }

    pub async fn request_piece_from_peer(&mut self, piece: u32) -> io::Result<()> {
        let req = Request {
            index: piece,
            begin: 0,
            length: self.torrent.nth_piece_size(piece).unwrap() as u32,
        };

        self.prune_expired_reqs();
        self.requested.insert(req, Instant::now());
        self.writer.feed(BtMessage::Request(req)).await?;

        Ok(())
    }

    pub async fn unchoke_peer(&mut self) -> io::Result<()> {
        let unchoke = Unchoke;
        self.writer.feed(BtMessage::Unchoke(unchoke)).await?;
        self.state.write().unwrap().choked_them = false;

        Ok(())
    }

    pub async fn choke_peer(&mut self) -> io::Result<()> {
        let choke = Choke;
        self.writer.feed(BtMessage::Choke(choke)).await?;
        self.state.write().unwrap().choked_them = true;

        Ok(())
    }

    pub async fn fancy_peer(&mut self) -> io::Result<()> {
        let interested = Interested;
        self.writer.feed(BtMessage::Interested(interested)).await?;
        self.state.write().unwrap().interested_them = true;

        Ok(())
    }

    pub async fn unfancy_peer(&mut self) -> io::Result<()> {
        let not_interested = NotInterested;
        self.writer.feed(BtMessage::NotInterested(not_interested)).await?;
        self.state.write().unwrap().interested_them = false;

        Ok(())
    }

    pub async fn send_bitfield(&mut self) -> io::Result<()> {
        let bit_field = BitField {
            has: self.sharing.verified().await.as_raw_slice().into(),
        };
        self.writer.feed(BtMessage::BitField(bit_field)).await?;

        Ok(())
    }

    pub async fn we_have(&mut self, index: u32) -> io::Result<()> {
        let have = Have { checked: index };
        self.writer.feed(BtMessage::Have(have)).await?;

        Ok(())
    }

    pub async fn flush_outbound(&mut self) -> io::Result<()> {
        self.writer.flush().await
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
        let t = self.sharing.verified_cnt().await as f64;
        let n_t = self.stat.picked_count as f64;
        self.stat.ucb = self.stat.mean_rx + c * (t.ln() / n_t).sqrt();

        // Publish updated statistics via watch channel
        let _ = self.stats_tx.send(self.stat);
    }

    pub async fn flush_blocks(&mut self) -> anyhow::Result<()> {
        // Collect complete pieces to write
        let mut to_write = vec![];
        self.block_buf.retain(|piece, (written, buf)| {
            let expected_size = self.torrent.nth_piece_size(*piece as u64).unwrap();
            if expected_size == *written {
                let data = mem::take(buf);
                to_write.push((*piece as u32, data.into_boxed_slice()));
                return false; // Remove from block_buf
            }
            return true; // Keep in block_buf
        });

        // Write all complete pieces
        for (piece, data) in to_write {
            let _ = self.sharing.write_piece(piece, data).await;
        }

        Ok(())
    }

    /// Reads and processes one incoming BitTorrent protocol message from this peer
    pub async fn process_message(&mut self) {
        if let Some(msg) = self.reader.next().await {
            match msg.unwrap() {
                BtMessage::KeepAlive(_) => return,
                BtMessage::Choke(_) => self.state.write().unwrap().choked_us = true,
                BtMessage::Unchoke(_) => self.state.write().unwrap().choked_us = false,
                BtMessage::Interested(_) => self.state.write().unwrap().interested_us = true,
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
                    let data = self.sharing.read_piece(request.index).await;
                    if data.is_err() {
                        return;
                    }

                    let data = data.unwrap();
                    let (head, tail) = data.split_at(request.begin as usize);
                    let tail: Box<[u8]> = tail.into();

                    let resp = BtMessage::Piece(Piece {
                        index: request.index,
                        begin: request.begin,
                        length: (data.len() - request.begin as usize) as u32,
                        data: tail,
                    });
                    self.writer.feed(resp).await.unwrap();
                }
                BtMessage::Piece(piece) => {
                    if !self.requested.contains_key(&Request {
                        index: piece.index,
                        begin: piece.begin,
                        length: piece.length,
                    }) {
                        // be wary of strangers sending data you didn't ask for, ignore them
                        return;
                    } else {
                        self.requested.remove(&Request {
                            index: piece.index,
                            begin: piece.begin,
                            length: piece.length,
                        });
                    }

                    let (written, buf) = self.block_buf.entry(piece.index as usize).or_insert_with(|| {
                        (
                            0,
                            vec![
                                0u8;
                                self.torrent
                                    .nth_piece_size(piece.index)
                                    .expect("people won't send us pieces with invalid index")
                            ],
                        )
                    });

                    let length: usize = piece.length.try_into().unwrap();
                    let begin: usize = piece.begin.try_into().unwrap();

                    *written += length;
                    buf[begin..begin + length].copy_from_slice(&piece.data);

                    self.flush_blocks().await.unwrap();
                }
                BtMessage::Cancel(_cancel) => return,
                BtMessage::Unknown(msg_type, _) => {
                    tracing::warn!("Unsupported message type {msg_type}");
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct PeerHandle {
    pub ctrl: Sender<PeerCommands>,
    pub state: Arc<RwLock<PeerState>>,
    pub stats_rx: watch::Receiver<PeerStatistics>,
}

impl PeerHandle {
    pub fn new(
        ctrl: Sender<PeerCommands>,
        state: Arc<RwLock<PeerState>>,
        stats_rx: watch::Receiver<PeerStatistics>,
    ) -> Self {
        PeerHandle {
            ctrl,
            state,
            stats_rx,
        }
    }

    pub async fn request_piece_from_peer(&self, piece: u32) -> anyhow::Result<()> {
        self.ctrl.send(PeerCommands::RequestPieceFromPeer(piece)).await?;
        Ok(())
    }
}

pub async fn create_peer_connection(
    peer: std::net::SocketAddrV4,
    info_hash: &InfoHash,
    peer_id: &[u8; 20],
    torrent: Arc<Torrent>,
    storage: TorrentStorageHandle,
) -> anyhow::Result<(PeerConnection, PeerHandle)> {
    let mut tcp = TcpStream::connect(peer).await?;
    let _ = shake_hands(&mut tcp, info_hash, peer_id).await?;
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
        torrent,
        state.clone(),
        FramedRead::new(reader, BtDecoder),
        FramedWrite::new(writer, BtEncoder),
        storage,
        stats_tx,
    );

    let handle = PeerHandle::new(commands_tx, state, stats_rx);

    Ok((peer, handle))
}
