mod settings;
mod sharing;
mod sys_tcp;

use anyhow::{anyhow, bail};
use bendy::decoding::Object;
use futures::SinkExt;
use futures::StreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use rand::prelude::*;
use reqwest::Client;
use sha1::{Digest, Sha1};
use sharing::SharedFileHandle;
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::io::ErrorKind;
use std::mem::zeroed;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;
use std::{env, fs, mem};
use std::{fs::File, io, sync::Arc};
use tokio::io::AsyncReadExt;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time;
use tokio::time::sleep;
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_util::{
    bytes::Buf,
    codec::{Decoder, Encoder, FramedRead, FramedWrite},
};
use url::form_urlencoded;

use sharing::SharedFile;
use crate::sharing::file_loop;

trait Encode {
    fn encode(&self, buf: &mut [u8]);
}

macro_rules! u32s_to_be_bytes {
    ( $( $x:expr ),* ) => {
        [
            $(
                (($x >> 24)& 0xFF) as u8,
                (($x >> 16)& 0xFF) as u8,
                (($x >> 8) & 0xFF) as u8,
                ( $x       & 0xFF) as u8,
            )*
        ]
    };
}

pub struct KeepAlive;
impl Encode for KeepAlive {
    fn encode(&self, buf: &mut [u8]) {
        buf.copy_from_slice(&0u32.to_be_bytes());
    }
}

pub struct Choke;
impl Encode for Choke {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        length.copy_from_slice(&1u32.to_be_bytes());
        header.copy_from_slice(&[0u8]);
    }
}

pub struct Unchoke;
impl Encode for Unchoke {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        length.copy_from_slice(&1u32.to_be_bytes());
        header.copy_from_slice(&[1u8]);
    }
}

pub struct Interested;
impl Encode for Interested {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        length.copy_from_slice(&1u32.to_be_bytes());
        header.copy_from_slice(&[2u8]);
    }
}

pub struct NotInterested;
impl Encode for NotInterested {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        length.copy_from_slice(&1u32.to_be_bytes());
        header.copy_from_slice(&[3u8]);
    }
}

pub struct Have {
    pub checked: u32,
}

impl Encode for Have {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        let (header, body) = header.split_at_mut(1);
        length.copy_from_slice(&5u32.to_be_bytes());
        header.copy_from_slice(&[4u8]);
        body.copy_from_slice(&self.checked.to_be_bytes());
    }
}

pub struct BitField {
    pub has: Box<[u8]>,
}

impl Encode for BitField {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        let (header, body) = header.split_at_mut(1);
        length.copy_from_slice(&(1 + self.has.len() as u32).to_be_bytes());
        header.copy_from_slice(&[5u8]);
        body.copy_from_slice(&self.has);
    }
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy)]
pub struct Request {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

impl Encode for Request {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        let (header, body) = header.split_at_mut(1);
        length.copy_from_slice(&(1 + 12 as u32).to_be_bytes());
        header.copy_from_slice(&[6u8]);
        body.copy_from_slice(&u32s_to_be_bytes!(self.index, self.begin, self.length));
    }
}

pub struct Piece {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
    pub data: Box<[u8]>,
}

impl Encode for Piece {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        let (header, body) = header.split_at_mut(1);
        length.copy_from_slice(&((1 + 12 + self.data.len()) as u32).to_be_bytes());
        header.copy_from_slice(&[7u8]);
        body.copy_from_slice(&self.data);
    }
}

pub struct Cancel {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

impl Encode for Cancel {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        let (header, body) = header.split_at_mut(1);
        length.copy_from_slice(&(1 + 12u32).to_be_bytes());
        header.copy_from_slice(&[8u8]);
        body.copy_from_slice(&u32s_to_be_bytes!(self.index, self.begin, self.length));
    }
}

pub struct Extended<'a> {
    pub inner: BencodeItemView<'a>,
}

impl Encode for Extended<'_> {
    #[allow(unused)]
    fn encode(&self, buf: &mut [u8]) {
        todo!()
    }
}

enum BtMessage {
    KeepAlive(KeepAlive),
    Choke(Choke),
    Unchoke(Unchoke),
    Interested(Interested),
    NotInterested(NotInterested),
    Have(Have),
    BitField(BitField),
    Request(Request),
    Piece(Piece),
    Cancel(Cancel),
    Unknown(u8, #[allow(unused)] Box<[u8]>),
}

struct BtEncoder;

impl Encoder<BtMessage> for BtEncoder {
    type Error = io::Error;

    fn encode(&mut self, item: BtMessage, dst: &mut tokio_util::bytes::BytesMut) -> Result<(), Self::Error> {
        let buf: &mut [u8] = &mut *dst;
        match item {
            BtMessage::KeepAlive(keep_alive) => keep_alive.encode(buf),
            BtMessage::Choke(choke) => choke.encode(buf),
            BtMessage::Unchoke(unchoke) => unchoke.encode(buf),
            BtMessage::Interested(interested) => interested.encode(buf),
            BtMessage::NotInterested(not_interested) => not_interested.encode(buf),
            BtMessage::Have(have) => have.encode(buf),
            BtMessage::BitField(bit_field) => bit_field.encode(buf),
            BtMessage::Request(request) => request.encode(buf),
            BtMessage::Piece(piece) => piece.encode(buf),
            BtMessage::Cancel(cancel) => cancel.encode(buf),
            BtMessage::Unknown(..) => panic!(),
        }

        Ok(())
    }
}

struct BtDecoder;

impl Decoder for BtDecoder {
    type Item = BtMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut tokio_util::bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_le_bytes(length_bytes) as usize;

        if 4 + length < src.len() {
            return Ok(None);
        }

        if length == 0 {
            src.advance(4);
            let keep_alive = BtMessage::KeepAlive(KeepAlive);
            return Ok(Some(keep_alive));
        }
        src.advance(4 + length);

        let msg_type = src[4..5][0];
        let buf = &src[5..5 + length];

        let msg = match msg_type {
            0 => BtMessage::Choke(Choke),
            1 => BtMessage::Unchoke(Unchoke),
            2 => BtMessage::Interested(Interested),
            3 => BtMessage::NotInterested(NotInterested),
            4 => BtMessage::Have(Have {
                checked: u32::from_be_bytes(buf[0..4].try_into().unwrap()),
            }),
            5 => BtMessage::BitField(BitField { has: Box::from(buf) }),
            6 => {
                let index = u32::from_be_bytes(buf[0..4].try_into().unwrap());
                let begin = u32::from_be_bytes(buf[4..8].try_into().unwrap());
                let length = u32::from_be_bytes(buf[8..12].try_into().unwrap());
                BtMessage::Request(Request { index, begin, length })
            }
            7 => {
                let index = u32::from_be_bytes(buf[0..4].try_into().unwrap());
                let begin = u32::from_be_bytes(buf[4..8].try_into().unwrap());
                let length = u32::from_be_bytes(buf[8..12].try_into().unwrap());
                let data = Box::from(&buf[12..]);
                BtMessage::Piece(Piece {
                    index,
                    begin,
                    length,
                    data,
                })
            }
            8 => {
                let index = u32::from_be_bytes(buf[0..4].try_into().unwrap());
                let begin = u32::from_be_bytes(buf[4..8].try_into().unwrap());
                let length = u32::from_be_bytes(buf[8..12].try_into().unwrap());
                BtMessage::Cancel(Cancel { index, begin, length })
            }
            t => BtMessage::Unknown(t, Box::from(buf)),
        };

        Ok(Some(msg))
    }
}

struct Handshake {
    extensions: [u8; 8],
    info_hash: InfoHash,
    peer_id: [u8; 20],
}

pub const HANDSHAKE: &'static [u8] = b"\x13BitTorrent protocol";

async fn shake_hands(peer: &mut TcpStream, info_hash: &InfoHash, peer_id: &[u8; 20]) -> io::Result<Handshake> {
    let extensions = [0u8; 8];

    let mut buf = vec![];
    buf.extend_from_slice(HANDSHAKE);
    buf.extend_from_slice(&extensions);
    buf.extend_from_slice(info_hash.as_bytes());
    buf.extend_from_slice(peer_id);

    peer.write_all(&*buf).await?;

    let read_buf = [0u8; 68];
    peer.read_exact(&mut buf).await?;

    // header, info_hash, and peer_id must match
    if read_buf[..20] != *HANDSHAKE || read_buf[28..48] != info_hash.0 || read_buf[48..68] != *peer_id {
        peer.shutdown().await?;
        return Err(io::Error::new(ErrorKind::Other, "handshake info didn't match"));
    }

    Ok(Handshake {
        extensions: read_buf[20..28].try_into().unwrap(),
        info_hash: InfoHash(read_buf[28..48].try_into().unwrap()),
        peer_id: read_buf[48..68].try_into().unwrap(),
    })
}

enum PeerCommands {
    UpdateStat(oneshot::Sender<PeerStatistics>),
    UnchokePeer,
    ChokePeer,
    RequestPieceFromPeer(u32),
}

async fn peer_loop(mut peer: PeerContext) {
    while let Some(command) = peer.commands.recv().await {
        match command {
            PeerCommands::UpdateStat(ack) => {
                peer.update_stat();
                let _ = ack.send(peer.stat);
            }
            PeerCommands::UnchokePeer => peer.unchoke_peer().await.unwrap(),
            PeerCommands::ChokePeer => peer.choke_peer().await.unwrap(),
            PeerCommands::RequestPieceFromPeer(piece) => peer.request_piece_from_peer(piece).await.unwrap(),
        }
    }
}

pub struct PeerContext {
    commands: Receiver<PeerCommands>,

    torrent: Arc<Torrent>,

    state: Arc<RwLock<PeerState>>,

    reader: FramedRead<OwnedReadHalf, BtDecoder>,
    writer: FramedWrite<OwnedWriteHalf, BtEncoder>,
    sharing: SharedFileHandle,

    requested: BTreeMap<Request, Instant>,

    /// a place to hold blocks before they form a complete piece and ready to be written to disk
    block_buf: BTreeMap<usize, (usize, Vec<u8>)>, // piece num -> (written, buffer)

    stat: PeerStatistics,

    /// if the connection is closed, then the peer should be disposed off when able
    connection_closed: bool,
}

#[derive(Clone, Debug)]
struct PeerState {
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
struct PeerStatistics {
    pub tcp_info: sys_tcp::tcp_info,
    pub mean_rx: f64,
    pub mean_rx_cnt: usize,
    pub mean_rx_last_checked: Instant,

    /// upper confidence bound
    pub ucb: f64,
    pub picked_count: usize,
}

impl PeerContext {
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
    }

    pub fn flush_blocks(&mut self) -> anyhow::Result<()> {
        self.block_buf.retain(|piece, (written, buf)| {
            let expected_size = self.torrent.nth_piece_size(*piece as u64).unwrap();
            if expected_size == *written {
                let data = mem::take(buf);
                self.sharing.write_piece(*piece as u32, data.into_boxed_slice());
            }

            return expected_size != *written;
        });

        Ok(())
    }

    /// Process one message from the queue of messages received from this peer
    pub async fn handle_one(&mut self) {
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

                    // TODO: revisit design
                    self.flush_blocks().unwrap();
                }
                BtMessage::Cancel(_cancel) => return,
                BtMessage::Unknown(msg_type, _) => {
                    tracing::warn!("Unsupported message type {msg_type}");
                }
            }
        }
    }
}

struct Torrent {
    announce: String,

    /// number of bytes for each piece, barring the last one
    piece_size: u32,

    /// each pieces' hash
    pieces: Vec<[u8; 20]>,

    /// size of the file and the path
    files: Vec<(u32, PathBuf)>,

    /// size of all the files combined in bytes
    total_size: u64,

    last_piece_size: u32,

    info_hash: InfoHash,
}

impl Torrent {
    pub fn valid_piece(&self, piece: u32, data: &[u8]) -> bool {
        let expected_hash = self.pieces[piece as usize];
        let got = Sha1::digest(data);
        &*got == &expected_hash
    }

    /// Returns the size of the ith piece in bytes
    pub fn nth_piece_size<T: Into<u64>>(&self, i: T) -> Option<usize> {
        let i = i.into();
        if i >= self.pieces.len().try_into().unwrap() {
            return None;
        }

        let last_piece_index = (self.pieces.len() - 1).try_into().unwrap();
        if i == last_piece_index {
            Some(self.last_piece_size.try_into().unwrap())
        } else {
            Some(self.piece_size as usize)
        }
    }
}

fn extract_torrent(metadata_file: &[u8]) -> anyhow::Result<Torrent> {
    let hash = info_hash(metadata_file);

    let (_, mut torrent) = juicy_bencode::parse_bencode_dict(metadata_file).map_err(|_| {
        // the error type has a reference on the input, we don't want that
        anyhow!("not a valid dict")
    })?;

    let BencodeItemView::Dictionary(mut info) = torrent.remove(b"info".as_slice()).unwrap() else {
        bail!("info needs to be a dict");
    };

    let BencodeItemView::ByteString(announce) = torrent.remove(b"announce".as_slice()).unwrap() else {
        bail!("announce needs to be a string");
    };

    let BencodeItemView::ByteString(name) = info.remove(b"name".as_slice()).unwrap() else {
        panic!();
    };
    let mut root = PathBuf::new();
    // TODO: make this configurable
    root.push("./");
    root.push(str::from_utf8(name).unwrap());

    let BencodeItemView::Integer(piece_len) = info.remove(b"piece length".as_slice()).unwrap() else {
        panic!();
    };
    let piece_len: u32 = piece_len.try_into()?;

    let BencodeItemView::ByteString(pieces) = info.remove(b"pieces".as_slice()).unwrap() else {
        panic!();
    };

    let mut files = vec![];

    // TODO: we expect each `Torrent` object's `files` to contain file paths that already exists,
    // need to make sure they do
    if let Some(BencodeItemView::Integer(length)) = info.remove(b"length".as_slice()) {
        files.push((length as u32, root));
    } else if let Some(BencodeItemView::List(file_lists)) = info.remove(b"files".as_slice()) {
        while let Some(BencodeItemView::Dictionary(entries)) = file_lists.iter().next() {
            let BencodeItemView::Integer(length) = entries.get(b"length".as_slice()).unwrap() else {
                panic!();
            };
            let BencodeItemView::List(paths) = entries.get(b"path".as_slice()).unwrap() else {
                panic!();
            };
            let mut f = root.clone();
            while let Some(BencodeItemView::ByteString(p)) = paths.iter().next() {
                f.push(str::from_utf8(p).unwrap());
            }

            files.push((*length as u32, f));
        }
    }

    let pieces = pieces.chunks(20).map(|e| e.try_into().unwrap()).collect();
    let total_size = files.iter().map(|(len, _f)| *len as u64).sum();
    let piece_len: u64 = piece_len.try_into().unwrap();
    let last_piece_len = total_size % piece_len;
    Ok(Torrent {
        announce: String::from_utf8(Vec::from(announce)).unwrap(),
        piece_size: piece_len as u32,
        pieces,
        total_size,
        files,
        last_piece_size: last_piece_len.try_into().unwrap(),
        info_hash: hash,
    })
}

struct Identity {
    peer_id: [u8; 20],
    serving: SocketAddrV4,
}

struct PeerHandle {
    ctrl: Sender<PeerCommands>,
    state: Arc<RwLock<PeerState>>,
}

impl PeerHandle {
    pub async fn request_piece_from_peer(&self, piece: u32) -> anyhow::Result<()> {
        self.ctrl.send(PeerCommands::RequestPieceFromPeer(piece)).await?;
        Ok(())
    }

    pub async fn update_stat(&self) -> anyhow::Result<PeerStatistics> {
        let (syn, ack) = oneshot::channel();
        self.ctrl.send(PeerCommands::UpdateStat(syn)).await?;
        let stat = ack.await?;
        Ok(stat)
    }
}

struct PeerShare {
    peer_handles: Vec<PeerHandle>,
    torrent: Arc<Torrent>,
    sharing: SharedFileHandle,
    announce_interval: Cell<Duration>,
    announce: String,

    id: Arc<Identity>,
}

impl PeerShare {
    pub fn new(torrent: Arc<Torrent>, sharing: SharedFileHandle, id: Arc<Identity>) -> PeerShare {
        let announce = torrent.announce.clone();
        let announce_interval = Cell::new(Duration::from_secs(0));

        PeerShare {
            peer_handles: vec![],
            torrent,
            sharing,
            announce_interval,
            announce,
            id,
        }
    }

    async fn try_add_peer(&self, peer: SocketAddrV4) -> anyhow::Result<()> {
        let mut tcp = TcpStream::connect(peer).await?;
        let _ = shake_hands(&mut tcp, &self.torrent.info_hash, &self.id.peer_id).await?;
        let (reader, writer) = tcp.into_split();
        let (commands_tx, commands_rx) = channel(1024);
        let peer = PeerContext {
            commands: commands_rx,
            torrent: Arc::clone(&self.torrent),

            state: Arc::new(RwLock::new(PeerState {
                they_have: vec![0u8; self.torrent.pieces.len()].into(),

                choked_us: false,
                choked_them: false,

                interested_them: false,
                interested_us: false,

                requested: BTreeMap::new(),
            })),
            reader: FramedRead::new(reader, BtDecoder),
            writer: FramedWrite::new(writer, BtEncoder),
            sharing: self.sharing.clone(),

            requested: Default::default(),
            block_buf: BTreeMap::new(),
            stat: PeerStatistics {
                tcp_info: unsafe { zeroed() },

                mean_rx: 0.0,
                mean_rx_cnt: 0,
                mean_rx_last_checked: Instant::now(),
                ucb: 0.0,
                picked_count: 0,
            },

            connection_closed: false,
        };

        tokio::spawn(peer_loop(peer));
        // TODO: instead of storing the peers, store some proxy that can talk to the peers
        //
        // self.peers.lock().unwrap().push(peer);
        // self.peers.borrow_mut().push(peer);
        Ok(())
    }

    async fn work_loop(&mut self) {
        tokio::select! {
            _ = self.download_loop() => {},
            _ = self.announce_loop() => {},
        }
    }

    async fn download_loop(&self) {
        let mut want: Vec<u32> = (0..self.torrent.pieces.len()).map(|p| p.try_into().unwrap()).collect();
        want.shuffle(&mut rand::rng());
        let mut sleep = false;

        loop {
            if sleep {
                time::sleep(Duration::from_millis(100)).await;
            }

            if want.is_empty() {
                break;
            }

            if self.sharing.all_verified().await {
                break;
            }

            let piece = want.last().unwrap();
            let best = choose(&self.peer_handles, *piece).await;
            if best.is_none() {
                sleep = true;
                continue;
            } else {
                let best = best.unwrap();
                best.request_piece_from_peer(*piece).await.unwrap();
                sleep = false;
            }
        }
    }

    async fn announce(&self) -> anyhow::Result<(Duration, Vec<SocketAddrV4>)> {
        // URL-encode info_hash and peer_id
        let info_hash_encoded: String = form_urlencoded::byte_serialize(&self.torrent.info_hash.0).collect();
        let peer_id_encoded: String = form_urlencoded::byte_serialize(&self.id.peer_id).collect();

        let stats = self.peer_handles.iter().map(|p| p.update_stat());
        let stats = join_all(stats).await;

        let stats: Vec<_> = stats.into_iter().filter_map(Result::ok).collect();

        let (total_tx, total_rx) = stats.into_iter().fold((0, 0), |(tx, rx), s| {
            (tx + s.tcp_info.tcpi_bytes_acked, rx + s.tcp_info.tcpi_bytes_received)
        });

        // Build the announce URL
        let url = format!(
            "{tracker_url}?info_hash={info_hash}&peer_id={peer_id}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&compact=1",
            tracker_url = self.torrent.announce,
            info_hash = info_hash_encoded,
            peer_id = peer_id_encoded,
            port = self.id.serving.port(),
            uploaded = total_tx,
            downloaded = total_rx,
            left = self.sharing.remaining_bytes().await,
        );

        // Send the GET request
        let client = Client::new();
        let response = client.get(&url).send().await.unwrap();
        let bytes = response.bytes().await.unwrap();

        // Parse the bencoded response
        let (_, dict) = juicy_bencode::parse_bencode_dict(&mut bytes.as_ref()).unwrap();

        let mut ppp = vec![];
        let Some(BencodeItemView::Integer(interval)) = dict.get(b"interval".as_slice()) else {
            bail!("response interval must be a number");
        };
        if let Some(BencodeItemView::ByteString(peers)) = dict.get(b"peers".as_slice()) {
            // Each peer is 6 bytes: 4 bytes IP, 2 bytes port
            for chunk in peers.chunks(6) {
                if chunk.len() != 6 {
                    break;
                }

                let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
                let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                ppp.push(SocketAddrV4::new(ip, port));
            }
        }

        Ok((Duration::from_secs(*interval as u64), ppp))
    }

    async fn announce_loop(&self) -> anyhow::Result<()> {
        sleep(self.announce_interval.get()).await;
        let (new_interval, peers) = self.announce().await?;

        self.announce_interval.set(new_interval);

        let mut add_peers: FuturesUnordered<_> = peers.into_iter().map(|p| self.try_add_peer(p)).collect();
        while let Some(_) = add_peers.next().await {}

        Ok(())
    }
}

async fn choose(peers: &Vec<PeerHandle>, piece: u32) -> Option<&PeerHandle> {
    // TODO: whatever is the following nonsense
    let mut v = vec![];
    for p in peers.iter() {
        let (syn, ack) = oneshot::channel();
        p.ctrl.send(PeerCommands::UpdateStat(syn)).await;
        v.push((ack.await.unwrap(), p));
    }

    // pick the best one using u.c.b.
    v.into_iter()
        .filter(|(_, h)| h.state.read().unwrap().ready() && h.state.read().unwrap().they_have(piece))
        .into_iter()
        .max_by(|(lhs, _), (rhs, _)| lhs.ucb.total_cmp(&rhs.ucb))
        .map(|(_, h)| h)
}

// TODO: use make each type juicy_bencode also contain a reference to the underlying characters
fn info_hash(input: &[u8]) -> InfoHash {
    let mut decoder = bendy::decoding::Decoder::new(input);
    let Some(Object::Dict(mut dict)) = decoder.next_object().unwrap() else {
        panic!()
    };

    while let Some((key, val)) = dict.next_pair().unwrap() {
        if key == b"info" {
            let buf = match val {
                Object::List(list_decoder) => list_decoder.into_raw().unwrap(),
                Object::Dict(dict_decoder) => dict_decoder.into_raw().unwrap(),
                Object::Integer(i) => i.as_bytes(),
                Object::Bytes(items) => items,
            };
            return InfoHash::from_bytes(Sha1::digest(buf).as_slice());
        }
    }

    panic!()
}

struct BtClient {
    id: Arc<Identity>,
    tasks: HashMap<InfoHash, PeerShare>,
}

impl BtClient {
    fn new(id: Identity) -> Self {
        BtClient {
            id: Arc::new(id),
            tasks: HashMap::new(),
        }
    }

    fn add_task(&mut self, mut torrent: Torrent) -> anyhow::Result<()> {
        if self.tasks.contains_key(&torrent.info_hash) {
            bail!("task with this info hash already exists");
        }

        let mut files = vec![];
        for (_size, file) in torrent.files.iter_mut() {
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            files.push(File::create(&file)?);
        }


        let torrent = Arc::new(torrent);
        let (file, handle) = SharedFile::file_and_handle(torrent.clone(), files);
        tokio::spawn(file_loop(file));

        let task = PeerShare::new(torrent.clone(), handle, self.id.clone());

        self.tasks.insert(torrent.info_hash, task);
        Ok(())
    }

    async fn work(&mut self) -> anyhow::Result<()> {
        let mut vec = vec![];
        for (info_hash, share) in self.tasks.iter_mut() {
            vec.push(share.work_loop());
        }

        // for handle in vec {
        //     handle.await;
        // }

        join_all(vec).await;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<_>>();

    let meta_file = PathBuf::from(&args[1]);
    let meta_bytes = std::fs::read(&meta_file).unwrap();
    let torrent = extract_torrent(&meta_bytes).unwrap();

    let mut client = BtClient::new(Identity {
        peer_id: [0u8; 20],
        serving: SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 6881),
    });

    client.add_task(torrent).unwrap();
    client.work().await?;

    Ok(())
}
