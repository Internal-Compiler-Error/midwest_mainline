mod sys_tcp;

use anyhow::bail;
use bendy::decoding::Object;
use futures::SinkExt;
use futures::StreamExt;
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use rand::prelude::*;
use reqwest::Client;
use sha1::{Digest, Sha1};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::mem;
use std::mem::zeroed;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::{fs::File, io, os::unix::fs::FileExt, sync::Arc};
use tokio::io::AsyncReadExt;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::time;
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_util::{
    bytes::Buf,
    codec::{Decoder, Encoder, FramedRead, FramedWrite},
};
use tracing::warn;
use url::form_urlencoded;

use bitvec::prelude::*;

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
        length.copy_from_slice(&(1 + 12 as u32).to_be_bytes());
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

struct SharedThing<'t> {
    pub torrent: &'t Torrent,
    pub files: Vec<SharedFile<'t>>,
    offsets: Vec<usize>,
    verified: BitBox,
}

impl SharedThing<'_> {
    pub fn all_verified(&self) -> bool {
        self.verified.iter().all(|f| *f)
    }

    fn files_responsible(&mut self, piece: u32) -> std::ops::Range<usize> {
        // TODO: last piece has a different size
        assert!(piece as usize != self.torrent.pieces.len() - 1);

        let piece_start = (piece * self.torrent.piece_len) as usize;
        let piece_end = piece_start + self.torrent.piece_len as usize;

        let first = self
            .offsets
            .partition_point(|&off| off <= piece_start)
            .saturating_sub(1);
        let last = self.offsets.partition_point(|&off| off < piece_end);

        first..last
    }

    pub fn write_piece(&mut self, piece: u32, buf: Box<[u8]>) {}

    pub fn check(&mut self, piece: u32) {
        let piece_start = (piece * self.torrent.piece_len) as usize;
        let piece_end = piece_start + self.torrent.piece_len as usize;

        let overlapping = self.files_responsible(piece);

        let mut buf = vec![0u8; self.torrent.piece_len as usize];
        let mut wrote = 0;

        for idx in overlapping {
            let rec_abs_start = self.offsets[idx];
            let rec_abs_end = if idx + 1 < self.offsets.len() {
                self.offsets[idx + 1]
            } else {
                self.torrent.len
            };

            // Intersection with the sought-for interval
            let overlap_start = piece_start.max(rec_abs_start);
            let overlap_end = piece_end.min(rec_abs_end);

            if overlap_start < overlap_end {
                let local_off = (overlap_start - rec_abs_start) as usize;
                let local_len = (overlap_end - overlap_start) as usize;

                let dst_slice = &mut buf[wrote..wrote + local_len];
                self.files[idx].file.read_exact_at(dst_slice, local_off as u64).unwrap();

                wrote += local_len
            }
        }

        let expected_hash = self.torrent.pieces[piece as usize];
        let got = Sha1::digest(&buf);

        if &*got == &expected_hash {
            self.verified.set(piece as usize, true);
        } else {
            warn!("hash failed");
        }
    }
}

struct SharedFile<'t> {
    /// Total number of bytes in the file
    pub len: usize,

    pub torrent: &'t Torrent,
    pub progress: Progress,
    pub file: File,
}

struct Progress {
    written: Box<[AtomicU8]>,
    verified: Box<[AtomicU8]>,
    pub remaining: AtomicUsize,
    pub completed: AtomicUsize,
}

impl SharedFile<'_> {
    pub fn write(&self, offset: u64, buf: Box<[u8]>) {
        self.file.write_all_at(&buf, offset).unwrap();
    }

    fn mask_and_offset(index: u32) -> (u8, usize) {
        let byte_index = index / 8;
        let mask = 0x01u8 << byte_index;
        let offset: usize = (index % 8) as usize;

        (mask, offset)
    }

    pub fn is_done(&self, index: u32) -> bool {
        let (mask, offset) = Self::mask_and_offset(index);

        let done = &self.progress.verified[offset];
        loop {
            let snapshot = done.load(Ordering::Acquire);
            let res = snapshot & mask;
            match done.compare_exchange(snapshot, snapshot, Ordering::Acquire, Ordering::Relaxed) {
                Ok(_) => return res != 0,
                Err(_) => continue, // Value changed, retry
            }
        }
    }

    fn done(&self, index: u32) {
        let (mask, offset) = Self::mask_and_offset(index);
        let progress = &self.progress;

        progress.remaining.fetch_sub(1, Ordering::SeqCst);
        progress.completed.fetch_add(1, Ordering::SeqCst);
    }

    fn all_done(&self) -> bool {
        self.progress.remaining.load(Ordering::Acquire) == 0
    }

    fn completed_bytes(&self) -> usize {
        let last_piece_size = self.len / self.torrent.piece_len as usize;

        if !self.is_done(self.pieces.len() as u32) {
            let completed = self.progress.completed.load(Ordering::SeqCst);
            completed * self.torrent.piece_len as usize
        } else {
            let completed = self.progress.completed.load(Ordering::SeqCst) - 1;
            completed * self.torrent.piece_len as usize + last_piece_size
        }
    }

    fn remaining_bytes(&self) -> usize {
        self.len - self.completed_bytes()
    }
}

pub struct PeerContext<'t> {
    choked: bool,
    interested: bool,
    reader: FramedRead<OwnedReadHalf, BtDecoder>,
    writer: FramedWrite<OwnedWriteHalf, BtEncoder>,
    sharing: Arc<SharedThing<'t>>,
    they_have: Box<[u8]>,

    requestd: BTreeMap<Request, Instant>,

    tcp_info: sys_tcp::tcp_info,
    mean_rx: f64,
    mean_rx_cnt: usize,
    mean_rx_last_checked: Instant,

    /// upper confidence bound
    ucb: f64,
    picked_count: usize,
}

impl PeerContext<'_> {
    /// Is the peer ready for more requests?
    pub fn ready(&self) -> bool {
        self.requestd.len() <= 500 && !self.choked
    }

    pub fn prune_old_reqs(&mut self) {
        let now = Instant::now();
        self.requestd
            .retain(|_k, v| now.duration_since(*v) <= Duration::from_secs(120));
    }

    pub async fn request_piece(&mut self, piece: u32) -> io::Result<()> {
        // TODO: the last piece has a different length than all others
        let req = Request {
            index: piece,
            begin: 0,
            length: self.sharing.torrent.piece_len,
        };

        self.prune_old_reqs();
        self.requestd.insert(req, Instant::now());
        self.writer.feed(BtMessage::Request(req)).await?;

        Ok(())
    }

    pub async fn unchoke(&mut self) -> io::Result<()> {
        let unchoke = Unchoke;
        self.writer.feed(BtMessage::Unchoke(unchoke)).await?;
        self.choked = false;

        Ok(())
    }

    pub async fn choke(&mut self) -> io::Result<()> {
        let choke = Choke;
        self.writer.feed(BtMessage::Choke(choke)).await?;
        self.choked = true;

        Ok(())
    }

    pub async fn interested(&mut self) -> io::Result<()> {
        let interested = Interested;
        self.writer.feed(BtMessage::Interested(interested)).await?;
        self.interested = true;

        Ok(())
    }

    pub async fn not_interested(&mut self) -> io::Result<()> {
        let not_interested = NotInterested;
        self.writer.feed(BtMessage::NotInterested(not_interested)).await?;
        self.interested = false;

        Ok(())
    }

    pub async fn bitfield(&mut self) -> io::Result<()> {
        let bit_field = BitField {
            has: self
                .sharing
                .progress
                .verified
                .iter()
                .map(|atomic| atomic.load(Ordering::Relaxed))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        };
        self.writer.feed(BtMessage::BitField(bit_field)).await?;

        Ok(())
    }

    pub async fn we_have(&mut self, index: u32) -> io::Result<()> {
        let have = Have { checked: index };
        self.writer.feed(BtMessage::Have(have)).await?;

        Ok(())
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
    }

    pub fn they_have(&self, piece: u32) -> bool {
        let index = piece / 8;
        let offset = piece % 8;

        let flag = 1u8 << offset;
        (self.they_have[index as usize] & flag) != 0
    }

    pub fn update_stat(&mut self) {
        if Instant::now().duration_since(self.mean_rx_last_checked) < Duration::from_secs(1) {
            return;
        }

        let fd = self.writer.get_ref().as_ref().as_raw_fd();
        unsafe {
            let mut len = mem::size_of::<sys_tcp::tcp_info>() as libc::socklen_t;
            libc::getsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_INFO,
                &mut self.tcp_info as *mut _ as *mut libc::c_void,
                &mut len,
            );
        };

        // online average update formula
        self.mean_rx_cnt += 1;
        self.mean_rx =
            self.mean_rx + (self.tcp_info.tcpi_delivery_rate as f64 - self.mean_rx) / self.mean_rx_cnt as f64;
        self.mean_rx_last_checked = Instant::now();

        // Upper Confidence Bound
        let c = 1f64;
        let t = self.sharing.progress.completed.load(Ordering::SeqCst) as f64;
        let n_t = self.picked_count as f64;
        self.ucb = self.mean_rx + c * (t.ln() / n_t).sqrt();
    }

    pub async fn handle_one(&mut self) {
        if let Some(msg) = self.reader.next().await {
            match msg.unwrap() {
                BtMessage::KeepAlive(_) => return,
                BtMessage::Choke(_) => self.choked = true,
                BtMessage::Unchoke(_) => self.choked = false,
                BtMessage::Interested(_) => self.interested = true,
                BtMessage::NotInterested(_) => self.interested = false,
                BtMessage::Have(have) => {
                    let index = have.checked / 8;
                    let offset = have.checked % 8;

                    let flag = 1u8 << offset;
                    self.they_have[index as usize] |= flag;
                }
                BtMessage::BitField(bit_field) => {
                    self.they_have = bit_field.has;
                }
                BtMessage::Request(request) => {
                    if !self.sharing.is_done(request.index) {
                        return;
                    }

                    // let sharing = self.sharing.clone();
                    // let piece_len = self.sharing.page_len;
                    // let buf = task::spawn_blocking(move || {
                    //     let mut buf = vec![];
                    //     buf.reserve(request.length as usize);
                    //     sharing
                    //         .file
                    //         .read_at(
                    //             &mut *buf,
                    //             (request.index as usize * piece_len + request.begin as usize) as u64,
                    //         )
                    //         .unwrap();
                    //     buf
                    // })
                    // .await
                    // .unwrap();

                    // let resp = BtMessage::Piece(Piece {
                    //     index: request.index,
                    //     begin: request.begin,
                    //     length: buf.len() as u32,
                    //     data: buf.into_boxed_slice(),
                    // });
                    // self.writer.send(resp).await.unwrap();
                    todo!()
                }
                BtMessage::Piece(piece) => {
                    todo!()
                    // if !self.requestd.contains_key(&Request {
                    //     index: piece.index,
                    //     begin: piece.begin,
                    //     length: piece.length,
                    // }) {
                    //     // be wary of strangers sending data you didn't ask for, ignore them
                    //     return;
                    // } else {
                    //     self.requestd.remove(&Request {
                    //         index: piece.index,
                    //         begin: piece.begin,
                    //         length: piece.length,
                    //     });
                    // }
                    // // TODO: the page size of the file and the block size might not match
                    // let sharing = self.sharing.clone();
                    // task::spawn_blocking(move || {
                    //     sharing.write(piece.index, piece.data);
                    // })
                    // .await
                    // .unwrap();
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
    piece_len: u32,
    pieces: Vec<[u8; 20]>,
    /// size of the file and the path
    files: Vec<(u32, PathBuf)>,

    /// size of all the files combined in bytes
    len: usize,
}

fn extract_torrent(torrent: BencodeItemView) -> anyhow::Result<Torrent> {
    let BencodeItemView::Dictionary(mut torrent) = torrent else {
        bail!("meta_file needs to be a dict");
    };

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
    root.push(str::from_utf8(name).unwrap());

    let BencodeItemView::Integer(piece_len) = info.remove(b"piece length".as_slice()).unwrap() else {
        panic!();
    };

    let BencodeItemView::ByteString(pieces) = info.remove(b"pieces".as_slice()).unwrap() else {
        panic!();
    };

    let mut files = vec![];

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

    Ok(Torrent {
        announce: String::from_utf8(Vec::from(announce)).unwrap(),
        piece_len: piece_len as u32,
        pieces: pieces.chunks(20).map(|e| e.try_into().unwrap()).collect(),
        len: files.iter().map(|(len, _f)| *len as usize).sum(),
        files,
    })
}

struct Identity {
    peer_id: [u8; 20],
    serving: SocketAddrV4,
}

struct PeerShare<'i, 't> {
    torrent: Torrent,
    peers: Vec<PeerContext<'t>>,
    sharing: Arc<SharedThing<'t>>,
    announce_interval: Duration,
    announce: String,
    info_hash: InfoHash,

    id: &'i Identity,
}

impl<'i, 't> PeerShare<'i, 't> {
    fn total_rx(&self) -> u64 {
        let mut total = 0;
        for p in &self.peers {
            total += p.tcp_info.tcpi_bytes_received;
        }

        total as u64
    }

    fn total_tx(&self) -> u64 {
        let mut total = 0;
        for p in &self.peers {
            total += p.tcp_info.tcpi_bytes_acked;
        }

        total as u64
    }

    async fn try_add_peer(&mut self, peer: SocketAddrV4) -> anyhow::Result<()> {
        let mut tcp = TcpStream::connect(peer).await?;
        let _ = shake_hands(&mut tcp, &self.info_hash, &self.id.peer_id).await?;
        let (reader, writer) = tcp.into_split();
        let peer = PeerContext {
            choked: false,
            interested: false,
            reader: FramedRead::new(reader, BtDecoder),
            writer: FramedWrite::new(writer, BtEncoder),
            sharing: Arc::clone(&self.sharing),
            they_have: vec![0u8; self.sharing.pieces.len()].into(),
            tcp_info: unsafe { zeroed() },

            requestd: BTreeMap::new(),
            mean_rx: 0.0,
            mean_rx_cnt: 0,
            mean_rx_last_checked: Instant::now(),
            ucb: 0.0,
            picked_count: 0,
        };

        self.peers.push(peer);
        Ok(())
    }

    fn choose(&mut self, piece: u32) -> Option<&mut PeerContext<'t>> {
        for p in &mut self.peers {
            p.update_stat();
        }

        // pick the best one using u.c.b.
        self.peers
            .iter_mut()
            .filter(|p| p.ready() && p.they_have(piece))
            .into_iter()
            .max_by(|lhs, rhs| lhs.ucb.total_cmp(&rhs.ucb))
    }

    async fn work(&mut self) {
        let mut want: Vec<u32> = (0..self.torrent.pieces.len()).map(|p| p.try_into().unwrap()).collect();
        want.shuffle(&mut rand::rng());

        loop {
            if self.sharing.all_done() {
                break;
            }

            let piece = want.last().unwrap();
            let best = self.choose(*piece);
            if best.is_none() {
                time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let best = best.unwrap();
            best.request_piece(*piece).await.unwrap();
        }
    }

    async fn announce(&self) -> anyhow::Result<(Duration, Vec<SocketAddrV4>)> {
        // URL-encode info_hash and peer_id
        let info_hash_encoded: String = form_urlencoded::byte_serialize(&self.info_hash.0).collect();
        let peer_id_encoded: String = form_urlencoded::byte_serialize(&self.id.peer_id).collect();

        // Build the announce URL
        let url = format!(
            "{tracker_url}?info_hash={info_hash}&peer_id={peer_id}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&compact=1",
            tracker_url = self.torrent.announce,
            info_hash = info_hash_encoded,
            peer_id = peer_id_encoded,
            port = self.id.serving.port(),
            uploaded = self.total_tx(),
            downloaded = self.total_rx(),
            left = self.sharing.remaining_bytes(),
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

#[tokio::main]
async fn main() {
    println!("Hello, world!");
}
