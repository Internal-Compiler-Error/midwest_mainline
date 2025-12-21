use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use std::io;
use std::io::ErrorKind;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::{
    bytes::Buf,
    codec::{Decoder, Encoder},
};

pub trait Encode {
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

pub(crate) enum BtMessage {
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct BtEncoder;

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

#[derive(Debug, Clone, Copy)]
pub(crate) struct BtDecoder;

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

pub(crate) struct Handshake {
    pub extensions: [u8; 8],
    pub info_hash: InfoHash,
    pub peer_id: [u8; 20],
}

pub const HANDSHAKE: &'static [u8] = b"\x13BitTorrent protocol";

pub(crate) async fn shake_hands(
    peer: &mut TcpStream,
    info_hash: &InfoHash,
    local_id: &[u8; 20],
) -> io::Result<Handshake> {
    let extensions = [0u8; 8];

    let mut buf = vec![];
    buf.extend_from_slice(HANDSHAKE);
    buf.extend_from_slice(&extensions);
    buf.extend_from_slice(info_hash.as_bytes());
    buf.extend_from_slice(local_id);

    peer.write_all(&*buf).await?;

    let read_buf = [0u8; 68];
    peer.read_exact(&mut buf).await?;

    // header, info_hash, and peer_id must match
    if read_buf[..20] != *HANDSHAKE || read_buf[28..48] != info_hash.0 || read_buf[48..68] != *local_id {
        peer.shutdown().await?;
        return Err(io::Error::new(ErrorKind::Other, "handshake info didn't match"));
    }

    Ok(Handshake {
        extensions: read_buf[20..28].try_into().unwrap(),
        info_hash: InfoHash(read_buf[28..48].try_into().unwrap()),
        peer_id: read_buf[48..68].try_into().unwrap(),
    })
}
