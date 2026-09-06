use crate::defs::Identity;
use midwest_mainline::types::InfoHash;
use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::{
    bytes::Buf,
    codec::{Decoder, Encoder},
};
use tracing::info;
use tracing::warn;
use zerocopy::FromBytes;
use zerocopy::Immutable;
use zerocopy::IntoBytes;
use zerocopy::KnownLayout;
use zerocopy::Unaligned;

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

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct KeepAlive;
impl Encode for KeepAlive {
    fn encode(&self, buf: &mut [u8]) {
        buf.copy_from_slice(&0u32.to_be_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct Choke;
impl Encode for Choke {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        length.copy_from_slice(&1u32.to_be_bytes());
        header.copy_from_slice(&[0u8]);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct Unchoke;
impl Encode for Unchoke {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        length.copy_from_slice(&1u32.to_be_bytes());
        header.copy_from_slice(&[1u8]);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct Interested;
impl Encode for Interested {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        length.copy_from_slice(&1u32.to_be_bytes());
        header.copy_from_slice(&[2u8]);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct NotInterested;
impl Encode for NotInterested {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        length.copy_from_slice(&1u32.to_be_bytes());
        header.copy_from_slice(&[3u8]);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy, Default)]
pub struct Request {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

impl Encode for Request {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        let (header, body) = header.split_at_mut(1);
        length.copy_from_slice(&(1 + 12_u32).to_be_bytes());
        header.copy_from_slice(&[6u8]);
        body.copy_from_slice(&u32s_to_be_bytes!(self.index, self.begin, self.length));
    }
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
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
        length.copy_from_slice(&((1 + 8 + self.data.len()) as u32).to_be_bytes());
        header.copy_from_slice(&[7u8]);
        let (index_begin, data) = body.split_at_mut(8);
        index_begin[0..4].copy_from_slice(&self.index.to_be_bytes());
        index_begin[4..8].copy_from_slice(&self.begin.to_be_bytes());
        data.copy_from_slice(&self.data);
    }
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy, Default)]
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

/// BEP 6 (Fast Extension): an advisory hint that the sender suggests downloading this piece.
/// Purely advisory -- a receiver is free to ignore it.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct SuggestPiece {
    pub piece: u32,
}

impl Encode for SuggestPiece {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        let (header, body) = header.split_at_mut(1);
        length.copy_from_slice(&5u32.to_be_bytes());
        header.copy_from_slice(&[13u8]);
        body.copy_from_slice(&self.piece.to_be_bytes());
    }
}

/// BEP 6 (Fast Extension): sent in place of `BitField` when the sender has every piece --
/// smaller than sending a full one-bits bitfield.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct HaveAll;
impl Encode for HaveAll {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        length.copy_from_slice(&1u32.to_be_bytes());
        header.copy_from_slice(&[14u8]);
    }
}

/// BEP 5: the UDP port the peer's DHT node listens on, sent after the handshake by peers
/// that set the DHT bit in the reserved bytes.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct Port {
    pub port: u16,
}

impl Encode for Port {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        let (header, body) = header.split_at_mut(1);
        length.copy_from_slice(&3u32.to_be_bytes());
        header.copy_from_slice(&[9u8]);
        body.copy_from_slice(&self.port.to_be_bytes());
    }
}

/// BEP 6 (Fast Extension): sent in place of `BitField` when the sender has no pieces at all.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct HaveNone;
impl Encode for HaveNone {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        length.copy_from_slice(&1u32.to_be_bytes());
        header.copy_from_slice(&[15u8]);
    }
}

/// BEP 6 (Fast Extension): once the fast extension is enabled for a connection, a peer MUST
/// send this for any `Request` it declines to service, instead of the classic protocol's
/// silent drop -- lets the requester stop waiting immediately rather than idling out a timeout.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy, Default)]
pub struct RejectRequest {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

impl Encode for RejectRequest {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        let (header, body) = header.split_at_mut(1);
        length.copy_from_slice(&(1 + 12u32).to_be_bytes());
        header.copy_from_slice(&[16u8]);
        body.copy_from_slice(&u32s_to_be_bytes!(self.index, self.begin, self.length));
    }
}

/// BEP 6 (Fast Extension): a hint that the receiver may request this piece even while choked.
/// Purely advisory -- acting on it is optional for the receiver.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct AllowedFast {
    pub piece: u32,
}

impl Encode for AllowedFast {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        let (header, body) = header.split_at_mut(1);
        length.copy_from_slice(&5u32.to_be_bytes());
        header.copy_from_slice(&[17u8]);
        body.copy_from_slice(&self.piece.to_be_bytes());
    }
}

/// BEP 10 extension protocol message: `<len><id=20><ext_id><payload>`. `ext_id` 0 is always the
/// extended handshake itself; any other value is whatever the two peers negotiated for a given
/// named extension (e.g. "ut_metadata") in their respective handshakes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extended {
    pub ext_id: u8,
    pub payload: Box<[u8]>,
}

impl Encode for Extended {
    fn encode(&self, buf: &mut [u8]) {
        let (length, header) = buf.split_at_mut(4);
        let (header, body) = header.split_at_mut(1);
        length.copy_from_slice(&((1 + 1 + self.payload.len()) as u32).to_be_bytes());
        header.copy_from_slice(&[20u8]);
        let (ext_id_byte, payload) = body.split_at_mut(1);
        ext_id_byte[0] = self.ext_id;
        payload.copy_from_slice(&self.payload);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    SuggestPiece(SuggestPiece),
    HaveAll(HaveAll),
    HaveNone(HaveNone),
    RejectRequest(RejectRequest),
    AllowedFast(AllowedFast),
    Extended(Extended),
    Port(Port),
    Unknown(u8, #[allow(unused)] Box<[u8]>),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BtEncoder;

impl Encoder<BtMessage> for BtEncoder {
    type Error = io::Error;

    fn encode(&mut self, item: BtMessage, dst: &mut tokio_util::bytes::BytesMut) -> Result<(), Self::Error> {
        // Total on-wire size, including the 4-byte length prefix itself.
        let total_len = match &item {
            BtMessage::KeepAlive(_) => 4,
            BtMessage::Choke(_) => 5,
            BtMessage::Unchoke(_) => 5,
            BtMessage::Interested(_) => 5,
            BtMessage::NotInterested(_) => 5,
            BtMessage::Have(_) => 9,
            BtMessage::BitField(bit_field) => 5 + bit_field.has.len(),
            BtMessage::Request(_) => 17,
            BtMessage::Piece(piece) => 13 + piece.data.len(),
            BtMessage::Cancel(_) => 17,
            BtMessage::SuggestPiece(_) => 9,
            BtMessage::HaveAll(_) => 5,
            BtMessage::HaveNone(_) => 5,
            BtMessage::RejectRequest(_) => 17,
            BtMessage::AllowedFast(_) => 9,
            BtMessage::Extended(ext) => 6 + ext.payload.len(),
            BtMessage::Port(_) => 7,
            BtMessage::Unknown(..) => panic!("cannot encode an Unknown message"),
        };

        let start = dst.len();
        dst.resize(start + total_len, 0);
        let buf = &mut dst[start..];

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
            BtMessage::SuggestPiece(suggest) => suggest.encode(buf),
            BtMessage::HaveAll(have_all) => have_all.encode(buf),
            BtMessage::HaveNone(have_none) => have_none.encode(buf),
            BtMessage::RejectRequest(reject) => reject.encode(buf),
            BtMessage::AllowedFast(allowed_fast) => allowed_fast.encode(buf),
            BtMessage::Extended(ext) => ext.encode(buf),
            BtMessage::Port(port) => port.encode(buf),
            BtMessage::Unknown(..) => panic!(),
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BtDecoder;

/// Both halves in one type, for a `Framed<TcpStream, _>` that reads and writes through the
/// same object (`FramedRead`/`FramedWrite` over split halves want the two separate types).
#[derive(Debug, Clone, Copy)]
pub(crate) struct BtCodec;

impl Encoder<BtMessage> for BtCodec {
    type Error = io::Error;

    fn encode(&mut self, item: BtMessage, dst: &mut tokio_util::bytes::BytesMut) -> Result<(), Self::Error> {
        BtEncoder.encode(item, dst)
    }
}

impl Decoder for BtCodec {
    type Item = BtMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut tokio_util::bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        BtDecoder.decode(src)
    }
}

impl Decoder for BtDecoder {
    type Item = BtMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut tokio_util::bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(length_bytes) as usize;

        if src.len() < 4 + length {
            return Ok(None);
        }

        if length == 0 {
            src.advance(4);
            let keep_alive = BtMessage::KeepAlive(KeepAlive);
            return Ok(Some(keep_alive));
        }

        let msg_type = src[4];
        let msg = {
            // the byte range [5, 4 + length) is the payload following the 1-byte message id;
            // `length` counts the id byte itself, so the payload is `length - 1` bytes
            let buf = &src[5..4 + length];
            match msg_type {
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
                    // on the wire a piece message is just <index><begin><block>, with no
                    // separate length field -- the block extends to the end of the message
                    let index = u32::from_be_bytes(buf[0..4].try_into().unwrap());
                    let begin = u32::from_be_bytes(buf[4..8].try_into().unwrap());
                    let data: Box<[u8]> = Box::from(&buf[8..]);
                    let length = data.len() as u32;
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
                9 if buf.len() == 2 => BtMessage::Port(Port {
                    port: u16::from_be_bytes([buf[0], buf[1]]),
                }),
                13 => BtMessage::SuggestPiece(SuggestPiece {
                    piece: u32::from_be_bytes(buf[0..4].try_into().unwrap()),
                }),
                14 => BtMessage::HaveAll(HaveAll),
                15 => BtMessage::HaveNone(HaveNone),
                16 => {
                    let index = u32::from_be_bytes(buf[0..4].try_into().unwrap());
                    let begin = u32::from_be_bytes(buf[4..8].try_into().unwrap());
                    let length = u32::from_be_bytes(buf[8..12].try_into().unwrap());
                    BtMessage::RejectRequest(RejectRequest { index, begin, length })
                }
                17 => BtMessage::AllowedFast(AllowedFast {
                    piece: u32::from_be_bytes(buf[0..4].try_into().unwrap()),
                }),
                20 => {
                    // BEP 10: <ext_id><payload>, with no wire-level length field of its own
                    let ext_id = buf[0];
                    let payload = Box::from(&buf[1..]);
                    BtMessage::Extended(Extended { ext_id, payload })
                }
                t => BtMessage::Unknown(t, Box::from(buf)),
            }
        };

        src.advance(4 + length);
        Ok(Some(msg))
    }
}

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub(crate) struct Handshake {
    pub extensions: [u8; 8],
    pub info_hash: InfoHash,
    pub peer_id: [u8; 20],
}

// pub const HANDSHAKE_STR: &'static [u8] = b"19BitTorrent protocol";
pub const HANDSHAKE_STR: &[u8] = b"\x13BitTorrent protocol";

impl Handshake {
    /// BEP 10: bit 0x10 of reserved byte 5 (0-indexed from the start of the 8-byte reserved area)
    /// signals extension protocol support.
    pub fn supports_extensions(&self) -> bool {
        self.extensions[5] & 0x10 != 0
    }

    /// BEP 6: bit 0x04 of reserved byte 7 signals Fast Extension support.
    pub fn supports_fast_extension(&self) -> bool {
        self.extensions[7] & 0x04 != 0
    }

    /// BEP 5: bit 0x01 of reserved byte 7 says the peer runs a DHT node and will send its
    /// port after the handshake.
    pub fn supports_dht(&self) -> bool {
        self.extensions[7] & 0x01 != 0
    }
}

/// Sends our half of the handshake. Used both when we dial out (before reading the remote's
/// handshake) and when we accept an inbound connection (after we've read theirs and confirmed
/// we have a matching torrent).
pub(crate) async fn send_handshake(peer: &mut TcpStream, info_hash: &InfoHash, local_id: &Identity) -> io::Result<()> {
    let mut extensions = [0u8; 8];
    extensions[5] |= 0x10; // BEP 10: we support the extension protocol
    extensions[7] |= 0x04; // BEP 6: we support the fast extension
    if local_id.dht {
        extensions[7] |= 0x01; // BEP 5: we'll send our DHT port
    }

    let mut buf = vec![];
    buf.extend_from_slice(HANDSHAKE_STR);
    buf.extend_from_slice(&extensions);
    buf.extend_from_slice(info_hash.as_bytes());
    buf.extend_from_slice(&local_id.peer_id);

    debug_assert!(buf.len() == 68);
    peer.write_all(&buf).await
}

/// Reads the remote's half of the handshake, without checking which info hash it names --
/// the accept path needs to read this first to find out which torrent (if any) the connection
/// is for, before it knows what to check against.
pub(crate) async fn read_handshake(peer: &mut TcpStream) -> io::Result<Handshake> {
    let mut read_buf = [0u8; HANDSHAKE_STR.len() + size_of::<Handshake>()];
    let Ok(_) = peer.read_exact(&mut read_buf).await else {
        info!("Peer didn't send enough bytes for a handshake");
        return Err(io::Error::other("early EOF during handshake"));
    };

    if read_buf[..HANDSHAKE_STR.len()] != *HANDSHAKE_STR {
        warn!(
            "protocol initiation string didn't match, expected {:?}, got {:?} from {}",
            HANDSHAKE_STR,
            &read_buf[..HANDSHAKE_STR.len()],
            peer.peer_addr().unwrap(),
        );
        peer.shutdown().await?;
        return Err(io::Error::other("protocol string didn't match"));
    }

    let handshake = Handshake::ref_from_bytes(&read_buf[HANDSHAKE_STR.len()..]).expect("shit should work");
    Ok(*handshake)
}

/// Performs the outbound side of a handshake: send ours, then read and validate theirs.
/// `TcpStream::connect` bounded by `CONNECT_TIMEOUT`, with a timeout reported like any other
/// connect failure.
pub(crate) async fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
    tokio::time::timeout(crate::settings::CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .unwrap_or_else(|_| Err(io::Error::new(ErrorKind::TimedOut, "connect timed out")))
}

#[tracing::instrument(skip(peer))]
pub(crate) async fn shake_hands(
    peer: &mut TcpStream,
    info_hash: &InfoHash,
    local_id: &Identity,
) -> io::Result<Handshake> {
    send_handshake(peer, info_hash, local_id).await?;
    let handshake = read_handshake(peer).await?;

    if &handshake.info_hash != info_hash {
        warn!(
            "handshake info hash didn't match, expected {:?}, got {:?}",
            info_hash, handshake.info_hash,
        );
        peer.shutdown().await?;
        return Err(io::Error::other("handshake hash info didn't match"));
    }

    Ok(handshake)
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_util::bytes::BytesMut;

    /// Exercises the outbound (`shake_hands`) and inbound (`read_handshake` then
    /// `send_handshake`) halves against each other over a real loopback socket, since
    /// splitting them apart is the change most likely to silently break framing.
    #[tokio::test]
    async fn handshake_round_trips_over_a_real_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let info_hash = InfoHash::from_bytes(&[7u8; 20]);
        let dialer_id = [1u8; 20];
        let acceptor_id = [2u8; 20];
        let identity = |peer_id: [u8; 20], dht: bool| Identity {
            peer_id,
            serving: "127.0.0.1:0".parse().unwrap(),
            dht,
        };

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let handshake = read_handshake(&mut sock).await.unwrap();
            assert_eq!(handshake.info_hash, info_hash);
            assert_eq!(handshake.peer_id, dialer_id);
            assert!(handshake.supports_dht());
            send_handshake(&mut sock, &info_hash, &identity(acceptor_id, false))
                .await
                .unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let handshake = shake_hands(&mut client, &info_hash, &identity(dialer_id, true))
            .await
            .unwrap();
        assert_eq!(handshake.peer_id, acceptor_id);
        assert!(!handshake.supports_dht());
        assert!(handshake.supports_extensions() && handshake.supports_fast_extension());

        server.await.unwrap();
    }

    fn round_trip(msg: BtMessage) -> BtMessage {
        let mut buf = BytesMut::new();
        BtEncoder.encode(msg, &mut buf).unwrap();
        let decoded = BtDecoder.decode(&mut buf).unwrap().expect("a full message");
        assert!(buf.is_empty(), "decoder should consume the whole frame");
        decoded
    }

    #[test]
    fn keep_alive_round_trips() {
        assert_eq!(
            round_trip(BtMessage::KeepAlive(KeepAlive)),
            BtMessage::KeepAlive(KeepAlive)
        );
    }

    #[test]
    fn choke_round_trips() {
        assert_eq!(round_trip(BtMessage::Choke(Choke)), BtMessage::Choke(Choke));
    }

    #[test]
    fn unchoke_round_trips() {
        assert_eq!(round_trip(BtMessage::Unchoke(Unchoke)), BtMessage::Unchoke(Unchoke));
    }

    #[test]
    fn interested_round_trips() {
        assert_eq!(
            round_trip(BtMessage::Interested(Interested)),
            BtMessage::Interested(Interested)
        );
    }

    #[test]
    fn not_interested_round_trips() {
        assert_eq!(
            round_trip(BtMessage::NotInterested(NotInterested)),
            BtMessage::NotInterested(NotInterested)
        );
    }

    #[test]
    fn have_round_trips() {
        let have = Have { checked: 0x1234abcd };
        assert_eq!(round_trip(BtMessage::Have(have)), BtMessage::Have(have));
    }

    #[test]
    fn bitfield_round_trips() {
        let bit_field = BitField {
            has: Box::from([0xffu8, 0x00, 0xa5]),
        };
        assert_eq!(
            round_trip(BtMessage::BitField(bit_field.clone())),
            BtMessage::BitField(bit_field)
        );
    }

    #[test]
    fn request_round_trips() {
        let request = Request {
            index: 1,
            begin: 2,
            length: 3,
        };
        assert_eq!(round_trip(BtMessage::Request(request)), BtMessage::Request(request));
    }

    #[test]
    fn cancel_round_trips() {
        let cancel = Cancel {
            index: 1,
            begin: 2,
            length: 3,
        };
        assert_eq!(round_trip(BtMessage::Cancel(cancel)), BtMessage::Cancel(cancel));
    }

    #[test]
    fn piece_round_trips() {
        let piece = Piece {
            index: 7,
            begin: 16384,
            length: 4,
            data: Box::from([1u8, 2, 3, 4]),
        };
        assert_eq!(round_trip(BtMessage::Piece(piece.clone())), BtMessage::Piece(piece));
    }

    #[test]
    fn extended_round_trips() {
        let ext = Extended {
            ext_id: 3,
            payload: Box::from(*b"d1:mi1ee"),
        };
        assert_eq!(round_trip(BtMessage::Extended(ext.clone())), BtMessage::Extended(ext));
    }

    #[test]
    fn extended_handshake_uses_ext_id_zero() {
        let ext = Extended {
            ext_id: 0,
            payload: Box::from(*b"d1:md11:ut_metadatai1ee13:metadata_sizei100ee"),
        };
        assert_eq!(round_trip(BtMessage::Extended(ext.clone())), BtMessage::Extended(ext));
    }

    #[test]
    fn port_round_trips() {
        let port = BtMessage::Port(Port { port: 6881 });
        assert_eq!(round_trip(port.clone()), port);
    }

    #[test]
    fn suggest_piece_round_trips() {
        let suggest = SuggestPiece { piece: 42 };
        assert_eq!(
            round_trip(BtMessage::SuggestPiece(suggest)),
            BtMessage::SuggestPiece(suggest)
        );
    }

    #[test]
    fn have_all_round_trips() {
        assert_eq!(round_trip(BtMessage::HaveAll(HaveAll)), BtMessage::HaveAll(HaveAll));
    }

    #[test]
    fn have_none_round_trips() {
        assert_eq!(round_trip(BtMessage::HaveNone(HaveNone)), BtMessage::HaveNone(HaveNone));
    }

    #[test]
    fn reject_request_round_trips() {
        let reject = RejectRequest {
            index: 1,
            begin: 2,
            length: 3,
        };
        assert_eq!(
            round_trip(BtMessage::RejectRequest(reject)),
            BtMessage::RejectRequest(reject)
        );
    }

    #[test]
    fn allowed_fast_round_trips() {
        let allowed = AllowedFast { piece: 9 };
        assert_eq!(
            round_trip(BtMessage::AllowedFast(allowed)),
            BtMessage::AllowedFast(allowed)
        );
    }

    /// The two frames back to back exercise that the decoder only consumes exactly one
    /// frame's worth of bytes and leaves the rest for the next call, per BEP 3 framing.
    #[test]
    fn decoder_only_consumes_one_frame_at_a_time() {
        let mut buf = BytesMut::new();
        BtEncoder.encode(BtMessage::Unchoke(Unchoke), &mut buf).unwrap();
        BtEncoder.encode(BtMessage::Interested(Interested), &mut buf).unwrap();

        assert_eq!(buf.len(), 10);
        let first = BtDecoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(first, BtMessage::Unchoke(Unchoke));
        assert_eq!(buf.len(), 5);
        let second = BtDecoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(second, BtMessage::Interested(Interested));
        assert!(buf.is_empty());
    }

    /// Decoder must wait (return `Ok(None)`) instead of erroring or panicking when a frame
    /// has been announced by its length prefix but hasn't fully arrived yet.
    #[test]
    fn decoder_waits_for_a_full_frame() {
        let mut full = BytesMut::new();
        BtEncoder
            .encode(BtMessage::Have(Have { checked: 5 }), &mut full)
            .unwrap();

        let mut partial = BytesMut::from(&full[..full.len() - 1]);
        assert_eq!(BtDecoder.decode(&mut partial).unwrap(), None);
        assert_eq!(
            partial.len(),
            full.len() - 1,
            "decoder must not consume a partial frame"
        );
    }

    /// BEP 3 requires the length prefix to be big-endian (network byte order), and the piece
    /// message to carry only `<index><begin><block>` with no embedded length field.
    #[test]
    fn wire_bytes_match_bep3_exactly() {
        // unchoke: <len=0001><id=1>
        let mut buf = BytesMut::new();
        BtEncoder.encode(BtMessage::Unchoke(Unchoke), &mut buf).unwrap();
        assert_eq!(&buf[..], &[0, 0, 0, 1, 1]);

        // have: <len=0005><id=4><piece index>
        let mut buf = BytesMut::new();
        BtEncoder
            .encode(BtMessage::Have(Have { checked: 1 }), &mut buf)
            .unwrap();
        assert_eq!(&buf[..], &[0, 0, 0, 5, 4, 0, 0, 0, 1]);

        // piece: <len=0009+X><id=7><index><begin><block>, no separate length field
        let mut buf = BytesMut::new();
        BtEncoder
            .encode(
                BtMessage::Piece(Piece {
                    index: 1,
                    begin: 2,
                    length: 3,
                    data: Box::from([9u8, 8, 7]),
                }),
                &mut buf,
            )
            .unwrap();
        assert_eq!(&buf[..], &[0, 0, 0, 12, 7, 0, 0, 0, 1, 0, 0, 0, 2, 9, 8, 7]);

        // extended: <len=0002+X><id=20><ext_id><payload>, per BEP 10
        let mut buf = BytesMut::new();
        BtEncoder
            .encode(
                BtMessage::Extended(Extended {
                    ext_id: 5,
                    payload: Box::from([1u8, 2]),
                }),
                &mut buf,
            )
            .unwrap();
        assert_eq!(&buf[..], &[0, 0, 0, 4, 20, 5, 1, 2]);

        // have all / have none: <len=0001><id>, no payload, per BEP 6
        let mut buf = BytesMut::new();
        BtEncoder.encode(BtMessage::HaveAll(HaveAll), &mut buf).unwrap();
        assert_eq!(&buf[..], &[0, 0, 0, 1, 14]);

        let mut buf = BytesMut::new();
        BtEncoder.encode(BtMessage::HaveNone(HaveNone), &mut buf).unwrap();
        assert_eq!(&buf[..], &[0, 0, 0, 1, 15]);

        // reject request: <len=0013><id=16><index><begin><length>, per BEP 6
        let mut buf = BytesMut::new();
        BtEncoder
            .encode(
                BtMessage::RejectRequest(RejectRequest {
                    index: 1,
                    begin: 2,
                    length: 3,
                }),
                &mut buf,
            )
            .unwrap();
        assert_eq!(&buf[..], &[0, 0, 0, 13, 16, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3]);
    }
}
