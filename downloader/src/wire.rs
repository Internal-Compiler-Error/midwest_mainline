//! The peer wire protocol: BEP 3's length-prefixed messages and the BEP 6, 10 and 52 ones
//! that share the framing, and the handshake that comes before them.

use crate::defs::Identity;
use midwest_mainline::types::InfoHash;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use tracing::warn;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct KeepAlive;

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct Choke;

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct Unchoke;

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct Interested;

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct NotInterested;

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct Have {
    pub checked: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BitField {
    pub has: Box<[u8]>,
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy, Default)]
pub struct Request {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

/// A block. On the wire it's `<index><begin><data>`; `length` is `data`'s, for symmetry with
/// `Request`.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
pub struct Piece {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
    pub data: Box<[u8]>,
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy, Default)]
pub struct Cancel {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

/// BEP 6 (Fast Extension): an advisory hint that the sender suggests downloading this piece.
/// Purely advisory -- a receiver is free to ignore it.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct SuggestPiece {
    pub piece: u32,
}

/// BEP 6 (Fast Extension): sent in place of `BitField` when the sender has every piece --
/// smaller than sending a full one-bits bitfield.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct HaveAll;

/// BEP 5: the UDP port the peer's DHT node listens on, sent after the handshake by peers
/// that set the DHT bit in the reserved bytes.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct Port {
    pub port: u16,
}

/// BEP 6 (Fast Extension): sent in place of `BitField` when the sender has no pieces at all.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct HaveNone;

/// BEP 6 (Fast Extension): once the fast extension is enabled for a connection, a peer MUST
/// send this for any `Request` it declines to service, instead of the classic protocol's
/// silent drop -- lets the requester stop waiting immediately rather than idling out a timeout.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy, Default)]
pub struct RejectRequest {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

/// BEP 6 (Fast Extension): a hint that the receiver may request this piece even while choked.
/// Purely advisory -- acting on it is optional for the receiver.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, Hash, PartialOrd, Ord)]
pub struct AllowedFast {
    pub piece: u32,
}

/// BEP 10 extension protocol message: `<len><id=20><ext_id><payload>`. `ext_id` 0 is always the
/// extended handshake itself; any other value is whatever the two peers negotiated for a given
/// named extension (e.g. "ut_metadata") in their respective handshakes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extended {
    pub ext_id: u8,
    pub payload: Box<[u8]>,
}

/// BEP 52 `hash request` (id 21) and `hash reject` (id 23): which hashes of the tree under
/// `root` -- `length` of them from layer `base` (0 is the leaves), starting at `index` -- and
/// how many layers of uncle hashes above them to prove them with.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Hash, PartialOrd, Ord)]
pub struct HashRequest {
    pub root: [u8; 32],
    pub base: u32,
    pub index: u32,
    pub length: u32,
    pub proof_layers: u32,
}

impl HashRequest {
    const LEN: usize = 32 + 16;

    fn put(&self, dst: &mut BytesMut) {
        dst.put_slice(&self.root);
        for n in [self.base, self.index, self.length, self.proof_layers] {
            dst.put_u32(n);
        }
    }

    /// From a payload of at least `LEN` bytes.
    fn decode(buf: &[u8]) -> Self {
        Self {
            root: buf[..32].try_into().unwrap(),
            base: be_u32(buf, 32),
            index: be_u32(buf, 36),
            length: be_u32(buf, 40),
            proof_layers: be_u32(buf, 44),
        }
    }
}

/// BEP 52 `hashes` (id 22): the answer to a `HashRequest`, the requested hashes followed by
/// the uncle hashes, bottom up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hashes {
    pub request: HashRequest,
    pub hashes: Box<[[u8; 32]]>,
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
    HashRequest(HashRequest),
    Hashes(Hashes),
    HashReject(HashRequest),
    Unknown(u8, Box<[u8]>),
}

/// The message ids, for encoding and decoding alike.
mod id {
    pub const CHOKE: u8 = 0;
    pub const UNCHOKE: u8 = 1;
    pub const INTERESTED: u8 = 2;
    pub const NOT_INTERESTED: u8 = 3;
    pub const HAVE: u8 = 4;
    pub const BITFIELD: u8 = 5;
    pub const REQUEST: u8 = 6;
    pub const PIECE: u8 = 7;
    pub const CANCEL: u8 = 8;
    pub const PORT: u8 = 9;
    pub const SUGGEST_PIECE: u8 = 13;
    pub const HAVE_ALL: u8 = 14;
    pub const HAVE_NONE: u8 = 15;
    pub const REJECT_REQUEST: u8 = 16;
    pub const ALLOWED_FAST: u8 = 17;
    pub const EXTENDED: u8 = 20;
    pub const HASH_REQUEST: u8 = 21;
    pub const HASHES: u8 = 22;
    pub const HASH_REJECT: u8 = 23;
}

impl BtMessage {
    /// The message id; `None` for a keep-alive, which has none.
    fn id(&self) -> Option<u8> {
        Some(match self {
            BtMessage::KeepAlive(_) => return None,
            BtMessage::Choke(_) => id::CHOKE,
            BtMessage::Unchoke(_) => id::UNCHOKE,
            BtMessage::Interested(_) => id::INTERESTED,
            BtMessage::NotInterested(_) => id::NOT_INTERESTED,
            BtMessage::Have(_) => id::HAVE,
            BtMessage::BitField(_) => id::BITFIELD,
            BtMessage::Request(_) => id::REQUEST,
            BtMessage::Piece(_) => id::PIECE,
            BtMessage::Cancel(_) => id::CANCEL,
            BtMessage::Port(_) => id::PORT,
            BtMessage::SuggestPiece(_) => id::SUGGEST_PIECE,
            BtMessage::HaveAll(_) => id::HAVE_ALL,
            BtMessage::HaveNone(_) => id::HAVE_NONE,
            BtMessage::RejectRequest(_) => id::REJECT_REQUEST,
            BtMessage::AllowedFast(_) => id::ALLOWED_FAST,
            BtMessage::Extended(_) => id::EXTENDED,
            BtMessage::HashRequest(_) => id::HASH_REQUEST,
            BtMessage::Hashes(_) => id::HASHES,
            BtMessage::HashReject(_) => id::HASH_REJECT,
            BtMessage::Unknown(id, _) => *id,
        })
    }

    /// Appends everything after the id.
    fn put_payload(&self, dst: &mut BytesMut) {
        let block = |dst: &mut BytesMut, index, begin, length| {
            dst.put_u32(index);
            dst.put_u32(begin);
            dst.put_u32(length);
        };
        match self {
            BtMessage::KeepAlive(_)
            | BtMessage::Choke(_)
            | BtMessage::Unchoke(_)
            | BtMessage::Interested(_)
            | BtMessage::NotInterested(_)
            | BtMessage::HaveAll(_)
            | BtMessage::HaveNone(_) => {}
            BtMessage::Have(Have { checked: piece })
            | BtMessage::SuggestPiece(SuggestPiece { piece })
            | BtMessage::AllowedFast(AllowedFast { piece }) => dst.put_u32(*piece),
            BtMessage::BitField(bits) => dst.put_slice(&bits.has),
            BtMessage::Request(r) => block(dst, r.index, r.begin, r.length),
            BtMessage::Cancel(c) => block(dst, c.index, c.begin, c.length),
            BtMessage::RejectRequest(r) => block(dst, r.index, r.begin, r.length),
            BtMessage::Piece(piece) => {
                dst.put_u32(piece.index);
                dst.put_u32(piece.begin);
                dst.put_slice(&piece.data);
            }
            BtMessage::Port(port) => dst.put_u16(port.port),
            BtMessage::Extended(ext) => {
                dst.put_u8(ext.ext_id);
                dst.put_slice(&ext.payload);
            }
            BtMessage::HashRequest(request) | BtMessage::HashReject(request) => request.put(dst),
            BtMessage::Hashes(h) => {
                h.request.put(dst);
                dst.put_slice(h.hashes.as_flattened());
            }
            BtMessage::Unknown(_, payload) => dst.put_slice(payload),
        }
    }
}

/// The message codec, for a `Framed` (or `FramedRead`/`FramedWrite` over split halves).
#[derive(Debug, Clone, Copy)]
pub(crate) struct BtCodec;

impl Encoder<BtMessage> for BtCodec {
    type Error = io::Error;

    fn encode(&mut self, item: BtMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let start = dst.len();
        // the length prefix is filled in once the message's length is known
        dst.put_u32(0);
        if let Some(id) = item.id() {
            dst.put_u8(id);
            item.put_payload(dst);
        }
        let length = (dst.len() - start - 4) as u32;
        dst[start..start + 4].copy_from_slice(&length.to_be_bytes());
        Ok(())
    }
}

/// The longest message we accept. The big legitimate ones are a block (16 KiB, 128 KiB from
/// the most generous clients), a bitfield (one bit per piece: 128 KiB covers a million pieces)
/// and a ut_metadata piece (16 KiB plus a small dict). Anything longer is an attempt to make us
/// buffer gigabytes, since the length prefix is the peer's to choose.
pub const MAX_MESSAGE_LEN: usize = 1 << 20;

fn invalid(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_string())
}

fn be_u32(buf: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(buf[at..at + 4].try_into().unwrap())
}

impl Decoder for BtCodec {
    type Item = BtMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Some(prefix) = src.first_chunk::<4>() else {
            return Ok(None);
        };
        let length = u32::from_be_bytes(*prefix) as usize;
        if length > MAX_MESSAGE_LEN {
            return Err(invalid(&format!("message of {length} bytes is over the limit")));
        }
        if src.len() < 4 + length {
            // one allocation for the rest of the message instead of growing as it trickles in
            src.reserve(4 + length - src.len());
            return Ok(None);
        }
        let msg = match length {
            0 => BtMessage::KeepAlive(KeepAlive),
            _ => decode_message(src[4], &src[5..4 + length])?,
        };
        src.advance(4 + length);
        Ok(Some(msg))
    }
}

/// One message from its id and payload. The peer controls the length, so a payload the wrong
/// size for its id is refused here rather than indexed past its end.
fn decode_message(id: u8, buf: &[u8]) -> io::Result<BtMessage> {
    let exact = |n: usize| {
        if buf.len() == n {
            Ok(())
        } else {
            Err(invalid(&format!(
                "message {id} has {} payload bytes, expected {n}",
                buf.len()
            )))
        }
    };
    let piece = || exact(4).map(|()| be_u32(buf, 0));
    let block = || exact(12).map(|()| (be_u32(buf, 0), be_u32(buf, 4), be_u32(buf, 8)));
    Ok(match id {
        id::CHOKE => BtMessage::Choke(Choke),
        id::UNCHOKE => BtMessage::Unchoke(Unchoke),
        id::INTERESTED => BtMessage::Interested(Interested),
        id::NOT_INTERESTED => BtMessage::NotInterested(NotInterested),
        id::HAVE => BtMessage::Have(Have { checked: piece()? }),
        id::BITFIELD => BtMessage::BitField(BitField { has: Box::from(buf) }),
        id::REQUEST => {
            let (index, begin, length) = block()?;
            BtMessage::Request(Request { index, begin, length })
        }
        id::PIECE => {
            let Some((head, data)) = buf.split_first_chunk::<8>() else {
                return Err(invalid("piece message without index and offset"));
            };
            BtMessage::Piece(Piece {
                index: be_u32(head, 0),
                begin: be_u32(head, 4),
                length: data.len() as u32,
                data: Box::from(data),
            })
        }
        id::CANCEL => {
            let (index, begin, length) = block()?;
            BtMessage::Cancel(Cancel { index, begin, length })
        }
        id::PORT if buf.len() == 2 => BtMessage::Port(Port {
            port: u16::from_be_bytes([buf[0], buf[1]]),
        }),
        id::SUGGEST_PIECE => BtMessage::SuggestPiece(SuggestPiece { piece: piece()? }),
        id::HAVE_ALL => BtMessage::HaveAll(HaveAll),
        id::HAVE_NONE => BtMessage::HaveNone(HaveNone),
        id::REJECT_REQUEST => {
            let (index, begin, length) = block()?;
            BtMessage::RejectRequest(RejectRequest { index, begin, length })
        }
        id::ALLOWED_FAST => BtMessage::AllowedFast(AllowedFast { piece: piece()? }),
        id::EXTENDED => {
            let Some((&ext_id, payload)) = buf.split_first() else {
                return Err(invalid("extended message without an id"));
            };
            BtMessage::Extended(Extended {
                ext_id,
                payload: Box::from(payload),
            })
        }
        id::HASH_REQUEST => {
            exact(HashRequest::LEN)?;
            BtMessage::HashRequest(HashRequest::decode(buf))
        }
        id::HASHES => {
            let (hashes, rest) = buf.get(HashRequest::LEN..).unwrap_or_default().as_chunks::<32>();
            if buf.len() < HashRequest::LEN || !rest.is_empty() {
                return Err(invalid("hashes message isn't a request and whole hashes"));
            }
            BtMessage::Hashes(Hashes {
                request: HashRequest::decode(buf),
                hashes: hashes.into(),
            })
        }
        id::HASH_REJECT => {
            exact(HashRequest::LEN)?;
            BtMessage::HashReject(HashRequest::decode(buf))
        }
        other => BtMessage::Unknown(other, Box::from(buf)),
    })
}

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Default, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub(crate) struct Handshake {
    pub extensions: [u8; 8],
    pub info_hash: InfoHash,
    pub peer_id: [u8; 20],
}

/// The protocol string a handshake opens with, its length byte included.
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

    /// BEP 52: bit 0x10 of reserved byte 7 says the peer supports v2 torrents.
    pub fn supports_v2(&self) -> bool {
        self.extensions[7] & 0x10 != 0
    }
}

/// What a handshake of ours says about BEP 52, which depends on the torrent it's for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum V2Support {
    /// a v1 torrent, or a magnet with nothing but a v1 hash
    #[default]
    None,
    /// a v2-only torrent: its swarm goes by the v2 hash, and every peer in it speaks v2
    Only,
    /// a hybrid, handshaken under its v1 hash: the remote may answer with this, the truncated
    /// v2 hash, to upgrade the connection to v2
    Hybrid(InfoHash),
}

impl V2Support {
    /// Whether the connection `theirs` opened, or answered, can carry v2 messages (hash
    /// requests): for a hybrid, the remote said it supports v2 or went by the v2 hash.
    pub fn v2_peer(self, theirs: &Handshake) -> bool {
        match self {
            V2Support::None => false,
            V2Support::Only => true,
            V2Support::Hybrid(v2) => theirs.supports_v2() || theirs.info_hash == v2,
        }
    }

    /// The hash to answer an inbound handshake for this torrent with: a hybrid's v1 hash is
    /// upgraded to the v2 one when the remote supports v2, as BEP 52 lets the answering side do.
    pub fn answer(self, theirs: &Handshake) -> InfoHash {
        match self {
            V2Support::Hybrid(v2) if theirs.supports_v2() => v2,
            _ => theirs.info_hash,
        }
    }
}

/// Sends our half of the handshake. Used both when we dial out (before reading the remote's
/// handshake) and when we accept an inbound connection (after we've read theirs and confirmed
/// we have a matching torrent).
pub(crate) async fn send_handshake<S: AsyncWrite + Unpin>(
    peer: &mut S,
    info_hash: &InfoHash,
    local_id: &Identity,
    v2: V2Support,
) -> io::Result<()> {
    let mut extensions = [0u8; 8];
    extensions[5] |= 0x10; // BEP 10: we support the extension protocol
    extensions[7] |= 0x04; // BEP 6: we support the fast extension
    if v2 != V2Support::None {
        extensions[7] |= 0x10; // BEP 52: we support v2 torrents
    }
    if local_id.dht {
        extensions[7] |= 0x01; // BEP 5: we'll send our DHT port
    }
    let ours = Handshake {
        extensions,
        info_hash: *info_hash,
        peer_id: local_id.peer_id,
    };
    let mut buf = [0u8; HANDSHAKE_STR.len() + size_of::<Handshake>()];
    buf[..HANDSHAKE_STR.len()].copy_from_slice(HANDSHAKE_STR);
    buf[HANDSHAKE_STR.len()..].copy_from_slice(ours.as_bytes());
    peer.write_all(&buf).await
}

/// Reads the remote's half of the handshake, without checking which info hash it names --
/// the accept path needs to read this first to find out which torrent (if any) the connection
/// is for, before it knows what to check against.
pub(crate) async fn read_handshake<S: AsyncRead + Unpin>(peer: &mut S) -> io::Result<Handshake> {
    let mut head = [0u8; HANDSHAKE_STR.len()];
    peer.read_exact(&mut head).await?;
    if head != *HANDSHAKE_STR {
        warn!("protocol initiation string didn't match, expected {HANDSHAKE_STR:?}, got {head:?}");
        return Err(io::Error::other("protocol string didn't match"));
    }
    read_handshake_body(peer).await
}

/// The rest of a handshake once the protocol string has been read and checked; the accept
/// path reads that string first to tell a plaintext peer from an encrypted one.
pub(crate) async fn read_handshake_body<S: AsyncRead + Unpin>(peer: &mut S) -> io::Result<Handshake> {
    let mut body = [0u8; size_of::<Handshake>()];
    peer.read_exact(&mut body).await?;
    Ok(Handshake::read_from_bytes(&body).expect("the buffer is a handshake's size"))
}

/// The outbound side of a handshake: sends ours, then reads theirs and checks it names
/// `info_hash` (or, for a hybrid, its v2 hash: the remote upgraded the connection).
#[tracing::instrument(skip(peer))]
pub(crate) async fn shake_hands<S: AsyncRead + AsyncWrite + Unpin>(
    peer: &mut S,
    info_hash: &InfoHash,
    local_id: &Identity,
    v2: V2Support,
) -> io::Result<Handshake> {
    send_handshake(peer, info_hash, local_id, v2).await?;
    let handshake = read_handshake(peer).await?;

    let upgraded = matches!(v2, V2Support::Hybrid(v2) if handshake.info_hash == v2);
    if &handshake.info_hash != info_hash && !upgraded {
        warn!(
            "handshake info hash didn't match, expected {:?}, got {:?}",
            info_hash, handshake.info_hash,
        );
        peer.shutdown().await?;
        return Err(io::Error::other("handshake hash info didn't match"));
    }
    if upgraded {
        tracing::debug!("the peer upgraded the connection to the v2 hash");
    }

    Ok(handshake)
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::bytes::BytesMut;

    #[test]
    fn short_and_oversized_messages_are_errors_not_panics() {
        for frame in [
            &b"\x00\x00\x00\x01\x04"[..],                // Have without its index
            b"\x00\x00\x00\x05\x06\x00\x00\x00\x01",     // Request with 4 of 12 bytes
            b"\x00\x00\x00\x03\x07\x00\x00",             // Piece without index and offset
            b"\x00\x00\x00\x01\x08",                     // Cancel
            b"\x00\x00\x00\x01\x0d",                     // Suggest
            b"\x00\x00\x00\x01\x10",                     // Reject
            b"\x00\x00\x00\x01\x11",                     // AllowedFast
            b"\x00\x00\x00\x01\x14",                     // Extended without an id
            b"\x00\x00\x00\x06\x04\x00\x00\x00\x01\x00", // Have with a trailing byte
        ] {
            let mut src = BytesMut::from(frame);
            assert!(BtCodec.decode(&mut src).is_err(), "{frame:?}");
        }

        let mut huge = BytesMut::from(&b"\xff\xff\xff\xff\x07"[..]);
        assert!(
            BtCodec.decode(&mut huge).is_err(),
            "a 4 GiB length is refused before buffering"
        );
    }

    #[test]
    fn a_well_formed_have_still_decodes() {
        let mut src = BytesMut::from(&b"\x00\x00\x00\x05\x04\x00\x00\x00\x2a"[..]);
        assert!(matches!(
            BtCodec.decode(&mut src).unwrap(),
            Some(BtMessage::Have(Have { checked: 42 }))
        ));
        assert!(src.is_empty());
    }

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
            encryption: crate::config::Encryption::Disabled,
        };

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let handshake = read_handshake(&mut sock).await.unwrap();
            assert_eq!(handshake.info_hash, info_hash);
            assert_eq!(handshake.peer_id, dialer_id);
            assert!(handshake.supports_dht());
            send_handshake(&mut sock, &info_hash, &identity(acceptor_id, false), V2Support::None)
                .await
                .unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let handshake = shake_hands(&mut client, &info_hash, &identity(dialer_id, true), V2Support::None)
            .await
            .unwrap();
        assert_eq!(handshake.peer_id, acceptor_id);
        assert!(!handshake.supports_dht());
        assert!(handshake.supports_extensions() && handshake.supports_fast_extension());

        server.await.unwrap();
    }

    /// BEP 52's upgrade: a hybrid dialled under its v1 hash with the v2 bit set is answered
    /// with its v2 hash, which the dialler takes; without the bit it stays v1, and any other
    /// hash is still refused.
    #[tokio::test]
    async fn a_hybrid_connection_upgrades_to_v2() {
        let (v1, v2) = (InfoHash::from_bytes(&[1; 20]), InfoHash::from_bytes(&[2; 20]));
        let hybrid = V2Support::Hybrid(v2);
        let identity = Identity {
            peer_id: [9; 20],
            serving: "127.0.0.1:0".parse().unwrap(),
            dht: false,
            encryption: crate::config::Encryption::Disabled,
        };
        // the acceptor's view, as `bt_client::welcome` has it, and what the dialler ends up with
        async fn open(
            dialler: V2Support,
            acceptor: V2Support,
            answer: Option<InfoHash>,
            identity: &Identity,
        ) -> (Handshake, io::Result<Handshake>) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let id = *identity;
            let server = tokio::spawn(async move {
                let (mut sock, _) = listener.accept().await.unwrap();
                let theirs = read_handshake(&mut sock).await.unwrap();
                let answer = answer.unwrap_or_else(|| acceptor.answer(&theirs));
                send_handshake(&mut sock, &answer, &id, acceptor).await.unwrap();
                theirs
            });
            let mut client = TcpStream::connect(addr).await.unwrap();
            let ours = shake_hands(&mut client, &InfoHash::from_bytes(&[1; 20]), identity, dialler).await;
            (server.await.unwrap(), ours)
        }

        let (theirs, ours) = open(hybrid, hybrid, None, &identity).await;
        assert!(theirs.supports_v2() && hybrid.v2_peer(&theirs));
        let ours = ours.expect("the v2 hash is a valid answer");
        assert_eq!(ours.info_hash, v2, "upgraded");
        assert!(ours.supports_v2() && hybrid.v2_peer(&ours));

        let (theirs, ours) = open(V2Support::None, hybrid, None, &identity).await;
        assert!(!theirs.supports_v2() && !hybrid.v2_peer(&theirs));
        assert_eq!(ours.unwrap().info_hash, v1, "a v1 peer isn't upgraded");

        let (_, ours) = open(V2Support::None, hybrid, Some(v2), &identity).await;
        assert!(ours.is_err(), "only a hybrid's dialler takes the v2 hash");
        let (_, ours) = open(hybrid, hybrid, Some(InfoHash::from_bytes(&[3; 20])), &identity).await;
        assert!(ours.is_err());
    }

    #[test]
    fn every_message_round_trips() {
        for msg in [
            BtMessage::KeepAlive(KeepAlive),
            BtMessage::Choke(Choke),
            BtMessage::Unchoke(Unchoke),
            BtMessage::Interested(Interested),
            BtMessage::NotInterested(NotInterested),
            BtMessage::Have(Have { checked: 0x1234abcd }),
            BtMessage::BitField(BitField {
                has: Box::from([0xffu8, 0x00, 0xa5]),
            }),
            BtMessage::Request(Request {
                index: 1,
                begin: 2,
                length: 3,
            }),
            BtMessage::Cancel(Cancel {
                index: 1,
                begin: 2,
                length: 3,
            }),
            BtMessage::RejectRequest(RejectRequest {
                index: 1,
                begin: 2,
                length: 3,
            }),
            BtMessage::Piece(Piece {
                index: 7,
                begin: 16384,
                length: 4,
                data: Box::from([1u8, 2, 3, 4]),
            }),
            BtMessage::Extended(Extended {
                ext_id: 3,
                payload: Box::from(*b"d1:mi1ee"),
            }),
            BtMessage::Extended(Extended {
                ext_id: 0,
                payload: Box::from(*b"d1:md11:ut_metadatai1ee13:metadata_sizei100ee"),
            }),
            BtMessage::Port(Port { port: 6881 }),
            BtMessage::SuggestPiece(SuggestPiece { piece: 42 }),
            BtMessage::HaveAll(HaveAll),
            BtMessage::HaveNone(HaveNone),
            BtMessage::AllowedFast(AllowedFast { piece: 9 }),
            BtMessage::Unknown(99, Box::from(*b"whatever")),
        ] {
            let mut buf = BytesMut::new();
            BtCodec.encode(msg.clone(), &mut buf).unwrap();
            assert_eq!(BtCodec.decode(&mut buf).unwrap(), Some(msg));
            assert!(buf.is_empty(), "decoder should consume the whole frame");
        }
    }

    /// The two frames back to back exercise that the decoder only consumes exactly one
    /// frame's worth of bytes and leaves the rest for the next call, per BEP 3 framing.
    #[test]
    fn decoder_only_consumes_one_frame_at_a_time() {
        let mut buf = BytesMut::new();
        BtCodec.encode(BtMessage::Unchoke(Unchoke), &mut buf).unwrap();
        BtCodec.encode(BtMessage::Interested(Interested), &mut buf).unwrap();

        assert_eq!(buf.len(), 10);
        let first = BtCodec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(first, BtMessage::Unchoke(Unchoke));
        assert_eq!(buf.len(), 5);
        let second = BtCodec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(second, BtMessage::Interested(Interested));
        assert!(buf.is_empty());
    }

    /// Decoder must wait (return `Ok(None)`) instead of erroring or panicking when a frame
    /// has been announced by its length prefix but hasn't fully arrived yet.
    #[test]
    fn decoder_waits_for_a_full_frame() {
        let mut full = BytesMut::new();
        BtCodec.encode(BtMessage::Have(Have { checked: 5 }), &mut full).unwrap();

        let mut partial = BytesMut::from(&full[..full.len() - 1]);
        assert_eq!(BtCodec.decode(&mut partial).unwrap(), None);
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
        BtCodec.encode(BtMessage::Unchoke(Unchoke), &mut buf).unwrap();
        assert_eq!(&buf[..], &[0, 0, 0, 1, 1]);

        // have: <len=0005><id=4><piece index>
        let mut buf = BytesMut::new();
        BtCodec.encode(BtMessage::Have(Have { checked: 1 }), &mut buf).unwrap();
        assert_eq!(&buf[..], &[0, 0, 0, 5, 4, 0, 0, 0, 1]);

        // piece: <len=0009+X><id=7><index><begin><block>, no separate length field
        let mut buf = BytesMut::new();
        BtCodec
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
        BtCodec
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
        BtCodec.encode(BtMessage::HaveAll(HaveAll), &mut buf).unwrap();
        assert_eq!(&buf[..], &[0, 0, 0, 1, 14]);

        let mut buf = BytesMut::new();
        BtCodec.encode(BtMessage::HaveNone(HaveNone), &mut buf).unwrap();
        assert_eq!(&buf[..], &[0, 0, 0, 1, 15]);

        // reject request: <len=0013><id=16><index><begin><length>, per BEP 6
        let mut buf = BytesMut::new();
        BtCodec
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

    #[test]
    fn hash_messages_round_trip() {
        let request = HashRequest {
            root: [3; 32],
            base: 2,
            index: 512,
            length: 4,
            proof_layers: 5,
        };
        let hashes = Hashes {
            request,
            hashes: vec![[1; 32], [2; 32], [4; 32]].into(),
        };
        for msg in [
            BtMessage::HashRequest(request),
            BtMessage::HashReject(request),
            BtMessage::Hashes(hashes),
        ] {
            let mut buf = BytesMut::new();
            BtCodec.encode(msg.clone(), &mut buf).unwrap();
            assert_eq!(BtCodec.decode(&mut buf).unwrap(), Some(msg));
            assert!(buf.is_empty());
        }
        // a request short of its proof layers, and hashes that aren't whole
        for frame in [&b"\x00\x00\x00\x30\x15"[..], b"\x00\x00\x00\x32\x16"] {
            let mut src = BytesMut::from(frame);
            src.resize(4 + u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize, 0);
            assert!(BtCodec.decode(&mut src).is_err(), "{frame:?}");
        }
    }
}
