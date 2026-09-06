//! Message Stream Encryption, the "protocol encryption" every mainstream client speaks.
//!
//! Obfuscation, not security: a 768-bit Diffie-Hellman exchange derives RC4 keys, and the
//! torrent's info hash is what ties the keys to a torrent so a responder serving several can
//! tell which one is meant. It hides the BitTorrent protocol from shallow packet inspection
//! and nothing more. Only RC4 is offered and accepted; a peer that insists on the spec's
//! "plaintext after the key exchange" option is treated as not supporting encryption.
//!
//! Spec: <https://wiki.vuze.com/w/Message_Stream_Encryption>.

use midwest_mainline::types::InfoHash;
use num_bigint::BigUint;
use sha1::{Digest, Sha1};
use std::io;
use std::pin::Pin;
use std::sync::LazyLock;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

const PRIME_HEX: &str = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563";
static PRIME: LazyLock<BigUint> = LazyLock::new(|| BigUint::parse_bytes(PRIME_HEX.as_bytes(), 16).unwrap());
const GENERATOR: u32 = 2;
/// public keys and the shared secret are 768-bit numbers, sent big-endian
const KEY_LEN: usize = 96;
/// each side pads its public key with up to this many random bytes, so the other has to hunt
/// for the next fixed-length field
const MAX_PAD: usize = 512;
/// the "verification constant": 8 zero bytes, sent encrypted, that prove the keys agree
const VC: [u8; 8] = [0; 8];
const CRYPTO_RC4: u32 = 0x02;
/// the spec discards this much RC4 output before use, to dodge the cipher's weak start
const RC4_DISCARD: usize = 1024;

/// RC4, straight from the textbook. Nobody should use it for secrecy; MSE's goal is only to
/// not look like BitTorrent.
#[derive(Clone)]
struct Rc4 {
    s: [u8; 256],
    i: u8,
    j: u8,
}

impl Rc4 {
    fn new(key: &[u8]) -> Self {
        let mut s = [0u8; 256];
        for (i, v) in s.iter_mut().enumerate() {
            *v = i as u8;
        }
        let mut j: u8 = 0;
        for i in 0..256 {
            j = j.wrapping_add(s[i]).wrapping_add(key[i % key.len()]);
            s.swap(i, j as usize);
        }
        Self { s, i: 0, j: 0 }
    }

    fn discarding(key: &[u8]) -> Self {
        let mut rc4 = Self::new(key);
        rc4.apply(&mut [0u8; RC4_DISCARD]);
        rc4
    }

    /// Encrypts or decrypts `data` in place (the two are the same operation).
    fn apply(&mut self, data: &mut [u8]) {
        for byte in data {
            self.i = self.i.wrapping_add(1);
            self.j = self.j.wrapping_add(self.s[self.i as usize]);
            self.s.swap(self.i as usize, self.j as usize);
            let k = self.s[(self.s[self.i as usize].wrapping_add(self.s[self.j as usize])) as usize];
            *byte ^= k;
        }
    }
}

fn sha1(parts: &[&[u8]]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn xor(a: [u8; 20], b: [u8; 20]) -> [u8; 20] {
    std::array::from_fn(|i| a[i] ^ b[i])
}

struct KeyPair {
    private: BigUint,
    public: [u8; KEY_LEN],
}

fn keypair() -> KeyPair {
    // the spec recommends 160 bits of private key: the group's strength doesn't justify more
    let mut seed = [0u8; 20];
    rand::fill(&mut seed);
    let private = BigUint::from_bytes_be(&seed);
    let public = fixed_width(BigUint::from(GENERATOR).modpow(&private, &PRIME));
    KeyPair { private, public }
}

fn shared_secret(ours: &KeyPair, theirs: &[u8; KEY_LEN]) -> [u8; KEY_LEN] {
    fixed_width(BigUint::from_bytes_be(theirs).modpow(&ours.private, &PRIME))
}

/// `to_bytes_be` drops leading zeros; the wire wants exactly 96 bytes.
fn fixed_width(n: BigUint) -> [u8; KEY_LEN] {
    let bytes = n.to_bytes_be();
    let mut out = [0u8; KEY_LEN];
    out[KEY_LEN - bytes.len()..].copy_from_slice(&bytes);
    out
}

fn random_pad() -> Vec<u8> {
    let mut pad = vec![0u8; rand::random_range(0..=MAX_PAD)];
    rand::fill(&mut pad[..]);
    pad
}

fn err(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_owned())
}

/// Reads from `stream` until `needle` shows up, tolerating up to `max_before` bytes of
/// padding in front of it. Returns whatever was read past the needle.
async fn skip_to<S: AsyncRead + Unpin>(stream: &mut S, needle: &[u8], max_before: usize) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(at) = buf.windows(needle.len()).position(|w| w == needle) {
            return Ok(buf.split_off(at + needle.len()));
        }
        if buf.len() >= max_before + needle.len() {
            return Err(err("no sync marker within the allowed padding"));
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "peer hung up mid-handshake",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// The next `n` bytes of the handshake: from `ahead` (what an earlier read pulled in past
/// its own field) first, then the stream. Decrypted with `cipher` if there is one.
async fn take<S: AsyncRead + Unpin>(
    stream: &mut S,
    ahead: &mut Vec<u8>,
    cipher: Option<&mut Rc4>,
    n: usize,
) -> io::Result<Vec<u8>> {
    while ahead.len() < n {
        let mut chunk = [0u8; 1024];
        let got = stream.read(&mut chunk).await?;
        if got == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "peer hung up mid-handshake",
            ));
        }
        ahead.extend_from_slice(&chunk[..got]);
    }
    let rest = ahead.split_off(n);
    let mut field = std::mem::replace(ahead, rest);
    if let Some(cipher) = cipher {
        cipher.apply(&mut field);
    }
    Ok(field)
}

fn u16_of(bytes: &[u8]) -> usize {
    u16::from_be_bytes([bytes[0], bytes[1]]) as usize
}

/// A stream whose bytes are RC4'd both ways, once the handshake has agreed on keys.
pub(crate) struct Encrypted<S> {
    inner: S,
    rx: Rc4,
    tx: Rc4,
    /// plaintext that arrived in the same read as the handshake's last field, handed out
    /// before anything more is read from `inner`
    leftover: Vec<u8>,
    /// ciphertext accepted by `poll_write` and not yet written out. Bytes are encrypted
    /// exactly once, when accepted; a short write must not run them through the cipher
    /// again or the keystreams desynchronise
    pending: Vec<u8>,
    written: usize,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Encrypted<S> {
    fn new(inner: S, rx: Rc4, tx: Rc4, leftover: Vec<u8>) -> Self {
        Self {
            inner,
            rx,
            tx,
            leftover,
            pending: Vec::new(),
            written: 0,
        }
    }

    fn drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.written < self.pending.len() {
            let n = ready!(Pin::new(&mut self.inner).poll_write(cx, &self.pending[self.written..]))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.written += n;
        }
        self.pending.clear();
        self.written = 0;
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for Encrypted<S> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.leftover.is_empty() {
            let n = this.leftover.len().min(buf.remaining());
            buf.put_slice(&this.leftover[..n]);
            this.leftover.drain(..n);
            return Poll::Ready(Ok(()));
        }
        let before = buf.filled().len();
        ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
        this.rx.apply(&mut buf.filled_mut()[before..]);
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Encrypted<S> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        ready!(this.drain(cx))?;
        this.pending.extend_from_slice(buf);
        this.tx.apply(&mut this.pending);
        // best effort now; whatever doesn't go out is flushed by the next write or flush
        if let Poll::Ready(Err(e)) = this.drain(cx) {
            return Poll::Ready(Err(e));
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.drain(cx))?;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.drain(cx))?;
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

/// The initiator's side, for a connection we opened to a peer of `info_hash`. On success the
/// BitTorrent handshake goes through the returned stream like through a plain one.
pub(crate) async fn initiate<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    info_hash: &InfoHash,
) -> io::Result<Encrypted<S>> {
    let keys = keypair();
    let mut hello = keys.public.to_vec();
    hello.extend(random_pad());
    stream.write_all(&hello).await?;

    let mut their_public = [0u8; KEY_LEN];
    stream.read_exact(&mut their_public).await?;
    let secret = shared_secret(&keys, &their_public);
    let skey = info_hash.as_bytes();
    let mut tx = Rc4::discarding(&sha1(&[b"keyA", &secret, skey]));
    let mut rx = Rc4::discarding(&sha1(&[b"keyB", &secret, skey]));

    let mut msg = Vec::new();
    msg.extend(sha1(&[b"req1", &secret]));
    msg.extend(xor(sha1(&[b"req2", skey]), sha1(&[b"req3", &secret])));
    let mut sealed = Vec::new();
    sealed.extend(VC);
    sealed.extend(CRYPTO_RC4.to_be_bytes());
    sealed.extend(0u16.to_be_bytes()); // no PadC
    sealed.extend(0u16.to_be_bytes()); // no initial payload: the handshake follows separately
    tx.apply(&mut sealed);
    msg.extend(sealed);
    stream.write_all(&msg).await?;

    // their reply starts after PadB, whose length we don't know: hunt for the encrypted VC
    let mut marker = VC;
    rx.clone().apply(&mut marker);
    let mut ahead = skip_to(&mut stream, &marker, MAX_PAD).await?;
    rx.apply(&mut [0u8; VC.len()]); // advance past the marker we matched raw
    let select = take(&mut stream, &mut ahead, Some(&mut rx), 4).await?;
    if u32::from_be_bytes(select.try_into().unwrap()) != CRYPTO_RC4 {
        return Err(err("peer didn't select RC4"));
    }
    let pad_len = u16_of(&take(&mut stream, &mut ahead, Some(&mut rx), 2).await?);
    take(&mut stream, &mut ahead, Some(&mut rx), pad_len).await?;
    rx.apply(&mut ahead);
    Ok(Encrypted::new(stream, rx, tx, ahead))
}

/// The responder's side, for an inbound connection whose first bytes (`head`, already read
/// to tell it apart from a plaintext handshake) weren't a BitTorrent handshake. `served` is
/// every torrent this client could be dialled for; the one the peer means is returned.
pub(crate) async fn respond<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    head: &[u8],
    served: &[InfoHash],
) -> io::Result<(Encrypted<S>, InfoHash)> {
    let mut their_public = [0u8; KEY_LEN];
    their_public[..head.len()].copy_from_slice(head);
    stream.read_exact(&mut their_public[head.len()..]).await?;
    let keys = keypair();
    let secret = shared_secret(&keys, &their_public);
    let mut hello = keys.public.to_vec();
    hello.extend(random_pad());
    stream.write_all(&hello).await?;

    let mut ahead = skip_to(&mut stream, &sha1(&[b"req1", &secret]), MAX_PAD).await?;
    let wanted = take(&mut stream, &mut ahead, None, 20).await?;
    let req3 = sha1(&[b"req3", &secret]);
    let info_hash = *served
        .iter()
        .find(|hash| xor(sha1(&[b"req2", hash.as_bytes()]), req3)[..] == wanted[..])
        .ok_or_else(|| err("no torrent we serve matches the encrypted handshake"))?;
    let skey = info_hash.as_bytes();
    let mut rx = Rc4::discarding(&sha1(&[b"keyA", &secret, skey]));
    let mut tx = Rc4::discarding(&sha1(&[b"keyB", &secret, skey]));

    let fields = take(&mut stream, &mut ahead, Some(&mut rx), VC.len() + 4 + 2).await?;
    if fields[..VC.len()] != VC {
        return Err(err("verification constant didn't decrypt to zeros"));
    }
    let provide = u32::from_be_bytes(fields[VC.len()..VC.len() + 4].try_into().unwrap());
    if provide & CRYPTO_RC4 == 0 {
        return Err(err("peer doesn't offer RC4"));
    }
    let pad_len = u16_of(&fields[VC.len() + 4..]);
    take(&mut stream, &mut ahead, Some(&mut rx), pad_len).await?;
    let payload_len = u16_of(&take(&mut stream, &mut ahead, Some(&mut rx), 2).await?);
    let mut leftover = take(&mut stream, &mut ahead, Some(&mut rx), payload_len).await?;
    rx.apply(&mut ahead);
    leftover.extend(ahead);

    let mut reply = Vec::new();
    reply.extend(VC);
    reply.extend(CRYPTO_RC4.to_be_bytes());
    reply.extend(0u16.to_be_bytes()); // no PadD
    tx.apply(&mut reply);
    stream.write_all(&reply).await?;
    Ok((Encrypted::new(stream, rx, tx, leftover), info_hash))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn rc4_matches_the_known_answer() {
        let mut rc4 = Rc4::new(b"Key");
        let mut data = *b"Plaintext";
        rc4.apply(&mut data);
        assert_eq!(data, [0xBB, 0xF3, 0x16, 0xE8, 0xD9, 0x40, 0xAF, 0x0A, 0xD3]);
    }

    #[test]
    fn both_sides_derive_the_same_secret() {
        let a = keypair();
        let b = keypair();
        assert_eq!(shared_secret(&a, &b.public), shared_secret(&b, &a.public));
    }

    /// The whole exchange over an in-memory pipe, with a responder serving several torrents
    /// and data flowing both ways afterwards, including some sent before the initiator has
    /// even read the responder's last field.
    #[tokio::test]
    async fn a_handshake_agrees_on_keys_and_the_torrent() {
        let served: Vec<InfoHash> = (0u8..3).map(|i| InfoHash::from_bytes(&[i; 20])).collect();
        let wanted = served[1];
        let (a, b) = tokio::io::duplex(4096);

        let initiator = tokio::spawn(async move {
            let mut stream = initiate(a, &wanted).await.unwrap();
            stream.write_all(b"hello from A").await.unwrap();
            let mut reply = [0u8; 12];
            stream.read_exact(&mut reply).await.unwrap();
            reply
        });
        let (mut stream, hash) = respond(b, &[], &served).await.unwrap();
        assert_eq!(hash, wanted);
        stream.write_all(b"hello from B").await.unwrap();
        let mut got = [0u8; 12];
        stream.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"hello from A");
        assert_eq!(&initiator.await.unwrap(), b"hello from B");
    }

    #[tokio::test]
    async fn a_torrent_we_do_not_serve_is_refused() {
        let (a, b) = tokio::io::duplex(4096);
        let other = InfoHash::from_bytes(&[9; 20]);
        let initiator = tokio::spawn(async move { initiate(a, &other).await.map(|_| ()) });
        let served = [InfoHash::from_bytes(&[1; 20])];
        assert!(respond(b, &[], &served).await.is_err());
        drop(initiator);
    }

    /// The first bytes of an inbound connection get read before anyone knows it's MSE;
    /// `respond` has to treat them as the start of the public key.
    #[tokio::test]
    async fn a_pre_read_head_is_part_of_the_key() {
        let served = [InfoHash::from_bytes(&[7; 20])];
        let (a, mut b) = tokio::io::duplex(4096);
        let initiator = tokio::spawn(async move { initiate(a, &served[0]).await.map(|_| ()) });
        let mut head = [0u8; 20];
        b.read_exact(&mut head).await.unwrap();
        respond(b, &head, &served).await.unwrap();
        initiator.await.unwrap().unwrap();
    }
}
