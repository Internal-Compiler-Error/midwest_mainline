//! The byte stream under a peer connection, whichever transport carries it.
//!
//! Everything above the handshake (`Peer`, the codec, the metadata fetch) reads and writes a
//! `PeerStream` and never asks what's inside. Each transport (TCP, uTP, MSE over either) is
//! one variant, so adding one touches only the connect/accept paths.

use crate::config::Encryption;
use crate::defs::Identity;
use crate::mse;
use crate::wire::{HANDSHAKE_STR, Handshake, V2Support, read_handshake, read_handshake_body, shake_hands};
use librqbit_utp::{UtpSocketUdp, UtpStream};
use midwest_mainline::types::InfoHash;
use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

pub(crate) enum PeerStream {
    Tcp(TcpStream),
    Utp(UtpStream),
    Encrypted(Box<mse::Encrypted<PeerStream>>),
}

impl PeerStream {
    pub fn is_encrypted(&self) -> bool {
        matches!(self, PeerStream::Encrypted(_))
    }

    pub fn is_utp(&self) -> bool {
        match self {
            PeerStream::Tcp(_) => false,
            PeerStream::Utp(_) => true,
            PeerStream::Encrypted(enc) => enc.get_ref().is_utp(),
        }
    }
}

/// What's remembered about a peer from earlier (see `torrent_swarm::KnownPeer`), to skip
/// attempts that are known to fail.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DialHints {
    /// PEX flagged it uTP-capable: try uTP before TCP
    pub prefer_utp: bool,
    /// it refused the encrypted opening last time: with `Prefer`, go straight to plaintext
    pub plaintext: bool,
    /// BEP 55: a holepunch, where both ends send uTP at once to open their NATs; TCP can't
    pub utp_only: bool,
}

/// How a second connection to the same peer is made, for the plaintext retry of `Prefer`.
#[derive(Clone)]
enum Transport {
    Tcp,
    Utp(Arc<UtpSocketUdp>),
}

impl Transport {
    fn name(&self) -> &'static str {
        match self {
            Transport::Tcp => "tcp",
            Transport::Utp(_) => "utp",
        }
    }

    async fn dial(&self, addr: SocketAddr) -> io::Result<PeerStream> {
        match self {
            Transport::Tcp => {
                let stream = tokio::time::timeout(crate::settings::CONNECT_TIMEOUT, TcpStream::connect(addr))
                    .await
                    .map_err(|_| io::Error::new(ErrorKind::TimedOut, "connect timed out"))??;
                Ok(PeerStream::Tcp(stream))
            }
            Transport::Utp(utp) => {
                // librqbit-utp parents the connection's long-lived span on whatever span is
                // current when it connects, which would keep our dial's span open for as long as
                // the connection lasts; a root span of its own (always enabled, kept out of the
                // traces by its target) cuts that link
                let root = tracing::error_span!(target: "librqbit_utp", parent: None, "utp_connect", %addr);
                let connect = tracing::Instrument::instrument(utp.connect(addr), root);
                let stream = tokio::time::timeout(crate::settings::CONNECT_TIMEOUT, connect)
                    .await
                    .map_err(|_| io::Error::new(ErrorKind::TimedOut, "uTP connect timed out"))?
                    .map_err(io::Error::other)?;
                Ok(PeerStream::Utp(stream))
            }
        }
    }
}

/// Opens a connection to `addr` and completes the BitTorrent handshake for `info_hash`.
/// With a uTP socket, TCP and uTP race happy-eyeballs style (see `race`): TCP gets a head
/// start, or uTP for a peer PEX flagged uTP-capable, and the first to connect is used. A peer
/// one transport reaches but that rejects the handshake isn't retried over the other: it's
/// reachable and just didn't want us.
/// Encryption follows `our_id.encryption`; with `Prefer`, a peer that doesn't take the
/// encrypted opening is dialled again in plaintext over the same transport, since the first
/// connection is spent once what we sent on it wasn't a handshake, and a peer remembered as
/// having refused it before is dialled in plaintext from the start. `v2` is what the handshake
/// says about BEP 52; a hybrid's peer may answer with its v2 hash instead of `info_hash`.
pub(crate) async fn connect(
    addr: SocketAddr,
    info_hash: &InfoHash,
    v2: V2Support,
    our_id: &Identity,
    utp: Option<&Arc<UtpSocketUdp>>,
    hints: DialHints,
) -> io::Result<(PeerStream, Handshake)> {
    let (first, transport) = if hints.utp_only {
        let utp = utp.ok_or_else(|| io::Error::other("holepunch without a uTP socket"))?;
        let transport = Transport::Utp(utp.clone());
        (transport.dial(addr).await?, transport)
    } else {
        open_transport(addr, utp, hints.prefer_utp).await?
    };
    let policy = match our_id.encryption {
        Encryption::Prefer if hints.plaintext => Encryption::Disabled,
        policy => policy,
    };

    let encrypt =
        |stream| async { io::Result::Ok(PeerStream::Encrypted(Box::new(mse::initiate(stream, info_hash).await?))) };
    let mut stream = match policy {
        Encryption::Disabled => first,
        Encryption::Require => encrypt(first).await?,
        // only the MSE exchange itself failing means "try plaintext": a peer that completed
        // it and then rejected the handshake has answered, and would reject again
        Encryption::Prefer => match encrypt(first).await {
            Ok(stream) => stream,
            Err(e) => {
                tracing::debug!("{addr} didn't take an encrypted opening ({e}); retrying in plaintext");
                transport.dial(addr).await?
            }
        },
    };
    let handshake = shake_hands(&mut stream, info_hash, our_id, v2).await?;
    Ok((stream, handshake))
}

/// A connection over whichever of TCP and uTP answers first, the one asked for given a head
/// start.
async fn open_transport(
    addr: SocketAddr,
    utp: Option<&Arc<UtpSocketUdp>>,
    utp_first: bool,
) -> io::Result<(PeerStream, Transport)> {
    let Some(utp) = utp else {
        let stream = Transport::Tcp
            .dial(addr)
            .await
            .map_err(|e| io::Error::other(format!("tcp: {e}")))?;
        return Ok((stream, Transport::Tcp));
    };
    let utp = Transport::Utp(utp.clone());
    let (first, second) = if utp_first {
        (&utp, &Transport::Tcp)
    } else {
        (&Transport::Tcp, &utp)
    };
    race(addr, first, second, crate::settings::HAPPY_EYEBALLS_DELAY).await
}

/// Happy eyeballs (RFC 8305) over two transports: `first` is dialled at once and `second`
/// after `head_start`, or as soon as `first` fails. The first connection up wins and the other
/// dial is dropped mid-flight; the race is to connect rather than to handshake, so a peer both
/// reach never sees two of our handshakes and drops one as a duplicate.
async fn race(
    addr: SocketAddr,
    first: &Transport,
    second: &Transport,
    head_start: Duration,
) -> io::Result<(PeerStream, Transport)> {
    let failed = |transport: &Transport, e: io::Error| format!("{}: {e}", transport.name());
    let mut first_dial = pin!(first.dial(addr));
    let early_failure = tokio::select! {
        result = &mut first_dial => match result {
            Ok(stream) => return Ok((stream, first.clone())),
            Err(e) => Some(failed(first, e)),
        },
        () = tokio::time::sleep(head_start) => None,
    };
    let mut second_dial = pin!(second.dial(addr));
    let (failure, last, last_dial) = match early_failure {
        Some(failure) => (failure, second, second_dial),
        None => tokio::select! {
            result = &mut first_dial => match result {
                Ok(stream) => return Ok((stream, first.clone())),
                Err(e) => (failed(first, e), second, second_dial),
            },
            result = &mut second_dial => match result {
                Ok(stream) => return Ok((stream, second.clone())),
                Err(e) => (failed(second, e), first, first_dial),
            },
        },
    };
    match last_dial.await {
        Ok(stream) => Ok((stream, last.clone())),
        Err(e) => Err(io::Error::other(format!("{failure}; {}", failed(last, e)))),
    }
}

/// Reads an inbound connection's opening, which is either a plaintext BitTorrent handshake
/// or an MSE exchange (told apart by the first bytes) followed by the handshake inside it.
/// `served` lists the torrents an encrypted peer may be after, looked up only when needed.
pub(crate) async fn accept(
    mut stream: PeerStream,
    policy: Encryption,
    served: impl FnOnce() -> Vec<InfoHash>,
) -> io::Result<(PeerStream, Handshake)> {
    let mut head = [0u8; HANDSHAKE_STR.len()];
    stream.read_exact(&mut head).await?;
    if head == *HANDSHAKE_STR {
        if policy == Encryption::Require {
            return Err(io::Error::other("plaintext handshake refused: encryption is required"));
        }
        let handshake = read_handshake_body(&mut stream).await?;
        return Ok((stream, handshake));
    }
    if policy == Encryption::Disabled {
        return Err(io::Error::other("not a BitTorrent handshake, and encryption is off"));
    }
    let (encrypted, _) = mse::respond(stream, &head, &served()).await?;
    let mut stream = PeerStream::Encrypted(Box::new(encrypted));
    let handshake = read_handshake(&mut stream).await?;
    Ok((stream, handshake))
}

impl AsyncRead for PeerStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(tcp) => Pin::new(tcp).poll_read(cx, buf),
            PeerStream::Utp(utp) => Pin::new(utp).poll_read(cx, buf),
            PeerStream::Encrypted(enc) => Pin::new(enc.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PeerStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            PeerStream::Tcp(tcp) => Pin::new(tcp).poll_write(cx, buf),
            PeerStream::Utp(utp) => Pin::new(utp).poll_write(cx, buf),
            PeerStream::Encrypted(enc) => Pin::new(enc.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(tcp) => Pin::new(tcp).poll_flush(cx),
            // librqbit-utp's flush waits until the remote has ACKed everything written, a
            // round trip per message. Its dispatcher sends what's been written without one, the
            // way the kernel does for TCP, whose flush is likewise a no-op. `Framed::send` flushes
            // after every message, and the swarm loop awaits each send inline, so the real flush
            // would let one uTP peer's round trip hold up every peer of the torrent.
            PeerStream::Utp(_) => Poll::Ready(Ok(())),
            PeerStream::Encrypted(enc) => Pin::new(enc.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(tcp) => Pin::new(tcp).poll_shutdown(cx),
            PeerStream::Utp(utp) => Pin::new(utp).poll_shutdown(cx),
            PeerStream::Encrypted(enc) => Pin::new(enc.as_mut()).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::wire::send_handshake;
    use std::net::Ipv4Addr;
    use tokio::net::TcpListener;

    fn identity(peer_id: u8, encryption: Encryption) -> Identity {
        Identity {
            peer_id: [peer_id; 20],
            serving: (Ipv4Addr::LOCALHOST, 0).into(),
            dht: false,
            encryption,
        }
    }

    /// A listener that finishes `accepts` openings under `policy`, replying to each handshake
    /// it gets, and reports whether each connection ended up encrypted.
    async fn listener(
        policy: Encryption,
        hash: InfoHash,
        accepts: usize,
    ) -> (SocketAddr, tokio::task::JoinHandle<Vec<bool>>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut outcomes = vec![];
            while outcomes.len() < accepts {
                let (tcp, _) = listener.accept().await.unwrap();
                match accept(PeerStream::Tcp(tcp), policy, || vec![hash]).await {
                    Ok((mut stream, handshake)) => {
                        assert_eq!(handshake.info_hash, hash);
                        send_handshake(&mut stream, &hash, &identity(2, policy), V2Support::None)
                            .await
                            .unwrap();
                        outcomes.push(stream.is_encrypted());
                    }
                    Err(_) => outcomes.push(false),
                }
            }
            outcomes
        });
        (addr, task)
    }

    #[tokio::test]
    async fn an_encrypted_dial_meets_an_encrypted_accept() {
        let hash = InfoHash::from_bytes(&[5; 20]);
        let (addr, task) = listener(Encryption::Prefer, hash, 1).await;
        let (stream, handshake) = connect(
            addr,
            &hash,
            V2Support::None,
            &identity(1, Encryption::Prefer),
            None,
            DialHints::default(),
        )
        .await
        .unwrap();
        assert!(stream.is_encrypted());
        assert_eq!(handshake.peer_id, [2; 20]);
        assert_eq!(task.await.unwrap(), [true]);
    }

    /// A peer with encryption off drops our encrypted opening; `Prefer` comes back in
    /// plaintext on a second connection and that one succeeds.
    #[tokio::test]
    async fn prefer_falls_back_to_plaintext() {
        let hash = InfoHash::from_bytes(&[6; 20]);
        let (addr, task) = listener(Encryption::Disabled, hash, 2).await;
        let (stream, _) = connect(
            addr,
            &hash,
            V2Support::None,
            &identity(1, Encryption::Prefer),
            None,
            DialHints::default(),
        )
        .await
        .unwrap();
        assert!(!stream.is_encrypted());
        assert_eq!(
            task.await.unwrap(),
            [false, false],
            "one refused opening, then a plaintext one"
        );
    }

    #[tokio::test]
    async fn require_refuses_plaintext_both_ways() {
        let hash = InfoHash::from_bytes(&[7; 20]);
        let (addr, task) = listener(Encryption::Require, hash, 1).await;
        assert!(
            connect(
                addr,
                &hash,
                V2Support::None,
                &identity(1, Encryption::Disabled),
                None,
                DialHints::default()
            )
            .await
            .is_err()
        );
        assert_eq!(task.await.unwrap(), [false]);

        let (addr, task) = listener(Encryption::Disabled, hash, 1).await;
        assert!(
            connect(
                addr,
                &hash,
                V2Support::None,
                &identity(1, Encryption::Require),
                None,
                DialHints::default()
            )
            .await
            .is_err()
        );
        assert_eq!(task.await.unwrap(), [false]);
    }

    /// A peer that completes the MSE exchange and then names another torrent in its handshake
    /// has answered; dialling it again in plaintext would only get the same answer.
    #[tokio::test]
    async fn a_handshake_refused_after_encryption_is_not_retried() {
        let hash = InfoHash::from_bytes(&[9; 20]);
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let (mut stream, _) = accept(PeerStream::Tcp(tcp), Encryption::Prefer, || vec![hash])
                .await
                .unwrap();
            let other = InfoHash::from_bytes(&[10; 20]);
            send_handshake(&mut stream, &other, &identity(2, Encryption::Prefer), V2Support::None)
                .await
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_millis(300), listener.accept())
                .await
                .is_err()
        });
        assert!(
            connect(
                addr,
                &hash,
                V2Support::None,
                &identity(1, Encryption::Prefer),
                None,
                DialHints::default()
            )
            .await
            .is_err()
        );
        assert!(task.await.unwrap(), "no second connection");
    }

    /// A port with a uTP socket and no TCP listener refuses TCP at once; the dial goes over
    /// uTP, with MSE on top when the policy wants it.
    #[tokio::test]
    async fn utp_is_tried_when_tcp_is_refused() {
        for policy in [Encryption::Disabled, Encryption::Prefer] {
            let hash = InfoHash::from_bytes(&[8; 20]);
            let server = UtpSocketUdp::new_udp((Ipv4Addr::LOCALHOST, 0).into()).await.unwrap();
            let client = UtpSocketUdp::new_udp((Ipv4Addr::LOCALHOST, 0).into()).await.unwrap();
            let addr = server.bind_addr();
            let acceptor = tokio::spawn(async move {
                let stream = server.accept().await.unwrap();
                let (mut stream, handshake) = accept(PeerStream::Utp(stream), policy, || vec![hash]).await.unwrap();
                assert_eq!(handshake.info_hash, hash);
                send_handshake(&mut stream, &hash, &identity(2, policy), V2Support::None)
                    .await
                    .unwrap();
                (stream.is_utp(), stream.is_encrypted())
            });
            let (stream, _) = connect(
                addr,
                &hash,
                V2Support::None,
                &identity(1, policy),
                Some(&client),
                DialHints::default(),
            )
            .await
            .unwrap();
            let expected = (true, policy == Encryption::Prefer);
            assert_eq!((stream.is_utp(), stream.is_encrypted()), expected);
            assert_eq!(acceptor.await.unwrap(), expected);
        }
    }

    /// A burst of small messages over uTP goes out at once. `Framed::send` flushes after each
    /// one, and were flush to wait for the remote's ACK (as librqbit-utp's own does), every
    /// message would cost a round trip plus the receiver's delayed-ACK timer: seconds for
    /// this burst instead of milliseconds.
    #[tokio::test]
    async fn small_messages_over_utp_do_not_wait_for_acks() {
        use crate::wire::{BlockRef, BtCodec, BtMessage};
        use futures::{SinkExt, StreamExt};
        use tokio_util::codec::Framed;

        const BURST: u32 = 200;
        let server = UtpSocketUdp::new_udp((Ipv4Addr::LOCALHOST, 0).into()).await.unwrap();
        let client = UtpSocketUdp::new_udp((Ipv4Addr::LOCALHOST, 0).into()).await.unwrap();
        let addr = server.bind_addr();
        let receiver = tokio::spawn(async move {
            let mut framed = Framed::new(PeerStream::Utp(server.accept().await.unwrap()), BtCodec);
            let mut got = 0;
            while got < BURST {
                match framed.next().await.unwrap().unwrap() {
                    BtMessage::Request(req) => {
                        assert_eq!(req.begin, got * 16384);
                        got += 1;
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
        });
        let mut framed = Framed::new(PeerStream::Utp(client.connect(addr).await.unwrap()), BtCodec);
        let started = std::time::Instant::now();
        for i in 0..BURST {
            framed
                .send(BtMessage::Request(BlockRef {
                    index: 0,
                    begin: i * 16384,
                    length: 16384,
                }))
                .await
                .unwrap();
        }
        let sent_in = started.elapsed();
        tokio::time::timeout(std::time::Duration::from_secs(5), receiver)
            .await
            .expect("the burst arrives")
            .unwrap();
        assert!(
            sent_in < std::time::Duration::from_secs(1),
            "{BURST} requests took {sent_in:?} to send"
        );
    }

    /// With TCP and uTP both listening on the port, the hints decide: uTP first, and no
    /// encrypted opening for a peer remembered as refusing one, even under `Prefer`.
    #[tokio::test]
    async fn hints_pick_utp_first_and_skip_the_encrypted_opening() {
        let hash = InfoHash::from_bytes(&[11; 20]);
        let server = UtpSocketUdp::new_udp((Ipv4Addr::LOCALHOST, 0).into()).await.unwrap();
        let addr = server.bind_addr();
        let tcp = TcpListener::bind(addr).await.unwrap();
        let client = UtpSocketUdp::new_udp((Ipv4Addr::LOCALHOST, 0).into()).await.unwrap();
        let acceptor = tokio::spawn(async move {
            let stream = server.accept().await.unwrap();
            let (mut stream, _) = accept(PeerStream::Utp(stream), Encryption::Prefer, || vec![hash])
                .await
                .unwrap();
            send_handshake(&mut stream, &hash, &identity(2, Encryption::Prefer), V2Support::None)
                .await
                .unwrap();
            stream.is_encrypted()
        });
        let hints = DialHints {
            prefer_utp: true,
            plaintext: true,
            ..Default::default()
        };
        let (stream, _) = connect(
            addr,
            &hash,
            V2Support::None,
            &identity(1, Encryption::Prefer),
            Some(&client),
            hints,
        )
        .await
        .unwrap();
        assert!(stream.is_utp() && !stream.is_encrypted());
        assert!(!acceptor.await.unwrap());
        drop(tcp);
    }

    /// A peer that answers TCP within the head start is reached over TCP alone: uTP is never
    /// dialled, so the peer doesn't see a second connection from us.
    #[tokio::test]
    async fn tcp_that_connects_within_the_head_start_wins_alone() {
        let hash = InfoHash::from_bytes(&[12; 20]);
        let (addr, task) = listener(Encryption::Disabled, hash, 1).await;
        let server = UtpSocketUdp::new_udp(addr).await.unwrap();
        let client = UtpSocketUdp::new_udp((Ipv4Addr::LOCALHOST, 0).into()).await.unwrap();
        let (stream, _) = connect(
            addr,
            &hash,
            V2Support::None,
            &identity(1, Encryption::Disabled),
            Some(&client),
            DialHints::default(),
        )
        .await
        .unwrap();
        assert!(!stream.is_utp());
        assert_eq!(task.await.unwrap(), [false]);
        let utp_accept = tokio::time::timeout(Duration::from_millis(500), server.accept()).await;
        assert!(utp_accept.is_err(), "no uTP connection");
    }

    /// A peer PEX flagged uTP-capable that doesn't answer uTP gets TCP after the head start,
    /// not after uTP's whole connect timeout.
    #[tokio::test]
    async fn tcp_takes_over_when_preferred_utp_does_not_answer() {
        let hash = InfoHash::from_bytes(&[13; 20]);
        let (addr, task) = listener(Encryption::Disabled, hash, 1).await;
        let client = UtpSocketUdp::new_udp((Ipv4Addr::LOCALHOST, 0).into()).await.unwrap();
        let hints = DialHints {
            prefer_utp: true,
            plaintext: false,
            ..Default::default()
        };
        let started = tokio::time::Instant::now();
        let (stream, _) = connect(
            addr,
            &hash,
            V2Support::None,
            &identity(1, Encryption::Disabled),
            Some(&client),
            hints,
        )
        .await
        .unwrap();
        let took = started.elapsed();
        assert!(!stream.is_utp());
        assert!(
            took >= crate::settings::HAPPY_EYEBALLS_DELAY && took < crate::settings::CONNECT_TIMEOUT / 2,
            "connected in {took:?}"
        );
        assert_eq!(task.await.unwrap(), [false]);
    }

    /// The losing dial is dropped mid-flight, and with both down the error names both.
    #[tokio::test]
    async fn a_failed_race_reports_both_transports() {
        let free = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = free.local_addr().unwrap();
        drop(free);
        let client = UtpSocketUdp::new_udp((Ipv4Addr::LOCALHOST, 0).into()).await.unwrap();
        let utp = Transport::Utp(client);
        let err = race(addr, &Transport::Tcp, &utp, Duration::from_secs(60))
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(err.starts_with("tcp: ") && err.contains("; utp: "), "{err}");
    }
}
