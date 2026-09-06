//! The byte stream under a peer connection, whichever transport carries it.
//!
//! Everything above the handshake (`Peer`, the codec, the metadata fetch) reads and writes a
//! `PeerStream` and never asks what's inside. Each transport (plain TCP, MSE over another
//! stream, uTP later) is one variant, so adding one touches only the connect/accept paths.

use crate::config::Encryption;
use crate::defs::Identity;
use crate::mse;
use crate::wire::{HANDSHAKE_STR, Handshake, read_handshake, read_handshake_body, shake_hands};
use midwest_mainline::types::InfoHash;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

pub(crate) enum PeerStream {
    Tcp(TcpStream),
    Encrypted(Box<mse::Encrypted<PeerStream>>),
}

impl PeerStream {
    pub fn is_encrypted(&self) -> bool {
        matches!(self, PeerStream::Encrypted(_))
    }
}

/// Opens a connection to `addr` and completes the BitTorrent handshake for `info_hash`,
/// encrypted or not as `our_id.encryption` says. With `Prefer`, a peer that doesn't take the
/// encrypted opening is dialled again in plaintext: the first connection is spent, since
/// what we sent on it wasn't a handshake.
pub(crate) async fn connect(
    addr: SocketAddr,
    info_hash: &InfoHash,
    our_id: &Identity,
) -> io::Result<(PeerStream, Handshake)> {
    let encrypted = async {
        let tcp = PeerStream::Tcp(crate::wire::connect(addr).await?);
        let mut stream = PeerStream::Encrypted(Box::new(mse::initiate(tcp, info_hash).await?));
        let handshake = shake_hands(&mut stream, info_hash, our_id).await?;
        Ok::<_, io::Error>((stream, handshake))
    };
    let plain = async {
        let mut stream = PeerStream::Tcp(crate::wire::connect(addr).await?);
        let handshake = shake_hands(&mut stream, info_hash, our_id).await?;
        Ok((stream, handshake))
    };
    match our_id.encryption {
        Encryption::Disabled => plain.await,
        Encryption::Require => encrypted.await,
        Encryption::Prefer => match encrypted.await {
            Ok(connected) => Ok(connected),
            Err(e) => {
                tracing::debug!("{addr} didn't take an encrypted opening ({e}); retrying in plaintext");
                plain.await
            }
        },
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
            PeerStream::Encrypted(enc) => Pin::new(enc.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PeerStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            PeerStream::Tcp(tcp) => Pin::new(tcp).poll_write(cx, buf),
            PeerStream::Encrypted(enc) => Pin::new(enc.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(tcp) => Pin::new(tcp).poll_flush(cx),
            PeerStream::Encrypted(enc) => Pin::new(enc.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(tcp) => Pin::new(tcp).poll_shutdown(cx),
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
                        send_handshake(&mut stream, &hash, &identity(2, policy)).await.unwrap();
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
        let (stream, handshake) = connect(addr, &hash, &identity(1, Encryption::Prefer)).await.unwrap();
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
        let (stream, _) = connect(addr, &hash, &identity(1, Encryption::Prefer)).await.unwrap();
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
        assert!(connect(addr, &hash, &identity(1, Encryption::Disabled)).await.is_err());
        assert_eq!(task.await.unwrap(), [false]);

        let (addr, task) = listener(Encryption::Disabled, hash, 1).await;
        assert!(connect(addr, &hash, &identity(1, Encryption::Require)).await.is_err());
        assert_eq!(task.await.unwrap(), [false]);
    }
}
