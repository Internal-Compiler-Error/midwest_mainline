//! The byte stream under a peer connection, whichever transport carries it.
//!
//! Everything above the handshake (`Peer`, the codec, the metadata fetch) reads and writes a
//! `PeerStream` and never asks what's inside. Each transport (plain TCP today, MSE-obfuscated
//! and uTP later) is one variant, so adding one touches only the connect/accept paths.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

pub(crate) enum PeerStream {
    Tcp(TcpStream),
}

impl AsyncRead for PeerStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(tcp) => Pin::new(tcp).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PeerStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            PeerStream::Tcp(tcp) => Pin::new(tcp).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(tcp) => Pin::new(tcp).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(tcp) => Pin::new(tcp).poll_shutdown(cx),
        }
    }
}
