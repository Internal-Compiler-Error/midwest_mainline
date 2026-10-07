//! uTP (BEP 29) through `librqbit-utp`: the same congestion-yielding transport the mainstream
//! clients speak, on the UDP port with the same number as the TCP listener, which is the only
//! port peers ever learn for us. The DHT node lives on the next port up because of it.
//!
//! Binding is asynchronous and can fail, so consumers get a watch like `DhtWatch`: `None`
//! until the socket is up, and a dropped sender for "no uTP, ever". Dials that run before
//! it's ready just don't try uTP.

use crate::settings::{PEER_TIMEOUT, UTP_MAX_CONNECTIONS, UTP_UDP_RECV_BUFFER, UTP_WINDOW};
use dontfrag::UdpSocketExt;
use librqbit_utp::{SocketOpts, UtpSocketUdp};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

pub type UtpWatch = watch::Receiver<Option<Arc<UtpSocketUdp>>>;

/// Binds the uTP socket on the current runtime; its dispatcher stops with `shutdown`.
pub(crate) fn start(port: u16, shutdown: CancellationToken) -> UtpWatch {
    let (tx, rx) = watch::channel(None);
    tokio::spawn(async move {
        match bind(port, shutdown).await {
            Ok(socket) => {
                info!("uTP on UDP port {}", socket.bind_addr().port());
                let _ = tx.send(Some(socket));
            }
            Err(e) => warn!("no uTP: {e:#}"),
        }
    });
    rx
}

/// A watch that never delivers a socket, for a client with uTP off.
pub(crate) fn none() -> UtpWatch {
    watch::channel(None).1
}

async fn bind(port: u16, shutdown: CancellationToken) -> anyhow::Result<Arc<UtpSocketUdp>> {
    // dual-stack when the platform allows it, else v4 only; like the TCP listener
    let attempts = [
        SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)),
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)),
    ];
    for addr in attempts {
        match bind_udp(addr) {
            Ok(udp) => return Ok(UtpSocketUdp::new_with_opts(udp, Default::default(), opts(shutdown))?),
            Err(e) => debug!("couldn't bind uTP on {addr}: {e:#}"),
        }
    }
    warn!("UDP port {port} is taken; uTP is on some free port instead, which inbound peers can't know about");
    let udp = bind_udp((Ipv4Addr::UNSPECIFIED, 0).into())?;
    Ok(UtpSocketUdp::new_with_opts(udp, Default::default(), opts(shutdown))?)
}

fn opts(shutdown: CancellationToken) -> SocketOpts {
    let window = NonZeroUsize::new(UTP_WINDOW);
    SocketOpts {
        // our writers already hand over whole messages and never wait on a flush (see
        // `PeerStream::poll_flush`); Nagle would hold a burst of Requests back a round trip
        disable_nagle: true,
        max_live_vsocks: NonZeroUsize::new(UTP_MAX_CONNECTIONS),
        vsock_rx_bufsize_bytes: window,
        vsock_tx_bufsize_bytes_max: window,
        // the crate's 10 s would end a connection that merely went quiet (a peer choking us,
        // say) long before a TCP one; a dead remote with data outstanding to it is still
        // caught by the retransmission limit
        remote_inactivity_timeout: Some(PEER_TIMEOUT),
        cancellation_token: shutdown,
        ..Default::default()
    }
}

/// Binds the UDP socket the way `UtpSocketUdp::new_udp_with_opts` does, except for the
/// receive buffer: that asks for room for every connection's whole window (gigabytes with our
/// options), which macOS refuses outright and leaves the small default in place.
fn bind_udp(addr: SocketAddr) -> anyhow::Result<librqbit_dualstack_sockets::UdpSocket> {
    let udp = librqbit_dualstack_sockets::UdpSocket::bind_udp(addr, Default::default())?;
    // path MTU probing needs oversized probes dropped, not fragmented
    let dontfrag = if addr.is_ipv4() {
        udp.socket().set_dontfrag_v4(true)
    } else {
        udp.socket().set_dontfrag_v6(true)
    };
    if let Err(e) = dontfrag {
        debug!("couldn't set don't-fragment on the uTP socket: {e:#}");
    }
    match grow_recv_buffer(udp.socket(), UTP_UDP_RECV_BUFFER) {
        Ok(size) => debug!("uTP UDP receive buffer is {} KiB", size / 1024),
        Err(e) => warn!("couldn't size the uTP UDP receive buffer: {e:#}"),
    }
    Ok(udp)
}

/// Sets the socket's receive buffer to `wanted`, or the largest size short of it the OS
/// accepts, and returns the size in effect.
fn grow_recv_buffer(udp: &tokio::net::UdpSocket, wanted: usize) -> io::Result<usize> {
    let sock = socket2::SockRef::from(udp);
    let mut accepted = sock.recv_buffer_size()?;
    if accepted >= wanted || sock.set_recv_buffer_size(wanted).is_ok() {
        return sock.recv_buffer_size();
    }
    // a refused size leaves the last accepted one in place
    let mut refused = wanted;
    while refused - accepted > 64 * 1024 {
        let mid = accepted + (refused - accepted) / 2;
        if sock.set_recv_buffer_size(mid).is_ok() {
            accepted = mid;
        } else {
            refused = mid;
        }
    }
    sock.recv_buffer_size()
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn the_receive_buffer_grows_as_far_as_the_os_allows() {
        let udp = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let before = socket2::SockRef::from(&udp).recv_buffer_size().unwrap();
        let after = grow_recv_buffer(&udp, UTP_UDP_RECV_BUFFER).unwrap();
        assert!(after >= before, "{before} -> {after}");
        // kern.ipc.maxsockbuf is 8 MiB by default, against a default buffer under 1 MiB
        #[cfg(target_os = "macos")]
        assert!(after > 4 * 1024 * 1024, "{before} -> {after}");
    }
}
