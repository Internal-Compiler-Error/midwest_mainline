//! uTP (BEP 29) through `librqbit-utp`: the same congestion-yielding transport the mainstream
//! clients speak, on the UDP port with the same number as the TCP listener, which is the only
//! port peers ever learn for us. The DHT node lives on the next port up because of it.
//!
//! Binding is asynchronous and can fail, so consumers get a watch like `DhtWatch`: `None`
//! until the socket is up, and a dropped sender for "no uTP, ever". Dials that run before
//! it's ready just don't try uTP.

use librqbit_utp::{SocketOpts, UtpSocketUdp};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
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

async fn bind(port: u16, shutdown: CancellationToken) -> librqbit_utp::Result<Arc<UtpSocketUdp>> {
    let opts = || SocketOpts {
        cancellation_token: shutdown.clone(),
        ..Default::default()
    };
    // dual-stack when the platform allows it, else v4 only; like the TCP listener
    let attempts = [
        SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)),
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)),
    ];
    for addr in attempts {
        match UtpSocketUdp::new_udp_with_opts(addr, opts(), Default::default()).await {
            Ok(socket) => return Ok(socket),
            Err(e) => debug!("couldn't bind uTP on {addr}: {e:#}"),
        }
    }
    warn!("UDP port {port} is taken; uTP is on some free port instead, which inbound peers can't know about");
    UtpSocketUdp::new_udp_with_opts((Ipv4Addr::UNSPECIFIED, 0).into(), opts(), Default::default()).await
}
