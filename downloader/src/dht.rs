//! The client's DHT node (BEP 5), from the sibling `midwest_mainline` crate. One per
//! client, shared by every torrent: swarms and the metadata fetcher ask it for peers and
//! announce themselves through `announcer::dht_announcer`.
//!
//! Starting it takes a while (an external-IP lookup for the BEP 42 node id, then
//! bootstrapping), so it happens in the background and consumers get a `watch` that turns
//! from `None` to a handle once the node is up. A client with no DHT at all just gets a
//! watch that stays `None` (`Dht::none`).

use midwest_mainline::dht::DhtSession;
use midwest_mainline::dht::client::DhtClient;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{UdpSocket, lookup_host};
use tokio::sync::watch;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{info, warn};

/// Well-known routers that answer `ping` and `find_node` for anyone joining the network.
const BOOTSTRAP_NODES: [&str; 5] = [
    "router.bittorrent.com:6881",
    "router.utorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.libtorrent.org:25401",
    "dht.aelitis.com:6881",
];

/// How long to wait to learn our external IP before giving up on a BEP 42 id
const EXTERNAL_IP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct DhtHandle {
    pub client: DhtClient,
    /// the UDP port the node listens on, so an announce can say "same port as TCP" when it is
    pub udp_port: u16,
}

pub type DhtWatch = watch::Receiver<Option<DhtHandle>>;

/// Owns the running node; dropping it stops the node.
pub struct Dht {
    handle: DhtWatch,
    _stop: DropGuard,
}

impl Dht {
    /// Starts a node on the current tokio runtime, listening on UDP `port` (or any free port
    /// if that one is taken) and keeping its routing table in the database at `db`.
    pub fn start(db: PathBuf, port: u16) -> Dht {
        let (tx, rx) = watch::channel(None);
        let stop = CancellationToken::new();
        tokio::spawn(run(db, port, tx, stop.clone()));
        Dht {
            handle: rx,
            _stop: stop.drop_guard(),
        }
    }

    /// A watch that never delivers a node, for clients that run without DHT. Its sender is
    /// gone, which is what tells a consumer "no DHT, ever" apart from "not up yet".
    pub fn none() -> DhtWatch {
        watch::channel(None).1
    }

    pub fn watch(&self) -> DhtWatch {
        self.handle.clone()
    }
}

async fn run(db: PathBuf, port: u16, ready: watch::Sender<Option<DhtHandle>>, stop: CancellationToken) {
    let socket = match bind(port).await {
        Ok(socket) => socket,
        Err(e) => {
            warn!("no DHT: couldn't bind a UDP socket ({e:#})");
            return;
        }
    };
    let udp_port = socket.local_addr().map(|a| a.port()).unwrap_or(port);

    // BEP 42 ties the node id to the external IP; without one the id is minted for 0.0.0.0
    // and the crate replaces it as soon as a later start learns the real address
    let external_ip = match tokio::time::timeout(EXTERNAL_IP_TIMEOUT, public_ip::addr_v4()).await {
        Ok(Some(ip)) => ip,
        _ => {
            warn!("couldn't learn the external IP, the DHT node id won't be BEP 42 compliant");
            Ipv4Addr::UNSPECIFIED
        }
    };

    let db = db.display().to_string();
    let session = match tokio::task::spawn_blocking(move || DhtSession::with_stable_id(socket, external_ip, &db)).await
    {
        Ok(Ok(session)) => Arc::new(session),
        Ok(Err(e)) => {
            warn!("no DHT: couldn't open its database ({e:#})");
            return;
        }
        Err(e) => {
            warn!("no DHT: {e}");
            return;
        }
    };

    // the node's own tasks must be running before any query can get an answer
    let runner = {
        let session = session.clone();
        tokio::spawn(async move { session.run().await })
    };
    tokio::select! {
        _ = stop.cancelled() => {}
        _ = bootstrap(&session) => {
            info!("DHT node up on UDP port {udp_port}, {} nodes known", session.node_count());
            let _ = ready.send(Some(DhtHandle {
                client: session.handle(),
                udp_port,
            }));
            stop.cancelled().await;
        }
    }
    runner.abort();
}

async fn bind(port: u16) -> std::io::Result<UdpSocket> {
    match UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).await {
        Ok(socket) => Ok(socket),
        Err(e) => {
            warn!("UDP port {port} is taken ({e}), the DHT node will use any free port");
            UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)).await
        }
    }
}

async fn bootstrap(session: &DhtSession) {
    let mut routers = Vec::new();
    for host in BOOTSTRAP_NODES {
        match lookup_host(host).await {
            Ok(addrs) => routers.extend(addrs.filter_map(|a| match a {
                std::net::SocketAddr::V4(v4) => Some(v4),
                _ => None,
            })),
            Err(e) => tracing::debug!("couldn't resolve DHT router {host}: {e}"),
        }
    }
    if routers.is_empty() {
        warn!("no DHT bootstrap router resolved; relying on the saved routing table");
        return;
    }
    if let Err(e) = session.bootstrap(routers).await {
        warn!("DHT bootstrap failed: {e:#}");
    }
}
