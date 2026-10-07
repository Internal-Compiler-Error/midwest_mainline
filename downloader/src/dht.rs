//! The client's DHT nodes (BEP 5), from the sibling `midwest_mainline` crate. One per address
//! family, shared by every torrent: swarms and the metadata fetcher ask them for peers and
//! announce themselves through `announcer::dht_announcer`. IPv6 is a separate DHT (BEP 32)
//! with its own socket, routing table and node id; the two nodes are paired so each seeds the
//! other's table while it's young.
//!
//! Bootstrapping takes a while, so starting happens in the background and consumers get a
//! `watch` that turns from `None` to a handle once the node is up. A client with no DHT at all just gets a
//! watch that stays `None` (`Dht::none`).

use crate::events::{Event, EventBus};
use midwest_mainline::dht::DhtSession;
use midwest_mainline::dht::client::DhtClient;
use midwest_mainline::types::Family;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{UdpSocket, lookup_host};
use tokio::sync::watch;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{info, warn};

/// Well-known routers that answer `ping` and `find_node` for anyone joining the network. Each
/// node bootstraps from the addresses of its own family.
const BOOTSTRAP_NODES: [&str; 6] = [
    "router.bittorrent.com:6881",
    "router.utorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.libtorrent.org:25401",
    "dht.aelitis.com:6881",
    // IPv6 only
    "router.silotis.us:6881",
];

#[derive(Clone)]
pub struct DhtHandle {
    /// the IPv4 node's lookups
    pub client: DhtClient,
    /// the IPv6 node's, if one could be started
    pub client6: Option<DhtClient>,
    /// the UDP port the IPv4 node listens on, sent to peers in the Port message
    pub udp_port: u16,
    pub udp_port6: Option<u16>,
    session: Arc<DhtSession>,
    session6: Option<Arc<DhtSession>>,
}

impl DhtHandle {
    /// Nodes in the routing tables, both families, for a status line.
    pub fn node_count(&self) -> usize {
        self.session.node_count() + self.session6.as_ref().map_or(0, |s| s.node_count())
    }

    /// Nodes in the IPv6 routing table, if there is an IPv6 node
    pub fn node_count6(&self) -> Option<usize> {
        self.session6.as_ref().map(|s| s.node_count())
    }

    /// One client per family
    pub fn clients(&self) -> Vec<DhtClient> {
        std::iter::once(self.client.clone())
            .chain(self.client6.clone())
            .collect()
    }

    /// The client of `addr`'s family, the one that can reach it
    pub fn client_for(&self, addr: &SocketAddr) -> Option<&DhtClient> {
        match addr {
            SocketAddr::V4(_) => Some(&self.client),
            SocketAddr::V6(_) => self.client6.as_ref(),
        }
    }

    /// The DHT port to tell a peer at `addr` about (BEP 5's Port message): the node it can
    /// reach is the one of its own family.
    pub fn udp_port_for(&self, addr: &SocketAddr) -> u16 {
        match addr {
            SocketAddr::V6(_) => self.udp_port6.unwrap_or(self.udp_port),
            SocketAddr::V4(_) => self.udp_port,
        }
    }
}

#[cfg(test)]
impl DhtHandle {
    /// A handle on one IPv4 node, for tests that run their own
    pub(crate) fn of(session: Arc<DhtSession>) -> Self {
        DhtHandle {
            client: session.handle(),
            client6: None,
            udp_port: session.local_addr().port(),
            udp_port6: None,
            session,
            session6: None,
        }
    }
}

pub type DhtWatch = watch::Receiver<Option<DhtHandle>>;

/// Owns the running node; dropping it stops the node.
pub struct Dht {
    handle: DhtWatch,
    _stop: DropGuard,
}

impl Dht {
    /// Starts the nodes on the current tokio runtime, listening on UDP `port` (or any free
    /// port if that one is taken) and keeping their routing tables in the database at `db`.
    pub fn start(db: PathBuf, port: u16, bus: EventBus) -> Dht {
        let (tx, rx) = watch::channel(None);
        let stop = CancellationToken::new();
        tokio::spawn(run(db, port, tx, stop.clone(), bus));
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

/// A node on `socket` with its database at `db`, off the async threads (opening the database
/// migrates it)
async fn open(socket: UdpSocket, db: String) -> anyhow::Result<Arc<DhtSession>> {
    // the crate learns our external address from other nodes and derives the BEP 42 node id
    // from it at the next start
    let session = tokio::task::spawn_blocking(move || DhtSession::with_stable_id(socket, None, &db)).await??;
    Ok(Arc::new(session))
}

async fn run(db: PathBuf, port: u16, ready: watch::Sender<Option<DhtHandle>>, stop: CancellationToken, bus: EventBus) {
    let socket = match bind(port).await {
        Ok(socket) => socket,
        Err(e) => {
            warn!("no DHT: couldn't bind a UDP socket ({e:#})");
            return;
        }
    };
    let udp_port = socket.local_addr().map(|a| a.port()).unwrap_or(port);
    // the same port for IPv6 where it's free, so peers see one DHT port
    let socket6 = match bind6(udp_port) {
        Ok(socket) => Some(socket),
        Err(e) => {
            warn!("no IPv6 DHT: couldn't bind an IPv6 UDP socket ({e:#})");
            None
        }
    };
    let udp_port6 = socket6.as_ref().and_then(|s| s.local_addr().ok()).map(|a| a.port());

    let db = db.display().to_string();
    let session = match open(socket, db.clone()).await {
        Ok(session) => session,
        Err(e) => {
            warn!("no DHT: couldn't open its database ({e:#})");
            return;
        }
    };
    let session6 = match socket6 {
        Some(socket) => match open(socket, db).await {
            Ok(session6) => {
                session.pair_with(&session6);
                Some(session6)
            }
            Err(e) => {
                warn!("no IPv6 DHT: couldn't open its database ({e:#})");
                None
            }
        },
        None => None,
    };

    // the nodes' own tasks must be running before any query can get an answer
    let runners: Vec<_> = std::iter::once(&session)
        .chain(session6.as_ref())
        .map(|session| {
            let session = session.clone();
            tokio::spawn(async move { session.run().await })
        })
        .collect();
    tokio::select! {
        _ = stop.cancelled() => {}
        _ = bootstrap(&session, session6.as_deref()) => {
            let handle = DhtHandle {
                client: session.handle(),
                client6: session6.as_ref().map(|s| s.handle()),
                udp_port,
                udp_port6,
                session: session.clone(),
                session6: session6.clone(),
            };
            match (udp_port6, handle.node_count6()) {
                (Some(port6), Some(nodes6)) => info!(
                    "DHT nodes up: IPv4 on UDP port {udp_port} with {} nodes, IPv6 on UDP port {port6} with {nodes6} nodes",
                    session.node_count()
                ),
                _ => info!("DHT node up on UDP port {udp_port}, {} nodes known", session.node_count()),
            }
            bus.emit(Event::DhtUp {
                port: udp_port,
                nodes: handle.node_count(),
            });
            let _ = ready.send(Some(handle));
            stop.cancelled().await;
        }
    }
    for runner in runners {
        runner.abort();
    }
}

async fn bind(port: u16) -> std::io::Result<UdpSocket> {
    match UdpSocket::bind(SocketAddrV4::new(crate::defs::BIND_V4, port)).await {
        Ok(socket) => Ok(socket),
        Err(e) => {
            warn!("UDP port {port} is taken ({e}), the DHT node will use any free port");
            UdpSocket::bind(SocketAddrV4::new(crate::defs::BIND_V4, 0)).await
        }
    }
}

/// An IPv6-only UDP socket: the IPv4 traffic belongs to the other node, and on Linux a
/// dual-stack socket couldn't share the port with it anyway
fn bind6(port: u16) -> std::io::Result<UdpSocket> {
    let bind_at = |port: u16| -> std::io::Result<UdpSocket> {
        let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_only_v6(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&SocketAddrV6::new(crate::defs::BIND_V6, port, 0, 0).into())?;
        UdpSocket::from_std(socket.into())
    };
    bind_at(port).or_else(|e| {
        warn!("IPv6 UDP port {port} is taken ({e}), the IPv6 DHT node will use any free port");
        bind_at(0)
    })
}

async fn bootstrap(session: &DhtSession, session6: Option<&DhtSession>) {
    let mut routers = Vec::new();
    let resolved =
        futures::future::join_all(BOOTSTRAP_NODES.map(|host| async move { (host, lookup_host(host).await) }));
    for (host, addrs) in resolved.await {
        match addrs {
            Ok(addrs) => routers.extend(addrs),
            Err(e) => tracing::debug!("couldn't resolve DHT router {host}: {e}"),
        }
    }
    let has = |family| routers.iter().any(|r| Family::of(r) == family);
    if !has(Family::V4) {
        warn!("no IPv4 DHT bootstrap router resolved; relying on the saved routing table");
    }
    let v4 = async {
        if let Err(e) = session.bootstrap(routers.clone()).await {
            warn!("DHT bootstrap failed: {e:#}");
        }
    };
    // the IPv4 node's lookups seed the IPv6 table meanwhile, by asking for nodes6 too
    let v6 = async {
        if let Some(session6) = session6
            && let Err(e) = session6.bootstrap(routers.clone()).await
        {
            warn!("IPv6 DHT bootstrap failed: {e:#}");
        }
    };
    tokio::join!(v4, v6);
}
