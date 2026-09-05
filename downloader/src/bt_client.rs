use crate::defs::Identity;
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use crate::torrent_swarm::{PeerFactory, TorrentSwarm, TorrentSwarmHandle, TorrentSwarmStats};
use anyhow::bail;
use futures::future::{join_all, select_all};
use midwest_mainline::types::InfoHash;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub struct BtClient {
    id: Arc<Identity>,
    swarms: HashMap<InfoHash, TorrentSwarm>,
    handles: HashMap<InfoHash, TorrentSwarmHandle>,
    peer_factories: HashMap<InfoHash, PeerFactory>,
    stats: HashMap<InfoHash, watch::Receiver<TorrentSwarmStats>>,
    /// cancelled to trigger a graceful shutdown: each tracker gets a best-effort
    /// event=stopped announce before the process exits
    shutdown: CancellationToken,
}

impl BtClient {
    pub fn new(id: Identity) -> Self {
        Self {
            id: Arc::new(id),
            swarms: HashMap::new(),
            handles: HashMap::new(),
            peer_factories: HashMap::new(),
            stats: HashMap::new(),
            shutdown: CancellationToken::new(),
        }
    }

    /// A handle the caller can cancel (e.g. on Ctrl+C) to trigger a graceful shutdown.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// A live view of `torrent`'s aggregate progress, if it's been added via `add_torrent`.
    /// Keeps updating for as long as the torrent's swarm task is running, including after
    /// `work()` has taken ownership of this `BtClient` -- the receiver only depends on the
    /// underlying channel, not on anything reachable through `self`.
    pub fn stats(&self, torrent: &Torrent) -> Option<watch::Receiver<TorrentSwarmStats>> {
        self.stats.get(&torrent.info_hash).cloned()
    }

    pub fn add_torrent(&mut self, mut torrent: Torrent) -> anyhow::Result<()> {
        if self.swarms.contains_key(&torrent.info_hash) {
            bail!("task with this info hash already exists");
        }

        let mut files = vec![];
        for (size, file) in torrent.files.iter_mut() {
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            let f = File::create(&file)?;
            // `TorrentStorage::new` reads each file's on-disk length back out (via
            // `file.metadata()`) to compute per-file offsets into the torrent's conceptual
            // single address space -- a freshly `File::create`d file is 0 bytes, so without
            // this every file's offset would come out as 0, corrupting storage for anything
            // but a single-file torrent.
            f.set_len(*size as u64)?;
            files.push(f);
        }

        let torrent = Arc::new(torrent);
        let storage = TorrentStorage::new(torrent.clone(), files);
        let storage = Arc::new(storage);

        let task = TorrentSwarm::new(torrent.clone(), storage, self.id.clone(), self.shutdown.clone());

        self.handles.insert(torrent.info_hash, task.make_handle());
        self.peer_factories.insert(torrent.info_hash, task.peer_factory());
        self.stats.insert(torrent.info_hash, task.subscribe_stats());
        self.swarms.insert(torrent.info_hash, task);
        Ok(())
    }

    pub async fn work(self) -> anyhow::Result<()> {
        let BtClient {
            id,
            mut swarms,
            handles,
            peer_factories,
            stats: _,
            shutdown,
        } = self;

        let mut tasks = vec![tokio::spawn(Self::accept_incoming(
            id,
            Arc::new(handles),
            Arc::new(peer_factories),
            shutdown,
        ))];
        for (_info_hash, swarm) in swarms.drain() {
            tasks.push(tokio::spawn(swarm.work_loop()));
        }

        join_all(tasks).await;
        Ok(())
    }

    /// Listens for and accepts inbound peer connections (BEP 3 requires this: we advertise a
    /// listening port to trackers, so we must actually accept connections on it, not only dial
    /// out). The remote's info hash isn't known until after we read their handshake, so this
    /// reads first and only replies once we've matched it to a torrent we're serving.
    async fn accept_incoming(
        id: Arc<Identity>,
        handles: Arc<HashMap<InfoHash, TorrentSwarmHandle>>,
        peer_factories: Arc<HashMap<InfoHash, PeerFactory>>,
        shutdown: CancellationToken,
    ) {
        // Listen on both families independently rather than relying on a single dual-stack
        // socket (whether an unspecified IPv6 bind also accepts v4-mapped connections is a
        // platform/sysctl-dependent default, not something we can portably assume). Bind IPv6
        // first: if the platform's default *does* make it dual-stack, the IPv4 bind below then
        // fails with AddrInUse, which we treat as "already covered", not an error.
        let port = id.serving.port();
        let mut listeners = Vec::new();

        match TcpListener::bind(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, port, 0, 0)).await {
            Ok(listener) => {
                tracing::info!("listening for inbound peer connections on {}", listener.local_addr().unwrap());
                listeners.push(listener);
            }
            Err(e) => tracing::warn!("no ipv6 inbound listener on port {port} ({e}); ipv6 peers can't dial us"),
        }
        match TcpListener::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).await {
            Ok(listener) => {
                tracing::info!("listening for inbound peer connections on {}", listener.local_addr().unwrap());
                listeners.push(listener);
            }
            Err(e) => tracing::warn!(
                "no ipv4 inbound listener on port {port} ({e}); relying on the ipv6 listener above if it's dual-stack"
            ),
        }

        if listeners.is_empty() {
            tracing::error!("failed to bind any inbound listener on port {port}; not accepting inbound connections");
            return;
        }

        loop {
            let (mut tcp, remote_addr) = tokio::select! {
                (accepted, _idx, _rest) = select_all(listeners.iter().map(|l| Box::pin(l.accept()))) => match accepted {
                    Ok(accepted) => accepted,
                    Err(e) => {
                        tracing::warn!("failed to accept an inbound connection: {e:?}");
                        continue;
                    }
                },
                _ = shutdown.cancelled() => break,
            };

            let id = id.clone();
            let handles = handles.clone();
            let peer_factories = peer_factories.clone();
            tokio::spawn(async move {
                let handshake = match crate::wire::read_handshake(&mut tcp).await {
                    Ok(handshake) => handshake,
                    Err(e) => {
                        tracing::debug!("bad handshake from {remote_addr}: {e:?}");
                        return;
                    }
                };

                let Some(factory) = peer_factories.get(&handshake.info_hash) else {
                    tracing::debug!("inbound connection from {remote_addr} named a torrent we're not serving");
                    return;
                };

                if let Err(e) = crate::wire::send_handshake(&mut tcp, &handshake.info_hash, &id.peer_id).await {
                    tracing::debug!("failed to reply to handshake from {remote_addr}: {e:?}");
                    return;
                }

                let peer = factory.accept(
                    tcp,
                    handshake.peer_id,
                    crate::wire::supports_extensions(&handshake.extensions),
                    crate::wire::supports_fast_extension(&handshake.extensions),
                );
                if let Some(handle) = handles.get(&handshake.info_hash) {
                    handle.add_initialized_peer(peer).await;
                }
            });
        }
    }
}
