use crate::defs::Identity;
use crate::dht::DhtWatch;
use crate::peer::PeerSnapshot;
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use crate::torrent_swarm::{ConnectedPeer, TorrentSwarm, TorrentSwarmHandle, TorrentSwarmStats};
use anyhow::{Context, bail};
use bitvec::prelude::*;
use futures::future::select_all;
use midwest_mainline::types::InfoHash;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::net::SocketAddr;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// The set of torrents being served, plus the one inbound listener they share. Torrents can be
/// added and removed at any time; each runs as its own swarm from the moment it's added.
///
/// Cheap to clone: clones share the same torrents. The swarms live for as long as any clone
/// does -- the listener only holds a weak reference to them, so dropping the last clone stops
/// every swarm and the listener with it.
#[derive(Clone)]
pub struct BtClient {
    id: Arc<Identity>,
    /// the handle is all there is of a swarm here; dropping it stops the swarm
    swarms: Arc<Mutex<HashMap<InfoHash, TorrentSwarmHandle>>>,
    /// cancelled to trigger a graceful shutdown: each tracker gets a best-effort
    /// event=stopped announce before the process exits
    shutdown: CancellationToken,
    /// the client's DHT node, shared by every swarm
    dht: DhtWatch,
}

impl BtClient {
    /// Must be called on a tokio runtime: the inbound listener starts right away.
    /// The DHT node this client discovers peers with; stays `None` if there is none.
    pub fn dht(&self) -> DhtWatch {
        self.dht.clone()
    }

    pub fn new(id: Identity, dht: DhtWatch) -> Self {
        Self::new_with_shutdown(id, CancellationToken::new(), dht)
    }

    /// Like `new`, but driven by a caller-supplied token, so an owner that already has its own
    /// cancellation scope (see `session::Session`) can stop the client along with everything
    /// else it started, rather than having to reach in for `shutdown_token` afterwards.
    pub fn new_with_shutdown(id: Identity, shutdown: CancellationToken, dht: DhtWatch) -> Self {
        let client = Self {
            id: Arc::new(id),
            swarms: Arc::new(Mutex::new(HashMap::new())),
            shutdown,
            dht,
        };
        tokio::spawn(Self::accept_incoming(
            client.id.clone(),
            Arc::downgrade(&client.swarms),
            client.shutdown.clone(),
        ));
        client
    }

    /// A handle the caller can cancel (e.g. on Ctrl+C) to trigger a graceful shutdown.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// A live view of `torrent`'s aggregate progress, if it's been added. Keeps updating for as
    /// long as the torrent's swarm runs -- the receiver only depends on the underlying channel.
    /// The connected peers of a torrent, refreshed once a second.
    pub fn peers(&self, torrent: &Torrent) -> Option<watch::Receiver<Vec<PeerSnapshot>>> {
        self.swarms
            .lock()
            .unwrap()
            .get(&torrent.info_hash)
            .map(TorrentSwarmHandle::peers)
    }

    pub fn stats(&self, torrent: &Torrent) -> Option<watch::Receiver<TorrentSwarmStats>> {
        self.swarms
            .lock()
            .unwrap()
            .get(&torrent.info_hash)
            .map(TorrentSwarmHandle::stats)
    }

    /// Tells a torrent's swarm about peers found some other way than its own announces, such
    /// as the ones a magnet's metadata fetch met. Unknown torrents are ignored.
    pub fn add_peers(&self, info_hash: &InfoHash, peers: Vec<SocketAddr>) {
        let handle = self.swarms.lock().unwrap().get(info_hash).cloned();
        if let Some(handle) = handle {
            tokio::spawn(async move { handle.peers_discovered(peers).await });
        }
    }

    /// Stops serving `info_hash`: its swarm ends, and every connection to its peers closes.
    /// The files and any resume data are left where they are. Returns whether it was there.
    pub fn remove_torrent(&self, info_hash: &InfoHash) -> bool {
        self.swarms.lock().unwrap().remove(info_hash).is_some()
    }

    /// Starts `torrent` from scratch under `root`: target files are created (or truncated)
    /// and sized. A single-file torrent becomes `root/<name>`, a multi-file one
    /// `root/<name>/...`, the way every mainstream client lays a download out.
    pub fn add_torrent(&self, torrent: Torrent, root: &Path) -> anyhow::Result<()> {
        let verified = bitvec![u8, Msb0; 0; torrent.pieces.len()].into_boxed_bitslice();
        self.add_torrent_with(torrent, root, verified, true)
    }

    /// Picks `torrent` back up where a previous run left it: pieces set in `verified` are
    /// taken to be on disk and correct, so they're neither downloaded nor re-hashed. Target
    /// files are opened in place and must already be their full size -- a missing or
    /// wrong-sized file is an error, since the bitfield can't be trusted against it.
    pub fn add_torrent_resumed(&self, torrent: Torrent, root: &Path, verified: BitBox<u8, Msb0>) -> anyhow::Result<()> {
        if verified.len() != torrent.pieces.len() {
            bail!(
                "resume bitfield covers {} pieces but the torrent has {}",
                verified.len(),
                torrent.pieces.len()
            );
        }
        self.add_torrent_with(torrent, root, verified, false)
    }

    fn add_torrent_with(
        &self,
        torrent: Torrent,
        root: &Path,
        verified: BitBox<u8, Msb0>,
        fresh: bool,
    ) -> anyhow::Result<()> {
        // held while the files are opened too: a second add of the same torrent must not get
        // as far as truncating files the first one is downloading into
        let mut swarms = self.swarms.lock().unwrap();
        if swarms.contains_key(&torrent.info_hash) {
            bail!("{} is already added", torrent.name);
        }

        let mut files = vec![];
        for (size, relative) in torrent.files.iter() {
            let file = root.join(relative);
            fs::create_dir_all(file.parent().unwrap())?;
            let f = File::options()
                .read(true)
                .write(true)
                .create(fresh)
                .truncate(fresh)
                .open(&file)
                .with_context(|| format!("opening {}", file.display()))?;
            if fresh {
                // `TorrentStorage::new` derives per-file offsets from on-disk lengths
                f.set_len(*size as u64)?;
            } else {
                let on_disk = f.metadata()?.len();
                if on_disk != *size as u64 {
                    bail!(
                        "{} is {on_disk} bytes on disk but the torrent says {size}; can't resume",
                        file.display()
                    );
                }
            }
            files.push(f);
        }

        let torrent = Arc::new(torrent);
        let storage = TorrentStorage::new(torrent.clone(), files);
        let storage = Arc::new(storage);

        let handle = TorrentSwarm::spawn(torrent.clone(), storage, self.id.clone(), verified, self.dht.clone());
        swarms.insert(torrent.info_hash, handle);
        Ok(())
    }

    /// Listens for and accepts inbound peer connections (BEP 3 requires this: we advertise a
    /// listening port to trackers, so we must actually accept connections on it, not only dial
    /// out). The remote's info hash isn't known until after we read their handshake, so this
    /// reads first and only replies once we've matched it to a torrent we're serving.
    async fn accept_incoming(
        id: Arc<Identity>,
        swarms: Weak<Mutex<HashMap<InfoHash, TorrentSwarmHandle>>>,
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
                tracing::info!(
                    "listening for inbound peer connections on {}",
                    listener.local_addr().unwrap()
                );
                listeners.push(listener);
            }
            Err(e) => tracing::warn!("no ipv6 inbound listener on port {port} ({e}); ipv6 peers can't dial us"),
        }
        match TcpListener::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).await {
            Ok(listener) => {
                tracing::info!(
                    "listening for inbound peer connections on {}",
                    listener.local_addr().unwrap()
                );
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
            // the client is gone: nothing to route connections to
            if swarms.strong_count() == 0 {
                break;
            }
            let (mut tcp, remote_addr) = tokio::select! {
                (accepted, _idx, _rest) = select_all(listeners.iter().map(|l| Box::pin(l.accept()))) => match accepted {
                    Ok(accepted) => accepted,
                    Err(e) => {
                        tracing::warn!("failed to accept an inbound connection: {e}");
                        continue;
                    }
                },
                _ = shutdown.cancelled() => break,
            };

            let id = id.clone();
            let swarms = swarms.clone();
            tokio::spawn(async move {
                let handshake = match crate::wire::read_handshake(&mut tcp).await {
                    Ok(handshake) => handshake,
                    Err(e) => {
                        tracing::debug!("bad handshake from {remote_addr}: {e:#}");
                        return;
                    }
                };

                // cloned out so the lock isn't held across the awaits below
                let handle = swarms
                    .upgrade()
                    .and_then(|swarms| swarms.lock().unwrap().get(&handshake.info_hash).cloned());
                let Some(handle) = handle else {
                    tracing::debug!("inbound connection from {remote_addr} named a torrent we're not serving");
                    return;
                };

                if let Err(e) = crate::wire::send_handshake(&mut tcp, &handshake.info_hash, &id.peer_id).await {
                    tracing::debug!("failed to reply to handshake from {remote_addr}: {e}");
                    return;
                }

                handle
                    .peer_connected(ConnectedPeer {
                        tcp,
                        remote_addr,
                        remote_supports_extensions: handshake.supports_extensions(),
                        remote_supports_fast: handshake.supports_fast_extension(),
                        peer_id: handshake.peer_id,
                    })
                    .await;
            });
        }
    }
}
