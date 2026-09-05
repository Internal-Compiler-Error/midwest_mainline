use crate::defs::Identity;
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use crate::torrent_swarm::{PeerFactory, TorrentSwarm, TorrentSwarmHandle};
use anyhow::bail;
use futures::future::join_all;
use midwest_mainline::types::InfoHash;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub struct BtClient {
    id: Arc<Identity>,
    swarms: HashMap<InfoHash, TorrentSwarm>,
    handles: HashMap<InfoHash, TorrentSwarmHandle>,
    peer_factories: HashMap<InfoHash, PeerFactory>,
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
            shutdown: CancellationToken::new(),
        }
    }

    /// A handle the caller can cancel (e.g. on Ctrl+C) to trigger a graceful shutdown.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub fn add_torrent(&mut self, mut torrent: Torrent) -> anyhow::Result<()> {
        if self.swarms.contains_key(&torrent.info_hash) {
            bail!("task with this info hash already exists");
        }

        let mut files = vec![];
        for (_size, file) in torrent.files.iter_mut() {
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            files.push(File::create(&file)?);
        }

        let torrent = Arc::new(torrent);
        let storage = TorrentStorage::new(torrent.clone(), files);
        let storage = Arc::new(storage);

        let task = TorrentSwarm::new(torrent.clone(), storage, self.id.clone(), self.shutdown.clone());

        self.handles.insert(torrent.info_hash, task.make_handle());
        self.peer_factories.insert(torrent.info_hash, task.peer_factory());
        self.swarms.insert(torrent.info_hash, task);
        Ok(())
    }

    pub(crate) async fn work(self) -> anyhow::Result<()> {
        let BtClient {
            id,
            mut swarms,
            handles,
            peer_factories,
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
        let listener = match TcpListener::bind(id.serving).await {
            Ok(listener) => listener,
            Err(e) => {
                tracing::error!("failed to bind inbound listen address {}: {e:?}", id.serving);
                return;
            }
        };
        tracing::info!("listening for inbound peer connections on {}", id.serving);

        loop {
            let (mut tcp, remote_addr) = tokio::select! {
                accepted = listener.accept() => match accepted {
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

                let peer = factory.accept(tcp, handshake.peer_id);
                if let Some(handle) = handles.get(&handshake.info_hash) {
                    handle.add_initialized_peer(peer).await;
                }
            });
        }
    }
}
