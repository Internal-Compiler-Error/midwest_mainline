use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::sync::Arc;
use anyhow::bail;
use futures::future::join_all;
use midwest_mainline::types::InfoHash;
use crate::defs::Identity;
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;
use crate::peer::torrent_swarm::TorrentSwarm;

pub struct BtClient {
    id: Arc<Identity>,
    swarms: HashMap<InfoHash, TorrentSwarm>,
}

impl BtClient {
    pub fn new(id: Identity) -> Self {
      Self {
            id: Arc::new(id),
            swarms: HashMap::new(),
        }
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

        let task = TorrentSwarm::new(torrent.clone(), storage, self.id.clone());

        self.swarms.insert(torrent.info_hash, task);
        Ok(())
    }

    pub(crate) async fn work(mut self) -> anyhow::Result<()> {
        let mut vec = vec![];
        for (_info_hash, share) in self.swarms.drain() {
            vec.push(tokio::spawn(share.work_loop()));
        }

        join_all(vec).await;
        Ok(())
    }
}