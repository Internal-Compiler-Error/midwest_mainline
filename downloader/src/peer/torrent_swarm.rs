use std::net::{Ipv4Addr, SocketAddrV4};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use bitvec::boxed::BitBox;
use tokio::sync::{mpsc, watch};
use tokio::time::{interval, sleep_until, Instant, Sleep};
use reqwest::Client;
use juicy_bencode::BencodeItemView;
use anyhow::bail;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use url::form_urlencoded;
use crate::defs::Identity;
use crate::peer::download::Download;
use crate::peer::{connect_peer, PeerHandle, PeerStatistics};
use crate::storage::TorrentStorage;
use crate::torrent::Torrent;

/// Handles announcements to a single tracker server
struct Announcer {
    tracker_url: String,
    torrent: Arc<Torrent>,
    identity: Arc<Identity>,
    next_ready: Instant,
    swarm_stat: watch::Receiver<TorrentSwarmStats>,
}

impl Announcer {
    fn new(
        tracker_url: String,
        torrent: Arc<Torrent>,
        identity: Arc<Identity>,
        swarm_stat: watch::Receiver<TorrentSwarmStats>,
    ) -> Self {
        Announcer {
            tracker_url,
            torrent,
            identity,
            next_ready: Instant::now() + Duration::from_millis(10),
            swarm_stat,
        }
    }

    /// The future resolves whenever the next round of announce is ready to be performed
    fn ready(&self) -> Sleep {
        sleep_until(self.next_ready)
    }

    /// Perform a single announce and return the interval and discovered peers
    async fn announce(&mut self) -> anyhow::Result<Vec<SocketAddrV4>> {
        // URL-encode info_hash and peer_id
        let info_hash_encoded: String = form_urlencoded::byte_serialize(&self.torrent.info_hash.0).collect();
        let peer_id_encoded: String = form_urlencoded::byte_serialize(&self.identity.peer_id).collect();

        let swarm_stat = self.swarm_stat.borrow().clone();

        // Build the announce URL
        let url = format!(
            "{tracker_url}?info_hash={info_hash}&peer_id={peer_id}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&compact=1",
            tracker_url = self.tracker_url,
            info_hash = info_hash_encoded,
            peer_id = peer_id_encoded,
            port = self.identity.serving.port(),
            uploaded = swarm_stat.uploaded,
            downloaded = swarm_stat.downloaded,
            left = swarm_stat.left,
        );

        // Send the GET request
        let client = Client::new();
        let response = client.get(&url).send().await?;
        let bytes = response.bytes().await?;

        // Parse the bencoded response
        let (_, dict) = juicy_bencode::parse_bencode_dict(&mut bytes.as_ref()).unwrap();

        let mut peers = vec![];
        let Some(BencodeItemView::Integer(interval)) = dict.get(b"interval".as_slice()) else {
            bail!("response interval must be a number");
        };

        if let Some(BencodeItemView::ByteString(peer_bytes)) = dict.get(b"peers".as_slice()) {
            // Each peer is 6 bytes: 4 bytes IP, 2 bytes port
            for chunk in peer_bytes.chunks(6) {
                if chunk.len() != 6 {
                    break;
                }

                let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
                let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                peers.push(SocketAddrV4::new(ip, port));
            }
        }

        self.next_ready = Instant::now() + Duration::from_secs(*interval as u64);
        Ok(peers)
    }
}

#[derive(Clone, Debug)]
struct TorrentSwarmStats {
    pub uploaded: u64,
    pub downloaded: u64,
    /// how many bytes we don't have yet
    pub left: usize,
    /// indexed by piece number, indicates which pieces have been verified, note it also implies we
    /// have a piece if it's verified
    pub verified: BitBox<u8>,
}

// TODO: move this into peer mod so it doesn't need to be pub
pub struct TorrentSwarm {
    peer_handles: Vec<PeerHandle>,
    torrent: Arc<Torrent>,
    storage: Arc<TorrentStorage>,

    id: Arc<Identity>,

    announcers: Vec<Announcer>,

    stat: TorrentSwarmStats,
    stat_snapshot_syn: watch::Sender<TorrentSwarmStats>,
}

impl TorrentSwarm {
    pub fn new(torrent: Arc<Torrent>, storage: Arc<TorrentStorage>, id: Arc<Identity>) -> TorrentSwarm {
        // Create shared peer handles
        let peer_handles = Vec::<PeerHandle>::new();

        let verified = vec![false; torrent.pieces.len()];
        let verified = BitBox::from_iter(verified.iter());
        // TODO: this only works for fresh downloads
        // TODO: verified is not updated
        let stat = TorrentSwarmStats {
            uploaded: 0,
            downloaded: 0,
            left: torrent.total_size as usize,
            verified,
        };
        let (stat_syn, stat_ack) = watch::channel(stat.clone());

        // Create announcer with access to peer handles and storage stats
        // For now, use the primary tracker (first tracker in first tier)
        // TODO: Support multiple trackers from announce_tiers
        let tracker_url = torrent
            .primary_tracker()
            .expect("torrent must have at least one tracker")
            .to_string();
        let announcer = Announcer::new(tracker_url, torrent.clone(), id.clone(), stat_ack);

        TorrentSwarm {
            peer_handles,
            torrent,
            storage,
            id,
            announcers: vec![announcer],
            stat,
            stat_snapshot_syn: stat_syn,
        }
    }

    pub fn aggregate_peer_stats(&mut self) {
        let mut uploaded = 0;
        let mut downloaded = 0;
        for p in self.peer_handles.iter() {
            let snapshot = p.stats_rx.borrow();
            uploaded += snapshot.tcp_info.tcpi_bytes_sent;
            downloaded += snapshot.tcp_info.tcpi_bytes_received;
        }

        self.stat.downloaded = downloaded;
        self.stat.uploaded = uploaded;
        let _ = self.stat_snapshot_syn.send(self.stat.clone());
    }

    pub(crate) async fn work_loop(mut self) {
        let (new_connection_tx, mut new_connection_rx) = mpsc::channel(200);

        let mut aggregate_ticker = interval(Duration::from_mins(5));

        // Get a raw pointer to self to bypass borrow checker
        let self_ptr: *const TorrentSwarm = &self;

        // SAFETY: This is safe because:
        // 1. Download::choose() only reads peer_handles and doesn't hold references across await points
        // 2. tokio::select! branches are mutually exclusive - only one executes at a time
        // 3. When download is polled (branch 3), it doesn't hold any references while other branches execute
        // 4. Mutable accesses to announcers and peer_handles in branches 1 and 2 don't overlap with
        //    download's immutable access to peer_handles in branch 3
        let download = unsafe { Download::new(&*self_ptr) };
        let mut download = pin!(download.download_loop());
        let mut download_done = false;

        loop {
            // TODO: support multiple announcers
            let announcer_ready = self.announcers[0].ready();
            tokio::select! {
                _ = aggregate_ticker.tick() => {
                    self.aggregate_peer_stats();
                }
                _ = announcer_ready => {
                    let mut new_peers = self.announcers[0].announce().await.unwrap();

                    // don't needlessly connect to peers we already have
                    self.peer_handles.sort_unstable();
                    new_peers.retain(|p|{
                        self.peer_handles.binary_search_by(|h| h.remote_addr.cmp(p)).is_err()
                    });

                    // successful connections are pushed to the new_connection_rx channel
                    let _ = tokio::spawn(try_connect(self.id.clone(), self.storage.clone(), self.torrent.clone(), new_peers, new_connection_tx.clone())).await;
                },
                Some(connection) = new_connection_rx.recv() => {
                    self.peer_handles.sort_unstable();
                    let insertion_idx = self.peer_handles.partition_point(|p| p < &connection);
                    if insertion_idx == self.peer_handles.len() || self.peer_handles[insertion_idx] != connection {
                        self.peer_handles.insert(insertion_idx, connection);
                    }
                },
                _ = &mut download, if !download_done => {
                    download_done = true;
                }
            }
        }
    }

    pub fn choose(&self, piece: u32) -> Option<PeerHandle> {
        // Read all peer stats from watch receivers (non-blocking)
        let candidates: Vec<(PeerHandle, PeerStatistics)> = self
            .peer_handles
            .iter()
            .filter_map(|h| {
                let state = h.state.read().unwrap();
                if !state.ready() || !state.they_have(piece) {
                    return None;
                }
                let stat = *h.stats_rx.borrow();
                Some((h.clone(), stat))
            })
            .collect();

        // Select best peer by UCB
        candidates
            .into_iter()
            .max_by(|(_, a), (_, b)| a.ucb.total_cmp(&b.ucb))
            .map(|(handle, _)| handle)
    }

    pub fn torrent(&self) -> &Arc<Torrent> {
        &self.torrent
    }

    pub fn storage(&self) -> &Arc<TorrentStorage> {
        &self.storage
    }
}

async fn try_connect(
    id: Arc<Identity>,
    storage: Arc<TorrentStorage>,
    torrent: Arc<Torrent>,
    newly_discovered: Vec<SocketAddrV4>,
    new_connction_tx: mpsc::Sender<PeerHandle>,
) {
    let work = FuturesUnordered::new();
    for new_peer in newly_discovered {
        work.push(connect_peer(
            new_peer.clone(),
            id.peer_id,
            torrent.clone(),
            storage.clone(),
        ));
    }

    let new_peers: Vec<_> = work.collect().await;
    for new_peer in new_peers.into_iter().filter_map(Result::ok) {
        new_peer.bit_field().await.unwrap();
        new_peer.unchoke_peer().await.unwrap();
        new_peer.fancy_peer().await.unwrap();
        let _ = new_connction_tx.send(new_peer).await;
    }
}