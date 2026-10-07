//! BEP 52 hash requests, both ways: fetching the piece layers a v2 torrent from a magnet
//! lacks (the info dict has only each file's root), and answering peers who ask us.
//!
//! A layer is fetched whole from one peer, in chunks of at most 512 hashes and without proof
//! hashes: once every chunk is in, the layer either rolls up to the file's root or the peer
//! lied. That needs nothing but the root to check against, and a lie costs one layer.

use crate::merkle::{self, Hash};
use crate::torrent::Torrent;
use crate::wire::{HashRequest, Hashes};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Hashes per request; libtorrent asks for (and expects to be asked for) 512.
const CHUNK: u32 = 512;
/// A peer that hasn't sent a whole layer by then loses it to another.
const LAYER_TIMEOUT: Duration = Duration::from_secs(30);

/// The base layer of piece hashes: how far above the 16 KiB leaves a piece's root sits.
pub fn piece_layer_base(torrent: &Torrent) -> u32 {
    (torrent.piece_size as usize / merkle::BLOCK).trailing_zeros()
}

struct Job {
    /// who's sending it, since when
    peer: Option<(SocketAddr, Instant)>,
    chunks: Vec<Option<Vec<Hash>>>,
    tried: BTreeSet<SocketAddr>,
}

/// What a `hashes` message did for the layer it belongs to.
#[derive(Debug, PartialEq, Eq)]
pub enum Received {
    /// a chunk, more to come (or one we didn't ask for, ignored)
    Partial,
    /// the layer of this file is complete and checks out
    Layer(usize),
    /// the layer is complete and wrong, or the chunk's the wrong shape: the sender lied
    Bad,
}

/// The layers still to come, per file.
#[derive(Default)]
pub struct LayerFetch {
    jobs: BTreeMap<usize, Job>,
}

impl LayerFetch {
    /// Only a v2-only torrent needs them: a hybrid's pieces are checked by SHA-1.
    pub fn new(torrent: &Torrent) -> Self {
        if !torrent.v2_only() {
            return Self::default();
        }
        let jobs = torrent
            .missing_layers()
            .into_iter()
            .map(|file| {
                let chunks = torrent.pieces_of_file(file).len().div_ceil(CHUNK as usize);
                let job = Job {
                    peer: None,
                    chunks: vec![None; chunks],
                    tried: BTreeSet::new(),
                };
                (file, job)
            })
            .collect();
        Self { jobs }
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    /// The request for chunk `chunk` of `file`'s layer.
    fn request(torrent: &Torrent, file: usize, chunk: usize) -> HashRequest {
        let pieces = torrent.pieces_of_file(file).len() as u32;
        let index = chunk as u32 * CHUNK;
        HashRequest {
            root: torrent
                .v2
                .as_ref()
                .and_then(|v2| v2.roots[file])
                .expect("a file with a layer has a root"),
            base: piece_layer_base(torrent),
            index,
            // BEP 52: a power of two, at least 2
            length: (pieces - index).next_power_of_two().clamp(2, CHUNK),
            proof_layers: 0,
        }
    }

    /// Hands every layer nobody is sending (or whose sender stalled) to a peer `has` says can
    /// serve that file, one it hasn't failed with yet; returns the requests to send.
    pub fn assign(
        &mut self,
        torrent: &Torrent,
        peers: &[SocketAddr],
        has: impl Fn(SocketAddr, usize) -> bool,
        now: Instant,
    ) -> Vec<(SocketAddr, HashRequest)> {
        let mut out = vec![];
        for (&file, job) in &mut self.jobs {
            if let Some((peer, since)) = job.peer {
                if now.duration_since(since) < LAYER_TIMEOUT {
                    continue;
                }
                job.tried.insert(peer);
                job.peer = None;
            }
            let able: Vec<SocketAddr> = peers.iter().copied().filter(|&p| has(p, file)).collect();
            if able.iter().all(|p| job.tried.contains(p)) {
                // everyone failed once; give them all another go
                job.tried.clear();
            }
            let Some(&peer) = able.iter().find(|p| !job.tried.contains(p)) else {
                continue;
            };
            job.peer = Some((peer, now));
            for (chunk, got) in job.chunks.iter().enumerate() {
                if got.is_none() {
                    out.push((peer, Self::request(torrent, file, chunk)));
                }
            }
        }
        out
    }

    /// A `hashes` message from `from`.
    pub fn received(&mut self, torrent: &Torrent, from: SocketAddr, msg: &Hashes) -> Received {
        let Some(file) = torrent.file_with_root(&msg.request.root) else {
            return Received::Partial;
        };
        let Some(job) = self.jobs.get_mut(&file) else {
            return Received::Partial;
        };
        let chunk = (msg.request.index / CHUNK) as usize;
        if job.peer.is_none_or(|(peer, _)| peer != from)
            || chunk >= job.chunks.len()
            || msg.request != Self::request(torrent, file, chunk)
        {
            return Received::Partial;
        }
        if msg.hashes.len() != msg.request.length as usize {
            return Received::Bad;
        }
        let pieces = torrent.pieces_of_file(file).len();
        let wanted = (pieces - chunk * CHUNK as usize).min(CHUNK as usize);
        job.chunks[chunk] = Some(msg.hashes[..wanted].to_vec());
        if job.chunks.iter().any(Option::is_none) {
            return Received::Partial;
        }
        let layer: Vec<Hash> = job.chunks.iter_mut().flat_map(|c| c.take().unwrap()).collect();
        if torrent.set_layer(file, layer) {
            self.jobs.remove(&file);
            Received::Layer(file)
        } else {
            job.tried.insert(from);
            job.peer = None;
            Received::Bad
        }
    }

    /// `from` won't send `request`, or is gone: its layers go to someone else.
    pub fn give_up(&mut self, from: SocketAddr) {
        for job in self.jobs.values_mut() {
            if job.peer.is_some_and(|(peer, _)| peer == from) {
                job.peer = None;
                job.tried.insert(from);
            }
        }
    }
}

/// Answers a peer's hash request from the piece layers we have, if it's for a whole-file
/// piece layer we know and within the tree; `trees` caches each file's tree above it.
pub fn answer(torrent: &Torrent, trees: &mut BTreeMap<usize, Vec<Vec<Hash>>>, req: &HashRequest) -> Option<Hashes> {
    let file = torrent.file_with_root(&req.root)?;
    let layer = torrent.layer(file)?;
    if req.base != piece_layer_base(torrent) || !req.length.is_power_of_two() || req.length < 2 || req.length > 8192 {
        return None;
    }
    let tree = trees.entry(file).or_insert_with(|| {
        let pad = merkle::zero_subtree(piece_layer_base(torrent));
        merkle::layers_above(layer, pad)
    });
    let (index, length) = (req.index as usize, req.length as usize);
    let height = tree.len() - 1;
    if !index.is_multiple_of(length) || index + length > tree[0].len() || req.proof_layers as usize >= height {
        return None;
    }
    let mut hashes = tree[0][index..index + length].to_vec();
    // as libtorrent does: uncles from the layer where the requested hashes stop implying
    // the tree, up to `proof_layers`
    for level in length.trailing_zeros() as usize..=req.proof_layers as usize {
        hashes.push(tree[level][(index >> level) ^ 1]);
    }
    Some(Hashes {
        request: *req,
        hashes: hashes.into(),
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::torrent::fixtures::{self, sorted};
    use crate::torrent::parse_torrent;

    const P: usize = 32768;

    fn data(len: usize, seed: usize) -> Vec<u8> {
        (0..len).map(|i| ((i * 11 + seed * 17) % 253) as u8).collect()
    }

    fn files() -> Vec<(&'static [&'static str], Vec<u8>)> {
        sorted(&[
            (&["big"], data(70_000, 1)),
            (&["dir", "small"], data(20_000, 2)),
            (&["dir", "empty"], vec![]),
            (&["more"], data(5 * P + 1, 3)),
        ])
    }

    fn addr(n: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, n], 6881))
    }

    #[test]
    fn answers_piece_layer_requests_with_proofs() {
        let files = files();
        let t = parse_torrent(&fixtures::torrent_file("v2", &files, P, false)).unwrap();
        let big = t.file_with_root(&fixtures::root(&files[0].1)).unwrap();
        let layer = fixtures::layer(&files[0].1, P);
        assert_eq!(layer.len(), 3);
        let pad = merkle::zero_subtree(1);
        let mut trees = BTreeMap::new();
        let req = |index, length, proof_layers| HashRequest {
            root: fixtures::root(&files[0].1),
            base: 1,
            index,
            length,
            proof_layers,
        };

        let whole = answer(&t, &mut trees, &req(0, 4, 0)).unwrap();
        assert_eq!(&*whole.hashes, &[layer[0], layer[1], layer[2], pad]);

        // the second half, proven by its uncle: the two make the root
        let half = answer(&t, &mut trees, &req(2, 2, 1)).unwrap();
        assert_eq!(half.hashes.len(), 3);
        assert_eq!(&half.hashes[..2], &[layer[2], pad]);
        let uncle = merkle::pair(&layer[0], &layer[1]);
        assert_eq!(half.hashes[2], uncle);
        assert_eq!(
            merkle::pair(&uncle, &merkle::pair(&layer[2], &pad)),
            t.v2.as_ref().unwrap().roots[big].unwrap()
        );

        assert!(
            answer(&t, &mut trees, &req(1, 2, 0)).is_none(),
            "index not a multiple of length"
        );
        assert!(
            answer(&t, &mut trees, &req(0, 3, 0)).is_none(),
            "length not a power of two"
        );
        assert!(answer(&t, &mut trees, &req(0, 8, 0)).is_none(), "past the layer");
        assert!(answer(&t, &mut trees, &req(0, 2, 2)).is_none(), "proof past the root");
        assert!(
            answer(
                &t,
                &mut trees,
                &HashRequest {
                    base: 0,
                    ..req(0, 2, 0)
                }
            )
            .is_none(),
            "leaf layer"
        );
        assert!(
            answer(
                &t,
                &mut trees,
                &HashRequest {
                    root: [7; 32],
                    ..req(0, 2, 0)
                }
            )
            .is_none(),
            "no such file"
        );
    }

    #[test]
    fn fetches_each_layer_from_one_peer_and_checks_it() {
        let files = files();
        let info = fixtures::info("v2", &files, P, false);
        let t = parse_torrent(&crate::metadata::build_torrent_file(&info, &[])).unwrap();
        let mut fetch = LayerFetch::new(&t);
        assert_eq!(t.missing_layers().len(), 2, "big and more; small is one piece");
        let now = Instant::now();

        // nobody has the pieces: nothing asked
        assert!(fetch.assign(&t, &[addr(1)], |_, _| false, now).is_empty());
        let asked = fetch.assign(&t, &[addr(1), addr(2)], |_, _| true, now);
        assert_eq!(asked.len(), 2);
        assert!(
            asked
                .iter()
                .all(|(peer, req)| *peer == addr(1) && req.proof_layers == 0 && req.index == 0)
        );
        assert!(
            fetch.assign(&t, &[addr(1), addr(2)], |_, _| true, now).is_empty(),
            "already asked"
        );

        let reply = |data: &[u8], req: &HashRequest, lie: bool| {
            let mut hashes = fixtures::layer(data, P);
            hashes.resize(req.length as usize, merkle::zero_subtree(1));
            if lie {
                hashes[0][0] ^= 1;
            }
            Hashes {
                request: *req,
                hashes: hashes.into(),
            }
        };
        let req_for = |data: &[u8]| asked.iter().find(|(_, r)| r.root == fixtures::root(data)).unwrap().1;
        let (big, more) = (&files[0].1, &files[3].1);
        assert_eq!(req_for(more).length, 8, "6 pieces, asked as a power of two");

        assert_eq!(
            fetch.received(&t, addr(2), &reply(big, &req_for(big), false)),
            Received::Partial,
            "not who we asked"
        );
        assert_eq!(
            fetch.received(&t, addr(1), &reply(big, &req_for(big), true)),
            Received::Bad
        );
        let file = t.file_with_root(&req_for(more).root).unwrap();
        assert_eq!(
            fetch.received(&t, addr(1), &reply(more, &req_for(more), false)),
            Received::Layer(file)
        );
        assert_eq!(t.missing_layers().len(), 1);

        // the liar's layer goes to the other peer
        let again = fetch.assign(&t, &[addr(1), addr(2)], |_, _| true, now);
        assert_eq!(again, [(addr(2), req_for(big))]);
        fetch.give_up(addr(2));
        let third = fetch.assign(&t, &[addr(1), addr(2)], |_, _| true, now);
        assert_eq!(third.len(), 1, "everyone failed once, so everyone gets another go");
        let (peer, req) = third[0];
        assert!(matches!(
            fetch.received(&t, peer, &reply(big, &req, false)),
            Received::Layer(_)
        ));
        assert!(fetch.is_empty() || t.missing_layers().is_empty());
        assert!(t.missing_layers().is_empty());
    }

    /// Two clients on loopback: one seeds a v2-only torrent, the other starts from its bare
    /// info dict (as from a magnet), so it has to get the piece layers with hash requests
    /// before any piece can be checked, then downloads and verifies everything by Merkle tree.
    #[tokio::test]
    async fn a_v2_torrent_downloads_between_two_clients() {
        use crate::BtClient;
        use crate::defs::Identity;
        use bitvec::prelude::*;

        let files = files();
        let full = parse_torrent(&fixtures::torrent_file("v2swarm", &files, P, false)).unwrap();
        let info = fixtures::info("v2swarm", &files, P, false);
        let bare = parse_torrent(&crate::metadata::build_torrent_file(&info, &[])).unwrap();
        assert_eq!(full.info_hash, bare.info_hash);

        let scratch = std::env::temp_dir().join(format!("downloader-v2swarm-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&scratch);
        let (seed_dir, leech_dir) = (scratch.join("seed"), scratch.join("leech"));
        for (path, data) in &files {
            let at = path.iter().fold(seed_dir.join("v2swarm"), |p, s| p.join(s));
            std::fs::create_dir_all(at.parent().unwrap()).unwrap();
            std::fs::write(at, data).unwrap();
        }
        let free_port = || {
            std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        };
        let identity = |port: u16, id: &[u8; 20]| Identity {
            peer_id: *id,
            serving: SocketAddr::from(([127, 0, 0, 1], port)),
            dht: false,
            encryption: crate::config::Encryption::Disabled,
        };
        let seed_port = free_port();
        let seeder = BtClient::new(identity(seed_port, b"-DL0100-v2-seeder..."), crate::dht::Dht::none());
        let all = bitvec![u8, Msb0; 1; full.num_pieces()].into_boxed_bitslice();
        seeder.add_torrent_resumed(full.clone(), &seed_dir, all).unwrap();
        assert!(seeder.stats(&full).unwrap().borrow().completed);

        let leecher = BtClient::new(identity(free_port(), b"-DL0100-v2-leecher.."), crate::dht::Dht::none());
        leecher.add_torrent(bare.clone(), &leech_dir).unwrap();
        leecher.add_peers(&bare.info_hash, vec![SocketAddr::from(([127, 0, 0, 1], seed_port))]);
        let mut stats = leecher.stats(&bare).unwrap();
        tokio::time::timeout(Duration::from_secs(30), stats.wait_for(|s| s.completed))
            .await
            .expect("download finished")
            .unwrap();

        assert!(bare.missing_layers().is_empty(), "the layers came from the seeder");
        for (path, data) in &files {
            let at = path.iter().fold(leech_dir.join("v2swarm"), |p, s| p.join(s));
            assert_eq!(&std::fs::read(&at).unwrap(), data, "{}", at.display());
        }
        assert!(!leech_dir.join("v2swarm/.pad").exists(), "padding stays off disk");
        let checked = crate::check::check_files(&bare, &leech_dir, |_| {});
        assert!(checked.all(), "a recheck agrees");
        drop((seeder, leecher));
        std::fs::remove_dir_all(&scratch).unwrap();
    }
}
