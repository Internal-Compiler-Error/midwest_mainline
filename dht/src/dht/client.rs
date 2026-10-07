//! The client half of the DHT: iterative lookups (find_node, get_peers, BEP 33's scrape,
//! BEP 44's get and put), ping, announce_peer and BEP 51's sample_infohashes, using the shared
//! state. Clone it freely, everything it touches is shared.

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

use crate::dht::bep42;
use crate::dht::item::{self, MutableItem};
use crate::dht::routing_table::sybil_group;
use crate::dht::state::{REQ_TIMEOUT, SharedState};
use crate::message::find_node_get_peers_response::{FindNodeGetPeersResponse, Item};
use crate::message::item_queries::{GetQuery, PutQuery, Signed};
use crate::message::{
    KrpcBody, Want, announce_peer_query::AnnouncePeerQuery, find_node_get_peers_response::ScrapeFilters,
    find_node_query::FindNodeQuery, get_peers_query::GetPeersQuery, ping_query::PingQuery,
    sample_infohashes_query::SampleInfohashesQuery,
};
use crate::our_error::{OurError, naur};
use crate::types::{Family, InfoHash, NodeId, NodeInfo, Token, cmp_resp};
use ed25519_dalek::SigningKey;

/// What a BEP 44 put came to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutOutcome {
    /// where the item is stored
    pub target: NodeId,
    /// how many nodes took it
    pub stored: usize,
}

fn check_value(value: &[u8]) -> Result<(), OurError> {
    if value.len() > item::MAX_VALUE {
        return Err(naur!("BEP 44 values are at most {} bytes bencoded", item::MAX_VALUE));
    }
    if !item::is_canonical_bencode(value) {
        return Err(naur!("a BEP 44 value is one value in canonical bencode"));
    }
    Ok(())
}

/// BEP 33's estimate of a swarm's size
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SwarmEstimate {
    pub seeds: u64,
    /// peers that aren't seeds
    pub peers: u64,
    /// nodes whose filters went into it
    pub nodes: usize,
}

/// Queries in flight at once in a lookup. BEP 5 suggests 3; more costs little on UDP and finishes
/// a lookup in seconds rather than a minute when many nodes are dead.
const CONCURRENT_REQS: usize = 8;
/// A lookup ends once the closest nodes that answered number this many and nothing closer is
/// left to ask: Kademlia's k, where announced peers are stored.
const LOOKUP_K: usize = 8;
/// Nodes from our routing table a lookup starts from. More than k, so a cluster of unhelpful
/// nodes nearest the target can't dead-end every path.
const LOOKUP_SEEDS: u16 = 32;
/// Bounds on one lookup, for a routing table so sparse or stale it never converges.
const LOOKUP_MAX_QUERIES: usize = 200;
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(15);
/// Below this many nodes, the paired node of the other family is still bootstrapping, and
/// our lookups ask for its family's nodes too.
const SEED_SIBLING_BELOW: usize = 500;

/// A node that hasn't answered a lookup's query in this long probably won't: another query
/// takes its slot, though its answer is still taken until REQ_TIMEOUT
const SLOW_REPLY: Duration = Duration::from_millis(1000);

/// `query` to `node`, given up on after `patience` (handing the query back to be waited for
/// elsewhere), or awaited to the end without one
async fn patiently<T, Fut: Future<Output = Result<T, OurError>>>(
    node: NodeInfo,
    mut query: Pin<Box<Fut>>,
    patience: Option<Duration>,
) -> (NodeInfo, Result<Result<T, OurError>, Pin<Box<Fut>>>) {
    match patience {
        Some(patience) => match tokio::time::timeout(patience, &mut query).await {
            Ok(result) => (node, Ok(result)),
            Err(_) => (node, Err(query)),
        },
        None => (node, Ok(query.await)),
    }
}

/// What a lookup makes of one node's answer
struct Heard {
    /// whether the answer counts towards the lookup's end
    useful: bool,
    /// the lookup has what it came for
    done: bool,
}

/// One node's answer to BEP 51's sample_infohashes
#[derive(Debug, Clone)]
pub struct Sampled {
    /// who answered
    pub node: NodeInfo,
    /// not to be asked again before this is up
    pub interval: Duration,
    /// info hashes the node stores in all
    pub num: u64,
    pub samples: Vec<InfoHash>,
    /// nodes close to the target, for walking the keyspace
    pub nodes: Vec<NodeInfo>,
}

/// Outcome of an iterative get_peers lookup (BEP 5).
#[derive(Debug)]
pub struct GetPeersResult {
    /// every peer contact found for the info hash
    pub peers: Vec<SocketAddr>,
    /// the closest nodes that issued us a token and will accept an announce_peer from us
    pub announce_candidates: Vec<(NodeInfo, Token)>,
}

/// The k of `holders` to write to at `target`: the closest, BEP 42 compliant ones first (BEP 42
/// would have nothing stored on the others)
fn closest_holders(mut holders: Vec<(NodeInfo, Token)>, target: NodeId) -> Vec<(NodeInfo, Token)> {
    holders.sort_by_cached_key(|(node, _)| {
        (
            !bep42::compliant(&node.id(), node.end_point().ip()),
            node.id().dist(&target),
        )
    });
    holders.truncate(LOOKUP_K);
    holders
}

#[derive(Debug, Clone)]
pub struct DhtClient {
    state: Arc<SharedState>,
}

impl DhtClient {
    pub(crate) fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    pub fn our_id(&self) -> NodeId {
        self.state.our_id
    }

    /// The address family of the DHT this client looks things up in
    pub fn family(&self) -> Family {
        self.state.family
    }

    /// BEP 32: while the paired node of the other family has few nodes, ask for both
    /// families' nodes and hand it the other family's (see `DhtSession::pair_with`); after
    /// that, only our own (the default, so no `want` at all).
    fn want(&self) -> Option<Want> {
        let sibling = self.state.sibling()?;
        (sibling.routing_table.node_count() < SEED_SIBLING_BELOW).then_some(Want::BOTH)
    }

    /// Asks `dest` a query answered with nodes (find_node, get_peers, sample_infohashes, get)
    async fn ask(&self, dest: SocketAddr, query: KrpcBody) -> Result<FindNodeGetPeersResponse, OurError> {
        let method = query.method();
        match self.state.rpc_manager.query(query, dest, REQ_TIMEOUT).await?.body {
            KrpcBody::FindNodeGetPeersResponse(res) => Ok(res),
            other => Err(naur!("unexpected answer to {method:?}: {other:?}")),
        }
    }

    /// Sends `dest` a query answered with just an id (ping, announce_peer, put), and returns
    /// the id
    async fn tell(&self, dest: SocketAddr, query: KrpcBody) -> Result<NodeId, OurError> {
        let method = query.method();
        match self.state.rpc_manager.query(query, dest, REQ_TIMEOUT).await?.body {
            KrpcBody::PingAnnouncePeerResponse(res) => Ok(res.queried()),
            other => Err(naur!("unexpected answer to {method:?}: {other:?}")),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn ping(&self, peer: SocketAddr) -> Result<NodeId, OurError> {
        let id = self
            .tell(peer, KrpcBody::PingQuery(PingQuery::new(self.our_id())))
            .await?;
        // the routing table learns of it from the broker too, but in its own time; a lookup
        // right after the ping (bootstrapping) needs it there now
        self.state.routing_table.add(id, peer);
        Ok(id)
    }

    /// The nodes closest to `target` that answered us, closest first; just the one if a node
    /// with exactly that id turned up.
    #[tracing::instrument(skip(self))]
    pub async fn find_node(&self, target: NodeId) -> Vec<NodeInfo> {
        if let Some(node) = self.state.routing_table.find_exact(&target) {
            return vec![node];
        }

        let query = KrpcBody::FindNodeQuery(FindNodeQuery::new(self.our_id(), target).with_want(self.want()));
        let family = self.family();
        let mut exact = None;
        let mut closest = self
            .lookup(target, query, |node, res| {
                let nodes = res.nodes_of(family);
                exact = std::iter::once(&node).chain(nodes).find(|n| n.id() == target).copied();
                Heard {
                    useful: !nodes.is_empty(),
                    done: exact.is_some(),
                }
            })
            .await;
        if let Some(exact) = exact {
            return vec![exact];
        }
        closest.truncate(LOOKUP_K);
        closest
    }

    /// An iterative Kademlia lookup towards `target`, streaming rather than in rounds:
    /// CONCURRENT_REQS queries are always in flight, and each answer starts the next query at
    /// once instead of waiting for a round's slowest node. Each node is asked `query`; `heard`
    /// makes of its answer whether it counts towards the lookup's end, and whether the lookup
    /// is done early. The lookup ends when the LOOKUP_K closest nodes that answered usefully
    /// leave nothing closer to ask. Returns those that answered usefully, closest first.
    async fn lookup(
        &self,
        target: NodeId,
        query: KrpcBody,
        mut heard: impl FnMut(NodeInfo, &FindNodeGetPeersResponse) -> Heard,
    ) -> Vec<NodeInfo> {
        let family = self.family();
        let by_distance = |l: &NodeInfo, r: &NodeInfo| cmp_resp(&l.id(), &r.id(), &target);
        // one node per public IP (see `RoutingTable::ip_taken`), here too: a Sybil's many ids
        // around the target would otherwise fill every slot of the lookup
        let mut seen: HashSet<NodeId> = HashSet::from([self.our_id()]);
        let mut seen_ips: HashSet<IpAddr> = HashSet::new();
        let mut fresh_node = move |n: &NodeInfo| {
            seen.insert(n.id()) && sybil_group(&n.end_point().ip()).is_none_or(|group| seen_ips.insert(group))
        };
        let mut known = self.state.routing_table.find_closest_n(target, LOOKUP_SEEDS);
        known.retain(|n| fresh_node(n));
        let mut queried: HashSet<NodeId> = HashSet::new();
        // nodes that answered usefully, closest first; the lookup is done when nothing
        // unqueried is closer than the LOOKUP_K-th of these
        let mut answered: Vec<NodeInfo> = vec![];
        let mut noncompliant = 0;
        let mut in_flight = FuturesUnordered::new();
        // queries still within SLOW_REPLY; the slower ones stay in flight but free their slot
        let mut prompt: Vec<NodeInfo> = vec![];
        let query = &query;
        let deadline = tokio::time::Instant::now() + LOOKUP_TIMEOUT;

        loop {
            while prompt.len() < CONCURRENT_REQS && queried.len() < LOOKUP_MAX_QUERIES {
                let Some(node) = known.iter().find(|n| !queried.contains(&n.id())).copied() else {
                    break;
                };
                if let Some(kth) = answered.get(LOOKUP_K - 1)
                    && by_distance(&node, kth).is_gt()
                {
                    break;
                }
                queried.insert(node.id());
                prompt.push(node);
                let asked = Box::pin(self.ask(node.end_point(), query.clone()));
                in_flight.push(patiently(node, asked, Some(SLOW_REPLY)));
            }
            // the k closest have answered and nothing closer is left to ask: what's still in
            // flight past SLOW_REPLY, typically dead nodes running out their timeouts, is
            // unlikely to change the outcome
            if let Some(kth) = answered.get(LOOKUP_K - 1) {
                let closer = |n: &NodeInfo| by_distance(n, kth).is_lt();
                if !prompt.iter().any(closer) && !known.iter().any(|n| !queried.contains(&n.id()) && closer(n)) {
                    break;
                }
            }
            let Ok(Some((node, outcome))) = tokio::time::timeout_at(deadline, in_flight.next()).await else {
                break;
            };
            prompt.retain(|n| n.id() != node.id());
            let result = match outcome {
                Ok(result) => result,
                Err(slow) => {
                    in_flight.push(patiently(node, slow, None));
                    continue;
                }
            };
            match result {
                Ok(res) => {
                    let Heard { useful, done } = heard(node, &res);
                    if done {
                        break;
                    }
                    // BEP 42: a node whose id doesn't match its address has no say in when the
                    // lookup is done; it can still refer us to others
                    if useful && !bep42::compliant(&node.id(), node.end_point().ip()) {
                        noncompliant += 1;
                    } else if useful {
                        let at = answered.partition_point(|a| by_distance(a, &node).is_lt());
                        answered.insert(at, node);
                    }
                    let nodes = res.nodes_of(family);
                    let before = known.len();
                    known.extend(nodes.iter().filter(|n| fresh_node(n)));
                    debug!(
                        "lookup: {} answered with {} nodes ({} new); {} known, {} queried",
                        node.end_point(),
                        nodes.len(),
                        known.len() - before,
                        known.len(),
                        queried.len()
                    );
                    known.sort_unstable_by(by_distance);
                }
                // our own network can't reach the node (no IPv6 route, say): not its fault
                Err(OurError::IoError(_)) => {}
                // timeouts and the like: record the failure so repeated ones get the node evicted
                Err(_) => self.state.routing_table.mark_failed(&node.id()),
            }
        }
        debug!(
            "lookup of {target:?} done: {} queried, {} answered with BEP 42 ids, {noncompliant} without",
            queried.len(),
            answered.len()
        );
        answered
    }

    /// BEP 51: a sample of the info hashes the node at `dest` stores, and the nodes it knows
    /// closest to `target`
    #[tracing::instrument(skip(self))]
    pub async fn sample_infohashes(&self, dest: SocketAddr, target: NodeId) -> Result<Sampled, OurError> {
        let query = KrpcBody::SampleInfohashesQuery(SampleInfohashesQuery::new(self.our_id(), target));
        let res = self.ask(dest, query).await?;
        let Some(samples) = res.samples() else {
            return Err(naur!("{dest} answered sample_infohashes without samples"));
        };
        Ok(Sampled {
            node: NodeInfo::new(res.queried(), dest),
            interval: Duration::from_secs(samples.interval.into()),
            num: samples.num,
            samples: samples.samples.clone(),
            nodes: res.nodes_of(self.family()).to_vec(),
        })
    }

    /// The peers for `info_hash` the nodes nearest it hold, and the nodes to announce to; nobody
    /// answering is no peers and no one to announce to.
    #[tracing::instrument(skip(self))]
    pub async fn get_peers(&self, info_hash: InfoHash) -> GetPeersResult {
        self.get_peers_with(info_hash, |_| {}).await
    }

    /// `get_peers`, also handing each batch of peers to `found` the moment it turns up, seconds
    /// before the lookup as a whole converges. The peers announced *to us* come first; the
    /// lookup still goes out, since the store holds only what was announced here, and the walk
    /// is what earns the tokens our own announce needs.
    pub async fn get_peers_with(
        &self,
        info_hash: InfoHash,
        mut found: impl FnMut(&[SocketAddr]) + Send,
    ) -> GetPeersResult {
        let ours = self.state.swarm_peers(&info_hash, self.family());
        if !ours.is_empty() {
            found(&ours);
        }
        let (mut result, _) = self.lookup_peers(info_hash, false, found).await;
        for peer in ours {
            if !result.peers.contains(&peer) {
                result.peers.push(peer);
            }
        }
        result
    }

    /// BEP 33: how big the swarm is, from the bloom filters of seeds and peers the nodes
    /// nearest the hash keep, ORed together (with our own, if we hold any), so a peer
    /// announced to several of them counts once
    #[tracing::instrument(skip(self))]
    pub async fn scrape(&self, info_hash: InfoHash) -> SwarmEstimate {
        let (_, filters) = self.lookup_peers(info_hash, true, |_| {}).await;
        let answered = filters.len();
        let both = filters.into_iter().chain(self.state.scrape_filters(&info_hash)).fold(
            ScrapeFilters::default(),
            |acc, f| ScrapeFilters {
                seeds: acc.seeds.union(&f.seeds),
                peers: acc.peers.union(&f.peers),
            },
        );
        SwarmEstimate {
            seeds: both.seeds.estimate().round() as u64,
            peers: both.peers.estimate().round() as u64,
            nodes: answered,
        }
    }

    /// The lookup behind `get_peers` and `scrape`: harvests peers, tokens and, with `scrape`,
    /// BEP 33 filters along the way
    async fn lookup_peers(
        &self,
        info_hash: InfoHash,
        scrape: bool,
        mut found: impl FnMut(&[SocketAddr]) + Send,
    ) -> (GetPeersResult, Vec<ScrapeFilters>) {
        let target = NodeId(info_hash.0);
        let query = KrpcBody::GetPeersQuery(
            GetPeersQuery::new(self.our_id(), info_hash)
                .with_want(self.want())
                .with_scrape(scrape),
        );
        let family = self.family();
        let mut peers: Vec<SocketAddr> = vec![];
        let mut holders: Vec<(NodeInfo, Token)> = vec![];
        let mut filters = vec![];
        self.lookup(target, query, |node, res| {
            let values = res.values();
            debug!("get_peers: {} answered with {} peers", node.end_point(), values.len());
            if !values.is_empty() {
                found(values);
            }
            // BEP 5 has a node without peers return closer nodes; one that sends neither
            // (typically a crawler parked next to popular hashes to collect announces) gets
            // no say in when the lookup is done, and leaves the table
            let useful = !(values.is_empty() && res.nodes_of(family).is_empty());
            if !useful {
                self.state.routing_table.evict(&node.id());
            }
            peers.extend_from_slice(values);
            holders.extend(res.token().map(|token| (node, token.clone())));
            filters.extend(res.scrape().copied());
            Heard { useful, done: false }
        })
        .await;

        peers.sort_unstable();
        peers.dedup();
        let result = GetPeersResult {
            peers,
            announce_candidates: closest_holders(holders, target),
        };
        (result, filters)
    }

    /// BEP 44: the immutable item stored at `target`, bencoded, if any node near it has one
    #[tracing::instrument(skip(self))]
    pub async fn get_immutable(&self, target: NodeId) -> Option<Vec<u8>> {
        let genuine = |item: &Item| item::immutable_target(&item.value) == target;
        if let Some(item) = self.state.stored_item(&target, None).filter(genuine) {
            return Some(item.value);
        }
        let (_, items) = self.lookup_items(target, None, genuine).await;
        items.into_iter().find(genuine).map(|item| item.value)
    }

    /// BEP 44: the newest mutable item of `key` and `salt` the nodes near its target hold, if
    /// any newer than `newer_than`; only ones the key really signed count
    #[tracing::instrument(skip(self))]
    pub async fn get_mutable(&self, key: [u8; 32], salt: &[u8], newer_than: Option<i64>) -> Option<MutableItem> {
        let target = item::mutable_target(&key, salt);
        let (_, items) = self.lookup_items(target, newer_than, |_| false).await;
        items
            .iter()
            .chain(self.state.stored_item(&target, newer_than).as_ref())
            .filter_map(|item| MutableItem::verified(item, &key, salt))
            .max_by_key(|item| item.seq)
    }

    /// BEP 44: stores `value` (one bencoded value) at the nodes closest to its hash
    #[tracing::instrument(skip(self, value))]
    pub async fn put_immutable(&self, value: Vec<u8>) -> Result<PutOutcome, OurError> {
        check_value(&value)?;
        let target = item::immutable_target(&value);
        self.put(target, None, |token| {
            PutQuery::new(self.our_id(), token, value.clone(), None)
        })
        .await
    }

    /// BEP 44: signs `value` (one bencoded value) with `key` under `salt` at `seq`, and stores
    /// it at the nodes closest to the key's target. With `cas`, a node keeps it only if what
    /// it holds has that sequence number.
    #[tracing::instrument(skip(self, key, value))]
    pub async fn put_mutable(
        &self,
        key: &SigningKey,
        salt: &[u8],
        seq: i64,
        value: Vec<u8>,
        cas: Option<i64>,
    ) -> Result<PutOutcome, OurError> {
        check_value(&value)?;
        if salt.len() > item::MAX_SALT {
            return Err(naur!("BEP 44 salts are at most {} bytes", item::MAX_SALT));
        }
        let public = key.verifying_key().to_bytes();
        let signed = Signed {
            key: public,
            salt: salt.to_vec(),
            seq,
            sig: item::sign(key, salt, seq, &value),
        };
        let target = item::mutable_target(&public, salt);
        self.put(target, Some(seq), |token| {
            PutQuery::new(self.our_id(), token, value.clone(), Some(signed.clone())).with_cas(cas)
        })
        .await
    }

    /// BEP 44: stores a mutable item someone else signed, as found, at the nodes closest to its
    /// target. What keeps an item alive past its publisher (BEP 46 has its followers do it).
    #[tracing::instrument(skip_all, fields(seq = found.seq))]
    pub async fn put_signed(&self, found: &MutableItem) -> Result<PutOutcome, OurError> {
        let signed = Signed {
            key: found.key,
            salt: found.salt.clone(),
            seq: found.seq,
            sig: found.sig,
        };
        let target = item::mutable_target(&found.key, &found.salt);
        self.put(target, Some(found.seq), |token| {
            PutQuery::new(self.our_id(), token, found.value.clone(), Some(signed.clone()))
        })
        .await
    }

    /// Gets write tokens from the nodes closest to `target` and puts `query(token)` to each
    async fn put(
        &self,
        target: NodeId,
        seq: Option<i64>,
        query: impl Fn(Token) -> PutQuery,
    ) -> Result<PutOutcome, OurError> {
        let (holders, _) = self.lookup_items(target, seq, |_| false).await;
        if holders.is_empty() {
            return Err(naur!("no node near {target:?} gave us a write token"));
        }
        let puts = holders
            .into_iter()
            .map(|(node, token)| self.tell(node.end_point(), KrpcBody::PutQuery(query(token))));
        let results = futures::future::join_all(puts).await;
        let stored = results.iter().filter(|r| r.is_ok()).count();
        for e in results.into_iter().filter_map(Result::err) {
            debug!("put of {target:?}: {e}");
        }
        Ok(PutOutcome { target, stored })
    }

    /// A lookup with BEP 44's `get`: the k closest nodes that gave us a write token, BEP 42
    /// compliant ones first, and every item found; done early once `wanted` finds one
    async fn lookup_items(
        &self,
        target: NodeId,
        newer_than: Option<i64>,
        wanted: impl Fn(&Item) -> bool,
    ) -> (Vec<(NodeInfo, Token)>, Vec<Item>) {
        let query = KrpcBody::GetQuery(
            GetQuery::new(self.our_id(), target)
                .with_seq(newer_than)
                .with_want(self.want()),
        );
        let family = self.family();
        let mut holders: Vec<(NodeInfo, Token)> = vec![];
        let mut items = vec![];
        self.lookup(target, query, |node, res| {
            holders.extend(res.token().map(|token| (node, token.clone())));
            let item = res.item().cloned();
            let heard = Heard {
                useful: item.is_some() || !res.nodes_of(family).is_empty(),
                done: item.as_ref().is_some_and(&wanted),
            };
            items.extend(item);
            heard
        })
        .await;
        (closest_holders(holders, target), items)
    }

    /// Announces us as a peer for `info_hash` to `recipient`, on `port` or, `None`, the port
    /// our packets come from; `seed` says we have it all (BEP 33)
    #[tracing::instrument(skip(self))]
    pub async fn announce_peers(
        &self,
        recipient: SocketAddr,
        info_hash: InfoHash,
        port: Option<u16>,
        token: Token,
        seed: bool,
    ) -> Result<(), OurError> {
        // with implied_port the port isn't used, but BEP 5 has it sent all the same
        let query = AnnouncePeerQuery::new(self.our_id(), port.is_none(), port.unwrap_or(6881), info_hash, token)
            .with_seed(seed);
        self.tell(recipient, KrpcBody::AnnouncePeerQuery(query)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::routing_table::RoutingTable;
    use crate::dht::rpc_manager::RpcManager;
    use crate::dht::txn_id_generator::TxnIdGenerator;
    use crate::message::Krpc;
    use crate::message::find_node_get_peers_response::Builder as ResBuilder;
    use crate::test_support::memory_pool;
    use std::net::{Ipv4Addr, SocketAddr};
    use tokio::net::UdpSocket;

    /// A fake DHT node that answers every get_peers and find_node query with the given body
    async fn fake_dht_node(socket: UdpSocket, body: KrpcBody) {
        let mut buf = [0u8; 1500];
        loop {
            let (n, peer) = socket.recv_from(&mut buf).await.unwrap();
            let Ok(msg) = Krpc::decode(&buf[..n]) else { continue };
            if !matches!(msg.body, KrpcBody::GetPeersQuery(_) | KrpcBody::FindNodeQuery(_)) {
                continue;
            }
            let resp = Krpc::new(msg.transaction_id().clone(), body.clone());
            let _ = socket.send_to(&resp.encode(), peer).await;
        }
    }

    #[tokio::test]
    async fn get_peers_iterates_referrals_and_captures_tokens() {
        // node B has the goods: a peer contact and a token
        let socket_b = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let SocketAddr::V4(addr_b) = socket_b.local_addr().unwrap() else {
            unreachable!("bound to an ipv4 address");
        };
        let node_b = NodeInfo::new(NodeId([0xBB; 20]), addr_b);
        let token_b = Token::from_bytes(b"tok_b");
        let peer_x: SocketAddr = "10.9.8.7:6881".parse().unwrap();
        let body_b = KrpcBody::FindNodeGetPeersResponse(
            ResBuilder::new(NodeId([0xBB; 20]))
                .with_token(token_b.clone())
                .with_value(peer_x)
                .build(),
        );
        tokio::spawn(fake_dht_node(socket_b, body_b));

        // node A only refers us to B (with its own token)
        let socket_a = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let SocketAddr::V4(addr_a) = socket_a.local_addr().unwrap() else {
            unreachable!("bound to an ipv4 address");
        };
        let token_a = Token::from_bytes(b"tok_a");
        let body_a = KrpcBody::FindNodeGetPeersResponse(
            ResBuilder::new(NodeId([0xAA; 20]))
                .with_token(token_a.clone())
                .with_node(node_b)
                .build(),
        );
        tokio::spawn(fake_dht_node(socket_a, body_a));

        // our client, with node A as the entire routing table
        let router_pool = memory_pool();
        let swarm_pool = memory_pool();

        let our_id = NodeId([0x01; 20]);
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let broker = RpcManager::new(socket, router_pool.clone(), Arc::new(TxnIdGenerator::new()), None);
        broker.run().await.unwrap();
        let routing_table = RoutingTable::new(our_id, broker.clone(), router_pool);
        routing_table.add(NodeId([0xAA; 20]), addr_a.into());

        let state = Arc::new(SharedState::new(our_id, routing_table, broker, swarm_pool));
        let client = DhtClient::new(state);

        let result = client.get_peers(InfoHash([0xFF; 20])).await;

        assert_eq!(result.peers, vec![peer_x], "the referral must be followed to B");
        assert!(
            result
                .announce_candidates
                .iter()
                .any(|(n, t)| n.id() == node_b.id() && *t == token_b),
            "B's token must be captured for a later announce"
        );
        assert!(
            result
                .announce_candidates
                .iter()
                .any(|(n, t)| n.id() == NodeId([0xAA; 20]) && *t == token_a),
            "A's token must be captured too"
        );
    }

    /// A client whose routing table holds just `known`
    async fn client_knowing(known: &[NodeInfo]) -> DhtClient {
        let pool = memory_pool();
        let our_id = NodeId([0x01; 20]);
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let broker = RpcManager::new(socket, pool.clone(), Arc::new(TxnIdGenerator::new()), None);
        broker.run().await.unwrap();
        let routing_table = RoutingTable::new(our_id, broker.clone(), pool.clone());
        for node in known {
            routing_table.add(node.id(), node.end_point());
        }
        DhtClient::new(Arc::new(SharedState::new(our_id, routing_table, broker, pool)))
    }

    /// A fake node with id `id` answering with `nodes`
    async fn fake_referrer(id: NodeId, nodes: &[NodeInfo]) -> NodeInfo {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = socket.local_addr().unwrap();
        let body = KrpcBody::FindNodeGetPeersResponse(ResBuilder::new(id).with_nodes(nodes).build());
        tokio::spawn(fake_dht_node(socket, body));
        NodeInfo::new(id, addr)
    }

    #[tokio::test]
    async fn find_node_streams_past_a_dead_node_to_the_exact_match() {
        // A refers us to B and to D, which is closer to the target but never answers; B
        // knows the target itself
        let target = NodeId([0xC0; 20]);
        let target_node = NodeInfo::new(target, SocketAddr::from((Ipv4Addr::LOCALHOST, 9)));
        let b = fake_referrer(NodeId([0xB0; 20]), &[target_node]).await;
        let dead = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let d = NodeInfo::new(NodeId([0xC1; 20]), dead.local_addr().unwrap());
        let a = fake_referrer(NodeId([0xA0; 20]), &[b, d]).await;

        let client = client_knowing(&[a]).await;
        let started = tokio::time::Instant::now();
        let found = client.find_node(target).await;
        assert_eq!(found, vec![target_node]);
        assert!(
            started.elapsed() < SLOW_REPLY,
            "B's answer ends the lookup without waiting for D: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn find_node_returns_the_closest_that_answered() {
        // a chain of referrals, each node knowing the next one closer to the target
        let target = NodeId([0xFF; 20]);
        let mut next: Vec<NodeInfo> = vec![];
        for byte in [0xF0u8, 0xE0, 0xC0, 0x80] {
            next = vec![fake_referrer(NodeId([byte; 20]), &next).await];
        }
        let client = client_knowing(&next).await;
        let found = client.find_node(target).await;
        let ids: Vec<u8> = found.iter().map(|n| n.id().0[0]).collect();
        // the 0xF0 node answers without nodes, which doesn't count as a useful answer
        assert_eq!(ids, vec![0xE0, 0xC0, 0x80]);
    }
}
