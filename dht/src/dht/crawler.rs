//! A BEP 51 crawler: walks the keyspace asking nodes for samples of the info hashes they
//! store, and keeps every one it hears of (the `sampled_infohash` table). Each query asks for
//! the nodes nearest a random target, so the answers keep feeding it nodes from all over the
//! keyspace.
//!
//! It's polite: queries go out at a fixed rate, and a node is asked again only once the
//! `interval` it gave is up (and not within MIN_REVISIT even if it said 0). A node that
//! doesn't answer, or doesn't speak BEP 51, rests for an hour.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use diesel::prelude::*;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use rand::RngExt;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, info, warn};

use crate::dht::client::DhtClient;
use crate::dht::routing_table::sybil_group;
use crate::dht::state::SharedState;
use crate::our_error::OurError;
use crate::schema::sampled_infohash;
use crate::types::{InfoHash, NodeId, NodeInfo};
use crate::utils::unix_timestmap_ms;

/// Even a node that says it has a fresh sample at once isn't asked more often
const MIN_REVISIT: Duration = Duration::from_secs(5 * 60);
/// How long a node that didn't answer is left alone
const FAILED_REST: Duration = Duration::from_secs(60 * 60);
/// Nodes queued to be asked; more than this and new ones are dropped
const MAX_FRONTIER: usize = 50_000;
/// Queries waiting on an answer at once
const MAX_IN_FLIGHT: usize = 64;

/// What the crawler has done so far
#[derive(Debug, Default)]
pub struct CrawlStats {
    pub queried: AtomicU64,
    pub answered: AtomicU64,
    /// info hashes in all answers, repeats included
    pub samples: AtomicU64,
    /// info hashes the store hadn't seen before
    pub new_info_hashes: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct Crawler {
    client: DhtClient,
    state: Arc<SharedState>,
    per_second: u32,
    stats: Arc<CrawlStats>,
}

impl Crawler {
    pub(crate) fn new(client: DhtClient, state: Arc<SharedState>, per_second: u32) -> Self {
        Self {
            client,
            state,
            per_second: per_second.max(1),
            stats: Arc::default(),
        }
    }

    pub fn stats(&self) -> &CrawlStats {
        &self.stats
    }

    /// Crawls until dropped
    pub async fn run(&self) {
        let mut tick = tokio::time::interval(Duration::from_secs(1) / self.per_second);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut frontier: VecDeque<NodeInfo> = VecDeque::new();
        let mut queued: HashSet<SocketAddr> = HashSet::new();
        // when each host may be asked again (see `host`)
        let mut rest_until: HashMap<SocketAddr, Instant> = HashMap::new();
        let mut in_flight = FuturesUnordered::new();
        let mut last_report = Instant::now();
        info!("BEP 51 crawler: {} queries a second", self.per_second);

        loop {
            tokio::select! {
                _ = tick.tick(), if in_flight.len() < MAX_IN_FLIGHT => {
                    let now = Instant::now();
                    if frontier.is_empty() {
                        rest_until.retain(|_, until| *until > now);
                        let random = NodeId(rand::rng().random());
                        for node in self.state.routing_table.find_closest_n(random, 64) {
                            if queued.insert(node.end_point()) {
                                frontier.push_back(node);
                            }
                        }
                    }
                    let Some(node) = next_node(&mut frontier, &mut queued, &rest_until, now) else {
                        continue;
                    };
                    // until it answers, as if it failed: a second query mustn't go out meanwhile
                    rest_until.insert(host(&node), now + FAILED_REST);
                    self.stats.queried.fetch_add(1, Relaxed);
                    let client = &self.client;
                    in_flight.push(async move {
                        let target = NodeId(rand::rng().random());
                        (node, client.sample_infohashes(node.end_point(), target).await)
                    });
                }
                Some((node, result)) = in_flight.next() => {
                    let Ok(sampled) = result else {
                        continue;
                    };
                    self.stats.answered.fetch_add(1, Relaxed);
                    self.stats.samples.fetch_add(sampled.samples.len() as u64, Relaxed);
                    rest_until.insert(host(&node), Instant::now() + sampled.interval.max(MIN_REVISIT));
                    match self.state.record_samples(&sampled.samples) {
                        Ok(new) => {
                            self.stats.new_info_hashes.fetch_add(new as u64, Relaxed);
                        }
                        Err(e) => warn!("couldn't store sampled info hashes: {e}"),
                    }
                    debug!(
                        "{} stores {} info hashes, sampled {}, gave {} nodes",
                        node.end_point(),
                        sampled.num,
                        sampled.samples.len(),
                        sampled.nodes.len()
                    );
                    for node in sampled.nodes {
                        if frontier.len() < MAX_FRONTIER
                            && !rest_until.contains_key(&host(&node))
                            && queued.insert(node.end_point())
                        {
                            frontier.push_back(node);
                        }
                    }
                }
            }
            if last_report.elapsed() > Duration::from_secs(60) {
                last_report = Instant::now();
                rest_until.retain(|_, until| *until > last_report);
                info!(
                    "BEP 51 crawler: {} queried, {} answered, {} new info hashes; {} nodes queued",
                    self.stats.queried.load(Relaxed),
                    self.stats.answered.load(Relaxed),
                    self.stats.new_info_hashes.load(Relaxed),
                    frontier.len()
                );
            }
        }
    }
}

/// What politeness counts as one host: a public IPv4 address or IPv6 /64 (port 0), however
/// many node ids sit behind it; on a LAN, each node
fn host(node: &NodeInfo) -> SocketAddr {
    match sybil_group(&node.end_point().ip()) {
        Some(group) => SocketAddr::new(group, 0),
        None => node.end_point(),
    }
}

fn next_node(
    frontier: &mut VecDeque<NodeInfo>,
    queued: &mut HashSet<SocketAddr>,
    rest_until: &HashMap<SocketAddr, Instant>,
    now: Instant,
) -> Option<NodeInfo> {
    while let Some(node) = frontier.pop_front() {
        queued.remove(&node.end_point());
        if rest_until.get(&host(&node)).is_none_or(|until| *until <= now) {
            return Some(node);
        }
    }
    None
}

impl SharedState {
    /// Keeps sampled info hashes; returns how many weren't known before
    pub(crate) fn record_samples(&self, samples: &[InfoHash]) -> Result<usize, OurError> {
        use crate::schema::sampled_infohash::dsl::*;
        let now = unix_timestmap_ms();
        self.with_conn(|conn| {
            conn.transaction(|conn| {
                let mut new = 0;
                for hash in samples {
                    let inserted = diesel::insert_into(sampled_infohash)
                        .values((
                            info_hash.eq(hash.as_bytes()),
                            first_sampled.eq(now),
                            last_sampled.eq(now),
                        ))
                        .on_conflict_do_nothing()
                        .execute(conn)?;
                    if inserted == 0 {
                        diesel::update(sampled_infohash.filter(info_hash.eq(hash.as_bytes())))
                            .set((last_sampled.eq(now), times_sampled.eq(times_sampled + 1)))
                            .execute(conn)?;
                    }
                    new += inserted;
                }
                Ok(new)
            })
        })
    }

    /// How many distinct info hashes crawling has turned up
    pub(crate) fn sampled_count(&self) -> usize {
        self.with_conn(|conn| sampled_infohash::table.count().get_result::<i64>(conn))
            .unwrap_or_default() as usize
    }
}
