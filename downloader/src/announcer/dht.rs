//! The DHT as a peer source (BEP 5): every DHT_ANNOUNCE_INTERVAL (sooner while lookups come
//! back empty, see DHT_RETRY), look the info hash up, hand whatever peers come back to the
//! swarm, and announce our port to the nodes that issued tokens. With an IPv6 node too (BEP
//! 32), both DHTs are looked up at once and their peers merged. Now and then a lookup also
//! collects BEP 33's scrape filters, for an estimate of the swarm's size.

use super::tracker::SCRAPE_INTERVAL;
use super::{Announcing, Row, SwarmCounts, TrackerState};
use crate::dht::{DhtHandle, DhtWatch};
use crate::events::{Event as BusEvent, PeerSource};
use crate::settings::{DHT_ANNOUNCE_INTERVAL, DHT_RETRY};
use crate::torrent_swarm::SwarmEvent;
use midwest_mainline::dht::client::{DhtClient, GetPeersResult, SwarmEstimate};
use midwest_mainline::types::{InfoHash, NodeInfo, Token};
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info};

/// Announces to the DHT until shutdown. Waits for the node to come up first, and does nothing
/// at all if it never does.
pub(super) async fn announce(args: Announcing, row: Row) {
    let Announcing {
        info_hash,
        identity,
        stats,
        events,
        shutdown,
        dht,
        bus,
        ..
    } = args;
    let Some(handle) = node(dht, &shutdown).await else {
        return;
    };
    let port = Some(identity.serving.port());
    let mut retry = DHT_RETRY;
    let mut last_scrape = None;

    loop {
        let started = Instant::now();
        let scrape = last_scrape.is_none_or(|at: Instant| at.elapsed() >= SCRAPE_INTERVAL / 2);
        if scrape {
            last_scrape = Some(started);
        }
        let lookups = handle
            .clients()
            .into_iter()
            .map(|client| lookup(client, info_hash, scrape, events.clone()));
        let lookups = tokio::select! {
            _ = shutdown.cancelled() => return,
            lookups = futures::future::join_all(lookups) => lookups,
        };
        let round = Round::of(lookups, scrape);
        let wait = if round.peers.is_empty() {
            let wait = retry;
            retry = (retry * 2).min(DHT_ANNOUNCE_INTERVAL);
            wait
        } else {
            retry = DHT_RETRY;
            DHT_ANNOUNCE_INTERVAL
        };
        let next = Some(Instant::now() + wait);
        bus.emit(BusEvent::DhtLookup {
            info_hash,
            peers: round.peers.len(),
            took_ms: started.elapsed().as_millis() as u64,
        });
        row.update(|row| {
            row.state = TrackerState::Working;
            row.peers = round.peers.len();
            row.next_announce = next;
            row.swarm = row.swarm.updated(round.swarm);
        });
        // the whole set again, in case a batch the lookup streamed found the queue full;
        // the swarm and the metadata fetch both skip addresses they already have
        let Some(events) = events.upgrade() else { return };
        if events
            .send(SwarmEvent::PeersDiscovered(round.peers, PeerSource::Dht))
            .await
            .is_err()
        {
            return;
        }
        // BEP 33: nodes count seeds apart, for DHT scrapes
        let seed = {
            let stats = stats.borrow();
            !stats.verified.is_empty() && stats.verified.all()
        };
        // all at once and in the background: one at a time, each dead node held the next
        // lookup back by a full request timeout
        tokio::spawn(announce_to(round.announce_to, info_hash, port, seed));
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

/// The DHT nodes once they're up; `None` on shutdown, or if the client never gets any.
async fn node(mut dht: DhtWatch, shutdown: &CancellationToken) -> Option<DhtHandle> {
    loop {
        if let Some(handle) = dht.borrow().clone() {
            return Some(handle);
        }
        tokio::select! {
            _ = shutdown.cancelled() => return None,
            changed = dht.changed() => changed.ok()?,
        }
    }
}

/// One family's lookup, with a BEP 33 scrape alongside if `scrape`. Peers go to the swarm as
/// nodes return them: waiting for the lookup to converge would leave them idle for the
/// seconds that takes.
async fn lookup(
    client: DhtClient,
    info_hash: InfoHash,
    scrape: bool,
    events: mpsc::WeakSender<SwarmEvent>,
) -> (DhtClient, GetPeersResult, SwarmEstimate) {
    let span = tracing::info_span!(
        "dht.lookup",
        info_hash = %info_hash,
        family = %client.family(),
        peers = tracing::field::Empty,
        announce_to = tracing::field::Empty,
        seeds = tracing::field::Empty,
        swarm_peers = tracing::field::Empty,
    );
    let streamed = client.get_peers_with(info_hash, move |peers| {
        if let Some(events) = events.upgrade() {
            let _ = events.try_send(SwarmEvent::PeersDiscovered(peers.to_vec(), PeerSource::Dht));
        }
    });
    // a walk of its own: nodes answer BEP 33's scrape=1 with filters instead of peers
    let estimate = async {
        if scrape {
            client.scrape(info_hash).await
        } else {
            SwarmEstimate::default()
        }
    };
    let (found, estimate) = futures::future::join(streamed, estimate).instrument(span.clone()).await;
    span.record("seeds", estimate.seeds);
    span.record("swarm_peers", estimate.peers);
    span.record("peers", found.peers.len());
    span.record("announce_to", found.announce_candidates.len());
    (client, found, estimate)
}

/// What one round of lookups, a family each, came to.
#[derive(Default)]
struct Round {
    peers: Vec<SocketAddr>,
    /// the nodes that took a token for our announce, and the client that can reach each
    announce_to: Vec<(DhtClient, (NodeInfo, Token))>,
    swarm: SwarmCounts,
}

impl Round {
    fn of(lookups: Vec<(DhtClient, GetPeersResult, SwarmEstimate)>, scraped: bool) -> Round {
        let mut round = Round::default();
        for (client, found, estimate) in lookups {
            // the families' filters can't be merged, so the larger estimate stands
            if scraped && estimate.nodes > 0 {
                let max = |a: Option<u32>, b: u64| Some(a.unwrap_or(0).max(b as u32));
                round.swarm.seeders = max(round.swarm.seeders, estimate.seeds);
                round.swarm.leechers = max(round.swarm.leechers, estimate.peers);
            }
            info!(
                "{} DHT lookup found {} peers, {} nodes accept our announce{}",
                client.family(),
                found.peers.len(),
                found.announce_candidates.len(),
                if scraped {
                    format!(
                        "; the swarm is ~{} seeds and ~{} peers by {} nodes' filters",
                        estimate.seeds, estimate.peers, estimate.nodes
                    )
                } else {
                    String::new()
                }
            );
            round.peers.extend(found.peers);
            round.announce_to.extend(
                found
                    .announce_candidates
                    .into_iter()
                    .map(|candidate| (client.clone(), candidate)),
            );
        }
        round
    }
}

async fn announce_to(nodes: Vec<(DhtClient, (NodeInfo, Token))>, info_hash: InfoHash, port: Option<u16>, seed: bool) {
    futures::future::join_all(nodes.into_iter().map(|(client, (node, token))| async move {
        if let Err(e) = client
            .announce_peers(node.end_point(), info_hash, port, token, seed)
            .await
        {
            tracing::debug!("announce to DHT node {} failed: {e:#}", node.end_point());
        }
    }))
    .await;
}
