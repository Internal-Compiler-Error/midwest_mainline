//! State shared by the DHT's two halves: who we are, who we know, what we store, and how
//! we talk to the network. The server answers queries with it, the client looks things
//! up with it; neither owns any of it alone.

use diesel::r2d2::{ConnectionManager, Pool};
use diesel::{SqliteConnection, prelude::*};
use rand::RngExt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use crate::dht::routing_table::RoutingTable;
use crate::dht::rpc_manager::RpcManager;
use crate::message::find_node_get_peers_response::{Samples, ScrapeFilters};
use crate::our_error::{OurError, naur};
use crate::schema::{peer, swarm};
use crate::token_generator::TokenGenerator;
use crate::types::{Family, InfoHash, NodeId};
use crate::utils::unix_timestmap_ms;
use tracing::warn;

/// How long to wait for a node to answer. Nodes that answer at all do so within a second
/// or two; a dead one held up an entire lookup round when this was 15 s.
pub const REQ_TIMEOUT: Duration = Duration::from_secs(3);

/// BEP 5's suggested lifetime of an announcement. Only peers announced within it are handed
/// out in get_peers responses, whatever the [`Retention`].
pub const PEER_LIFETIME: Duration = Duration::from_secs(45 * 60);

/// How long the BEP 51 sample we hand out stands. BEP 51 allows up to 6 hours; a quarter of
/// an hour lets crawlers see more of a big store, and costs a query of the store that often.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(15 * 60);
/// Info hashes in a BEP 51 sample; 20 of them and 8 nodes fit a UDP packet comfortably
pub const MAX_SAMPLES: i64 = 20;

/// What happens to an announced peer once it's older than [`PEER_LIFETIME`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Retention {
    /// deleted, as a normal DHT node does
    #[default]
    Expire,
    /// kept for good, so the node doubles as a long-term index of who announced what (see
    /// [`DhtSession::stored_peers`](crate::dht::DhtSession::stored_peers)). Stale peers still
    /// aren't served to other nodes.
    Forever,
}

/// A peer announced to us, as the store remembers it. Timestamps are unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredPeer {
    pub addr: SocketAddr,
    pub first_announced: i64,
    pub last_announced: i64,
}

fn lifetime_cutoff() -> i64 {
    unix_timestmap_ms() - PEER_LIFETIME.as_millis() as i64
}

/// A peer's address as the `peer` table keeps it; `None` for a row that isn't one
fn parse_addr(ip: &str, port: i32) -> Option<SocketAddr> {
    Some(SocketAddr::new(ip.parse().ok()?, port.try_into().ok()?))
}

/// What a read of the store that failed comes to: nothing, and a line in the log
fn or_nothing<T: Default>(read: Result<T, impl std::fmt::Display>) -> T {
    read.inspect_err(|e| warn!("couldn't read the store: {e}"))
        .unwrap_or_default()
}

#[derive(Debug)]
pub(crate) struct SharedState {
    pub(crate) our_id: NodeId,
    pub(crate) family: Family,
    pub(crate) routing_table: RoutingTable,
    pub(crate) conn: Pool<ConnectionManager<SqliteConnection>>,
    pub(crate) token_generator: TokenGenerator,
    pub(crate) rpc_manager: RpcManager,
    /// the node of the other address family on this host, if any (see
    /// [`DhtSession::pair_with`](crate::dht::DhtSession::pair_with)); weak, as each points at
    /// the other
    pub(crate) sibling: OnceLock<Weak<SharedState>>,
    /// the BEP 51 sample we hand out, and when it was taken
    samples: Mutex<Option<(Instant, Samples)>>,
}

impl SharedState {
    pub(crate) fn new(
        our_id: NodeId,
        routing_table: RoutingTable,
        rpc_manager: RpcManager,
        conn: Pool<ConnectionManager<SqliteConnection>>,
    ) -> Self {
        Self {
            our_id,
            family: rpc_manager.family(),
            routing_table,
            conn,
            token_generator: TokenGenerator::new(rand::rng().random()),
            rpc_manager,
            sibling: OnceLock::new(),
            samples: Mutex::new(None),
        }
    }

    pub(crate) fn sibling(&self) -> Option<std::sync::Arc<SharedState>> {
        self.sibling.get()?.upgrade()
    }

    /// The routing table holding `family`'s nodes: ours, or the sibling's
    pub(crate) fn table_of(&self, family: Family) -> Option<RoutingTable> {
        if family == self.family {
            Some(self.routing_table.clone())
        } else {
            self.sibling().map(|s| s.routing_table.clone())
        }
    }

    /// `f` of the state, on the blocking pool: what reads or writes the store, called from
    /// async code. `None` if it panicked.
    pub(crate) async fn blocking<T: Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(&SharedState) -> T + Send + 'static,
    ) -> Option<T> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || f(&this)).await.ok()
    }

    /// Runs `f` on a connection of the pool
    pub(crate) fn with_conn<T>(
        &self,
        f: impl FnOnce(&mut SqliteConnection) -> Result<T, diesel::result::Error>,
    ) -> Result<T, OurError> {
        let mut conn = self
            .conn
            .get()
            .map_err(|e| naur!("could not check out a db connection: {e}"))?;
        Ok(f(&mut conn)?)
    }

    /// Peers of `family` for `info_hash` that were announced *to us* within
    /// [`PEER_LIFETIME`], freshest first, capped so a get_peers response fits BEP 32's 1024
    /// bytes. A get_peers answer only carries the family it was asked over (BEP 32).
    pub(crate) fn swarm_peers(&self, info_hash: &InfoHash, family: Family) -> Vec<SocketAddr> {
        self.swarm_peers_preferring(info_hash, family, false)
    }

    /// `swarm_peers`, and with `noseed` (BEP 33), the ones that aren't seeds first
    pub(crate) fn swarm_peers_preferring(&self, info_hash: &InfoHash, family: Family, noseed: bool) -> Vec<SocketAddr> {
        let query = peer::table
            .filter(peer::swarm.eq(&info_hash.0))
            .filter(peer::last_announced.ge(lifetime_cutoff()))
            .select((peer::ip_addr, peer::port))
            .into_boxed();
        let query = match noseed {
            true => query.order((peer::seed.asc(), peer::last_announced.desc())),
            false => query.order(peer::last_announced.desc()),
        };
        // an IPv6 address in text always has a colon, an IPv4 one never does
        let query = match family {
            Family::V4 => query.filter(peer::ip_addr.not_like("%:%")).limit(50),
            Family::V6 => query.filter(peer::ip_addr.like("%:%")).limit(25),
        };
        or_nothing(self.with_conn(|conn| query.load::<(String, i32)>(conn)))
            .into_iter()
            .filter_map(|(ip, port)| parse_addr(&ip, port))
            .collect()
    }

    /// BEP 33: bloom filters of the seeds' and the other peers' addresses announced to us for
    /// `info_hash` within [`PEER_LIFETIME`], both families; `None` if there are none
    pub(crate) fn scrape_filters(&self, info_hash: &InfoHash) -> Option<ScrapeFilters> {
        let peers = or_nothing(self.with_conn(|conn| {
            peer::table
                .filter(peer::swarm.eq(&info_hash.0))
                .filter(peer::last_announced.ge(lifetime_cutoff()))
                .select((peer::ip_addr, peer::seed))
                .load::<(String, bool)>(conn)
        }));
        if peers.is_empty() {
            return None;
        }
        let mut filters = ScrapeFilters::default();
        for (ip, seed) in peers {
            let Ok(ip) = ip.parse() else { continue };
            match seed {
                true => filters.seeds.insert(ip),
                false => filters.peers.insert(ip),
            }
        }
        Some(filters)
    }

    /// Every peer ever announced to us for `info_hash` that the store still holds, stale or
    /// not, most recently announced first.
    pub(crate) fn stored_peers(&self, info_hash: &InfoHash) -> Vec<StoredPeer> {
        let rows = or_nothing(self.with_conn(|conn| {
            peer::table
                .filter(peer::swarm.eq(&info_hash.0))
                .order(peer::last_announced.desc())
                .select((peer::ip_addr, peer::port, peer::first_announced, peer::last_announced))
                .load::<(String, i32, i64, i64)>(conn)
        }));
        rows.into_iter()
            .filter_map(|(ip, port, first_announced, last_announced)| {
                Some(StoredPeer {
                    addr: parse_addr(&ip, port)?,
                    first_announced,
                    last_announced,
                })
            })
            .collect()
    }

    /// Info hashes the store holds at least one peer for.
    pub(crate) fn stored_swarms(&self) -> Vec<InfoHash> {
        or_nothing(self.with_conn(|conn| peer::table.select(peer::swarm).distinct().load::<Vec<u8>>(conn)))
            .iter()
            .filter_map(|bytes| InfoHash::try_from_bytes(bytes))
            .collect()
    }

    /// Records that `addr` announced itself for `info_hash`, a seed or not (BEP 33). Announcing
    /// again keeps when it was first seen.
    pub(crate) fn store_peer(&self, info_hash: &InfoHash, addr: SocketAddr, seed: bool) -> Result<(), OurError> {
        let info_hash = info_hash.as_bytes();
        let now = unix_timestmap_ms();
        self.with_conn(|conn| {
            conn.transaction(|conn| {
                // the peer table references the swarm, so make sure it exists first
                diesel::insert_into(swarm::table)
                    .values(swarm::info_hash.eq(info_hash))
                    .on_conflict_do_nothing()
                    .execute(conn)?;
                diesel::insert_into(peer::table)
                    .values((
                        peer::ip_addr.eq(addr.ip().to_string()),
                        peer::port.eq(i32::from(addr.port())),
                        peer::swarm.eq(info_hash),
                        peer::first_announced.eq(now),
                        peer::last_announced.eq(now),
                        peer::seed.eq(seed),
                    ))
                    .on_conflict((peer::ip_addr, peer::port, peer::swarm))
                    .do_update()
                    .set((peer::last_announced.eq(now), peer::seed.eq(seed)))
                    .execute(conn)?;
                Ok(())
            })
        })
    }

    /// BEP 51: up to [`MAX_SAMPLES`] info hashes from the store at random, refreshed every
    /// [`SAMPLE_INTERVAL`], and how many the store holds
    pub(crate) fn sample(&self) -> Samples {
        let mut cached = self.samples.lock().unwrap();
        if let Some((taken, samples)) = &*cached
            && taken.elapsed() < SAMPLE_INTERVAL
        {
            let left = SAMPLE_INTERVAL.saturating_sub(taken.elapsed());
            return Samples {
                interval: left.as_secs() as u32,
                ..samples.clone()
            };
        }
        let (num, hashes) = or_nothing(self.with_conn(|conn| {
            let num: i64 = swarm::table.count().get_result(conn)?;
            let hashes = swarm::table
                .select(swarm::info_hash)
                .order(diesel::dsl::sql::<diesel::sql_types::Integer>("random()"))
                .limit(MAX_SAMPLES)
                .load::<Vec<u8>>(conn)?;
            Ok((num, hashes))
        }));
        let samples = Samples {
            interval: SAMPLE_INTERVAL.as_secs() as u32,
            num: num as u64,
            samples: hashes.iter().filter_map(|h| InfoHash::try_from_bytes(h)).collect(),
        };
        *cached = Some((Instant::now(), samples.clone()));
        samples
    }

    /// Deletes peers announced longer than [`PEER_LIFETIME`] ago, and swarms left with none.
    pub(crate) fn expire_peers(&self) -> Result<(), OurError> {
        self.with_conn(|conn| {
            conn.transaction(|conn| {
                diesel::delete(peer::table.filter(peer::last_announced.lt(lifetime_cutoff()))).execute(conn)?;
                diesel::delete(swarm::table.filter(diesel::dsl::not(diesel::dsl::exists(
                    peer::table.filter(peer::swarm.eq(swarm::info_hash)),
                ))))
                .execute(conn)?;
                Ok(())
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::rpc_manager::RpcManager;
    use crate::dht::txn_id_generator::TxnIdGenerator;
    use crate::test_support::memory_pool;
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use tokio::net::UdpSocket;

    async fn state() -> SharedState {
        let pool = memory_pool();
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let rpc = RpcManager::new(socket, pool.clone(), Arc::new(TxnIdGenerator::new()), None);
        let id = NodeId([7; 20]);
        SharedState::new(id, RoutingTable::new(id, rpc.clone(), pool.clone()), rpc, pool)
    }

    fn announce(state: &SharedState, info_hash: &InfoHash, ip: &str, port: u16, first: i64, last: i64) {
        let mut conn = state.conn.get().unwrap();
        diesel::insert_into(swarm::table)
            .values(swarm::info_hash.eq(info_hash.0.to_vec()))
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .unwrap();
        diesel::insert_into(peer::table)
            .values((
                peer::ip_addr.eq(ip),
                peer::port.eq(port as i32),
                peer::swarm.eq(info_hash.0.to_vec()),
                peer::first_announced.eq(first),
                peer::last_announced.eq(last),
            ))
            .execute(&mut conn)
            .unwrap();
    }

    #[tokio::test]
    async fn only_fresh_peers_are_served_and_expiry_deletes_the_rest() {
        let state = state().await;
        let fresh_hash = InfoHash([1; 20]);
        let stale_hash = InfoHash([2; 20]);
        let now = unix_timestmap_ms();
        let two_hours_ago = now - 2 * 60 * 60 * 1000;
        announce(&state, &fresh_hash, "10.0.0.1", 1000, two_hours_ago, now);
        announce(&state, &fresh_hash, "10.0.0.1", 1001, two_hours_ago, two_hours_ago);
        announce(&state, &stale_hash, "10.0.0.1", 1002, two_hours_ago, two_hours_ago);

        let fresh: SocketAddr = "10.0.0.1:1000".parse().unwrap();
        assert_eq!(state.swarm_peers(&fresh_hash, Family::V4), vec![fresh]);
        assert!(state.swarm_peers(&stale_hash, Family::V4).is_empty());
        assert_eq!(state.stored_swarms().len(), 2, "kept until something expires them");
        assert_eq!(state.stored_peers(&fresh_hash).len(), 2);

        state.expire_peers().unwrap();
        assert_eq!(state.stored_swarms(), vec![fresh_hash]);
        assert_eq!(
            state.stored_peers(&fresh_hash),
            vec![StoredPeer {
                addr: fresh,
                first_announced: two_hours_ago,
                last_announced: now,
            }]
        );
        let mut conn = state.conn.get().unwrap();
        let swarms: i64 = swarm::table.count().get_result(&mut conn).unwrap();
        assert_eq!(swarms, 1, "a swarm with no peers left goes too");
    }

    #[tokio::test]
    async fn announcing_again_keeps_when_the_peer_was_first_seen() {
        let state = state().await;
        let info_hash = InfoHash([3; 20]);
        let addr = SocketAddr::from((Ipv4Addr::new(10, 0, 0, 2), 6881));

        state.store_peer(&info_hash, addr, false).unwrap();
        let mut conn = state.conn.get().unwrap();
        diesel::update(peer::table)
            .set((peer::first_announced.eq(1), peer::last_announced.eq(1)))
            .execute(&mut conn)
            .unwrap();
        drop(conn);
        state.store_peer(&info_hash, addr, true).unwrap();

        let [stored] = state.stored_peers(&info_hash)[..] else {
            panic!("one peer")
        };
        assert_eq!(stored.first_announced, 1);
        assert!(stored.last_announced > 1);
    }

    #[tokio::test]
    async fn peers_are_served_to_their_own_family() {
        let state = state().await;
        let hash = InfoHash([1; 20]);
        let now = unix_timestmap_ms();
        announce(&state, &hash, "10.0.0.1", 1000, now, now);
        announce(&state, &hash, "2001:470:1:2::1", 1001, now, now);

        assert_eq!(
            state.swarm_peers(&hash, Family::V4),
            vec!["10.0.0.1:1000".parse().unwrap()]
        );
        assert_eq!(
            state.swarm_peers(&hash, Family::V6),
            vec!["[2001:470:1:2::1]:1001".parse().unwrap()]
        );
        assert_eq!(state.stored_peers(&hash).len(), 2);
    }
}
