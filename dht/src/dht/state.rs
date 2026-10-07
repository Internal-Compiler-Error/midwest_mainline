//! State shared by the DHT's two halves: who we are, who we know, what we store, and how
//! we talk to the network. The server answers queries with it, the client looks things
//! up with it; neither owns any of it alone.

use diesel::r2d2::{ConnectionManager, Pool};
use diesel::{SqliteConnection, prelude::*};
use rand::RngExt;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

use crate::dht::routing_table::RoutingTable;
use crate::dht::rpc_manager::RpcManager;
use crate::schema::{peer, swarm};
use crate::token_generator::TokenGenerator;
use crate::types::{InfoHash, NodeId};
use crate::utils::unix_timestmap_ms;

// TODO: make these configurable some day
/// How long to wait for a node to answer. Nodes that answer at all do so within a second
/// or two; a dead one held up an entire lookup round when this was 15 s.
pub const REQ_TIMEOUT: Duration = Duration::from_secs(3);

/// BEP 5's suggested lifetime of an announcement. Only peers announced within it are handed
/// out in get_peers responses, whatever the [`Retention`].
pub const PEER_LIFETIME: Duration = Duration::from_secs(45 * 60);

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
    pub addr: SocketAddrV4,
    pub first_announced: i64,
    pub last_announced: i64,
}

fn lifetime_cutoff() -> i64 {
    unix_timestmap_ms() - PEER_LIFETIME.as_millis() as i64
}

fn parse_addr(ip: &str, port: i32) -> SocketAddrV4 {
    assert!(port >= 0 && port <= u16::MAX.into(), "port should fit inside an u16");
    let ip: Ipv4Addr = ip
        .parse()
        .unwrap_or_else(|_| panic!("invalid ip string representation got into the database: {}", ip));
    SocketAddrV4::new(ip, port as u16)
}

#[derive(Debug)]
pub(crate) struct SharedState {
    pub(crate) our_id: NodeId,
    pub(crate) routing_table: RoutingTable,
    pub(crate) conn: Pool<ConnectionManager<SqliteConnection>>,
    pub(crate) token_generator: TokenGenerator,
    pub(crate) rpc_manager: RpcManager,
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
            routing_table,
            conn,
            token_generator: TokenGenerator::new(rand::rng().random()),
            rpc_manager,
        }
    }

    /// Peers for `info_hash` that were announced *to us* within [`PEER_LIFETIME`], freshest
    /// first, capped so a get_peers response fits a datagram.
    pub(crate) fn swarm_peers(&self, info_hash: &InfoHash) -> Vec<SocketAddrV4> {
        let mut conn = self.conn.get().expect("failed to get one connection from pool");

        peer::table
            .filter(peer::swarm.eq(&info_hash.0))
            .filter(peer::last_announced.ge(lifetime_cutoff()))
            .order(peer::last_announced.desc())
            .select((peer::ip_addr, peer::port))
            .limit(50)
            .load::<(String, i32)>(&mut conn)
            .unwrap()
            .into_iter()
            .map(|(ip, port)| parse_addr(&ip, port))
            .collect()
    }

    /// Every peer ever announced to us for `info_hash` that the store still holds, stale or
    /// not, most recently announced first.
    pub(crate) fn stored_peers(&self, info_hash: &InfoHash) -> Vec<StoredPeer> {
        let mut conn = self.conn.get().expect("failed to get one connection from pool");

        peer::table
            .filter(peer::swarm.eq(&info_hash.0))
            .order(peer::last_announced.desc())
            .select((peer::ip_addr, peer::port, peer::first_announced, peer::last_announced))
            .load::<(String, i32, i64, i64)>(&mut conn)
            .unwrap()
            .into_iter()
            .map(|(ip, port, first_announced, last_announced)| StoredPeer {
                addr: parse_addr(&ip, port),
                first_announced,
                last_announced,
            })
            .collect()
    }

    /// Info hashes the store holds at least one peer for.
    pub(crate) fn stored_swarms(&self) -> Vec<InfoHash> {
        let mut conn = self.conn.get().expect("failed to get one connection from pool");

        peer::table
            .select(peer::swarm)
            .distinct()
            .load::<Vec<u8>>(&mut conn)
            .unwrap()
            .iter()
            .filter_map(|bytes| InfoHash::try_from_bytes(bytes))
            .collect()
    }

    /// Deletes peers announced longer than [`PEER_LIFETIME`] ago, and swarms left with none.
    pub(crate) fn expire_peers(&self) -> Result<(), diesel::result::Error> {
        let mut conn = self.conn.get().expect("failed to get one connection from pool");

        conn.transaction(|conn| {
            diesel::delete(peer::table.filter(peer::last_announced.lt(lifetime_cutoff()))).execute(conn)?;
            diesel::delete(swarm::table.filter(diesel::dsl::not(diesel::dsl::exists(
                peer::table.filter(peer::swarm.eq(swarm::info_hash)),
            ))))
            .execute(conn)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::rpc_manager::RpcManager;
    use crate::dht::txn_id_generator::TxnIdGenerator;
    use crate::test_support::memory_pool;
    use std::sync::Arc;
    use tokio::net::UdpSocket;

    async fn state() -> SharedState {
        let pool = memory_pool();
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let rpc = RpcManager::new(socket, pool.clone(), Arc::new(TxnIdGenerator::new()), None);
        let id = NodeId([7; 20]);
        SharedState::new(id, RoutingTable::new(id, rpc.clone(), pool.clone()), rpc, pool)
    }

    fn announce(state: &SharedState, info_hash: &InfoHash, port: u16, first: i64, last: i64) {
        let mut conn = state.conn.get().unwrap();
        diesel::insert_into(swarm::table)
            .values(swarm::info_hash.eq(info_hash.0.to_vec()))
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .unwrap();
        diesel::insert_into(peer::table)
            .values((
                peer::ip_addr.eq("10.0.0.1"),
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
        announce(&state, &fresh_hash, 1000, two_hours_ago, now);
        announce(&state, &fresh_hash, 1001, two_hours_ago, two_hours_ago);
        announce(&state, &stale_hash, 1002, two_hours_ago, two_hours_ago);

        let fresh = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 1000);
        assert_eq!(state.swarm_peers(&fresh_hash), vec![fresh]);
        assert!(state.swarm_peers(&stale_hash).is_empty());
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
}
