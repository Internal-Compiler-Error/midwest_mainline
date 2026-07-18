//! State shared by the DHT's two halves: who we are, who we know, what we store, and how
//! we talk to the network. The server answers queries with it, the client looks things
//! up with it; neither owns any of it alone.

use diesel::r2d2::{ConnectionManager, Pool};
use diesel::{SqliteConnection, prelude::*};
use rand::Rng;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

use crate::dht::krpc_broker::KrpcBroker;
use crate::dht::router::Router;
use crate::schema::{peer, swarm};
use crate::token_generator::TokenGenerator;
use crate::types::{InfoHash, NodeId};
use crate::utils::unix_timestmap_ms;

// TODO: make these configurable some day
pub const REQ_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug)]
pub(crate) struct SharedState {
    pub(crate) our_id: NodeId,
    pub(crate) router: Router,
    pub(crate) conn: Pool<ConnectionManager<SqliteConnection>>,
    pub(crate) token_generator: TokenGenerator,
    pub(crate) message_broker: KrpcBroker,
}

impl SharedState {
    pub(crate) fn new(
        our_id: NodeId,
        router: Router,
        message_broker: KrpcBroker,
        conn: Pool<ConnectionManager<SqliteConnection>>,
    ) -> Self {
        Self {
            our_id,
            router,
            conn,
            token_generator: TokenGenerator::new(rand::rng().random()),
            message_broker,
        }
    }

    /// Peers for `info_hash` that were announced *to us*, non-expired per BEP 5's
    /// suggested 45-minute lifetime, capped so a get_peers response fits a datagram.
    pub(crate) fn swarm_peers(&self, info_hash: &InfoHash) -> Vec<SocketAddrV4> {
        fn cutoff() -> i64 {
            let forty_five_minutes_ms = 45 * 60 * 1000;
            unix_timestmap_ms() - forty_five_minutes_ms
        }

        let mut conn = self.conn.get().expect("failed to get one connection from pool");

        let peers = peer::table
            .inner_join(swarm::table.on(peer::swarm.eq(swarm::info_hash)))
            .filter(swarm::info_hash.eq(&info_hash.0))
            .filter(peer::last_announced.ge(cutoff()))
            .select((peer::ip_addr, peer::port))
            .limit(50)
            .load::<(String, i32)>(&mut conn)
            .unwrap();
        let peers: Vec<SocketAddrV4> = peers
            .into_iter()
            .map(|(ip, port)| {
                assert!(port >= 0 && port <= u16::MAX.into(), "port should fit inside an u16");

                let ip: Ipv4Addr = ip.parse().expect(&format!(
                    "invalid ip string representation got into the database: {}",
                    ip
                ));
                SocketAddrV4::new(ip, port as u16)
            })
            .collect();
        peers
    }
}
