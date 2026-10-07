//! The server half of the DHT: answers the queries other nodes send us (ping,
//! find_node, get_peers, announce_peer), using the shared state. Runs as its own task
//! fed by the broker's inbound queue.

use diesel::insert_into;
use diesel::r2d2::{ConnectionManager, PooledConnection};
use diesel::{SqliteConnection, prelude::*};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::Builder as TskBuilder;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument, error, info_span, trace, warn};

use crate::dht::state::SharedState;
use crate::message::error::KrpcError;
use crate::message::find_node_get_peers_response::Builder as ResBuilder;
use crate::message::ping_announce_peer_response::PingAnnouncePeerResponse;
use crate::message::{
    KrpcBody, Want, announce_peer_query::AnnouncePeerQuery, find_node_query::FindNodeQuery,
    get_peers_query::GetPeersQuery, ping_query::PingQuery,
};
use crate::schema::{peer, swarm};
use crate::types::{Family, InfoHash, NodeId, NodeInfo};
use crate::utils::unix_timestmap_ms;

#[derive(Debug, Clone)]
pub(crate) struct DhtServer {
    state: Arc<SharedState>,
}

impl DhtServer {
    pub(crate) fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn run(&self) {
        let rx = self.state.rpc_manager.subscribe_inbound();
        let rx = ReceiverStream::new(rx);
        let mut requests = rx.filter(|(msg, _)| !msg.is_error() && !msg.is_response());

        // respond to messages, as fast as possible
        while let Some((inbound_msg, socket_addr)) = requests.next().await {
            // BEP 43: a read-only node answers nothing
            if self.state.rpc_manager.is_read_only() {
                continue;
            }
            // the server is a cheap handle (one refcount on the shared state), so each
            // response task just gets its own clone
            let this = self.clone();
            let _ = TskBuilder::new().name(&format!("responding to {socket_addr}")).spawn(
                async move {
                    trace!("Handling request from {socket_addr}");
                    let response = this.generate_response(&inbound_msg.body, socket_addr);

                    let txn_id = inbound_msg.transaction_id().clone();
                    let node_info =
                        NodeInfo::new(inbound_msg.node_id().expect("non qeuries are filtered"), socket_addr);
                    this.state.rpc_manager.reply(response, &node_info, txn_id);
                    trace!("response sending for {socket_addr}");
                }
                .instrument(info_span!("handle_requests")),
            );
        }
    }

    #[tracing::instrument(skip(self))]
    fn generate_response(&self, request: &KrpcBody, from: SocketAddr) -> KrpcBody {
        assert!(request.is_query());

        match request {
            KrpcBody::PingQuery(ping) => self.generate_ping_response(ping, from),
            KrpcBody::FindNodeQuery(find_node) => self.generate_find_node_response(find_node, from),
            KrpcBody::AnnouncePeerQuery(announce_peer) => self.generate_announce_peer_response(announce_peer, from),
            KrpcBody::GetPeersQuery(get_peers) => self.generate_get_peers_response(get_peers, from),
            KrpcBody::SampleInfohashesQuery(query) => {
                let res = ResBuilder::new(self.state.our_id).with_samples(self.state.sample());
                KrpcBody::FindNodeGetPeersResponse(self.with_closest(res, query.target(), query.want()).build())
            }
            KrpcBody::GetQuery(query) => {
                let token = self.state.token_generator.token_for_ip(&from.ip());
                let mut res = ResBuilder::new(self.state.our_id).with_token(token);
                if let Some(item) = self.state.stored_item(&query.target(), query.seq()) {
                    res = res.with_item(item);
                }
                KrpcBody::FindNodeGetPeersResponse(self.with_closest(res, query.target(), query.want()).build())
            }
            KrpcBody::PutQuery(put) => {
                if !self.state.token_generator.is_valid_token(&from.ip(), put.token()) {
                    return KrpcBody::ErrorResponse(KrpcError::new_protocol());
                }
                match self.state.store_item(put) {
                    Ok(()) => KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(self.state.our_id)),
                    Err(e) => KrpcBody::ErrorResponse(e),
                }
            }
            _ => unreachable!("caught by assert"),
        }
    }

    #[tracing::instrument(skip(self))]
    fn generate_ping_response(&self, ping: &PingQuery, origin: SocketAddr) -> KrpcBody {
        KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(self.state.our_id))
    }

    /// The closest nodes to `target` under the keys BEP 32 asks for: `nodes` and/or `nodes6`
    /// as `want` lists them, or else the family the query arrived over. The other family's
    /// come from the paired node, if there is one. An exact match goes alone.
    fn with_closest(&self, mut res: ResBuilder, target: NodeId, want: Option<Want>) -> ResBuilder {
        let want = want.unwrap_or(Want::only(self.state.family));
        for family in [Family::V4, Family::V6] {
            if !want.includes(family) {
                continue;
            }
            let closest = self
                .state
                .table_of(family)
                .map(|table| table.find_closest(target))
                .unwrap_or_default();
            let closest = match closest.first() {
                Some(exact) if exact.id() == target => vec![*exact],
                _ => closest,
            };
            res = res.with_nodes_of(family, &closest);
        }
        res
    }

    #[tracing::instrument(skip(self))]
    fn generate_find_node_response(&self, query: &FindNodeQuery, origin: SocketAddr) -> KrpcBody {
        let res = self.with_closest(ResBuilder::new(self.state.our_id), query.target(), query.want());
        KrpcBody::FindNodeGetPeersResponse(res.build())
    }

    #[tracing::instrument(skip(self))]
    fn generate_get_peers_response(&self, query: &GetPeersQuery, origin: SocketAddr) -> KrpcBody {
        let peers = self
            .state
            .swarm_peers_preferring(&query.info_hash(), self.state.family, query.noseed());
        let token = self.state.token_generator.token_for_ip(&origin.ip());
        let mut res = ResBuilder::new(self.state.our_id).with_token(token);
        // BEP 33: filters only when we hold something for the hash
        if query.scrape()
            && let Some(filters) = self.state.scrape_filters(&query.info_hash())
        {
            res = res.with_scrape(filters);
        }

        let res = if !peers.is_empty() {
            res.with_values(&peers)
        } else {
            // when we don't have peer info on an info hash, respond with the closest nodes
            // we know *to that info hash* so the querier can iterate towards it
            self.with_closest(res, NodeId(query.info_hash().0), query.want())
        };
        KrpcBody::FindNodeGetPeersResponse(res.build())
    }

    #[tracing::instrument(skip(self))]
    fn generate_announce_peer_response(&self, announce: &AnnouncePeerQuery, origin: SocketAddr) -> KrpcBody {
        // the token must have been issued to this IP address (BEP 5)
        if !self
            .state
            .token_generator
            .is_valid_token(&origin.ip(), announce.token())
        {
            return KrpcBody::ErrorResponse(KrpcError::new_protocol());
        }

        // generate the correct peer contact according to the implied port argument, the port
        // argument is ignored if the implied port is not 0 and we use the origin port instead
        let peer_contact = {
            if !announce.implied_port() {
                SocketAddr::new(origin.ip(), announce.port())
            } else {
                origin
            }
        };

        let mut conn = self.state.conn.get().unwrap();
        let _ = Self::add_peers_to_db(&announce.info_hash(), peer_contact, announce.seed(), &mut conn)
            .inspect_err(|e| warn!("{e}"));

        KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(self.state.our_id))
    }

    pub(crate) fn add_peers_to_db(
        info_hash: &InfoHash,
        peer_contact: SocketAddr,
        seed: bool,
        conn: &mut PooledConnection<ConnectionManager<SqliteConnection>>,
    ) -> Result<usize, diesel::result::Error> {
        conn.transaction(|conn| {
            // the peer table references the swarm, so make sure it exists first
            let info_hash = info_hash.as_bytes().to_vec();
            let _ = insert_into(swarm::table)
                .values(swarm::info_hash.eq(&info_hash))
                .on_conflict_do_nothing()
                .execute(conn)
                .inspect_err(|e| error!("{e}"))?;

            let now = unix_timestmap_ms();
            insert_into(peer::table)
                .values(
                    // NOTE: this comes in the host native endianness, but it should be fine as long as the db
                    // file is not transferred between computers
                    (
                        peer::ip_addr.eq(peer_contact.ip().to_string()),
                        peer::port.eq(peer_contact.port() as i32),
                        peer::swarm.eq(info_hash),
                        peer::first_announced.eq(now),
                        peer::last_announced.eq(now),
                        peer::seed.eq(seed),
                    ),
                )
                .on_conflict((peer::ip_addr, peer::port, peer::swarm))
                .do_update()
                .set((peer::last_announced.eq(now), peer::seed.eq(seed)))
                .execute(conn)
                .inspect_err(|e| warn!("{e}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::memory_pool;
    use std::net::Ipv4Addr;

    #[test]
    fn announcing_again_keeps_when_the_peer_was_first_seen() {
        let pool = memory_pool();
        let mut conn = pool.get().unwrap();
        let info_hash = InfoHash([3; 20]);
        let addr = SocketAddr::from((Ipv4Addr::new(10, 0, 0, 2), 6881));

        DhtServer::add_peers_to_db(&info_hash, addr, false, &mut conn).unwrap();
        diesel::update(peer::table)
            .set((peer::first_announced.eq(1), peer::last_announced.eq(1)))
            .execute(&mut conn)
            .unwrap();
        DhtServer::add_peers_to_db(&info_hash, addr, false, &mut conn).unwrap();

        let (first, last): (i64, i64) = peer::table
            .select((peer::first_announced, peer::last_announced))
            .first(&mut conn)
            .unwrap();
        assert_eq!(first, 1);
        assert!(last > 1);
    }
}
