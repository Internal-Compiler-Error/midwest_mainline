//! The server half of the DHT: answers the queries other nodes send us (ping,
//! find_node, get_peers, announce_peer), using the shared state. Runs as its own task
//! fed by the broker's inbound queue.

use diesel::insert_into;
use diesel::r2d2::{ConnectionManager, PooledConnection};
use diesel::{SqliteConnection, prelude::*};
use std::net::SocketAddrV4;
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
    KrpcBody, announce_peer_query::AnnouncePeerQuery, find_node_query::FindNodeQuery, get_peers_query::GetPeersQuery,
    ping_query::PingQuery,
};
use crate::schema::{peer, swarm};
use crate::types::{InfoHash, NodeId, NodeInfo};
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
    fn generate_response(&self, request: &KrpcBody, from: SocketAddrV4) -> KrpcBody {
        assert!(request.is_query());

        match request {
            KrpcBody::PingQuery(ping) => self.generate_ping_response(ping, from),
            KrpcBody::FindNodeQuery(find_node) => self.generate_find_node_response(find_node, from),
            KrpcBody::AnnouncePeerQuery(announce_peer) => self.generate_announce_peer_response(announce_peer, from),
            KrpcBody::GetPeersQuery(get_peers) => self.generate_get_peers_response(get_peers, from),
            _ => unreachable!("caught by assert"),
        }
    }

    #[tracing::instrument(skip(self))]
    fn generate_ping_response(&self, ping: &PingQuery, origin: SocketAddrV4) -> KrpcBody {
        KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(self.state.our_id))
    }

    #[tracing::instrument(skip(self))]
    fn generate_find_node_response(&self, query: &FindNodeQuery, origin: SocketAddrV4) -> KrpcBody {
        let table = &self.state.routing_table;
        let closest_eight = table.find_closest(query.target_id());

        // if we have an exact match, it will be the first element in the vector
        let res = if closest_eight.first().is_some_and(|n| n.id() == query.target_id()) {
            ResBuilder::new(self.state.our_id).with_node(closest_eight[0]).build()
        } else {
            ResBuilder::new(self.state.our_id).with_nodes(&closest_eight).build()
        };
        KrpcBody::FindNodeGetPeersResponse(res)
    }

    #[tracing::instrument(skip(self))]
    fn generate_get_peers_response(&self, query: &GetPeersQuery, origin: SocketAddrV4) -> KrpcBody {
        let peers = self.state.swarm_peers(query.info_hash());
        let token_pool = &self.state.token_generator;

        let token = token_pool.token_for_ip(origin.ip());
        if !peers.is_empty() {
            let res = ResBuilder::new(self.state.our_id)
                .with_token(token)
                .with_values(&peers)
                .build();
            KrpcBody::FindNodeGetPeersResponse(res)
        } else {
            // when we don't have peer info on an info hash, respond with the closest nodes
            // we know *to that info hash* so the querier can iterate towards it
            let target = NodeId(query.info_hash().0);
            let closest_eight: Vec<_> = self.state.routing_table.find_closest(target).into_iter().collect();

            let res = ResBuilder::new(self.state.our_id)
                .with_token(token)
                .with_nodes(&closest_eight)
                .build();
            KrpcBody::FindNodeGetPeersResponse(res)
        }
    }

    #[tracing::instrument(skip(self))]
    fn generate_announce_peer_response(&self, announce: &AnnouncePeerQuery, origin: SocketAddrV4) -> KrpcBody {
        // the token must have been issued to this IP address (BEP 5)
        if !self.state.token_generator.is_valid_token(origin.ip(), announce.token()) {
            return KrpcBody::ErrorResponse(KrpcError::new_protocol());
        }

        // generate the correct peer contact according to the implied port argument, the port
        // argument is ignored if the implied port is not 0 and we use the origin port instead
        let peer_contact = {
            if !announce.implied_port() {
                SocketAddrV4::new(*origin.ip(), announce.port())
            } else {
                origin
            }
        };

        let mut conn = self.state.conn.get().unwrap();
        let _ = Self::add_peers_to_db(announce.info_hash(), peer_contact, &mut conn).inspect_err(|e| warn!("{e}"));

        KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(self.state.our_id))
    }

    fn add_peers_to_db(
        info_hash: &InfoHash,
        peer_contact: SocketAddrV4,
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
                        peer::last_announced.eq(now),
                    ),
                )
                .on_conflict((peer::ip_addr, peer::port, peer::swarm))
                .do_update()
                .set(peer::last_announced.eq(now))
                .execute(conn)
                .inspect_err(|e| warn!("{e}"))
        })
    }
}
