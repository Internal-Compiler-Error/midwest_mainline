//! The server half of the DHT: answers the queries other nodes send us, using the shared
//! state. Runs as its own task fed by the broker's inbound queue.

use std::net::SocketAddr;
use std::sync::Arc;

use tracing::{trace, warn};

use crate::dht::state::SharedState;
use crate::message::error::KrpcError;
use crate::message::find_node_get_peers_response::Builder as ResBuilder;
use crate::message::ping_announce_peer_response::PingAnnouncePeerResponse;
use crate::message::{
    Krpc, KrpcBody, Want, announce_peer_query::AnnouncePeerQuery, get_peers_query::GetPeersQuery,
    item_queries::PutQuery,
};
use crate::types::{Family, NodeId, NodeInfo};

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
        let mut inbox = self.state.rpc_manager.subscribe_inbound();
        while let Some((msg, from)) = inbox.recv().await {
            // BEP 43: a read-only node answers nothing
            if !msg.is_query() || self.state.rpc_manager.is_read_only() {
                continue;
            }
            // answering reads and writes the database, so it's off the async workers
            let this = self.clone();
            tokio::task::spawn_blocking(move || this.answer(msg, from));
        }
    }

    fn answer(&self, query: Krpc, from: SocketAddr) {
        trace!("answering {from}");
        let Some(querier) = query.node_id() else {
            return;
        };
        let response = self.respond(&query.body, from);
        self.state
            .rpc_manager
            .reply(response, &NodeInfo::new(querier, from), query.txn_id);
    }

    fn respond(&self, query: &KrpcBody, from: SocketAddr) -> KrpcBody {
        let us = self.state.our_id;
        let closest =
            |res, target, want| KrpcBody::FindNodeGetPeersResponse(self.with_closest(res, target, want).build());
        match query {
            KrpcBody::PingQuery(_) => self.ack(),
            KrpcBody::FindNodeQuery(q) => closest(ResBuilder::new(us), q.target(), q.want()),
            KrpcBody::GetPeersQuery(q) => self.get_peers(q, from),
            KrpcBody::AnnouncePeerQuery(q) => self.announce_peer(q, from),
            KrpcBody::SampleInfohashesQuery(q) => closest(
                ResBuilder::new(us).with_samples(self.state.sample()),
                q.target(),
                q.want(),
            ),
            KrpcBody::GetQuery(q) => {
                let token = self.state.token_generator.token_for_ip(&from.ip());
                let mut res = ResBuilder::new(us).with_token(token);
                if let Some(item) = self.state.stored_item(&q.target(), q.seq()) {
                    res = res.with_item(item);
                }
                closest(res, q.target(), q.want())
            }
            KrpcBody::PutQuery(q) => self.put(q, from),
            KrpcBody::PingAnnouncePeerResponse(_)
            | KrpcBody::FindNodeGetPeersResponse(_)
            | KrpcBody::ErrorResponse(_) => {
                unreachable!("only queries are answered")
            }
        }
    }

    /// The bare answer of ping, announce_peer and put
    fn ack(&self) -> KrpcBody {
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

    fn get_peers(&self, query: &GetPeersQuery, from: SocketAddr) -> KrpcBody {
        let info_hash = query.info_hash();
        let token = self.state.token_generator.token_for_ip(&from.ip());
        let mut res = ResBuilder::new(self.state.our_id).with_token(token);
        // BEP 33: filters only when we hold something for the hash
        if query.scrape()
            && let Some(filters) = self.state.scrape_filters(&info_hash)
        {
            res = res.with_scrape(filters);
        }
        let peers = self
            .state
            .swarm_peers_preferring(&info_hash, self.state.family, query.noseed());
        let res = if !peers.is_empty() {
            res.with_values(&peers)
        } else {
            // the querier walks on towards the hash from the nodes we know closest to it
            self.with_closest(res, NodeId(info_hash.0), query.want())
        };
        KrpcBody::FindNodeGetPeersResponse(res.build())
    }

    fn announce_peer(&self, announce: &AnnouncePeerQuery, from: SocketAddr) -> KrpcBody {
        // the token must have been issued to this IP address (BEP 5)
        if !self.state.token_generator.is_valid_token(&from.ip(), announce.token()) {
            return KrpcBody::ErrorResponse(KrpcError::new_protocol());
        }
        let peer = match announce.implied_port() {
            true => from,
            false => SocketAddr::new(from.ip(), announce.port()),
        };
        match self.state.store_peer(&announce.info_hash(), peer, announce.seed()) {
            Ok(()) => self.ack(),
            Err(e) => {
                warn!("couldn't store an announced peer: {e}");
                KrpcBody::ErrorResponse(KrpcError::new_server())
            }
        }
    }

    fn put(&self, put: &PutQuery, from: SocketAddr) -> KrpcBody {
        if !self.state.token_generator.is_valid_token(&from.ip(), put.token()) {
            return KrpcBody::ErrorResponse(KrpcError::new_protocol());
        }
        match self.state.store_item(put) {
            Ok(()) => self.ack(),
            Err(e) => KrpcBody::ErrorResponse(e),
        }
    }
}
