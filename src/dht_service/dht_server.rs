use crate::{
    domain_knowledge::{CompactPeerContact, NodeId, ToConcatedNodeContact},
    message::{
        announce_peer_query::AnnouncePeerQuery, find_node_query::FindNodeQuery, get_peers_query::GetPeersQuery,
        ping_query::PingQuery, Krpc,
    },
    routing::RoutingTable,
};
use rand::RngCore;

use crate::message::InfoHash;
use sha3::{Digest, Sha3_256};
use std::{
    collections::{hash_map::Entry, HashMap},
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc::Receiver, Mutex, RwLock},
    time::Instant,
};
use tracing::{error, trace};

#[derive(Debug)]
struct TokenPool {
    assigned: Arc<Mutex<HashMap<Ipv4Addr, (Box<[u8]>, Instant)>>>,
    salt: Arc<RwLock<[u8; 128]>>,
}

const TOKEN_EXPIRATION_TIME: Duration = Duration::from_secs(60 * 10);

impl TokenPool {
    pub(crate) fn new() -> Self {
        let salt = {
            let mut salt = [0u8; 128];
            rand::thread_rng().fill_bytes(&mut salt);
            salt
        };

        Self {
            assigned: Arc::new(Mutex::new(HashMap::new())),
            salt: Arc::new(RwLock::new(salt)),
        }
    }

    /// Event loop for the token pool. It will keep running until you drop the future, usually, you
    /// spawn a task for it and drop the join handle when you want to stop
    pub(crate) async fn run(self: Arc<Self>) {
        let new_salt_every_five_minutes = async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60 * 5)).await;
                let mut salt = self.salt.write().await;
                rand::thread_rng().fill_bytes(&mut *salt);
            }
        };

        let task = tokio::spawn(new_salt_every_five_minutes);
        let _ = task.await;
    }

    /// Generate a new token if the address is not in the pool or expired, otherwise return the
    /// existing token.
    pub(crate) fn token_for_addr(&self, addr: &Ipv4Addr) -> Box<[u8]> {
        let mut assigned = self.assigned.blocking_lock();
        let entry = assigned.entry(*addr);

        // we need to assign the ip a new token if the
        return match entry {
            // if we already have a token assigned to the address *and* the token hasn't expired,
            // return the existing token, otherwise generate a new token.
            Entry::Occupied(mut e) => {
                let (token, last_update) = e.get_mut();

                if last_update.elapsed() > TOKEN_EXPIRATION_TIME {
                    *token = self.generate_token(addr);
                    *last_update = Instant::now();
                }

                token.clone()
            }
            Entry::Vacant(v) => {
                let hasher = Sha3_256::new();
                let salt = self.salt.blocking_read();

                let token = self.generate_token(addr);
                let last_update = Instant::now();

                let (token, _) = v.insert((token, last_update));

                token.clone()
            }
        };
    }

    /// See as the moment of calling, is the token correct?
    pub(crate) fn is_valid_token(&self, addr: &Ipv4Addr, token: &[u8]) -> bool {
        let expected_token = self.generate_token(addr);
        &*expected_token == token
    }

    /// generate what the token for the address should be as this current moment
    fn generate_token(&self, addr: &Ipv4Addr) -> Box<[u8]> {
        let mut hasher = Sha3_256::new();
        let salt = self.salt.blocking_read();
        hasher.update(&*salt);
        hasher.update(addr.octets());

        let digest = hasher.finalize();

        Box::from(digest.as_slice())
    }
}

#[derive(Debug)]
pub(crate) struct DHTServer {
    routing_table: Arc<RwLock<RoutingTable>>,
    our_id: NodeId,
    requests: Mutex<Receiver<(Krpc, SocketAddrV4)>>,
    hash_table: Arc<RwLock<HashMap<InfoHash, Vec<CompactPeerContact>>>>,
    token_pool: TokenPool,
    socket: Arc<UdpSocket>,
}

impl DHTServer {
    pub(crate) fn new(
        requests: Receiver<(Krpc, SocketAddrV4)>,
        socket: Arc<UdpSocket>,
        id: NodeId,
        routing_table: Arc<RwLock<RoutingTable>>,
    ) -> Self {
        Self {
            requests: Mutex::new(requests),
            hash_table: Arc::new(RwLock::new(HashMap::new())),
            token_pool: TokenPool::new(),
            socket,
            our_id: id,
            routing_table,
        }
    }

    pub(crate) async fn handle_requests(self: Arc<Self>) {
        let mut requests = (&self).requests.lock().await;
        while let Some((request, socket_addr)) = requests.recv().await {
            trace!("Received request: {:?}", request);

            let server = self.clone();
            tokio::spawn(async move {
                let server = &*server;
                let response = server.generate_response(request, socket_addr).await;
                trace!("Handling request from {socket_addr}");

                if let Some(response) = response {
                    let serialized = bendy::serde::to_bytes(&response)?;
                    server.socket.send_to(&serialized, socket_addr).await?;
                    trace!("response sent for {socket_addr}");
                }

                Ok::<_, color_eyre::Report>(())
            });
        }
    }
    #[tracing::instrument]
    async fn generate_response(&self, request: Krpc, socket_addr: SocketAddrV4) -> Option<Krpc> {
        let response = match request {
            Krpc::PingQuery(ping) => Some(self.generate_ping_response(ping)),
            Krpc::FindNodeQuery(find_node) => Some(self.generate_find_node_response(find_node).await),
            Krpc::AnnouncePeerQuery(announce_peer) => {
                Some(self.generate_announce_peer_response(announce_peer, socket_addr).await)
            }
            Krpc::GetPeersQuery(get_peers) => {
                Some(self.generate_get_peers_response(get_peers, *socket_addr.ip()).await)
            }
            _ => {
                // TODO: implement Value for Krpc so we can use structured logging
                error!("unexpected message in the server response queue, {request:?}");
                None
            }
        };
        response
    }

    #[tracing::instrument]
    fn generate_ping_response(&self, ping: PingQuery) -> Krpc {
        Krpc::new_ping_response(ping.transaction_id, self.our_id)
    }

    #[tracing::instrument]
    async fn generate_find_node_response(&self, query: FindNodeQuery) -> Krpc {
        let table = self.routing_table.read().await;
        let closest_eight: Vec<_> = table.find_closest(&query.body.target).into_iter().take(8).collect();

        // if we have an exact match, it will be the first element in the vector
        return if closest_eight[0].node_id() == &query.body.target {
            Krpc::new_find_node_response(
                query.transaction_id,
                self.our_id,
                Box::new(closest_eight[0].node_id().clone()),
            )
        } else {
            let bytes = closest_eight.to_concated_node_contact();
            Krpc::new_find_node_response(query.transaction_id, self.our_id, bytes)
        };
    }

    #[tracing::instrument]
    async fn generate_get_peers_response(&self, query: GetPeersQuery, origin: Ipv4Addr) -> Krpc {
        // see if know about the info hash
        let table = self.hash_table.read().await;
        let token_pool = &self.token_pool;

        return if let Some(peers) = table.get(&query.body.info_hash) {
            let peers: Vec<_> = peers.iter().cloned().collect();

            let token = token_pool.token_for_addr(&origin);
            Krpc::new_get_peers_success_response(query.transaction_id, self.our_id, token, peers)
        } else {
            let closest_eight: Vec<_> = self
                .routing_table
                .read()
                .await
                .find_closest(&query.body.info_hash)
                .into_iter()
                .take(8)
                .cloned()
                .collect();

            let token = token_pool.token_for_addr(&origin);
            let bytes = closest_eight.to_concated_node_contact();

            Krpc::new_get_peers_deferred_response(query.transaction_id, self.our_id, token, bytes)
        };
    }

    #[tracing::instrument]
    async fn generate_announce_peer_response(&self, announce: AnnouncePeerQuery, origin: SocketAddrV4) -> Krpc {
        // see if the token is valid
        if !self.token_pool.is_valid_token(&origin.ip(), &announce.body.token) {
            return Krpc::new_standard_protocol_error(announce.transaction_id);
        }

        // generate the correct peer contact according to the implied port argument, the port
        // argument is ignored if the implied port is not 0 and we use the origin port instead
        let peer_contact = {
            if announce.body.implied_port == 0 {
                CompactPeerContact::from(SocketAddrV4::new(*origin.ip(), announce.body.port))
            } else {
                CompactPeerContact::from(origin)
            }
        };

        // add the peer contact to the hash table, if it already exists, we don't care
        let mut table = self.hash_table.write().await;
        table
            .entry(announce.body.info_hash)
            .or_insert_with(Vec::new)
            .push(peer_contact);

        Krpc::new_announce_peer_response(announce.transaction_id, self.our_id)
    }
}
