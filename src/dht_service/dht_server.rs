use crate::{
    domain_knowledge::{BetterCompactNodeInfo, BetterCompactPeerContact, BetterInfoHash, BetterNodeId},
    message::{
        announce_peer_query::BetterAnnouncePeerQuery, find_node_query::BetterFindNodeQuery, get_peers_query::BetterGetPeersQuery,
        ping_query::BetterPingQuery, Krpc, ToRawKrpc,
    },
    routing::RoutingTable,
};
use rand::RngCore;

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
    task::Builder,
    time::Instant,
};
use tracing::{error, info, info_span, trace, Instrument};

#[derive(Debug)]
struct TokenPool {
    // TODO: define a type for token
    assigned: Arc<Mutex<HashMap<Ipv4Addr, (String, Instant)>>>,
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

        // huh?
        let task = Builder::new()
            .name("five minute salt")
            .spawn(new_salt_every_five_minutes);
        // let _ = task.await;
    }

    /// Generate a new token if the address is not in the pool or expired, otherwise return the
    /// existing token.
    pub(crate) async fn token_for_addr(&self, addr: &Ipv4Addr) -> String {
        let mut assigned = self.assigned.lock().await;
        let entry = assigned.entry(*addr);

        // we need to assign the ip a new token if the
        return match entry {
            // if we already have a token assigned to the address *and* the token hasn't expired,
            // return the existing token, otherwise generate a new token.
            Entry::Occupied(mut e) => {
                let (token, last_update) = e.get_mut();

                if last_update.elapsed() > TOKEN_EXPIRATION_TIME {
                    *token = self.generate_token(addr).await;
                    *last_update = Instant::now();
                }

                token.clone()
            }
            Entry::Vacant(v) => {
                let token = self.generate_token(addr).await;
                let last_update = Instant::now();

                let (token, _) = v.insert((token, last_update));

                token.clone()
            }
        };
    }

    /// See as the moment of calling, is the token correct?
    pub(crate) async fn is_valid_token(&self, addr: &Ipv4Addr, token: &str) -> bool {
        let expected_token = self.generate_token(addr).await;
        &*expected_token == token
    }

    /// generate what the token for the address should be as this current moment
    async fn generate_token(&self, addr: &Ipv4Addr) -> String {
        let mut hasher = Sha3_256::new();
        let salt = self.salt.read().await;
        hasher.update(&*salt);
        hasher.update(addr.octets());

        let digest = hasher.finalize();
        String::from_utf8(digest.as_slice().to_vec()).unwrap()
        // Box::from(digest.as_slice())
    }
}

#[derive(Debug)]
pub struct DhtServer {
    routing_table: Arc<RwLock<RoutingTable>>,
    our_id: BetterNodeId,
    requests: Mutex<Receiver<(Krpc, SocketAddrV4)>>,
    hash_table: Arc<RwLock<HashMap<BetterInfoHash, Vec<BetterCompactPeerContact>>>>,
    token_pool: Arc<TokenPool>,
    socket: Arc<UdpSocket>,
}

impl DhtServer {
    pub(crate) fn new(
        requests: Receiver<(Krpc, SocketAddrV4)>,
        socket: Arc<UdpSocket>,
        id: BetterNodeId,
        routing_table: Arc<RwLock<RoutingTable>>,
    ) -> Self {
        Self {
            requests: Mutex::new(requests),
            hash_table: Arc::new(RwLock::new(HashMap::new())),
            token_pool: Arc::new(TokenPool::new()),
            socket,
            our_id: id,
            routing_table,
        }
    }

    #[tracing::instrument]
    pub(crate) async fn run(self: Arc<Self>) {
        Builder::new().name("token pool").spawn(self.token_pool.clone().run());

        let mut requests = (&self).requests.lock().await;
        while let Some((request, socket_addr)) = requests.recv().await {
            trace!("Received request: {:?}", request);

            let server = self.clone();

            Builder::new().name(&*format!("responding to {socket_addr}")).spawn(
                async move {
                    let server = &*server;
                    let response = server.generate_response(request, socket_addr).await;
                    trace!("Handling request from {socket_addr}");

                    if let Some(response) = response {
                        let serialized = response.to_raw_krpc();
                        server.socket.send_to(&serialized, socket_addr).await?;
                        trace!("response sent for {socket_addr}");
                    }

                    info!("table = {:#?}", server.hash_table.read().await.len());
                    Ok::<_, color_eyre::Report>(())
                }
                .instrument(info_span!("handle_requests")),
            );
        }
    }

    #[tracing::instrument]
    async fn generate_response(&self, request: Krpc, from: SocketAddrV4) -> Option<Krpc> {
        let response = match request {
            Krpc::PingQuery(ping) => Some(self.generate_ping_response(ping, from).await),
            Krpc::FindNodeQuery(find_node) => Some(self.generate_find_node_response(find_node, from).await),
            Krpc::AnnouncePeerQuery(announce_peer) => {
                Some(self.generate_announce_peer_response(announce_peer, from).await)
            }
            Krpc::GetPeersQuery(get_peers) => Some(self.generate_get_peers_response(get_peers, from).await),
            _ => {
                // TODO: implement Value for Krpc so we can use structured logging
                error!("unexpected message in the server response queue, {request:?}");
                None
            }
        };
        response
    }

    #[tracing::instrument]
    async fn generate_ping_response(&self, ping: BetterPingQuery, origin: SocketAddrV4) -> Krpc {
        // add the node into our routing table
        {
            let mut routing_table = self.routing_table.write().await;
            // TODO: use new
            let peer = BetterCompactNodeInfo {
                id: ping.target_id().clone(),
                contact: BetterCompactPeerContact(origin),
            };

            routing_table.add_new_node(peer);
        }

        Krpc::new_ping_response(ping.txn_id().to_string(), self.our_id.clone())
    }

    #[tracing::instrument]
    async fn generate_find_node_response(&self, query: BetterFindNodeQuery, origin: SocketAddrV4) -> Krpc {
        // add the node into our routing table
        {
            let mut routing_table = self.routing_table.write().await;
            let peer = BetterCompactNodeInfo {
                id: query.target_id().clone(),
                contact: BetterCompactPeerContact(origin),
            };
            routing_table.add_new_node(peer);
        }

        let table = self.routing_table.read().await;
        let closest_eight: Vec<_> = table.find_closest(query.target_id()).into_iter().collect();

        // if we have an exact match, it will be the first element in the vector
        return if &closest_eight[0].id == query.target_id() {
            Krpc::new_find_node_response(
                query.txn_id().to_string(),
                self.our_id.clone(),
                vec![closest_eight[0].clone()],
            )
        } else {
            // let bytes = closest_eight.to_concated_node_contact();
            Krpc::new_find_node_response(query.txn_id().to_string(), self.our_id.clone(), closest_eight.into_iter().cloned().collect())
        };
    }

    #[tracing::instrument]
    async fn generate_get_peers_response(&self, query: BetterGetPeersQuery, origin: SocketAddrV4) -> Krpc {
        // add the node into our routing table
        {
            let mut routing_table = self.routing_table.write().await;
            let peer = BetterCompactNodeInfo {
                id: query.our_id().clone(),
                contact: BetterCompactPeerContact(origin),
            };
            routing_table.add_new_node(peer);
        }

        // see if know about the info hash
        let table = self.hash_table.read().await;
        let token_pool = &self.token_pool;

        return if let Some(peers) = table.get(query.info_hash()) {
            let peers: Vec<_> = peers.iter().cloned().collect();

            let token = token_pool.token_for_addr(origin.ip()).await;
            Krpc::new_get_peers_success_response(query.txn_id().to_string(), self.our_id.clone(), token, peers)
        } else {
            let closest_eight: Vec<_> = self
                .routing_table
                .read()
                .await
                .find_closest(query.our_id())   // TODO: wtf, why are we finding via info_hash
                                                // before
                .into_iter()
                .cloned()
                .collect();

            let token = token_pool.token_for_addr(&origin.ip()).await;

            Krpc::new_get_peers_deferred_response(query.txn_id().to_string(), self.our_id.clone(), token, closest_eight)
        };
    }

    #[tracing::instrument]
    async fn generate_announce_peer_response(&self, announce: BetterAnnouncePeerQuery, origin: SocketAddrV4) -> Krpc {
        // see if the token is valid
        if !self.token_pool.is_valid_token(&origin.ip(), announce.token()).await {
            return Krpc::new_standard_protocol_error(announce.txn_id().to_string());
        }

        // add the node into our routing table
        {
            let mut routing_table = self.routing_table.write().await;
            let peer = BetterCompactNodeInfo {
                id: announce.querying().clone(),
                contact: BetterCompactPeerContact(origin),
            };
            routing_table.add_new_node(peer);
        }

        // generate the correct peer contact according to the implied port argument, the port
        // argument is ignored if the implied port is not 0 and we use the origin port instead
        let peer_contact = {
            if !announce.implied_port() {
                BetterCompactPeerContact(SocketAddrV4::new(*origin.ip(), announce.port()))
                // CompactPeerContact::from(SocketAddrV4::new(*origin.ip(), announce.port()))
            } else {
                BetterCompactPeerContact(origin)
                // CompactPeerContact::from(origin)
            }
        };

        // add the peer contact to the hash table, if it already exists, we don't care
        let mut table = self.hash_table.write().await;
        table
            .entry(announce.info_hash().clone())
            .or_insert_with(Vec::new)
            .push(peer_contact);

        Krpc::new_announce_peer_response(announce.txn_id().to_string(), self.our_id.clone())
    }
}
