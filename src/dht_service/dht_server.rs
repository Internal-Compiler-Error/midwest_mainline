use crate::{
    dht_service::DhtServiceInnerV4,
    domain_knowledge::{CompactPeerContact, NodeId, ToConcatedNodeContact},
    message::{
        announce_peer_query::AnnouncePeerQuery, find_node_query::FindNodeQuery, get_peers_query::GetPeersQuery,
        ping_query::PingQuery, Krpc,
    },
};
use rand::{Rng, RngCore};
use serde::de::Unexpected::Bytes;
use sha3::{Digest, Sha3_256};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    mem::take,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc::Receiver, Mutex, RwLock},
    time::Instant,
};

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
        task.await;
    }

    /// Generate a new token if the address is not in the pool or expired, otherwise return the
    /// existing token.
    pub(crate) fn token_for_addr(&self, addr: &Ipv4Addr) -> Box<[u8]> {
        let mut assigned = self.assigned.blocking_lock();
        let mut entry = assigned.entry(*addr);

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
                let mut hasher = Sha3_256::new();
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

pub(crate) struct DHTServer {
    inner: Arc<DhtServiceInnerV4>,
    requests: Receiver<Krpc>,
    hash_table: Arc<RwLock<HashMap<[u8; 20], Vec<CompactPeerContact>>>>,
    token_pool: TokenPool,
}

impl DHTServer {
    pub(crate) fn new(inner: Arc<DhtServiceInnerV4>, requests: Receiver<Krpc>) -> Self {
        Self {
            inner,
            requests,
            hash_table: Arc::new(RwLock::new(HashMap::new())),
            token_pool: TokenPool::new(),
        }
    }

    pub(crate) async fn handle_requests(&mut self) {
        while let Some(request) = self.requests.recv().await {}
    }

    fn generate_ping_response(&self, ping: PingQuery) -> Krpc {
        Krpc::new_ping_response(ping.transaction_id, self.inner.our_id)
    }

    async fn generate_find_node_response(&self, query: FindNodeQuery) -> Krpc {
        let table = self.inner.routing_table.read().await;
        let closest_eight: Vec<_> = table.find_closest(&query.body.target).into_iter().take(8).collect();

        // if we have an exact match, it will be the first element in the vector
        return if closest_eight[0].node_id() == &query.body.target {
            Krpc::new_find_node_response(
                query.transaction_id,
                self.inner.our_id,
                Box::new(closest_eight[0].node_id().clone()),
            )
        } else {
            let bytes = closest_eight.to_concated_node_contact();
            Krpc::new_find_node_response(query.transaction_id, self.inner.our_id, bytes)
        };
    }

    async fn generate_get_peers_response(&self, query: GetPeersQuery, origin: Ipv4Addr) -> Krpc {
        // see if know about the info hash
        let table = self.hash_table.read().await;
        let token_pool = &self.token_pool;

        return if let Some(peers) = table.get(&query.body.info_hash) {
            let peers: Vec<_> = peers.iter().cloned().collect();

            let token = token_pool.token_for_addr(&origin);
            Krpc::new_get_peers_success_response(query.transaction_id, self.inner.our_id, token, peers)
        } else {
            let closest_eight: Vec<_> = self
                .inner
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

            Krpc::new_get_peers_deferred_response(query.transaction_id, self.inner.our_id, token, bytes)
        };
    }

    async fn generate_announce_peer_response(&self, announce: AnnouncePeerQuery, origin: Ipv4Addr) -> Krpc {
        unimplemented!()
    }
}
