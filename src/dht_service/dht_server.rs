use crate::{
    dht_service::DhtServiceInnerV4,
    domain_knowledge::{CompactPeerContact, NodeId},
    message::{find_node_query::FindNodeQuery, get_peers_query::GetPeersQuery, ping_query::PingQuery, Krpc},
};
use rand::Rng;
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
    used: HashSet<[u8; 20]>,
    assigned: HashMap<Ipv4Addr, ([u8; 20], Instant)>,
    salt: String,
}

const TOKEN_EXPIRATION_TIME: Duration = Duration::from_secs(60 * 10);

impl TokenPool {
    fn new(salt: String) -> Self {
        Self {
            used: HashSet::new(),
            assigned: HashMap::new(),
            salt,
        }
    }

    /// Generate a new token if the address is not in the pool or expired, otherwise return the
    /// existing token.
    fn token_for_addr(&mut self, addr: &Ipv4Addr) -> [u8; 20] {
        let mut entry = self.assigned.entry(*addr);

        match entry {
            Entry::Occupied(mut e) => {
                let (token, expiry) = e.get_mut();
                if expiry.elapsed() > TOKEN_EXPIRATION_TIME {
                    *token = self.toke();
                    *expiry = Instant::now();
                }
            }
            Entry::Vacant(v) => {}
        }

        if let Entry::Occupied((token, expiry)) = self.assigned.entry(addr) {
            if expiry.elapsed() < TOKEN_EXPIRATION_TIME {
                // if the token is not expired, return it
                *expiry = Instant::now();
                return *token;
            } else {
                // otherwise we generate a new token
                let mut hasher = Sha3_256::new();
                hasher.update(self.salt.as_bytes());
                hasher.update(addr.octets());

                let token = hasher.finalize();
            }
        }
        let token = self.next_token();
        self.assigned.insert(addr.clone(), (token, Instant::now()));
        token
    }

    fn remove(&mut self, token: &[u8; 20]) {
        self.used.remove(token);
    }
}

pub(crate) struct DHTServer {
    inner: Arc<DhtServiceInnerV4>,
    requests: Receiver<Krpc>,
    hash_table: Arc<RwLock<HashMap<[u8; 20], Vec<CompactPeerContact>>>>,
    token_pool: Arc<Mutex<TokenPool>>,
}

impl DHTServer {
    pub(crate) fn new(inner: Arc<DhtServiceInnerV4>, requests: Receiver<Krpc>) -> Self {
        Self {
            inner,
            requests,
            hash_table: Arc::new(RwLock::new(HashMap::new())),
            token_pool: Arc::new(Mutex::new(TokenPool::new())),
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
            let bytes = {
                let inner = closest_eight
                    .into_iter()
                    .map(|contact| contact.bytes)
                    .flatten()
                    .collect::<Vec<_>>();
                inner.into_boxed_slice()
            };
            Krpc::new_find_node_response(query.transaction_id, self.inner.our_id, bytes)
        };
    }

    async fn generate_get_peers_response(&self, query: GetPeersQuery, origin: Ipv4Addr) -> Krpc {
        // see if know about the info hash
        let table = self.hash_table.read().await;
        let mut token_pool = self.token_pool.lock().await;

        if let Some(contacts) = table.get(&query.body.info_hash) {
            let contacts: Vec<_> = contacts.iter().cloned().collect();

            let token = token_pool.token_for_addr(&origin);
            return Krpc::new_get_peers_success_response(
                query.transaction_id,
                self.inner.our_id,
                Box::from(token),
                contacts,
            );
        }
        todo!()
    }
}
