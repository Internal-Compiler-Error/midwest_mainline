use crate::{
    domain_knowledge::{InfoHash, NodeId, NodeInfo, PeerContact, Token, TransactionId},
    message::{
        announce_peer_query::AnnouncePeerQuery, find_node_query::FindNodeQuery, get_peers_query::GetPeersQuery,
        ping_query::PingQuery, Krpc,
    },
    our_error::{naur, OurError},
    utils::ParSpawnAndAwait,
};
use rand::RngCore;

use crate::message::find_node_get_peers_response::Builder as ResBuilder;
use sha3::{Digest, Sha3_256};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, RwLock},
    task::Builder as TskBuilder,
    time::{timeout, Instant},
};
use tracing::{info, info_span, trace, warn, Instrument};

use super::{
    router::Router,
    transaction_id_pool::{self, TransactionIdPool},
    MessageBroker,
};
#[derive(Debug)]
struct TokenPool {
    assigned: Arc<Mutex<HashMap<Ipv4Addr, (Token, Instant)>>>,
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

        TskBuilder::new()
            .name("five minute salt")
            .spawn(new_salt_every_five_minutes)
            .unwrap();
    }

    /// Generate a new token if the address is not in the pool or expired, otherwise return the
    /// existing token.
    pub(crate) async fn token_for_addr(&self, addr: &Ipv4Addr) -> Token {
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
    pub(crate) async fn is_valid_token(&self, addr: &Ipv4Addr, token: &Token) -> bool {
        let expected_token = self.generate_token(addr).await;
        expected_token == *token
    }

    /// generate what the token for the address should be as this current moment
    async fn generate_token(&self, addr: &Ipv4Addr) -> Token {
        let mut hasher = Sha3_256::new();
        let salt = self.salt.read().await;
        hasher.update(&*salt);
        hasher.update(addr.octets());

        let digest = hasher.finalize();
        // String::from_utf8(digest.as_slice().to_vec()).unwrap()
        // Box::from(digest.as_slice())
        Token::from_bytes(digest.as_slice())
    }
}

// TODO: make these configurable some day
pub const REQ_TIMEOUT: Duration = Duration::from_secs(15);
const ROUNDS_LIMIT: i32 = 8;
const CONCURRENT_REQS: usize = 3;

#[derive(Debug)]
pub struct DhtHandle {
    pub(crate) our_id: NodeId,
    pub(crate) router: Router,

    // parts needed to respond to requests
    /// Records which bittorrent clients are last known to be downloading identified by the info
    /// hash.
    /// TODO: replace this with a db
    swarm_records: Arc<RwLock<HashMap<InfoHash, Vec<PeerContact>>>>,
    token_pool: Arc<TokenPool>,
    message_broker: MessageBroker,

    // parts needed to make requests
    pub(crate) transaction_id_pool: Arc<TransactionIdPool>,
}

impl DhtHandle {
    pub(crate) fn new(
        id: NodeId,
        router: Router,
        message_broker: MessageBroker,
        transaction_id_pool: Arc<TransactionIdPool>,
    ) -> Self {
        Self {
            swarm_records: Arc::new(RwLock::new(HashMap::new())),
            token_pool: Arc::new(TokenPool::new()),
            our_id: id,
            router,
            message_broker,
            transaction_id_pool,
        }
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn run(self: Arc<Self>) {
        TskBuilder::new()
            .name("token pool")
            .spawn(self.token_pool.clone().run())
            .unwrap();
        let mut rx = self.message_broker.subscribe_inbound();

        // respond to messages, as fast as possible
        while let Some((inbound_msg, socket_addr)) = rx.recv().await {
            let this = self.clone();
            let _ = TskBuilder::new().name(&*format!("responding to {socket_addr}")).spawn(
                async move {
                    let this = &*this;

                    this.add_to_routing_table(socket_addr, &inbound_msg);
                    let response = match this.generate_response(&inbound_msg, socket_addr).await {
                        Some(msg) => msg,
                        None => return,
                    };

                    trace!("Handling request from {socket_addr}");

                    this.message_broker.send_msg(response, socket_addr);
                    trace!("response sending for {socket_addr}");
                }
                .instrument(info_span!("handle_requests")),
            );
        }
    }

    /**************************************   SERVER SECTION   *********************************************/

    fn add_to_routing_table(&self, from: SocketAddrV4, message: &Krpc) {
        if let Krpc::ErrorResponse(_) = message {
            return;
        }

        let node_id = match message {
            Krpc::AnnouncePeerQuery(announce_peer_query) => *announce_peer_query.querier(),
            Krpc::FindNodeQuery(find_node_query) => find_node_query.querier(),
            Krpc::GetPeersQuery(get_peers_query) => *get_peers_query.querier(),
            Krpc::PingQuery(ping_query) => *ping_query.querier(),
            Krpc::PingAnnouncePeerResponse(ping_announce_peer_response) => *ping_announce_peer_response.target_id(),
            Krpc::FindNodeGetPeersResponse(find_node_get_peers_response) => *find_node_get_peers_response.queried(),
            Krpc::ErrorResponse(_) => unreachable!("errors should get early returned"),
        };

        self.router.add(node_id, from);
        info!("Add {from} with {:?} to the routing table", node_id);

        // if it's response from find_peers or get_nodes, they have additional info
        if let Krpc::FindNodeGetPeersResponse(res) = message {
            for node in res.nodes() {
                // TODO: this is a bit stupid as we destroy the structure just to copy but fix later
                self.router.add(node.id(), node.contact().0);
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn generate_response(&self, request: &Krpc, from: SocketAddrV4) -> Option<Krpc> {
        let response = match request {
            Krpc::PingQuery(ping) => Some(self.generate_ping_response(ping, from).await),
            Krpc::FindNodeQuery(find_node) => Some(self.generate_find_node_response(find_node, from).await),
            Krpc::AnnouncePeerQuery(announce_peer) => {
                Some(self.generate_announce_peer_response(announce_peer, from).await)
            }
            Krpc::GetPeersQuery(get_peers) => Some(self.generate_get_peers_response(get_peers, from).await),
            _ => {
                assert!(request.is_response());
                None
            }
        };
        response
    }

    #[tracing::instrument(skip(self))]
    async fn generate_ping_response(&self, ping: &PingQuery, origin: SocketAddrV4) -> Krpc {
        Krpc::new_ping_response(ping.txn_id().clone(), self.our_id.clone())
    }

    #[tracing::instrument(skip(self))]
    async fn generate_find_node_response(&self, query: &FindNodeQuery, origin: SocketAddrV4) -> Krpc {
        let table = &self.router;
        let closest_eight: Vec<_> = table.find_closest(query.target_id()).into_iter().collect();

        // if we have an exact match, it will be the first element in the vector
        return if closest_eight[0].id() == query.target_id() {
            let res = ResBuilder::new(query.txn_id().clone(), self.our_id)
                .with_node(closest_eight[0].clone())
                .build();
            Krpc::FindNodeGetPeersResponse(res)
        } else {
            let stupid: Vec<_> = closest_eight.into_iter().collect();
            let res = ResBuilder::new(query.txn_id().clone(), self.our_id)
                .with_nodes(&stupid)
                .build();
            Krpc::FindNodeGetPeersResponse(res)
        };
    }

    #[tracing::instrument(skip(self))]
    async fn generate_get_peers_response(&self, query: &GetPeersQuery, origin: SocketAddrV4) -> Krpc {
        // see if know about the info hash
        let table = self.swarm_records.read().await;
        let token_pool = &self.token_pool;

        return if let Some(peers) = table.get(query.info_hash()) {
            let peers: Vec<_> = peers.iter().cloned().collect();

            let token = token_pool.token_for_addr(origin.ip()).await;

            let res = ResBuilder::new(query.txn_id().clone(), self.our_id.clone())
                .with_token(token)
                .with_values(&peers)
                .build();
            Krpc::FindNodeGetPeersResponse(res)
        } else {
            let closest_eight: Vec<_> = self
                .router
                .find_closest(*query.querier()) // TODO: wtf, why are we finding via info_hash
                .into_iter()
                .collect();

            let token = token_pool.token_for_addr(&origin.ip()).await;

            // defferred

            let res = ResBuilder::new(query.txn_id().clone(), self.our_id.clone())
                .with_token(token)
                .with_nodes(&closest_eight)
                .build();
            Krpc::FindNodeGetPeersResponse(res)
        };
    }

    #[tracing::instrument(skip(self))]
    async fn generate_announce_peer_response(&self, announce: &AnnouncePeerQuery, origin: SocketAddrV4) -> Krpc {
        // see if the token is valid
        if !self.token_pool.is_valid_token(&origin.ip(), announce.token()).await {
            return Krpc::new_standard_protocol_error(announce.txn_id().clone());
        }

        // generate the correct peer contact according to the implied port argument, the port
        // argument is ignored if the implied port is not 0 and we use the origin port instead
        let peer_contact = {
            if !announce.implied_port() {
                PeerContact(SocketAddrV4::new(*origin.ip(), announce.port()))
            } else {
                PeerContact(origin)
            }
        };

        // add the peer contact to the hash table, if it already exists, we don't care
        let mut table = self.swarm_records.write().await;
        table
            .entry(announce.info_hash().clone())
            .or_insert_with(Vec::new)
            .push(peer_contact);

        Krpc::new_announce_peer_response(announce.txn_id().clone(), self.our_id.clone())
    }

    /**************************************   CLIENT SECTION   *********************************************/

    // TODO: need a function to send with timeout, unsubscribe and clean up when timeout expires

    /// Send a message out and await for a response.
    ///
    /// It does not alter the routing table, callers must decide what to do with the response.
    pub async fn send_and_wait(&self, message: Krpc, recipient: SocketAddrV4) -> Result<Krpc, OurError> {
        let rx = {
            let rx = self.message_broker.subscribe_one(message.transaction_id().clone());
            self.message_broker.send_msg(message.clone(), recipient);
            rx
        };
        let (response, _addr) = rx.await.unwrap();
        Ok(response)
    }

    pub async fn ping(&self, peer: SocketAddrV4) -> Result<NodeId, OurError> {
        let txn_id = self.transaction_id_pool.next();
        let ping_msg = Krpc::new_ping_query(TransactionId::from(txn_id), self.our_id);

        let response = self.send_and_wait(ping_msg, peer).await?;

        return if let Krpc::PingAnnouncePeerResponse(response) = response {
            Ok(*response.target_id())
        } else {
            warn!("Unexpected response to ping: {:?}", response);
            Err(naur!("Unexpected response to ping"))
        };
    }

    /// starting point of trying to find any nodes on the network
    pub async fn find_node(self: Arc<Self>, target: NodeId) -> Result<NodeInfo, OurError> {
        // if we already know the node, then no need for any network requests
        if let Some(node) = (&self).router.find_exact(target) {
            return Ok(node);
        }

        let mut queried: HashSet<NodeInfo> = HashSet::new();

        // find the closest nodes that we know
        let mut closest = self.router.find_closest(target);
        for node in closest.iter() {
            queried.insert(*node);
        }

        let mut round = 0;
        loop {
            if round == ROUNDS_LIMIT {
                return Err(naur!("Too many rounds of find node"));
            }
            round += 1;

            let returned_nodes = closest
                .iter()
                .map(|node| node.contact().0)
                .map(|ip| self.clone().send_find_nodes_rpc(ip, target))
                .collect::<Vec<_>>();

            let returned_nodes = returned_nodes.par_spawn_and_await().await?;

            // filter out the ones that resulted in failure, such as due to time out
            let returned_nodes: Vec<_> = returned_nodes
                .into_iter()
                .filter_map(|node| node.ok())
                .flatten()
                .collect();

            // it's possible that some of the nodes returned are actually the node we're looking for
            // so we check for that and return it if it's the case
            let target_node = returned_nodes.iter().find(|node| node.id() == target);
            if target_node.is_some() {
                return Result::Ok(*target_node.unwrap());
            }

            // if we don't have the node, then we find the alpha closest nodes and ask them in turn
            let mut sorted_by_distance: Vec<_> = returned_nodes
                .into_iter()
                .map(|node| {
                    let distance = node.id().dist(&self.our_id);
                    (node, distance)
                })
                .collect();
            sorted_by_distance.sort_unstable_by_key(|(_, distance)| distance.clone());

            closest.clear();
            for (node_info, _dist) in sorted_by_distance.iter().take(CONCURRENT_REQS) {
                if !queried.contains(node_info) {
                    closest.push(*node_info);
                }
            }
        }
    }

    // attempt to find the target node via a peer on this address
    async fn send_find_nodes_rpc(
        self: Arc<Self>,
        dest: SocketAddrV4,
        target: NodeId,
    ) -> Result<Vec<NodeInfo>, OurError> {
        // construct the message to query our friends
        let txn_id = self.transaction_id_pool.next();
        let query = Krpc::new_find_node_query(TransactionId::from(txn_id), self.our_id, target);

        // send the message and await for a response
        let time_out = REQ_TIMEOUT;
        // TODO: make this configurable or let parent handle timeout, wooo maybe we can configure this
        // based on ip geo location distance
        let response = timeout(time_out, self.send_and_wait(query, dest))
            .await
            .inspect_err(|_e| trace!("find_nodes for {:?} timed out", dest))
            ?  // timeout error
            ?; // send_and_wait error

        if let Krpc::FindNodeGetPeersResponse(find_node_response) = response {
            // TODO:: does the following actually handle the giant bitstring thing correctly?
            //
            // the nodes come back as one giant byte string, each 26 bytes is a node
            // we split them up and create a vector of them
            let mut nodes: Vec<_> = find_node_response.nodes().clone();

            // some clients will return duplicate nodes, so we remove them
            nodes.sort_unstable_by_key(|node| node.contact().0);
            nodes.dedup();

            Result::Ok(nodes)
        } else {
            Err(naur!("Did not get a find node response"))
        }
    }

    // TODO: API is broken, since we can't guarantee that the peer will exist or we can find them,
    // we should return a list of K closest nodes or the target itself if can be found
    pub async fn get_peers(self: Arc<Self>, info_hash: InfoHash) -> Result<(Token, Vec<PeerContact>), OurError> {
        let resonsible = NodeId(info_hash.0);
        //
        // if we already know the node, then no need for any network requests
        if let Some(node) = (&self).router.find_exact(resonsible) {
            let (token, _nodes, peers) = self.send_get_peers_rpc(node.contact().0, info_hash).await?;
            return Ok((
                token.expect("A node directly responsible for a piece would return a token"),
                peers,
            ));
        }

        let mut queried: HashSet<NodeInfo> = HashSet::new();

        // find the closest nodes that we know
        let mut closest = self.router.find_closest(resonsible);
        for node in closest.iter() {
            queried.insert(*node);
        }

        let mut round = 0;
        loop {
            if round == ROUNDS_LIMIT {
                return Err(naur!("Too many rounds of find node"));
            }
            round += 1;

            let returned_nodes = closest
                .iter()
                .map(|node| node.contact().0)
                .map(|ip| self.clone().send_get_peers_rpc(ip, info_hash))
                .collect::<Vec<_>>();

            let returned_nodes = returned_nodes.par_spawn_and_await().await?;

            // filter out the ones that resulted in failure, such as due to time out
            let responses: Vec<_> = returned_nodes.into_iter().filter_map(|res| res.ok()).collect();

            // if any of them has a list of peers already, we can stop
            let peers_list = responses.iter().find(|(_token, _nodes, peers)| !peers.is_empty());
            if peers_list.is_some() {
                let peers_list = peers_list.unwrap().clone();
                return Ok((
                    peers_list
                        .0
                        .expect("If they have a list of peers, they will provide a token"),
                    peers_list.2,
                ));
            }

            // if we don't have the node, then we find the alpha closest nodes and ask them in turn
            let mut sorted_by_distance: Vec<_> = responses
                .into_iter()
                .flat_map(|(_token, nodes, _peers)| {
                    let our_id = self.our_id;
                    let distances = nodes
                        .into_iter()
                        .map(move |node| (node, node.id().dist_big_unit(&our_id)));
                    distances
                })
                .collect();
            sorted_by_distance.sort_unstable_by_key(|(_, distance)| distance.clone());

            closest.clear();
            for (node_info, _dist) in sorted_by_distance.iter().take(CONCURRENT_REQS) {
                if !queried.contains(node_info) {
                    closest.push(*node_info);
                }
            }
        }
    }

    pub async fn announce_peers(
        self: Arc<Self>,
        recipient: SocketAddrV4,
        info_hash: InfoHash,
        port: Option<u16>,
        token: Token,
    ) -> Result<(), OurError> {
        let transaction_id = self.transaction_id_pool.next();

        let query = if let Some(port) = port {
            Krpc::new_announce_peer_query(
                TransactionId::from(transaction_id),
                info_hash,
                self.our_id.clone(),
                port,
                true,
                token,
            )
        } else {
            Krpc::new_announce_peer_query(
                TransactionId::from(transaction_id),
                info_hash,
                self.our_id.clone(),
                // TODO: hmm, this interesting
                // self.socket_address.port(),
                0, // obviously replace this later
                true,
                token,
            )
        };

        let response = self.send_and_wait(query, recipient).await?;

        return match response {
            Krpc::PingAnnouncePeerResponse(_) => Ok(()),
            Krpc::ErrorResponse(err) => Err(naur!(
                "node responded with an error to our announce peer request {err:?}"
            )),
            _ => Err(naur!("non-compliant response from DHT node")),
        };
    }

    #[tracing::instrument(skip(self))]
    async fn send_get_peers_rpc(
        self: Arc<Self>,
        dest: SocketAddrV4,
        info_hash: InfoHash,
    ) -> Result<(Option<Token>, Vec<NodeInfo>, Vec<PeerContact>), OurError> {
        trace!("Asking {:?} for peers", dest);
        // construct the message to query our friends
        let transaction_id = self.transaction_id_pool.next();

        let query = Krpc::new_get_peers_query(TransactionId::from(transaction_id), self.our_id.clone(), info_hash);

        // send the message and await for a response
        let time_out = REQ_TIMEOUT;
        let response = timeout(time_out, self.send_and_wait(query, dest))
            .await
            .inspect_err(|_e| {
                trace!("get_peers for {:?} timed out", dest);
            })
            ?  // timeout error
            ?; // send_and_wait related error

        return match response {
            Krpc::ErrorResponse(response) => {
                warn!("Got an error response to get peers: {:?}", response);
                return Err(naur!("Got an error response to get peers"));
            }
            Krpc::FindNodeGetPeersResponse(response) => {
                let token = response.token().cloned();

                let mut nodes = response.nodes().clone();
                nodes.sort_unstable_by_key(|node| node.contact().0);
                nodes.dedup();

                let mut values = response.values().clone();
                nodes.sort_unstable_by_key(|node| node.contact().0);
                values.dedup();

                Ok((token, nodes, values))
            }
            other => {
                warn!("Unexpected response to get peers: {:?}", other);
                Err(naur!("Unexpected response to get peers"))
            }
        };
    }
}
