use std::{
    collections::HashMap,
    io,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    time::Duration,
};

use diesel::{
    SqliteConnection,
    r2d2::{ConnectionManager, Pool},
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tracing::{debug, info, instrument, trace, warn};

use crate::{
    message::{Krpc, KrpcBody, error::KrpcError},
    our_error::{OurError, naur},
    types::{Family, NodeInfo, TransactionId},
    utils::db_put,
};

use super::{TxnIdGenerator, external_ip::ExternalIp, scope::Scope};

/// BEP 5's `v`, which every message we send carries
const CLIENT_VERSION: &[u8] = b"MW01";

/// A message and who sent it
pub type Inbound = (Krpc, SocketAddr);
/// Who a query went to, and who is waiting for the answer
type Pending = (SocketAddr, oneshot::Sender<Inbound>);

/// A message broker keeps reading Krpc messages from a queue and place them either into the
/// server response queue when we haven't seen this transaction id before, or into a oneshot channel
/// so the client and await the response.
#[derive(Debug, Clone)]
pub struct RpcManager {
    /// a map to keep track of the responses we await from the client; the address is the
    /// endpoint we queried, so responses from anywhere else are ignored
    pending_responses: Arc<Mutex<HashMap<TransactionId, Pending>>>,

    socket: Arc<UdpSocket>,
    /// the socket's address family, which is the family of everything this broker talks to
    family: Family,
    txn_id_generator: Arc<TxnIdGenerator>,

    /// a SPMC-esque queue, each readers can progress indepednelty
    inbound_subscribers: Arc<Mutex<Vec<mpsc::Sender<Inbound>>>>,
    db: Pool<ConnectionManager<SqliteConnection>>,
    external_ip: Arc<ExternalIp>,
    /// where in the database the node behind this socket keeps its own things
    scope: Scope,
    /// BEP 43: we answer no queries, and say so in ours
    read_only: Arc<AtomicBool>,
}

impl RpcManager {
    /// `external_ip` is the address other nodes reported for us last time, if any. The
    /// socket's family (IPv4, or IPv6 — bound IPv6-only) is the family of the DHT it serves.
    pub fn new(
        socket: UdpSocket,
        db: Pool<ConnectionManager<SqliteConnection>>,
        txn_id_generator: Arc<TxnIdGenerator>,
        external_ip: Option<IpAddr>,
    ) -> RpcManager {
        let family = Family::of(&socket.local_addr().expect("the socket should be bound already"));
        Self {
            pending_responses: Arc::new(Mutex::new(HashMap::new())),
            socket: Arc::new(socket),
            family,
            inbound_subscribers: Arc::new(Mutex::new(vec![])),
            db,
            txn_id_generator,
            external_ip: Arc::new(ExternalIp::new(external_ip)),
            scope: Scope::primary(family),
            read_only: Arc::default(),
        }
    }

    pub(crate) fn set_read_only(&self, read_only: bool) {
        self.read_only.store(read_only, Relaxed);
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.read_only.load(Relaxed)
    }

    /// The broker of a node in `scope` rather than its family's (BEP 45)
    pub(crate) fn with_scope(mut self, scope: Scope) -> Self {
        self.scope = scope;
        self
    }

    pub(crate) fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn family(&self) -> Family {
        self.family
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Reads the socket until dropped: the socket lives as long as this future or a clone of
    /// the broker does
    #[instrument(skip_all, fields(family = %self.family))]
    pub async fn run(&self) {
        let mut buf = [0u8; 1500];

        loop {
            // recv_from can fail transiently (e.g. ICMP port-unreachable from an
            // earlier send on macOS/BSD); that must not kill the broker loop
            let (amount, socket_addr) = match self.socket.recv_from(&mut buf).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("udp recv_from failed: {e}");
                    continue;
                }
            };
            trace!("received packet from {socket_addr}");
            match Krpc::decode(&buf[..amount]) {
                Ok(msg) => {
                    trace!("{} sent {:?}", socket_addr, msg);
                    self.fan_out(&msg, socket_addr);
                    // a query is never the answer to one of ours, whatever its transaction id
                    if msg.is_query() {
                        continue;
                    }
                    let Some(waiting) = self.take_pending(msg.transaction_id(), socket_addr) else {
                        continue;
                    };
                    // only answers to our own queries get a say in what our external address
                    // is; anyone can send a query
                    if let Some(seen) = msg.ip {
                        self.record_external_ip(socket_addr.ip(), seen.ip());
                    }
                    // failing means the receiver has dropped, meaning they are no longer
                    // interested in the message, not a bug
                    let _ = waiting.send((msg, socket_addr));
                }
                // BEP 5: unknown query methods get a 204 Method Unknown error reply, unless
                // we're read-only (BEP 43) and answer nothing
                Err(OurError::UnsupportedQuery(txn)) if !self.is_read_only() => {
                    let response = Krpc::new(txn, KrpcBody::ErrorResponse(KrpcError::new_method_unknown()));
                    self.send_msg_background(response, socket_addr);
                }
                Err(e) => {
                    tracing::debug!("ignoring an unparseable packet from {socket_addr}: {e}")
                }
            }
        }
    }

    /// Hands `msg` to every subscriber of the inbound queue. A subscriber that lags loses the
    /// message rather than stall the broker.
    fn fan_out(&self, msg: &Krpc, from: SocketAddr) {
        let mut subscribers = self.inbound_subscribers.lock().unwrap();
        subscribers.retain(|s| !s.is_closed());
        for sub in &*subscribers {
            if sub.try_send((msg.clone(), from)).is_err() {
                warn!("inbound subscriber lagging, dropping a message for it");
            }
        }
    }

    /// Who waits for the answer to transaction `id`, if it was sent to `from`
    fn take_pending(&self, id: &TransactionId, from: SocketAddr) -> Option<oneshot::Sender<Inbound>> {
        let mut pending = self.pending_responses.lock().unwrap();
        let (expected, _) = pending.get(id)?;
        if *expected != from {
            // the genuine answer may still come
            debug!("an answer for a query to {expected} came from {from}");
            return None;
        }
        pending.remove(id).map(|(_, waiting)| waiting)
    }

    /// Subscribe to the reply with the provided transaction_id, expected from `endpoint`
    pub fn subscribe_one(&self, transaction_id: TransactionId, endpoint: SocketAddr) -> oneshot::Receiver<Inbound> {
        let (tx, rx) = oneshot::channel();

        let mut guard = self.pending_responses.lock().unwrap();
        // it's possible that the response never came and we a new request is now using the same
        // transaction id
        let _ = guard.insert(transaction_id, (endpoint, tx));
        rx
    }

    /// A node answering our query told us the address it sees us at (BEP 42). Once enough
    /// agree, the address is stored for the next start to derive the node id from.
    fn record_external_ip(&self, voter: IpAddr, seen: IpAddr) {
        if Family::of_ip(&seen) != self.family {
            return;
        }
        let Some(agreed) = self.external_ip.vote(voter, seen) else {
            return;
        };
        info!("other nodes see us at {agreed}; the node id follows it at the next start");
        let db = self.db.clone();
        let key = self.scope.key(super::OBSERVED_IP_KEY);
        tokio::task::spawn_blocking(move || {
            let mut conn = match db.get() {
                Ok(conn) => conn,
                Err(e) => {
                    warn!("could not check out a db connection to store the external address: {e}");
                    return;
                }
            };
            // db_put logs its own failures
            let _ = db_put(key, agreed.to_string(), &mut conn);
        });
    }

    fn encode_for(&self, mut msg: Krpc, peer: SocketAddr) -> Box<[u8]> {
        if msg.body.is_query() {
            // BEP 43: so nobody puts us in a routing table only to find we don't answer
            msg.read_only = self.is_read_only();
        } else {
            // BEP 42: a response tells the querier the external address we see for it
            msg.ip = Some(peer);
        }
        msg.encode_with_version(CLIENT_VERSION)
    }

    /// Sends `msg` from a task of its own
    fn send_msg_background(&self, msg: Krpc, peer: SocketAddr) {
        let socket = self.socket.clone();
        let buf = self.encode_for(msg, peer);

        tokio::spawn(async move {
            if let Err(e) = socket.send_to(&buf, peer).await {
                warn!("failed to send message to {peer}: {e}");
            }
        });
    }

    /// Send a message out and await for a response. A send that fails outright (no route
    /// to the address, typically IPv6 on a host without it) is an [`OurError::IoError`] at
    /// once, rather than a timeout later.
    async fn send_and_wait(&self, message: Krpc, endpoint: SocketAddr) -> Result<Krpc, OurError> {
        let rx = self.subscribe_one(message.transaction_id().clone(), endpoint);
        self.socket
            .send_to(&self.encode_for(message, endpoint), endpoint)
            .await?;
        let (response, _addr) = rx
            .await
            .map_err(|_| naur!("pending request superseded or dropped before a response arrived"))?;

        match response.body {
            KrpcBody::ErrorResponse(e) => Err(OurError::Remote(e)),
            _ => Ok(response),
        }
    }

    async fn send_and_wait_timeout(
        &self,
        message: Krpc,
        endpoint: SocketAddr,
        time_out: Duration,
    ) -> Result<Krpc, OurError> {
        let txn_id = message.transaction_id().clone();
        let result = timeout(time_out, self.send_and_wait(message, endpoint)).await;
        if !matches!(result, Ok(Ok(_))) {
            // timed out or failed: free the pending slot so dead endpoints don't leak entries
            self.pending_responses.lock().unwrap().remove(&txn_id);
        }
        let response = result??;
        Ok(response)
    }

    pub fn subscribe_inbound(&self) -> mpsc::Receiver<Inbound> {
        let (tx, rx) = mpsc::channel(1024);
        let mut subscribers = self.inbound_subscribers.lock().unwrap();
        subscribers.push(tx);
        rx
    }

    /// Send a response to a query. Fire-and-forget: responses to responses are not a
    /// thing in KRPC, so there is nothing to wait for (and waiting leaked a task per
    /// inbound query).
    pub fn reply(&self, body: KrpcBody, node: &NodeInfo, txn_id: TransactionId) {
        let message = Krpc::new(txn_id, body);
        self.send_msg_background(message, node.end_point());
    }

    pub async fn query(
        &self,
        body: KrpcBody,
        endpoint: impl Into<SocketAddr>,
        timeout: Duration,
    ) -> Result<Krpc, OurError> {
        let endpoint = endpoint.into();
        let message = Krpc::new(self.txn_id_generator.next().into(), body);

        self.send_and_wait_timeout(message, endpoint, timeout).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::txn_id_generator::TxnIdGenerator;
    use crate::message::ping_announce_peer_response::PingAnnouncePeerResponse;
    use crate::message::ping_query::PingQuery;
    use crate::test_support::memory_pool;
    use crate::types::NodeId;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn spawn_run(broker: &RpcManager) {
        let broker = broker.clone();
        tokio::spawn(async move { broker.run().await });
    }

    async fn test_broker() -> RpcManager {
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        RpcManager::new(socket, memory_pool(), Arc::new(TxnIdGenerator::new()), None)
    }

    #[tokio::test]
    async fn reply_does_not_wait_for_a_response() {
        let broker = test_broker().await;
        let node = NodeInfo::new(NodeId([1u8; 20]), SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9));
        let body = KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(NodeId([2u8; 20])));

        // nothing listens on the other end, and a response never gets one anyway: this
        // must return immediately instead of waiting forever
        timeout(Duration::from_secs(1), async {
            broker.reply(body, &node, TransactionId::from_bytes(&[7]));
        })
        .await
        .expect("reply must not wait for a response to the response");
    }

    #[tokio::test]
    async fn timed_out_query_frees_its_pending_slot() {
        let broker = test_broker().await;
        let body = KrpcBody::PingQuery(PingQuery::new(NodeId([1u8; 20])));
        let dead = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9);

        let result = broker.query(body.clone(), dead, Duration::from_millis(50)).await;
        assert!(result.is_err());
        assert!(
            broker.pending_responses.lock().unwrap().is_empty(),
            "a timed-out query must not leak its pending_requests entry"
        );

        // an IPv6 address on an IPv4 socket can't be sent to at all: an error at once, no
        // waiting out the timeout
        let unroutable: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
        let result = timeout(
            Duration::from_secs(1),
            broker.query(body, unroutable, Duration::from_secs(30)),
        )
        .await
        .expect("a failed send must not wait for the timeout");
        assert!(matches!(result, Err(OurError::IoError(_))));
        assert!(
            broker.pending_responses.lock().unwrap().is_empty(),
            "a timed-out query must not leak its pending_requests entry"
        );
    }

    #[tokio::test]
    async fn unknown_query_method_gets_a_204_with_version_and_ip() {
        let broker = test_broker().await;
        spawn_run(&broker);
        let broker_addr = broker.socket.local_addr().unwrap();

        let us = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        // a query with a made-up method name
        us.send_to(
            b"d1:ad2:id20:0123456789abcdefghije1:q9:scrape_it1:t2:aa1:y1:qe",
            broker_addr,
        )
        .await
        .unwrap();

        let mut buf = [0u8; 1500];
        let (n, _) = timeout(Duration::from_secs(2), us.recv_from(&mut buf))
            .await
            .expect("no error reply received")
            .unwrap();
        let text = String::from_utf8_lossy(&buf[..n]);
        assert!(
            text.contains("i204e"),
            "expected a 204 Method Unknown error, got: {text}"
        );
        assert!(
            text.contains("1:v4:MW01"),
            "expected the client version key, got: {text}"
        );
        assert!(text.contains("2:ip6:"), "expected the BEP 42 ip key, got: {text}");
    }

    #[tokio::test]
    async fn a_query_with_the_transaction_id_of_ours_is_not_its_answer() {
        let broker = test_broker().await;
        spawn_run(&broker);
        let broker_addr = broker.socket.local_addr().unwrap();
        let other = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let txn = TransactionId::from_bytes(&[9, 9]);
        let mut rx = broker.subscribe_one(txn.clone(), other.local_addr().unwrap());

        // the node we asked happens to ask us something under the same transaction id
        let query = Krpc::new(txn.clone(), KrpcBody::PingQuery(PingQuery::new(NodeId([4; 20])))).encode();
        other.send_to(&query, broker_addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(rx.try_recv().is_err(), "a query must not be taken for the answer");

        let answer = Krpc::new(
            txn,
            KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(NodeId([4; 20]))),
        )
        .encode();
        other.send_to(&answer, broker_addr).await.unwrap();
        let (msg, _) = timeout(Duration::from_secs(1), &mut rx).await.unwrap().unwrap();
        assert!(msg.is_response());
    }

    #[tokio::test]
    async fn responses_from_the_wrong_address_are_ignored() {
        let broker = test_broker().await;
        spawn_run(&broker);

        let legit = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let legit_addr = legit.local_addr().unwrap();
        let spoofer = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();

        let txn = TransactionId::from_bytes(&[42]);
        let mut rx = broker.subscribe_one(txn.clone(), legit_addr);
        let broker_addr = broker.socket.local_addr().unwrap();

        let pkt = Krpc::new(
            txn,
            KrpcBody::PingAnnouncePeerResponse(PingAnnouncePeerResponse::new(NodeId([3u8; 20]))),
        )
        .encode();

        // a packet with the right transaction id but from the wrong address: ignored
        spoofer.send_to(&pkt, broker_addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            rx.try_recv().is_err(),
            "a response from the wrong address must not be delivered"
        );

        // the same packet from the queried address: delivered
        legit.send_to(&pkt, broker_addr).await.unwrap();
        let (_msg, from) = timeout(Duration::from_secs(1), &mut rx).await.unwrap().unwrap();
        assert_eq!(from, legit_addr);
    }
}
