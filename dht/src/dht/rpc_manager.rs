use std::{
    borrow::Cow,
    collections::HashMap,
    io,
    net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6},
    sync::{Arc, Mutex},
    time::Duration,
};

use bendy::value;
use diesel::{
    SqliteConnection,
    r2d2::{ConnectionManager, Pool},
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tracing::{info, instrument, trace, warn};

use crate::{
    message::{Krpc, KrpcBody, ParseKrpc},
    our_error::{OurError, naur},
    types::{Family, NodeInfo, TransactionId},
    utils::{db_put, unix_timestmap_ms},
};

use super::{TxnIdGenerator, external_ip::ExternalIp, misc_key, routing_table::update_last_sent};

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
}

pub trait Routable {
    fn endpoint(&self) -> SocketAddr;
}

impl Routable for SocketAddr {
    fn endpoint(&self) -> SocketAddr {
        *self
    }
}

impl Routable for SocketAddrV4 {
    fn endpoint(&self) -> SocketAddr {
        (*self).into()
    }
}

impl Routable for SocketAddrV6 {
    fn endpoint(&self) -> SocketAddr {
        (*self).into()
    }
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
        }
    }

    pub fn family(&self) -> Family {
        self.family
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    #[instrument(skip_all, fields(family = %self.family))]
    pub async fn run(&self) -> io::Result<JoinHandle<()>> {
        let socket = self.socket.clone();
        let pending_responses = self.pending_responses.clone();
        let inbound_subscribers = self.inbound_subscribers.clone();
        let this = self.clone();

        let event_loop = async move {
            let mut buf = [0u8; 1500];

            loop {
                // recv_from can fail transiently (e.g. ICMP port-unreachable from an
                // earlier send on macOS/BSD); that must not kill the broker loop
                let (amount, socket_addr) = match socket.recv_from(&mut buf).await {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("udp recv_from failed: {e}");
                        continue;
                    }
                };
                trace!("received packet from {socket_addr}");
                match (&buf[..amount]).parse() {
                    Ok(msg) => {
                        trace!("{} sent {:?}", socket_addr, msg);

                        let id = msg.transaction_id();
                        trace!(
                            "received message for transaction id {:?}",
                            hex::encode_upper(id.as_bytes())
                        );

                        // notify those that subscribed for all inbound messages
                        {
                            let mut subcribers = inbound_subscribers.lock().unwrap();

                            subcribers.retain(|s| !s.is_closed());

                            for sub in &*subcribers {
                                // Not using send here because `subcribers` mutex guard
                                // is not Send and it lives across await points; a lagging
                                // subscriber loses messages but must not stall the broker
                                if sub.try_send((msg.clone(), socket_addr)).is_err() {
                                    warn!("inbound subscriber lagging, dropping a message for it");
                                }
                            }
                        }

                        {
                            // see if we have a slot for this transaction id, if we do, that means one of the
                            // messages that we expect, otherwise the message is a query we need to handle
                            let entry = pending_responses.lock().unwrap().remove(id);
                            if let Some((expected, sender)) = entry {
                                if expected == socket_addr {
                                    // only answers to our own queries get a say in what our
                                    // external address is; anyone can send a query
                                    if let Some(seen) = msg.ip {
                                        this.record_external_ip(socket_addr.ip(), seen.ip());
                                    }
                                    // failing means the receiver has dropped, meaning they are no
                                    // longer interested in the message, not a bug
                                    let _ = sender.send((msg, socket_addr));
                                } else {
                                    warn!(
                                        "ignoring response for a pending transaction from the wrong address: expected {expected}, got {socket_addr}"
                                    );
                                    // the genuine response may still arrive; keep the slot
                                    pending_responses.lock().unwrap().insert(id.clone(), (expected, sender));
                                }
                            }
                        }
                    }
                    Err(OurError::UnsupportedQuery(txn)) => {
                        // BEP 5: unknown query methods get a 204 Method Unknown error reply
                        let response = Krpc::new_unsupported_error(txn);
                        this.send_msg_background(&response, socket_addr);
                    }
                    Err(e) => {
                        tracing::debug!("ignoring an unparseable packet from {socket_addr}: {e}")
                    }
                }
            }
        };
        use tokio::task::Builder;
        Builder::new()
            .name(&format!("Message broker ({})", self.family))
            .spawn(event_loop)
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
        let key = misc_key(super::OBSERVED_IP_KEY, self.family);
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

    fn encode_for(msg: &Krpc, peer: SocketAddr) -> Box<[u8]> {
        let mut additional = HashMap::new();
        // BEP 5: every message should carry our client version
        additional.insert(&b"v"[..], value::Value::Bytes(Cow::Borrowed(&b"MW01"[..])));

        if msg.body.is_query() {
            msg.encode_with_additional(&additional)
        } else {
            // BEP 42: a response tells the querier the external address we see for it
            let mut msg = msg.clone();
            msg.ip = Some(peer);
            msg.encode_with_additional(&additional)
        }
    }

    /// Send a message, fires up a new stask in background
    fn send_msg_background(&self, msg: &Krpc, peer: SocketAddr) {
        let socket = self.socket.clone();
        let buf = Self::encode_for(msg, peer);

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
        let sent_time = unix_timestmap_ms();
        let rx = self.subscribe_one(message.transaction_id().clone(), endpoint);
        self.socket
            .send_to(&Self::encode_for(&message, endpoint), endpoint)
            .await?;
        let (response, _addr) = rx
            .await
            .map_err(|_| naur!("pending request superseded or dropped before a response arrived"))?;

        // no node_id means the reponse is a krpc error message, only error message omit the node
        // id
        let response_node_id = response.node_id().ok_or(naur!("node responded with error"))?;
        let mut conn = self
            .db
            .get()
            .map_err(|e| naur!("could not check out a db connection: {e}"))?;
        // it's a double update but that's issue for another day
        update_last_sent(&response_node_id, self.family, sent_time, &mut conn);
        Ok(response)
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
        // TODO: make this configurable
        let (tx, rx) = mpsc::channel(1024);
        let mut subscribers = self.inbound_subscribers.lock().unwrap();
        subscribers.push(tx);
        rx
    }

    /// Send a response to a query. Fire-and-forget: responses to responses are not a
    /// thing in KRPC, so there is nothing to wait for (and waiting leaked a task per
    /// inbound query).
    pub fn reply(&self, body: KrpcBody, node: &NodeInfo, txn_id: TransactionId) {
        let message = Krpc::new_with_body(txn_id, body);
        self.send_msg_background(&message, node.end_point());
    }

    pub async fn query<E: Routable>(&self, body: KrpcBody, endpoint: &E, timeout: Duration) -> Result<Krpc, OurError> {
        let endpoint = endpoint.endpoint();
        let message = Krpc::new_with_body(self.txn_id_generator.next().into(), body);

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
    use std::net::Ipv4Addr;

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

        let result = broker.query(body.clone(), &dead, Duration::from_millis(50)).await;
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
            broker.query(body, &unroutable, Duration::from_secs(30)),
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
        broker.run().await.unwrap();
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
    async fn responses_from_the_wrong_address_are_ignored() {
        let broker = test_broker().await;
        broker.run().await.unwrap();

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

        let pkt = Krpc::new_with_body(
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
