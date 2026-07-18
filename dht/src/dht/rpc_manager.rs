use std::{
    borrow::Cow,
    collections::HashMap,
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
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
    types::{NodeInfo, TransactionId},
    utils::unix_timestmap_ms,
};

use super::{TxnIdGenerator, routing_table::update_last_sent};

/// A message broker keeps reading Krpc messages from a queue and place them either into the
/// server response queue when we haven't seen this transaction id before, or into a oneshot channel
/// so the client and await the response.
#[derive(Debug, Clone)]
pub struct RpcManager {
    /// a map to keep track of the responses we await from the client; the address is the
    /// endpoint we queried, so responses from anywhere else are ignored
    pending_responses: Arc<Mutex<HashMap<TransactionId, (SocketAddrV4, oneshot::Sender<(Krpc, SocketAddrV4)>)>>>,

    socket: Arc<UdpSocket>,
    txn_id_generator: Arc<TxnIdGenerator>,

    /// a SPMC-esque queue, each readers can progress indepednelty
    inbound_subscribers: Arc<Mutex<Vec<mpsc::Sender<(Krpc, SocketAddrV4)>>>>,
    db: Pool<ConnectionManager<SqliteConnection>>,

    public_ip: Ipv4Addr,
}

pub trait Routable {
    fn endpoint(&self) -> SocketAddrV4;
}

impl Routable for SocketAddrV4 {
    fn endpoint(&self) -> SocketAddrV4 {
        *self
    }
}

impl RpcManager {
    pub fn new(
        socket: UdpSocket,
        db: Pool<ConnectionManager<SqliteConnection>>,
        txn_id_generator: Arc<TxnIdGenerator>,
        public_ip: Ipv4Addr,
    ) -> RpcManager {
        Self {
            pending_responses: Arc::new(Mutex::new(HashMap::new())),
            socket: Arc::new(socket),
            inbound_subscribers: Arc::new(Mutex::new(vec![])),
            db,
            txn_id_generator,
            public_ip,
        }
    }

    #[instrument(skip_all)]
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
                        let socket_addr = {
                            match socket_addr {
                                SocketAddr::V4(addr) => addr,
                                _ => {
                                    info!("Non Ipv4 UDP packet received, should not be possible");
                                    continue;
                                }
                            }
                        };

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
                        if let SocketAddr::V4(addr) = socket_addr {
                            let response = Krpc::new_unsupported_error(txn);
                            this.send_msg_background(&response, addr);
                        }
                    }
                    Err(e) => {
                        warn!("Error in parsing packets {e} from {socket_addr}")
                    }
                }
            }
        };
        use tokio::task::Builder;
        Builder::new().name("Message broker").spawn(event_loop)
    }

    /// Subscribe to the reply with the provided transaction_id
    /// Subscribe to the reply with the provided transaction_id, expected from `endpoint`
    pub fn subscribe_one(
        &self,
        transaction_id: TransactionId,
        endpoint: SocketAddrV4,
    ) -> oneshot::Receiver<(Krpc, SocketAddrV4)> {
        let (tx, rx) = oneshot::channel();

        let mut guard = self.pending_responses.lock().unwrap();
        // it's possible that the response never came and we a new request is now using the same
        // transaction id
        let _ = guard.insert(transaction_id, (endpoint, tx));
        rx
    }

    /// Send a message, fires up a new stask in background
    fn send_msg_background(&self, msg: &Krpc, peer: SocketAddrV4) {
        let socket = self.socket.clone();

        let mut additional = HashMap::new();
        // BEP 5: every message should carry our client version
        additional.insert(&b"v"[..], value::Value::Bytes(Cow::Borrowed(&b"MW01"[..])));
        // BEP 42: in responses, tell the requester the external address we see for it
        if !msg.body.is_query() {
            let mut ip = [0u8; 6];
            ip[..4].copy_from_slice(&peer.ip().octets());
            ip[4..].copy_from_slice(&peer.port().to_be_bytes());
            additional.insert(&b"ip"[..], value::Value::Bytes(Cow::Owned(ip.to_vec())));
        }

        let buf = msg.encode_with_additional(&additional);

        tokio::spawn(async move {
            if let Err(e) = socket.send_to(&buf, peer).await {
                warn!("failed to send message to {peer}: {e}");
            }
        });
    }

    /// Send a message out and await for a response.
    async fn send_and_wait(&self, message: Krpc, endpoint: SocketAddrV4) -> Result<Krpc, OurError> {
        let sent_time = unix_timestmap_ms();
        let rx = {
            let rx = self.subscribe_one(message.transaction_id().clone(), endpoint);
            self.send_msg_background(&message, endpoint);
            rx
        };
        let (response, _addr) = rx
            .await
            .map_err(|_| naur!("pending request superseded or dropped before a response arrived"))?;

        // no node_id means the reponse is a krpc error message, only error message omit the node
        // id
        let response_node_id = response.node_id().ok_or(naur!("node responded with error"))?;
        let mut conn = self.db.get().unwrap();
        // it's a double update but that's issue for another day
        update_last_sent(&response_node_id, sent_time, &mut conn);
        Ok(response)
    }

    async fn send_and_wait_timeout(
        &self,
        message: Krpc,
        endpoint: SocketAddrV4,
        time_out: Duration,
    ) -> Result<Krpc, OurError> {
        let txn_id = message.transaction_id().clone();
        let result = timeout(time_out, self.send_and_wait(message, endpoint)).await;
        if result.is_err() {
            // timed out: free the pending slot so dead endpoints don't leak entries forever
            self.pending_responses.lock().unwrap().remove(&txn_id);
        }
        let response = result??;
        Ok(response)
    }

    pub fn subscribe_inbound(&self) -> mpsc::Receiver<(Krpc, SocketAddrV4)> {
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
    use crate::dht::SensibleOptions;
    use crate::dht::txn_id_generator::TxnIdGenerator;
    use crate::message::ping_announce_peer_response::PingAnnouncePeerResponse;
    use crate::message::ping_query::PingQuery;
    use crate::types::NodeId;

    async fn test_broker() -> RpcManager {
        let manager = ConnectionManager::<SqliteConnection>::new(":memory:");
        let pool = Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(SensibleOptions))
            .build(manager)
            .unwrap();
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        RpcManager::new(socket, pool, Arc::new(TxnIdGenerator::new()), Ipv4Addr::LOCALHOST)
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

        let result = broker.query(body, &dead, Duration::from_millis(50)).await;
        assert!(result.is_err());
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
        let SocketAddr::V4(legit_addr) = legit.local_addr().unwrap() else {
            unreachable!("bound to an ipv4 address");
        };
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
