use std::{
    collections::HashMap,
    io,
    net::{SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex},
    time::Duration,
};

use diesel::{
    r2d2::{ConnectionManager, Pool},
    SqliteConnection,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tracing::{instrument, trace, warn};

use crate::{
    message::{Krpc, ParseKrpc, ToRawKrpc},
    our_error::OurError,
    types::{NodeInfo, TransactionId},
    utils::unix_timestmap_ms,
};

use super::router::update_last_sent;

/// A message broker keeps reading Krpc messages from a queue and place them either into the
/// server response queue when we haven't seen this transaction id before, or into a oneshot channel
/// so the client and await the response.
/// TODO: replace this with some proper pubsub
#[derive(Debug, Clone)]
pub struct KrpcBroker {
    /// a map to keep track of the responses we await from the client
    pending_responses: Arc<Mutex<HashMap<TransactionId, oneshot::Sender<(Krpc, SocketAddrV4)>>>>,

    socket: Arc<UdpSocket>,

    /// a list of subscribers that wishes to hear all messages inbound
    inbound_subscribers: Arc<Mutex<Vec<mpsc::Sender<(Krpc, SocketAddrV4)>>>>,
    db: Pool<ConnectionManager<SqliteConnection>>,
}

impl KrpcBroker {
    pub fn new(socket: UdpSocket, db: Pool<ConnectionManager<SqliteConnection>>) -> Self {
        Self {
            pending_responses: Arc::new(Mutex::new(HashMap::new())),
            socket: Arc::new(socket),
            inbound_subscribers: Arc::new(Mutex::new(vec![])),
            db,
        }
    }

    #[instrument(skip_all)]
    pub async fn run(&self) -> io::Result<JoinHandle<()>> {
        let socket = self.socket.clone();
        let pending_responses = self.pending_responses.clone();
        let inbound_subscribers = self.inbound_subscribers.clone();

        let socket_reader = async move {
            let mut buf = [0u8; 1500];

            loop {
                let (amount, socket_addr) = socket.recv_from(&mut buf).await.expect("common MTU 1500 exceeded");
                trace!("received packet from {socket_addr}");
                match (&buf[..amount]).parse() {
                    Ok(msg) => {
                        trace!("{} sent {:?}", socket_addr, msg);
                        let socket_addr = {
                            match socket_addr {
                                SocketAddr::V4(addr) => addr,
                                _ => panic!("Expected V4 socket address"), // TODO: obviously we can't panic
                            }
                        };

                        let id = msg.transaction_id();
                        trace!(
                            "received message for transaction id {:?}",
                            hex::encode_upper(id.as_bytes())
                        );

                        // notify those that subscribed for all inbound messages
                        {
                            let subcribers = inbound_subscribers.lock().unwrap();
                            for sub in &*subcribers {
                                // TODO: worry about what to do about disconnected or full queues later
                                let _ = sub.try_send((msg.clone(), socket_addr));
                            }
                        }

                        {
                            // see if we have a slot for this transaction id, if we do, that means one of the
                            // messages that we expect, otherwise the message is a query we need to handle
                            if let Some(sender) = pending_responses.lock().unwrap().remove(id) {
                                // failing means we're no longer interested, which is ok
                                let _ = sender.send((msg, socket_addr));
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Error in parsing packets {e} from {socket_addr}")
                    }
                }
            }
        };
        use tokio::task::Builder;
        Builder::new().name("Message broker").spawn(socket_reader)
    }

    /// Tell the broker we should expect some messages
    // TODO: maybe it should return Receiver<Option<(Krpc, SocketAddrV4)>
    pub fn subscribe_one(&self, transaction_id: TransactionId) -> oneshot::Receiver<(Krpc, SocketAddrV4)> {
        let (tx, rx) = oneshot::channel();

        let mut guard = self.pending_responses.lock().unwrap();
        // it's possible that the response never came and we a new request is now using the same
        // transaction id
        let _ = guard.insert(transaction_id, tx);
        rx
    }

    pub fn send_msg(&self, msg: Krpc, peer: SocketAddrV4) {
        let socket = self.socket.clone();

        tokio::spawn(async move {
            let buf = msg.to_raw_krpc();

            socket.send_to(&buf, peer).await.unwrap();
        });
    }

    /// Send a message out and await for a response.
    pub async fn send_and_wait(&self, message: Krpc, endpoint: SocketAddrV4) -> Result<Krpc, OurError> {
        let sent_time = unix_timestmap_ms();
        let rx = {
            let rx = self.subscribe_one(message.transaction_id().clone());
            self.send_msg(message.clone(), endpoint);
            rx
        };
        let (response, _addr) = rx.await.unwrap();

        // don't wanna write a new function, so abuse a loop so I have access to `break`
        loop {
            // TODO: we really need to agree on whether to copy or share with node_id by default
            let response_node_id = match &response {
                Krpc::AnnouncePeerQuery(announce_peer_query) => *announce_peer_query.querier(),
                Krpc::FindNodeQuery(find_node_query) => find_node_query.querier(),
                Krpc::GetPeersQuery(get_peers_query) => *get_peers_query.querier(),
                Krpc::PingQuery(ping_query) => *ping_query.querier(),
                Krpc::PingAnnouncePeerResponse(ping_announce_peer_response) => *ping_announce_peer_response.target_id(),
                Krpc::FindNodeGetPeersResponse(find_node_get_peers_response) => *find_node_get_peers_response.queried(),
                Krpc::ErrorResponse(_) => break,
            };

            let mut conn = self.db.get().unwrap();
            // it's a double update but that's issue for another day
            update_last_sent(&response_node_id, sent_time, &mut conn);
            break;
        }
        Ok(response)
    }

    pub async fn send_and_wait_timeout(
        &self,
        message: Krpc,
        endpoint: SocketAddrV4,
        time_out: Duration,
    ) -> Result<Krpc, OurError> {
        let response = timeout(time_out, self.send_and_wait(message, endpoint))
            .await
            .inspect_err(|_e| {
                trace!("operation timedout for {} timed out after {:?}", endpoint, time_out);
            })
            ?  // timeout error
            ?; // send_and_wait related error
        Ok(response)
    }

    pub fn send_msg_to_node(&self, msg: Krpc, peer: &NodeInfo) {
        self.send_msg(msg, peer.end_point())
    }

    // TODO: think of a good way to let them unsubscribe later
    pub fn subscribe_inbound(&self) -> mpsc::Receiver<(Krpc, SocketAddrV4)> {
        // TODO: make this configurable
        let (tx, rx) = mpsc::channel(1024);
        let mut subscribers = self.inbound_subscribers.lock().unwrap();
        subscribers.push(tx);
        rx
    }
}
