use std::{
    collections::HashMap,
    io,
    net::{SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex},
};

use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{instrument, trace, warn};

use crate::{
    domain_knowledge::{NodeInfo, TransactionId},
    message::{Krpc, ParseKrpc, ToRawKrpc},
};

/// A message broker keeps reading Krpc messages from a queue and place them either into the
/// server response queue when we haven't seen this transaction id before, or into a oneshot channel
/// so the client and await the response.
/// TODO: replace this with some proper pubsub
#[derive(Debug, Clone)]
pub struct MessageBroker {
    /// a map to keep track of the responses we await from the client
    pending_responses: Arc<Mutex<HashMap<TransactionId, oneshot::Sender<(Krpc, SocketAddrV4)>>>>,

    socket: Arc<UdpSocket>,

    /// a list of subscribers that wishes to hear all messages inbound
    inbound_subscribers: Arc<Mutex<Vec<mpsc::Sender<(Krpc, SocketAddrV4)>>>>,
}

impl MessageBroker {
    pub fn new(socket: UdpSocket) -> Self {
        Self {
            pending_responses: Arc::new(Mutex::new(HashMap::new())),
            socket: Arc::new(socket),
            inbound_subscribers: Arc::new(Mutex::new(vec![])),
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
                trace!("recived packet from {socket_addr}");
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

        // TODO: maybe this is too expensive?
        tokio::spawn(async move {
            let buf = msg.to_raw_krpc();

            socket.send_to(&buf, peer).await.unwrap();
        });
    }

    pub fn send_msg_to_node(&self, msg: Krpc, peer: &NodeInfo) {
        self.send_msg(msg, peer.contact().0)
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
