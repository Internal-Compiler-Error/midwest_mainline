use std::{
    collections::HashMap,
    io,
    net::{SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex},
};

use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot, Mutex as AsyncMutex},
    task::JoinHandle,
};
use tracing::{instrument, trace, warn};

use crate::{
    domain_knowledge::TransactionId,
    message::{Krpc, ParseKrpc, ToRawKrpc},
};

/// A message broker keeps reading Krpc messages from a queue and place them either into the
/// server response queue when we haven't seen this transaction id before, or into a oneshot channel
/// so the client and await the response.
/// TODO: replace this with some proper pubsub
#[derive(Debug)]
pub(crate) struct MessageBroker {
    /// a map to keep track of the responses we await from the client
    pending_responses: Arc<AsyncMutex<HashMap<TransactionId, oneshot::Sender<(Krpc, SocketAddrV4)>>>>,

    /// all messages belong to the server are put into this queue.
    // query_queue: Arc<AsyncMutex<mpsc::Sender<(Krpc, SocketAddrV4)>>>,

    /// a channel to receive new krpc read from the socket
    // outbound_messages: Arc<AsyncMutex<mpsc::Receiver<(Krpc, SocketAddrV4)>>>,
    socket: Arc<UdpSocket>,

    /// a list of subscribers that wishes to hear all messages inbound
    inbound_subscribers: Arc<Mutex<Vec<mpsc::Sender<(Krpc, SocketAddrV4)>>>>,
}

impl MessageBroker {
    pub fn new(
        // incoming_queue: mpsc::Receiver<(Krpc, SocketAddrV4)>,
        // query_channel: mpsc::Sender<(Krpc, SocketAddrV4)>,
        socket: UdpSocket,
    ) -> Self {
        Self {
            pending_responses: Arc::new(AsyncMutex::new(HashMap::new())),
            // query_queue: Arc::new(AsyncMutex::new(query_channel)),
            // outbound_messages: Arc::new(AsyncMutex::new(incoming_queue)),
            socket: Arc::new(socket),
            inbound_subscribers: Arc::new(Mutex::new(vec![])),
        }
    }

    #[instrument(skip_all)]
    pub async fn run(&self) -> io::Result<JoinHandle<()>> {
        let socket = self.socket.clone();
        let pending_responses = self.pending_responses.clone();
        // let query_queue = self.query_queue.clone();
        let inbound_subscribers = self.inbound_subscribers.clone();

        let socket_reader = async move {
            let mut buf = [0u8; 1500];

            loop {
                let (amount, socket_addr) = socket.recv_from(&mut buf).await.expect("common MTU 1500 exceeded");
                trace!("packet from {socket_addr}");
                if let Ok(msg) = (&buf[..amount]).parse() {
                    let socket_addr = {
                        match socket_addr {
                            SocketAddr::V4(addr) => addr,
                            _ => panic!("Expected V4 socket address"),
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

                    // see if we have a slot for this transaction id, if we do, that means one of the
                    // messages that we expect, otherwise the message is a query we need to handle
                    if let Some(sender) = pending_responses.lock().await.remove(id) {
                        // failing means we're no longer interested, which is ok
                        let _ = sender.send((msg, socket_addr));
                    }
                    warn!(
                        "Failed to parse message from {socket_addr}, bytes = {:?}",
                        &buf[..amount]
                    );
                }
            }
        };

        // let outbound_messages = self.outbound_messages.clone();
        // let socket = self.socket.clone();
        // let stupid = async move {
        //     let mut outbound_messages = outbound_messages.lock().await;
        //     loop {
        //         let outbound = outbound_messages.recv().await;
        //         if outbound.is_none() {
        //             break;
        //         }
        //
        //         let (msg, peer) = outbound.unwrap();
        //         let _ = socket.send_to(&msg.to_raw_krpc(), peer).await;
        //     }
        // };

        use tokio::task::Builder;
        Builder::new().name("Message broker").spawn(socket_reader)
    }

    /// Tell the broker we should expect some messages
    // TODO: maybe it should return Receiver<Option<(Krpc, SocketAddrV4)>
    pub async fn subscribe_one(&self, transaction_id: TransactionId) -> oneshot::Receiver<(Krpc, SocketAddrV4)> {
        let (tx, rx) = oneshot::channel();

        let mut guard = self.pending_responses.lock().await;
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

            let _ = socket.send_to(&buf, peer);
        });
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
