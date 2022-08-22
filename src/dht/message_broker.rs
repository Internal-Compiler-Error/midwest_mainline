#![allow(unused)]

use crate::message::{Krpc, TransactionId};
use std::{
    collections::HashMap,
    error::Error,
    net::{SocketAddr, SocketAddrV4},
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    join, net,
    sync::{mpsc, oneshot, Mutex},
    task::Builder,
    try_join,
};
use tracing::{instrument, trace};

/// A message demultiplexer keeps reading Krpc messages from a queue and place them either into the
/// server response queue when we haven't seen this transaction id before, or into a oneshot channel
/// so the client and await the response.
#[derive(Debug)]
pub(crate) struct MessageBroker<D>
where
    D: MessageDemultiplexer + 'static,
{
    /// a map to keep track of the responses we await from the client
    pending_responses: Mutex<HashMap<TransactionId, oneshot::Sender<Krpc>>>,

    /// all messages belong to the server are put into this queue.
    query_queue: Mutex<mpsc::Sender<(Krpc, SocketAddrV4)>>,

    /// a channel to receive new krpc read from the socket
    incoming_messages: Mutex<mpsc::Receiver<(Krpc, SocketAddrV4)>>,

    /// a channel to store messages we intend to send, somewhere else in the system, the message
    /// will be pulled from this channel and sent to the socket.
    outgoing_messages: Mutex<mpsc::Sender<(Krpc, SocketAddrV4)>>,

    /// a demultiplexer to handle the messages we receive from a source, currently the socket.
    demultiplexer: Option<D>,
}

/// A message demultiplexer will continuously read a source of IO and demultiplex messages into a
/// queue for incoming messages and it will continuously read a queue of outgoing messages and send
/// them to the IO sink.
///
/// Currently the trait doesn't really convey the information about the queues, I would love to use
/// the type system to make this more explicit but I don't know how to do that yet.
#[async_trait::async_trait]
pub trait MessageDemultiplexer {
    type Error: Error + Send + Sync + 'static;
    async fn run(self) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub(crate) struct UdpMessageDemultiplexer {
    socket: Arc<net::UdpSocket>,

    /// a channel to put messages to the UDP socket
    outgoing_messages: mpsc::Receiver<(Krpc, SocketAddrV4)>,

    /// a channel to get messages from the UDP socket
    incoming_messages: mpsc::Sender<(Krpc, SocketAddrV4)>,
}

impl UdpMessageDemultiplexer {
    pub(crate) fn new(
        socket: net::UdpSocket,
        outgoing_messages: mpsc::Receiver<(Krpc, SocketAddrV4)>,
        incoming_messages: mpsc::Sender<(Krpc, SocketAddrV4)>,
    ) -> Self {
        UdpMessageDemultiplexer {
            socket: Arc::new(socket),
            outgoing_messages,
            incoming_messages,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum MessageDemultiplexerError {
    #[error("Internal IO Error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("The server is no longer running")]
    IncomingQueueClosed(#[from] mpsc::error::SendError<(Krpc, SocketAddrV4)>),

    #[error("JoinError: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

#[async_trait::async_trait]
impl MessageDemultiplexer for UdpMessageDemultiplexer {
    type Error = std::io::Error;
    async fn run(mut self) -> Result<(), Self::Error> {
        let socket = Arc::clone(&self.socket);
        let put_message_to_socket = async move {
            while let Some((msg, recipient)) = self.outgoing_messages.recv().await {
                if let Ok(encoded) = bendy::serde::to_bytes(&msg) {
                    socket.send_to(&encoded, &recipient).await.unwrap();
                } else {
                    tracing::warn!("Failed to encode message");
                }
            }

            Ok::<_, MessageDemultiplexerError>(())
        };

        let socket = Arc::clone(&self.socket);
        let get_message_from_socket = async move {
            let mut buf = [0u8; 2048];
            while let Ok((amt, src)) = socket.recv_from(&mut buf).await {
                // we only accept ipv4 addresses
                let src = match src {
                    SocketAddr::V4(src) => src,
                    SocketAddr::V6(_) => {
                        panic!("IPv6 addresses are not supported");
                    }
                };

                if let Ok(msg) = bendy::serde::from_bytes::<Krpc>(&buf[..amt]) {
                    self.incoming_messages.send((msg, src)).await?;
                }
            }
            Ok::<_, MessageDemultiplexerError>(())
        };

        let res = try_join!(
            Builder::new()
                .name("send messages to socket")
                .spawn(put_message_to_socket),
            Builder::new()
                .name("transfer messages to socket")
                .spawn(get_message_from_socket)
        );

        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug, Error)]
pub enum SendMessageError {
    #[error("Outgoing channel is closed")]
    OutgoingChannelClosed(#[from] mpsc::error::SendError<(Krpc, SocketAddrV4)>),

    #[error("Message trampled, somehow a new request with the same transaction has overwritten the old channel")]
    MessageTrampled(#[from] oneshot::error::RecvError),
}

impl<D> MessageBroker<D>
where
    D: MessageDemultiplexer + Send + 'static,
{
    pub fn new(
        incoming_messages: mpsc::Receiver<(Krpc, SocketAddrV4)>,
        query_channel: mpsc::Sender<(Krpc, SocketAddrV4)>,
        outgoing_channel: mpsc::Sender<(Krpc, SocketAddrV4)>,
        demultiplexer: D,
    ) -> Self {
        Self {
            pending_responses: Mutex::new(HashMap::new()),
            query_queue: Mutex::new(query_channel),
            incoming_messages: Mutex::new(incoming_messages),
            outgoing_messages: Mutex::new(outgoing_channel),
            demultiplexer: Some(demultiplexer),
        }
    }

    #[instrument(skip_all)]
    pub async fn run(mut self) {
        let forward_messages_into_right_queue = async move {
            let mut incoming_messages = self.incoming_messages.lock().await;
            while let Some((msg, socket_addr)) = incoming_messages.recv().await {
                let id = msg.transaction_id();
                trace!("received message for transaction id {:?}", hex::encode_upper(&*id));

                // if the message is a query, we put it into the queue to be sent to the server
                if msg.is_query() {
                    let query_queue = self.query_queue.lock().await;
                    query_queue
                        .send((msg, socket_addr))
                        .await
                        .expect("the query queue closed unexpectedly");
                } else {
                    // if the message is a response, we check if we have a pending request for this
                    // transaction id, if so, we send the response to the client, otherwise we drop it.
                    let mut pending_responses = self.pending_responses.lock().await;
                    if let Some(sender) = pending_responses.remove(id) {
                        // totally normal if the receiver is gone, this usually happens when the client
                        // is no longer interested in the request
                        let _ = sender.send(msg);
                    }
                }
            }
        };

        join!(
            Builder::new()
                .name("forward messages to right queue")
                .spawn(forward_messages_into_right_queue),
            Builder::new()
                .name("demultiplex messages")
                .spawn(self.demultiplexer.take().unwrap().run())
        );
    }

    /// Tell the placer we should expect some messages
    pub async fn register(&self, transaction_id: TransactionId, sending_half: oneshot::Sender<Krpc>) {
        let mut guard = self.pending_responses.lock().await;
        // it's possible that the response never came and we a new request is now using the same
        // transaction id
        let _ = guard.insert(transaction_id, sending_half);
    }

    pub async fn send_queries(&self, msg: Krpc, socket_addr: SocketAddrV4) -> Result<Krpc, SendMessageError> {
        let transaction_id = msg.transaction_id();

        let (tx, rx) = oneshot::channel();

        self.register(transaction_id.clone(), tx).await;

        let outgoing_messages = self.outgoing_messages.lock().await;
        outgoing_messages.send((msg, socket_addr)).await?;

        Ok(rx.await?)
    }

    pub async fn send_responses(&self, msg: Krpc, socket_addr: SocketAddrV4) -> Result<(), SendMessageError> {
        let outgoing_messages = self.outgoing_messages.lock().await;
        Ok(outgoing_messages.send((msg, socket_addr)).await?)
    }
}
