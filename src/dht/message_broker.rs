#![allow(unused)]

use crate::{
    domain_knowledge::{CompactNodeContact, CompactPeerContact, NodeId},
    error::Error,
    message::{Krpc, TransactionId},
};
use futures::TryFutureExt;
use redis::RedisResult;
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    net::{SocketAddr, SocketAddrV4},
    sync::Arc,
};
use tokio::{
    join, net,
    sync::{mpsc, oneshot, Mutex},
    task::Builder,
    try_join,
};
use tracing::{instrument, trace, warn};
use crate::error::ErrorKind;
use crate::message::MessageType;

/// A PostalOffice is responsible for delivering messages to the appropriate nodes. *It is not
/// responsible for storing the messages sent to ourselves.*
#[async_trait::async_trait]
pub trait PostalOffice {
    async fn send_to_addr(&self, message: &Krpc, to: &SocketAddr) -> Result<(), Error>;
    async fn send_to_peer(&self, message: &Krpc, to: &CompactPeerContact) -> Result<(), Error>;
}

/// MailBoxes are where messages are stored temporarily, retrieval is done via the transaction id.
/// *It will only contain responses to our own requests. Requests from others sent to us *not* be
/// found here.*
#[async_trait::async_trait]
pub trait MailBoxes {
    fn get(&self, transaction_id: &TransactionId) -> Option<Krpc>;
    fn has(&self, transaction_id: &TransactionId) -> bool;
    async fn wait_for(&self, transaction_id: &TransactionId) -> Krpc;
}

pub struct UdpV4PostalOffice {
    socket: Arc<net::UdpSocket>,
}

#[async_trait::async_trait]
impl PostalOffice for UdpV4PostalOffice {
    async fn send_to_addr(&self, message: &Krpc, to: &SocketAddr) -> Result<(), Error> {
        let bytes = bendy::serde::to_bytes(message)?;
        self.socket.send_to(&bytes, to).await?;
        Ok(())
    }

    async fn send_to_peer(&self, message: &Krpc, to: &CompactPeerContact) -> Result<(), Error> {
        let sock_addr: SocketAddrV4 = to.into();
        self.send_to_addr(message, &SocketAddr::from(sock_addr)).await
    }
}

pub struct RedisMailBoxes {
    redis_client: redis::Client,
    redis_connection: redis::aio::Connection,
    socket: Arc<net::UdpSocket>,
}

impl RedisMailBoxes {
    pub async fn new(udp_sock: Arc<net::UdpSocket>, redis_client: redis::Client) -> RedisResult<Self> {
        let redis_connection = redis_client.get_async_connection().await?;

        Ok(Self {
            redis_client,
            redis_connection,
            socket: udp_sock,
        })
    }

    pub async fn run(&mut self) {
        loop {
            let mut buf = [0u8; 2048];
            let read_result = self.socket.recv_from(&mut buf)
                .map_err(|e| {
                    Error {
                        kind: ErrorKind::IoError,
                        source: Some(Box::new(e)),
                    }
                })
                .and_then(|(read_size, sender)| async {
                use redis::AsyncCommands;

                let message= bendy::serde::from_bytes::<Krpc>(&buf)?;

                message
            });
        }
    }
}
