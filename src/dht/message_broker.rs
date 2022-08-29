#![allow(unused)]

use crate::{
    domain_knowledge::{CompactNodeContact, CompactPeerContact, NodeId},
    error::Error,
    message::{Krpc, TransactionId},
};
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
use tracing::{instrument, trace};

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
