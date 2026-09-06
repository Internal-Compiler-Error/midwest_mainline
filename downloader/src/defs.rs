use std::net::SocketAddr;

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Copy)]
pub struct Identity {
    pub peer_id: [u8; 20],
    /// Only `.port()` is actually used as a bind address: `bt_client::accept_incoming` listens
    /// on that port on both an IPv4 and an IPv6 socket regardless of which family `serving`
    /// itself is, so we accept inbound connections over either.
    pub serving: SocketAddr,
    /// we run a DHT node: announced in the handshake, and followed by a Port message
    pub dht: bool,
}
