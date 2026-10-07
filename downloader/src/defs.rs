use crate::config::Encryption;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

#[derive(Debug, Clone, Eq, PartialEq, Copy)]
pub struct Identity {
    pub peer_id: [u8; 20],
    /// Only `.port()` is actually used as a bind address: `bt_client::accept_incoming` listens
    /// on that port on both an IPv4 and an IPv6 socket regardless of which family `serving`
    /// itself is, so we accept inbound connections over either.
    pub serving: SocketAddr,
    /// we run a DHT node: announced in the handshake, and followed by a Port message
    pub dht: bool,
    pub encryption: Encryption,
}

/// A fresh peer id in the Azureus style other clients recognise: `-DL0100-` (this client,
/// version 0.1.0) then twelve random bytes, new every start so peers can't track us across
/// sessions by it.
pub fn random_peer_id() -> [u8; 20] {
    let mut id = *b"-DL0100-............";
    rand::Rng::fill_bytes(&mut rand::rng(), &mut id[8..]);
    id
}

/// Where sockets bind: every interface, except in unit tests, which keep to loopback so macOS's
/// firewall doesn't ask about each freshly built test binary.
pub(crate) const BIND_V4: Ipv4Addr = if cfg!(test) {
    Ipv4Addr::LOCALHOST
} else {
    Ipv4Addr::UNSPECIFIED
};
pub(crate) const BIND_V6: Ipv6Addr = if cfg!(test) {
    Ipv6Addr::LOCALHOST
} else {
    Ipv6Addr::UNSPECIFIED
};
