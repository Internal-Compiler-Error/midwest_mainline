//! Local Service Discovery (BEP 14): peers on the same LAN find each other by multicasting
//! `BT-SEARCH` messages naming the torrents they serve. One task per client sends one
//! message naming every torrent every LSD_INTERVAL (and right after a torrent is added),
//! and listens for everyone else's; a message naming a torrent we have turns its sender
//! into a `PeersDiscovered` for that swarm.

use crate::events::PeerSource;
use crate::settings::LSD_INTERVAL;
use crate::torrent_swarm::TorrentSwarmHandle;
use midwest_mainline::types::InfoHash;
use rand::RngExt;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex, Weak};
use tokio::net::UdpSocket;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::debug;

const GROUP: Ipv4Addr = Ipv4Addr::new(239, 192, 152, 143);
const PORT: u16 = 6771;
/// Infohash lines per message, keeping it well inside one datagram
const HASHES_PER_MESSAGE: usize = 20;

type Swarms = Weak<Mutex<HashMap<InfoHash, TorrentSwarmHandle>>>;

/// Runs discovery for a client; `announce_now` (see `Lsd::announce`) asks for an announce
/// ahead of the next interval, which a freshly added torrent wants.
pub(crate) struct Lsd {
    announce_now: Arc<Notify>,
}

impl Lsd {
    pub fn spawn(swarms: Swarms, tcp_port: u16, shutdown: CancellationToken) -> Lsd {
        let announce_now = Arc::new(Notify::new());
        tokio::spawn(run(swarms, tcp_port, announce_now.clone(), shutdown));
        Lsd { announce_now }
    }

    pub fn announce(&self) {
        self.announce_now.notify_one();
    }
}

async fn run(swarms: Swarms, tcp_port: u16, announce_now: Arc<Notify>, shutdown: CancellationToken) {
    let socket = match multicast_socket() {
        Ok(socket) => socket,
        Err(e) => {
            debug!("no local service discovery: {e}");
            return;
        }
    };
    // tells our own announces apart from another client's on this machine
    let cookie = format!("{:08x}", rand::rng().random::<u32>());
    let target = SocketAddrV4::new(GROUP, PORT);

    let announce = async {
        loop {
            let hashes: Vec<InfoHash> = match swarms.upgrade() {
                Some(swarms) => swarms.lock().unwrap().keys().copied().collect(),
                None => return,
            };
            for chunk in hashes.chunks(HASHES_PER_MESSAGE) {
                let message = bt_search(tcp_port, chunk, &cookie);
                if let Err(e) = socket.send_to(message.as_bytes(), target).await {
                    debug!("LSD announce failed: {e}");
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(LSD_INTERVAL) => {}
                _ = announce_now.notified() => {}
            }
        }
    };
    let listen = async {
        let mut buf = [0u8; 1500];
        loop {
            let Ok((n, from)) = socket.recv_from(&mut buf).await else {
                continue;
            };
            let Some(search) = parse_bt_search(&buf[..n]) else {
                continue;
            };
            if search.cookie.as_deref() == Some(&cookie) {
                continue;
            }
            let Some(swarms) = swarms.upgrade() else { return };
            let peer = SocketAddr::new(from.ip(), search.port);
            let handles: Vec<TorrentSwarmHandle> = {
                let swarms = swarms.lock().unwrap();
                search
                    .info_hashes
                    .iter()
                    .filter_map(|h| swarms.get(h).cloned())
                    .collect()
            };
            for handle in handles {
                debug!("LSD: {peer} has one of our torrents");
                handle.peers_discovered(vec![peer], PeerSource::Lsd).await;
            }
        }
    };
    tokio::select! {
        _ = shutdown.cancelled() => {}
        _ = announce => {}
        _ = listen => {}
    }
}

fn bt_search(tcp_port: u16, hashes: &[InfoHash], cookie: &str) -> String {
    let mut message = format!("BT-SEARCH * HTTP/1.1\r\nHost: {GROUP}:{PORT}\r\nPort: {tcp_port}\r\n");
    for hash in hashes {
        let hex: String = hash.as_bytes().iter().map(|b| format!("{b:02X}")).collect();
        message.push_str(&format!("Infohash: {hex}\r\n"));
    }
    message.push_str(&format!("cookie: {cookie}\r\n\r\n\r\n"));
    message
}

/// A socket on the LSD group that other clients on this host can share: every client binds
/// the same port.
fn multicast_socket() -> std::io::Result<UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, PORT).into())?;
    socket.join_multicast_v4(&GROUP, &Ipv4Addr::UNSPECIFIED)?;
    // BEP 14: link-local only
    socket.set_multicast_ttl_v4(1)?;
    socket.set_multicast_loop_v4(true)?;
    socket.set_nonblocking(true)?;
    UdpSocket::from_std(socket.into())
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BtSearch {
    pub port: u16,
    pub info_hashes: Vec<InfoHash>,
    pub cookie: Option<String>,
}

/// Parses a `BT-SEARCH` message: HTTP-style headers, `Port` and one or more `Infohash`
/// (40 hex digits) lines required, `cookie` optional. Header names are case-insensitive.
pub(crate) fn parse_bt_search(bytes: &[u8]) -> Option<BtSearch> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.split("\r\n");
    if !lines.next()?.starts_with("BT-SEARCH ") {
        return None;
    }
    let mut port = None;
    let mut info_hashes = vec![];
    let mut cookie = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "port" => port = value.parse().ok(),
            "infohash" => {
                if value.len() == 40
                    && let Ok(bytes) = (0..40)
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&value[i..i + 2], 16))
                        .collect::<Result<Vec<u8>, _>>()
                {
                    info_hashes.push(InfoHash::from_bytes(&bytes));
                }
            }
            "cookie" => cookie = Some(value.to_string()),
            _ => {}
        }
    }
    if info_hashes.is_empty() {
        return None;
    }
    Some(BtSearch {
        port: port?,
        info_hashes,
        cookie,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parses_a_search_and_rejects_other_things() {
        let msg = b"BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: 6881\r\nInfohash: 000102030405060708090A0B0C0D0E0F10111213\r\ncookie: deadbeef\r\n\r\n\r\n";
        let search = parse_bt_search(msg).unwrap();
        assert_eq!(search.port, 6881);
        assert_eq!(search.cookie.as_deref(), Some("deadbeef"));
        assert_eq!(
            search.info_hashes,
            vec![InfoHash::from_bytes(&(0u8..20).collect::<Vec<_>>())]
        );

        // BEP 14 allows several Infohash lines and lower-case hex; cookie is optional
        let two = b"BT-SEARCH * HTTP/1.1\r\nport: 1\r\ninfohash: 000102030405060708090a0b0c0d0e0f10111213\r\nInfohash: FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\r\n\r\n";
        let search = parse_bt_search(two).unwrap();
        assert_eq!(search.info_hashes.len(), 2);
        assert_eq!(search.cookie, None);

        assert!(parse_bt_search(b"GET / HTTP/1.1\r\n\r\n").is_none());
        assert!(parse_bt_search(b"BT-SEARCH * HTTP/1.1\r\nInfohash: zz\r\n\r\n").is_none());
        assert!(
            parse_bt_search(b"BT-SEARCH * HTTP/1.1\r\nInfohash: 000102030405060708090A0B0C0D0E0F10111213\r\n\r\n")
                .is_none(),
            "no port"
        );
        assert!(parse_bt_search(&[0xff, 0xfe]).is_none());
    }

    #[test]
    fn what_we_send_parses_back() {
        let hashes = [InfoHash::from_bytes(&[1u8; 20]), InfoHash::from_bytes(&[2u8; 20])];
        let search = parse_bt_search(bt_search(6881, &hashes, "c0ffee").as_bytes()).unwrap();
        assert_eq!(search.port, 6881);
        assert_eq!(search.info_hashes, hashes);
        assert_eq!(search.cookie.as_deref(), Some("c0ffee"));
    }
}
