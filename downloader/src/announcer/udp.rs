//! UDP trackers (BEP 15): a connect exchange buys a connection ID, good for a minute, which
//! every announce and scrape carries; requests are retransmitted on a doubling timeout.

use super::tracker::{AnnounceEvent, Announced, NUMWANT, Tracker, announce_interval, preview};
use super::{SwarmCounts, compact_peers};
use crate::settings::{UDP_CONNECTION_ID_TTL, UDP_TRACKER_ATTEMPTS, UDP_TRACKER_TIMEOUT};
use anyhow::{Context, anyhow, bail};
use rand::RngExt;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::Duration;
use tokio::net::{UdpSocket, lookup_host};
use tokio::time::Instant;
use tracing::{debug, info};
use url::{Host, Url};
use zerocopy::network_endian::{I32, I64, U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// Room for the largest UDP datagram, so a long peer list is never silently truncated.
const MAX_DATAGRAM: usize = 64 * 1024;

#[repr(i32)]
enum Action {
    Connect = 0,
    Announce = 1,
    Scrape = 2,
    Error = 3,
}

impl AnnounceEvent {
    /// BEP 15 encodes the event as an int, in an order of its own.
    fn udp_code(self) -> i32 {
        match self {
            AnnounceEvent::Regular => 0,
            AnnounceEvent::Completed => 1,
            AnnounceEvent::Started => 2,
            AnnounceEvent::Stopped => 3,
        }
    }
}

/// A connection ID, and when it arrived.
type Connection = (i64, Instant);

pub(super) struct UdpClient {
    /// connected to the tracker's address that answered
    socket: Option<UdpSocket>,
    connection: Option<Connection>,
    /// BEP 15: lets the tracker tell us apart if our address changes; fixed for our lifetime
    key: u32,
}

impl UdpClient {
    pub fn new() -> Self {
        UdpClient {
            socket: None,
            connection: None,
            key: rand::rng().random(),
        }
    }

    pub fn disconnect(&mut self) {
        self.socket = None;
        self.connection = None;
    }

    pub fn is_connected(&self) -> bool {
        self.socket.is_some()
    }

    /// One announce: the peers, and the interval until the next. Connects first if need be.
    pub async fn announce(&mut self, tracker: &Tracker, event: AnnounceEvent) -> anyhow::Result<Announced> {
        if self.socket.is_none() {
            let (socket, connection) = open(&tracker.url).await?;
            self.socket = Some(socket);
            self.connection = Some(connection);
        }

        #[derive(FromBytes, IntoBytes, Immutable)]
        #[repr(C)]
        struct Request {
            connection_id: I64,
            action: I32,
            transaction_id: I32,
            info_hash: [u8; 20],
            peer_id: [u8; 20],
            downloaded: I64,
            left: I64,
            uploaded: I64,
            event: I32,
            /// 0: the tracker takes the address the datagram came from
            ip: U32,
            key: U32,
            num_want: I32,
            port: U16,
            /// BEP 41's option list, empty
            extensions: U16,
        }

        let transaction_id = rand::rng().random::<i32>();
        let progress = tracker.progress();
        let signed = |n: u64| i64::try_from(n).unwrap_or(i64::MAX);
        let request = Request {
            connection_id: 0.into(),
            action: (Action::Announce as i32).into(),
            transaction_id: transaction_id.into(),
            info_hash: tracker.announcer.info_hash.0,
            peer_id: tracker.announcer.identity.peer_id,
            downloaded: signed(progress.downloaded).into(),
            left: signed(progress.left).into(),
            uploaded: signed(progress.uploaded).into(),
            event: event.udp_code().into(),
            ip: 0.into(),
            key: self.key.into(),
            num_want: (NUMWANT as i32).into(),
            port: tracker.announcer.identity.serving.port().into(),
            extensions: 0.into(),
        };
        let reply = self.request(request.as_bytes(), transaction_id, "announce").await?;
        let is_v6 = matches!(
            self.socket.as_ref().map(UdpSocket::peer_addr),
            Some(Ok(SocketAddr::V6(_)))
        );
        let announced = parse_announce(&reply, is_v6)?;
        debug!("Tracker [{}] announce returned {:?}", tracker.url, announced.peers);
        Ok(announced)
    }

    /// Seeders, completed downloads and leechers for our info hash, over the connection the
    /// announce left open.
    pub async fn scrape(&mut self, tracker: &Tracker) -> anyhow::Result<SwarmCounts> {
        #[derive(FromBytes, IntoBytes, Immutable)]
        #[repr(C)]
        struct Request {
            connection_id: I64,
            action: I32,
            transaction_id: I32,
            info_hash: [u8; 20],
        }

        let transaction_id = rand::rng().random::<i32>();
        let request = Request {
            connection_id: 0.into(),
            action: (Action::Scrape as i32).into(),
            transaction_id: transaction_id.into(),
            info_hash: tracker.announcer.info_hash.0,
        };
        let reply = self.request(request.as_bytes(), transaction_id, "scrape").await?;
        parse_scrape(&reply)
    }

    /// Sends `request` (its first 8 bytes, the connection ID, filled in here) on BEP 15's
    /// retransmit schedule until the answer comes, connecting again whenever the connection ID
    /// has expired: retransmits can outlast it.
    async fn request(&mut self, request: &[u8], transaction_id: i32, what: &str) -> anyhow::Result<Vec<u8>> {
        let socket = self.socket.as_ref().context("not connected to the tracker")?;
        let mut request = request.to_vec();
        for n in 0..UDP_TRACKER_ATTEMPTS {
            let connection_id = match self.connection {
                Some((id, at)) if at.elapsed() < UDP_CONNECTION_ID_TTL => id,
                _ => {
                    let connection = connect(socket, UDP_TRACKER_ATTEMPTS).await?;
                    self.connection = Some(connection);
                    connection.0
                }
            };
            request[..8].copy_from_slice(&connection_id.to_be_bytes());
            if let Some(reply) = exchange(socket, &request, transaction_id, retransmit_wait(n)).await? {
                return Ok(reply);
            }
        }
        bail!("tracker didn't answer the {what} after {UDP_TRACKER_ATTEMPTS} tries")
    }
}

/// Resolves the tracker and connects to the first address that answers, IPv4 first.
async fn open(url: &Url) -> anyhow::Result<(UdpSocket, Connection)> {
    let port = url.port().context("tracker URL has no port")?;
    let mut addresses: Vec<SocketAddr> = match url.host().context("tracker URL has no host")? {
        Host::Ipv4(ip) => vec![SocketAddr::new(ip.into(), port)],
        Host::Ipv6(ip) => vec![SocketAddr::new(ip.into(), port)],
        Host::Domain(domain) => lookup_host((domain, port))
            .await
            .with_context(|| format!("resolving {domain}"))?
            .collect(),
    };
    addresses.sort_by_key(SocketAddr::is_ipv6);
    addresses.dedup();
    debug!("Tracker [{url}] resolved to {addresses:?}");

    let mut last_error = None;
    for (i, &address) in addresses.iter().enumerate() {
        // only the last candidate gets the whole retransmit schedule, so a dead address
        // ahead of it costs one timeout rather than all of them
        let attempts = if i + 1 == addresses.len() {
            UDP_TRACKER_ATTEMPTS
        } else {
            1
        };
        match open_at(address, attempts).await {
            Ok(opened) => {
                info!("Tracker [{url}] connected on {address}");
                return Ok(opened);
            }
            Err(e) => {
                debug!("Tracker [{url}] on {address}: {e:#}");
                last_error = Some(e.context(format!("on {address}")));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("tracker did not resolve to any address")))
}

async fn open_at(address: SocketAddr, attempts: u32) -> anyhow::Result<(UdpSocket, Connection)> {
    let ours: SocketAddr = match address {
        SocketAddr::V4(_) => SocketAddrV4::new(crate::defs::BIND_V4, 0).into(),
        SocketAddr::V6(_) => SocketAddrV6::new(crate::defs::BIND_V6, 0, 0, 0).into(),
    };
    let socket = UdpSocket::bind(ours)
        .await
        .with_context(|| format!("binding a UDP socket on {ours}"))?;
    socket.connect(address).await.context("connecting the UDP socket")?;
    let connection = connect(&socket, attempts).await?;
    Ok((socket, connection))
}

/// How long to wait for an answer to the `n`th try (from 0) of a request.
fn retransmit_wait(n: u32) -> Duration {
    UDP_TRACKER_TIMEOUT * 2u32.pow(n)
}

/// Sends `request` once and waits up to `wait` for the datagram answering it, matched on the
/// transaction ID every response carries at bytes 4..8; anything else is a late answer to an
/// earlier request, or noise. `None` means it's time to retransmit. The tracker's error
/// action comes back as an error carrying its message.
async fn exchange(
    socket: &UdpSocket,
    request: &[u8],
    transaction_id: i32,
    wait: Duration,
) -> anyhow::Result<Option<Vec<u8>>> {
    socket.send(request).await.context("sending to the tracker")?;
    let deadline = Instant::now() + wait;
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let Ok(received) = tokio::time::timeout_at(deadline, socket.recv(&mut buf)).await else {
            return Ok(None);
        };
        let len = received.context("receiving from the tracker")?;
        let reply = &buf[..len];
        if len < 8 || reply[4..8] != transaction_id.to_be_bytes() {
            continue;
        }
        if reply[..4] == (Action::Error as i32).to_be_bytes() {
            bail!("tracker: {}", preview(&reply[8..]));
        }
        buf.truncate(len);
        return Ok(Some(buf));
    }
}

/// The connect exchange: a connection ID, and when it arrived.
async fn connect(socket: &UdpSocket, attempts: u32) -> anyhow::Result<Connection> {
    #[derive(FromBytes, IntoBytes, Immutable)]
    #[repr(C)]
    struct Request {
        protocol_id: I64,
        action: I32,
        transaction_id: I32,
    }

    #[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
    #[repr(C)]
    struct Response {
        action: I32,
        transaction_id: I32,
        connection_id: I64,
    }

    let transaction_id = rand::rng().random::<i32>();
    let request = Request {
        protocol_id: 0x41727101980.into(),
        action: (Action::Connect as i32).into(),
        transaction_id: transaction_id.into(),
    };
    for n in 0..attempts {
        let Some(reply) = exchange(socket, request.as_bytes(), transaction_id, retransmit_wait(n)).await? else {
            continue;
        };
        let Ok((response, _)) = Response::ref_from_prefix(&reply) else {
            bail!("tracker's connect response is too short");
        };
        if response.action.get() != Action::Connect as i32 {
            bail!("tracker answered connect with action {}", response.action.get());
        }
        return Ok((response.connection_id.get(), Instant::now()));
    }
    bail!("tracker didn't answer connect after {attempts} tries")
}

/// A count of the swarm as BEP 15 sends it, signed.
fn count(n: I32) -> Option<u32> {
    u32::try_from(n.get()).ok()
}

/// An announce response. BEP 15 only ever defined 6-byte peer entries; common tracker
/// software sends 18-byte (IPv6) ones when the announce came over IPv6, so `is_v6` is which
/// family we reached the tracker over, not anything in the response.
fn parse_announce(reply: &[u8], is_v6: bool) -> anyhow::Result<Announced> {
    #[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
    #[repr(C)]
    struct Header {
        action: I32,
        transaction_id: I32,
        interval_secs: I32,
        leechers: I32,
        seeders: I32,
    }

    let Ok((header, peers)) = Header::ref_from_prefix(reply) else {
        bail!("announce response is shorter than a header");
    };
    if header.action.get() != Action::Announce as i32 {
        bail!("tracker answered announce with action {}", header.action.get());
    }
    let peer_size = if is_v6 { 18 } else { 6 };
    if peers.len() % peer_size != 0 {
        bail!("announce response's peers are not a multiple of {peer_size} bytes");
    }
    Ok(Announced {
        peers: compact_peers(peers, peer_size).collect(),
        interval: announce_interval(header.interval_secs.get().into(), None),
        counts: SwarmCounts {
            seeders: count(header.seeders),
            leechers: count(header.leechers),
            downloaded: None,
        },
    })
}

fn parse_scrape(reply: &[u8]) -> anyhow::Result<SwarmCounts> {
    #[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
    #[repr(C)]
    struct Response {
        action: I32,
        transaction_id: I32,
        seeders: I32,
        completed: I32,
        leechers: I32,
    }

    let Ok((response, _)) = Response::ref_from_prefix(reply) else {
        bail!("scrape response is too short");
    };
    if response.action.get() != Action::Scrape as i32 {
        bail!("tracker answered scrape with action {}", response.action.get());
    }
    Ok(SwarmCounts {
        seeders: count(response.seeders),
        leechers: count(response.leechers),
        downloaded: count(response.completed),
    })
}

#[cfg(test)]
mod test {
    use super::super::test::{announcer, row};
    use super::*;
    use crate::settings::ANNOUNCE_INTERVAL_MIN;
    use tokio::sync::mpsc;

    fn ints(ns: &[i32]) -> Vec<u8> {
        ns.iter().flat_map(|n| n.to_be_bytes()).collect()
    }

    #[test]
    fn announce_response_parsing() {
        let mut reply = ints(&[1, 7, -30, 4, 12]);
        reply.extend_from_slice(&[10, 0, 0, 1, 0x1a, 0xe1]);
        let announced = parse_announce(&reply, false).unwrap();
        assert_eq!(announced.peers, ["10.0.0.1:6881".parse::<SocketAddr>().unwrap()]);
        assert_eq!(announced.interval, ANNOUNCE_INTERVAL_MIN);
        assert_eq!(
            (announced.counts.leechers, announced.counts.seeders),
            (Some(4), Some(12))
        );
        assert!(parse_announce(&reply, true).is_err(), "6 bytes is no IPv6 peer");
        assert!(parse_announce(&ints(&[2, 7, 60, 4, 12]), false).is_err());
        assert!(parse_announce(&reply[..10], false).is_err());
    }

    #[test]
    fn scrape_response_parsing() {
        assert_eq!(
            parse_scrape(&ints(&[2, 7, 5, 50, -1])).unwrap(),
            SwarmCounts {
                seeders: Some(5),
                leechers: None,
                downloaded: Some(50),
            }
        );
        assert!(parse_scrape(&ints(&[1, 7, 5, 50, 1])).is_err(), "not a scrape");
        assert!(parse_scrape(&ints(&[2, 7, 5])).is_err(), "short");
    }

    /// An unanswered request comes back as time to retransmit; answers to other transactions
    /// are skipped; the tracker's error action carries its message.
    #[tokio::test]
    async fn exchange_matches_transactions_and_times_out() {
        let tracker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.connect(tracker.local_addr().unwrap()).await.unwrap();

        let wait = Duration::from_millis(100);
        assert_eq!(exchange(&socket, b"hello", 42, wait).await.unwrap(), None);
        let mut buf = [0; 64];
        tracker.recv_from(&mut buf).await.unwrap();

        let answer = tokio::spawn(async move {
            let (_, from) = tracker.recv_from(&mut buf).await.unwrap();
            tracker.send_to(&ints(&[1, 41]), from).await.unwrap();
            let mut error = ints(&[3, 42]);
            error.extend_from_slice(b"go away");
            tracker.send_to(&error, from).await.unwrap();
        });
        let e = exchange(&socket, b"hello", 42, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert_eq!(format!("{e:#}"), "tracker: go away");
        answer.await.unwrap();
    }

    /// Connect, then announce with the connection ID the tracker handed out, then scrape on the
    /// same connection.
    #[tokio::test]
    async fn announce_and_scrape_against_a_fake_tracker() {
        let tracker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("udp://{}", tracker.local_addr().unwrap())).unwrap();
        let served = tokio::spawn(async move {
            let mut buf = [0; 1500];
            let (n, from) = tracker.recv_from(&mut buf).await.unwrap();
            assert_eq!(n, 16);
            let mut reply = ints(&[0]);
            reply.extend_from_slice(&buf[12..16]);
            reply.extend_from_slice(&77i64.to_be_bytes());
            tracker.send_to(&reply, from).await.unwrap();

            let (n, from) = tracker.recv_from(&mut buf).await.unwrap();
            assert_eq!(n, 100, "98 bytes and an empty BEP 41 option list");
            assert_eq!(buf[..8], 77i64.to_be_bytes(), "connection ID");
            assert_eq!(buf[80..84], 2i32.to_be_bytes(), "event=started");
            let mut reply = ints(&[1]);
            reply.extend_from_slice(&buf[12..16]);
            reply.extend_from_slice(&ints(&[0, 0, 1]));
            reply.extend_from_slice(&[10, 0, 0, 1, 0x1a, 0xe1]);
            tracker.send_to(&reply, from).await.unwrap();

            let (n, from) = tracker.recv_from(&mut buf).await.unwrap();
            assert_eq!(n, 36);
            assert_eq!(buf[..8], 77i64.to_be_bytes(), "connection ID");
            let mut reply = ints(&[2]);
            reply.extend_from_slice(&buf[12..16]);
            reply.extend_from_slice(&ints(&[1, 9, 0]));
            tracker.send_to(&reply, from).await.unwrap();
        });
        let (events, _rx) = mpsc::channel(1);
        let tracker = Tracker::new(url, announcer(&events), row());
        let mut client = UdpClient::new();
        let announced = client.announce(&tracker, AnnounceEvent::Started).await.unwrap();
        assert_eq!(announced.peers, ["10.0.0.1:6881".parse::<SocketAddr>().unwrap()]);
        assert_eq!(announced.interval, ANNOUNCE_INTERVAL_MIN);
        let scraped = client.scrape(&tracker).await.unwrap();
        assert_eq!((scraped.seeders, scraped.downloaded), (Some(1), Some(9)));
        served.await.unwrap();
    }
}
