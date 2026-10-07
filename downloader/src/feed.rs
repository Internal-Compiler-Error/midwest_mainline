//! BEP 46: torrents that update through the DHT. A magnet names an ed25519 public key (and
//! optionally a salt), `magnet:?xs=urn:btpk:<hex key>&s=<hex salt>`, and the key's BEP 44
//! mutable item holds the info hash of the torrent's current version, `d2:ih20:...e`. The
//! publisher moves it on by putting a new value at a higher `seq`; followers poll for one.
//!
//! A torrent that follows a key keeps it, and the seq it's at, in its resume file. When a
//! newer version turns up the session adds it as a torrent of its own, which follows the key
//! from then on; the old one keeps seeding until it's removed, marked superseded.

use crate::dht::DhtWatch;
use anyhow::{bail, ensure};
use juicy_bencode::{BencodeItemView, parse_bencode_dict};
use midwest_mainline::dht::client::DhtClient;
use midwest_mainline::dht::item::{MutableItem, SigningKey};
use midwest_mainline::types::InfoHash;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Between polls of a key whose item was found
pub const POLL: Duration = Duration::from_secs(60 * 60);
/// The first retry after a poll that found nothing; doubled each time, up to `POLL`
pub const RETRY: Duration = Duration::from_secs(5 * 60);
/// How long adding a key-only magnet keeps looking for its item before giving up
const RESOLVE_PATIENCE: Duration = Duration::from_secs(10 * 60);

/// What a BEP 46 magnet names: the key, and the salt that picks one of its items
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeedKey {
    pub public: [u8; 32],
    pub salt: Vec<u8>,
}

impl FeedKey {
    pub fn of(signing: &SigningKey, salt: &[u8]) -> Self {
        Self {
            public: signing.verifying_key().to_bytes(),
            salt: salt.to_vec(),
        }
    }

    pub fn public_hex(&self) -> String {
        hex::encode(self.public)
    }
}

/// As `key` and `salt`, in hex.
impl serde::Serialize for FeedKey {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut key = s.serialize_struct("FeedKey", 2)?;
        key.serialize_field("key", &self.public_hex())?;
        key.serialize_field("salt", &hex::encode(&self.salt))?;
        key.end()
    }
}

/// A torrent's place in its key's history.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Feed {
    #[serde(flatten)]
    pub key: FeedKey,
    /// the seq of the item that named this torrent; `None` when it came from the magnet's
    /// own `xt` because the DHT had nothing yet
    pub seq: Option<i64>,
    /// a newer version, this seq, was handed to a torrent of its own; this one only seeds
    pub superseded: Option<i64>,
}

/// The torrent an item names. `ih` is 20 bytes in BEP 46; a 32-byte one is taken as a v2
/// torrent's SHA-256 info hash, whose swarm goes by its first 20 bytes (BEP 52).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    pub info_hash: InfoHash,
    pub info_hash_v2: Option<[u8; 32]>,
}

impl Version {
    pub fn v1(info_hash: InfoHash) -> Self {
        Self {
            info_hash,
            info_hash_v2: None,
        }
    }

    /// The `xt` that names it in a magnet
    pub fn exact_topic(&self) -> String {
        match &self.info_hash_v2 {
            Some(v2) => format!("urn:btmh:1220{}", hex::encode(v2)),
            None => format!("urn:btih:{}", self.info_hash),
        }
    }
}

/// An item found for a key, and the version it names
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub seq: i64,
    pub version: Version,
    pub item: MutableItem,
}

/// The value BEP 46 puts: `{"ih": <info hash>}`
pub fn item_value(version: &Version) -> Vec<u8> {
    let ih: &[u8] = match &version.info_hash_v2 {
        Some(v2) => v2,
        None => version.info_hash.as_bytes(),
    };
    let mut value = format!("d2:ih{}:", ih.len()).into_bytes();
    value.extend_from_slice(ih);
    value.push(b'e');
    value
}

/// The version a BEP 46 value names, if it is one
pub fn parse_item_value(value: &[u8]) -> Option<Version> {
    let (_, dict) = parse_bencode_dict(value).ok()?;
    let Some(BencodeItemView::ByteString(ih)) = dict.get(b"ih".as_slice()) else {
        return None;
    };
    match ih.len() {
        20 => Some(Version::v1(InfoHash::from_bytes(ih))),
        32 => Some(Version {
            info_hash: InfoHash::from_bytes(&ih[..20]),
            info_hash_v2: Some((*ih).try_into().ok()?),
        }),
        _ => None,
    }
}

/// A magnet for `key`, naming `version` too where it's known
pub fn magnet_uri(key: &FeedKey, version: Option<&Version>, name: Option<&str>, trackers: &[String]) -> String {
    let mut uri = String::from("magnet:?");
    if let Some(version) = version {
        uri.push_str(&format!("xt={}&", version.exact_topic()));
    }
    uri.push_str(&format!("xs=urn:btpk:{}", key.public_hex()));
    if !key.salt.is_empty() {
        uri.push_str(&format!("&s={}", hex::encode(&key.salt)));
    }
    let encode = |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>();
    if let Some(name) = name {
        uri.push_str(&format!("&dn={}", encode(name)));
    }
    for tracker in trackers {
        uri.push_str(&format!("&tr={}", encode(tracker)));
    }
    uri
}

/// The newest valid item for `key` that the DHT nodes near it hold, over every address family,
/// if one is newer than `newer_than`. An item whose value names no torrent is skipped.
pub async fn lookup(clients: &[DhtClient], key: &FeedKey, newer_than: Option<i64>) -> Option<Found> {
    let gets = clients
        .iter()
        .map(|client| client.get_mutable(key.public, &key.salt, newer_than));
    futures::future::join_all(gets)
        .await
        .into_iter()
        .flatten()
        .filter_map(|item| match parse_item_value(&item.value) {
            Some(version) => Some(Found {
                seq: item.seq,
                version,
                item,
            }),
            None => {
                tracing::warn!(
                    "BEP 46 item of {} at seq {} names no torrent",
                    key.public_hex(),
                    item.seq
                );
                None
            }
        })
        .max_by_key(|found| found.seq)
}

/// Puts `item` again at the nodes near its target, so it outlives its two hours there; BEP 46
/// asks followers to as well as the publisher. Returns how many nodes took it.
pub async fn keep_alive(clients: &[DhtClient], item: &MutableItem) -> usize {
    let puts = clients.iter().map(|client| client.put_signed(item));
    futures::future::join_all(puts)
        .await
        .into_iter()
        .filter_map(Result::ok)
        .map(|outcome| outcome.stored)
        .sum()
}

/// What `publish` did
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published {
    pub seq: i64,
    /// how many nodes took the item
    pub stored: usize,
}

/// Points `signing`'s item under `salt` at `version`: one seq past what the DHT holds (or 1),
/// put with that as the CAS so a concurrent publish isn't overwritten. If the DHT already
/// names `version`, its item is only put again.
pub async fn publish(
    clients: &[DhtClient],
    signing: &SigningKey,
    salt: &[u8],
    version: &Version,
) -> anyhow::Result<Published> {
    let key = FeedKey::of(signing, salt);
    let current = lookup(clients, &key, None).await;
    if let Some(current) = current.as_ref().filter(|c| c.version == *version) {
        let stored = keep_alive(clients, &current.item).await;
        ensure!(stored > 0, "no DHT node took the item");
        return Ok(Published {
            seq: current.seq,
            stored,
        });
    }
    let cas = current.map(|c| c.seq);
    let seq = cas.map_or(1, |seq| seq + 1);
    let value = item_value(version);
    let puts = clients
        .iter()
        .map(|client| client.put_mutable(signing, salt, seq, value.clone(), cas));
    let mut stored = 0;
    for put in futures::future::join_all(puts).await {
        match put {
            Ok(outcome) => stored += outcome.stored,
            Err(e) => tracing::debug!("BEP 46 publish: {e}"),
        }
    }
    ensure!(stored > 0, "no DHT node took the item at seq {seq}");
    Ok(Published { seq, stored })
}

/// The DHT's clients once a node is up; an error if this client runs without one
pub(crate) async fn clients(mut dht: DhtWatch) -> anyhow::Result<Vec<DhtClient>> {
    match dht.wait_for(Option::is_some).await {
        Ok(handle) => Ok(handle.as_ref().map(|h| h.clients()).unwrap_or_default()),
        Err(_) => bail!("updating torrents (BEP 46) needs the DHT, which is off"),
    }
}

/// What a key-only magnet (or one whose `xt` may be stale) currently names: the DHT's newest
/// item, looked for with growing pauses for up to `RESOLVE_PATIENCE`. With `fallback` (the
/// magnet's own `xt`) one round that finds nothing settles on that instead.
pub(crate) async fn resolve(
    dht: DhtWatch,
    key: &FeedKey,
    fallback: Option<Version>,
    cancel: CancellationToken,
) -> anyhow::Result<(Option<Found>, Version)> {
    let clients = tokio::select! {
        clients = clients(dht) => clients?,
        _ = cancel.cancelled() => bail!("cancelled"),
    };
    let started = tokio::time::Instant::now();
    let mut pause = Duration::from_secs(10);
    loop {
        if let Some(found) = lookup(&clients, key, None).await {
            tracing::info!(
                "BEP 46 key {} is at seq {}: {}",
                key.public_hex(),
                found.seq,
                found.version.info_hash
            );
            let version = found.version;
            return Ok((Some(found), version));
        }
        if let Some(fallback) = fallback {
            tracing::info!(
                "no BEP 46 item for key {} yet, going by the magnet's {}",
                key.public_hex(),
                fallback.info_hash
            );
            return Ok((None, fallback));
        }
        if started.elapsed() + pause > RESOLVE_PATIENCE {
            bail!("no DHT node has an item for key {} (BEP 46)", key.public_hex());
        }
        tracing::info!(
            "no BEP 46 item for key {} yet, asking again in {}s",
            key.public_hex(),
            pause.as_secs()
        );
        tokio::select! {
            _ = tokio::time::sleep(pause) => {}
            _ = cancel.cancelled() => bail!("cancelled"),
        }
        pause = (pause * 2).min(Duration::from_secs(120));
    }
}

/// How long to wait after `failures` polls in a row found nothing (0: the last one did)
pub(crate) fn next_poll(failures: u32, poll: Duration, retry: Duration) -> Duration {
    match failures {
        0 => poll,
        n => retry.saturating_mul(1 << (n - 1).min(16)).min(poll),
    }
}

/// When `follow` polls
#[derive(Debug, Clone, Copy)]
pub(crate) struct Schedule {
    pub first: Duration,
    pub poll: Duration,
    pub retry: Duration,
}

impl Schedule {
    /// A torrent just resolved from the DHT polls an interval later; one picked up from a
    /// resume file has been away for who knows how long, so soon.
    pub fn standard(just_resolved: bool) -> Self {
        Self {
            first: if just_resolved { POLL } else { Duration::from_secs(30) },
            poll: POLL,
            retry: RETRY,
        }
    }
}

/// Polls `feed`'s key for as long as the torrent `current` follows it, putting each item found
/// again to keep it alive. A higher seq that names `current` too only moves the feed's seq on;
/// one that names another torrent is returned, for the caller to hand off. Returns `None` if
/// there's no DHT or the feed was superseded or dropped.
pub(crate) async fn follow(
    dht: DhtWatch,
    feed: watch::Receiver<Option<Feed>>,
    feed_tx: impl Fn(Feed),
    current: InfoHash,
    schedule: Schedule,
) -> Option<Found> {
    let clients = clients(dht).await.ok()?;
    let mut wait = schedule.first;
    let mut failures = 0;
    loop {
        tokio::time::sleep(wait).await;
        let mut following = feed.borrow().clone().filter(|f| f.superseded.is_none())?;
        match lookup(&clients, &following.key, None).await {
            Some(found) if following.seq.is_none_or(|seq| found.seq >= seq) => {
                failures = 0;
                let stored = keep_alive(&clients, &found.item).await;
                tracing::debug!(
                    "BEP 46 key {} at seq {}, put again at {stored} nodes",
                    following.key.public_hex(),
                    found.seq
                );
                if found.version.info_hash != current {
                    return Some(found);
                }
                if following.seq != Some(found.seq) {
                    following.seq = Some(found.seq);
                    feed_tx(following);
                }
            }
            // older than what we follow: a node that missed the newer puts
            Some(_) => failures = 0,
            None => failures += 1,
        }
        wait = next_poll(failures, schedule.poll, schedule.retry);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dht::DhtHandle;
    use midwest_mainline::dht::DhtSession;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use tokio::net::UdpSocket;

    #[test]
    fn values_name_a_v1_or_v2_torrent() {
        let v1 = Version::v1(InfoHash([0xab; 20]));
        let value = item_value(&v1);
        assert_eq!(&value[..9], b"d2:ih20:\xab");
        assert!(midwest_mainline::dht::item::is_canonical_bencode(&value));
        assert_eq!(parse_item_value(&value), Some(v1));

        let v2 = Version {
            info_hash: InfoHash([7; 20]),
            info_hash_v2: Some([7; 32]),
        };
        assert_eq!(parse_item_value(&item_value(&v2)), Some(v2));

        assert_eq!(parse_item_value(b"d2:ih3:abce"), None);
        assert_eq!(parse_item_value(b"d1:xi1ee"), None);
        assert_eq!(parse_item_value(b"12:Hello World!"), None);
    }

    #[test]
    fn polls_back_off_after_failures() {
        let (poll, retry) = (POLL, RETRY);
        assert_eq!(next_poll(0, poll, retry), poll);
        assert_eq!(next_poll(1, poll, retry), retry);
        assert_eq!(next_poll(2, poll, retry), retry * 2);
        assert_eq!(next_poll(4, poll, retry), retry * 8);
        assert_eq!(next_poll(5, poll, retry), poll, "capped at the interval");
        assert_eq!(next_poll(400, poll, retry), poll);
    }

    #[test]
    fn magnets_for_a_key() {
        let key = FeedKey {
            public: [0x11; 32],
            salt: b"v".to_vec(),
        };
        let version = Version::v1(InfoHash([0x22; 20]));
        let uri = magnet_uri(&key, Some(&version), Some("a b"), &["udp://t.test:1".into()]);
        assert_eq!(
            uri,
            format!(
                "magnet:?xt=urn:btih:{}&xs=urn:btpk:{}&s=76&dn=a+b&tr=udp%3A%2F%2Ft.test%3A1",
                "22".repeat(20),
                "11".repeat(32)
            )
        );
        let parsed = crate::magnet::parse_magnet(&uri).unwrap();
        assert_eq!(parsed.feed, Some(key.clone()));
        assert_eq!(parsed.info_hash, version.info_hash);
        assert_eq!(parsed.trackers, ["udp://t.test:1"]);
        assert_eq!(
            crate::magnet::parse_feed(&magnet_uri(&key, None, None, &[])).unwrap(),
            Some(key)
        );
    }

    struct Node {
        session: Arc<DhtSession>,
        run: tokio::task::JoinHandle<()>,
    }

    impl Drop for Node {
        fn drop(&mut self) {
            self.run.abort();
        }
    }

    async fn node(dir: &std::path::Path, name: &str) -> Node {
        let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .unwrap();
        let db = dir.join(format!("{name}.db"));
        let session = Arc::new(DhtSession::with_stable_id(socket, None, db.to_str().unwrap()).unwrap());
        let run = tokio::spawn({
            let session = session.clone();
            async move { session.run().await }
        });
        Node { session, run }
    }

    /// Three stores that know each other, a publisher and a follower that know the first
    async fn network(dir: &std::path::Path) -> (Vec<Node>, Node, Node) {
        let mut stores = vec![];
        for name in ["a", "b", "c"] {
            stores.push(node(dir, name).await);
        }
        let first = stores[0].session.local_addr();
        for store in &stores[1..] {
            store.session.bootstrap(vec![first]).await.unwrap();
        }
        let publisher = node(dir, "publisher").await;
        let follower = node(dir, "follower").await;
        publisher.session.bootstrap(vec![first]).await.unwrap();
        follower.session.bootstrap(vec![first]).await.unwrap();
        (stores, publisher, follower)
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("downloader-feed-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_published_version_is_resolved_and_an_update_followed() {
        let dir = scratch("follow");
        let (_stores, publisher, follower) = network(&dir).await;
        let signing = SigningKey::from_bytes(&[3; 32]);
        let key = FeedKey::of(&signing, b"salt");
        let pub_clients = [publisher.session.handle()];
        let first = Version::v1(InfoHash([1; 20]));
        let second = Version::v1(InfoHash([2; 20]));

        let published = publish(&pub_clients, &signing, b"salt", &first).await.unwrap();
        assert_eq!(published.seq, 1);
        assert!(published.stored >= 3, "{published:?}");
        assert_eq!(
            publish(&pub_clients, &signing, b"salt", &first).await.unwrap().seq,
            1,
            "the same version again is only kept alive"
        );

        let (dht_tx, dht) = watch::channel(Some(DhtHandle::of(follower.session.clone())));
        let (found, version) = resolve(dht.clone(), &key, None, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!((found.unwrap().seq, version), (1, first));
        let other_salt = FeedKey::of(&signing, b"other");
        let (none, fallback) = resolve(dht.clone(), &other_salt, Some(second), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            (none, fallback),
            (None, second),
            "the magnet's xt when the DHT has nothing"
        );

        // the follower comes from the fallback, with no seq: the item for its own version
        // only sets the seq; the next version is handed back
        let (feed_tx, feed_rx) = watch::channel(Some(Feed {
            key: key.clone(),
            seq: None,
            superseded: None,
        }));
        let schedule = Schedule {
            first: Duration::ZERO,
            poll: Duration::from_millis(100),
            retry: Duration::from_millis(100),
        };
        let following = tokio::spawn({
            let feed_tx = feed_tx.clone();
            follow(
                dht.clone(),
                feed_rx.clone(),
                move |f| {
                    feed_tx.send_replace(Some(f));
                },
                first.info_hash,
                schedule,
            )
        });
        let mut seqs = feed_rx.clone();
        tokio::time::timeout(
            Duration::from_secs(10),
            seqs.wait_for(|f| f.as_ref().is_some_and(|f| f.seq == Some(1))),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!following.is_finished());

        let published = publish(&pub_clients, &signing, b"salt", &second).await.unwrap();
        assert_eq!(published.seq, 2);
        let update = tokio::time::timeout(Duration::from_secs(10), following)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!((update.seq, update.version), (2, second));

        // a superseded feed isn't followed
        feed_tx.send_replace(Some(Feed {
            key,
            seq: Some(1),
            superseded: Some(2),
        }));
        let stopped = follow(dht, feed_rx, |_| {}, first.info_hash, schedule);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), stopped).await.unwrap(),
            None
        );
        drop(dht_tx);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
