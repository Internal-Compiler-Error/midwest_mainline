//! HTTP(S) trackers: BEP 3 announces (with BEP 23's compact peers, BEP 7's `peers6` and BEP
//! 24's `external ip`) and BEP 48 scrapes.

use super::tracker::{AnnounceEvent, Announced, NUMWANT, Progress, Tracker, announce_interval, preview};
use super::{SwarmCounts, compact_peers};
use crate::settings::{HTTP_TRACKER_TIMEOUT, TRACKER_RESPONSE_MAX};
use anyhow::{Context, bail};
use juicy_bencode::BencodeItemView;
use midwest_mainline::types::InfoHash;
use reqwest::{Client, StatusCode};
use std::collections::BTreeMap;
use std::fmt::Write;
use std::net::{IpAddr, SocketAddr};
use std::sync::LazyLock;
use tracing::{debug, warn};
use url::Url;

/// One shared client, so announces reuse connections; the timeout covers the whole request.
static HTTP_CLIENT: LazyLock<Client> = LazyLock::new(|| {
    Client::builder()
        .timeout(HTTP_TRACKER_TIMEOUT)
        .build()
        .expect("the tracker HTTP client should build")
});

impl AnnounceEvent {
    fn http_str(self) -> Option<&'static str> {
        match self {
            AnnounceEvent::Regular => None,
            AnnounceEvent::Started => Some("started"),
            AnnounceEvent::Completed => Some("completed"),
            AnnounceEvent::Stopped => Some("stopped"),
        }
    }
}

/// One announce: the peers, and the interval until the next.
pub(super) async fn announce(tracker: &Tracker, event: AnnounceEvent) -> anyhow::Result<Announced> {
    let identity = &tracker.args.identity;
    let url = announce_url(
        &tracker.url,
        &tracker.args.info_hash,
        &identity.peer_id,
        identity.serving.port(),
        tracker.progress(),
        event,
    )?;
    debug!("Announcing to {url}");
    let (status, body) = get(url, "the announce").await?;
    debug!("Tracker [{}] responded {status}: {}", tracker.url, preview(&body));

    let response = parse_response(&body).map_err(|e| {
        if status.is_success() {
            e
        } else {
            e.context(format!("HTTP {status}"))
        }
    })?;
    if let Some(warning) = &response.warning {
        warn!("Tracker [{}] warns: {warning}", tracker.url);
    }
    if let Some(ip) = response.external_ip {
        let _ = tracker
            .args
            .external
            .vote(ip, tracker.url.host_str().unwrap_or_default());
    }
    Ok(response.announced)
}

/// BEP 48: the tracker's count of the swarm, from its scrape URL.
pub(super) async fn scrape(tracker: &Tracker) -> anyhow::Result<SwarmCounts> {
    let info_hash = &tracker.args.info_hash;
    let url = scrape_url(&tracker.url, info_hash).context("the tracker has no scrape URL")?;
    let (_, body) = get(url, "the scrape").await?;
    parse_scrape(&body, info_hash)
}

/// A GET's status and body. Errors leave the URL out: private trackers' URLs carry a passkey,
/// and errors end up in the GUI.
async fn get(url: Url, what: &str) -> anyhow::Result<(StatusCode, Vec<u8>)> {
    let response = HTTP_CLIENT
        .get(url)
        .send()
        .await
        .map_err(|e| e.without_url())
        .with_context(|| format!("sending {what}"))?;
    let status = response.status();
    Ok((status, read_capped(response, TRACKER_RESPONSE_MAX).await?))
}

/// Reads a response body, giving up past `cap` bytes rather than buffering whatever a broken
/// tracker sends.
async fn read_capped(mut response: reqwest::Response, cap: usize) -> anyhow::Result<Vec<u8>> {
    if response.content_length().is_some_and(|len| len > cap as u64) {
        bail!("tracker response is larger than {cap} bytes");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| e.without_url())
        .context("reading the tracker's response")?
    {
        if body.len() + chunk.len() > cap {
            bail!("tracker response is larger than {cap} bytes");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// RFC 3986 percent-encoding of everything outside the unreserved set. BEP 3 sends the info
/// hash and peer ID as raw bytes this way, so no tracker has to guess whether `+` means a space.
pub(crate) fn percent_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

/// The URL of one announce. Private trackers' announce URLs carry a passkey in their own
/// query, which our parameters join rather than replace.
fn announce_url(
    tracker: &Url,
    info_hash: &InfoHash,
    peer_id: &[u8; 20],
    port: u16,
    progress: Progress,
    event: AnnounceEvent,
) -> anyhow::Result<Url> {
    let mut base = tracker.clone();
    base.set_fragment(None);
    let separator = match base.query() {
        None => "?",
        Some("") => "",
        Some(_) => "&",
    };
    let Progress {
        uploaded,
        downloaded,
        left,
    } = progress;
    let url = format!(
        "{base}{separator}info_hash={}&peer_id={}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&compact=1&numwant={NUMWANT}{}",
        percent_encode(&info_hash.0),
        percent_encode(peer_id),
        event.http_str().map(|e| format!("&event={e}")).unwrap_or_default(),
    );
    Url::parse(&url).context("building the announce URL")
}

/// BEP 48: the scrape URL is the announce URL with its last path segment's leading
/// `announce` swapped for `scrape`, and the info hash in the query. A tracker whose announce
/// URL doesn't end that way has no scrape.
fn scrape_url(announce: &Url, info_hash: &InfoHash) -> Option<Url> {
    let mut url = announce.clone();
    let last = url.path_segments()?.next_back()?.to_string();
    let rest = last.strip_prefix("announce")?;
    url.path_segments_mut().ok()?.pop().push(&format!("scrape{rest}"));
    let hash = percent_encode(&info_hash.0);
    let query = match url.query() {
        Some(q) if !q.is_empty() => format!("{q}&info_hash={hash}"),
        _ => format!("info_hash={hash}"),
    };
    url.set_query(Some(&query));
    Some(url)
}

type Dict<'a> = BTreeMap<&'a [u8], BencodeItemView<'a>>;

/// A tracker's bencoded answer, unless it's not bencode or carries a `failure reason`.
fn parse_dict<'a>(body: &'a [u8], what: &str) -> anyhow::Result<Dict<'a>> {
    let Ok((_, mut dict)) = juicy_bencode::parse_bencode_dict(body) else {
        bail!("{what} is invalid bencode: {}", preview(body));
    };
    if let Some(BencodeItemView::ByteString(reason)) = dict.remove(b"failure reason".as_slice()) {
        bail!("tracker: {}", preview(reason));
    }
    Ok(dict)
}

fn integer(dict: &mut Dict, key: &[u8]) -> Option<i64> {
    match dict.remove(key) {
        Some(BencodeItemView::Integer(n)) => Some(n),
        _ => None,
    }
}

/// The `complete`, `incomplete` and `downloaded` counts an announce or scrape answer has.
fn counts(dict: &mut Dict) -> SwarmCounts {
    let mut count = |key: &[u8]| integer(dict, key).and_then(|n| u32::try_from(n).ok());
    SwarmCounts {
        seeders: count(b"complete"),
        leechers: count(b"incomplete"),
        downloaded: count(b"downloaded"),
    }
}

/// BEP 48 scrape response: `files` maps each 20-byte info hash to its counts.
fn parse_scrape(body: &[u8], info_hash: &InfoHash) -> anyhow::Result<SwarmCounts> {
    let mut dict = parse_dict(body, "scrape response")?;
    let Some(BencodeItemView::Dictionary(mut files)) = dict.remove(b"files".as_slice()) else {
        bail!("scrape response has no files");
    };
    let Some(BencodeItemView::Dictionary(mut ours)) = files.remove(info_hash.0.as_slice()) else {
        bail!("scrape response doesn't mention this torrent");
    };
    Ok(counts(&mut ours))
}

/// What an announce response says.
#[derive(Debug, PartialEq, Eq)]
struct Response {
    announced: Announced,
    warning: Option<String>,
    /// BEP 24: our address as the tracker saw it
    external_ip: Option<IpAddr>,
}

fn parse_response(body: &[u8]) -> anyhow::Result<Response> {
    let mut dict = parse_dict(body, "tracker response")?;
    let warning = match dict.remove(b"warning message".as_slice()) {
        Some(BencodeItemView::ByteString(warning)) => Some(preview(warning)),
        _ => None,
    };
    let Some(interval) = integer(&mut dict, b"interval") else {
        match warning {
            Some(warning) => bail!("tracker sent no interval, warning: {warning}"),
            None => bail!("tracker sent no interval"),
        }
    };
    let min_interval = integer(&mut dict, b"min interval");
    let external_ip = match dict.remove(b"external ip".as_slice()) {
        Some(BencodeItemView::ByteString(ip)) => match ip.len() {
            4 => Some(IpAddr::from(<[u8; 4]>::try_from(ip).expect("4 bytes"))),
            16 => Some(IpAddr::from(<[u8; 16]>::try_from(ip).expect("16 bytes"))),
            _ => None,
        },
        _ => None,
    };
    let mut peers: Vec<SocketAddr> = match dict.remove(b"peers".as_slice()) {
        Some(BencodeItemView::ByteString(compact)) => compact_peers(compact, 6).collect(),
        // trackers may ignore compact=1 and send the original list of dicts
        Some(BencodeItemView::List(entries)) => entries.iter().filter_map(dict_peer).collect(),
        _ => vec![],
    };
    if let Some(BencodeItemView::ByteString(compact)) = dict.remove(b"peers6".as_slice()) {
        peers.extend(compact_peers(compact, 18));
    }
    Ok(Response {
        announced: Announced {
            peers,
            interval: announce_interval(interval, min_interval),
            counts: counts(&mut dict),
        },
        warning,
        external_ip,
    })
}

/// A peer of BEP 3's original, non-compact list: a dict with an `ip` (an address literal; we
/// don't resolve names a tracker hands out) and a `port`.
fn dict_peer(entry: &BencodeItemView) -> Option<SocketAddr> {
    let BencodeItemView::Dictionary(entry) = entry else {
        return None;
    };
    let (Some(BencodeItemView::ByteString(ip)), Some(BencodeItemView::Integer(port))) =
        (entry.get(b"ip".as_slice()), entry.get(b"port".as_slice()))
    else {
        return None;
    };
    let ip = std::str::from_utf8(ip).ok()?.parse::<IpAddr>().ok()?;
    Some(SocketAddr::new(ip, u16::try_from(*port).ok()?))
}

#[cfg(test)]
mod test {
    use super::super::test::{announcing, row};
    use super::*;
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn announce_url_for(tracker: &str, event: AnnounceEvent) -> String {
        let mut info_hash = [b'a'; 20];
        info_hash[0] = b' ';
        info_hash[1] = 0xff;
        let progress = Progress {
            uploaded: 1,
            downloaded: 2,
            left: 3,
        };
        announce_url(
            &Url::parse(tracker).unwrap(),
            &InfoHash::from_bytes(&info_hash),
            &[b'-'; 20],
            6881,
            progress,
            event,
        )
        .unwrap()
        .to_string()
    }

    #[test]
    fn announce_url_joins_an_existing_query() {
        let params = format!(
            "info_hash=%20%FF{}&peer_id={}&port=6881&uploaded=1&downloaded=2&left=3&compact=1&numwant=200",
            "a".repeat(18),
            "-".repeat(20)
        );
        assert_eq!(
            announce_url_for("http://t.test/announce", AnnounceEvent::Regular),
            format!("http://t.test/announce?{params}")
        );
        assert_eq!(
            announce_url_for("https://t.test/announce?passkey=abc#frag", AnnounceEvent::Started),
            format!("https://t.test/announce?passkey=abc&{params}&event=started")
        );
        assert_eq!(
            announce_url_for("http://t.test/announce?", AnnounceEvent::Regular),
            format!("http://t.test/announce?{params}")
        );
    }

    #[test]
    fn response_failure_reason_is_the_error() {
        let e = parse_response(b"d14:failure reason17:unregistered hashe").unwrap_err();
        assert_eq!(format!("{e:#}"), "tracker: unregistered hash");

        let e = parse_response(b"d15:warning message4:slowe").unwrap_err();
        assert_eq!(format!("{e:#}"), "tracker sent no interval, warning: slow");
        assert!(parse_response(b"<html>").is_err());
    }

    #[test]
    fn response_peers_and_intervals() {
        let mut body = b"d8:intervali0e12:min intervali900e5:peers6:".to_vec();
        body.extend_from_slice(&[10, 0, 0, 1, 0x1a, 0xe1]);
        body.extend_from_slice(b"6:peers618:");
        body.extend_from_slice(&[0; 15]);
        body.extend_from_slice(&[1, 0x1a, 0xe2]);
        body.extend_from_slice(
            b"15:warning message2:hi8:completei513e11:external ip4:\xcb\x00\x71\x0910:incompletei30ee",
        );
        let response = parse_response(&body).unwrap();
        assert_eq!(
            response,
            Response {
                announced: Announced {
                    peers: vec!["10.0.0.1:6881".parse().unwrap(), "[::1]:6882".parse().unwrap()],
                    interval: Duration::from_secs(900),
                    counts: SwarmCounts {
                        seeders: Some(513),
                        leechers: Some(30),
                        downloaded: None,
                    },
                },
                warning: Some("hi".into()),
                external_ip: Some("203.0.113.9".parse().unwrap()),
            }
        );

        let body = b"d8:intervali60e5:peersld2:ip9:127.0.0.14:porti7eed2:ip3:bad4:porti1eeee";
        let response = parse_response(body).unwrap();
        assert_eq!(response.announced.peers, ["127.0.0.1:7".parse::<SocketAddr>().unwrap()]);
    }

    #[test]
    fn scrape_urls_and_responses() {
        let hash = InfoHash([0x41; 20]);
        let url = |s: &str| scrape_url(&Url::parse(s).unwrap(), &hash).map(|u| u.to_string());
        assert_eq!(
            url("https://torrent.ubuntu.com/announce").as_deref(),
            Some("https://torrent.ubuntu.com/scrape?info_hash=AAAAAAAAAAAAAAAAAAAA")
        );
        assert_eq!(
            url("http://t.test/x/announce.php?passkey=k").as_deref(),
            Some("http://t.test/x/scrape.php?passkey=k&info_hash=AAAAAAAAAAAAAAAAAAAA")
        );
        assert_eq!(url("http://t.test/a"), None, "BEP 48: no 'announce' segment, no scrape");

        let mut body = b"d5:filesd20:".to_vec();
        body.extend_from_slice(&hash.0);
        body.extend_from_slice(b"d8:completei5e10:downloadedi50e10:incompletei7eeee");
        assert_eq!(
            parse_scrape(&body, &hash).unwrap(),
            SwarmCounts {
                seeders: Some(5),
                leechers: Some(7),
                downloaded: Some(50),
            }
        );
        assert!(parse_scrape(&body, &InfoHash([0; 20])).is_err(), "not our torrent");
        assert!(parse_scrape(b"d14:failure reason4:nopee", &hash).is_err());
    }

    /// A one-shot HTTP server answering with `response`, handing back the request line.
    async fn http_server(response: Vec<u8>) -> (String, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let served = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let n = stream.read(&mut request).await.unwrap();
            let _ = stream.write_all(&response).await;
            let _ = stream.shutdown().await;
            String::from_utf8_lossy(&request[..n])
                .lines()
                .next()
                .unwrap_or_default()
                .to_owned()
        });
        (base, served)
    }

    #[tokio::test]
    async fn response_bodies_are_capped() {
        let mut response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n".to_vec();
        response.extend(std::iter::repeat_n(b'x', 64 * 1024));
        let (base, served) = http_server(response).await;
        let response = HTTP_CLIENT.get(&base).send().await.unwrap();
        let e = read_capped(response, 1024).await.unwrap_err();
        assert!(format!("{e:#}").contains("larger than 1024 bytes"), "{e:#}");
        served.await.unwrap();

        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello".to_vec();
        let (base, served) = http_server(response).await;
        let response = HTTP_CLIENT.get(&base).send().await.unwrap();
        assert_eq!(read_capped(response, 5).await.unwrap(), b"hello");
        served.await.unwrap();
    }

    /// A private tracker's passkey survives into the request, and its failure reason is what
    /// the announce fails with, even on a non-200 answer.
    #[tokio::test]
    async fn announce_reports_the_failure_reason() {
        let body = b"d14:failure reason11:bad passkeye";
        let mut response = format!(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        let (base, served) = http_server(response).await;
        let (events, _rx) = mpsc::channel(1);
        let tracker = Tracker::new(
            Url::parse(&format!("{base}/announce?passkey=s3cret")).unwrap(),
            announcing(&events),
            row(),
        );
        let e = announce(&tracker, AnnounceEvent::Started).await.unwrap_err();
        assert_eq!(format!("{e:#}"), "HTTP 403 Forbidden: tracker: bad passkey");
        let request = served.await.unwrap();
        assert!(
            request.starts_with("GET /announce?passkey=s3cret&info_hash=%02%02"),
            "{request}"
        );
        assert!(request.contains("&event=started "), "{request}");
    }
}
