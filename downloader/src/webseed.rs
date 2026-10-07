//! BEP 19 (GetRight-style) web seeds: an HTTP(S) server holding the torrent's files, which
//! therefore has every piece and never chokes.
//!
//! The swarm treats each URL as one more source next to its peers (see `TorrentSwarm`): it
//! hands a seed runs of consecutive pieces, and a job here fetches a run with one HTTP Range
//! request per file it touches, cutting the body into blocks that go through the same
//! assembly and hash check as blocks from peers.

use crate::announcer::percent_encode;
use crate::peer::PeerStatistics;
use crate::settings::{
    BLOCK_SIZE, WEB_SEED_BACKOFF, WEB_SEED_BACKOFF_MAX, WEB_SEED_CONNECT_TIMEOUT, WEB_SEED_JOBS, WEB_SEED_MAX_RUN,
    WEB_SEED_PIPELINE, WEB_SEED_READ_TIMEOUT, WEB_SEED_RUN_TARGET,
};
use crate::torrent::Torrent;
use crate::wire::Piece;
use bytes::Bytes;
use reqwest::{StatusCode, Url, header};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::net::{Ipv6Addr, SocketAddr};
use std::ops::RangeInclusive;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::DropGuard;
use tracing::Instrument;

/// One client for every web seed of every torrent: connections to a host are pooled and kept
/// alive across requests, and HTTP/2 is negotiated where the server offers it.
static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .user_agent(concat!("midwest_mainline/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(WEB_SEED_CONNECT_TIMEOUT)
        .read_timeout(WEB_SEED_READ_TIMEOUT)
        .pool_idle_timeout(Duration::from_secs(90))
        .http2_adaptive_window(true)
        .build()
        .expect("the HTTP client's configuration is static")
});

/// A stretch of one file: what one Range request fetches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileRange {
    pub file: usize,
    pub offset: u64,
    pub len: u64,
}

/// The files holding torrent bytes `start..start + len`, in order. Empty files hold nothing
/// and are skipped.
pub(crate) fn file_ranges(torrent: &Torrent, start: u64, len: u64) -> Vec<FileRange> {
    torrent
        .file_segments(start..start + len)
        .map(|(file, within)| FileRange {
            file,
            offset: within.start,
            len: within.end - within.start,
        })
        .collect()
}

/// BEP 19: a single-file torrent's URL names the file itself, unless it ends in '/', when
/// the torrent's name is appended. A multi-file torrent's URL is the directory the torrent's
/// `name` directory lives in.
pub(crate) fn file_url(base: &str, torrent: &Torrent, file: usize) -> String {
    let path = &torrent.files[file].raw_path;
    let single_file = path.len() == 1;
    if single_file && !base.ends_with('/') {
        return base.to_string();
    }
    let mut url = base.to_string();
    if !url.ends_with('/') {
        url.push('/');
    }
    let segments: Vec<String> = path.iter().map(|segment| percent_encode(segment.as_bytes())).collect();
    url.push_str(&segments.join("/"));
    url
}

/// Why a job stopped short.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Failure {
    /// the server can't serve this torrent (404, no Range support, ...): stop asking it
    Permanent(String),
    /// worth retrying after a while; `retry_after` is the server's own say on when
    Transient {
        error: String,
        retry_after: Option<Duration>,
    },
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Permanent(e) | Failure::Transient { error: e, .. } => f.write_str(e),
        }
    }
}

fn transient(error: impl fmt::Display) -> Failure {
    Failure::Transient {
        error: error.to_string(),
        retry_after: None,
    }
}

/// Torrent bytes `start..end` from the seed at `base`. Both ends sit on block boundaries
/// within their pieces (or at a piece's end), so the body cuts into exactly the blocks a peer
/// would have been asked for.
pub(crate) struct Job {
    pub torrent: Arc<Torrent>,
    pub base: String,
    pub host: String,
    pub start: u64,
    pub end: u64,
    pub redirects: Arc<Mutex<Redirects>>,
}

/// Where a web seed's URLs redirected to last time. A mirror network that redirects every
/// request to the node holding the files (archive.org does) would otherwise cost an extra
/// round trip per request, and a torrent of many small files is mostly round trips.
#[derive(Default)]
pub(crate) struct Redirects {
    /// per file, the URL it ended up at
    files: HashMap<usize, Url>,
    /// the seed's base URL as redirected, when a file's redirect kept the path below the
    /// base: files not fetched yet go straight there
    base: Option<String>,
}

impl Redirects {
    /// The URL to ask for `file`, and whether it came from a redirect.
    fn url(&self, base: &str, torrent: &Torrent, file: usize) -> Result<(Url, bool), Failure> {
        if let Some(url) = self.files.get(&file) {
            return Ok((url.clone(), true));
        }
        let url = |base: &str| Url::parse(&file_url(base, torrent, file));
        if let Some(Ok(url)) = self.base.as_deref().map(url) {
            return Ok((url, true));
        }
        let url = url(base).map_err(|e| Failure::Permanent(format!("bad URL: {e}")))?;
        Ok((url, false))
    }

    fn redirected(&mut self, base: &str, torrent: &Torrent, file: usize, to: &Url) {
        self.files.insert(file, to.clone());
        let asked = file_url(base, torrent, file);
        let below_base = &asked[base.len().min(asked.len())..];
        if !below_base.is_empty()
            && let Some(new_base) = to.as_str().strip_suffix(below_base)
        {
            self.base = Some(new_base.to_string());
        }
    }

    fn forget(&mut self, file: usize) {
        self.files.remove(&file);
        self.base = None;
    }
}

impl Job {
    fn pieces(&self) -> RangeInclusive<u64> {
        let piece = self.torrent.piece_size as u64;
        self.start / piece..=(self.end - 1) / piece
    }

    /// Fetches the range, handing each block to `deliver` as soon as it's complete. Returns
    /// early, successfully, once `deliver` says nobody wants the blocks any more.
    ///
    /// A range spanning many files takes a request per file; up to WEB_SEED_PIPELINE of them
    /// run at once, so a run of small files costs about one round trip, not one each.
    pub async fn run<F>(&self, mut deliver: impl FnMut(Piece) -> F) -> Result<(), Failure>
    where
        F: Future<Output = bool>,
    {
        let mut blocks = Blocks::new(&self.torrent, self.start, self.end);
        let mut ranges = file_ranges(&self.torrent, self.start, self.end - self.start).into_iter();
        let mut pipeline = VecDeque::new();
        loop {
            while pipeline.len() < WEB_SEED_PIPELINE
                && let Some(range) = ranges.next()
            {
                // BEP 47: padding is zeros, and not on the server
                pipeline.push_back(if self.torrent.files[range.file].attr.pad {
                    Part::Zeros(range.len)
                } else {
                    Part::Fetch(self.fetch(range)?)
                });
            }
            let mut fetch = match pipeline.pop_front() {
                None => return Ok(()),
                Some(Part::Fetch(fetch)) => fetch,
                Some(Part::Zeros(len)) => {
                    for block in blocks.feed(&vec![0; len as usize]) {
                        if !deliver(block).await {
                            return Ok(());
                        }
                    }
                    continue;
                }
            };
            let mut left = fetch.range.len;
            while left > 0 {
                let chunk = match fetch.body.recv().await {
                    Some(Ok(chunk)) => chunk,
                    Some(Err(e)) => return Err(self.failed(&fetch, e)),
                    None => return Err(transient("the request was dropped")),
                };
                let take = chunk.len().min(left as usize);
                left -= take as u64;
                for block in blocks.feed(&chunk[..take]) {
                    if !deliver(block).await {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// What a failed fetch says about the seed. A request that went where an earlier one
    /// was redirected says nothing about the seed itself, so the next try asks it again.
    fn failed(&self, fetch: &Fetch, failure: Failure) -> Failure {
        if !fetch.redirected {
            return failure;
        }
        self.redirects.lock().unwrap().forget(fetch.range.file);
        match failure {
            Failure::Permanent(error) => transient(error),
            failure => failure,
        }
    }

    /// Starts the request for `range` on a task of its own that reads the body as it comes:
    /// bodies left waiting on a shared HTTP/2 connection would hold up its flow control
    /// window, and with it the body being read.
    fn fetch(&self, range: FileRange) -> Result<Fetch, Failure> {
        let (url, redirected) = self
            .redirects
            .lock()
            .unwrap()
            .url(&self.base, &self.torrent, range.file)?;
        let pieces = self.pieces();
        let span = tracing::info_span!(
            "webseed",
            info_hash = %self.torrent.info_hash,
            host = %self.host,
            pieces = %format!("{}-{}", pieces.start(), pieces.end()),
            file = range.file,
            offset = range.offset,
            len = range.len,
            bytes = tracing::field::Empty,
            status = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        let (tx, body) = mpsc::unbounded_channel();
        let torrent = self.torrent.clone();
        let base = self.base.clone();
        let redirects = self.redirects.clone();
        let task = tokio::spawn(
            async move {
                let span = tracing::Span::current();
                let mut read = 0u64;
                let fetched = async {
                    let mut response = request(&url, range).await?;
                    if response.url() != &url {
                        redirects
                            .lock()
                            .unwrap()
                            .redirected(&base, &torrent, range.file, response.url());
                    }
                    while read < range.len {
                        let chunk = response
                            .chunk()
                            .await
                            .map_err(|e| transient(format!("{:#}", anyhow::Error::from(e))))?
                            .ok_or_else(|| transient(format!("the body ended {} bytes short", range.len - read)))?;
                        read += chunk.len() as u64;
                        if tx.send(Ok(chunk)).is_err() {
                            break;
                        }
                    }
                    Ok::<_, Failure>(())
                }
                .await;
                span.record("bytes", read.min(range.len));
                if let Err(e) = fetched {
                    span.record("error", e.to_string());
                    let _ = tx.send(Err(e));
                }
            }
            .instrument(span),
        );
        Ok(Fetch {
            range,
            redirected,
            body,
            _task: AbortOnDrop(task),
        })
    }
}

/// One file's share of a job, in the order they're delivered.
enum Part {
    /// padding, which no server has: this many zeros
    Zeros(u64),
    Fetch(Fetch),
}

/// A request a job has started: the body arrives through `body`, then the channel closes (or
/// a failure comes instead).
struct Fetch {
    range: FileRange,
    /// it went to where an earlier request was redirected, not to the seed's own URL
    redirected: bool,
    body: mpsc::UnboundedReceiver<Result<Bytes, Failure>>,
    _task: AbortOnDrop,
}

/// A job that stops calls off the requests it started.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One Range request, checked down to its head.
async fn request(url: &Url, range: FileRange) -> Result<reqwest::Response, Failure> {
    let last = range.offset + range.len - 1;
    let response = CLIENT
        .get(url.clone())
        .header(header::RANGE, format!("bytes={}-{last}", range.offset))
        .header(header::ACCEPT_ENCODING, "identity")
        .send()
        .await
        .map_err(|e| transient(format!("{:#}", anyhow::Error::from(e))))?;
    let status = response.status();
    tracing::Span::current().record("status", status.as_u16());
    match status {
        StatusCode::PARTIAL_CONTENT => {
            let content_range = response
                .headers()
                .get(header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            if !content_range.starts_with(&format!("bytes {}-", range.offset)) {
                return Err(Failure::Permanent(format!(
                    "asked for bytes {}-{last}, got {content_range:?}",
                    range.offset
                )));
            }
        }
        // the whole file, Range ignored: fine from the top, useless anywhere else
        StatusCode::OK if range.offset == 0 => {}
        StatusCode::OK => return Err(Failure::Permanent("the server ignores Range requests".into())),
        s if s.is_server_error() || s == StatusCode::TOO_MANY_REQUESTS || s == StatusCode::REQUEST_TIMEOUT => {
            let retry_after = response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse().ok())
                .map(Duration::from_secs);
            return Err(Failure::Transient {
                error: format!("HTTP {status} from {}", response.url()),
                retry_after,
            });
        }
        s => return Err(Failure::Permanent(format!("HTTP {s} from {}", response.url()))),
    }
    Ok(response)
}

/// Cuts a byte stream starting at a block boundary into the torrent's blocks.
struct Blocks<'t> {
    torrent: &'t Torrent,
    /// torrent offset of `buf[0]`
    at: u64,
    end: u64,
    buf: Vec<u8>,
}

impl<'t> Blocks<'t> {
    fn new(torrent: &'t Torrent, start: u64, end: u64) -> Self {
        Self {
            torrent,
            at: start,
            end,
            buf: Vec::with_capacity(BLOCK_SIZE),
        }
    }

    /// The length of the block starting at `at`.
    fn block_len(&self) -> usize {
        let piece_size = self.torrent.piece_size as u64;
        let piece = self.at / piece_size;
        let begin = self.at % piece_size;
        let piece_len = self.torrent.nth_piece_size(piece).unwrap_or(0) as u64;
        (BLOCK_SIZE as u64).min(piece_len - begin).min(self.end - self.at) as usize
    }

    fn feed(&mut self, mut bytes: &[u8]) -> Vec<Piece> {
        let mut out = vec![];
        while !bytes.is_empty() && self.at < self.end {
            let want = self.block_len() - self.buf.len();
            let take = want.min(bytes.len());
            self.buf.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buf.len() == self.block_len() {
                let piece_size = self.torrent.piece_size as u64;
                let data: Box<[u8]> = std::mem::replace(&mut self.buf, Vec::with_capacity(BLOCK_SIZE)).into();
                let length = data.len() as u32;
                out.push(Piece {
                    index: (self.at / piece_size) as u32,
                    begin: (self.at % piece_size) as u32,
                    data,
                });
                self.at += length as u64;
            }
        }
        out
    }
}

/// The swarm's side of one web seed: how it's doing, and the jobs it has running.
pub(crate) struct WebSeed {
    pub url: String,
    pub host: String,
    /// stands in for the seed wherever the swarm keys a source by address (claims,
    /// holdings, a piece's senders): `[100::n]:0`, from the discard-only prefix (RFC 6666)
    /// and port 0, so no peer ever has it
    pub addr: SocketAddr,
    pub stats: PeerStatistics,
    pub jobs: BTreeMap<u64, WebJob>,
    pub redirects: Arc<Mutex<Redirects>>,
    consecutive_failures: u32,
    retry_at: Option<Instant>,
    /// why it's no longer asked for anything
    pub gave_up: Option<String>,
}

/// A job in flight; dropping it calls the fetch off.
pub(crate) struct WebJob {
    pub pieces: RangeInclusive<u32>,
    pub _cancel: DropGuard,
}

impl WebSeed {
    pub fn new(index: usize, url: String) -> Self {
        let host = Url::parse(&url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_string))
            .unwrap_or_default();
        Self {
            addr: SocketAddr::new(Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, 0, index as u16 + 1).into(), 0),
            url,
            host,
            stats: PeerStatistics::default(),
            jobs: BTreeMap::new(),
            redirects: Arc::default(),
            consecutive_failures: 0,
            retry_at: None,
            gave_up: None,
        }
    }

    /// It may take another job now.
    pub fn has_room(&self, now: Instant) -> bool {
        self.gave_up.is_none() && self.retry_at.is_none_or(|at| at <= now) && self.jobs.len() < WEB_SEED_JOBS
    }

    /// How much one job should fetch: enough that the seed's jobs together cover
    /// WEB_SEED_RUN_TARGET at its measured rate, so a fast mirror gets long runs (few requests,
    /// big sequential reads) and a slow one single pieces it can finish soon.
    pub fn run_bytes(&self) -> usize {
        let bytes = self.stats.rx_rate * WEB_SEED_RUN_TARGET.as_secs_f64() / WEB_SEED_JOBS as f64;
        (bytes as usize).min(WEB_SEED_MAX_RUN)
    }

    pub fn succeeded(&mut self) {
        self.consecutive_failures = 0;
    }

    pub fn failed(&mut self, failure: &Failure, now: Instant) {
        match failure {
            Failure::Permanent(e) => self.gave_up = Some(e.clone()),
            Failure::Transient { retry_after, .. } => {
                // the jobs that were in flight alongside the one that failed first tend to
                // fail too; that's one failure of the seed's, not several
                if self.retry_at.is_some_and(|at| at > now) {
                    return;
                }
                self.consecutive_failures += 1;
                let backoff = WEB_SEED_BACKOFF.saturating_mul(1 << (self.consecutive_failures - 1).min(16));
                let wait = retry_after.unwrap_or(backoff).min(WEB_SEED_BACKOFF_MAX);
                self.retry_at = Some(now + wait);
            }
        }
    }

    /// The pieces its jobs are fetching, for the peer list.
    pub fn outstanding_blocks(&self, piece_size: u32) -> usize {
        let blocks_per_piece = (piece_size as usize).div_ceil(BLOCK_SIZE);
        self.jobs
            .values()
            .map(|j| (j.pieces.end() - j.pieces.start() + 1) as usize * blocks_per_piece)
            .sum()
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::metadata::build_torrent_file;
    use crate::torrent::parse_torrent;
    use sha1::{Digest, Sha1};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    fn multi_file(files: &[(&[&str], usize)], piece: usize) -> (Torrent, Vec<u8>) {
        let total: usize = files.iter().map(|(_, len)| len).sum();
        let content: Vec<u8> = (0..total).map(|i| (i * 7 % 251) as u8).collect();
        let hashes: Vec<u8> = content.chunks(piece).flat_map(|c| Sha1::digest(c).to_vec()).collect();
        let mut info = b"d5:filesl".to_vec();
        for (path, len) in files {
            info.extend_from_slice(format!("d6:lengthi{len}e4:pathl").as_bytes());
            for segment in *path {
                info.extend_from_slice(format!("{}:{segment}", segment.len()).as_bytes());
            }
            info.extend_from_slice(b"ee");
        }
        info.extend_from_slice(
            format!("e4:name8:the dir!12:piece lengthi{piece}e6:pieces{}:", hashes.len()).as_bytes(),
        );
        info.extend_from_slice(&hashes);
        info.push(b'e');
        (parse_torrent(&build_torrent_file(&info, &[])).unwrap(), content)
    }

    #[test]
    fn urls_follow_bep_19() {
        let (multi, _) = multi_file(&[(&["a b", "ü#1.txt"], 10), (&["c"], 5)], 16);
        assert_eq!(
            file_url("http://m.test/pub/", &multi, 0),
            "http://m.test/pub/the%20dir%21/a%20b/%C3%BC%231.txt"
        );
        assert_eq!(
            file_url("http://m.test/pub", &multi, 1),
            "http://m.test/pub/the%20dir%21/c",
            "a multi-file URL is a directory even without the slash"
        );

        let info = b"d6:lengthi5e4:name7:x y.iso12:piece lengthi16e6:pieces20:aaaaaaaaaaaaaaaaaaaae";
        let single = parse_torrent(&build_torrent_file(info, &[])).unwrap();
        assert_eq!(
            file_url("https://m.test/isos/", &single, 0),
            "https://m.test/isos/x%20y.iso"
        );
        assert_eq!(
            file_url("https://m.test/isos/renamed.iso", &single, 0),
            "https://m.test/isos/renamed.iso"
        );
    }

    #[test]
    fn piece_ranges_map_onto_files() {
        let (t, _) = multi_file(&[(&["a"], 10), (&["empty"], 0), (&["b"], 20), (&["c"], 6)], 16);
        // piece 0 is a[0..10] + b[0..6], piece 1 b[6..20] + c[0..2], piece 2 c[2..6]
        let at = |file, offset, len| FileRange { file, offset, len };
        assert_eq!(file_ranges(&t, 0, 16), [at(0, 0, 10), at(2, 0, 6)]);
        assert_eq!(file_ranges(&t, 16, 16), [at(2, 6, 14), at(3, 0, 2)]);
        assert_eq!(file_ranges(&t, 32, 4), [at(3, 2, 4)]);
        assert_eq!(file_ranges(&t, 0, 36), [at(0, 0, 10), at(2, 0, 20), at(3, 0, 6)]);
        assert_eq!(file_ranges(&t, 12, 3), [at(2, 2, 3)]);
    }

    /// Serves `files` (URL path to contents) over HTTP/1.1 with Range support and keep-alive,
    /// enough of a web seed for tests. Anything else is a 404.
    pub(crate) async fn serve(files: HashMap<String, Vec<u8>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let files = Arc::new(files);
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let files = files.clone();
                tokio::spawn(async move {
                    let (read, mut write) = socket.into_split();
                    let mut read = BufReader::new(read);
                    loop {
                        let mut request_line = String::new();
                        if read.read_line(&mut request_line).await.unwrap_or(0) == 0 {
                            return;
                        }
                        let path = request_line.split_whitespace().nth(1).unwrap_or("").to_string();
                        let mut range = None;
                        loop {
                            let mut line = String::new();
                            read.read_line(&mut line).await.unwrap();
                            if line.trim().is_empty() {
                                break;
                            }
                            if let Some(r) = line.to_ascii_lowercase().strip_prefix("range: bytes=") {
                                let (a, b) = r.trim().split_once('-').unwrap();
                                range = Some((a.parse::<usize>().unwrap(), b.parse::<usize>().unwrap()));
                            }
                        }
                        let response = match (files.get(&path), range) {
                            _ if path.starts_with("/moved/") => format!(
                                "HTTP/1.1 302 Found\r\nLocation: /{}\r\nContent-Length: 0\r\n\r\n",
                                &path["/moved/".len()..]
                            )
                            .into_bytes(),
                            (Some(body), Some((a, b))) if b < body.len() => {
                                let mut r = format!(
                                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {a}-{b}/{}\r\n\r\n",
                                    b - a + 1,
                                    body.len()
                                )
                                .into_bytes();
                                r.extend_from_slice(&body[a..=b]);
                                r
                            }
                            _ => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_vec(),
                        };
                        if write.write_all(&response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        format!("http://{addr}/")
    }

    #[tokio::test]
    async fn a_job_across_files_comes_back_as_blocks() {
        const PIECE: usize = 2 * BLOCK_SIZE;
        let sizes = [BLOCK_SIZE + 100, 0, 3 * BLOCK_SIZE, 1000];
        let (t, content) = multi_file(
            &[
                (&["one"], sizes[0]),
                (&["sub", "empty"], 0),
                (&["sub", "two"], sizes[2]),
                (&["3"], sizes[3]),
            ],
            PIECE,
        );
        let mut files = HashMap::new();
        let mut at = 0;
        for (i, name) in [
            "/seed/the%20dir%21/one",
            "/seed/the%20dir%21/sub/empty",
            "/seed/the%20dir%21/sub/two",
            "/seed/the%20dir%21/3",
        ]
        .into_iter()
        .enumerate()
        {
            files.insert(name.to_string(), content[at..at + sizes[i]].to_vec());
            at += sizes[i];
        }
        let base = serve(files).await;
        let job = |start: u64, end: u64| Job {
            torrent: Arc::new(t.clone()),
            base: format!("{base}seed"),
            host: "localhost".into(),
            start,
            end,
            redirects: Arc::default(),
        };

        let mut got = vec![];
        job(0, content.len() as u64)
            .run(|b| {
                got.push(b);
                std::future::ready(true)
            })
            .await
            .unwrap();
        let mut rebuilt = vec![];
        for b in &got {
            assert_eq!(b.index as u64 * PIECE as u64 + b.begin as u64, rebuilt.len() as u64);
            assert!(b.data.len() <= BLOCK_SIZE);
            rebuilt.extend_from_slice(&b.data);
        }
        assert_eq!(rebuilt, content);
        assert_eq!(
            got.len(),
            5,
            "2 blocks for each of 2 full pieces, 1 for the short last one"
        );

        // the second half of piece 1 only, as an endgame racer would ask
        let mut got = vec![];
        job((PIECE + BLOCK_SIZE) as u64, 2 * PIECE as u64)
            .run(|b| {
                got.push(b);
                std::future::ready(true)
            })
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!((got[0].index, got[0].begin), (1, BLOCK_SIZE as u32));

        // a mirror that redirects: once one file has been, files not fetched yet go straight
        // to where it went
        let moved = Job {
            base: format!("{base}moved/seed/"),
            ..job(0, content.len() as u64)
        };
        for redirected in [3, 0] {
            moved.redirects.lock().unwrap().files.clear();
            let mut rebuilt = vec![];
            moved
                .run(|b| {
                    rebuilt.extend_from_slice(&b.data);
                    std::future::ready(true)
                })
                .await
                .unwrap();
            assert_eq!(rebuilt, content);
            let redirects = moved.redirects.lock().unwrap();
            assert_eq!(redirects.base.as_deref(), Some(format!("{base}seed/").as_str()));
            assert_eq!(redirects.files.len(), redirected);
        }

        let missing = Job {
            base: format!("{base}elsewhere/"),
            ..job(0, 10)
        };
        assert!(matches!(
            missing.run(|_| std::future::ready(true)).await,
            Err(Failure::Permanent(_))
        ));
    }

    #[test]
    fn transient_failures_back_off_and_permanent_ones_give_up() {
        let now = Instant::now();
        let mut seed = WebSeed::new(0, "http://m.test/".into());
        assert!(seed.has_room(now));
        seed.failed(&transient("reset"), now);
        assert!(!seed.has_room(now));
        assert!(seed.has_room(now + WEB_SEED_BACKOFF));
        seed.failed(&transient("reset"), now);
        assert!(
            seed.has_room(now + WEB_SEED_BACKOFF),
            "failing alongside the first is no new failure"
        );
        let later = now + WEB_SEED_BACKOFF;
        seed.failed(&transient("reset"), later);
        assert!(!seed.has_room(later + WEB_SEED_BACKOFF), "the backoff doubles");
        assert!(seed.has_room(later + 2 * WEB_SEED_BACKOFF));
        seed.succeeded();
        seed.failed(&Failure::Permanent("HTTP 404".into()), now);
        assert!(!seed.has_room(now + WEB_SEED_BACKOFF_MAX));
    }

    /// BEP 47: padding isn't asked of the server (it has no such file); it comes back as zeros.
    #[tokio::test]
    async fn padding_is_zeros_not_a_request() {
        use crate::torrent::fixtures::{self, sorted};
        const PIECE: usize = BLOCK_SIZE;
        let files = sorted(&[(&["a"][..], vec![1; 5000]), (&["b"][..], vec![2; 3000])]);
        let t = crate::torrent::parse_torrent(&fixtures::torrent_file("pad", &files, PIECE, true)).unwrap();
        assert!(t.files[1].attr.pad);
        let base = serve(HashMap::from([
            ("/seed/pad/a".to_string(), files[0].1.clone()),
            ("/seed/pad/b".to_string(), files[1].1.clone()),
        ]))
        .await;
        let job = Job {
            torrent: Arc::new(t),
            base: format!("{base}seed"),
            host: "localhost".into(),
            start: 0,
            end: (PIECE + 3000) as u64,
            redirects: Arc::default(),
        };
        let mut rebuilt = vec![];
        job.run(|b| {
            rebuilt.extend_from_slice(&b.data);
            std::future::ready(true)
        })
        .await
        .unwrap();
        let mut expected = vec![1; 5000];
        expected.resize(PIECE, 0);
        expected.extend_from_slice(&[2; 3000]);
        assert_eq!(rebuilt, expected);
    }
}
