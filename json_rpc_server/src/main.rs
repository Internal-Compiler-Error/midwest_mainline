use anyhow::anyhow;
use axum::{Json, Router, extract::State, routing::post};
use futures::future::join_all;
use midwest_mainline::{
    dht::{
        DhtSession, Retention,
        crawler::{CrawlStats, Crawler},
    },
    types::{InfoHash, NodeId},
};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::{
    env,
    net::{Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::{self, TcpListener, UdpSocket},
    task::JoinSet,
    time::sleep,
};
use tracing::{info, instrument, level_filters::LevelFilter, warn};
use tracing_subscriber::{Layer, util::SubscriberInitExt};
use tracing_subscriber::{fmt, layer::SubscriberExt};

#[derive(Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    method: String,
    params: Option<serde_json::Value>,
    id: serde_json::Value,
}

/// A JSON-RPC 2.0 response: `result` on success, `error` (`{code, message}`) otherwise
#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    #[serde(flatten)]
    outcome: Outcome,
    id: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum Outcome {
    Result(serde_json::Value),
    Error { code: i32, message: String },
}

impl Outcome {
    fn error(code: i32, message: impl ToString) -> Self {
        Outcome::Error {
            code,
            message: message.to_string(),
        }
    }

    fn invalid_params() -> Self {
        Self::error(-32602, "Invalid params")
    }
}

/// Info hashes `stored_swarms` returns unless asked for fewer, and at most
const SWARMS_PAGE: usize = 1000;
const MAX_SWARMS_PAGE: usize = 10_000;

/// `{"info_hash": "<40 hex digits>"}`
fn info_hash_param(params: Option<&serde_json::Value>) -> Option<InfoHash> {
    let hex = params?.get("info_hash")?.as_str()?;
    InfoHash::try_from_bytes(&hex::decode(hex).ok()?)
}

async fn handle_rpc(State(s): State<AppState>, Json(req): Json<JsonRpcRequest>) -> Json<JsonRpcResponse> {
    let outcome = match req.jsonrpc.as_str() {
        "2.0" => call(&s, &req.method, req.params.as_ref()).await,
        _ => Outcome::error(-32600, "Invalid Request"),
    };
    Json(JsonRpcResponse {
        jsonrpc: "2.0",
        outcome,
        id: req.id,
    })
}

async fn call(s: &AppState, method: &str, params: Option<&serde_json::Value>) -> Outcome {
    let result = match method {
        "node_count" => serde_json::json!(s.dht.node_count() + s.dht6.as_ref().map_or(0, |d| d.node_count())),
        "node_counts" => serde_json::json!({
            "v4": s.dht.node_count(),
            "v6": s.dht6.as_ref().map(|d| d.node_count()),
            // nodes with BEP 42 compliant ids, of each family
            "bep42_v4": s.dht.bep42_compliance().0,
            "bep42_v6": s.dht6.as_ref().map(|d| d.bep42_compliance().0),
        }),
        // BEP 33: the swarm's size as the DHT knows it, over IPv4
        "scrape" => {
            let Some(info_hash) = info_hash_param(params) else {
                return Outcome::invalid_params();
            };
            let estimate = s.dht.handle().scrape(info_hash).await;
            serde_json::json!({
                "seeds": estimate.seeds,
                "peers": estimate.peers,
                "nodes": estimate.nodes,
            })
        }
        // BEP 44, immutable items: `{"text": "..."}` is stored as a bencoded string
        "put" => {
            let Some(text) = params.and_then(|p| p.get("text")?.as_str()) else {
                return Outcome::invalid_params();
            };
            let value = [format!("{}:", text.len()).as_bytes(), text.as_bytes()].concat();
            match s.dht.handle().put_immutable(value).await {
                Ok(put) => serde_json::json!({ "target": hex::encode(put.target.0), "stored": put.stored }),
                Err(e) => return Outcome::error(-32000, e),
            }
        }
        // `{"target": "<40 hex digits>"}`: the bencoded value, as text
        "get" => {
            let target = params.and_then(|p| p.get("target")?.as_str());
            let Some(target) = target.and_then(|t| NodeId::try_from_bytes(&hex::decode(t).ok()?)) else {
                return Outcome::invalid_params();
            };
            let value = s.dht.handle().get_immutable(target).await;
            serde_json::json!(value.map(|v| String::from_utf8_lossy(&v).into_owned()))
        }
        "sampled" => {
            let count = |field: fn(&CrawlStats) -> &AtomicU64| -> u64 {
                s.crawlers.iter().map(|c| field(c.stats()).load(Relaxed)).sum()
            };
            let dht = s.dht.clone();
            let Ok(info_hashes) = tokio::task::spawn_blocking(move || dht.sampled_count()).await else {
                return Outcome::error(-32603, "Internal error");
            };
            serde_json::json!({
                "info_hashes": info_hashes,
                "crawling": !s.crawlers.is_empty(),
                "queried": count(|s| &s.queried),
                "answered": count(|s| &s.answered),
                "samples": count(|s| &s.samples),
                "new_info_hashes": count(|s| &s.new_info_hashes),
            })
        }
        // a page of the store's info hashes, in order: `{"after": "<40 hex digits>", "limit": n}`,
        // both optional; the next page starts after the last one returned
        "stored_swarms" => {
            let param = |key| params.and_then(|p| p.get(key));
            let after = match param("after") {
                None => None,
                Some(after) => match after
                    .as_str()
                    .and_then(|h| InfoHash::try_from_bytes(&hex::decode(h).ok()?))
                {
                    Some(after) => Some(after),
                    None => return Outcome::invalid_params(),
                },
            };
            let limit = match param("limit") {
                None => SWARMS_PAGE,
                Some(limit) => match limit.as_u64() {
                    Some(limit) => (limit as usize).min(MAX_SWARMS_PAGE),
                    None => return Outcome::invalid_params(),
                },
            };
            let dht = s.dht.clone();
            let Ok(swarms) = tokio::task::spawn_blocking(move || dht.stored_swarms(after, limit)).await else {
                return Outcome::error(-32603, "Internal error");
            };
            serde_json::json!(swarms.iter().map(|h| hex::encode(h.0)).collect::<Vec<_>>())
        }
        "stored_peers" => {
            let Some(info_hash) = info_hash_param(params) else {
                return Outcome::invalid_params();
            };
            let dht = s.dht.clone();
            let Ok(peers) = tokio::task::spawn_blocking(move || dht.stored_peers(&info_hash)).await else {
                return Outcome::error(-32603, "Internal error");
            };
            let peers: Vec<_> = peers
                .into_iter()
                .map(|p| {
                    serde_json::json!({
                        "addr": p.addr.to_string(),
                        "first_announced": p.first_announced,
                        "last_announced": p.last_announced,
                    })
                })
                .collect();
            serde_json::json!(peers)
        }
        _ => return Outcome::error(-32601, "Method not found"),
    };
    Outcome::Result(result)
}

/// Every address of every router, both families; each node bootstraps from its own
async fn bootstrap_nodes() -> Vec<SocketAddr> {
    let bootstrap = vec![
        "router.bittorrent.com:6881",
        "router.utorrent.com:6881",
        "dht.transmissionbt.com:6881",
        "dht.libtorrent.org:25401",
        "dht.aelitis.com:6881",
        "router.silotis.us:6881",
    ];

    let tasks = bootstrap.into_iter().map(net::lookup_host);
    join_all(tasks)
        .await
        .into_iter()
        .filter_map(Result::ok)
        .flatten()
        .collect()
}

/// IPv6-only, so it can share the port number with the IPv4 socket
fn bind_v6(port: u16) -> std::io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_only_v6(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)).into())?;
    UdpSocket::from_std(socket.into())
}

/// A lookup of a random id, which fills the routing table with the nodes it meets
#[instrument(skip(dht))]
async fn populate_random(dht: &DhtSession) {
    let node_id = NodeId(rand::rng().random());
    info!("Randomly generated {:?}", node_id);
    dht.find_node(node_id).await;
}

fn set_up_tracing() {
    let fmt_layer = fmt::layer()
        .compact()
        .with_line_number(true)
        .with_filter(LevelFilter::DEBUG);
    // tokio-console keeps every task it has seen for an hour, and the node spawns one per
    // answer and per ping: on for a session of looking, not always
    let console = env::var("TOKIO_CONSOLE").is_ok_and(|v| v == "1");
    tracing_subscriber::registry()
        .with(console.then(console_subscriber::spawn))
        .with(fmt_layer)
        .init();
}

/// Where the JSON-RPC listens: `RPC_ADDR`, else loopback, as what it offers (puts, lookups on
/// demand, the store's whole contents) is for the machine's own use
fn rpc_addr(configured: Option<String>) -> String {
    configured.unwrap_or_else(|| "127.0.0.1:3000".to_string())
}

#[derive(Clone)]
struct AppState {
    pub dht: Arc<DhtSession>,
    /// the IPv6 node (BEP 32), if an IPv6 socket could be bound
    pub dht6: Option<Arc<DhtSession>>,
    /// BEP 51 crawlers, one per node, if crawling
    pub crawlers: Vec<Crawler>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    set_up_tracing();

    let dht_port: u16 = env::var("DHT_PORT").map_or(Ok(44444), |p| p.parse())?;
    let rpc_addr = rpc_addr(env::var("RPC_ADDR").ok());
    let database_url = env::var("DATABASE_URL").map_err(|_| anyhow!("DATABASE_URL is not set"))?;
    let dht_socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], dht_port))).await?;
    // DHT_RETENTION=forever keeps every announced peer, making this node a long-term index
    let retention = match env::var("DHT_RETENTION").as_deref() {
        Ok("forever") => Retention::Forever,
        Ok("expire") | Err(_) => Retention::Expire,
        Ok(other) => return Err(anyhow!("DHT_RETENTION must be `expire` or `forever`, not `{other}`")),
    };
    // DHT_READ_ONLY=1: ask, but answer nothing (BEP 43)
    let read_only = env::var("DHT_READ_ONLY").is_ok_and(|v| v == "1");
    let open = |socket| -> anyhow::Result<Arc<DhtSession>> {
        let session = DhtSession::with_stable_id(socket, None, &database_url)?
            .with_retention(retention)
            .with_read_only(read_only);
        Ok(Arc::new(session))
    };
    let dht = open(dht_socket)?;
    let dht6 = match bind_v6(dht_port) {
        Ok(socket) => {
            let dht6 = open(socket)?;
            dht.pair_with(&dht6);
            Some(dht6)
        }
        Err(e) => {
            warn!("no IPv6 DHT: couldn't bind [::]:{dht_port} ({e})");
            None
        }
    };
    let nodes: Vec<Arc<DhtSession>> = std::iter::once(dht.clone()).chain(dht6.clone()).collect();
    // DHT_CRAWL=<queries a second> grows the index with BEP 51 samples from other nodes
    let crawl_rate: Option<u32> = env::var("DHT_CRAWL").ok().map(|r| r.parse()).transpose()?;
    let crawlers: Vec<Crawler> = match crawl_rate {
        Some(rate) if retention == Retention::Forever => nodes.iter().map(|node| node.crawler(rate)).collect(),
        Some(_) => {
            warn!("DHT_CRAWL is for the long-term index, DHT_RETENTION=forever; not crawling");
            vec![]
        }
        None => vec![],
    };

    let mut event_loops = JoinSet::new();

    // DHT event loops
    let state = AppState {
        dht: dht.clone(),
        dht6: dht6.clone(),
        crawlers: crawlers.clone(),
    };
    for node in &nodes {
        let node = node.clone();
        event_loops.spawn(async move {
            node.run().await;
        });
    }

    let bootstraping = bootstrap_nodes().await;
    join_all(nodes.iter().map(|node| node.bootstrap(bootstraping.clone())))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let json_rpc_server = Router::new().route("/json_rpc", post(handle_rpc)).with_state(state);

    // server event loop
    let listener = TcpListener::bind(&rpc_addr).await?;
    info!("JSON-RPC on http://{}/json_rpc", listener.local_addr()?);
    event_loops.spawn(async {
        let _ = axum::serve(listener, json_rpc_server).await;
    });

    for crawler in crawlers {
        event_loops.spawn(async move { crawler.run().await });
    }

    // populate the DHT routing tables
    for node in nodes {
        event_loops.spawn(async move {
            loop {
                populate_random(&node).await;
                sleep(Duration::from_secs(7)).await;
            }
        });
    }

    event_loops.join_all().await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rpc_listens_on_loopback_unless_told_otherwise() {
        assert_eq!(rpc_addr(None), "127.0.0.1:3000");
        assert_eq!(rpc_addr(Some("0.0.0.0:3001".to_string())), "0.0.0.0:3001");
    }

    #[test]
    fn a_result_and_an_error_are_json_rpc_2s_shapes() {
        let response = |outcome| {
            serde_json::to_value(JsonRpcResponse {
                jsonrpc: "2.0",
                outcome,
                id: serde_json::json!(1),
            })
            .unwrap()
        };
        assert_eq!(
            response(Outcome::Result(serde_json::json!(7))),
            serde_json::json!({"jsonrpc": "2.0", "result": 7, "id": 1})
        );
        assert_eq!(
            response(Outcome::invalid_params()),
            serde_json::json!({"jsonrpc": "2.0", "error": {"code": -32602, "message": "Invalid params"}, "id": 1})
        );
    }
}
