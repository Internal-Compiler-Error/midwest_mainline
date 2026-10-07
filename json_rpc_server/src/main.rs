use anyhow::anyhow;
use axum::{Json, Router, extract::State, routing::post};
use futures::future::join_all;
use midwest_mainline::{
    dht::{DhtSession, Retention},
    types::{InfoHash, NodeId},
};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
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

#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    result: serde_json::Value,
    id: serde_json::Value,
}

#[derive(Serialize)]
struct JsonRpcErrorResp {
    jsonrpc: &'static str,
    error: serde_json::Value,
    id: serde_json::Value,
}

fn unsupported(id: serde_json::Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0",
        result: serde_json::json!({ "code": -32601, "message": "Method not found"}),
        id,
    }
}

fn invalid_params(id: serde_json::Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0",
        result: serde_json::json!({ "code": -32602, "message": "Invalid params"}),
        id,
    }
}

/// `{"info_hash": "<40 hex digits>"}`
fn info_hash_param(params: &Option<serde_json::Value>) -> Option<InfoHash> {
    let hex = params.as_ref()?.get("info_hash")?.as_str()?;
    InfoHash::try_from_bytes(&hex::decode(hex).ok()?)
}

async fn handle_rpc(State(s): State<AppState>, Json(req): Json<JsonRpcRequest>) -> Json<JsonRpcResponse> {
    let res = match req.method.as_str() {
        "node_count" => serde_json::json!(s.dht.node_count() + s.dht6.as_ref().map_or(0, |d| d.node_count())),
        "node_counts" => serde_json::json!({
            "v4": s.dht.node_count(),
            "v6": s.dht6.as_ref().map(|d| d.node_count()),
            // nodes with BEP 42 compliant ids, of each family
            "bep42_v4": s.dht.bep42_compliance().0,
            "bep42_v6": s.dht6.as_ref().map(|d| d.bep42_compliance().0),
        }),
        "stored_swarms" => {
            let swarms: Vec<String> = s.dht.stored_swarms().iter().map(|h| hex::encode(h.0)).collect();
            serde_json::json!(swarms)
        }
        "stored_peers" => {
            let Some(info_hash) = info_hash_param(&req.params) else {
                return Json(invalid_params(req.id));
            };
            let peers: Vec<_> = s
                .dht
                .stored_peers(&info_hash)
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
        _ => return Json(unsupported(req.id)),
    };

    let response = JsonRpcResponse {
        jsonrpc: "2.0",
        result: res,
        id: req.id,
    };

    Json(response)
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

/// Randomly generates a node_id and send a find_node, used to populate the DHT
#[instrument(skip(dht))]
pub async fn populate_random(dht: &DhtSession) {
    let node_id = {
        let mut rng = rand::rng();
        let node_id: [u8; 20] = rng.random();
        NodeId(node_id)
    };

    info!("Randomly generated {:?}", node_id);
    dht.find_node(node_id).await;
}

fn set_up_tracing() {
    let fmt_layer = fmt::layer()
        .compact()
        .with_line_number(true)
        .with_filter(LevelFilter::DEBUG);

    // global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    // let tracer = opentelemetry_jaeger::new_pipeline().install_simple().unwrap();

    // let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(console_subscriber::spawn())
        // .with(telemetry)
        .with(fmt_layer)
        .init();
}

#[derive(Clone)]
struct AppState {
    pub dht: Arc<DhtSession>,
    /// the IPv6 node (BEP 32), if an IPv6 socket could be bound
    pub dht6: Option<Arc<DhtSession>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    set_up_tracing();

    let dht_port: u16 = env::var("DHT_PORT").map_or(Ok(44444), |p| p.parse())?;
    let rpc_addr = env::var("RPC_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
    let database_url = env::var("DATABASE_URL").unwrap();
    let dht_socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], dht_port))).await?;
    // DHT_RETENTION=forever keeps every announced peer, making this node a long-term index
    let retention = match env::var("DHT_RETENTION").as_deref() {
        Ok("forever") => Retention::Forever,
        Ok("expire") | Err(_) => Retention::Expire,
        Ok(other) => return Err(anyhow!("DHT_RETENTION must be `expire` or `forever`, not `{other}`")),
    };
    let dht = Arc::new(
        DhtSession::with_stable_id(dht_socket, None, &database_url)
            .unwrap()
            .with_retention(retention),
    );
    let dht6 = match bind_v6(dht_port) {
        Ok(socket) => {
            let dht6 = DhtSession::with_stable_id(socket, None, &database_url)
                .unwrap()
                .with_retention(retention);
            dht.pair_with(&dht6);
            Some(Arc::new(dht6))
        }
        Err(e) => {
            warn!("no IPv6 DHT: couldn't bind [::]:{dht_port} ({e})");
            None
        }
    };
    let nodes: Vec<Arc<DhtSession>> = std::iter::once(dht.clone()).chain(dht6.clone()).collect();

    let mut event_loops = JoinSet::new();

    // DHT event loops
    let state = AppState {
        dht: dht.clone(),
        dht6: dht6.clone(),
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
