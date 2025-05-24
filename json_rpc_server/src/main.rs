use anyhow::anyhow;
use axum::{Json, Router, routing::post};
use futures::future::join_all;
use midwest_mainline::{
    dht::{DhtV4, dht_handle::DhtHandle},
    types::NodeId,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    net::{SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::{self, TcpListener, UdpSocket},
    task::JoinSet,
    time::sleep,
};
use tracing::{info, level_filters::LevelFilter};
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

async fn handle_rpc(Json(req): Json<JsonRpcRequest>) -> Json<JsonRpcResponse> {
    let result = serde_json::json!({"message": "Hello, world!"});

    let response = JsonRpcResponse {
        jsonrpc: "2.0",
        result,
        id: req.id,
    };

    Json(response)
}

async fn resolve_v4(s: &str) -> anyhow::Result<SocketAddrV4> {
    net::lookup_host(s)
        .await?
        .filter_map(|addr| match addr {
            SocketAddr::V4(v4) => Some(v4),
            _ => None,
        })
        .next()
        .ok_or(anyhow!("no ipv4 address"))
}

async fn bootstrap_nodes() -> Vec<SocketAddrV4> {
    let bootstrap = vec![
        "dht.tansmissionbt.com:6881",
        "router.utorrent.com:6881",
        "router.bittorrent.com:6881",
        "dht.aelitis.com:6881",
        "router.bitcomet.com:6881",
    ];

    let tasks = bootstrap.into_iter().map(resolve_v4);
    join_all(tasks).await.into_iter().filter_map(Result::ok).collect()
}

/// Randomly generates a node_id and send a find_node, used to populate the DHT
pub async fn populate_random(dht: &DhtV4) {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    set_up_tracing();

    let json_rpc_server = Router::new().route("/json_rpc", post(handle_rpc));

    let bootstraping = bootstrap_nodes().await;
    let external_ip = public_ip::addr_v4().await.unwrap();
    let dht_socket = UdpSocket::bind("0.0.0.0:44444".parse::<SocketAddr>()?).await?;
    let dht = DhtV4::with_stable_id(dht_socket, external_ip).unwrap();

    let mut event_loops = JoinSet::new();

    // DHT event loop
    let dht = Arc::new(dht);
    let dhtt = Arc::clone(&dht);
    event_loops.spawn(async move {
        dhtt.run().await;
    });
    // server event loop
    event_loops.spawn(async {
        let _ = axum::serve(TcpListener::bind("0.0.0.0:3000").await.unwrap(), json_rpc_server).await;
    });
    dht.bootstrap(bootstraping).await?;

    // populate the DHT routing table
    let dhtt = Arc::clone(&dht);
    event_loops.spawn(async move {
        loop {
            populate_random(&dhtt).await;
            sleep(Duration::from_secs(7)).await;
        }
    });

    event_loops.join_all().await;

    Ok(())
}
