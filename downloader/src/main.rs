mod defs;
mod peer;
mod settings;
mod storage;
mod sys_tcp;
mod torrent;
mod bt_client;
mod wire;
pub mod download;
pub mod torrent_swarm;

use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::env;
use defs::Identity;
use torrent::parse_torrent;
use crate::bt_client::BtClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<_>>();

    let meta_file = PathBuf::from(&args[1]);
    let meta_bytes = std::fs::read(&meta_file).unwrap();
    let torrent = parse_torrent(&meta_bytes).unwrap();

    let mut client = BtClient::new(Identity {
        peer_id: [0u8; 20],
        serving: SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 6881),
    });

    client.add_torrent(torrent).unwrap();
    client.work().await?;

    Ok(())
}
