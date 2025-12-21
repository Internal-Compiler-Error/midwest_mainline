mod bt_client;
mod defs;
pub mod download;
mod peer;
mod settings;
mod storage;
mod sys_tcp;
mod torrent;
pub mod torrent_swarm;
mod wire;

use crate::bt_client::BtClient;
use defs::Identity;
use std::env;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::str::FromStr;
use torrent::parse_torrent;

fn random_idv4(external_ip: &Ipv4Addr, rand: u8) -> [u8; 20] {
    let mut rng = rand::rng();
    let r = rand & 0x07;
    let mut id = [0u8; 20];
    let mut ip = external_ip.octets();
    let mask = [0x03, 0x0f, 0x3f, 0xff];

    for (ip, mask) in ip.iter_mut().zip(mask.iter()) {
        *ip &= mask;
    }

    ip[0] |= r << 5;
    let crc = crc32c::crc32c(&ip);

    id[0] = (crc >> 24) as u8;
    id[1] = (crc >> 16) as u8;
    id[2] = (((crc >> 8) & 0xf8) as u8) | (rand::Rng::random::<u8>(&mut rng) & 0x7);

    rand::RngCore::fill_bytes(&mut rng, &mut id[3..19]);

    id[19] = rand;

    id
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<_>>();

    let meta_file = PathBuf::from(&args[1]);
    let meta_bytes = std::fs::read(&meta_file).unwrap();
    let torrent = parse_torrent(&meta_bytes).unwrap();

    let public_ip = Ipv4Addr::from_str("99.226.33.190")?;
    let mut client = BtClient::new(Identity {
        peer_id: random_idv4(&public_ip, 3),
        serving: SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 6881),
    });

    client.add_torrent(torrent).unwrap();
    client.work().await?;

    Ok(())
}
