use downloader::{Session, SessionConfig, Settings, TorrentState, data_dir};

fn main() {
    let magnet = std::env::args().nth(1).unwrap();
    let secs: u64 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(90);
    let mut session = Session::new(SessionConfig {
        peer_id: *b"-DL0100-probe-probe.",
        data_dir: data_dir(),
        settings: Settings { listen_port: 0, ..Settings::default() },
    })
    .unwrap();
    let id = session.add(magnet, std::env::temp_dir().join("probe"));
    for t in 0..secs {
        std::thread::sleep(std::time::Duration::from_secs(1));
        if t % 5 != 4 {
            continue;
        }
        if let Some((_, TorrentState::Downloading(p))) = session.torrents().into_iter().find(|(i, _)| *i == id) {
            let (mut utp_n, mut utp_bps, mut utp_active, mut tcp_n, mut tcp_bps, mut tcp_active) = (0, 0.0, 0, 0, 0.0, 0);
            let mut best_utp = 0.0f64;
            let mut best_tcp = 0.0f64;
            for peer in &p.peers {
                if peer.utp {
                    utp_n += 1;
                    utp_bps += peer.download_bps;
                    best_utp = best_utp.max(peer.download_bps);
                    if peer.download_bps > 0.0 { utp_active += 1; }
                } else {
                    tcp_n += 1;
                    tcp_bps += peer.download_bps;
                    best_tcp = best_tcp.max(peer.download_bps);
                    if peer.download_bps > 0.0 { tcp_active += 1; }
                }
            }
            println!(
                "t={:>3}s total {:>7.0} KiB/s pieces {}/{} | uTP {} peers ({} sending) {:>7.0} KiB/s best {:>6.0} | TCP {} peers ({} sending) {:>7.0} KiB/s best {:>6.0}",
                t + 1, p.download_bps / 1024.0, p.verified_pieces, p.total_pieces,
                utp_n, utp_active, utp_bps / 1024.0, best_utp / 1024.0,
                tcp_n, tcp_active, tcp_bps / 1024.0, best_tcp / 1024.0,
            );
        } else {
            println!("t={:>3}s resolving", t + 1);
        }
    }
    session.remove(id, true);
}
