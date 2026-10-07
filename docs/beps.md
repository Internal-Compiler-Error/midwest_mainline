# BEP status

Which BitTorrent Enhancement Proposals (https://www.bittorrent.org/beps/bep_0000.html) this
workspace implements, which are in flight, and which are next. **Keep it current**: when you
implement or start one, move its row and say where the code is. Pick up from **Next** in order
unless the user says otherwise; the order is by value to a fast, modern client.

`downloader/` is the client, `dht/` the DHT crate (`midwest_mainline`), `gui/` the app.

## Implemented

| BEP | What | Where |
|---|---|---|
| 3 | The protocol: metainfo, tracker HTTP, peer wire | `torrent.rs`, `announcer.rs`, `wire.rs`, `peer.rs`, `torrent_swarm.rs` |
| 5 | DHT | `dht/`; client side `downloader/src/dht.rs`, `announcer.rs` (`dht_announcer`) |
| 6 | Fast extension (HaveAll/None, Reject, AllowedFast parsed; we don't grant allowed-fast) | `peer.rs`, `torrent_swarm.rs` |
| 7 | IPv6 tracker peers (`peers6`), and the same split in PEX | `announcer.rs`, `peer.rs` |
| 9 | Metadata from peers (`ut_metadata`), magnet links | `metadata.rs`, `magnet.rs` |
| 10 | Extension protocol | `peer.rs` (`build_extended_handshake`, advertises `reqq`) |
| 11 | PEX | `peer.rs`, `torrent_swarm.rs` (`run_pex_round`; random sample of 50 per round) |
| 12 | Multitracker (`announce-list`) | `torrent.rs`, `announcer.rs` |
| 14 | Local service discovery | `lsd.rs` |
| 15 | UDP trackers (connect + announce; retransmits, connection-id expiry) | `announcer.rs` |
| 20 | Peer id convention (`-DL0100-` + random) | `defs.rs` (`random_peer_id`) |
| 23 | Compact peer lists | `announcer.rs` |
| 27 | Private torrents (no DHT/PEX/LSD for them) | `torrent.rs`, `torrent_swarm.rs`, `announcer.rs` |
| 29 | uTP (via `librqbit-utp`; happy-eyeballs with TCP) | `utp.rs`, `stream.rs` |
| 41 | UDP tracker extensions: we send an empty option list only | `announcer.rs` |
| 42 | DHT security extension: our node id is derived from our external IP (we don't yet *verify* others') | `dht/src/dht.rs` |
| MSE | Message stream encryption (not a BEP; the Vuze/libtorrent spec) | `mse.rs` |

## In progress

| BEP | What | Notes |
|---|---|---|
| 19 | Web seeds (`url-list`, magnet `ws=`) | HTTP range requests as a peer-like source in the swarm, with a `webseed` span |
| 32 | IPv6 DHT (`nodes6`, `want`) | separate v4/v6 routing tables, lookups on both families |

## Next, in order

| BEP | What | Why / notes |
|---|---|---|
| 52 + 47 | BitTorrent v2 (SHA-256 Merkle trees, `piece layers`, `btmh` magnets) and padding files / file attributes | Hybrid v1+v2 torrents are increasingly common; without 47 we'd write pad files to disk. Big: per-file Merkle verification, hash requests (`hash request`/`hashes`/`hash reject` messages), v2 info hash in the handshake |
| 48 + 15 scrape | Tracker scrape (seeds/leechers/completed) | Swarm size in the UI; cheap. The UDP `Scrape` action is only an enum value today |
| 53 | Magnet `so=` (select only) | Small: map to `select_files` once metadata is in |
| magnet `x.pe` | Peer addresses in a magnet | Small: hand to the swarm like any discovered peer |
| 21 | Partial seeds (`upload_only` in the extended handshake) | Tell peers we're done with what we selected; don't count partial seeds as seeds |
| 54 | `lt_donthave` | Small; needed if pieces can be dropped (e.g. a file deselected and deleted) |
| 24 + BEP 10 `yourip` | Our external address from trackers and peers | Today only the DHT tells us; feeds BEP 42 ids and port mapping checks |
| 55 | Holepunch (`ut_holepunch`, via a relaying peer, over uTP) | More reachable peers behind NAT |
| 40 | Canonical peer priority | Deterministic choice of which connections to keep under churn |
| 42 (verify) | Check other nodes' ids against their IPs | Complements the one-node-per-IP Sybil defence in the routing table |
| 51 | DHT `sample_infohashes` | Fits the DHT's long-term index mode (`Retention::Forever`) |
| 33 | DHT scrape (bloom filters of seeds/peers) | Swarm size without a tracker |
| 45 | Multiple-address DHT operation | Small, once 32 lands |
| 43 | Read-only DHT nodes | For a node that can't take inbound UDP |
| 44 + 46 | DHT arbitrary data; updating torrents via mutable items | Storage in the DHT; lowest priority of the DHT ones |
| 16 | Super-seeding | Only useful when we're the initial seeder |

## Not planned

| BEP | Why |
|---|---|
| 8 | Tracker peer obfuscation: deprecated |
| 17 | Hoffman-style HTTP seeding: rare; BEP 19 covers web seeds |
| 30 | Merkle hash torrents: superseded by v2 (52) |
| 35, 38, 39, 49, 50 | Signing, local-data reuse, feeds, pub/sub: drafts with little adoption |
