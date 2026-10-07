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
| 10 | Extension protocol: `reqq`, `v`, `yourip` (both ways: we tell peers, and their word on ours is a vote), ids 1..=255 with 0 disabling | `peer.rs` (`build_extended_handshake`, `handle_extended_handshake`), `external.rs` |
| 11 | PEX | `peer.rs`, `torrent_swarm.rs` (`run_pex_round`; random sample of 50 per round) |
| 12 | Multitracker (`announce-list`) | `torrent.rs`, `announcer.rs` |
| 14 | Local service discovery | `lsd.rs` |
| 15 | UDP trackers (connect, announce, scrape; retransmits, connection-id expiry) | `announcer.rs` |
| 19 | Web seeds (`url-list`, magnet `ws=`): HTTP(S) range requests over a shared HTTP/2 client, runs of consecutive pieces scheduled by UCB next to peers, endgame racing, backoff/give-up; `webseed` spans | `webseed.rs`, `torrent_swarm.rs`, `torrent.rs`, `magnet.rs` |
| 20 | Peer id convention (`-DL0100-` + random) | `defs.rs` (`random_peer_id`) |
| 21 | Partial seeds: `upload_only` re-sent when the selection completes but not every piece is in; peers' flag read (PEX marks them as seeds) | `torrent_swarm.rs` (`partial_seed`), `peer.rs` |
| 23 | Compact peer lists | `announcer.rs` |
| 24 | Tracker `external ip`, a vote for our public address (two agreeing voters needed; shown in the status bar when port mapping doesn't know it) | `announcer.rs`, `external.rs` |
| 27 | Private torrents (no DHT/PEX/LSD for them) | `torrent.rs`, `torrent_swarm.rs`, `announcer.rs` |
| 29 | uTP (via `librqbit-utp`; happy-eyeballs with TCP) | `utp.rs`, `stream.rs` |
| 32 | IPv6 DHT: a second `DhtSession` on an IPv6 socket, `nodes6`/`want`, 18-byte values, per-/64 Sybil rule, v6 table seeded from v4 `nodes6` answers (untested on a live v6 route: the dev Mac has none) | `dht/` (`DhtSession::pair_with`, migration `2026-10-07-000000_ipv6_nodes`), `downloader/src/dht.rs` |
| 40 | Canonical peer priority: each batch of addresses is dialled best-ranked first; at the connection cap a higher-ranked newcomer replaces the lowest-ranked peer that hasn't delivered a block (productive peers are never evicted). Needs our agreed external address. IPv6 masks follow the IPv4 pattern over the first 8 bytes (the BEP only has IPv4 examples) | `priority.rs`, `torrent_swarm.rs` (`make_room_for`, `connect_to_peers`) |
| 41 | UDP tracker extensions: we send an empty option list only | `announcer.rs` |
| 48 | Tracker scrape: swarm counts from announce replies, a scrape only when they leave something unsaid (at most every 30 min); shown per tracker and as the torrent's swarm size | `announcer.rs` (`SwarmCounts`, `http_scrape_url`), `Details.svelte` |
| 42 | DHT security extension: our node id is derived from our external IP (we don't yet *verify* others') | `dht/src/dht.rs` |
| 54 | `lt_donthave`: received (availability and claims follow); we never drop pieces, so never sent | `torrent_swarm.rs`, `peer.rs` (`drop_have`) |
| 55 | Holepunch: a failed dial to a PEX-learned peer asks the peer that told us about it to relay (once); as a relay we introduce both sides or answer with the error; a `connect` is dialled over uTP only. Seen working against the live Arch swarm | `torrent_swarm.rs` (`try_holepunch`, `on_holepunch`), `peer.rs` (`Holepunch`), `stream.rs` (`DialHints::utp_only`) |
| 53 | Magnet `so=` (select only): indices and ranges, applied when the metadata arrives; nothing valid selects all | `magnet.rs` (`MagnetLink::selection`), `session.rs` (`add`) |
| magnet `x.pe` | Peer addresses in a magnet (address literals only), tried first by the metadata fetch | `magnet.rs`, `metadata.rs` |
| MSE | Message stream encryption (not a BEP; the Vuze/libtorrent spec) | `mse.rs` |

## Next, in order

| BEP | What | Why / notes |
|---|---|---|
| 52 + 47 | BitTorrent v2 (SHA-256 Merkle trees, `piece layers`, `btmh` magnets) and padding files / file attributes | Hybrid v1+v2 torrents are increasingly common; without 47 we'd write pad files to disk. Big: per-file Merkle verification, hash requests (`hash request`/`hashes`/`hash reject` messages), v2 info hash in the handshake |
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
