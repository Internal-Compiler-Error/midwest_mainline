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
| 16 | Super-seeding, per torrent (Details switch, CLI `--super-seed`, kept in the resume file): while complete, a new peer gets HaveNone (or an empty bitfield) and one piece at a time, the least seen; the next once that piece turns up at another peer (or the peer is alone, or 2 min pass); requests for unshown pieces are rejected; switching off reveals the rest. Locally, two leechers off one super-seed: the seed sent 1.01x the torrent, against 1.29x without | `torrent_swarm.rs` (`reveal_next_piece`, `reveal_where_spread`), `peer.rs` (`SuperSeedView`), `session.rs` / `resume.rs` (`Modes`) |
| 19 | Web seeds (`url-list`, magnet `ws=`): HTTP(S) range requests over a shared HTTP/2 client, runs of consecutive pieces scheduled by UCB next to peers, endgame racing, backoff/give-up; `webseed` spans | `webseed.rs`, `torrent_swarm.rs`, `torrent.rs`, `magnet.rs` |
| 20 | Peer id convention (`-DL0100-` + random) | `defs.rs` (`random_peer_id`) |
| 21 | Partial seeds: `upload_only` re-sent when the selection completes but not every piece is in; peers' flag read (PEX marks them as seeds) | `torrent_swarm.rs` (`partial_seed`), `peer.rs` |
| 23 | Compact peer lists | `announcer.rs` |
| 24 | Tracker `external ip`, a vote for our public address (two agreeing voters needed; shown in the status bar when port mapping doesn't know it) | `announcer.rs`, `external.rs` |
| 27 | Private torrents (no DHT/PEX/LSD for them) | `torrent.rs`, `torrent_swarm.rs`, `announcer.rs` |
| 29 | uTP (via `librqbit-utp`; happy-eyeballs with TCP) | `utp.rs`, `stream.rs` |
| 32 | IPv6 DHT: a second `DhtSession` on an IPv6 socket, `nodes6`/`want`, 18-byte values, per-/64 Sybil rule, v6 table seeded from v4 `nodes6` answers (untested on a live v6 route: the dev Mac has none) | `dht/` (`DhtSession::pair_with`, migration `2026-10-07-000000_ipv6_nodes`), `downloader/src/dht.rs` |
| 33 | DHT scrape: `scrape=1` get_peers answered with `BFsd`/`BFpe` bloom filters of the fresh peers we hold (seed status from `seed=1` announces, which the downloader sends once it has every piece); `noseed` puts non-seeds first in `values`. `DhtClient::scrape` ORs the filters of the nodes a lookup reaches into a seeds/peers estimate; the downloader scrapes in a walk of its own next to its get_peers (nodes answer `scrape=1` with filters instead of values), at most every 15 min, and Details shows it on the DHT row as ~N seeds (json_rpc_server RPC `scrape`). Arch ISO: ~860 seeds, ~120 peers from 15 nodes | `dht/src/bloom.rs`, `client.rs` (`scrape`), `state.rs` (`scrape_filters`), migration `2026-10-08-000002_peer_seed`, `announcer.rs` |
| 40 | Canonical peer priority: each batch of addresses is dialled best-ranked first; at the connection cap a higher-ranked newcomer replaces the lowest-ranked peer that hasn't delivered a block (productive peers are never evicted). Needs our agreed external address. IPv6 masks follow the IPv4 pattern over the first 8 bytes (the BEP only has IPv4 examples) | `priority.rs`, `torrent_swarm.rs` (`make_room_for`, `connect_to_peers`) |
| 41 | UDP tracker extensions: we send an empty option list only | `announcer.rs` |
| 42 | DHT security extension: our node id is derived from our external IP; others' ids are checked (LAN exempt) and compliant nodes preferred: a full bucket evicts a non-compliant node for a compliant one, only compliant answers count towards a lookup's end, announces go to compliant nodes first. ~55-65% of live IPv4 nodes comply | `dht/src/dht/bep42.rs`, `routing_table.rs` (`bep42` column), `client.rs` (`lookup`) |
| 43 | Read-only DHT nodes: a query with `ro=1` is answered but its sender stays out of the routing table; `DhtSession::with_read_only` makes us one (queries carry `ro=1`, nothing is answered, not even with a 204). `json_rpc_server` takes `DHT_READ_ONLY=1`, the downloader the `dht_read_only` setting (Settings dialog, next start); an Arch magnet resolves and downloads as usual with it on | `message.rs` (`Krpc::read_only`), `rpc_manager.rs`, `server.rs`, `routing_table.rs` |
| 44 | DHT arbitrary data: `get`/`put` of immutable (SHA-1 of the value) and mutable items (ed25519 via `ed25519-dalek`, salt, seq, `cas`; errors 205/206/207/301/302), stored in SQLite (`item`) and served for 2 hours after the last put. `DhtClient::{get,put}_{immutable,mutable}` look up the k closest token holders (BEP 42 compliant first); json_rpc_server RPCs `put`/`get`. A put on the live DHT was stored by 7 nodes and read back by a fresh node | `dht/src/dht/item.rs`, `message/item_queries.rs`, `client.rs`, migration `2026-10-08-000003_item` |
| 45 | Multiple-address operation: `DhtSession::on_own_address` runs another node on a socket bound to one of the host's addresses, with its own node id (kept per address across starts), routing table and BEP 42 address votes, in the same database and sharing the announced-peer store; tokens and answers stay per socket. Nothing in the downloader uses it yet (one node per family) | `dht/src/dht/scope.rs`, `DhtSession::on_own_address` |
| 46 | Updating torrents via DHT mutable items: `xs=urn:btpk:<key>&s=<salt>` magnets, with or without an `xt` (used only while the DHT has no item); the item `{ih: <20 bytes>}` (32 taken as a v2 hash) at its highest valid seq decides the torrent. Followers poll hourly (5 min retries doubling after a miss; 30 s after a resume) and put the item again each time to keep it alive; a new seq naming another torrent adds that as an entry of its own (`TorrentUpdateFound` event), beside the old one or in `<name> (seq N)` when the name is taken, starting from the old files of the same path and size (copied, then checked); the old one is marked superseded and seeds on. Key, salt, seq and superseded are in the resume file (`btpk`). `downloader publish <key-file> <.torrent \| info hash> [--salt]` signs and puts (seq + 1, CAS). Details shows "via DHT key …, seq N". Live: put at 7 nodes, a fresh node resolved it 4 s after its DHT came up and fetched the Arch ISO; seq 2 was picked up 40 s after a restart. An update quit on before its metadata arrives is lost, like any magnet | `feed.rs`, `magnet.rs` (`parse_feed`), `session.rs` (`add_feed`, `TorrentTask::follow`, `add_update`, `place_update`), `resume.rs`, `main.rs` (`publish`), `dht/src/dht/client.rs` (`put_signed`) |
| 47 | Padding files and attributes: `attr` (`p` padding, `x` executable, `h` hidden, `l` symlink + `symlink path`), BitComet `_____padding_file_` names; padding stays in the piece stream but is never on disk, written, requested from peers or web seeds (reads as zeros, its blocks start out "received"), wanted, or listed in the GUI | `torrent.rs` (`FileAttr`, `padding_in_piece`), `storage.rs`, `check.rs`, `webseed.rs`, `torrent_swarm.rs` (`InFlight::skip_padding`), `bt_client.rs` (symlinks, mode 755) |
| 48 | Tracker scrape: swarm counts from announce replies, a scrape only when they leave something unsaid (at most every 30 min); shown per tracker and as the torrent's swarm size | `announcer.rs` (`SwarmCounts`, `http_scrape_url`), `Details.svelte` |
| 51 | DHT `sample_infohashes`: answered from the store (20 random info hashes, refreshed every 15 min); a crawler walks the keyspace with it (fixed query rate, each host asked again only after its `interval`), keeping what it samples in `sampled_infohash`. Opt-in in `json_rpc_server`'s index mode (`DHT_CRAWL=<queries/s>`, RPC `sampled`); ~10k distinct info hashes in the first minute at 20 q/s | `dht/src/dht/crawler.rs`, `server.rs`, `client.rs` (`sample_infohashes`), `json_rpc_server` |
| 52 | BitTorrent v2: `meta version` 2, `file tree`, per-file SHA-256 Merkle trees, `piece layers` (checked against `pieces root` on parse, kept in resume files, partial ones too), v2-only torrents laid out as BEP 52's piece-aligned stream (padding synthesised) and swarmed under the truncated SHA-256 hash; hybrids checked for agreement (else v1 only), each piece verified by SHA-1 and, once its file's layer is known, the Merkle tree (one read, both hashes); SHA-1 decides, since nothing else passes it: a piece that fails only the Merkle tree means the halves disagree, so the torrent drops to SHA-1 alone and stops answering hash requests, and no peer is blamed. Pieces in before a magnet's layer came aren't re-read for it, announced and looked up under both hashes (DHT and every tracker, a row each, peers into one swarm); reserved bit 0x10 set for v2 and hybrids, a hybrid connection upgraded to the v2 hash when both sides set it (we upgrade inbound ones, take an upgrade on outbound ones); `xt=urn:btmh:` magnets (a hybrid magnet's fetch announces both hashes); `hash request`/`hashes`/`hash reject` both ways (layers a magnet lacks, v2 or hybrid, are fetched whole per file from one v2 peer and checked against the root; we answer with libtorrent's proof layout: the piece layer and up from the layers, below it down to the leaves from verified data on disk, on the blocking pool) | `torrent.rs`, `merkle.rs`, `layers.rs`, `wire.rs` (`V2Support`), `magnet.rs`, `metadata.rs`, `resume.rs`, `announcer.rs` (`Announcing::v2`), `bt_client.rs`, `torrent_swarm.rs` |
| 53 | Magnet `so=` (select only): indices and ranges, applied when the metadata arrives; nothing valid selects all | `magnet.rs` (`MagnetLink::selection`), `session.rs` (`add`) |
| 54 | `lt_donthave`: received (availability and claims follow); we never drop pieces, so never sent | `torrent_swarm.rs`, `peer.rs` (`drop_have`) |
| 55 | Holepunch: a failed dial to a PEX-learned peer asks the peer that told us about it to relay (once); as a relay we introduce both sides or answer with the error; a `connect` is dialled over uTP only. Seen working against the live Arch swarm | `torrent_swarm.rs` (`try_holepunch`, `on_holepunch`), `peer.rs` (`Holepunch`), `stream.rs` (`DialHints::utp_only`) |
| MSE | Message stream encryption (not a BEP; the Vuze/libtorrent spec) | `mse.rs` |
| magnet `x.pe` | Peer addresses in a magnet (address literals only), tried first by the metadata fetch | `magnet.rs`, `metadata.rs` |

## Next, in order

Every BEP worth having has been started. What's left are gaps in implemented ones:

| BEP | Gap | Notes |
|---|---|---|
| 45 | The downloader runs one node per address family, not one per address | Library only (`DhtSession::on_own_address`); worth it for multi-homed hosts |

## Not planned

| BEP | Why |
|---|---|
| 8 | Tracker peer obfuscation: deprecated |
| 17 | Hoffman-style HTTP seeding: rare; BEP 19 covers web seeds |
| 30 | Merkle hash torrents: superseded by v2 (52) |
| 35, 38, 39, 49, 50 | Signing, local-data reuse, feeds, pub/sub: drafts with little adoption |
