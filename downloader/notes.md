# Thoughts on actors in rust
An actor should probably do the following
1. define publicly an enum of events it can emit
2. define privately an enum of commands it can process
3. a handle that just holds a sender to the actual actor, it sends the commands
4. the actor takes itself by value to run its event loop

# general design
One key mantra, *slow peers* can't block other peers from proceeding.

The peer-selection algorithm is UCB (Upper Confidence Bound) over per-peer download rate --
see `PeerStatistics::score`/`rx_speed_ucb` in peer.rs and `TorrentSwarm::best_peer`. This is
the whole point of this project, not an implementation detail. Anything that would pollute the
reward signal (e.g. duplicate/redundant requests feeding stats from discarded data) should be
treated as a cost, not a free feature.

# endgame mode: deliberately not implemented
Considered and rejected (see git history around the "rarest-first piece selection" /
choking-algorithm commits). Reasons:
- A safe implementation needs "is this piece already verified" checked and the storage write
  to happen atomically. The natural way to add it here splits check-from-write across an
  await point, which is a TOCTOU: a slower duplicate fetch can finish after the check passes
  and overwrite an already-verified-good piece with bad/different data. Closing that requires
  moving the write behind the actor that owns `verified` (a bigger change than endgame mode
  itself), not a quick guard.
- Duplicate concurrent fetches of the same piece from multiple peers would feed
  `PeerStatistics` (`picked_count`, `mean_rx`) from the losing/discarded attempts too --
  directly polluting the UCB signal above.
- The actual problem endgame mode exists to solve (one slow/stalled peer holding up the last
  piece indefinitely) is already handled by `BLOCK_REQUEST_TIMEOUT` + the retry path in
  `download.rs`: a stalled request now fails within ~30s and gets re-picked, and UCB naturally
  deprioritizes a peer with a poor track record on retry.
If this gets revisited, the write-atomicity issue is the one that actually has to be solved
first -- everything else is secondary.

# magnet links / DHT: out of scope by explicit user decision
The workspace already has a working `dht` crate (`midwest_mainline`, sibling of this crate --
see `../dht`, 33 tests passing) with `DhtSession::bootstrap`/`get_peers`/`announce_peers`, so
DHT-based peer discovery for a magnet link is not a research problem here, it's plumbing. It was
still ruled out of scope for "fully spec compliant" (asked explicitly, 2026-09-05), because it
isn't just plumbing on top of what exists here: `BtClient::add_torrent`, `TorrentStorage::new`,
and `PeerFactory` all currently require a fully-parsed `Torrent` (piece count, piece length, file
list) up front. A magnet link starts with only an info hash -- every one of those needs a
pre-metadata phase (discover peers with no metadata yet -> BEP 10 extended handshake -> BEP 9
*consume* ut_metadata, the side that was deliberately not built when ut_metadata was added
serve-side-only -> verify reassembled bytes against the info hash -> only then construct a
`Torrent` and proceed as today). That's a restructure of the client's entry path, not a feature
addition, so it was called out and deferred rather than built silently. The `.torrent`-file entry
point is the accepted scope; BEP 6 (Fast Extension) and BEP 11 (PEX) are not.
