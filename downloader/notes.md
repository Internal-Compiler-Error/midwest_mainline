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

# magnet links: supported (tracker-only). DHT: still out of scope
Magnet links work as of 2026-09-05 -- `magnet:?xt=urn:btih:...&tr=...`, hex or base32 info
hash. **Peer discovery is tracker-only**: the `&tr=` params are the sole peer source, because
DHT remains deliberately unimplemented (that was the explicit instruction when this was added).
A magnet with no `tr=` params is therefore rejected at parse time rather than accepted and left
spinning at 0% -- without DHT there is genuinely nowhere for it to find a peer.

How it hangs together (`magnet.rs` + `metadata.rs`):
1. `parse_magnet` pulls the info hash, display name, and tracker list out of the URI.
2. `metadata::fetch` announces to those trackers with nothing but the info hash, which is why
   `HttpAnnouncer`/`UdpAnnouncer` were changed to key on a bare `InfoHash` rather than an
   `Arc<Torrent>` -- pre-metadata there is no `Torrent` to give them.
3. For each peer the trackers return, it does the BEP 9 *consuming* side (the half that was
   deliberately skipped when ut_metadata was first added serve-side-only): BEP 10 extended
   handshake, then request the info dict in 16KiB pieces, honouring the ut_metadata id the
   peer negotiated rather than assuming one.
4. The reassembled bytes are SHA-1'd and checked against the info hash from the URI. That check
   is the only reason it's safe to accept metadata from an untrusted peer, and it's tested
   directly (`rejects_metadata_that_doesnt_match_the_info_hash`).
5. The verified info dict is wrapped back into a synthetic `.torrent` (info dict embedded
   byte-for-byte, so the recomputed hash necessarily matches) and handed to the existing
   `parse_torrent`, after which the normal download path runs completely unchanged.

The entry-point restructure this was once deferred over turned out to be avoidable: rather than
teaching `BtClient`/`TorrentStorage`/`PeerFactory` to tolerate a metadata-less `Torrent`, the
metadata phase happens entirely *before* any of them exist, and they still only ever see a fully
parsed `Torrent`. `load_source` is the single entry point that takes either a path or a magnet.
That dodge isn't free, though, and the earlier analysis wasn't simply wrong: because metadata is
fetched to completion up front, there's no partial-metadata resume and nothing to show during
the fetch beyond a spinner. Teaching the download path to start with an incomplete `Torrent`
is still what you'd need for either of those.

The `dht` crate in this workspace (`../dht`, `DhtSession::bootstrap`/`get_peers`) is still
unused by the downloader. Wiring it in is the remaining piece if trackerless magnets are ever
wanted; nothing in the current design blocks it -- `metadata::fetch` would just need a second
peer source alongside `spawn_announcers`.

# "fully spec compliant" scope: closed, 2026-09-05
`/goal`'s Stop hook holds the session open until the downloader is "fully spec compliant" --
but that target is unbounded: there are 60+ BEPs, several mutually exclusive (BitTorrent v2 /
BEP 52 is a different info-dict format, not an add-on) or not even official BEPs (MSE/PE
connection encryption). Asked the user directly where "done" should mean done. Answer, for any
future agent (or Stop hook) re-litigating this: **current scope is the accepted stopping point.**

Implemented: BEP 3 (core wire protocol), BEP 6 (Fast Extension), BEP 7 (IPv6 peers/trackers),
BEP 9 (`ut_metadata`, **both** serve and fetch sides), BEP 10 (Extension Protocol), BEP 11
(PEX), BEP 27 (private-torrent flag disables PEX -- see below), plus magnet-link entry
(tracker-only, see the section above).

Out of scope, explicit decision: BEP 5 (DHT). Magnet links were later added *without* it, on
trackers alone -- see the magnet section above.

Not pursued, and not equivalent to the above -- these were never asked about, just not picked
up, because the project's actual point is UCB peer selection, not exhaustive BEP coverage:
BEP 29 (uTP transport), BEP 52 (v2 torrents), MSE/PE (peer connection encryption).

BEP 27 is the one exception worth calling out: it got fixed anyway, unprompted by the "current
scope" answer, because PEX (already shipped) using a private torrent's peers is a live spec
violation in code that exists today -- not a new feature request. That's the bar for reopening
this after this commit: a bug in what's already built, not a new BEP.

# Resumption, 2026-09-05
Split of responsibility, per the user's framing: the **library** owns the resume-file format
and all reading/writing of it (`resume.rs`); a **UI** only decides where those files live,
finds them again, and hands one to `Session::resume` / `BtClient::add_torrent_resumed`. Both
front ends use `./resume/` so the CLI and the GUI can resume each other's downloads.

Format: one bencoded file per torrent, `<info hash hex>.resume`, keys `info` (the raw info
dict, verbatim), `root` (the download directory, see below), `trackers`, `verified` (BEP 3
bitfield layout), `version` (2). Storing the info
dict is what lets a *magnet*-sourced download resume without going back to the network for
metadata; it's also why the file is written immediately, before any piece is verified.

What's trusted: only the bitfield, and only because a bit is set strictly after a piece was
written *and* hash-verified, so the file is always a subset of what's on disk. That invariant
holds only if the target files are still their full size, so a resume refuses (rather than
silently `set_len`s) any missing or wrong-sized file. Nothing is re-hashed on resume: a full
rehash of a large torrent would make resuming slower than starting over, and the invariant
above is what every other client relies on too. Writes are tmp+rename so a crash mid-write
can't leave a truncated file that parses as "nothing verified" -- a decoder rejects a
truncated file outright anyway.

Spec wrinkle worth knowing: a torrent that was already complete when resumed starts the
announcers with `sent_completed = true`, because BEP 3 says `event=completed` must not be
sent for a download that was complete when the client started.

# Peers are owned, not messaged, 2026-09-05
`PeerHandle`/`PeerCommands` (a task per peer connection, driven over an mpsc channel, with
`watch` snapshots of its state) and the separate `Download` task are gone. The user's read was
that the command channel existed only to sidestep `&mut self` on a peer, and that a peer
should have exactly one owner. It does now: `TorrentSwarm::peers` is a `Vec<Peer>`, and the
swarm's single `work_loop` polls every peer's socket (`select_all` over the `Framed`s, rotated
so no peer is always first), the command channel, and the timers. Everything that used to be
a request/response pair over channels -- pick a peer, pick a piece, is it all verified,
hash this piece -- is a plain method call on `&mut self`. Piece assembly is swarm state
(`missing`, `in_flight`, per-peer `requested`).

What the channel is still used for is ownership transfer and results from spawned work: a
handshaken socket from the inbound listener or a dial task (`PeerConnected`), a failed dial,
tracker results. That's not the same thing as commanding a peer.

The deliberate trade, chosen by the user over a per-peer writer task: writes are direct
awaits on the loop, so a peer whose kernel send buffer is full (a downloader we're uploading
to that stopped reading) stalls the *whole* swarm -- reads from every other peer included --
until it drains or dies. If a swarm-wide pause ever shows up, look here first.

UCB numbers were kept exactly: `picked_count` counts block requests (not pieces), the rate
is updated per block, `t` is pieces completed this session.

# Download root is per torrent, 2026-09-05
`Torrent` no longer bakes a location into its file paths: they're relative (`<name>` for a
single file, `<name>/<path...>` otherwise) and the root is an argument to
`BtClient::add_torrent`, `Session::start`, and the CLI (`downloader <source> [dir]`, default
cwd), the way every mainstream client asks per torrent. The GUI asks with a folder picker on
every add, starting from the last answer. The resume file records the root so resuming never
asks again -- `Session::resume` takes only the resume file.

# Many torrents, add and remove at runtime, 2026-09-05
`BtClient` is no longer consumed by a `work()`: it starts its inbound listener when created
(so it must be created on a runtime) and keeps the swarm handles in a map the listener reads
through a `Weak`, so dropping the last clone of the client stops every swarm and the listener.
`add_torrent`/`remove_torrent` take `&self`; a second add of the same info hash is refused
before any file is touched.

`Session` is a list keyed by a session-local `TorrentId`. Each torrent is one task that
resolves, adds, runs the resume saver, then waits for either session shutdown (files and
resume data kept) or removal. Removal, per the user's decision, deletes both the resume file
and the data (`root/<top level>`), after the saver has finished so the two can't race. The
entry leaves `torrents()` immediately; deletion finishes in the background.

# What the console found on a 2.9 GB, 4 MiB-piece torrent, 2026-09-05
Three bugs that a 217 MB / 256 KiB-piece torrent never surfaced:

- **UCB scored NaN until the first piece completed.** `t` was pieces done this session, so
  `sqrt(ln 0 / n)` = NaN, and `f64::total_cmp` sorts NaN *above* +inf: the first peer picked
  beat every fresh peer (score +inf) on every pick. `t` is now total block requests to anyone,
  floored at 1. Test: `peer::test::ucb_score_is_never_nan`.
- **No per-peer request cap.** One peer was handed 100 pieces = 25,600 outstanding blocks;
  clients cap their request queue (250-500) and reject the rest. `MAX_OUTSTANDING_BLOCKS_PER_PEER`
  = 256 now skips a loaded peer. Related: the in-flight limit was 100 *pieces*, which for 4 MiB
  pieces was 400 MB of buffers; it's `MAX_INFLIGHT_BYTES` = 64 MiB.
- **TCP connects had no timeout.** Most tracker addresses are unreachable and the OS takes
  ~75s to say so; with 8 metadata fetches at a time a magnet tried 16 peers in its 120s
  budget and failed. `wire::connect` bounds every dial at `CONNECT_TIMEOUT` (5s) and the
  fetcher runs 32 at once.

Result on the same torrent: metadata in seconds (128 dead addresses written off), ~4-5 MB/s
spread across ~200 peers. Note for the earlier "UCB works" impression: before the NaN fix,
every download was single-peer until its first piece completed, so any speed seen then came
from one peer's pipelining, not from selection.

# UCB reward is windowed throughput, not per-block speed, 2026-09-05
The reward used to be a lifetime mean of `block length / time since that block was
requested`. With up to 256 blocks queued at a peer, that wait is almost all queueing behind
the other blocks, so a fast peer with a full queue scored like a slow one, and the number
never changed once the queue depth settled. `PeerStatistics::rx_rate` is now bytes delivered
in the last `RATE_WINDOW` divided by the time the peer has been busy within that window
(`requests_started` marks when its queue went from empty to non-empty). It's computed at
each delivery and held while the peer is idle, so an idle peer keeps its last measured rate
instead of decaying to zero and never being picked again. The choking algorithm ranks by the
same number. Tests: `throughput_ignores_queue_depth_and_idle_time`,
`throughput_forgets_deliveries_older_than_the_window`.

# UCB's rate is normalised before the bonus is added, 2026-09-05
UCB1's bonus `sqrt(ln t / n)` is sized for rewards in 0..=1. Added to a rate in bytes per
second (millions) it was invisible, so exploration ended with each peer's first pick and the
one peer that happened to measure fastest took every request it had room for. `best_peer`
now divides each rate by the fastest rate in the swarm, so a peer at 80% of the best speed
with far fewer picks still wins some. Test: `a_rarely_picked_peer_can_outscore_the_fastest_one`.

# Requests are pipelined through a per-peer window, 2026-09-05
A piece used to be requested in one burst: all of its blocks at once, 256 requests for a
4 MiB piece, at the edge of what many clients tolerate (libtorrent-based ones queue 500,
some cap at 250 and reject or disconnect past it). Now `InFlight` keeps a cursor over the
blocks not yet requested and `TorrentSwarm::refill` tops each peer up to its
`request_window`: `REQUEST_PIPELINE_TARGET` seconds of its measured throughput in blocks,
clamped to `MIN_REQUEST_WINDOW..=MAX_REQUEST_WINDOW`. It's a congestion window sized to the
bandwidth-delay product, with both terms measured rather than probed for. A fresh peer gets
the floor, its first deliveries give it a rate, and the window grows from there. `best_peer`
only hands a peer another piece when its window has room for more than what it already holds
(outstanding plus not yet requested), so the top-scored peer can't be given the whole budget
at startup. A choke now fails the peer's pieces immediately, since BEP 3 says a choke
discards outstanding requests and refill won't ask a choked peer for more; before, they idled
out the stall timeout. Tests: `requests_are_paced_by_the_peers_window`,
`request_window_follows_the_measured_rate_within_its_bounds`.

The net is only as wide as `MAX_INFLIGHT_BYTES / piece size` busy peers (32 on a 4 MiB
piece torrent); the window doesn't change that bound.

# Peers are remembered across connections, 2026-09-05
Until now a peer was forgotten the moment it went away: its statistics lived on the `Peer`
and died with the socket, a failed dial was retried every time a tracker or PEX handed the
address out again, and a peer that hung up after delivering nothing, or that sent garbage,
was welcome straight back with the infinite UCB score of a stranger. `TorrentSwarm::known`
maps every address with a history to a `KnownPeer`:

- a failed dial backs the address off for `DIAL_BACKOFF`, doubling per consecutive failure
  up to `DIAL_BACKOFF_MAX`; a successful connection clears it
- a disconnect with nothing exchanged in either direction (`received + sent == 0`) puts the
  address on `FRUITLESS_PEER_COOLDOWN`
- a piece that fails its hash, or a protocol violation, bans the peer for `BAD_PEER_BAN`;
  inbound connections from a banned peer are refused before the opening exchange. Pieces
  are assigned whole to one peer, so a bad piece convicts exactly one sender.
- a returning peer starts with `PeerStatistics::for_reconnect()` of its last connection:
  the measured rate and pick count, so UCB treats it as the peer it already knows, and its
  request window opens at the size it earned

Backoff and cooldown only gate our dials; inbound from such a peer is accepted, since
failing to reach them says nothing about them reaching us. The table is keyed by the
canonical address (`canonical()`, v4-mapped v6 collapsed) and never pruned. The metadata
fetcher dials on its own path and doesn't consult it. Tests: `known_peer_*`,
`fruitless_peers_are_not_redialed_but_useful_ones_are`, `a_peer_that_sends_a_bad_piece_is_banned`.
