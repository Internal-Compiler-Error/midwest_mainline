# Roadmap (autonomous session started 2026-09-06)
The user asked for "as many features of a typical bt client as make sense", with quality
passes on existing code in between. One feature per commit. Status is kept here so it
survives context resets; re-read before acting.

- [x] Data directory: `dirs::data_local_dir()/downloader`, override `DOWNLOADER_DATA_DIR`,
      for dht.db and resume files, CLI and GUI alike
- [x] DHT (BEP 5) via the sibling `dht` crate: embedded migrations in the crate, session
      started in the background, announcer for swarms and the metadata fetcher, Port message
- [ ] DHT crate rework (user: "very badly designed, change it however you wish"): learn the
      external IP from BEP 42 responses instead of public-ip; lookups that don't wait for a
      whole round; drop SQLite for an in-memory table saved to a file?
- [x] Auto-resume every torrent in the data dir on startup
- [x] Pause/resume per torrent; remove with files vs remove keeping files
- [x] Peer list in the GUI (address, client, rates, progress, flags)
- [x] Persisted settings (listen port, default download dir, max peers) + settings dialog
- [x] Global speed limits
- [x] File selection (priorities not done: the scheduler is rarest-first, a priority order would fight it)
- [x] Local Service Discovery (BEP 14)
- [x] Seeding ratio / stop seeding
- [x] Peer stream abstraction (`PeerStream`: Tcp | Encrypted | Utp), no behaviour change
- [x] MSE (BEP "protocol encryption"): DH + RC4 stream wrapper, initiator and responder,
      settings Disabled/Prefer/Require with plaintext fallback
- [x] Port mapping: NAT-PMP/PCP via `crab_nat`, UPnP via `igd-next`
- [x] Force recheck
- [x] Sequential download
- [x] Status bar: total rates, DHT node count, listen port, port mapping state
- [x] uTP via `librqbit-utp` on the listen port's UDP number; the DHT node moved to the
      next port up (the crate can't take a shared socket, see the uTP section)
- Quality pass every 2-3 features: tests, clippy, pnpm check, code review, notes vs code

Decisions made without the user (to report): data dir location; remove now asks (keep files
or delete files) instead of always deleting; the endgame raced-piece cap is now 32 MiB of
pieces rather than 5% of the piece count (2% waste on the 3068-piece Arch ISO vs 0.2% on
Silo came from the count-based cap). The DHT node's UDP port is `listen_port + 1` (6882 by
default), not 6881: uTP needs the listen port's number and the uTP crate can't share a
socket; anyone with only 6881/UDP forwarded loses inbound DHT queries until 6882 is too.

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

# UCB runs on a subsample of peers, 2026-09-05
A 3-minute run on the Silo swarm with 251 peers collapsed to 89 pieces: 134 of 160 piece
assignments went to peers UCB hadn't tried (infinite score), each trial cost a whole 4 MiB
piece and one of the 32 in-flight slots, and the proven fast peers sat idle once the slots
were full of strangers trickling data at the floor window. That's the many-armed regime of
Bayati, Hamidi, Johari, Khosravi, "The Unreasonable Effectiveness of Greedy Algorithms in
Multi-Armed Bandit with Many Arms" (arXiv:2002.10121): with more arms than about
sqrt(horizon), UCB over all arms is provably sub-optimal (trying each once already costs
order-k regret) and UCB on a random subsample of sqrt(horizon) arms is rate-optimal.

`TorrentSwarm::subsample` is that subsample, sized `ceil(sqrt(pieces missing at start))`
(27 on Silo). Ready peers are admitted in peer order until it's full; a member's slot frees
only when it disconnects. `best_peer` considers members only. Peers outside it are still
connected, served, and gossiped over PEX. Test: `only_the_subsample_is_asked_for_pieces`.

Also from that run: trackers and PEX hand out addresses with port 0 (232 of 3175 failed
dials); they're dropped at discovery now. And the CLI log has timestamps again, since
"lines from request to completion" was the only duration available.

## Noted, not done: greedy instead of UCB
The same paper's stronger finding is that in this regime plain greedy (try each arm once,
then always pull the empirically best) beats UCB and Thompson sampling, and subsampled
greedy beats everything, because with many arms there are many near-optimal ones and
fixating on a good one costs little. Their sequential greedy (Algorithm 5) is written for
arms that arrive one at a time. The version that fits this swarm: score a known peer by its
windowed rate alone, score a stranger by the mean rate of peers tried so far (so it beats
only below-average known peers), no exploration bonus, no normalisation. The window
mechanism keeps good peers saturated and the leftover budget flows to strangers, which is
sequential greedy running in parallel across the in-flight slots. Try this if SS-UCB isn't
enough.

# Endgame, 2026-09-05
The last pieces of a download used to wait on whichever peer happened to hold them, while
faster peers sat idle with nothing left to assign. Now an in-flight piece can have several
claimants (`InFlight::claims`, one request cursor each). When `missing` is empty after the
normal assignment loop, `race_the_last_pieces` gives the in-flight pieces furthest from
done (bytes left over the claimants' combined rate) to more peers, up to `ENDGAME_RACERS`
per piece and with at most `ENDGAME_MAX_RACED_FRACTION` of the torrent's pieces raced at
once. The trigger is "nothing left to assign and a peer has room", not a percentage: it
fires at the right moment for any torrent size, and the fraction only caps the damage.
The second claimant walks the piece from the end, so the two meet in the middle and the
bytes fetched twice are about halved. Blocks record their sender; when the piece
completes, every other claimant gets Cancel for what it still owed (`cancel_losers`), a
duplicate block is counted in `TorrentSwarmStats::wasted` (shown in the GUI), and a hash
failure bans only when every block came from one peer. `release_claim` replaced
`fail_piece`: a choke, reject, stall, malformed block, or disconnect takes that peer off
the piece, and the piece goes back on the pile only when nobody else holds it. Test:
`the_last_pieces_are_raced_and_the_loser_is_cancelled`.

# The GUI is a Tauri 2 + Svelte 5 app, 2026-09-06
`gui/` at the workspace root: `gui/src` is the Svelte frontend (Vite, TypeScript, runes),
`gui/src-tauri` is the Rust shell, a workspace member named `downloader-gui`. The shell
holds a `Mutex<Session>` in Tauri state and exposes one command per `Session` method plus
`resumable`, `logs_since`/`clear_logs` and `default_download_dir`; the DTOs mirror
`TorrentState`/`Progress` so the library stays free of serde. The frontend polls `torrents`
and `logs_since` every 250 ms (the `LogBuffer` grew a monotonic `pushed` counter for that)
rather than using Tauri events: simpler, and it survives a hot reload. Folder and file
pickers come from the dialog plugin, which must be both registered in `main.rs` and allowed
in `capabilities/default.json`. The session is shut down from the run-event callback on
`ExitRequested`, on the main thread, because it owns a tokio runtime that can't be dropped
from inside Tauri's own runtime.

Run it with `pnpm tauri dev` in `gui/` (needs `pnpm install` once); `pnpm check` type-checks
the Svelte side.

Styling is Tailwind 4 with shadcn-svelte (preset chosen by the user on shadcn-svelte.com,
recorded in `gui/components.json`; Vega style, neutral base, green primary, Inter, Lucide).
Components are copied into `gui/src/lib/components/ui` by `pnpm dlx shadcn-svelte@latest add
<name>`, so they're ours to edit. `mode-watcher` follows the system dark mode by setting the
`.dark` class the stylesheet keys on. The console sits in a vertical paneforge pane group
under the main view, so its height is dragged, and the toggle lives in a footer bar so the
pane can be removed entirely. The `$lib` alias is set in both `vite.config.ts` and the
tsconfigs, and shadcn's CLI checks the root `tsconfig.json` for it.

Known gap: like the CLI, it resolves `resume/` and the default download dir against the
current directory, which is `/` when launched from Finder. A proper data directory is the
next thing to decide.

# DHT, 2026-09-06
The client runs one BEP 5 node (`dht.rs`, on the sibling `midwest_mainline` crate) per
process, on UDP port 6881 or any free port if that's taken, with its routing table in
`<data dir>/dht.db`. It starts in the background: bind, learn the external IP for the
BEP 42 node id (`public-ip`, 5 s, else 0.0.0.0 and a warning), open the database, spawn the
node's tasks, bootstrap from five well-known routers, then publish a `DhtHandle` through a
`watch` that consumers hold as `DhtWatch`. `Dht::none()` is the watch for a client without
DHT (tests). `spawn_announcers` gained a DHT announcer next to the tracker ones: every
`DHT_ANNOUNCE_INTERVAL` it runs `get_peers`, emits `PeersDiscovered`, and announces our TCP
port to the nodes that issued tokens (implied_port only when UDP and TCP ports match). Both
the swarm and the metadata fetcher get it, so a magnet with no trackers works: the Arch
Linux ISO magnet resolved in 25 s and pulled 2000 pieces in 90 s from DHT peers alone. The
Port message (id 9) is decoded now, and a peer's DHT node gets pinged into our table.

Things that had to change around it: `parse_magnet` and `parse_torrent` no longer require
trackers; the metadata fetch's 120 s timeout counts from the last peer discovery rather
than from the start, because the DHT can take 20 s to come up; and the peers a metadata
fetch met are handed to the swarm (`Loaded::peers`, `BtClient::add_peers`) so it doesn't
start cold. In the crate: embedded migrations, a lenient parser (unsorted keys are common
in the wild), 3 s request timeout and 8 queries per round (a lookup went from 90 s to
about 20 s).

The data directory (`paths::data_dir`, `DOWNLOADER_DATA_DIR` to override) came with it,
since the database needed a home that isn't the current directory: resume files moved there
too, for the CLI and the GUI alike. Existing `./resume` files aren't migrated.

`DOWNLOADER_LOG_ADDR=host:port` makes the GUI's `LogBuffer` also stream every line to a TCP
listener (`nc -l 9999`), so its console can be watched from a terminal.

# Pause, unpause, remove with or without files, auto-resume, 2026-09-06
`Session` torrents are driven by a `TorrentTask` per torrent with a command channel
(`Pause`, `Unpause`, `Remove { delete_files }`), replacing the one-shot remove. Running means
the torrent is in the `BtClient` with a resume-file saver alongside; pausing takes it out of
the client (connections and announces stop, files stay), publishes `TorrentState::Paused`
with the last stats, and writes the resume file with a `paused` flag (format unchanged
otherwise: the key is simply absent when false) so a restart brings it back paused.
Unpausing adds it back as a resumed torrent from the verified bits the swarm reported when
it left. Remove takes the resume file always and the data only when asked; the GUI's row
menu offers both. `Session::resume_all` resumes every file in the resume dir that isn't
already in the session, and `Session::resumable` is that same filtered list, so the GUI
starts with everything from last time and only offers what isn't running. Tests:
`pause_survives_a_restart_and_unpause_starts_again`, `remove_can_keep_the_files`.

# Peer list, 2026-09-06
`Peer::snapshot` is taken for every peer once a second in housekeeping and published on the
swarm handle's second watch (`TorrentSwarmHandle::peers`), kept apart from the stats watch so
the announcers and the resume saver don't wake for it. The handshake's peer id is kept on
the `Peer` and `peer::client_name` turns it into "qBittorrent 5.1.0.0" and friends
(Azureus and Shadow styles, printable prefix otherwise). The session adds per-peer rates
from deltas (same `Rates` as the torrent's) and the usual `D/d U/u` flags, and the GUI
shows a table under the details.

# Settings and rate limits, 2026-09-06
`config::Settings` is `<data dir>/settings.json` (serde, missing keys default): listen
port, default download dir, DHT on/off, peers per torrent, download and upload limits.
`Session::new` takes them in `SessionConfig`; `Session::update_settings` saves and pushes
them through a `watch` that every swarm holds. The connection cap applies to inbound and
outbound alike; the port and the DHT switch only apply at the next start, and the GUI's
dialog says so. The library grew serde for this, so the GUI's DTO for settings is a
field-for-field copy in the units the dialog edits (limits in KiB/s).

Limits are one `limiter::RateLimiter` per client, a token bucket per direction that reads
its rate from the settings watch on every take (0 = unlimited). Download: `refill` asks for
a block's worth before each request and stops when refused; housekeeping's `schedule()`
retries a second later. Upload: `send_block` asks before sending and parks refused blocks
in `held_uploads`, which housekeeping drains as the bucket allows. Granularity is therefore
about a second, and a bucket holds at most one second's worth, so a quiet spell can't bank a
burst. On the Arch ISO with a 500 KiB/s limit the mean over 30 s was 443 KiB/s and the
worst second 640 KiB/s. Test: `the_download_limit_paces_requests`.

# File selection, 2026-09-06
`Torrent::wanted_pieces(selected)` marks every piece holding a byte of a selected file (a
piece shared with an unselected file is still wanted, so both files' bytes in it are
correct). `TorrentSwarmStats` carries that `wanted` bitfield next to `verified`: `left`,
`completed`, `verified_cnt` and `total_pieces` all count wanted pieces only, so announces,
the progress bar and "complete" mean "what the user asked for". `SwarmEvent::FilesSelected`
rebuilds `missing` from the new selection (pieces in flight finish either way). The
selection travels as one flag per file: `Session::select_files`, a `watch` in the torrent
task that the resume saver reads (stored as the `skip` list of file indices, absent when
everything is selected) and that the phase exposes for `Progress::files`, which now carries
path, size and the flag; the GUI's file list has a checkbox per file. Priorities (this file
first) were left out on purpose: the scheduler is rarest-first with UCB peer choice, and a
priority order would fight both; sequential download is the same story. Test:
`deselected_files_pieces_are_not_requested`.

# Local Service Discovery, 2026-09-06
`lsd.rs`: one multicast socket per client on 239.192.152.143:6771 (SO_REUSEADDR and
SO_REUSEPORT so several clients on one host share it), one task that every `LSD_INTERVAL`,
and right after a torrent is added, sends a `BT-SEARCH` naming every torrent in the client
(20 Infohash lines per message), and listens for everyone else's; a sender naming a torrent
we have becomes `PeersDiscovered` for that swarm. A per-torrent socket was tried first and
made the swarm test module take 11 s instead of 1: fifteen sockets on the same multicast
port in one process, all joined to the group, contend on something in the kernel even when
idle. One per client is also what BEP 14 intends. Own announces are told apart by a random
cookie. Tests: the parser and the round trip of what we send; the live check was a
multicast from Python naming a running torrent, which the client dialled within a second.

# Code review findings fixed, 2026-09-06
From a review of the day's diff: a torrent that failed after running couldn't be removed
(its task had exited; now `TorrentTask::fail` keeps taking commands and deletes the resume
file on Remove); a pause during resolve killed the resolve (now remembered and applied
after); a paused torrent showed zeroed counters (two publishes of `Phase::Paused`, the
second a blank; now one, fed the last stats); a tracker-less magnet with the DHT off hung
120 s instead of failing at once (`Dht::none` is a watch with a dropped sender, and
`metadata::fetch` bails when there are no trackers and no DHT can ever come); the handshake
never set the BEP 5 bit so no peer ever sent Port (now set from `Identity::dht`, and we send
our own Port after the handshake); the metadata deadline had no ceiling (`OVERALL_TIMEOUT`
of 10 min on top of the 120 s idle timeout); the log forwarder queued without bound while
a connect blocked (bounded channel, connect timeout, 5 s between attempts); and doc comments
displaced by inserted functions.

# Seeding ratio, 2026-09-06
`Settings::seed_ratio_limit` (0 = seed forever): a complete torrent whose lifetime upload
total reaches that many times its size pauses itself (`TorrentTask::seeded_enough`, checked
on every stats change, on every settings change, and once at the start of a stretch, so a
torrent that comes back already over the limit stops at once). The lifetime total is the
resume file's new `uploaded` key plus the running swarm's count; the task carries it as
`uploaded_before` across pause/unpause and sessions, and `Progress::uploaded` is the total,
with `Progress::ratio()` for display. The GUI shows the ratio in the details and edits the
limit in the settings dialog. Test: `seeding_stops_at_the_ratio_limit`.

# Protocol encryption (MSE), 2026-09-06
`mse.rs` is the Vuze/Azureus "Message Stream Encryption" that every mainstream client speaks:
768-bit Diffie-Hellman, SHA-1-derived RC4 keys with the first 1024 keystream bytes dropped,
and the info hash mixed into the key so a responder serving several torrents can tell which
one the initiator means. It is obfuscation against traffic shaping, not secrecy, and the
settings dialog shouldn't pretend otherwise. Only RC4 is offered and accepted; the spec's
"plaintext after the key exchange" option is treated as no encryption, since it would need a
third stream flavour for no gain.

`PeerStream` (`stream.rs`) is what everything above the handshake holds now: `Tcp`, or
`Encrypted` wrapping another `PeerStream` (so MSE-over-uTP is free later). `stream::connect`
and `stream::accept` are the only places that know the policy: `Disabled`, `Prefer`
(default; try encrypted, reconnect in plaintext if that fails), `Require`. Inbound, the
first 20 bytes decide: the protocol string means plaintext, anything else is a DH public
key. The policy lives on `Identity`, so like the port it takes effect at the next start.
The whole opening (connect, MSE, handshake) is bounded by `HANDSHAKE_TIMEOUT`, which it
wasn't before.

A `Prefer` fallback costs one extra connection per peer that doesn't speak MSE. `KnownPeer`
could remember which peers those are and skip the encrypted try; not done, since nearly
every client accepts it. Peers show `E` in their flags when encrypted.

Tests: RC4 known answer, DH agreement, both handshake sides over an in-memory pipe (several
served torrents, a pre-read head, a torrent we don't serve), and `stream::connect` against
`stream::accept` over TCP for encrypted, fallback, and both `Require` refusals.

# uTP, 2026-09-06
`utp.rs` binds a `librqbit_utp::UtpSocketUdp` on UDP `listen_port` (dual-stack if the
platform allows, else v4, else any port with a loud warning) and hands it out through a
watch like the DHT's. `PeerStream::Utp` is the third stream flavour; `Encrypted` wraps it
like it wraps TCP, so MSE over uTP came for free. The inbound side is `BtClient::accept_utp`,
which feeds the same `welcome` (opening, handshake reply, hand to the swarm) as the TCP
listener. Peers show `T` in their flags.

Dial order: TCP first; uTP only when TCP can't *connect* (refused, unreachable, timed out).
A peer TCP reaches but that rejects the handshake is reachable and just didn't want us, so
it isn't retried over uTP. That means outbound uTP fires rarely; inbound uTP is where most
of it happens, and on a VPN without a port forward there is none. libtorrent prefers uTP
for peers PEX flagged uTP-capable (`added.f` bit 0x04), which this PEX neither sends nor
reads; that's the refinement if uTP is ever wanted more often. The whole dial (both
transports, an MSE attempt, a plaintext retry) is bounded by `HANDSHAKE_TIMEOUT`, now 30 s.
While here, `Prefer` no longer opens two TCP connections to a dead peer: the first connect
happens once, and only the plaintext retry dials again.

The plan was to share UDP 6881 between uTP and the DHT with a first-byte demux (KRPC is
bencode and starts with `d`; a uTP header's low nibble is the version, 1). `librqbit-utp`'s
`Transport` trait allows a custom transport, but the constructor that takes one also wants
a `UtpEnvironment`, and neither that trait nor its default type is exported, so the
constructor is unreachable from outside the crate. Forking it for one `pub use` isn't
"using an existing solution". Instead the DHT node takes `listen_port + 1`
(`Settings::dht_port`): BEP 5 carries the node's port in the Port message precisely so it
can differ from the peer port, and the routing table in SQLite is about other nodes, so
ours changing port costs a warm-up and nothing else. The announcer's "implied_port when
the UDP and TCP ports match" branch went with it, since they never match now.

Tests: `utp_is_tried_when_tcp_is_refused` dials a port that has a uTP socket and no TCP
listener, under `Disabled` and `Prefer`, and checks both ends see uTP (and encryption).

# Port mapping, 2026-09-06
`portmap.rs` asks the router to forward the listen port (TCP and UDP, for peers and uTP)
and the DHT port (UDP): NAT-PMP/PCP through `crab_nat` first, since that's one UDP round
trip and what every router of the last decade speaks, then UPnP IGD through `igd-next` if
nothing answers. Leases are two hours, renewed at half, removed at shutdown. Nothing else
in the client knows whether a mapping exists; the outcome is one log line.

The router is found with `netdev`: the default route's interface if it has a gateway, else
any interface that does. The second case is the user's own machine: on a VPN the default
route is the tunnel, with no router behind it, and the LAN interface still has one. A
mapping made there is useless while the VPN is up (peers learn the VPN address, not the
ISP's), but harmless, and it would be what makes the feature testable here, except that the
router at 192.168.2.1 answers neither NAT-PMP/PCP nor UPnP (a raw SSDP M-SEARCH from the
LAN address gets no responder at all), so only the "found the gateway, tried both, gave up
cleanly, retries in ten minutes" path has run for real. Port 0 (tests) maps nothing.
Setting: `port_mapping`, default on, next start.

# Force recheck, 2026-09-06
`Session::recheck(id)` re-hashes a torrent's files (`check::check_files`, off the runtime in
`spawn_blocking`) and continues from what's really on disk, in whichever of downloading or
paused it was in. While it runs the torrent is `TorrentState::Checking` with a piece count;
only removal and shutdown interrupt it, and the hashing itself finishes regardless. A file
that's missing or short fails every piece touching it. The GUI has it in the row's menu.

Tests: `check::good_bad_and_missing_pieces_are_told_apart` (a piece straddling two files, a
corrupted piece, a missing file) and `session::recheck_finds_what_is_on_disk`.

# Sequential download, 2026-09-06
A per-torrent switch (`Session::set_sequential`, `sequential` in the resume file, a Switch in
the details panel) that makes `TorrentSwarm::next_piece` return the lowest missing piece
anyone has instead of the rarest. Peer selection is untouched: UCB still decides who gets
the piece. For playing a file while it downloads; costs the swarm some piece diversity,
which is why it's off by default and per torrent. Test:
`sequential_asks_for_pieces_in_order`.

# Status bar, 2026-09-06
`Session::status` sums the per-torrent rates and reports the DHT's routing table size (the
handle now carries the session for `node_count`), the listen port, and where port mapping
stands (`portmap::MappingState`, published through a watch like everything else the client
starts in the background). The GUI shows it at the right of the footer, polled with the
torrent list. Test: `status_reports_the_network_state`.
