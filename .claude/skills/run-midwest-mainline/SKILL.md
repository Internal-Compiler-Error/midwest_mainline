---
name: run-midwest-mainline
description: Build, run, test, and drive this workspace's subsystems - the `downloader` CLI, the Tauri GUI (incl. its live Traces pane), the DHT node (`json_rpc_server`, standalone or as a long-term index), and OpenTelemetry traces in Jaeger. Use when asked to start or run the downloader, the GUI, or the DHT node, run their tests, build them, take a screenshot of the GUI, watch logs or traces, profile or measure throughput, query or announce to the DHT, or check a download against a real swarm.
---

A Rust workspace: `downloader/` (the BitTorrent library plus a CLI), `gui/` (a Tauri 2 +
Svelte 5 desktop app over the same library), `dht/` (the `midwest_mainline` DHT crate they
use), and `json_rpc_server/` (a standalone DHT node with a JSON-RPC front, optionally a
long-term index of announced peers). Drive all of it with
`.claude/skills/run-midwest-mainline/driver.sh`, which runs each binary against a scratch
directory; for the GUI it streams the console to a file and screenshots the window, for the
DHT node it speaks JSON-RPC over curl and KRPC over UDP (`krpc.py`).

All paths are relative to the workspace root. Verified on macOS 26 with the Apple toolchain
(the GUI needs macOS's WebKit; the CLI and the library are portable).

## Prerequisites

Rust stable with the 2024 edition, Node 26 and pnpm 11 (`brew install node pnpm`), and the
Xcode command line tools for `swiftc`, `screencapture`, and `python3` (macOS ships all three). The
`juicy_bencode` crate is a path dependency at `../../juicy_bencode`, i.e. a sibling of the
workspace's parent directory; clone it there or the workspace won't resolve.

Network: the DHT and trackers need outbound UDP. Some ISPs block UDP trackers; the user runs
a VPN for that. If a run shows tracker connects with no replies and no DHT node, it's the
network, not the code.

## Build

```bash
.claude/skills/run-midwest-mainline/driver.sh build
```

`cargo build -p downloader -p downloader-gui`, then `pnpm install` and `pnpm build` in
`gui/`. About four minutes cold, under a minute warm. `dht-up` builds `json_rpc_server`
itself.

## Run (agent path)

Both commands run against `/tmp/midwest-mainline-run/data` (override with `RUN_DIR`), so
the user's own resume files, settings, and DHT table in `~/Library/Application
Support/downloader` are never touched. A good test torrent is the Arch Linux ISO, which is
tracker-less and finds a couple of hundred peers over the DHT; metadata arrives about 7-10 s
after the DHT node is up:

```bash
ARCH='magnet:?xt=urn:btih:f45add9d1a5185d8588df7dd6cd89993dd0174fa&dn=archlinux-2026.09.01-x86_64.iso'
```

**CLI** - runs for N seconds (or until the download completes, when the CLI exits by
itself), then prints a one-line summary of what happened:

```bash
.claude/skills/run-midwest-mainline/driver.sh cli "$ARCH" 120
# metadata: 1  dht up: 1  dht lookups: 2  peer connections: 274  pieces: 3068  complete: 1  warn/error lines: 2
# 1.5G	/tmp/midwest-mainline-run/cli-download
```

The full log is `/tmp/midwest-mainline-run/cli.log` (timestamped; `RUST_LOG=debug` for
more); the CLI logs a progress line every 5 s (`77.3%  2371/3068 pieces  down 31.1 MiB/s ...`).
"pieces" is verified pieces. The whole 1.5 GB ISO takes about 40-60 s in a debug build. The
CLI resumes from its resume file when run again on the same source; `--seed` keeps it
uploading after completion.

**BEP 46 (updating torrents)** - `downloader publish <key-file> <.torrent | info hash> [--salt
<text>]` points a throwaway key (made in the key file if missing) at a torrent and prints the
`magnet:?xs=urn:btpk:...` that follows it; a second publish to another hash moves it on (seq + 1).
Give the publisher and the follower separate data dirs and `listen_port`s in their
`settings.json` (never 6881). A follower resumed from its `.resume` (`--seed`, so a complete one
keeps running) polls 30 s after its DHT is up and logs "its BEP 46 key ... is at seq N now".

**GUI** - launches the debug app, optionally adding a source at startup (the app takes one
as its first argument), waits N seconds (default 45), screenshots the window, quits:

```bash
.claude/skills/run-midwest-mainline/driver.sh gui "$ARCH" 45
# screenshot: /tmp/midwest-mainline-run/gui.png (window 552)
# metadata: 0  dht up: 1  dht lookups: 2  peer connections: 225  pieces: 224  ...
PANE=traces .claude/skills/run-midwest-mainline/driver.sh gui "$ARCH" 20   # open the Traces pane instead
```

Look at `gui.png`: it should show the torrent row with a progress bar and rates, the details
panel (the first torrent is selected by itself), and the bottom pane: `PANE` (`console`,
`insights` (default), or `traces`; passed to the app as `DOWNLOADER_PANE`) picks which. The
Traces pane is a live timeline of the torrent's spans: lanes for metadata, trackers, DHT,
dials, peers and pieces, coloured by outcome, with a header of counts (`240 peers · 256
pieces in flight · 100 dialling · 1028 verified`). The GUI downloads into `$RUN_DIR/gui-download` (the driver writes a scratch
`settings.json` saying so; without it the app's default is `~/Downloads`). The GUI's console is also streamed to
`/tmp/midwest-mainline-run/gui.log`, which is how the summary is computed ("metadata" stays
0 there because only the CLI logs that line).

**DHT node** - `json_rpc_server` in the background on UDP 44444 and
`http://127.0.0.1:3000/json_rpc` (override with `DHT_PORT`, `RPC_PORT`), database
`$RUN_DIR/dht.db`. `dht-up` defaults to `forever` retention (the long-term index); pass
`expire` for a normal BEP 5 node. It returns once the node has bootstrapped and answers RPC:

```bash
D=.claude/skills/run-midwest-mainline/driver.sh
$D dht-up
# DHT node up (pid 46893, retention forever): UDP 44444, JSON-RPC http://127.0.0.1:3000/json_rpc
$D rpc node_count                       # {"jsonrpc":"2.0","result":182,"id":1}, ~1000 after two minutes
$D krpc ping                            # KRPC over UDP, as another DHT node would
$D krpc announce f45add9d1a5185d8588df7dd6cd89993dd0174fa 51413   # get_peers for a token, then announce_peer
$D rpc stored_swarms                    # {"jsonrpc":"2.0","result":["f45add9d..."],"id":1}
$D rpc stored_peers '{"info_hash":"f45add9d1a5185d8588df7dd6cd89993dd0174fa"}'
# {"result":[{"addr":"127.0.0.1:51413","first_announced":1791339694655,"last_announced":1791339694655}],...}
$D krpc get_peers f45add9d1a5185d8588df7dd6cd89993dd0174fa   # 'values': ['7f000001c8d5'] is 127.0.0.1:51413
$D dht-down
```

`$D dht-up expire` runs the same node as a normal one; it stores and serves the test announce
the same way, and deletes stale peers on a timer. The node's log (DEBUG, fixed in
`json_rpc_server`) is `$RUN_DIR/dht.log`.

`DHT_CRAWL=20 $D dht-up` also runs a BEP 51 crawler (20 `sample_infohashes` queries a second,
index mode only); `$D rpc sampled` counts what it found (`info_hashes`, about 10k after the
first minute) and how many nodes it asked and heard back from (about half answer).
`$D rpc node_counts` splits the routing tables by family and BEP 42 compliance.

**Traces in Jaeger** (optional, for developers; users get the Traces pane). The image is
pulled already (`docker.io/jaegertracing/jaeger:latest` in Podman). Any run with
`OTEL_EXPORTER_OTLP_ENDPOINT` set exports its spans over OTLP/HTTP:

```bash
podman run -d --name jaeger -p 127.0.0.1:16686:16686 -p 127.0.0.1:4317:4317 -p 127.0.0.1:4318:4318 docker.io/jaegertracing/jaeger:latest
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 .claude/skills/run-midwest-mainline/driver.sh cli "$ARCH" 25
curl -s "http://127.0.0.1:16686/api/v3/operations?service=downloader"   # dht.lookup, dial, metadata, metadata.peer, peer, piece, piece.check, shake_hands, ...
```

The UI is http://127.0.0.1:16686 (search service `downloader`, tag `info_hash=<hex>`). Jaeger
v2 serves its API under `/api/v3`; `/api/services` is a 404.

**Throughput** - measure in a release build; a debug build is CPU-bound at roughly a tenth
of the speed. The Ubuntu ISO has HTTPS trackers and a big swarm (`releases.ubuntu.com`, the
user offered it for testing):

```bash
curl -sfL -o /tmp/midwest-mainline-run/ubuntu-26.04.1-desktop-amd64.iso.torrent https://releases.ubuntu.com/26.04/ubuntu-26.04.1-desktop-amd64.iso.torrent
cargo build -q --release -p downloader
DOWNLOADER_DATA_DIR=/tmp/midwest-mainline-run/data timeout -s INT 60 target/release/downloader /tmp/midwest-mainline-run/ubuntu-26.04.1-desktop-amd64.iso.torrent /tmp/midwest-mainline-run/rel
# 75.8%  18738/24729 pieces  down 111.2 MiB/s  up 0 KiB/s  339 peers
```

110-125 MiB/s on the user's line at ~220% CPU. To see where the CPU goes, `sample` the
process mid-run (release builds keep line tables): `sample $(pgrep -x downloader) 6 -file
/tmp/midwest-mainline-run/sample.txt`, then read the "Sort by top of stack" section.

**Logs of a GUI the user is running**: start the GUI with
`DOWNLOADER_LOG_ADDR=127.0.0.1:9999` and run `driver.sh logs` to tail its console from a
terminal.

| command | what it does |
|---|---|
| `build` | cargo and pnpm builds |
| `test` | `cargo test --workspace` and `pnpm check` |
| `cli <source> [secs]` | run the CLI on a .torrent or magnet, summarize |
| `gui [source] [secs]` | launch the GUI, screenshot it, quit |
| `dht-up [expire\|forever]` | start the DHT node in the background, wait for RPC |
| `rpc <method> [params]` | JSON-RPC: `node_count`, `node_counts`, `stored_swarms`, `stored_peers`, `sampled`, `scrape`, `put`, `get` |
| `krpc <cmd> [args]` | KRPC over UDP: `ping`, `get_peers <hash>`, `announce <hash> <port>` |
| `dht-down` | stop the DHT node |
| `logs` | tail a running GUI's console over TCP |
| `clean` | delete the scratch directory |

## Direct invocation

Everything the GUI does goes through `downloader::Session`. `downloader/examples/probe.rs`
(committed) drives one directly: it adds a source, then every 5 s prints the rate, verified
pieces, and a uTP-vs-TCP breakdown of the peers. Arguments: source, then seconds (default 90).
Edit it in place to poke a library change without either binary:

```bash
DOWNLOADER_DATA_DIR=/tmp/midwest-mainline-run/data cargo run -q -p downloader --example probe -- "$ARCH" 60
# t= 60s total   52572 KiB/s pieces 768/3068 | uTP 14 peers (11 sending)     892 KiB/s best    204 | TCP 144 peers (126 sending)   78366 KiB/s best   4560
```

Nothing for the first several seconds is normal (DHT lookup, then metadata).

The DHT crate's internals (store, retention, routing table, KRPC parsing) are covered by its
unit tests on in-memory SQLite, which is the fastest loop for a change there:

```bash
cargo test -p midwest_mainline
```

## Run (human path)

Without `DOWNLOADER_DATA_DIR` both use the user's real data directory (`~/Library/Application
Support/downloader`), resuming their torrents; keep it set unless that's the point.

```bash
cd gui && DOWNLOADER_DATA_DIR=/tmp/midwest-mainline-run/data pnpm tauri dev   # Vite plus the debug app, window in ~10 s; Ctrl-C stops both (pnpm then says "Command failed", harmless)
DOWNLOADER_DATA_DIR=/tmp/midwest-mainline-run/data cargo run -q -p downloader -- "$ARCH" /tmp/midwest-mainline-run/human   # Ctrl-C: "interrupted ..., stopping tracker announces" and exits
```

## Test

```bash
.claude/skills/run-midwest-mainline/driver.sh test
```

145 downloader tests and 44 dht tests (one more is ignored: it needs the live DHT), in about
six seconds (the uTP dialing tests wait out real timeouts); `pnpm check` reports 0 errors.
Tests use free ports on loopback only (so macOS's firewall doesn't prompt for each new test
binary) and no DHT, so they run offline.

## Gotchas

- **The screenshot is blank (dark, no UI) when the screen is locked.** WebKit paints nothing
  behind the lock screen; check with
  `ioreg -n Root -d1 -a | grep -A1 CGSSessionScreenIsLocked` (a `<true/>` means locked) and
  wait for the user. Same symptom, different cause, below.
- **The screenshot is blank when another window covers the app.** WebKit
  stops painting a fully covered window, and an app launched from a script opens behind
  whatever is in front, typically a full-screen terminal. The driver sets
  `DOWNLOADER_WINDOW_ON_TOP=1`, which makes the app keep its window on top for the run.
  Diagnosed with `/tmp/midwest-mainline-run/windowid --list` (front to back). Front-end
  exceptions are reported to the backend log as `front end: ...` (see `gui/src/main.ts`).
- **A debug GUI binary shows a blank window unless Vite is running.** Tauri's debug profile
  loads `build.devUrl` (http://localhost:5173) from `gui/src-tauri/tauri.conf.json`, not the
  embedded bundle. The driver starts `pnpm dev` when nothing listens there and stops it
  afterwards. Vite binds `[::1]` only, so probe it as `localhost`, not `127.0.0.1`.
- **The window server calls the app `downloader-gui`**, the binary name, not the product
  name `downloader` in the title bar. `windowid.swift` looks it up by that owner name to
  feed `screencapture -l`, which captures just the window and needs no accessibility
  permission (Screen Recording permission for the terminal is enough).
- **Port 6881 is usually busy** because the user keeps a GUI open. Both binaries then log
  "Address already in use" warnings and carry on: the TCP listener falls back to the other
  address family or none, the DHT node takes a free UDP port. Harmless for a test run.
- **Never run the dev server from `gui/src-tauri`**: before resume files and downloads moved
  to the data directory, a run from there once committed a 1.5 GB video into the repo.

- **A trackerless magnet's resume file didn't load on the next start** ("skipping
  .../<hash>.resume: ... not a bencoded dict"): `juicy_bencode` rejected the empty list `le`
  that an empty `trackers` field encodes to. Fixed in `../juicy_bencode` (a separate repo,
  `many1` -> `many0`); if it recurs, check that repo's state.
- **A persisted DHT routing table can be poisoned.** Sybil nodes (many ids on one IP, next
  to popular hashes) answer get_peers with a token and nothing else; once they filled the
  table near the Arch hash every lookup came back empty, run after run. The node now keeps
  one node per IP and evicts empty answerers, and the startup purge logs "dropped N routing
  table nodes sharing an IP". If lookups still find 0 peers, compare against a fresh table
  (move `$RUN_DIR/data/dht.db*` aside).
- **Debug builds are slow at full speed.** Dependencies are optimised in dev
  (`[profile.dev.package."*"]`), but our own swarm code isn't: a debug download of a big
  swarm runs ~10 MB/s where release does 110+. Measure throughput in release.
- **The uTP crate parents its connection spans on the current span**, so anything that
  connects uTP inside one of our spans keeps that span open for the connection's life (it
  showed as 408 "dialling" with a 256-dial cap). `stream.rs` gives uTP connects a root span.
- **`krpc.py` adds itself to the node's routing table** as a `127.0.0.1` contact, one per
  run (the node learns from everyone who talks to it). Harmless in the scratch database,
  but don't point it at the user's own.
- **A fresh index node collects no real announces for a long while.** Peers announce to the
  nodes closest to a torrent's hash, and a new node with a random id is close to almost
  nothing; `stored_swarms` stays `[]` for minutes. Use `krpc announce` to exercise the index.
- **`json_rpc_server` reports errors inside `result`** (`{"result":{"code":-32602,...}}`),
  not in a JSON-RPC `error` member, so check the body, not just the HTTP status.

## Troubleshooting

- **`DHT bootstrapped, routing table has 0 nodes`** in `dht.log`: none of the routers
  answered. `json_rpc_server` once had `dht.tansmissionbt.com` (sic) and lacked
  `dht.libtorrent.org:25401`, the routers that actually answer; its list now matches
  `downloader/src/dht.rs`. Otherwise it's the network (VPN).
- **`no downloader window found` or `could not create image from window`** while the app
  ran fine (peer connections in the summary): the window opened on a different Space than
  the one showing, typically because the user is in a full-screen app. It came and went
  between runs while the user worked. `windowid` now exits 3 for this case and the driver
  says "on another Space"; `/tmp/midwest-mainline-run/windowid --list` shows what's on
  screen. Wait until the user is on a normal desktop, or ask them; the capture is retried
  once anyway.

- **The Traces pane drew only the first lanes** (pieces empty though the header counted
  them): ECharts' progressive rendering restarts with every `setOption`, and the pane redraws
  ~15 times a second. The series sets `progressive: 0`.
- **The GUI showed nothing and exited**: a second session on the same data dir (the user's
  GUI, or a CLI run) holds `<data_dir>/lock`. Now a dialog says so; it's drawn by
  `UserNotificationCenter`, not `downloader-gui`, so `windowid downloader-gui` won't find it.

- **`Vite never came up, see /tmp/midwest-mainline-run/vite.log`**: the port check used
  `127.0.0.1` while Vite listens on `[::1]`; fixed in the driver, but the same shape
  recurs with any tool that probes IPv4 only.
- **`no downloader window found`** after the wait, with nothing in the summary: the app is still starting (a cold debug
  binary plus Vite's first dependency optimisation can take 10 s), give it longer, or the
  owner name changed; `/tmp/midwest-mainline-run/windowid --list` prints every window.
- **`no metadata for "..." after 120s (tried 0 peers)`** on a magnet: no peer source
  answered; check the VPN (UDP trackers blocked) and that `DHT node up` appears in the log.
