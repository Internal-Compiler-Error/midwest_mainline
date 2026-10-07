---
name: run-midwest-mainline
description: Build, run, test, and drive this workspace's subsystems - the `downloader` CLI, the Tauri GUI, and the DHT node (`json_rpc_server`, standalone or as a long-term index). Use when asked to start or run the downloader, the GUI, or the DHT node, run their tests, build them, take a screenshot of the GUI, watch logs, query or announce to the DHT, or check a download against a real swarm.
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
tracker-less and finds a couple of hundred peers over the DHT, though the lookup alone takes
20-30 s:

```bash
ARCH='magnet:?xt=urn:btih:f45add9d1a5185d8588df7dd6cd89993dd0174fa&dn=archlinux-2026.09.01-x86_64.iso'
```

**CLI** - runs for N seconds, then prints a one-line summary of what happened:

```bash
.claude/skills/run-midwest-mainline/driver.sh cli "$ARCH" 40
# metadata: 1  dht up: 1  dht lookups: 1  peer connections: 203  pieces: 112  complete: 0  warn/error lines: 3
#  56M	/tmp/midwest-mainline-run/cli-download
```

The full log is `/tmp/midwest-mainline-run/cli.log` (timestamped; `RUST_LOG=debug` for
more). "pieces" is verified pieces; most of the 40 s goes on the DHT lookup and metadata,
so anything from about a hundred up is normal.

**GUI** - launches the debug app, optionally adding a source at startup (the app takes one
as its first argument), waits N seconds (default 45), screenshots the window, quits:

```bash
.claude/skills/run-midwest-mainline/driver.sh gui "$ARCH" 45
# screenshot: /tmp/midwest-mainline-run/gui.png (window 552)
# metadata: 0  dht up: 1  dht lookups: 2  peer connections: 225  pieces: 224  ...
```

Look at `gui.png`: it should show the torrent row with a progress bar and rates, the details
panel (the first torrent is selected by itself), and the console pane with piece completions
scrolling. The GUI downloads into `$RUN_DIR/gui-download` (the driver writes a scratch
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
| `rpc <method> [params]` | JSON-RPC: `node_count`, `stored_swarms`, `stored_peers` |
| `krpc <cmd> [args]` | KRPC over UDP: `ping`, `get_peers <hash>`, `announce <hash> <port>` |
| `dht-down` | stop the DHT node |
| `logs` | tail a running GUI's console over TCP |
| `clean` | delete the scratch directory |

## Direct invocation

Everything the GUI does goes through `downloader::Session`; a throwaway example is the
quickest way to poke a library change without either binary:

```bash
mkdir -p downloader/examples && cat > downloader/examples/probe.rs <<'RS'
use downloader::{Session, SessionConfig, Settings, TorrentState, data_dir};
fn main() {
    let magnet = std::env::args().nth(1).unwrap();
    let mut session = Session::new(SessionConfig {
        peer_id: *b"-DL0100-probe-probe.",
        data_dir: data_dir(),
        settings: Settings { listen_port: 0, ..Settings::default() },
    })
    .unwrap();
    let id = session.add(magnet, std::env::temp_dir().join("probe"));
    for _ in 0..60 {
        std::thread::sleep(std::time::Duration::from_secs(1));
        if let Some((_, TorrentState::Downloading(p))) = session.torrents().into_iter().find(|(i, _)| *i == id) {
            println!("{} peers, {:.0} B/s, {}/{} pieces", p.peers.len(), p.download_bps, p.verified_pieces, p.total_pieces);
        }
    }
    session.remove(id, true);
}
RS
DOWNLOADER_DATA_DIR=/tmp/midwest-mainline-run/data cargo run -q -p downloader --example probe -- "$ARCH"
rm -r downloader/examples
```

Delete the example afterwards; `examples/` isn't part of the repo. It prints a line a
second; expect ~200 peers and 10+ MB/s by the end of the minute.

The DHT crate's internals (store, retention, routing table, KRPC parsing) are covered by its
unit tests on in-memory SQLite, which is the fastest loop for a change there:

```bash
cargo test -p midwest_mainline
```

## Run (human path)

```bash
cd gui && pnpm tauri dev      # Vite plus the debug app with hot reload; Ctrl-C stops both
cargo run -p downloader -- "$ARCH" ~/Downloads   # the CLI; Ctrl-C sends the trackers a farewell and exits
```

## Test

```bash
.claude/skills/run-midwest-mainline/driver.sh test
```

119 downloader tests and 43 dht tests (one more is ignored: it needs the live DHT), all in
about two seconds; `pnpm check` reports 0 errors.
Tests use free ports and no DHT, so they run offline.

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

- **A trackerless magnet's resume file doesn't load on the next start** ("skipping
  .../<hash>.resume: ... not a bencoded dict"): `juicy_bencode` rejects the empty list `le`
  that an empty `trackers` field encodes to. The run carries on as a fresh add, so the
  driver still works, but resumed progress is lost. A bug, not the environment.
- **The DHT lookup is the slow part of a magnet run.** The node is up in ~5 s, but the
  lookup that finds peers finishes 20-30 s in. A GUI run of 30 s once ended with 0 peers
  because the node came up late; hence the 45 s default.
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

- **`Vite never came up, see /tmp/midwest-mainline-run/vite.log`**: the port check used
  `127.0.0.1` while Vite listens on `[::1]`; fixed in the driver, but the same shape
  recurs with any tool that probes IPv4 only.
- **`no downloader window found`** after the wait, with nothing in the summary: the app is still starting (a cold debug
  binary plus Vite's first dependency optimisation can take 10 s), give it longer, or the
  owner name changed; `/tmp/midwest-mainline-run/windowid --list` prints every window.
- **`no metadata for "..." after 120s (tried 0 peers)`** on a magnet: no peer source
  answered; check the VPN (UDP trackers blocked) and that `DHT node up` appears in the log.
