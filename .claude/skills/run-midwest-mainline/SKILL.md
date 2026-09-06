---
name: run-midwest-mainline
description: Build, run, test, and drive this workspace's BitTorrent client - the `downloader` CLI and the Tauri GUI. Use when asked to start or run the downloader, run its tests, build it, take a screenshot of the GUI, watch its logs, or check a download against a real swarm.
---

A Rust workspace: `downloader/` (the BitTorrent library plus a CLI), `gui/` (a Tauri 2 +
Svelte 5 desktop app over the same library), `dht/` (the DHT crate they use). Drive it with
`.claude/skills/run-midwest-mainline/driver.sh`, which runs either binary against a scratch
data directory and, for the GUI, streams its console to a file and screenshots the window.

All paths are relative to the workspace root. Verified on macOS 26 with the Apple toolchain
(the GUI needs macOS's WebKit; the CLI and the library are portable).

## Prerequisites

Rust stable with the 2024 edition, Node 26 and pnpm 11 (`brew install node pnpm`), and the
Xcode command line tools for `swiftc` and `screencapture` (macOS ships both). The
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
`gui/`. About four minutes cold, seconds warm.

## Run (agent path)

Both commands run against `/tmp/midwest-mainline-run/data` (override with `RUN_DIR`), so
the user's own resume files, settings, and DHT table in `~/Library/Application
Support/downloader` are never touched. A good test torrent is the Arch Linux ISO, which is
tracker-less and finds hundreds of peers over the DHT within 30 s:

```bash
ARCH='magnet:?xt=urn:btih:f45add9d1a5185d8588df7dd6cd89993dd0174fa&dn=archlinux-2026.09.01-x86_64.iso'
```

**CLI** - runs for N seconds, then prints a one-line summary of what happened:

```bash
.claude/skills/run-midwest-mainline/driver.sh cli "$ARCH" 40
# metadata: 1  dht up: 1  dht lookups: 1  peer connections: 50  pieces: 692  complete: 0  warn/error lines: 4
# 662M	/tmp/midwest-mainline-run/cli-download
```

The full log is `/tmp/midwest-mainline-run/cli.log` (timestamped; `RUST_LOG=debug` for
more). "pieces" is verified pieces; on this torrent expect several hundred in 40 s.

**GUI** - launches the debug app, optionally adding a source at startup (the app takes one
as its first argument), waits N seconds, screenshots the window, quits:

```bash
.claude/skills/run-midwest-mainline/driver.sh gui "$ARCH" 40
# screenshot: /tmp/midwest-mainline-run/gui.png (window 7290)
# metadata: 0  dht up: 1  dht lookups: 2  peer connections: 67  pieces: 720  ...
```

Look at `gui.png`: it should show the torrent row with a progress bar and rates, the details
panel (the first torrent is selected by itself), and the console pane with piece completions
scrolling. The GUI downloads into `$RUN_DIR/gui-download` (the driver writes a scratch
`settings.json` saying so; without it the app's default is `~/Downloads`). The GUI's console is also streamed to
`/tmp/midwest-mainline-run/gui.log`, which is how the summary is computed ("metadata" stays
0 there because only the CLI logs that line).

**Logs of a GUI the user is running**: start the GUI with
`DOWNLOADER_LOG_ADDR=127.0.0.1:9999` and run `driver.sh logs` to tail its console from a
terminal.

| command | what it does |
|---|---|
| `build` | cargo and pnpm builds |
| `test` | `cargo test --workspace` and `pnpm check` |
| `cli <source> [secs]` | run the CLI on a .torrent or magnet, summarize |
| `gui [source] [secs]` | launch the GUI, screenshot it, quit |
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

Delete the example afterwards; `examples/` isn't part of the repo.

## Run (human path)

```bash
cd gui && pnpm tauri dev      # Vite plus the debug app with hot reload; Ctrl-C stops both
cargo run -p downloader -- "$ARCH" ~/Downloads   # the CLI; Ctrl-C sends the trackers a farewell and exits
```

## Test

```bash
.claude/skills/run-midwest-mainline/driver.sh test
```

96 downloader tests and 34 dht tests (one more is ignored: it needs the live DHT), all in
about a second; `pnpm check` reports 0 errors.
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

## Troubleshooting

- **`Vite never came up, see /tmp/midwest-mainline-run/vite.log`**: the port check used
  `127.0.0.1` while Vite listens on `[::1]`; fixed in the driver, but the same shape
  recurs with any tool that probes IPv4 only.
- **`no downloader window found`** after the wait: the app is still starting (a cold debug
  binary plus Vite's first dependency optimisation can take 10 s), give it longer, or the
  owner name changed; `/tmp/midwest-mainline-run/windowid --list` prints every window.
- **`no metadata for "..." after 120s (tried 0 peers)`** on a magnet: no peer source
  answered; check the VPN (UDP trackers blocked) and that `DHT node up` appears in the log.
