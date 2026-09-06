#!/bin/zsh
# Builds, launches, and drives this workspace's two binaries: the `downloader` CLI and the
# Tauri GUI (`downloader-gui`). Everything runs against a scratch data directory so the
# user's own downloads, resume files, and DHT table are never touched.
#
#   driver.sh build                     cargo + pnpm builds
#   driver.sh test                      cargo tests for every crate, svelte-check for the GUI
#   driver.sh cli <source> [seconds]    run the CLI on a .torrent/magnet for N seconds (default 60),
#                                       then print a summary; log at $RUN_DIR/cli.log
#   driver.sh gui [source] [seconds]    launch the GUI (optionally adding a source at start),
#                                       stream its console to $RUN_DIR/gui.log, screenshot the
#                                       window to $RUN_DIR/gui.png after N seconds (default 20), quit
#   driver.sh logs                      tail the console of a GUI started by `gui` (or any GUI
#                                       started with DOWNLOADER_LOG_ADDR=127.0.0.1:9999)
#   driver.sh clean                     delete the scratch directory
set -euo pipefail
ROOT=${0:A:h:h:h:h}            # the workspace root, four levels up from this file
RUN_DIR=${RUN_DIR:-/tmp/midwest-mainline-run}
DATA_DIR=$RUN_DIR/data
LOG_PORT=9999
mkdir -p "$RUN_DIR" "$DATA_DIR"

summarize() {
  # counts the lines that say what happened; the log has ANSI colour and no timestamps in
  # the pretty format, so strip and grep
  local log=$1
  sed 's/\x1b\[[0-9;]*m//g' "$log" | awk '
    /got metadata/ {meta++}
    /DHT node up/ {dht++}
    /DHT lookup found/ {lookups++}
    /connected, [0-9]+ peers now/ {conn++}
    /is completed/ {pieces++}
    /download complete/ {done++}
    /WARN|ERROR/ {warns++}
    END {
      printf "metadata: %d  dht up: %d  dht lookups: %d  peer connections: %d  pieces: %d  complete: %d  warn/error lines: %d\n",
        meta, dht, lookups, conn, pieces, done, warns
    }'
}

case ${1:-} in
  build)
    (cd "$ROOT" && cargo build -p downloader -p downloader-gui)
    (cd "$ROOT/gui" && pnpm install --frozen-lockfile && pnpm build)
    ;;
  test)
    (cd "$ROOT" && cargo test --workspace)
    (cd "$ROOT/gui" && pnpm check)
    ;;
  cli)
    source=${2:?usage: driver.sh cli <torrent-or-magnet> [seconds]}
    secs=${3:-60}
    rm -rf "$RUN_DIR/cli-download"
    echo "running the CLI for ${secs}s, log at $RUN_DIR/cli.log"
    DOWNLOADER_DATA_DIR=$DATA_DIR RUST_LOG=${RUST_LOG:-info} RUST_BACKTRACE=0 \
      timeout -s INT "$secs" "$ROOT/target/debug/downloader" "$source" "$RUN_DIR/cli-download" \
      > "$RUN_DIR/cli.log" 2>&1 || true
    summarize "$RUN_DIR/cli.log"
    du -sh "$RUN_DIR/cli-download" 2>/dev/null || true
    ;;
  gui)
    source=${2:-}
    secs=${3:-20}
    swift_bin=$RUN_DIR/windowid
    [[ -x $swift_bin ]] || swiftc -O -o "$swift_bin" "${0:A:h}/windowid.swift"
    # the GUI keeps its log in its console panel; DOWNLOADER_LOG_ADDR copies every line to
    # this listener as well
    pkill -f "nc -l 127.0.0.1 $LOG_PORT" 2>/dev/null || true
    (nc -l 127.0.0.1 $LOG_PORT > "$RUN_DIR/gui.log" 2>&1 &)
    sleep 0.3
    # a debug Tauri build loads the frontend from Vite's dev server (build.devUrl in
    # gui/src-tauri/tauri.conf.json), not from the embedded bundle, so Vite must be up first
    if ! nc -z localhost 5173 2>/dev/null; then
      (cd "$ROOT/gui" && pnpm dev > "$RUN_DIR/vite.log" 2>&1 &)
      started_vite=1
      timeout 30 zsh -c 'until nc -z localhost 5173 2>/dev/null; do sleep 0.2; done' || { echo "Vite never came up, see $RUN_DIR/vite.log" >&2; exit 1; }
    fi
    echo "launching the GUI for ${secs}s, console at $RUN_DIR/gui.log"
    # the GUI downloads into its settings' download_dir, which defaults to ~/Downloads: point
    # it at the scratch dir so a test run never writes into the user's own folders
    mkdir -p "$DATA_DIR" "$RUN_DIR/gui-download"
    [[ -f $DATA_DIR/settings.json ]] || printf '{"download_dir": "%s"}\n' "$RUN_DIR/gui-download" > "$DATA_DIR/settings.json"
    # DOWNLOADER_WINDOW_ON_TOP: the window opens behind whatever is in front (a full-screen
    # terminal, say) and WebKit stops painting a covered window, which screenshots as blank
    DOWNLOADER_DATA_DIR=$DATA_DIR DOWNLOADER_LOG_ADDR=127.0.0.1:$LOG_PORT DOWNLOADER_WINDOW_ON_TOP=1 \
      RUST_LOG=${RUST_LOG:-info} RUST_BACKTRACE=0 \
      "$ROOT/target/debug/downloader-gui" ${source:+"$source"} > "$RUN_DIR/gui.stderr" 2>&1 &
    gui_pid=$!
    sleep "$secs"
    if id=$("$swift_bin" downloader-gui); then
      screencapture -x -l"$id" "$RUN_DIR/gui.png" && echo "screenshot: $RUN_DIR/gui.png (window $id)"
    else
      echo "no downloader window found; is the app still starting?" >&2
    fi
    kill -INT "$gui_pid" 2>/dev/null || true
    sleep 1
    kill "$gui_pid" 2>/dev/null || true
    pkill -f "nc -l 127.0.0.1 $LOG_PORT" 2>/dev/null || true
    [[ ${started_vite:-0} == 1 ]] && pkill -f 'vite' 2>/dev/null || true
    summarize "$RUN_DIR/gui.log"
    ;;
  logs)
    echo "listening on 127.0.0.1:$LOG_PORT; start the GUI with DOWNLOADER_LOG_ADDR=127.0.0.1:$LOG_PORT"
    exec nc -l 127.0.0.1 $LOG_PORT
    ;;
  clean)
    rm -rf "$RUN_DIR"
    ;;
  *)
    sed -n '2,15p' "$0"
    exit 2
    ;;
esac
