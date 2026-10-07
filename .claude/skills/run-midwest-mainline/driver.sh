#!/bin/zsh
# Builds, launches, and drives this workspace's binaries: the `downloader` CLI, the Tauri GUI
# (`downloader-gui`), and the standalone DHT node (`json_rpc_server`). Everything runs against
# a scratch directory so the user's own downloads, resume files, and DHT table are never touched.
#
#   driver.sh build                     cargo + pnpm builds
#   driver.sh test                      cargo tests for every crate, svelte-check for the GUI
#   driver.sh cli <source> [seconds]    run the CLI on a .torrent/magnet for N seconds (default 60),
#                                       then print a summary; log at $RUN_DIR/cli.log
#   driver.sh gui [source] [seconds]    launch the GUI (optionally adding a source at start),
#                                       stream its console to $RUN_DIR/gui.log, screenshot the
#                                       window to $RUN_DIR/gui.png after N seconds (default 45), quit
#   driver.sh dht-up [expire|forever]   start the standalone DHT node in the background (default
#                                       forever: a long-term index), wait for its JSON-RPC; log at
#                                       $RUN_DIR/dht.log, database $RUN_DIR/dht.db
#   driver.sh rpc <method> [params]     call the node's JSON-RPC (node_count, stored_swarms,
#                                       stored_peers '{"info_hash":"<hex>"}')
#   driver.sh krpc <command> [args]     talk KRPC to the node over UDP (ping, get_peers <hash>,
#                                       announce <hash> <port>), see krpc.py
#   driver.sh dht-down                  stop the node
#   driver.sh logs                      tail the console of a GUI started by `gui` (or any GUI
#                                       started with DOWNLOADER_LOG_ADDR=127.0.0.1:9999)
#   driver.sh clean                     delete the scratch directory
set -euo pipefail
ROOT=${0:A:h:h:h:h}            # the workspace root, four levels up from this file
RUN_DIR=${RUN_DIR:-/tmp/midwest-mainline-run}
DATA_DIR=$RUN_DIR/data
LOG_PORT=9999
DHT_PORT=${DHT_PORT:-44444}
RPC_PORT=${RPC_PORT:-3000}
mkdir -p "$RUN_DIR" "$DATA_DIR"
# the user's own GUI holds 6881/6882 (the defaults): runs listen elsewhere unless settings.json
# already names a port, and download into the scratch dir rather than ~/Downloads
LISTEN_PORT=${LISTEN_PORT:-51001}
python3 - "$DATA_DIR/settings.json" "$LISTEN_PORT" "$RUN_DIR/gui-download" <<'PY'
import json, sys, os
path, port, downloads = sys.argv[1], int(sys.argv[2]), sys.argv[3]
settings = json.load(open(path)) if os.path.exists(path) else {}
settings.setdefault("listen_port", port)
settings.setdefault("download_dir", downloads)
json.dump(settings, open(path, "w"))
PY

summarize() {
  # counts the lines that say what happened; the log has ANSI colour and no timestamps in
  # the pretty format, so strip and grep
  local log=$1
  sed 's/\x1b\[[0-9;]*m//g' "$log" | awk '
    /got metadata/ {meta++}
    /DHT nodes? up/ {dht++}
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
    # the resume files go too: a resume whose data is gone fails on purpose (an unmounted drive)
    rm -rf "$RUN_DIR/cli-download" "$RUN_DIR/data/resume"
    # always the current code: a stale binary against a database the newer GUI has migrated
    # fails in confusing ways
    (cd "$ROOT" && cargo build -q -p downloader)
    echo "running the CLI for ${secs}s, log at $RUN_DIR/cli.log"
    DOWNLOADER_DATA_DIR=$DATA_DIR RUST_LOG=${RUST_LOG:-info} RUST_BACKTRACE=0 \
      timeout -s INT "$secs" "$ROOT/target/debug/downloader" "$source" "$RUN_DIR/cli-download" \
      > "$RUN_DIR/cli.log" 2>&1 || true
    summarize "$RUN_DIR/cli.log"
    du -sh "$RUN_DIR/cli-download" 2>/dev/null || true
    ;;
  gui)
    source=${2:-}
    secs=${3:-45}
    swift_bin=$RUN_DIR/windowid
    [[ -x $swift_bin && $swift_bin -nt ${0:A:h}/windowid.swift ]] || swiftc -O -o "$swift_bin" "${0:A:h}/windowid.swift"
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
    (cd "$ROOT" && cargo build -q -p downloader-gui)
    echo "launching the GUI for ${secs}s, console at $RUN_DIR/gui.log"
    # the GUI downloads into its settings' download_dir, which defaults to ~/Downloads: point
    # it at the scratch dir so a test run never writes into the user's own folders
    mkdir -p "$DATA_DIR" "$RUN_DIR/gui-download"
    # DOWNLOADER_WINDOW_ON_TOP: the window opens behind whatever is in front (a full-screen
    # terminal, say) and WebKit stops painting a covered window, which screenshots as blank
    DOWNLOADER_DATA_DIR=$DATA_DIR DOWNLOADER_LOG_ADDR=127.0.0.1:$LOG_PORT DOWNLOADER_WINDOW_ON_TOP=1 \
      DOWNLOADER_PANE=${PANE:-insights} \
      RUST_LOG=${RUST_LOG:-info} RUST_BACKTRACE=0 \
      "$ROOT/target/debug/downloader-gui" ${source:+"$source"} > "$RUN_DIR/gui.stderr" 2>&1 &
    gui_pid=$!
    sleep "$secs"
    if id=$("$swift_bin" downloader-gui); then
      # the capture occasionally fails with "could not create image from window" on the first
      # try; a second one a moment later works
      { screencapture -x -l"$id" "$RUN_DIR/gui.png" || { sleep 1; screencapture -x -l"$id" "$RUN_DIR/gui.png"; }; } &&
        echo "screenshot: $RUN_DIR/gui.png (window $id)"
    elif (( $? == 3 )); then
      echo "the window is open but on another Space (a full-screen app is in front?); no screenshot" >&2
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
  dht-up)
    retention=${2:-forever}
    if [[ -f $RUN_DIR/dht.pid ]] && kill -0 "$(<$RUN_DIR/dht.pid)" 2>/dev/null; then
      echo "already running (pid $(<$RUN_DIR/dht.pid))"; exit 0
    fi
    (cd "$ROOT" && cargo build -p json_rpc_server > "$RUN_DIR/dht-build.log" 2>&1) ||
      { tail -20 "$RUN_DIR/dht-build.log" >&2; exit 1; }
    DATABASE_URL=$RUN_DIR/dht.db DHT_RETENTION=$retention DHT_PORT=$DHT_PORT RPC_ADDR=127.0.0.1:$RPC_PORT \
      "$ROOT/target/debug/json_rpc_server" > "$RUN_DIR/dht.log" 2>&1 &
    echo $! > "$RUN_DIR/dht.pid"
    # the RPC listener only opens once bootstrapping is over, ~10 s
    if ! timeout 60 zsh -c "until curl -sf -o /dev/null -X POST -H 'content-type: application/json' -d '{\"jsonrpc\":\"2.0\",\"method\":\"node_count\",\"id\":1}' http://127.0.0.1:$RPC_PORT/json_rpc; do sleep 0.5; done"; then
      echo "the node never answered, see $RUN_DIR/dht.log" >&2; exit 1
    fi
    echo "DHT node up (pid $(<$RUN_DIR/dht.pid), retention $retention): UDP $DHT_PORT, JSON-RPC http://127.0.0.1:$RPC_PORT/json_rpc"
    ;;
  rpc)
    method=${2:?usage: driver.sh rpc <method> [params-json]}
    params=${3:-null}
    curl -s -X POST -H 'content-type: application/json' \
      -d "{\"jsonrpc\":\"2.0\",\"method\":\"$method\",\"params\":$params,\"id\":1}" \
      "http://127.0.0.1:$RPC_PORT/json_rpc"
    echo
    ;;
  krpc)
    shift
    exec python3 "${0:A:h}/krpc.py" "127.0.0.1:$DHT_PORT" "$@"
    ;;
  dht-down)
    [[ -f $RUN_DIR/dht.pid ]] && kill "$(<$RUN_DIR/dht.pid)" 2>/dev/null && echo stopped || echo "not running"
    rm -f "$RUN_DIR/dht.pid"
    ;;
  logs)
    echo "listening on 127.0.0.1:$LOG_PORT; start the GUI with DOWNLOADER_LOG_ADDR=127.0.0.1:$LOG_PORT"
    exec nc -l 127.0.0.1 $LOG_PORT
    ;;
  clean)
    rm -rf "$RUN_DIR"
    ;;
  *)
    sed -n '2,25p' "$0"
    exit 2
    ;;
esac
