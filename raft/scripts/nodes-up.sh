#!/usr/bin/env bash
# Spawn one raft-node process per peer in config.json.
#
# Each process logs to data/node-<id>.log and persists its PID at
# data/pids/node-<id>.pid so `nodes-down.sh` can stop it cleanly.
# Re-running is idempotent: an already-running node is reported and
# skipped.

set -euo pipefail
cd "$(dirname "$0")/.."

BIN="bin/raft-node"
CONFIG="config.json"

if [ ! -x "$BIN" ]; then
  echo "  $BIN not found — run 'make build' first." >&2
  exit 1
fi

if [ ! -f "$CONFIG" ]; then
  echo "  config.json missing. Open the UI (vite dev server) and use the Seed page first." >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "  python3 is required to parse config.json" >&2
  exit 1
fi

ids=$(python3 -c '
import json, sys
try:
    cfg = json.load(open("config.json"))
except Exception as e:
    print(f"failed to parse config.json: {e}", file=sys.stderr)
    sys.exit(1)
peers = cfg.get("peers") or []
print(" ".join(str(p["id"]) for p in peers))
')

if [ -z "$ids" ]; then
  echo "  config.json has no peers. Seed via the UI first." >&2
  exit 1
fi

mkdir -p data data/pids

for id in $ids; do
  pidfile="data/pids/node-$id.pid"
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    echo "  node $id already running (pid=$(cat "$pidfile"))"
    continue
  fi
  # Use nohup so the process detaches cleanly when this script exits.
  nohup "./$BIN" --id="$id" --config="$CONFIG" --data-dir=data \
    > "data/node-$id.log" 2>&1 &
  echo $! > "$pidfile"
  echo "  started node $id (pid=$!)"
done
