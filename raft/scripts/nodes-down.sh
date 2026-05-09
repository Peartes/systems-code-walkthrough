#!/usr/bin/env bash
# Kill all raft-node processes recorded by `nodes-up.sh`.
#
# Reads PIDs from data/pids/node-*.pid. Sends SIGTERM (the node binary
# handles graceful shutdown via signal.Notify in cmd/node/main.go).

set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -d data/pids ]; then
  echo "  no pid files found — nothing to stop"
  exit 0
fi

shopt -s nullglob
any=0
for pidfile in data/pids/node-*.pid; do
  any=1
  pid=$(cat "$pidfile")
  name=$(basename "$pidfile" .pid)
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    echo "  stopped $name (pid=$pid)"
  else
    echo "  $name already stopped"
  fi
  rm "$pidfile"
done

if [ $any -eq 0 ]; then
  echo "  no pid files matched data/pids/node-*.pid"
fi
