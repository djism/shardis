#!/usr/bin/env bash
# Run the 6-node cluster as local processes (no Docker).
# Usage: deploy/local/cluster.sh up|down|status
# Logs and data go to .cluster-local/ in the repo root.
# APPENDONLY=no turns persistence off (the benchmarks compare against Redis without it).
set -euo pipefail
cd "$(dirname "$0")/../.."
RUN=.cluster-local
TOPO=deploy/local/topology.conf
nodes=(a b c d e f)

# redis-cli with a timeout. On macOS, AirPlay Receiver listens on *:7000 and
# accepts connections without ever replying: a plain redis-cli that reaches it
# (e.g. before our node has bound 127.0.0.1:7000) hangs forever.
rcli() { perl -e 'alarm shift; exec @ARGV' 2 redis-cli "$@"; }

start_node() { # index
  local i=$1 n=${nodes[$1]} port=$((7000 + $1))
  if [ -f "$RUN/$n.pid" ] && kill -0 "$(cat "$RUN/$n.pid")" 2>/dev/null; then
    echo "node $n already running"; return
  fi
  mkdir -p "$RUN/$n"
  ./bin/shardis --port "$port" --cluster-config "$TOPO" --cluster-node "$n" \
    --appendonly "${APPENDONLY:-yes}" --dir "$RUN/$n" >>"$RUN/$n.log" 2>&1 &
  echo $! >"$RUN/$n.pid"
}

wait_node() { # index
  for _ in $(seq 50); do [ "$(rcli -p $((7000 + $1)) ping 2>/dev/null)" = PONG ] && return; sleep 0.1; done
}

up() {
  make build >/dev/null
  mkdir -p "$RUN"
  # Primaries first, replicas once the primaries answer. Started together, a
  # replica can dial 127.0.0.1:7000 before node a has bound it and reach
  # macOS's AirPlay Receiver instead (it listens on *:7000 and never replies).
  for i in 0 1 2; do start_node "$i"; done
  for i in 0 1 2; do wait_node "$i"; done
  for i in 3 4 5; do start_node "$i"; done
  for i in 3 4 5; do wait_node "$i"; done
  status
}

down() {
  for n in "${nodes[@]}"; do
    if [ -f "$RUN/$n.pid" ]; then kill "$(cat "$RUN/$n.pid")" 2>/dev/null || true; rm -f "$RUN/$n.pid"; fi
  done
  echo "cluster stopped (data kept in $RUN/; rm -rf it for a clean start)"
}

status() {
  for i in "${!nodes[@]}"; do
    port=$((7000 + i))
    role=$(rcli -p "$port" role 2>/dev/null | head -1 || true)
    printf "node %s  127.0.0.1:%d  %s\n" "${nodes[$i]}" "$port" "${role:-down}"
  done
}

case "${1:-}" in
  up) up ;; down) down ;; status) status ;;
  *) echo "usage: $0 up|down|status" >&2; exit 2 ;;
esac
