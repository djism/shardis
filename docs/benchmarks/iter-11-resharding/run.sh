#!/usr/bin/env bash
# Iteration 11: live resharding under load.
#
# 3 primaries + 3 replicas, 1M keys preloaded, a cluster-aware loadgen
# running the whole time. After 5 s an empty 4th primary joins and the
# cluster is rebalanced onto it (4096 slots, ~250k keys move). We record
# throughput, errors, MOVED/ASK and latency every 100 ms.
# Same scenario on real Redis (redis-cli --cluster add-node / rebalance).
# Each also runs once with no rebalance at all (the "control"), so noise
# from the machine itself (GC, other apps, swapping) isn't blamed on
# resharding.
#
# Before rebalancing we wait for the new node to report cluster_state:ok.
# redis-cli --cluster add-node returns before that, and a rebalance started
# right away made Redis's new node answer CLUSTERDOWN for ~1.4 s once it
# owned slots (see SUMMARY.md).
#
#   bash docs/benchmarks/iter-11-resharding/run.sh [shardis|redis|both]   (~6 min)
set -euo pipefail
cd "$(dirname "$0")/../../.."
OUT=docs/benchmarks/iter-11-resharding
WHICH=${1:-both}
KEYS=${KEYS:-1000000}
DURATION=${DURATION:-45s}
make build >/dev/null
RUN=$(mktemp -d "${TMPDIR:-/tmp}/shardis-reshard.XXXXXX")
pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill -9 "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [ -n "${KEEP:-}" ]; then echo "logs kept in $RUN"; else rm -rf "$RUN"; fi
}
trap cleanup EXIT
now() { perl -MTime::HiRes=time -e 'printf "%.3f\n", time'; }
rcli() { perl -e 'alarm shift; exec @ARGV' 5 redis-cli "$@"; }
wait_ok() { # port
  for _ in $(seq 300); do [ "$(rcli -p "$1" cluster info 2>/dev/null | tr -d '\r' | grep cluster_state)" = "cluster_state:ok" ] && return; sleep 0.1; done
  echo "cluster on $1 never became ok" >&2; exit 1
}

# phase <name> <file>: run the loadgen, add node g at +5 s, rebalance, and
# record when the rebalance started and ended (relative to loadgen start).
phase() {
  local name=$1 base=$2 newport=$3 add=$4 rebal=$5
  if [ "${REBALANCE:-yes}" = no ]; then name=$name-control add=true rebal=true; fi
  bin/shardis-loadgen -cluster -addr 127.0.0.1:$base -keys "$KEYS" -preload -warmup 0 -duration 1ms -clients 64 >/dev/null
  local t0; t0=$(now)
  bin/shardis-loadgen -cluster -addr 127.0.0.1:$base -keys "$KEYS" -warmup 0 -duration "$DURATION" -clients 50 \
    -timeline 100ms -timeline-file "$OUT/$name-timeline.csv" >"$OUT/$name-loadgen.txt" &
  local lg=$!
  sleep 5
  eval "$add" >"$RUN/$name-add.log" 2>&1 || { echo "$name: add-node failed:"; cat "$RUN/$name-add.log"; exit 1; }
  [ "$add" = true ] || wait_ok "$newport"
  sleep 1
  local ts; ts=$(now)
  eval "$rebal" >"$RUN/$name-rebalance.log" 2>&1 || { echo "$name: rebalance failed:"; tail "$RUN/$name-rebalance.log"; exit 1; }
  local te; te=$(now)
  wait $lg
  [ "$rebal" = true ] || printf '%s,%.1f,%.1f\n' "$name" "$(echo "$ts - $t0" | bc)" "$(echo "$te - $t0" | bc)" >>"$OUT/phases.csv"
  echo "== $name: rebalance from $(echo "$ts - $t0" | bc)s to $(echo "$te - $t0" | bc)s"
  tail -3 "$RUN/$name-rebalance.log"
  cat "$OUT/$name-loadgen.txt"
}

shardis() {
  pids=()
  local D; D=$(mktemp -d "$RUN/run.XXXXXX")
  local topo=$D/topology.conf names=(a b c d e f) i
  : >"$topo"
  for i in 0 1 2; do echo "node ${names[$i]} 127.0.0.1:$((7100+i)) 127.0.0.1:$((7100+i)) primary $(echo "0-5460 5461-10922 10923-16383" | cut -d' ' -f$((i+1)))" >>"$topo"; done
  for i in 3 4 5; do echo "node ${names[$i]} 127.0.0.1:$((7100+i)) 127.0.0.1:$((7100+i)) replica ${names[$((i-3))]}" >>"$topo"; done
  for i in 0 1 2 3 4 5; do
    mkdir -p "$D/${names[$i]}"
    bin/shardis --port $((7100+i)) --dir "$D/${names[$i]}" --cluster-config "$topo" --cluster-node "${names[$i]}" \
      --cluster-node-timeout 5000 >"$D/${names[$i]}.log" 2>&1 &
    pids+=($!)
  done
  mkdir -p "$D/g"
  bin/shardis --port 7106 --dir "$D/g" --cluster-enabled yes --cluster-node-timeout 5000 >"$D/g.log" 2>&1 &
  pids+=($!)
  wait_ok 7100
  phase shardis 7100 7106 \
    "bin/shardis-admin add-node -addr 127.0.0.1:7100 -new 127.0.0.1:7106" \
    "bin/shardis-admin rebalance -addr 127.0.0.1:7100"
  [ "${REBALANCE:-yes}" = no ] || bin/shardis-admin nodes -addr 127.0.0.1:7100 | tee "$OUT/shardis-after.txt"
}

redis() {
  pids=()
  local i D; D=$(mktemp -d "$RUN/run.XXXXXX")
  for i in 0 1 2 3 4 5 6; do
    mkdir -p "$D/r$i"
    redis-server --port $((6400+i)) --cluster-enabled yes --cluster-config-file "$D/r$i/nodes.conf" --dir "$D/r$i" \
      --cluster-node-timeout 5000 --save "" --appendonly no --daemonize no >"$D/r$i.log" 2>&1 &
    pids+=($!)
  done
  sleep 0.5
  rcli --cluster create 127.0.0.1:6400 127.0.0.1:6401 127.0.0.1:6402 127.0.0.1:6403 127.0.0.1:6404 127.0.0.1:6405 \
    --cluster-replicas 1 --cluster-yes >"$D/redis-create.log" 2>&1 || true
  wait_ok 6400
  phase redis 6400 6406 \
    "redis-cli --cluster add-node 127.0.0.1:6406 127.0.0.1:6400 && sleep 2" \
    "redis-cli --cluster rebalance 127.0.0.1:6400 --cluster-use-empty-masters --cluster-yes"
  [ "${REBALANCE:-yes}" = no ] || rcli -p 6400 cluster nodes | awk '/master/ {print $2, NF-8 " slot ranges"}' | tee "$OUT/redis-after.txt"
}

# stop: kill this system's processes before starting the next cluster.
stop() { for p in "${pids[@]:-}"; do kill -9 "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; sleep 1; }
both_ways() { # system
  REBALANCE=yes "$1"; stop
  REBALANCE=no "$1"; stop
}
echo "system,rebalance_start_s,rebalance_end_s" >"$OUT/phases.csv"
case $WHICH in
  shardis) both_ways shardis ;;
  redis) both_ways redis ;;
  both) both_ways shardis; both_ways redis ;;
esac
