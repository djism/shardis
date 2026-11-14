#!/usr/bin/env bash
# Iteration 7: one node vs a 3-primary cluster (Shardis and real Redis).
# Usage (from repo root): bash docs/benchmarks/iter-07-cluster/run.sh
#
# redis-benchmark --cluster discovers the nodes via CLUSTER NODES and spreads
# its clients over the primaries. Everything runs on one laptop, so client
# and servers compete for the same cores; see SUMMARY.md.
set -euo pipefail
cd "$(dirname "$0")/../../.."
DIR=docs/benchmarks/iter-07-cluster
OUT=$DIR/raw.csv
N=${N:-1000000}
SCRATCH=$(mktemp -d)
rcli() { perl -e 'alarm shift; exec @ARGV' 3 redis-cli "$@"; }
# Servers started inside the { ... } | tee pipeline below live in a subshell,
# so `jobs` can't see them: shut each one down by port instead.
cleanup() {
  for p in 7100 6399 6400 6401 6402 6403 6404 6405; do rcli -p $p shutdown nosave >/dev/null 2>&1 || true; done
  deploy/local/cluster.sh down >/dev/null 2>&1 || true
  rm -rf "$SCRATCH"
}
trap cleanup EXIT
make build >/dev/null

bench() { # label port extra-args...
  local label=$1 port=$2; shift 2
  for pipe in 1 16; do
    redis-benchmark -p "$port" "$@" -t set,get -n "$N" -r 1000000 -c 50 -P "$pipe" --csv 2>/dev/null \
      | grep -E '^"(SET|GET)"' | tr -d '"' | sed "s/^/$label,$pipe,/"
  done
}

{
  echo "# machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(uname -sr), $(go version | cut -d' ' -f3), redis $(redis-server --version | cut -d' ' -f3), $(date -u +%FT%TZ), n=$N"
  echo "setup,pipeline,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms"

  # 1. Shardis, one node
  ./bin/shardis --port 7100 >/dev/null 2>&1 &
  pid=$!; sleep 0.5
  bench "shardis single node" 7100
  kill $pid; wait $pid 2>/dev/null || true

  # 2. Shardis cluster: 3 primaries + 3 replicas (local processes, 7000-7005)
  rm -rf .cluster-local
  APPENDONLY=no deploy/local/cluster.sh up >/dev/null # same as the other setups: no persistence
  bench "shardis cluster (3 primaries)" 7000 --cluster
  deploy/local/cluster.sh down >/dev/null

  # 3. Redis, one node
  redis-server --port 6399 --save "" --appendonly no >/dev/null 2>&1 &
  pid=$!; sleep 0.5
  bench "redis single node" 6399
  kill $pid; wait $pid 2>/dev/null || true

  # 4. Redis cluster: 3 primaries + 3 replicas, created the standard way
  for p in 6400 6401 6402 6403 6404 6405; do
    mkdir -p "$SCRATCH/$p"
    redis-server --port $p --cluster-enabled yes --cluster-config-file "$SCRATCH/$p/nodes.conf" \
      --dir "$SCRATCH/$p" --save "" --appendonly no >/dev/null 2>&1 &
  done
  sleep 1
  redis-cli --cluster create 127.0.0.1:6400 127.0.0.1:6401 127.0.0.1:6402 127.0.0.1:6403 127.0.0.1:6404 \
    127.0.0.1:6405 --cluster-replicas 1 --cluster-yes >/dev/null 2>&1
  for _ in $(seq 50); do rcli -p 6400 cluster info | grep -q cluster_state:ok && break; sleep 0.2; done
  bench "redis cluster (3 primaries)" 6400 --cluster
} | tee "$OUT"
