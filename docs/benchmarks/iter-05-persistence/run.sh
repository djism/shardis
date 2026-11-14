#!/usr/bin/env bash
# Iteration 5: durability vs throughput for each AOF fsync policy.
# Usage (from repo root): bash docs/benchmarks/iter-05-persistence/run.sh
#
# 1. crash.csv: shardis-crashtest, 10 runs per policy x crash kind
#    (process = kill -9; power = kill -9 + drop everything not fsynced).
# 2. throughput.csv: redis-benchmark SET, 50 clients, without and with
#    pipelining, for Shardis and Redis at each policy. "always" gets fewer
#    requests because it's ~50x slower.
set -euo pipefail
cd "$(dirname "$0")/../../.."
DIR=docs/benchmarks/iter-05-persistence
RUNS=${RUNS:-10}
make build >/dev/null
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
header="# machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(uname -sr), $(go version | cut -d' ' -f3), redis $(redis-server --version | cut -d' ' -f3), $(date -u +%FT%TZ)"

{ echo "$header"; ./bin/shardis-crashtest -runs "$RUNS"; } | tee "$DIR/crash.csv"

{
  echo "$header"
  echo "server,policy,clients,pipeline,requests,rps,p50_ms,p99_ms"
  for policy in off no everysec always; do
    case $policy in always) n=20000 ;; *) n=300000 ;; esac
    for server in shardis redis; do
      tmp=$(mktemp -d)
      if [ "$server" = shardis ]; then
        if [ "$policy" = off ]; then a=(--appendonly no); else a=(--appendonly yes --appendfsync "$policy"); fi
        ./bin/shardis --port 7108 --dir "$tmp" "${a[@]}" >/dev/null 2>&1 &
        port=7108
      else
        if [ "$policy" = off ]; then a=(--appendonly no); else a=(--appendonly yes --appendfsync "$policy"); fi
        redis-server --port 6399 --dir "$tmp" --save "" "${a[@]}" >/dev/null 2>&1 &
        port=6399
      fi
      pid=$!; sleep 0.7
      for pipe in 1 16; do
        redis-benchmark -p "$port" -t set -n "$n" -r 1000000 -c 50 -P "$pipe" --csv 2>/dev/null | tail -1 | tr -d '"' \
          | awk -F, -v s="$server" -v p="$policy" -v pp="$pipe" -v n="$n" '{print s","p",50,"pp","n","$2","$6","$8}'
      done
      kill "$pid"; wait "$pid" 2>/dev/null || true
      rm -rf "$tmp"
    done
  done
} | tee "$DIR/throughput.csv"
