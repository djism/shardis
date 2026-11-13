#!/usr/bin/env bash
# Iteration 1 baseline: PING throughput/latency, Shardis vs real Redis.
# Usage (from repo root): bash docs/benchmarks/iter-01-ping/run.sh
# Ports avoid 7000, which macOS's AirPlay Receiver often holds.
set -euo pipefail
cd "$(dirname "$0")/../../.."
OUT=docs/benchmarks/iter-01-ping/raw.csv
N=${N:-300000}

make build >/dev/null
./bin/shardis --port 7100 >/dev/null 2>&1 & SP=$!
redis-server --port 6399 --save "" --appendonly no >/dev/null 2>&1 & RP=$!
trap 'kill $SP $RP 2>/dev/null' EXIT
sleep 1
redis-cli -p 7100 ping >/dev/null && redis-cli -p 6399 ping >/dev/null

{
  echo "# machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(getconf _NPROCESSORS_ONLN) cores, $(uname -sr)"
  echo "# go: $(go version | cut -d' ' -f3)  redis-server: $(redis-server --version | cut -d' ' -f3)  date: $(date -u +%FT%TZ)  n=$N"
  echo "server,clients,pipeline,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms"
  for srv in shardis:7100 redis:6399; do
    name=${srv%%:*} port=${srv##*:}
    for cfg in "1 1" "50 1" "50 16"; do
      read -r clients pipe <<<"$cfg"
      redis-benchmark -h 127.0.0.1 -p "$port" -t ping -n "$N" -c "$clients" -P "$pipe" --csv 2>/dev/null \
        | tail -n +2 | tr -d '"' | sed "s/^/$name,$clients,$pipe,/"
    done
  done
} | tee "$OUT"
