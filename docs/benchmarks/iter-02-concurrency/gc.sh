#!/usr/bin/env bash
# Iteration 2: memory per key and GC pauses with ~1M keys loaded.
# Usage (from repo root): bash docs/benchmarks/iter-02-concurrency/gc.sh
#
# For each server: load keys (16-byte values, keys like key:000000123456),
# record RSS, then run a GET benchmark with the data in memory. For Shardis we
# also capture GODEBUG=gctrace=1, whose lines look like
#   gc 7 @2.1s 3%: 0.021+1.2+0.034 ms clock, ...
# The 1st and 3rd numbers are the stop-the-world pauses (all goroutines
# frozen); the middle one runs concurrently with the program.
set -euo pipefail
cd "$(dirname "$0")/../../.."
DIR=docs/benchmarks/iter-02-concurrency
SCRATCH=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$SCRATCH"' EXIT
make build >/dev/null

rss_mb() { ps -o rss= -p "$1" | awk '{printf "%.1f", $1/1024}'; }

load_and_measure() { # name port pid
  local name=$1 port=$2 pid=$3
  local rss0; rss0=$(rss_mb "$pid")
  redis-benchmark -p "$port" -t set -n 3000000 -r 1000000 -d 16 -P 32 -c 50 -q >/dev/null 2>&1
  local keys; keys=$(redis-cli -p "$port" dbsize)
  local rss1; rss1=$(rss_mb "$pid")
  local per; per=$(awk -v a="$rss0" -v b="$rss1" -v k="$keys" 'BEGIN{printf "%.0f", (b-a)*1024*1024/k}')
  local lat; lat=$(redis-benchmark -p "$port" -t get -n 600000 -r 1000000 -c 50 --csv 2>/dev/null | tail -1 | tr -d '"')
  echo "$name,$keys,$rss0,$rss1,$per,$lat"
}

{
  echo "# machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(uname -sr), $(go version | cut -d' ' -f3), redis $(redis-server --version | cut -d' ' -f3), $(date -u +%FT%TZ)"
  echo "server,keys,rss_before_mb,rss_after_mb,rss_bytes_per_key,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms"
  for engine in executor sharded; do
    GODEBUG=gctrace=1 ./bin/shardis --port 7103 --engine "$engine" 2>"$SCRATCH/gctrace-$engine.txt" >/dev/null &
    pid=$!; sleep 0.7
    load_and_measure "shardis-$engine" 7103 "$pid"
    kill "$pid"; wait "$pid" 2>/dev/null || true
  done
  redis-server --port 6399 --save "" --appendonly no >/dev/null 2>&1 &
  pid=$!; sleep 0.7
  load_and_measure redis 6399 "$pid"
  echo "# redis INFO used_memory: $(redis-cli -p 6399 info memory | grep '^used_memory:' | tr -d '\r')"
  kill "$pid"; wait "$pid" 2>/dev/null || true
} | tee "$DIR/memory.csv"

echo
echo "GC pauses (stop-the-world, ms), per engine:" | tee "$DIR/gc-pauses.txt"
for engine in executor sharded; do
  grep '^gc ' "$SCRATCH/gctrace-$engine.txt" | awk -v e="$engine" '
    { split($5, p, "+"); stw = p[1] + p[3]; n++; sum += stw; if (stw > max) max = stw }
    END { printf "%s: %d GCs, avg STW %.3f ms, max STW %.3f ms\n", e, n, sum/n, max }' | tee -a "$DIR/gc-pauses.txt"
  cp "$SCRATCH/gctrace-$engine.txt" "$DIR/gctrace-$engine.txt"
done
