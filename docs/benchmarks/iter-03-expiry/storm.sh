#!/usr/bin/env bash
# Iteration 3: an "expiry storm". About 1M keys expire at roughly the same
# moment and nobody ever reads them, so only active expiry can remove them.
# Usage (from repo root): bash docs/benchmarks/iter-03-expiry/storm.sh
#
# For Shardis (sharded engine) and real Redis, both at hz=10:
#   1. load 100k "live:*" keys with no TTL (used for background GET traffic)
#   2. load ~1M "tmp:*" keys with PX 5000 (16-byte values)
#   3. for 20s after loading, record every 200ms: DBSIZE and RSS, and run
#      back-to-back ~1s GET benchmarks on live:* to get a latency timeline
# Shardis also runs with GODEBUG=gctrace=1 to record the live Go heap after each GC.
set -euo pipefail
cd "$(dirname "$0")/../../.."
DIR=docs/benchmarks/iter-03-expiry
SCRATCH=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$SCRATCH"' EXIT
make build >/dev/null
VAL=0123456789abcdef

now_s() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }
rss_mb() { ps -o rss= -p "$1" | awk '{printf "%.1f", $1/1024}'; }

storm() { # name port pid
  local name=$1 port=$2 pid=$3
  redis-benchmark -p "$port" -n 300000 -r 100000 -P 32 -q SET 'live:__rand_int__' "$VAL" >/dev/null 2>&1
  local t_load=$(now_s)
  redis-benchmark -p "$port" -n 1000000 -r 100000000 -P 32 -c 50 -q SET 'tmp:__rand_int__' "$VAL" PX 5000 >/dev/null 2>&1
  local t0=$(now_s)
  echo "# $name: loading tmp keys took $(awk -v a="$t_load" -v b="$t0" 'BEGIN{printf "%.2f", b-a}')s; t=0 is the end of loading, so keys expire between t=$(awk -v a="$t_load" -v b="$t0" 'BEGIN{printf "%.2f", 5-(b-a)}')s and t=5s" >>"$SCRATCH/notes.txt"

  ( end=$(awk -v t="$t0" 'BEGIN{printf "%.3f", t+20}')
    while awk -v n="$(now_s)" -v e="$end" 'BEGIN{exit !(n<e)}'; do
      echo "$name,$(awk -v n="$(now_s)" -v t="$t0" 'BEGIN{printf "%.2f", n-t}'),$(redis-cli -p "$port" dbsize),$(rss_mb "$pid")"
      sleep 0.2
    done ) >>"$SCRATCH/timeline.csv" &
  local sampler=$!

  local end; end=$(awk -v t="$t0" 'BEGIN{printf "%.3f", t+20}')
  while awk -v n="$(now_s)" -v e="$end" 'BEGIN{exit !(n<e)}'; do
    local ts; ts=$(awk -v n="$(now_s)" -v t="$t0" 'BEGIN{printf "%.2f", n-t}')
    redis-benchmark -p "$port" -n 100000 -r 100000 -c 50 --csv GET 'live:__rand_int__' 2>/dev/null \
      | tail -1 | tr -d '"' | sed "s/^/$name,$ts,/" >>"$SCRATCH/latency.csv"
  done
  wait "$sampler"
  echo "# $name: expired_keys from INFO (Redis only): $(redis-cli -p "$port" info stats 2>/dev/null | grep -a '^expired_keys:' | tr -d '\r' || true)" >>"$SCRATCH/notes.txt"
}

echo "server,t_s,dbsize,rss_mb" >"$SCRATCH/timeline.csv"
echo "server,t_start_s,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms" >"$SCRATCH/latency.csv"
: >"$SCRATCH/notes.txt"

GODEBUG=gctrace=1 ./bin/shardis --port 7104 --hz 10 2>"$SCRATCH/gctrace.txt" >/dev/null &
pid=$!; sleep 0.7
storm shardis 7104 "$pid"
kill "$pid"; wait "$pid" 2>/dev/null || true

redis-server --port 6399 --save "" --appendonly no --hz 10 >/dev/null 2>&1 &
pid=$!; sleep 0.7
storm redis 6399 "$pid"
kill "$pid"; wait "$pid" 2>/dev/null || true

{
  echo "# machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(uname -sr), $(go version | cut -d' ' -f3), redis $(redis-server --version | cut -d' ' -f3), $(date -u +%FT%TZ)"
  cat "$SCRATCH/notes.txt"
} >"$DIR/notes.txt"
cp "$SCRATCH/timeline.csv" "$SCRATCH/latency.csv" "$SCRATCH/gctrace.txt" "$DIR/"
cat "$DIR/notes.txt"
