#!/usr/bin/env bash
# Iteration 4: hit rate of eviction policies under a skewed (Zipfian) workload.
# Usage (from repo root): bash docs/benchmarks/iter-04-eviction/run.sh
#
# Workload (shardis-loadgen, cache-aside): 1M distinct keys, Zipf theta=0.99,
# 100-byte values, 50 clients. GET; on a miss, SET. 25s warmup (fills the cache
# and lets eviction reach steady state), then 20s measured.
#
# Memory limit: 32 MB for Shardis, about 10-15% of the keys. Hit rate depends
# mostly on how many keys fit, so we compare two ways:
#   - same maxmemory: what you get for the same RAM (exact LRU fits fewer keys)
#   - same resident keys: pure algorithm quality (exact LRU gets extra memory;
#     Redis gets the maxmemory that holds about as many keys as Shardis)
set -euo pipefail
cd "$(dirname "$0")/../../.."
DIR=docs/benchmarks/iter-04-eviction
OUT=$DIR/raw.csv
WARMUP=${WARMUP:-25s}
DURATION=${DURATION:-20s}
LIMIT=$((32 << 20))
make build >/dev/null
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

loadgen() { # label port
  ./bin/shardis-loadgen -addr "127.0.0.1:$2" -keys 1000000 -theta 0.99 -value 100 -clients 50 \
    -warmup "$WARMUP" -duration "$DURATION" -csv -label "$1"
}

shardis_run() { # label policy samples maxmemory
  ./bin/shardis --port 7106 --maxmemory "$4" --maxmemory-policy "$2" --maxmemory-samples "$3" >/dev/null 2>&1 &
  local pid=$!; sleep 0.5
  loadgen "$1" 7106 | tee -a "$OUT"
  kill "$pid"; wait "$pid" 2>/dev/null || true
}

redis_run() { # label policy samples maxmemory
  redis-server --port 6399 --save "" --appendonly no --maxmemory "$4" --maxmemory-policy "$2" \
    --maxmemory-samples "$3" >/dev/null 2>&1 &
  local pid=$!; sleep 0.5
  loadgen "$1" 6399 | tee -a "$OUT"
  kill "$pid"; wait "$pid" 2>/dev/null || true
}

col() { echo "$1" | cut -d, -f"$2"; } # pick a CSV column

{
  echo "# machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(uname -sr), $(go version | cut -d' ' -f3), redis $(redis-server --version | cut -d' ' -f3), $(date -u +%FT%TZ), warmup=$WARMUP duration=$DURATION"
  ./bin/shardis-loadgen -csv-header
} >"$OUT"

shardis_run "shardis allkeys-random" allkeys-random 5 $LIMIT
lru5=$(shardis_run "shardis allkeys-lru (5 samples)" allkeys-lru 5 $LIMIT)
shardis_run "shardis allkeys-lru (10 samples)" allkeys-lru 10 $LIMIT
exact=$(shardis_run "shardis exact LRU (same maxmemory)" allkeys-lru-exact 5 $LIMIT)

# Same resident keys for exact LRU: give it 107 extra bytes per key (its list
# node cost, ADR 012) for as many keys as approximate LRU held.
target=$(col "$lru5" 16)
shardis_run "shardis exact LRU (same keys)" allkeys-lru-exact 5 $((LIMIT + target * 107))

# Redis: find the maxmemory that holds about $target keys, then measure.
probe=$(WARMUP=20s DURATION=2s redis_run "redis calibration" allkeys-lru 5 $LIMIT)
rkeys=$(col "$probe" 16)
rlimit=$(awk -v l=$LIMIT -v t="$target" -v k="$rkeys" 'BEGIN{printf "%d", l * t / k}')
echo "# redis calibration: $rkeys keys at $LIMIT bytes; using maxmemory $rlimit for ~$target keys" >>"$OUT"
redis_run "redis allkeys-random" allkeys-random 5 "$rlimit"
redis_run "redis allkeys-lru (5 samples)" allkeys-lru 5 "$rlimit"
redis_run "redis allkeys-lru (10 samples)" allkeys-lru 10 "$rlimit"
