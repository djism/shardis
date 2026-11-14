#!/usr/bin/env bash
# Iteration 2: executor vs sharded locks vs real Redis, SET/GET over 100k random keys.
# Usage (from repo root): bash docs/benchmarks/iter-02-concurrency/run.sh
# Env: REPS (default 3).
#
# Note: no --threads. redis-benchmark's multi-threaded mode measures elapsed time
# in coarse steps (200k requests always came out as 114,220 or 800,000 req/s),
# and it was slower than the single-threaded client anyway. Checked 2026-11-13.
set -euo pipefail
cd "$(dirname "$0")/../../.."
OUT=docs/benchmarks/iter-02-concurrency/raw.csv
REPS=${REPS:-3}

make build >/dev/null
./bin/shardis --port 7101 --engine executor >/dev/null 2>&1 & P1=$!
./bin/shardis --port 7102 --engine sharded >/dev/null 2>&1 & P2=$!
redis-server --port 6399 --save "" --appendonly no >/dev/null 2>&1 & P3=$!
trap 'kill $P1 $P2 $P3 2>/dev/null' EXIT
sleep 1
for p in 7101 7102 6399; do redis-cli -p $p ping >/dev/null; done

{
  echo "# machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(getconf _NPROCESSORS_ONLN) cores, $(uname -sr)"
  echo "# go: $(go version | cut -d' ' -f3)  redis-server: $(redis-server --version | cut -d' ' -f3)  date: $(date -u +%FT%TZ)  reps=$REPS keyspace=100000"
  echo "rep,server,clients,pipeline,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms"
  for rep in $(seq 1 "$REPS"); do
    # Rotate server order each rep so warm-up / thermal effects don't favor one.
    case $rep in
      1) order="executor:7101 sharded:7102 redis:6399" ;;
      2) order="sharded:7102 redis:6399 executor:7101" ;;
      *) order="redis:6399 executor:7101 sharded:7102" ;;
    esac
    for srv in $order; do
      name=${srv%%:*} port=${srv##*:}
      redis-cli -p "$port" flushall >/dev/null
      # clients pipeline requests: sized so each run lasts a few seconds
      for cfg in "1 1 150000" "50 1 600000" "200 1 600000" "50 16 3000000"; do
        read -r clients pipe n <<<"$cfg"
        redis-benchmark -h 127.0.0.1 -p "$port" -t set,get -n "$n" -r 100000 \
          -c "$clients" -P "$pipe" --csv 2>/dev/null \
          | tail -n +2 | tr -d '"' | sed "s/^/$rep,$name,$clients,$pipe,/"
      done
    done
  done
} | tee "$OUT"
