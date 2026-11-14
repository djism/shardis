#!/usr/bin/env bash
# Iteration 6: the replication lost-write window.
# Usage (from repo root): bash docs/benchmarks/iter-06-replication/run.sh
#
# shardis-crashtest -failover: a real primary + replica (separate processes),
# 16 writers, kill -9 the primary after 1.5-3 s of load, promote the replica,
# count acknowledged writes it lacks. 10 runs per variant.
set -euo pipefail
cd "$(dirname "$0")/../../.."
DIR=docs/benchmarks/iter-06-replication
make build >/dev/null
{
  echo "# machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(uname -sr), $(go version | cut -d' ' -f3), $(date -u +%FT%TZ)"
  ./bin/shardis-crashtest -failover plain,wait,slowlink,slowlink+wait -runs "${RUNS:-10}"
} | tee "$DIR/failover.csv"
