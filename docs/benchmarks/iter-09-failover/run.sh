#!/usr/bin/env bash
# Iteration 9: automatic failover time.
# Usage (from repo root): bash docs/benchmarks/iter-09-failover/run.sh
#
# 3 primaries + 3 replicas as real processes; kill -9 a primary under write
# load; time until its replica accepts writes for its slots. 20 runs per
# node timeout. Per-run rows go to raw.csv; p50/p99 lines to summary.txt.
set -euo pipefail
cd "$(dirname "$0")/../../.."
DIR=docs/benchmarks/iter-09-failover
make build >/dev/null
{
  echo "# machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(uname -sr), $(go version | cut -d' ' -f3), $(date -u +%FT%TZ)"
  ./bin/shardis-crashtest -cluster-failover "${TIMEOUTS:-1000,2000,5000}" -runs "${RUNS:-20}" 2>"$DIR/summary.txt"
} | tee "$DIR/raw.csv"
cat "$DIR/summary.txt"
