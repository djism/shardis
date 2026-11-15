#!/usr/bin/env bash
# Iteration 12: what an application sees through our smart client during a
# pause (SIGSTOP 1.5 s), a crash + failover, a restart and a rebalance, for
# each retry policy. 5 runs each, ~8 min.
set -euo pipefail
cd "$(dirname "$0")/../../.."
make build >/dev/null
out=docs/benchmarks/iter-12-client/raw.csv
bin/shardis-crashtest -client-chaos safe,all,never -runs 5 | tee "$out"
echo "wrote $out"
