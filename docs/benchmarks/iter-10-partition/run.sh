#!/usr/bin/env bash
# Iteration 10: the split-brain window. Cuts primary a off from the other
# five nodes while its clients keep writing to it, 10 runs each at node
# timeouts of 2 s and 5 s. ~5 min.
set -euo pipefail
cd "$(dirname "$0")/../../.."
make build >/dev/null
out=docs/benchmarks/iter-10-partition/raw.csv
bin/shardis-crashtest -partition 2000,5000 -runs 10 | tee "$out"
echo "wrote $out"
