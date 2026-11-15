# Shared helpers for the demo scripts. Source it; don't run it.
set -euo pipefail
PROFILE=${PROFILE:-shardis}           # a separate minikube profile (doesn't touch "minikube")
NS=shardis
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
K8S=$ROOT/deploy/k8s
FQDN=shardis.$NS.svc.cluster.local

kc() { kubectl --context "$PROFILE" -n "$NS" "$@"; }
say() { printf '\n\033[1m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }

# admin <args...>: run shardis-admin inside shardis-0 (the image has no shell,
# but it does have the binary).
admin() { kc exec shardis-0 -c shardis -- /shardis-admin "$@"; }
admin_on() { local pod=$1; shift; kc exec "$pod" -c shardis -- /shardis-admin "$@"; }

nodes() { admin_on "${1:-shardis-0}" nodes -addr localhost:7000; }

# primary_pod: a pod that currently owns slots, e.g. "shardis-2".
primary_pod() {
  nodes "${1:-shardis-0}" | awk '$3 > 0 {print $2}' | head -1 | sed -E 's/\..*//'
}

# start_checker <duration>: the consistency checker as a Job, in the cluster.
start_checker() {
  kc delete job shardis-checker --ignore-not-found --wait=true >/dev/null
  sed "s/DURATION/$1/" "$K8S/checker-job.yaml" | kc apply -f - >/dev/null
  kc wait --for=condition=Ready pod -l job-name=shardis-checker --timeout=120s >/dev/null
  say "checker running for $1 (writes increasing values, reads them back, records every op)"
}

# finish_checker: wait for the checker and print its report.
finish_checker() {
  kc wait --for=condition=complete job/shardis-checker --timeout=600s >/dev/null || true
  say "checker report"
  kc logs job/shardis-checker | grep -v '^ *[0-9]*s  moved=' || true
}

wait_cluster_ok() {
  for _ in $(seq 120); do
    if kc exec "${1:-shardis-0}" -c shardis -- /shardis-admin nodes -addr localhost:7000 >/dev/null 2>&1 &&
       [ "$(nodes "${1:-shardis-0}" | awk '$3 > 0' | wc -l | tr -d ' ')" -ge 3 ]; then return; fi
    sleep 2
  done
  echo "cluster never settled" >&2; return 1
}
