#!/usr/bin/env bash
# Wait until all 6 compose nodes answer and the replicas are in sync.
set -euo pipefail
rcli() { perl -e 'alarm shift; exec @ARGV' 2 redis-cli "$@"; } # AirPlay-safe (see deploy/local/cluster.sh)
for port in 7000 7001 7002 7003 7004 7005; do
  for _ in $(seq 100); do [ "$(rcli -p "$port" ping 2>/dev/null)" = PONG ] && break; sleep 0.2; done
done
for port in 7003 7004 7005; do
  for _ in $(seq 100); do rcli -p "$port" info replication 2>/dev/null | grep -q master_link_status:up && break; sleep 0.2; done
done
for port in 7000 7001 7002 7003 7004 7005; do
  printf "127.0.0.1:%s  %s\n" "$port" "$(rcli -p "$port" role 2>/dev/null | head -1 || echo down)"
done
