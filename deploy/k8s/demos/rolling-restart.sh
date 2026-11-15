#!/usr/bin/env bash
# kubectl rollout restart: every pod is replaced, one at a time, under load.
# Each primary gets SIGTERM and hands its slots to its replica first
# (ADR 043), so no acknowledged write should be lost.
source "$(dirname "$0")/lib.sh"
start_checker 150s
sleep 10
say "rolling restart of all 6 pods"
kc rollout restart statefulset/shardis
kc rollout status statefulset/shardis --timeout=300s
say "after the restart (primaries and replicas have swapped roles):"
nodes
finish_checker
