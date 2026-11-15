#!/usr/bin/env bash
# Crash a primary: SIGKILL its container, with no warning, like the process
# dying. (kubectl delete pod, even with --grace-period=0 --force, still sends
# SIGTERM first, and a primary uses that to hand off gracefully; see
# rolling-restart.sh.) The kubelet restarts the container in the same pod,
# same volume and same node ID. Whether its replica takes over first depends
# on the race between failure detection (node timeout) and the restart. On
# minikube the restart wins (~1 s): the node keeps its role.
#
# Part 2 freezes a primary instead (docker pause: like a VM pause or a hung
# kernel). The process is stuck but nothing restarts it, so the replica is
# elected; on unfreezing, the old primary finds it was replaced and rejoins
# as a replica.
source "$(dirname "$0")/lib.sh"
start_checker 100s
sleep 15
P=$(primary_pod)
say "crashing $P (SIGKILL to its container: no SIGTERM, no handoff)"
CID=$(minikube -p "$PROFILE" ssh -- "docker ps -q --filter name=k8s_shardis_${P}_" | tr -d '\r' | head -1)
minikube -p "$PROFILE" ssh -- "docker kill --signal=KILL $CID" >/dev/null
sleep 20
say "after the crash: who serves the slots now? ($P restarted with the same volume and node ID)"
nodes shardis-0 2>/dev/null || nodes shardis-1
kc get pod "$P" -o jsonpath='{.metadata.name}: restarts={.status.containerStatuses[0].restartCount}{"\n"}'

P=$(primary_pod)
CID=$(minikube -p "$PROFILE" ssh -- "docker ps -q --filter name=k8s_shardis_${P}_" | tr -d '\r' | head -1)
say "freezing $P for 8 s (docker pause): stuck, but not dead, so Kubernetes won't restart it"
minikube -p "$PROFILE" ssh -- "docker pause $CID" >/dev/null
sleep 8
minikube -p "$PROFILE" ssh -- "docker unpause $CID" >/dev/null
sleep 10
say "after the freeze: its replica took over and $P is a replica now"
nodes shardis-0 2>/dev/null || nodes shardis-1
finish_checker
