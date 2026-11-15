#!/usr/bin/env bash
# Cut a primary pod off from the other Shardis pods with a NetworkPolicy
# (needs Calico). The checker can still reach it, like clients on the
# minority side of a real partition. The majority fails over; when the
# policy is removed, the old primary rejoins as a replica and anything it
# accepted alone is lost (Iteration 10, now on Kubernetes).
source "$(dirname "$0")/lib.sh"
start_checker 90s
sleep 15
P=$(primary_pod)
say "isolating $P from the other Shardis pods (NetworkPolicy)"
sed "s/POD/$P/" "$K8S/networkpolicy-partition.yaml" | kc apply -f -
# Calico (like most iptables-based CNIs) checks policy on NEW connections;
# packets of connections that already exist are let through by connection
# tracking. The cluster bus and replication links are long-lived, so the
# policy alone cuts nothing. A real network failure breaks existing
# connections too: deleting the pod's conntrack entries makes every next
# packet go through the policy (ADR 044).
IP=$(kc get pod "$P" -o jsonpath='{.status.podIP}')
sleep 1 # let Calico program the rules first
minikube -p "$PROFILE" ssh -- "sudo conntrack -D -s $IP; sudo conntrack -D -d $IP" >/dev/null 2>&1 || true
say "dropped existing connections of $P ($IP)"
sleep 25
say "healing (deleting the NetworkPolicy)"
kc delete networkpolicy isolate-one-node
sleep 15
nodes shardis-0 2>/dev/null | sort -k2
finish_checker
