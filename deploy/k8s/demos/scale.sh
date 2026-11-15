#!/usr/bin/env bash
# kubectl scale to 8 pods, then add them as a 4th shard (primary + replica)
# and rebalance, all under load.
source "$(dirname "$0")/lib.sh"
start_checker 150s
sleep 10
say "scaling the StatefulSet to 8 pods"
kc scale statefulset/shardis --replicas=8
kc wait --for=condition=Ready pod/shardis-6 pod/shardis-7 --timeout=180s
say "adding shardis-6 (new primary) and shardis-7 (its replica)"
# -addr must be a name the NEW node can reach (it's told to MEET it).
admin add-node -addr "shardis-0.$FQDN:7000" -new "shardis-6.$FQDN:7000"
admin add-node -addr "shardis-0.$FQDN:7000" -new "shardis-7.$FQDN:7000" -replica-of "shardis-6.$FQDN:7000"
say "rebalancing onto the new shard"
admin rebalance -addr localhost:7000
sleep 5 # replicas learn new owners by gossip, a second or two later
nodes
finish_checker
