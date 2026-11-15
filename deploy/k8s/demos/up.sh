#!/usr/bin/env bash
# Start minikube (profile "shardis", with Calico so NetworkPolicy works),
# build the image inside it, deploy, and form the cluster.
source "$(dirname "$0")/lib.sh"
if ! minikube status -p "$PROFILE" >/dev/null 2>&1; then
  say "starting minikube profile $PROFILE (Calico CNI, 4 CPUs, 4 GB)"
  minikube start -p "$PROFILE" --driver=docker --cni=calico --cpus=4 --memory=4096
fi
say "building shardis:dev inside minikube"
minikube -p "$PROFILE" image build -t shardis:dev "$ROOT"
say "deploying"
kubectl --context "$PROFILE" apply -k "$K8S"
kc rollout status statefulset/shardis --timeout=300s
say "forming the cluster (bootstrap Job)"
kc wait --for=condition=complete job/shardis-bootstrap --timeout=300s
kc logs job/shardis-bootstrap | tail -8
nodes
