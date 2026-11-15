#!/usr/bin/env bash
# Remove everything (including the volumes); leave minikube running.
# minikube stop -p shardis / minikube delete -p shardis to go further.
source "$(dirname "$0")/lib.sh"
kubectl --context "$PROFILE" delete namespace "$NS" --wait=true
