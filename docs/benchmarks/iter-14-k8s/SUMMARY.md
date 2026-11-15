# Iteration 14: Shardis on Kubernetes (minikube)

**Setup:**
- **Kubernetes:** minikube v1.38 profile `shardis` (Docker driver, Calico CNI, 4 CPUs, 4 GB) on an
  Apple M4. The Mac was under heavy memory pressure (see Iteration 11).
- **Cluster:** 6 pods (StatefulSet), each with an AOF on its own PersistentVolume. Node timeout
  is 2 s (a demo setting, see below).
- **Checker:** each demo runs `shardis-chaos -external` as a Job *inside* the cluster while the
  script acts with `kubectl`. That's the Iteration 13 checker: increasing values per key, every
  operation recorded.
- **Reproduce:** `deploy/k8s/demos/up.sh`, then any demo script.

| demo (script) | what happens | ops | errors | rolled-back writes | stale reads | longest stall |
|---|---|---:|---:|---:|---:|---|
| `failover.sh` part 1 | primary's container SIGKILLed (a crash) | 505,177 | 0 | 0 | 0 | 1.5 s (restarted before failover, see 1) |
| `failover.sh` part 2 | primary frozen 8 s (`docker pause`) | 917,959 (both parts) | 0 | 0 | 0 | 8.5 s (the freeze; replica took over) |
| `rolling-restart.sh` | all 6 pods replaced, one at a time | 1,122,348 | 0 | **0** | 0 | 0.2–2.6 s, outside the rollout window (see 2) |
| `partition.sh` | NetworkPolicy cuts a primary off for 25 s | 943,699 | 0 | **2,029** | **130** | 2.3 s (the failover) |
| `scale.sh` | 6 → 8 pods, new shard, rebalance (4096 slots, 12 s) | 1,403,012 | 0 | 0 | 0 | see 2 |

Every demo had zero client errors (the smart client retried through everything) and zero
phantom values.

## What this tells us

1. **On Kubernetes, a crashed process usually comes back before anyone fails it over.**
   SIGKILL the primary's container, and the kubelet restarts it in the same pod, with the same
   IP, volume and node ID, in about 1 s. That's faster than failure detection (2 s here, 5–15 s
   in production). It resumes as the primary from its AOF, with nothing lost, and its slots
   stall for ~1.5 s.

   What *does* cause a failover is a node that's stuck but not dead (a VM pause, a hung
   kernel, a full disk). Kubernetes doesn't restart it, because its TCP probe still connects.
   Part 2 freezes the primary for 8 s:
   1. the majority marks it FAIL
   2. its replica is elected
   3. on unfreezing, it logs "my slots were taken over (higher epoch); becoming a replica"

   Two traps on the way:
   - **`kubectl delete pod --grace-period=0 --force` is not a crash.** The kubelet still sends
     SIGTERM first, and our primary used it to hand off gracefully. Only SIGKILLing the
     container (via the node's container runtime) is a real crash.
   - **An earlier run with a 5 s node timeout** showed the force-deleted pod back in ~5 s and
     still primary. The demos now use 2 s.
2. **Rolling restarts lose nothing.** `kubectl rollout restart` replaces every pod under load.
   Each primary, on SIGTERM:
   1. pauses writes
   2. waits for its replica (`WAIT 1`)
   3. hands over with `CLUSTER FAILOVER TAKEOVER`
   4. exits

   The pod logs show `graceful handoff done took=13ms` for each primary, and the checker found
   0 rolled-back writes and 0 stale reads over 1.1M operations (ADR 043). The longest stalls
   (0.2–2.6 s) all happened a minute or more after the rollout ended, when nothing was
   happening in the cluster. They're the swapping Mac freezing processes; the quiet run's worst
   was 207 ms. In-process, the same handoff measures 22–24 ms with the slowest request at 28–37 ms
   (`TestGracefulHandoff`).
3. **A partition loses acknowledged writes, here as everywhere.** The NetworkPolicy cut
   `shardis-5` off:
   1. it noticed it was in the minority after 2 s and refused commands
   2. its replica `shardis-2` was voted in (epoch 33)
   3. after the heal, `shardis-5` rejoined as a replica

   The writes it accepted before noticing (2,029) were rolled back, and 130 reads went
   backwards. The same split-brain window as Iterations 10 and 13, now with a real Kubernetes
   NetworkPolicy.
4. **Scaling out is live.** `kubectl scale` to 8, `add-node` twice, and `rebalance`: 4096 slots
   in 12 s, zero errors, zero loss.

## Things we found along the way

- **A NetworkPolicy doesn't cut existing connections.** Calico, like most iptables-based CNIs,
  checks policy on *new* connections. Packets of established ones pass through connection
  tracking. The first partition run changed nothing: no node even suspected the "isolated"
  pod, because the cluster bus and replication links were already open. The demo now deletes
  the pod's conntrack entries right after applying the policy, which is what a real network
  failure does to open connections (ADR 044).
- **The checker caught a harness bug: 233 "phantom" reads.** In external mode every run wrote
  the same keys into a cluster that keeps its data (AOF on PVCs). A shorter run read the
  previous, longer run's higher values. Keys now have a unique prefix per run.
- **`add-node -addr localhost:7000` run inside a pod tells the new node to MEET *itself*.**
  `add-node` now refuses a loopback `-addr` for a non-loopback new node.
- **A short node timeout on a struggling machine causes false alarms.** Twice, every node
  marked every other node unreachable at the same instant, and once the whole cluster briefly
  declared itself in the minority. That was the minikube VM freezing for over 2 s under memory
  pressure. No data was lost (nothing could be written while frozen), but it's exactly the
  tradeoff of ADR 027: 2 s is a demo setting.
- **`minikube start -p shardis` switched kubectl's current context to `shardis`.** The demo
  scripts always pass `--context shardis` and don't depend on it.
