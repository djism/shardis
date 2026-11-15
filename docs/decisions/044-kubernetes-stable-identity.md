# 044: On Kubernetes, nodes are identified by stable DNS names, and a Job forms the cluster
- Context: A Shardis node's identity has three parts:
  - its node ID (nodes.conf)
  - its data (the AOF)
  - its address, which other nodes and clients use

  On Kubernetes, a restarted pod gets a new IP. If nodes announced IPs, every restart would
  look like a stranger with a familiar ID, and peers would keep dialing a dead address.
- Options considered:
  1. **StatefulSet + headless Service + DNS names.**
     - Pod `shardis-N` always has the name `shardis-N.shardis.<ns>.svc.cluster.local` and
       always gets back its own PersistentVolume.
     - Nodes announce that name (`SHARDIS_CLUSTER_ANNOUNCE_ADDR`) and resolve it on every dial.
     - A bootstrap Job (`shardis-admin create`) forms the cluster once all pods answer.
  2. **Announce pod IPs and fix them up after restarts** (gossip the new IP under the same ID).
     That's what Redis does by default. It works, but a restarted node and its peers disagree
     until gossip catches up, and clients cache dead IPs.
  3. **The seed topology file in a ConfigMap** (Iteration 7): no Job needed, but scaling means
     editing the ConfigMap, and pods not in the file can't start.
- Decision: Option 1.
- Why:
  - **Identity survives everything we did to it:** force-deleting a pod, rolling restarts,
    NetworkPolicy partitions. Each time the pod came back with the same name, ID and data, and
    rejoined in the right role.
  - **The Job is idempotent,** so it's safe to apply the manifests again. `create` checks
    whether the cluster is already formed and does nothing if so (`TestCreate`).
  - **Details that matter:**
    - `publishNotReadyAddresses: true`, so pods can resolve each other while starting (and a
      node in a minority isn't Ready but still a node).
    - Readiness means "accepting connections", not `cluster_state:ok`. Otherwise one failed
      shard would pull every pod out of DNS, and they'd never find each other again.
    - `podManagementPolicy: Parallel` for the first start.
    - A PodDisruptionBudget of 1.
    - fsGroup, so the non-root container can write its volume.
- Tradeoffs we accept:
  - **Clients must resolve cluster DNS:** MOVED says `shardis-2.shardis.shardis.svc.cluster.local:7000`.
    Clients inside Kubernetes are fine. A laptop needs port-forwarding or an in-cluster client
    (the demos run the checker as a Job).
  - **A crashed container restarts (~1 s) faster than failure detection,** so it usually keeps
    its primary role. That's fine, but failovers on Kubernetes come mostly from *stuck* nodes
    (frozen, partitioned), which Kubernetes doesn't restart because its TCP probe still
    passes. A smarter liveness check (one that runs a command) would catch a frozen process,
    at the risk of restarting nodes that are merely slow.
  - **NetworkPolicy partitions need help:** Calico enforces policy on new connections only, so
    the partition demo also deletes the pod's conntrack entries to cut its open connections.
  - **Scaling in isn't automated:** remove a shard with `reshard` + `remove-node` before scaling
    the StatefulSet down, or its slots go with it.
- What real Redis does: Redis 7 can announce hostnames (`cluster-announce-hostname`,
  `cluster-preferred-endpoint-type hostname`). The common Redis operators use StatefulSets and
  an init Job much like this, and the Bitnami chart runs `redis-cli --cluster create` in a
  hook.
- At bigger scale I'd: Write an operator (a controller that watches a `ShardisCluster` resource)
  that does what the demo scripts do by hand: add or remove shards on scale, rebalance, finish
  half-moved slots, and roll pods only when replicas are in sync. Add pod anti-affinity, so a
  primary and its replica never share a Kubernetes node or zone.
