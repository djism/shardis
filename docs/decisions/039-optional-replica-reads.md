# 039: Replica reads are opt-in, with fallback to the primary
- Context: Each primary has a replica sitting mostly idle. Sending reads there spreads load
  (useful for read-heavy caches), but replication is asynchronous (ADR 017): a replica can be a
  few milliseconds behind, so a read right after a write may return the old value.
- Options considered:
  1. **Off by default; `ReadFromReplicas: true`** sends read-only commands to a random healthy
     replica of the slot, with `READONLY` sent on every new connection. On any failure (network
     error, `MOVED`, `CLUSTERDOWN`) the retry goes to the primary.
  2. **Always on:** more read capacity by default, and surprising stale reads for apps that
     expect "read your writes".
  3. **"Nearest" or latency-based routing** (go-redis `RouteByLatency`): better across zones, but
     needs latency tracking per node.
- Decision: Option 1.
- Why:
  - **Correctness first:** with the default, a client reads its own writes (same primary,
    same connection order). Stale reads are a choice the app makes, not a surprise. They're
    real: see the measurement below.
  - **A replica failing doesn't fail reads:** `TestReplicaReads` stops the only replica of a
    shard, and 50 reads in a row still succeed, falling back to the primary. A replica that
    just failed is skipped for a second.
  - **The `readonly` flag comes from COMMAND (ADR 038),** so writes never go to replicas.
    Unknown commands are treated as writes.
- Tradeoffs we accept:
  - **Stale reads, measured:** `TestReplicaReadStaleness` writes through the primary and reads
    the same key right away through a replica. **2.6–6.7% of those reads were stale** (3 runs
    of 2000), even on loopback, where replication takes well under a millisecond. Over a real
    network, or with a slow replica, it's more. If the replica's link is down, there's no
    bound at all. We don't check lag before reading. Redis
    doesn't either; a node in `--cluster-allow-reads-when-down` mode can serve old data.
  - **No read-your-writes across the switch:** after a fallback, reads may go back to a replica
    that's behind the primary they just read from.
- What real Redis clients do: go-redis has `ReadOnly`, `RouteRandomly` and `RouteByLatency`,
  all off by default, and Lettuce has `ReadFrom` (MASTER, REPLICA_PREFERRED, NEAREST). We match
  the "opt-in" default.
- At bigger scale I'd: Route by zone (read from the replica in my availability zone, saving
  cross-zone cost), and offer bounded staleness: skip replicas whose replication offset trails
  the primary's by more than N bytes or ms. That's what Aurora readers and Spanner's bounded-
  staleness reads give you.
