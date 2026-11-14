# 021: Start the cluster from a static topology file (gossip takes over later)
- Context: In cluster mode every node must know who owns each of the 16384 slots, so it can serve
  its own and send `MOVED` for the rest, and replicas must know which primary to follow. There's
  no cluster bus yet (that's Iteration 8), so nothing can spread topology changes at runtime.
- Options considered:
  1. A static topology file, identical on every node: one line per node with its client address,
     internal address, role, and slots or primary. Each node picks its own line with
     `--cluster-node`.
  2. Configure at runtime the Redis way (`CLUSTER ADDSLOTS`, `CLUSTER MEET`, `CLUSTER
     REPLICATE`). It's the real workflow, but without gossip every node would need every command
     by hand to learn about the others.
  3. Per-node flags (`--slots 0-5460`): each node knows only itself, so it couldn't say *where* to
     redirect.
- Decision: Option 1:
  - Every slot must be owned by exactly one primary, names must be unique, and replicas must
    point at primaries. A node refuses to start otherwise.
  - Node IDs are 40-hex strings derived from the name (SHA-1), so they look like Redis IDs to
    tools that parse `CLUSTER NODES`.
  - Primaries get static config epochs 1..N.
  - A replica in the file starts replicating from its primary's internal address automatically
    (Iteration 6's `REPLICAOF`).
- Why:
  - **Deterministic:** the same file brings up the same cluster in local processes
    (`deploy/local`), Docker Compose (`deploy/compose`) and, later, Kubernetes (a ConfigMap). No
    bootstrap script races.
  - **Easy to check:** a bad file fails at startup with the exact problem ("slot 9000 has no
    owner").
  - **It becomes a seed:** in Iteration 8, gossip starts from this file and carries every change
    (failover, resharding) after that.
- Tradeoffs we accept:
  - **No changes without a restart** until Iteration 8. A dead primary stays dead: its slots
    can't be served until someone edits the file, and there's no automatic failover yet.
  - Every node must get the same file; a mismatched copy would route inconsistently. (Gossip and
    config epochs fix this in Iteration 8/9.)
  - `CLUSTER NODES` reports `ping-sent`/`pong-recv` as 0 and every link as `connected`. They're
    static claims, not measurements, until the bus exists.
- What real Redis does: Each node keeps its own `nodes.conf`, which it writes itself. A cluster is
  formed at runtime (`redis-cli --cluster create` sends `CLUSTER MEET`/`ADDSLOTS`/`REPLICATE`),
  and gossip spreads everything after that. We take a static shortcut for this iteration only.
- At bigger scale I'd: Keep a declarative desired topology (like this file, or a Kubernetes custom
  resource) and have an operator reconcile the running cluster towards it, while gossip and
  failover handle the fast, automatic changes. That's roughly how managed Redis services work.
