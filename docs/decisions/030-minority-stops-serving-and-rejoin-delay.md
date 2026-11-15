# 030: A node on the minority side of a partition stops serving, and waits before serving again
- Context: A network partition can cut a primary off from the rest of the cluster while its own
  clients can still reach it. The majority side will elect its replica and take over its slots.
  If the cut-off primary keeps accepting writes, two nodes act as primary for the same slots
  (split brain), and every write the old one accepts is thrown away when the partition heals.
- Options considered:
  1. **Minority check:** each node counts the slot-serving primaries it can reach (not
     PFAIL/FAIL from its point of view, itself included). With no majority, it refuses keyed
     commands with `CLUSTERDOWN`. When the majority comes back, it waits a **rejoin delay**
     (node timeout, clamped to 0.5–5 s) before serving again, so gossip can tell it whether it
     was replaced.
  2. **Leases:** the primary may only serve while it holds a time-limited lease granted by a
     majority. The majority never elects a replacement before the old lease has expired. This
     gives a hard guarantee instead of a timing race, but it depends on bounded clock drift and
     adds a lease protocol.
  3. **Do nothing:** keep serving until told otherwise. Maximum availability, unbounded split
     brain.
- Decision: Option 1, like Redis.
- Why:
  - **It shrinks split brain to about one node timeout,** and in practice removes the overlap.
    The minority notices after one timeout. The majority needs that timeout plus FAIL agreement,
    an election delay and a vote. In all 20 measured runs, the old primary stopped 0.2–1.4 s
    *before* the replica started ([benchmark](../benchmarks/iter-10-partition/SUMMARY.md)).
  - **The rejoin delay closes a second window.** Right after healing, the old primary can reach
    everyone again but may not yet have heard that it was replaced. Without the delay, it would
    accept a few writes in that gap. `TestMinorityThenRejoinDelay`: serving resumes ~1.06 s after
    regaining the majority (delay 1 s).
  - It reuses failure detection we already have (ADR 027), so it costs nothing extra on the wire.
- Tradeoffs we accept:
  - **Writes acknowledged during the first node timeout of a split are lost.** We measured
    ~2.1 million of them over 10 runs at a 2 s timeout, 99.98% of what the cut-off primary
    acknowledged during the split. That isn't a bug; it's the design.
  - **"No overlap" is a timing margin, not a guarantee.** In one 5 s run it was 199 ms. A long GC
    pause or a stalled cron on the old primary could make both sides write at once for a moment.
    Leases (option 2) would make that impossible.
  - **A minority node refuses reads too,** unless `--cluster-allow-reads-when-down yes`. Then it
    serves possibly stale data.
  - Replicas count too: a replica in the minority refuses even `READONLY` reads by default.
- What real Redis does: The same idea. A primary that can't reach a majority of primaries for
  node_timeout sets `cluster_state:fail` and refuses queries. It also has a rejoin delay
  (`CLUSTER_MAX_REJOIN_DELAY` 5 s, `CLUSTER_MIN_REJOIN_DELAY` 0.5 s, otherwise node_timeout), and
  the same `cluster-allow-reads-when-down` option. Redis documents the lost-write window
  explicitly. We match it.
- At bigger scale I'd: Use leases (or fencing tokens, which are increasing numbers that storage
  checks so a stale primary's writes are rejected) where losing acknowledged writes isn't
  acceptable. Or use a consensus-replicated store (Raft/Paxos: a write is acknowledged only once
  a majority has it, so a minority can never acknowledge anything). For a cache, Redis's tradeoff
  is usually right.
