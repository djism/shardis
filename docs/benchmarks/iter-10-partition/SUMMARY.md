# Iteration 10: the split-brain window

**Setup:** Apple M4, macOS 26.6, Go 1.26.1. 3 primaries + 3 replicas as real `shardis` processes
with `--enable-fault-injection yes`. 16 writers keep writing keys owned by primary `a`, straight
to `a`, as if those clients sit on `a`'s side of the network. After 1 s, every node gets
`FAULT PARTITION a`. That cuts `a` off from the other five on the cluster bus and the
replication stream, but `a`'s clients can still reach it. We probe `a`'s replica `d` every 10 ms
with a write to one of `a`'s slots. One second after `d` accepts it, every node gets `FAULT HEAL`.
Once `a` has rejoined as `d`'s replica, we count the writes `a` acknowledged that `d` doesn't have.
10 runs per node timeout.
Reproduce: `bash docs/benchmarks/iter-10-partition/run.sh` (~5 min). Raw: `raw.csv`.

All times are measured from the moment of the split.

| `cluster-node-timeout` | `a` kept acking writes (p50, max) | `d` took over (p50, max) | nobody writable (min – max) | acked writes lost after heal |
|---:|---|---|---|---|
| 2 s | **2.07 s** (2.19 s) | **3.18 s** (3.48 s) | 0.79 – 1.41 s | 2,125,387 of 2,125,779 acked during the split |
| 5 s | **6.67 s** (7.67 s) | **7.72 s** (8.68 s) | **0.20** – 1.21 s | 6,769,461 of 6,769,865 acked during the split |

Writes acknowledged *before* the split: none lost in any of the 20 runs.

## What this tells us

1. **The minority side stops first, but only by a timing margin.** `a` refuses writes once it
   has gone a node timeout without hearing from `b` and `c`, because it can no longer reach a
   majority of the primaries (ADR 030). `d` needs the same timeout plus FAIL agreement, the
   election delay and a vote round. So in all 20 runs there was **no moment when both `a` and `d`
   accepted writes** for the same slots. That's two "true" primaries never existing at once. But
   the margin is just two timers racing: in one 5 s run it was only 199 ms. A GC pause or a stalled
   cron on `a` could flip it. Nothing like a lease enforces it (see ADR 030).
2. **Everything `a` acknowledged during the split is lost.** This is the expensive part. For
   about one node timeout, `a` keeps saying "OK" to its clients. Those writes can't reach `d`.
   When the partition heals, `a` learns `d` has a higher epoch, becomes its replica, and replaces
   its data with `d`'s. 99.98% of what `a` acked during the split vanished. The ~0.02% that
   survived was already in flight to `d` when the cut happened. The loss is **throughput × node
   timeout**. Our loopback writers did ~100k writes/s, so this is a worst case in count, but the
   *shape* is real: Redis Cluster's docs describe exactly this window.
3. **There's also a gap with no writable primary** (0.2–1.4 s here), on top of the ~node timeout
   in which the majority side's clients can't write `a`'s slots at all. With full coverage on
   (the default), clients on the majority side get `CLUSTERDOWN` for *every* slot while `a` is
   marked FAIL.
4. **A smaller node timeout shrinks the loss window but raises false alarms**, the same tradeoff
   as Iteration 9. Here it's starker. 2 s → ~2 s of acknowledged-then-lost writes; Redis's default
   15 s → ~15 s of them.

## Compared with Iteration 9 (crash)

| | primary crashes (iter 9, 2 s timeout) | primary partitioned (iter 10, 2 s timeout) |
|---|---|---|
| clients get errors | right away (connection refused) | only after ~2 s; until then they get "OK" |
| acked writes lost | ≤ 146 per failover (in-flight replication) | **~215,000 per run** (everything for ~2 s) |

A crash is the *easy* failure: the dead node can't lie. A partition is worse, because the cut-off
node is alive and keeps acknowledging writes it can't protect.
