# Iteration 9: automatic failover time

**Setup:** Apple M4, macOS 26.6, Go 1.26.1. 3 primaries + 3 replicas as real `shardis` processes
from a seed topology. 16 writers write keys owned by primary `a`, straight to `a`. After 1 s it gets
`kill -9`. We probe its replica `d` every 10 ms with a write to one of `a`'s slots, until `d`
answers OK (before that: `MOVED` to the dead `a`, then `CLUSTERDOWN`). Then we count writes `a`
had acknowledged that `d` doesn't have. 20 runs per node timeout.
Reproduce: `bash docs/benchmarks/iter-09-failover/run.sh`. Raw: `raw.csv`, `summary.txt`.

| `cluster-node-timeout` | p50 | p99* | min – max | acknowledged writes lost |
|---:|---:|---:|---|---|
| 1 s | **2.26 s** | 2.47 s | 1.97 – 2.47 s | 30 of 1,829,826 (7 of 20 runs, max 13 in a run) |
| 2 s | **3.07 s** | 3.37 s | 2.96 – 3.37 s | 152 of 1,901,538 (5 of 20 runs, max 146) |
| 5 s | **7.17 s** | 7.91 s | 6.68 – 8.46 s | 88 of 1,868,748 (4 of 20 runs, max 84) |

\* With 20 samples, "p99" is the second-slowest run; the max is shown too.

No manual steps in any of the 60 runs: every time, the replica was elected and took over.

## What this tells us

1. **Failover ≈ node timeout + 1 to 2.5 s.** The timeout itself is the biggest part (we can't
   suspect a node sooner). The rest:
   - waiting for failure reports from a majority of primaries to arrive by gossip
   - the election's built-in 0.5–1 s random delay, which keeps two replicas from starting at the
     same instant and splitting the votes
   - one round trip to collect votes
   - announcing the new owner

   With Redis's default 15 s timeout, expect roughly 16–18 s of unavailability for that shard.
2. **The timeout is a real tradeoff, not a free knob.** 1 s gives 2.3 s failovers, but a GC pause,
   a busy CPU or a network hiccup longer than a second would now trigger a failover, and every
   failover can lose acknowledged writes. Production setups choose seconds, not milliseconds.
3. **Failovers lose acknowledged writes, as Iteration 6 predicted.** In 16 of 60 runs some writes
   that `a` had acknowledged weren't on `d`: they were in flight in the asynchronous replication
   stream when `a` died. Here that's ≤ 146 writes per failover (~0.01%), because the replica is on
   the same machine. On a real network the window, and the loss, grows with replication lag.
4. **Keyed commands fail cluster-wide during the gap.** With `cluster-require-full-coverage yes`
   (the default, as in Redis), every keyed command on every node gets
   `CLUSTERDOWN The cluster is down` until the failover completes, not just commands for `a`'s
   slots. `--cluster-require-full-coverage no` limits the outage to that shard.
5. **Clients matter too:** go-redis by default refreshes its slot map only on `MOVED` or every 60 s,
   so after a failover it can keep trying the dead primary for up to a minute even though the
   cluster recovered in ~3 s (see `TestFailoverEndToEnd`). Our client in Iteration 12 should
   refresh on connection errors.

**Caveat:** one machine and loopback, so this is close to a best case for gossip and vote latency.
The timeout-dominated shape carries over to real networks.
