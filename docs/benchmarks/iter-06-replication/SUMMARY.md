# Iteration 6: the replication lost-write window

**Setup:** Apple M4, macOS 26.6, Go 1.26.1. A primary and a replica run as separate `shardis`
processes on one machine. 16 writers each send numbered `SET`s one at a time. After 1.5–3 s of
load the primary gets `kill -9`, the replica is promoted (`REPLICAOF NO ONE`), and we count
acknowledged writes the new primary doesn't have. 10 runs per variant.
Reproduce: `bash docs/benchmarks/iter-06-replication/run.sh`. Raw: `failover.csv`.

| Variant | What counts as "acknowledged" | Acked writes lost | Runs with any loss | Writes/s |
|---|---|---:|---:|---:|
| plain | `SET` returned OK | **4** of 2,446,017 | 3 / 10 | 99k |
| wait | `SET` OK *and* `WAIT 1 1000` returned 1 | **0** of 1,176,653 | 0 / 10 | 64k |
| slowlink | `SET` returned OK | **1,515,354** of 2,320,525 (up to 68% per run) | 10 / 10 | 107k |
| slowlink + wait | `SET` OK and `WAIT 1` returned 1 | **0** of 5,648 | 0 / 10 | 293 |

"slowlink" = fault injection holds back each chunk of the replication stream (up to 64 KB) by
50 ms, capping it at ~1.3 MB/s, below the primary's ~4 MB/s write rate. The replica falls further
and further behind, as it would on a congested or cross-region link.

## What this tells us

1. **The window is real even in the best case.** Replica on the same machine, sub-millisecond
   replication: still 4 acknowledged writes lost across 10 failovers. Those writes were applied
   and acknowledged by the primary in the instant before it died, and hadn't reached the replica
   yet. Asynchronous replication can't avoid this; it can only make the window small.
2. **The window is as big as the replication lag.** On a slow link the replica fell up to 2/3
   behind, and a failover lost up to 200k acknowledged writes in one run. Lag is the number to
   monitor, and the number that bounds data loss on failover.
3. **WAIT closed the window in every run, at a price.** Counting a write as acknowledged only
   after `WAIT 1` confirmed it: 0 lost, but 35% less throughput on a fast link (every write waits
   for a replica round trip). On the slow link it was **366x slower** (293 writes/s), because
   every write waits for the stream to catch up.
4. **But WAIT doesn't make replication synchronous, and doesn't prevent every loss.** The tests
   show the two gaps:
   - **Demo B:** when WAIT times out (returns 0), the write is *still applied* on the primary and
     visible to other clients. WAIT reports; it can't roll back.
   - **Demo C:** with two replicas, `WAIT 1` can be satisfied by replica A while replica B lags.
     If B is promoted, the "confirmed" write is lost anyway. Avoiding that needs the failover to
     pick the most up-to-date replica (Iteration 9's ranked election) or `WAIT` for *all* replicas.

**Caveat:** both processes share one machine, so the "plain" lag is unrealistically small. Across
real networks (0.1–1 ms within a datacenter, tens of ms across regions) the plain window grows
accordingly.
