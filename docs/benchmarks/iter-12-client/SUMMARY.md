# Iteration 12: what the application sees through our smart client

**Setup:** Apple M4, macOS 26.6 (under heavy swap, see Iteration 11), Go 1.26.1.
- **Cluster:** 3 primaries + 3 replicas + 1 empty node as real `shardis` processes, node timeout 2 s.
- **Load:** 32 writers INCR 2000 counters through `pkg/client` (500 ms per-attempt timeout,
  30 s retry budget).
- **What happens during a run** (about 25 s of traffic):
  1. **Pause:** primary `b` is frozen for 1.5 s (SIGSTOP/SIGCONT), like a long GC pause or a
     network hiccup. That's shorter than the node timeout, so there's no failover.
  2. **Crash:** primary `a` is killed (`kill -9`), and its replica `d` takes over.
  3. **Restart:** `a` comes back and rejoins as `d`'s replica.
  4. **Rebalance:** node `g` joins and the cluster is rebalanced onto it (4096 slots).
- **Afterwards,** every counter is compared with what the client reported.

5 runs per retry policy. Reproduce: `bash docs/benchmarks/iter-12-client/run.sh` (~8 min).
Raw: `raw.csv`.

| retry policy | errors the app saw | unknown outcomes (pause / total) | INCRs applied twice | acked INCRs lost | slowest request |
|---|---:|---|---:|---:|---|
| **RetrySafe** (default) | **0** | 32 / 48 every run | **0** | 0 | 3.5–3.8 s |
| **RetryAll** (go-redis's default behaviour) | **0** | 0 / 0 | **32 every run** | 0, 0, 1, 1, 9 | 3.4–3.8 s |
| **RetryNever** | **0** | 32–36 / 48–52 | 0 | 0, 0, 0, 0, 1 | 3.4–3.9 s |

Per run: ~1.7M INCRs acknowledged, failover 2.9–3.4 s, rebalance 13–15 s.

## What this tells us

1. **Failover and resharding are invisible to the application.** Through all of it there were
   0 errors in all 15 runs: MOVED, ASK, CLUSTERDOWN, READONLY and refused connections were all
   retried within the budget. The cost is latency. The slowest request took about as long as the
   failover (3.4–3.9 s), because it waited the failover out instead of failing.
2. **The pause is where retry policy matters,** and it's a clean experiment:
   - 32 writers means exactly 32 INCRs were stuck on the frozen node. Each client timed out at
     500 ms.
   - **RetryAll** resent all 32 on new connections. Then the frozen node woke up and *also* ran
     the originals, still waiting in its socket buffers. **32 of 32 were applied twice, in
     every run.** The application saw zero errors and wrong counters.
   - **RetrySafe** returned those 32 as `ErrUnknownOutcome` and applied nothing twice.
     Everything that was safe to resend (reads, SETs, every definite failure) was still retried
     invisibly.

   That's the whole argument of ADR 036 in one number.
3. **A crash produces fewer duplicates than a pause.** At the crash (~16 in-flight INCRs per
   run), RetryAll's resends didn't double-count. An in-flight INCR on a node that dies usually
   never reached the replica, so resending it on the new primary is the *only* copy. Duplicates
   come from nodes that are slow, not dead.
4. **No client fixes asynchronous replication.** RetryAll lost up to 9 acknowledged INCRs in a
   run (RetryNever: 1): writes the old primary acknowledged that never reached its replica
   before the `kill -9` (Iterations 6 and 9). The client can only hide *unavailability*, not
   undo lost data.
5. **RetryNever looks like RetrySafe here** because every command in this workload is INCR,
   which RetrySafe never resends after an unknown outcome either. The difference shows up for
   reads and SETs: RetryNever would fail those too.

## Replica reads

`TestReplicaReadStaleness`: with `ReadFromReplicas`, a read right after a write to the same key
returned the old value **2.6–6.7% of the time** (3 runs × 2000), on loopback, with replication
far under a millisecond (ADR 039).

## How go-redis behaves (from its source, v9.23)

`MaxRetries` defaults to 3. Its `shouldRetry` (error.go) retries on `io.EOF` and read timeouts for
every command not marked `NoRetry`. That's at-least-once: our RetryAll row. We didn't run
go-redis in this tool: it's approved as a test-only dependency, and the crash tool is a binary.
