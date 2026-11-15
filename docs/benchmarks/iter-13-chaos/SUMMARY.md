# Iteration 13: chaos suite + consistency checker

**Setup:** Apple M4, macOS 26.6 (under heavy swap, see Iteration 11), Go 1.26.1.
- **Cluster:** 7 real `shardis` processes, run by `make chaos`:
  - 3 primaries and 3 replicas
  - 1 empty node for resharding
  - node timeout 2 s, AOF fsync every second, fault injection on
- **Workload:** 8 writers each own 64 keys and write 1, 2, 3, ... to them, one write at a time.
  8 readers read random keys. Each worker does about 1 operation per ms, through `pkg/client`
  (RetrySafe, 30 s retry budget).
- **Each scenario:** 3 s of load, then the fault, then recovery, then 3 s of load. After that,
  every key is read once more.

Every operation's start, end and result is recorded and checked (ADR 040). Reproduce with
`make chaos` (~5 min). Raw numbers: `report.csv`. Traces: `website/public/traces/*.json`.

| scenario | ops | errors | acked writes | lost (final state) | **rolled back** | stale reads | phantoms | longest stall | p99 |
|---|---:|---:|---:|---:|---:|---|---:|---:|---:|
| kill-primary | 105,351 | 0 | 50,833 | 0 | 0 | 0 / 54,518 | 0 | 3.6 s | 3.1 ms |
| kill-replica | 155,870 | 0 | 75,907 | 0 | 0 | 0 / 79,963 | 0 | 10 ms | 2.8 ms |
| partition-minority-primary | 162,860 | 0 | 79,401 | 0 | **3,480** | **200** / 83,459 | 0 | 1.4 s | 2.7 ms |
| flapping-network | 300,466 | 0 | 135,146 | 0 | 0 | 0 / 165,320 | 0 | 1.8 s | 4.7 ms |
| slow-node | 178,962 | 0 | 84,552 | 0 | 0 | 0 / 94,410 | 0 | 1.0 s | 4.0 ms |
| reshard-during-failover | 236,732 | 0 | 100,932 | 0 | 0 | 0 / 135,800 | 0 | 4.0 s | 5.5 ms |
| restart-all | 90,307 | 0 | 42,672 | 0 | 0 | 0 / 47,635 | 0 | 1.5 s | 3.9 ms |

How to read the columns:
- **lost:** an acknowledged write whose value isn't in the final state.
- **rolled back:** an acknowledged write that some later read didn't see. It includes "lost".
- **stale read:** a read that returned a lower value than one acknowledged before the read began.
- **phantom:** a value nobody wrote.
- **longest stall:** the longest any writer went without an acknowledged write.

## What this tells us

1. **The client hides every one of these failures: 0 errors in 1.2M operations.** What a failure
   costs is *time*. The longest stall equals the failover time (3.6–4.0 s) when a primary dies,
   the freeze when a node stalls (1.0 s), and the restart when everything restarts (1.5 s).
2. **Only a partition loses data, and only "rolled back" sees it.** In
   `partition-minority-primary`, the cut-off primary kept acknowledging writes for a moment
   (Iteration 10's split-brain window). **3,480 acknowledged writes vanished** when it rejoined
   as a replica, and **200 reads** went backwards.

   The final state looks perfect ("lost: 0"), because writers kept writing those keys after the
   heal and the newer values covered the gap. A checker that only compares the final state
   would have certified a split brain as harmless. This is why ADR 040 counts rolled-back
   writes.
3. **A crash loses nothing here, but that's a best case.**
   - `kill-primary` lost 0 writes. With replication over loopback taking microseconds, the
     window for in-flight writes is tiny. (Iteration 9 measured up to 146 lost per failover
     under a much heavier, unpaced load.)
   - `restart-all` lost 0 writes even with fsync every second. A `kill -9` leaves the OS page
     cache intact, and only a power cut loses that (Iteration 5's simulated power loss did).
4. **Losing a replica, or a 1 s stall, is invisible:** no PFAIL, no failover, just a latency bump.
5. **Resharding survives a crash in the middle.** When source primary `b` was killed mid-
   rebalance, its replica `e` took over, and `shardis-admin fix` finished the half-moved slot
   from the *current* owner (`e`). This needed a fix in this iteration: the first version tried
   to talk to the dead node. The second rebalance completed with 0 errors and nothing lost.

## Run-to-run variation, and an honest caveat

- **`flapping-network` failed over** (the trace shows `failover_won`), although every cut (1.5 s)
  was shorter than the node timeout (2 s). Our partitions drop messages inside the node (ADR
  031), so a PING sent during a cut is gone for good, and its PONG never comes. Real TCP would
  retransmit it once the link recovered. Our flapping is harsher than a real network blip.
- Whether that failover also loses writes depends on whether clients wrote to `a` in the gap:
  an earlier run had 103 stale reads, and this one had 0.
- Each scenario runs once per `make chaos`, with about 20 s of traffic. Run it repeatedly to
  catch rare races (ADR 042).

## Traces

Each scenario's trace (`website/public/traces/<scenario>.json`, 70 KB–3 MB) holds four things:
- every node's events: gossip, PFAIL/FAIL, failure reports, votes, `failover_won`, `demoted`,
  slot moves, replication links
- the chaos tool's own actions: kill, pause, partition, heal, fix
- a 100 ms timeline of client operations (ok/failed)
- the report above

For example, `kill-primary`:
1. `a` is killed at 3.0 s.
2. Five nodes report it failing at 5.5 s.
3. `d` wins the election at 6.25 s (epoch 4, 2 votes).
4. `a` restarts at 7.3 s.

The website will replay these.
