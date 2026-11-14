# Iteration 3: expiry storm (Shardis vs real Redis)

**Setup:** Apple M4 (10 cores), macOS 26.6, Go 1.26.1, Redis 8.10.1 (persistence off), both at
`hz=10`, Shardis on the sharded engine. Reproduce: `bash docs/benchmarks/iter-03-expiry/storm.sh`.
Raw data per run in `run1/` and `run2/` (`timeline.csv`, `latency.csv`, `gctrace.txt`, `notes.txt`).

**Scenario:** load 95k `live:*` keys with no TTL, then ~995k `tmp:*` keys with `PX 5000` (16-byte
values). Nobody ever reads the `tmp:*` keys, so only **active expiry** can remove them. For 20 s
afterwards we record DBSIZE and RSS every 200 ms, and run back-to-back ~1 s GET benchmarks on the
`live:*` keys to see whether cleanup hurts other clients.

## Results (2 runs)

| | Shardis | Redis 8.10 |
|---|---|---|
| ~995k unread expired keys all deleted | **0.83–1.19 s** after the last deadline | 0.96–1.16 s |
| Live Go heap after the storm (gctrace) | **16 MB** | n/a |
| RSS before → 15 s after | 323–342 → **57–59 MB** | 131 → 24 MB |
| GET p99, windows overlapping the storm | 0.36–1.68 ms | 0.43–3.44 ms |
| GET p99, calm windows (median) | 0.38–0.62 ms | 0.40–0.59 ms |

Max latency was noisy on both servers in calm and storm windows alike (7–44 ms), so we read the
p99 column, not max.

## Before the compaction fix (same scenario, one run, kept for the record)

| | Shardis before | after (ADR 010) |
|---|---|---|
| Live Go heap after the storm | **102 MB** | 16 MB |
| RSS 15 s after | 381 MB | 57–59 MB |

## What this tells us

1. **Sampling works and keeps up.** With no reads at all, about a million expired keys were gone
   about a second after their deadline, on par with Redis using the same algorithm (20-key
   samples, repeat while >25% are expired).
2. **Clients barely notice.** During the storm our p99 peaked around 1–1.7 ms, against Redis's
   1.7–3.4 ms. Two reasons: we hold each shard lock for at most about 1 ms before moving on
   (ADR 008), so a client waits for one small group at a time, not a 25 ms cycle, and cleanup runs
   on its own goroutine while other cores keep serving. Redis's single thread does both jobs.
3. **The big finding: Go maps never shrink.** Our first version only freed a slot's map once it
   was completely empty. Here the surviving `live:*` keys share slots with the expired ones, so
   all 16,384 slot maps stayed sized for ~66 keys while holding ~6. The result was 102 MB of live
   heap for ~95k keys (about 12 MB of actual data). Our unit test missed it because there *every*
   key expired, which emptied the maps. A new test mixes survivors in.
   Fix: the background pass rebuilds a slot's maps once they're below 1/4 of their peak size.
4. **RSS follows the live heap, slowly.** Go returns freed memory to the OS gradually (its
   "scavenger"). Once the live heap really shrank, RSS dropped from ~330 MB to ~58 MB. Redis still
   uses ~2.4x less memory, as we saw in Iteration 2.

**Caveat:** single laptop, client and server sharing cores. Run-to-run noise is large for
latency, so treat the p99 ranges as "similar, with ours a bit flatter", not as a precise ranking.
