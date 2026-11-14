# Iteration 5: durability vs throughput per fsync policy

**Setup:** Apple M4 (10 cores), macOS 26.6 (APFS, internal SSD), Go 1.26.1, Redis 8.10.1. On macOS,
Go's `File.Sync` uses `F_FULLFSYNC`, which forces the drive to flush its own cache too (true
durability, and expensive). Reproduce: `bash docs/benchmarks/iter-05-persistence/run.sh`. Raw:
`crash.csv` (70 crash runs), `throughput.csv`.

## The table

| AOF policy | lost after a **process crash** (kill -9) | lost after a **power cut** (simulated) | SET throughput, 50 clients | with pipelining (-P 16) |
|---|---|---|---:|---:|
| off            | **100%** (all 2.3M acked writes, 10/10 runs) | n/a | 129k/s | 1.46M/s |
| `no`           | **0** (0 of 2.3M, 10 runs) | **100%** (10/10 runs) | 112k/s | 1.02M/s |
| `everysec`     | **0** (0 of 2.1M, 10 runs) | **0.32–0.91 s** of writes per crash (10/10 runs) | 115k/s | 1.13M/s |
| `always`       | **0** | **0** (10 runs) | 5.9k/s | 82k/s |
| Redis `everysec` | | | 141k/s | 652k/s |
| Redis `always` | | | 5.9k/s | 80k/s |

"Lost" counts writes the client had already received `OK` for. Crash test: 16 writers sending
numbered SETs one at a time, crash after 1.5–3 s of load, restart on the same files, check every
acknowledged key.

## What this tells us

1. **A process crash loses nothing with any AOF policy, even `no`.** Replies wait until their
   write has been handed to the kernel with `write()` (ADR 015), and a process crash doesn't take
   the kernel's copy with it. Across 30 crash runs with the AOF on, **0 of ~6.4M acknowledged
   writes** were lost. Without an AOF, every one was.
2. **fsync only matters when the machine dies.** In the simulated power cut, data not yet fsynced
   is gone:
   - `always` lost nothing.
   - `everysec` lost 0.3–0.9 s of writes each time, within its "up to ~1 s" promise.
   - `no` lost everything written during the run, because nothing was ever fsynced in those few
     seconds. A real OS flushes dirty data eventually (Linux within ~30 s), so in practice `no`
     means "up to ~30 s".
3. **`always` costs ~20x throughput** (5.9k vs 115k writes/s), and that's *with* group commit.
   Each `F_FULLFSYNC` takes ~8 ms here, so throughput is about (fsyncs per second) × (writes
   per fsync). With 50 clients waiting, one fsync covers ~50 writes. Pipelining puts more writes
   in each batch: 82k/s. Redis lands on the same numbers, so the drive sets the limit, not the
   software.
4. **With pipelining we write faster than Redis** (1.0–1.1M vs 650k/s at `everysec`), because
   several cores run commands and group commit amortizes the log writes. Without pipelining we're
   ~18% behind (115k vs 141k), the same per-request overhead as in earlier iterations, plus one
   goroutine hand-off to the writer per reply.
5. **Known gap: tail latency with the AOF on.** At `everysec`, our SET p99 was 9 ms vs Redis's
   1.5 ms. At `no` it was 20 ms for us and 10 ms for Redis, so the OS's own flushing hurts both.
   Our hypothesis for `everysec`: a `write()` can block while an fsync of the same file is in
   progress. Redis handles this by postponing writes (for up to 2 s) while its background fsync
   runs, so the event loop never blocks on it. We don't do that yet. Single runs, so the p99s are
   noisy, but the gap is consistent.

**Caveat:** the power-cut simulation is a model. It assumes *nothing* after the last fsync
survives (worst case). A real power cut can leave partial data and torn records, which our
loader handles by truncating the last record (tested separately).
