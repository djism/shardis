# Iteration 1: PING baseline (Shardis vs real Redis)

**Setup:** Apple M4 (10 cores), macOS 26.6, Go 1.26.1, Redis 8.10.1 with persistence off.
Client and servers on the same machine over loopback. `redis-benchmark -t ping -n 300000`.
Reproduce: `bash docs/benchmarks/iter-01-ping/run.sh`. Raw data: `raw-run1.csv`, `raw-run2.csv`.

## Results (PING_MBULK, two runs)

| clients | pipeline | Shardis req/s | Redis req/s | Shardis p50 / p99 (ms) | Redis p50 / p99 (ms) |
|---:|---:|---:|---:|---|---|
| 1  | 1  | 35.3k – 37.3k | 39.0k – 39.2k | 0.023 / 0.047–0.063 | 0.023 / 0.031–0.039 |
| 50 | 1  | 132.7k – 133.1k | 142.5k – 143.3k | 0.199 / 0.287 | 0.183 / 0.207–0.223 |
| 50 | 16 | 1.71M – 1.83M | 1.85M – 1.86M | 0.231–0.239 / 0.495–0.727 | 0.231 / 0.367–0.455 |

Parser microbenchmark (`make bench`): **181 ns and 4 allocations per 3-argument command**.

## What this tells us

- **Pipelining is the big lever: ~13x throughput** (133k → 1.8M req/s) for the same 50 clients.
  Without it, each request pays a full network round trip plus two syscalls (read + write).
  With 16 commands per batch, those costs are split 16 ways. That's ADR 003 at work:
  we flush once per batch, not once per reply.
- **One client is round-trip bound** (~37k req/s ≈ 27 µs per request), for both servers.
  The server isn't busy; it's waiting for the next request.
- **We're ~7% behind Redis at 50 clients, and p99 is ~0.08 ms worse.** Expected: one goroutine
  per connection means the Go scheduler wakes 50 goroutines, while Redis runs one event loop
  that handles all sockets ready at once with no context switches.
- **Max latency outliers:** Shardis had occasional single requests at 6–55 ms (Redis max ≤ 3.2 ms).
  These are rare (p99.99 was under 1 ms) and are probably the Go garbage collector or goroutine
  scheduling. Not investigated yet; worth a GC trace (`GODEBUG=gctrace=1`) once there's real
  data in memory (Iteration 2+).
- **Caveat:** at 1.8M req/s the single-threaded `redis-benchmark` client is probably the
  bottleneck, so the pipelined row measures the benchmark tool almost as much as the servers.
