# Iteration 2: executor vs sharded locks vs real Redis

**Setup:** Apple M4 (10 cores), macOS 26.6, Go 1.26.1, Redis 8.10.1 (persistence off). Client and
servers share the laptop and talk over loopback. `redis-benchmark -t set,get -r 100000` (random keys
out of 100k), single-threaded client, 3 reps with the server order rotated each rep.
Reproduce: `bash docs/benchmarks/iter-02-concurrency/run.sh` and `gc.sh`.
Raw: `raw.csv`, `micro.txt`, `memory.csv`, `gc-pauses.txt`, `gctrace-*.txt`.

## 1. Over the network (median of 3 reps, req/s, p99 in ms)

| clients | pipeline | test | executor | sharded | Redis 8.10 |
|---:|---:|---|---:|---:|---:|
| 1   | 1  | SET | 32.4k · 0.047 | 36.7k · 0.039 | 38.4k · 0.031 |
| 1   | 1  | GET | 32.9k · 0.047 | 37.0k · 0.039 | 38.6k · 0.031 |
| 50  | 1  | SET | 121.6k · 0.415 | 128.8k · 0.335 | 141.1k · 0.231 |
| 50  | 1  | GET | 124.9k · 0.383 | 126.9k · 0.367 | 141.4k · 0.223 |
| 200 | 1  | SET | 114.5k · 2.231 | 123.1k · 1.399 | 138.9k · 1.167 |
| 200 | 1  | GET | 115.3k · 1.703 | 115.3k · 1.975 | 140.7k · 0.807 |
| 50  | 16 | SET | 886k · 1.671 | **1.46M** · 0.871 | 1.01M · 1.183 |
| 50  | 16 | GET | 919k · 1.391 | **1.56M** · 0.767 | 1.36M · 0.767 |

## 2. Engine only, no network (`go test -bench BenchmarkDo -cpu 1,4,10`, median ns per op, lower is better)

| benchmark | cores | executor | sharded |
|---|---:|---:|---:|
| random key out of 100k (`DoSpread`) | 1  | 760 | 528 |
|                                      | 4  | 640 | 107 |
|                                      | 10 | 553 (≈1.8M ops/s) | **90 (≈11M ops/s)** |
| one hot key (`DoHotKey`)             | 1  | 448 | 195 |
|                                      | 10 | 478 | 368 |

## 3. Memory and GC with ~950k keys (16-byte keys, 16-byte values)

| | Go heap bytes/key | RSS bytes/key | GET p99 / max (ms) | GC stop-the-world pauses |
|---|---:|---:|---|---|
| Shardis executor | 108 | 249 | 0.407 / 5.4 | 23 GCs, max 0.14 ms |
| Shardis sharded  | 108 | 252 | 0.327 / 1.0 | 27 GCs, max 0.26 ms |
| Redis 8.10       | n/a  | 85 (`used_memory`: 79) | 0.367 / 0.9 | n/a (no GC) |

## What this tells us

1. **Without pipelining, everyone is network-bound.** All three land at 115–140k req/s. Each request
   costs two syscalls and a trip through loopback, and that dwarfs the ~100–500 ns of store work.
   Redis stays ~10% ahead because its single event loop handles many ready sockets per wakeup,
   while we wake one goroutine per request.
2. **The executor's hand-off is a real cost.** At 1 client, p50 goes from 0.023 to 0.031 ms. That's
   two extra goroutine wake-ups per command (send to the executor, wake the caller back up). When
   pipelined, it caps the executor at ~0.9M req/s, below even Redis.
   *Redis doesn't pay this:* its one thread does the network I/O *and* runs the command. Copying
   the "single owner" idea into Go, where each connection has its own goroutine, adds a hand-off
   Redis never has.
3. **Sharded scales with cores.** Engine only: 11M vs 1.8M ops/s at 10 cores (6x). Over the network
   with pipelining it beats real Redis (1.46–1.56M vs 1.01–1.36M), because Redis runs commands on one
   core and we use several. Even the hot-key worst case (all cores on one lock) is faster than the executor.
4. **Memory: we use ~3x Redis per key (RSS).** Go heap is 108 B/key for 32 B of payload: map
   bucket space, the string header (16 B), the slice header (24 B) and allocator rounding. RSS is
   ~2.3x the live heap because Go's GC lets the heap grow to about 2x live data before collecting
   (`GOGC=100`). Redis packs small strings into one allocation (embstr).
5. **GC is NOT the cause of the latency spikes from Iteration 1.** With ~1M pointer-heavy keys,
   stop-the-world pauses never exceeded 0.26 ms (a forced full GC took 8 ms of wall time, but almost
   all of it runs concurrently with the program). The max-latency outliers in section 1 (Shardis
   up to 49–69 ms, Redis up to 22 ms) are mostly the shared laptop: background apps and macOS
   scheduling. Ours are larger and not yet explained. Next suspects: goroutine scheduling and
   GC mark-assist (the GC borrowing a busy goroutine to help mark). A Go execution trace
   (`go tool trace`) would show it.

**Caveat:** single machine, client and server competing for the same cores. The relative order is
stable across reps. The absolute numbers would be higher on a dedicated server.
