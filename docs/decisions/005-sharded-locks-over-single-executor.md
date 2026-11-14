# 005: Sharded locks (256) instead of a single executor goroutine
- Context: Every command must be atomic. No client may see another command half-done: INCR never
  loses an update, SET NX has one winner, MSET is all-or-nothing. Each connection runs on its own
  goroutine, so something has to stop commands on different goroutines from interleaving.
- Options considered:
  1. Single executor goroutine owns all data (Redis's model): atomic by construction, no locks,
     but every command is handed off to it and back.
  2. Keyspace split into 256 shards, each with a mutex. Commands run on their own goroutine and
     lock the shards their keys live in, always in ascending order.
  3. One global mutex: simplest locking, but all cores fight over one lock (the "hot key" worst
     case for every key). Not benchmarked; option 2's hot-key number is its upper bound.
- Decision: Option 2, sharded locks, as the default. The executor stays behind `--engine=executor`
  for comparison. Both engines implement the same `Engine.Do(keys, fn)` interface, so commands are
  written once and the benchmark compares only the concurrency strategy.
- Why ([benchmarks](../benchmarks/iter-02-concurrency/SUMMARY.md)):
  - Faster in every scenario. Without pipelining, +5–7% throughput and lower p99 (both are
    network-bound). With pipelining, 1.46–1.56M vs 0.89–0.92M req/s, faster than Redis 8 itself
    (1.01–1.36M). Engine alone at 10 cores: 11M vs 1.8M ops/s.
  - The executor pays a hand-off Redis doesn't: Redis's one thread does I/O *and* execution. In
    Go, with I/O on per-connection goroutines, "one owner" means two goroutine wake-ups per command
    (+8 µs at p50 for a single client).
  - Atomicity is just as strong, and enforced rather than trusted:
    - A command must declare its keys to `Do`. The `Tx` it gets panics if it touches any other key,
      so a forgotten key fails loudly in tests instead of racing in production.
    - Locks are always taken in ascending shard order, which makes deadlock impossible.
      `TestNoDeadlockOnOverlappingKeys` runs 64k random overlapping multi-key operations.
    - The same atomicity tests (INCR x100k, SET NX one winner, MSET never torn, DEL multi) pass
      on both engines under `-race`, both in-process and over TCP.
- Tradeoffs we accept:
  - Harder to reason about than "one goroutine does everything". Every new multi-key feature has
    to declare its keys up front.
  - Whole-keyspace commands (KEYS, SCAN, DBSIZE, FLUSHALL) take all 256 locks and stall every client
    while they run. Same as Redis for KEYS. Worse than Redis for DBSIZE, which Redis answers in O(1).
  - Plain mutexes, not read/write locks. GETs on the same shard wait for each other. With 256
    shards and ~100 ns critical sections, contention was too low to measure, so RWMutex wasn't worth
    its extra overhead yet.
  - 128 bytes of padding per lock (32 KB total) so locks don't share CPU cache lines.
- What real Redis does: Option 1, a single thread that runs every command, plus (since Redis 6)
  optional I/O threads for socket reads and writes. It's fast because there's no hand-off and no
  locking. It's atomic because nothing runs in parallel. We differ, on measured evidence, because
  Go's goroutine-per-connection design makes the single-owner model cost a hand-off. Our
  atomicity guarantee to clients is the same.
- Future features check: In Redis Cluster, multi-key commands, MULTI/EXEC and Lua scripts must
  use keys from **one hash slot** (otherwise `-CROSSSLOT`). One slot means exactly one of our
  shard locks, so the hardest case for sharded locking (long transactions across many keys) is
  ruled out by the cluster spec itself. WATCH/MULTI can lock the slot's shard for the EXEC.
- At bigger scale I'd: Look at a shared-nothing, thread-per-core design (like Dragonfly or
  ScyllaDB): pin each shard to one core and route connections to the core that owns their keys,
  with no locks and no cache-line bouncing. And/or have each connection goroutine batch a whole
  pipeline into one `Do` so lock and hand-off costs are paid once per batch.
