# 013: Evict synchronously, before a write takes its locks
- Context: With a memory limit, someone has to free space when we're full. We had to decide who
  does it, and when, given that commands hold shard locks and eviction has to touch whichever
  shard holds the best victim.
- Options considered:
  1. Synchronous, in the write path: before a command that may grow memory runs, evict until
     we're under the limit, *then* let it take its locks.
  2. A background goroutine that keeps memory under the limit. Writes never wait for eviction,
     but memory overshoots between rounds, and a burst of writes can blow far past the limit.
  3. Both: background normally, synchronous when far over. Smoother latency, more moving parts.
- Decision: Option 1. Each command is tagged in the command table with `write` and/or `denyoom`
  ("refuse when out of memory"), like Redis. Before a `denyoom` command (SET, MSET, INCR…), the
  server calls `Evictor.Reserve()`:
  - **Under the limit (the common case):** two atomic loads, done.
  - **`noeviction`, or nothing left to evict:** reply `-OOM command not allowed when used memory >
    'maxmemory'.` (Redis's exact text). Reads and DEL keep working, since deleting is how you
    get out of an OOM.
  - **Otherwise:** evict until under the limit. One eviction at a time (a mutex guards the
    candidate pool). Each eviction locks only the one shard group it touches.
- Why:
  - **No deadlock by construction.** Eviction picks victims in any shard. If a command evicted
    while holding its own shard locks, two commands could each hold a lock the other's eviction
    needs. Evicting *before* taking command locks means eviction only ever holds one group lock
    at a time, never alongside a command's locks.
  - The limit holds tightly: in tests, memory stayed within one write (plus one possible map
    doubling, ≤ 64 KB) of the limit across 30k writes for every policy, and under 8 concurrent
    writers (`TestEvictingPoliciesStayUnderTheLimit`, `TestConcurrentEviction`).
  - DEL, EXPIRE and reads aren't blocked by a full cache.
- Tradeoffs we accept:
  - Writes pay for eviction when the cache is full: a SET may evict a key first, costing roughly
    one extra lock acquisition and a 5-key sample.
  - Under memory pressure, writers serialize on the eviction mutex.
  - Concurrent writers can each land one write after their Reserve, so with N writers memory can
    briefly exceed the limit by about N writes.
  - A command that frees memory but is tagged `denyoom` (SET with a smaller value) still triggers
    eviction first. Redis has the same quirk.
- What real Redis does: The same idea. Before running a command, `performEvictions` frees memory
  if over the limit, and `denyoom` commands get `-OOM` if it can't. Redis 6+ also caps how long
  eviction may run per command and finishes in the background (option 3) to bound write latency.
  We don't have that time cap yet.
- At bigger scale I'd: Add the time cap plus background finishing (option 3). Start evicting a
  little *before* the limit (a low-water mark), so bursts don't hit the synchronous path. Evict in
  batches per shard to amortize locking. Emit metrics for eviction rate and time spent evicting,
  because rising eviction is the earliest sign a cache is undersized.
