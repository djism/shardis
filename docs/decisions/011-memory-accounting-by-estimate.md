# 011: Enforce maxmemory against our own memory estimate, not the Go heap
- Context: `--maxmemory` needs an answer to "how much memory does the data use right now?" on every
  write that might grow it. It has to be cheap (it's on the write path), stable (it shouldn't jump
  around), and close to what the process really uses (that's what gets a container OOM-killed).
- Options considered:
  1. Our own running estimate: every write and delete adds or subtracts what that key costs,
     using a model of how Go lays out the data.
  2. Ask the Go runtime: `runtime.ReadMemStats` (pauses the whole program) or `runtime/metrics`
     (no pause). Both report the heap including garbage not yet collected, so the number sawtooths
     with each GC cycle.
  3. Ask the OS for RSS: what really matters to Kubernetes, but it lags (Go returns memory to the
     OS slowly) and it's expensive or unavailable on some platforms.
- Decision: Option 1, a model built from measurements. `INFO memory` shows it next to the Go
  runtime's numbers (and RSS on Linux), so the gap is always visible. The model has two parts:
  - **Per key:** the key string and the value bytes, rounded up to Go's allocator size classes
    (a 100-byte value really takes 112). Exact LRU adds 107 bytes per key.
  - **Per slot map:** Go map capacity is a power of two (at least 8), and maps never shrink, so
    capacity follows from the most entries the map has held. We already track that peak for
    compaction (ADR 010). Map cost = header + slot array (57 bytes per slot, or 25 for the TTL
    index), with the array also rounded to a size class.
- Why:
  - Accurate: within **1–3% of real heap growth** for 20k to 1M keys, 16 B to 1 KB values, with
    and without TTLs, and with exact LRU (`TestMemoryEstimateMatchesHeap`,
    `TestMemoryEstimateAtOtherSizes`).
  - Never drifts: after 30k random sets, deletes, TTL changes, expiries and compactions, the
    running total equals a from-scratch recount *exactly* (`TestMemoryAccountingNeverDrifts`).
  - Cheap: one atomic add per write. Checking the limit is two atomic loads.
  - How we got there: the first version used a flat "+84 bytes per key", fitted at 200k keys. It
    was **off by 2x at 20k keys**, because there the 16,384 mostly-empty slot maps cost more than
    the keys. Modeling maps explicitly fixed it. A second round found that the map's slot array
    also gets rounded to a size class (7,296 → 8,192 bytes), and that Go pads `struct{key;
    struct{}}` from 16 to 24 bytes.
- Tradeoffs we accept:
  - **A memory floor of about 9 MB** (up to about 14 MB with TTLs): the 16,384 slot maps cost that
    once keys spread over every slot. A `maxmemory` below 32 MB logs a warning. Below about 10 MB,
    eviction has to empty whole slots to free their maps, so it effectively evicts at random.
  - The model is tied to Go's current map and allocator layout. A Go upgrade can make it drift.
    The calibration tests (`make test-memory`, also run in CI) will catch that.
  - It doesn't count connection buffers, goroutine stacks or uncollected garbage. Real Go heap
    runs above the estimate between GCs (72 MB heap vs 60 MB estimate in one run), and RSS above
    that. Leave headroom: set a container's memory limit well above `maxmemory`.
  - One write can push memory slightly past the limit (eviction happens before the write), and
    more if that write doubles a slot map. Redis has the same "one command over" behavior.
- What real Redis does: Redis wraps every allocation (`zmalloc`) and keeps an exact counter of
  bytes requested from its allocator (`used_memory`). That's closer to the truth than our model,
  but it also excludes allocator fragmentation, which shows up only in RSS. Redis reports both,
  and so do we. Redis's maxmemory also has a floor in cluster mode (per-slot dictionaries), just
  much smaller in C.
- At bigger scale I'd: Move values into large off-heap slabs (one allocation holding many values,
  like memcached's slab allocator), so we count bytes exactly instead of modeling, and the GC has
  far fewer objects to scan. Also set `GOMEMLIMIT` just above `maxmemory` plus headroom, so the
  Go GC works harder as the process nears its container limit instead of getting OOM-killed.
