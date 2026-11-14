# 012: Approximate (sampled) LRU instead of an exact LRU list
- Context: When memory is full and the policy is `allkeys-lru`, we have to pick a victim: ideally
  the key unused for the longest time. Exact LRU keeps every key in a linked list ordered by last
  use. That's what the textbooks teach, but it means every read updates shared state.
- Options considered:
  1. Exact LRU: a doubly linked list plus a key → node index. On every access, move the node to
     the front. Evict from the back.
  2. Approximate LRU (Redis's design): each key stores a last-used timestamp at 1-second
     resolution. To evict, sample N random keys (default 5) into a pool of the 16 best candidates
     seen so far, and evict the oldest in the pool.
  3. Random: no bookkeeping at all.
- Decision: Option 2 for `allkeys-lru`. We also built option 1 as an experimental policy,
  `allkeys-lru-exact` (startup only), purely to measure what we'd be giving up.
- Why ([benchmarks](../benchmarks/iter-04-eviction/SUMMARY.md)), Zipf 0.99 workload, ~10% of keys fit:
  - **Quality:** approximate LRU hit 77.2% against exact LRU's 77.6% at the same key count. That's
    0.4 points. Random got 73.7%. Doubling samples to 10 added 0.05 points.
  - **Memory:** exact LRU costs ~107 bytes per key for the list node and its index entry. With the
    same 32 MB, it holds 26% fewer keys and gets **74.6%: worse than approximate LRU, barely above
    random**. Its bookkeeping crowds out the data it's supposed to manage.
  - **Speed:** every GET under exact LRU takes one global mutex to move a list node. At 10 cores:
    ~560 ns per GET vs ~105 ns (5x slower), and adding cores made it *worse* (4 → 10 cores).
    Approximate LRU writes a per-key timestamp at most once a second, inside the shard lock the
    GET already holds.
  - Matches Redis's results within 0.2–0.3 points for every policy at the same key count.
- Tradeoffs we accept:
  - Occasionally evicts a key that isn't the oldest. A sample can miss the true oldest key, and
    a 1-second clock can't separate keys used within the same second.
  - Each key carries a 4-byte timestamp (its entry struct grew from 32 to 40 bytes with padding).
  - A hot key's timestamp update is a map write (once per second per key), not a free read.
  - The pool can hold stale candidates (keys touched since they were sampled). We re-check that
    the key still exists before evicting, but not that it's still idle, as Redis does.
- What real Redis does: Exactly option 2: a 24-bit LRU clock per object at 1-second resolution,
  `maxmemory-samples` default 5, and a 16-entry eviction pool. Redis's docs show the same result
  we measured: with 10 samples it's very close to true LRU. Redis also offers LFU
  (`allkeys-lfu`), which counts accesses with a logarithmic counter that decays over time. That
  often beats LRU on skewed traffic. Not built yet.
- At bigger scale I'd: Add LFU (Redis-style decaying counters) and measure it on this same
  workload, since under Zipf traffic "how often" usually beats "how recently". Consider
  admission policies (TinyLFU, as in Caffeine) that refuse to cache one-hit wonders at all.
  Tune per tenant, because the right policy depends on the access pattern, and the loadgen makes
  that cheap to test.
