# Iteration 4: eviction policies under a skewed workload

**Setup:** Apple M4 (10 cores), macOS 26.6, Go 1.26.1, Redis 8.10.1. Workload from
`shardis-loadgen` in "cache-aside" mode (GET; on a miss, SET): 1M distinct keys, Zipf θ = 0.99
(YCSB's default skew: the top 10% of keys get ~70% of requests), 100-byte values, 50 clients,
25 s warmup then 20 s measured. Shardis `maxmemory` 32 MB, roughly 10% of the keys.
Redis got the `maxmemory` (18.4 MB) that holds about as many keys, so hit rates compare the
*algorithms*, not the memory overhead.
Reproduce: `bash docs/benchmarks/iter-04-eviction/run.sh`, then `python3 .../chart.py`.
Raw: `raw.csv`, `micro-get.txt`.

![Hit rate by eviction policy](hit-rate.svg)

## Hit rate

| Setup | Hit rate | Keys in memory | Memory used |
|---|---:|---:|---:|
| Shardis `allkeys-random` | 73.7% | 114k | 32.0 MB |
| Shardis `allkeys-lru`, 5 samples | **77.2%** | 104k | 32.0 MB |
| Shardis `allkeys-lru`, 10 samples | 77.3% | 103k | 32.0 MB |
| Shardis exact LRU, same 32 MB | 74.6% | 77k | 32.0 MB |
| Shardis exact LRU, same key count | 77.6% | 106k | 42.6 MB |
| Redis `allkeys-random` | 73.8% | 101k | 18.3 MB |
| Redis `allkeys-lru`, 5 samples | 77.0% | 102k | 18.3 MB |
| Redis `allkeys-lru`, 10 samples | 77.1% | 102k | 18.3 MB |

## Cost of exact LRU on the read path (engine only, `BenchmarkGetParallel`, median ns per GET)

| cores | approximate LRU | exact LRU |
|---:|---:|---:|
| 1  | ~520 | ~1,100 |
| 4  | ~127 | ~495 |
| 10 | **~105 (≈10M GET/s)** | ~560 (≈1.8M GET/s) |

## What this tells us

1. **LRU beats random, by less than you'd think.** 77.2% vs 73.7% hit rate. Under heavy skew
   the hottest keys are requested so often that even random eviction rarely keeps them out for
   long. The useful framing is **misses, since every miss is a database query**: 26.3% → 22.8%
   means **13% less load on the database** for the same memory.
2. **Approximate LRU is within 0.4 points of exact LRU.** Sampling 5 keys and keeping a pool of
   the best 16 candidates finds a near-ideal victim almost every time. Going to 10 samples bought
   0.05 points.
3. **For the same memory, exact LRU is worse.** Its list node and index cost ~107 bytes per key
   (~30% more here), so 32 MB holds 77k keys instead of 104k and the hit rate drops to 74.6%,
   barely above random. Exact LRU only catches up when given 33% more memory (42.6 MB).
4. **And it's 5x slower to read.** Every GET moves a node in one shared list behind one mutex.
   Approximate LRU writes a per-key timestamp at most once a second, so reads scale with cores
   (10M GET/s at 10 cores). Exact LRU gets *slower* from 4 to 10 cores as they queue on the lock.
   Over the network both did ~100–110k GET/s, because there the network is the bottleneck.
5. **We match Redis's algorithm quality.** At the same key count, Redis and Shardis land within
   0.2–0.3 points of each other for every policy.
6. **We still need ~1.7x Redis's memory per key**: 322 vs 188 bytes for a ~24-byte key and a
   100-byte value (Go string and slice headers, map slots, per-slot maps).
7. Odd one out: `allkeys-random` held 10% more keys than LRU in the same 32 MB. Random victims
   are spread evenly over slots, while LRU victims cluster in slots whose keys are all old, so
   LRU leaves more half-empty slot maps (which still cost memory until compaction). Even with
   the extra keys, random lost.

**Caveats:** a single laptop, one run per setup (repeat runs varied by a few tenths of a point,
smaller than the gaps above). Hit rates depend strongly on skew and on the fraction of keys that
fit: with uniform traffic every policy converges to "fraction of keys in memory".
