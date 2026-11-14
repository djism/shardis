# 010: Compact mostly-empty slot maps in the background pass
- Context: Go maps never shrink. A map that grew to hold 66 keys keeps the memory for 66 after 60 of
  them are deleted. Our first version freed a slot's map only once it was completely empty. In
  the expiry-storm benchmark, about 95k surviving keys shared slots with about 995k expired ones,
  so every one of the 16,384 slot maps stayed at peak size. The result was **102 MB of live heap
  for about 12 MB of data**, and RSS stuck at 381 MB. A unit test where *all* keys expired had
  missed it, because empty maps were freed.
- Options considered:
  1. Never shrink, only free empty maps: zero cost, but memory never comes back after a mass
     delete or expiry while some keys survive.
  2. Shrink on delete: whenever a slot map drops below 1/4 of its peak, rebuild it right away.
     Tried this first. Memory came back, but a slot being drained by a storm got rebuilt several
     times mid-storm. In that one run, cleanup finished ~1.6 s after the deadline (vs ~0.6 s), and
     GET p99 rose to 1.4–1.9 ms.
  3. Compact in the background pass: deletes only free *empty* maps. The active-expiry loop, which
     already visits every slot group 10 times a second, rebuilds a slot's maps after it has
     finished with that slot, if the maps are below 1/4 of their peak (and the peak was at least 16).
- Decision: Option 3.
- Why ([benchmarks](../benchmarks/iter-03-expiry/SUMMARY.md)):
  - Memory: live heap after the storm went from 102 MB to **16 MB**, and RSS from 381 MB to
    **57–59 MB**.
  - Cleanup speed stayed on par with Redis (0.8–1.2 s after the last deadline).
  - A slot is rebuilt once after it settles, not repeatedly while it drains. Covered by two tests:
    survivors mixed with expired keys (23.3 → 9.0 MB) and a mass DEL with no TTLs (same numbers).
  - Cost is O(len) per rebuild, and a rebuild needs at least 3/4·peak deletes first, so it averages
    out to O(1) per delete. It runs under the same 1 ms per-lock budget as expiry.
- Tradeoffs we accept:
  - Memory comes back with a delay of up to one cycle (100 ms at hz=10), plus however long Go's
    scavenger takes to hand pages back to the OS.
  - Two int32 "peak" counters per slot (128 KB total).
  - The 1/4 and 16 thresholds are judgment calls, not tuned: small maps aren't worth rebuilding,
    and rebuilding at 1/2 would risk shrink/grow flapping.
- What real Redis does: Redis's dictionaries shrink too. When a dict is less than ~10% full, Redis
  resizes it, using incremental rehashing (moving a few buckets per operation) so a big resize
  never pauses the server. Same goal. We do the rebuild in one step, because each of our slot maps
  is small (it's 1/16384 of the keyspace).
- At bigger scale I'd: Track compaction in metrics (maps rebuilt, bytes reclaimed). Consider
  `debug.FreeOSMemory` or a lower `GOGC` / `GOMEMLIMIT` so RSS follows the live heap more closely
  in memory-tight containers. Kubernetes memory limits look at RSS, not at Go's heap.
