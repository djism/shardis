# 008: Lazy expiry plus Redis-style sampled active expiry
- Context: Keys can have a TTL. A key nobody reads after it expires still takes memory, so reads
  alone can't clean up. But scanning every key to find expired ones costs O(n) per pass, which
  doesn't work with millions of keys. We need cleanup that's cheap when nothing is expiring and
  keeps up when a million keys expire at once, without stalling clients.
- Options considered:
  1. Lazy on read + an active loop that randomly samples keys with TTLs (Redis's design).
  2. A min-heap ordered by deadline: always knows the next key to die, so it deletes almost
     exactly on time. Costs ~40 B/key, O(log n) per TTL change, and stale entries when keys are
     overwritten.
  3. One timer per key (`time.AfterFunc`): trivial code, but a million runtime timers and a
     goroutine wakeup per expiry. Falls over.
- Decision: Option 1.
  - Lazy: every read checks the deadline and deletes an expired key on the spot.
  - Active: `hz` times a second (default 10), visit slot groups round-robin. For each slot with
    TTL keys, sample 20. Delete the expired ones. Repeat while more than 25% of a sample was
    expired, otherwise move on.
  - Two time limits: the whole cycle may use 25% of its interval (25 ms at hz=10), and it holds any
    one shard lock for at most 1 ms before moving to the next group.
  - Sampling uses Go's randomized map iteration order: "the first 20 keys we range over" is a
    random sample, at no cost.
  - Each command reads the clock once, so a key can't be alive at the start of a command and
    expired by its end. Tests use a fake clock, so time moves only when they say so.
- Why ([benchmarks](../benchmarks/iter-03-expiry/SUMMARY.md)):
  - Adapts to load by itself. With nothing expiring, a cycle looks at each slot once (tested
    exactly: `TestActiveExpiryIsCheapWhenNothingExpires`). In a storm, about 995k unread keys were
    gone 0.8–1.2 s after their deadline, on par with Redis.
  - Clients barely notice: GET p99 peaked at 0.4–1.7 ms during the storm (Redis: 0.4–3.4 ms). The
    1 ms per-lock cap means a client waits for one small group, not a 25 ms cycle.
  - No extra memory beyond the index of keys that have a TTL (ADR 009).
- Tradeoffs we accept:
  - Expired keys can linger. A key is invisible the moment it expires (reads check), but its
    memory may stay for a while. In the worst case the loop keeps up to about 25% of TTL keys
    stale in a slot before it "gives up" sampling that slot for this cycle.
  - DBSIZE counts expired keys not yet cleaned up, same as Redis.
  - Expiry follows the wall clock. If the system clock jumps forward (NTP correction, a VM
    resuming), many keys expire early at once. Redis has the same weakness.
  - The 25% threshold is the classic Redis value. Newer Redis uses 10% by default with a
    tunable "effort" setting. We kept the roadmap's 25%.
- What real Redis does: Exactly this shape: lazy expire on access, plus `activeExpireCycle` sampling
  20 keys per loop, repeating while too many are stale, with a 25% CPU time limit at `hz`. Redis
  also has a "fast cycle" run before each event-loop sleep, and replicas never expire keys
  themselves: the primary sends them DEL. We don't have the fast cycle. Every expiry goes through
  one function (`Tx.expire`), which is where replication will send that DEL in Iteration 6.
- At bigger scale I'd: Add the fast cycle for bursty workloads. Make the threshold and budget
  tunable per node. Export metrics (keys expired per second, estimated stale fraction, time spent
  per cycle) so operators see a storm coming. And spread TTLs with jitter on the client side, so a
  cache refill doesn't schedule a million keys to expire in the same second.
