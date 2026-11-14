# 019: Replicas hide expired keys but never delete them; only the primary does
- Context: Keys have TTLs (Iteration 3). On a replica, a key's deadline passes too. Who deletes
  it? If both primary and replica delete expired keys on their own, their clocks (which always
  differ a little) decide when, and the two copies can disagree about which keys exist.
- Options considered:
  1. Only the primary expires keys and replicates a `DEL`. Replicas hide logically expired keys
     from reads but keep them until that `DEL` arrives.
  2. Each node expires keys on its own clock.
  3. Replicas delete expired keys on read (lazy only), but don't run active expiry.
- Decision: Option 1. A store-level replica mode (`Engine.SetReplicaMode`):
  - Reads treat an expired key as missing but don't delete it.
  - Active expiry is a no-op.
  - Replicas don't evict for `maxmemory`: the primary's eviction `DEL`s arrive anyway.
  - Replaying the primary's `DEL` uses `Tx.Remove`, which deletes even an expired key.
  - Promotion turns replica mode off, and normal expiry resumes.
- Why:
  - **The replica's data is always a prefix of the primary's history,** never a divergent copy.
    That's what makes offsets meaningful and partial resync correct.
  - **Clients never read a stale key from a replica** (`TestExpiryOnReplicaFollowsPrimary`).
  - The primary's single expiry code path already logs `DEL` (ADR 014), so this needed no new
    replication logic.
- Tradeoffs we accept:
  - **Replicas can hold expired keys a little longer,** until the primary's active expiry finds
    them (usually within a second).
  - **If the primary is down, nothing expires on its replicas** until one is promoted.
  - DBSIZE on a replica counts hidden expired keys, as it does on the primary.
- What real Redis does: Exactly this. Since Redis 3.2, replicas return nil for logically expired
  keys but leave deletion to the primary's `DEL` (writable replicas excepted). Replicas also ignore
  `maxmemory` by default (`replica-ignore-maxmemory yes`).
- At bigger scale I'd: Keep the rule, and watch replica memory. A primary that falls behind on
  expiry pushes extra memory onto every replica too.
