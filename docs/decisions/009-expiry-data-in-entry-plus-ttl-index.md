# 009: Store the deadline in each entry, plus a per-slot index of TTL keys
- Context: For every key we need to know "does it have a TTL, and when does it die?" Reads ask
  this on every lookup (lazy expiry). The active loop needs to *find* keys that have a TTL,
  ideally without wading through keys that never expire.
- Options considered:
  1. An `expireAt` field in every entry, plus a separate per-slot set of only the keys that have a
     TTL (used for sampling).
  2. A separate map of deadlines for TTL keys only, like Redis's `expires` dictionary. Keys without
     a TTL pay nothing, but a read of a TTL key needs two map lookups.
  3. Only the field, no index: the loop samples all keys. With 1% of keys having a TTL, 99% of
     samples would be wasted.
- Decision: Option 1. The deadline is an **absolute** Unix-millisecond timestamp (0 = none).
- Why:
  - A read is always one map lookup, whether or not the key has a TTL.
  - The index means the active loop samples only keys that can expire.
  - Its strings share memory with the main map's keys (one allocation, used by both).
  - **Absolute, not relative:** "dies at 12:00:05.250" means the same thing after a restart, in an
    AOF replay, or on a replica receiving the write later. "Dies in 5 s" would silently restart the
    countdown each time the write is copied. Redis stores absolute times for the same reason.
  - A randomized test checks the index never drifts from the entries over 20k random operations
    (`TestVolatileIndexStaysConsistent`).
- Tradeoffs we accept:
  - Every key pays for the field: measured **108 → 124 heap bytes per key** (+16, not +8,
    because the entry struct is padded from 24 to 32 bytes and Go maps store entries inline).
  - TTL keys pay again for their index entry (about 30–40 B).
  - Every write path must keep the field and the index in sync. They're only changed together
    inside `Tx`, and the consistency test guards it.
- What real Redis does: Option 2. A second `expires` dictionary maps key → deadline, so keys without
  a TTL cost nothing extra. Redis can afford the second lookup because its dictionaries are very
  cheap and it checks the `expires` dictionary only when it's non-empty. Same absolute-time choice.
- At bigger scale I'd: Pack the deadline into fewer bits (e.g. seconds since a custom epoch in
  32 bits for TTLs over a second), or keep values and metadata in a slab outside Go's heap. Both
  cut the per-key overhead that makes us ~3x Redis's memory.
