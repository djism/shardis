# 006: Organize the keyspace as one map per hash slot
- Context: We need `SCAN`, a way to walk all keys in small steps with a cursor, without blocking
  for long. Go's built-in map has no stable "resume from here" position (iteration order is
  deliberately random). Later, the cluster needs to count, list and migrate the keys of one slot.
- Options considered:
  1. One big `map[string]entry`: simplest, but SCAN would have to snapshot all keys or give up its
     guarantees.
  2. 16384 small maps, one per hash slot (`CRC16(key) mod 16384`, Redis Cluster's function).
     Each map is created on the first key and dropped when it's empty.
  3. Our own hash table with Redis's reverse-binary cursor trick: most flexible, but a lot of
     code for no benefit yet.
- Decision: Option 2. `SCAN`'s cursor is the next slot number to visit.
- Why:
  - Simple, provable SCAN guarantee: a key's slot never changes, so a key that exists for the
    whole scan is returned exactly once, no matter what else is added or removed in between.
    Tested with churn between calls (`TestScanVisitsStableKeysOnce`).
  - Sets up the cluster work: "which slot?" (Iteration 7), `CLUSTER COUNTKEYSINSLOT` /
    `GETKEYSINSLOT` and migrating one slot (Iteration 11) become direct lookups instead of scans.
  - Pairs naturally with ADR 005: shard = slot mod 256, so a hash-tagged multi-key command (one
    slot) always takes exactly one lock.
  - CRC16 is verified against real Redis's `CLUSTER KEYSLOT` for 507 keys (`TestMatchesRealRedis`).
- Tradeoffs we accept:
  - Every operation computes CRC16 of the key, about 10 ns for a 20-byte key.
  - Empty slots cost an 8-byte nil pointer each (128 KB total). A slot that keeps flipping between
    0 and 1 keys re-allocates its map each time.
  - Our SCAN cursor differs from Redis's. Clients treat cursors as opaque, so that's fine.
  - SCAN visits whole slots, so `COUNT` is approximate (one slot can hold more keys than COUNT).
    And it takes the whole-keyspace lock briefly on every call.
- What real Redis does: In cluster mode, recent Redis (7.4+) also keeps a separate dictionary per
  slot. Older versions kept a side index from slot to keys. Its SCAN cursor uses reverse-binary iteration
  over its own hash table, which keeps the guarantee even while the table resizes. We match the
  layout and the guarantee, using a simpler cursor.
- At bigger scale I'd: Keep per-slot structures, but make SCAN lock only the shard it's visiting
  (not all 256), and track slot sizes to keep `DBSIZE` O(1).
