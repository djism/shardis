# 014: Log the effects of commands, not the commands themselves
- Context: The AOF records changes so a restart can rebuild the keyspace. Iteration 6 needs the
  same stream to keep replicas in sync. Either way, replaying the log must land on exactly the
  state the primary had, whenever and wherever it's replayed.
- Options considered:
  1. Log commands as received (`INCR x`, `EXPIRE k 10`, `SET k v EX 5`). Simplest, but not
     deterministic. `EXPIRE k 10` replayed tomorrow sets a different deadline. A key that expired
     or was evicted leaves no trace, so after a replay it's back (or a replica keeps it forever).
  2. Log effects: what actually changed, stated absolutely. `INCR x` → `SET x 6`;
     `EXPIRE k 10` → `PEXPIREAT k <unix ms>`; `SET k v EX 5` → `SET k v PXAT <unix ms>`;
     expiry and eviction → `DEL k`; `FLUSHALL` → `FLUSHALL`.
  3. Log physical pages or a binary diff of memory: tied to our in-memory layout and useless for
     replication to a different version.
- Decision: Option 2. Each `Tx` records the effects of its command into a small buffer as RESP.
  While the command still holds its locks, the engine hands that buffer to the log, so effects on
  the same key reach the log in commit order.
- Why:
  - **Deterministic:** absolute deadlines and absolute values replay identically any time.
    `TestKeysExpiredWhileDownStayGone`: a key that expired during downtime doesn't come back.
  - **Complete:** expiry and eviction already go through one function each (Iterations 3–4), so
    they're logged as `DEL` with one line of code apiece.
  - **Idempotent per key:** each record sets a key's value or TTL outright. Replaying a record
    whose result is already in place changes nothing. That's what makes the fuzzy snapshot work
    (ADR 016).
  - **Reusable:** the same byte stream becomes the replication feed in Iteration 6, with no second
    format.
  - **RESP encoding:** loaded with our existing, fuzzed parser, and readable in any text editor.
- Tradeoffs we accept:
  - **Bigger log for some commands.** `INCR` on a counter logs the whole value. A future `APPEND`
    to a 1 MB string would log 1 MB each time (Redis logs the APPEND itself).
  - **A read can produce a log record.** A GET that lazily expires a key logs `DEL`.
  - **One shared log mutex.** Encoding happens inside the command's critical section, and every
    write takes that mutex for a short memcpy. Shard locks spread the store across cores, but the
    log is a single sequence.
- What real Redis does: A mix. It logs most commands as received, but rewrites the
  non-deterministic ones before logging: `EXPIRE` → `PEXPIREAT`, `SET EX` → `SET PXAT`,
  `INCRBYFLOAT` → `SET`, `SPOP` → `SREM`, and expired or evicted keys → `DEL`. Same goal, reached
  command by command. We apply the rule uniformly: always log the effect.
- At bigger scale I'd: Use per-shard logs instead of one global sequence, so writes on different
  cores don't contend. Multi-key commands are confined to one slot in a cluster, so per-slot
  ordering is enough. Add a checksum per record (RESP has none) to catch disk corruption
  precisely. Batch-encode effects with a compact binary format if log bandwidth matters.
