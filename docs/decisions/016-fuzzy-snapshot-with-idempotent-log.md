# 016: Compact the AOF with a fuzzy snapshot, made correct by the idempotent log
- Context: An append-only log grows forever: 50 overwrites of a key are 50 records. Rewriting it
  means writing a fresh "base" file with one record per live key, while the server keeps serving
  writes. Redis does this with `fork()`: the child process gets a frozen copy-on-write view of
  memory and dumps it at leisure. Go can't fork a running multi-threaded program safely.
- Options considered:
  1. Lock everything (`DoAll`) and dump: exact, but every client waits for the whole dump.
  2. A fuzzy snapshot: switch to a new incremental file first, then dump the keyspace one slot
     group at a time, locking only that group (1/256 of the data) while copying it out.
  3. Copy-on-write data structures (persistent maps) that give a true point-in-time view: a major
     redesign of the store and more memory per update.
- Decision: Option 2:
  - **Switch:** the writer goroutine finishes the current batch, fsyncs the old incr file, opens
    a new one, and atomically writes a manifest that lists [old base, old incr, **new incr**]
    *before* writing anything to the new file.
  - **Dump:** for each of 256 groups, take its lock, encode its keys as `SET k v [PXAT t]` into a
    buffer, release the lock, write the buffer. Then fsync, and rename the temp file to the base
    name.
  - **Commit:** atomically switch the manifest to [new base, new incr], then delete the old files.
- Why it's correct even though the base isn't from one instant: every change after the switch is
  in the new incr file, and effects are absolute and idempotent per key (ADR 014). Say key `k` was
  dumped at a moment when its value already reflected some records in the new incr file. Replaying
  those records again lands on the same values, and the *last* record for `k` is its true latest
  state. Databases call this a fuzzy checkpoint (the idea goes back to ARIES-style recovery).
  - Tested: 6 writers change keys randomly (overwrite, TTL, delete, persist) *during* the
    rewrite. After a restart, base + incr reproduce the exact final state, in 40 of 40 rounds under
    `-race` (`TestFuzzyRewriteUnderConcurrentWrites`).
  - Crash mid-rewrite: a copy of the directory taken right after the switch restores everything
    (`TestCrashMidRewriteLosesNothing`). The manifest is only replaced via temp file + fsync +
    rename + directory fsync, so it always names a complete set of files.
  - Each lock is held only while copying 1/256 of the keyspace into a buffer, never while writing
    to disk.
- Tradeoffs we accept:
  - The base file can contain values that are immediately overwritten by records in the incr
    file. That's wasted bytes, never wrong state.
  - The rewrite needs a buffer for one group at a time and disk space for old + new files until
    commit.
  - The new incr file opens with a manifest write, which blocks the writer briefly (one fsync).
  - It only works because every record is idempotent. Logging a non-idempotent command (an
    `APPEND`, a relative `INCRBY`) would break it, and so would logging commands instead of effects
    (ADR 014 enforces this).
- What real Redis does: Forks, and the child writes the base (as an RDB file by default, or as
  AOF) from a true point-in-time copy-on-write snapshot, while the parent opens a new incr file.
  Since Redis 7 there's no rewrite buffer any more; the multi-part AOF with a manifest is the same
  file layout we use. Fork costs page-table copying (it can stall a large instance for tens of ms)
  and up to 2x memory under heavy writes. Our approach avoids both, at the price of a fuzzy base.
- At bigger scale I'd: Write the base as a compact binary snapshot (length-prefixed, compressed,
  checksummed) instead of RESP, dump groups in parallel on several cores, and stream the base
  straight to a replica that needs a full sync (Iteration 6) instead of to disk.
