# 018: Full sync by streaming a fuzzy snapshot straight over the socket
- Context: A new replica, or one that fell too far behind, needs a complete copy of the primary's
  data before it can follow the stream. The primary must keep serving clients while that copy
  goes out.
- Options considered:
  1. **Diskless fuzzy snapshot:** reply `+FULLRESYNC <replid> <O>`, then dump the keyspace one slot
     group at a time (each group's lock held only while copying it into a buffer) straight to the
     replica's socket, then a `SNAPSHOT-END` marker, then the live stream from offset O.
  2. **Snapshot to a file first, then send the file:** older Redis's default. Extra disk I/O and
     time, but a transfer could be resumed and several replicas could share one file.
  3. **Lock everything and copy:** an exact snapshot, but every client waits for the whole copy.
- Decision: Option 1. The replica flushes its data, applies the snapshot's `SET` records (not
  counted in its offset), resets its backlog to start at O, then applies the stream.
- Why:
  - **The same correctness argument as the AOF rewrite (ADR 016).** Every change after O is in
    the backlog from O, and effect records are idempotent per key. A key dumped after it already
    changed just gets the same change replayed: same final state.
    `TestFullSyncThenStream` checks key-by-key equality (value and absolute deadline) after the
    snapshot, after every kind of effect, and after FLUSHALL.
  - **No disk involved,** so a replica can sync from a primary with no persistence at all. A
    client waits for at most one group (1/256 of the data) at a time.
  - **One code path:** `Tx.AppendSnapshot` serves both the AOF rewrite and full sync.
- Tradeoffs we accept:
  - **No resume:** a dropped transfer starts over.
  - **One snapshot per replica:** each full sync walks the keyspace again. N replicas syncing at
    once cost N passes.
  - **The backlog must hold every change made during the transfer.** If more than
    `repl-backlog-size` is written while a big snapshot is going out, the replica's start offset
    is overwritten, it's dropped, and it starts over. Under heavy writes that could loop.
  - **The replica serves partial data while syncing.** It answers reads mid-sync with whatever it
    has so far. Redis would answer `-LOADING`.
- What real Redis does: Forks a child that writes an RDB snapshot, from a true point-in-time
  copy-on-write view, either to disk (classic) or straight to the socket (`repl-diskless-sync yes`,
  the default since Redis 7). Changes during the transfer go to the replica's output buffer.
  Diskless, like us. Redis's snapshot is exact thanks to fork; ours is fuzzy but correct thanks
  to idempotent records.
- At bigger scale I'd: Share one snapshot pass among replicas that start syncing within a few
  seconds of each other (Redis's `repl-diskless-sync-delay`), use a compact compressed binary
  snapshot format, rate-limit the transfer so it can't saturate the primary's network, and add
  `-LOADING` on the replica.
