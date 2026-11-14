# 015: Acknowledge a write only after it reaches the kernel (fsync too for "always"), with group commit
- Context: A client that hears "OK" assumes its write is saved. We had to decide when "OK" is
  allowed to leave, for each fsync policy, without making every write pay for a disk round trip.
- Options considered:
  1. Reply after the effect has been `write()`'n to the AOF file (and fsynced, for `always`).
     Replies wait, batched per pipeline the same way replies are already batched (ADR 003).
  2. Reply immediately and write the log in the background: lowest latency, but a `kill -9` loses
     whatever was still buffered in our process, even under `everysec`.
  3. fsync on every write, no batching: the textbook way, and one fsync per write.
- Decision: Option 1, with group commit for `always`:
  - One writer goroutine takes *everything* appended since its last pass and writes it in one
    `write()` call. For `always` it then fsyncs once; every client whose write was in that batch
    is covered.
  - The reply path waits in the socket writer itself: the connection's `durableConn` blocks
    before sending any bytes if the connection ran a write since its last reply. So no path can
    leak an early "OK", not even the reply buffer filling up and flushing itself mid-pipeline.
  - `everysec` fsyncs from a separate goroutine once a second, so a slow fsync never delays
    `write()` calls or replies.
  - If a write or fsync fails (disk full, I/O error), the AOF goes into a sticky error state.
    Waiting replies are never sent (the connection drops), and new writes get
    `-MISCONF Errors writing to the AOF file: ...`.
- Why ([benchmarks](../benchmarks/iter-05-persistence/SUMMARY.md)):
  - **Process crashes lose nothing, under every policy.** 30 `kill -9` runs under load, 0 of ~6.4M
    acknowledged writes lost. `TestReplyMeansWrittenToAOF` checks directly that the key is already
    in the file when "OK" arrives.
  - **Machine crashes lose exactly what the policy promises:** `always` 0; `everysec` 0.3–0.9 s;
    `no` whatever the OS hadn't flushed.
  - **Group commit makes `always` usable:** 16 writers averaged ~16 writes per fsync in tests, 50
    clients ~50. Throughput is 5.9k writes/s vs about 120/s (one ~8 ms `F_FULLFSYNC` per write)
    without it. With pipelining: 82k/s.
  - With `everysec`, pipelined throughput is 1.13M writes/s (faster than Redis's 650k) because the
    wait is paid once per batch, not per command.
- Tradeoffs we accept:
  - **Every write reply waits for a hand-off to the writer goroutine.** Without pipelining that's
    ~18% behind Redis (115k vs 141k writes/s at `everysec`).
  - **Tail latency:** p99 9 ms vs Redis's 1.5 ms at `everysec`. Likely a `write()` blocking behind
    an in-progress fsync of the same file. Redis avoids this by postponing writes (up to 2 s)
    while a background fsync runs; we don't yet.
  - Reads don't wait, even right after a write on the same connection. That's fine: a read's
    reply doesn't promise durability.
  - `always` latency is bounded below by the device: ~10 ms p50 on this Mac.
- What real Redis does: The same contract. Redis writes the AOF buffer to the file in
  `beforeSleep`, before replies go out, so a process crash loses nothing with any policy.
  `always` fsyncs in that same step, which is group commit for free, since one event-loop pass
  covers every client. `everysec` fsyncs on a background thread and postpones `write()` while
  that fsync is running (up to 2 s, then it writes anyway and counts `aof_delayed_fsync`).
- At bigger scale I'd: Add Redis's postponed-write trick for `everysec` to fix the tail. Use
  `fdatasync` on Linux (skips metadata), and pre-allocate the file so appends don't change its
  size. Put the AOF on its own volume. For real durability, a replica acknowledgment (`WAIT`,
  Iteration 6) usually beats `always`: memory on two machines is often both safer and faster
  than one fsync.
