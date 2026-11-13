# 003: Flush replies only when we're about to wait for more input
- Context: Each connection reads commands and writes replies through buffers. We have to pick
  when buffered replies actually go out on the socket. That choice decides whether
  pipelining (a client sending many commands without waiting) is cheap or expensive.
- Options considered:
  1. Flush after every reply: simple, but 100 pipelined commands = 100 write syscalls.
  2. Flush when the input buffer has no more commands: one write per batch.
  3. Flush on a timer (every N ms): batches well, but adds up to N ms of delay to every
     lone request. Bad for a cache whose whole point is sub-millisecond latency.
- Decision: Option 2, implemented as a tiny wrapper (`flushBeforeRead`) between the socket and
  the read buffer. The read buffer only asks the socket for more bytes once it has run out, so
  that call is exactly the moment we've answered everything received. We flush there, then read.
- Why: A pipelined batch costs one write instead of N, and a single request still gets its
  reply immediately (no timer). Hooking into the read, rather than checking `Buffered() == 0`
  in the loop, also covers a tricky case: one full command plus *half* of the next. A
  `Buffered()` check sees leftover bytes and skips the flush, so the first reply would sit in
  the buffer while we wait for the rest of the second command. The read hook flushes before
  any wait. `TestPartialCommands` covers it.
  Measured: 50 clients go from 133k req/s unpipelined to 1.8M req/s with `-P 16`
  ([benchmarks](../benchmarks/iter-01-ping/SUMMARY.md)).
- Tradeoffs we accept: Replies to a large pipeline sit in memory until the batch is done.
  That's capped, because the 16 KB write buffer flushes itself when full, so memory per
  connection stays bounded. A client that sends a huge pipeline and never reads will block
  our writes. That's TCP backpressure working as intended: it slows only that client's goroutine.
- What real Redis does: Similar. Redis handles every ready socket in one event-loop pass,
  queues replies in per-client buffers, and writes them all just before going back to sleep
  waiting for events (`beforeSleep`). Same effect: one write per batch, no timer.
- At bigger scale I'd: Add per-client output buffer limits with disconnection (Redis's
  `client-output-buffer-limit`), so a slow reader of big replies (e.g. a replica, or pub/sub
  subscriber) can't pin unbounded memory. I'd also look at `writev` to send several buffers
  in one syscall without copying.
