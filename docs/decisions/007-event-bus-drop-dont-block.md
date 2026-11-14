# 007: Event bus is free when idle, and drops events rather than block
- Context: Every important internal action (key writes now; gossip, votes, failovers later) emits
  an event for the website and trace recorder. Key writes happen hundreds of thousands of times a
  second, so the bus must cost nothing when nobody listens, and a slow listener (a laggy WebSocket)
  must never slow the cache down.
- Options considered:
  1. Non-blocking delivery: if a subscriber's buffer is full, drop the event and count the drop.
  2. Blocking delivery: wait for the subscriber. Every event arrives, but a stuck browser tab
     would freeze writes.
  3. Unbounded queue per subscriber: no drops and no blocking, but memory grows without limit
     when a subscriber stalls.
- Decision: Option 1.
  - Callers guard with `if bus.Active()`, a single atomic load, so with no subscribers they don't
    even build the event (no allocation).
  - Each subscriber has a bounded channel. A full channel means drop and `Dropped()++`.
  - Close takes the write lock before closing the channel, so an `Emit` can never send on a
    closed channel.
- Why: Measured cost with no subscribers is 3.4 ns and 0 allocations per guarded emit. With one
  subscriber it's 477 ns and 2 allocations, paid only while someone is watching. The cache's
  latency must not depend on the website.
- Tradeoffs we accept:
  - A trace can have gaps under heavy load. The recorder will save the drop count with each trace,
    so a gap is visible rather than silent, and the website can say so.
  - Tests that check events need a big enough buffer.
  - With subscribers present, Emit takes a read lock and allocates a map per event. Fine for
    observability, too slow to leave on in a benchmark.
- What real Redis does: The closest equivalents are `MONITOR` (streams every command to a client,
  documented to cut throughput significantly) and keyspace notifications over Pub/Sub (fire and
  forget, delivered at most once). Redis also disconnects slow Pub/Sub clients past an output
  buffer limit. Same philosophy: observers never get to slow the data path.
- At bigger scale I'd: Sample high-volume events (e.g. 1 in 100 key writes) instead of emitting
  all of them, use typed event structs instead of maps to avoid allocations, and ship events to a
  real pipeline (Kafka or an OpenTelemetry collector) rather than in-process channels.
