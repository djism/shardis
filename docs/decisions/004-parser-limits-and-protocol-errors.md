# 004: Hard limits in the parser; a protocol error closes the connection
- Context: RESP is length-prefixed. A client says "*1000000" or "$500000000" *before* sending
  the data. A naive parser allocates whatever the header claims, so a single 20-byte message
  could make us allocate 500 MB. And once input is malformed, we can't tell where the next
  command starts.
- Options considered:
  1. Trust the headers: allocate the claimed size, parse leniently. Easy DoS.
  2. Hard limits plus incremental allocation; on bad input, reply with an error and disconnect.
  3. Try to resync after a bad message (skip to the next "*"). The payload could contain "*",
     so we might start executing garbage as commands.
- Decision: Option 2.
  - Limits: 512 MB per bulk string, 1M arguments per command, 64 KB per inline command or header
    line, 32 levels of array nesting (replies only). The first three are Redis's defaults.
  - Allocation: arrays pre-allocate at most 1024 slots. Bulk strings over 1 MB grow as bytes
    actually arrive, so a lying header costs nothing.
  - Strict integers: digits with an optional '-', no '+', spaces or overflow.
  - On any framing error: send `-ERR Protocol error: <reason>` and close.
- Why: The cost of a bad header should be paid by the attacker (sending real bytes), not by us
  (allocating). Disconnecting is the only safe recovery, because after a framing error every
  later byte is ambiguous. Fuzzing (3 targets, ~7M inputs each in 60s) found no panic, hang or
  unknown error type. Each fuzz target checks for exactly those.
- Tradeoffs we accept: Legitimate values over 512 MB are rejected, as in Redis. A buggy client
  loses its connection instead of getting a recoverable error. Inline commands don't support
  Redis's "quoted strings" (only matters for telnet users; real clients use multibulk).
  Memory is still limited per connection, not in total: 10k connections each sending 500 MB is
  a separate problem (maxmemory and client limits, Iteration 4+).
- What real Redis does: Same limits (`proto-max-bulk-len`, 1M multibulk cap, 64 KB inline), same
  error texts ("invalid bulk length", "expected '$', got 'x'"), same disconnect-on-error. Redis
  also grows large bulk buffers gradually. We match it so clients see identical behavior.
- At bigger scale I'd: Add a global memory cap on query buffers across all clients
  (`client-query-buffer-limit` + `maxmemory-clients`), auth before accepting large payloads,
  and per-tenant connection/rate limits at a proxy layer.
