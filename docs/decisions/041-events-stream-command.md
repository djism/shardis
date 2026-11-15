# 041: Each node streams its events over a normal connection (the EVENTS command)
- Context: Since Iteration 0, every important internal action has emitted an event on the
  node's in-process bus (ADR 007): gossip, PFAIL/FAIL, votes, failovers, slot moves,
  replication links. To record traces of chaos runs, and later to animate the website, those
  events have to get out of seven separate processes.
- Options considered:
  1. **An `EVENTS` command:** the connection turns into a stream of JSON events, one RESP bulk
     string each, like Redis's `MONITOR`. Any tool that speaks RESP can subscribe.
  2. **A file per node** (`--events-file`): simple, but the recorder has to find and merge files,
     and it doesn't work for a live website.
  3. **A separate HTTP or WebSocket port** on every node: what the website ultimately wants, but
     another listener and another protocol inside the cache server.
- Decision: Option 1. The recorder (`shardis-chaos`) and the future observer
  (`shardis-observer`, which will relay to WebSocket) both subscribe this way.
- Why:
  - **No new port or protocol:** it reuses the connection takeover PSYNC already uses. The
    chaos recorder reconnects within 50 ms when a node restarts.
  - **Still free when nobody listens,** and now also free for events nobody wants. A
    subscription excludes `key_write` (one event per write) by default, and the bus tracks
    whether *any* subscriber wants it (`WantsKeyWrites`). If none does, the store doesn't build
    them at all, instead of building 100k events a second to throw away. `TestEventsStream`
    checks that the producer turns off when the last `EVENTS ALL` subscriber hangs up.
  - **A slow subscriber can't slow the cache down:** events go into a 16k buffer, overflow is
    dropped and counted, and the stream reports `events_dropped` so a trace with gaps says so.
- Tradeoffs we accept:
  - **No authentication:** anyone who can connect can watch node internals (key names
    included, with ALL). Same as `MONITOR` on an unauthenticated Redis. Shardis assumes a
    trusted network, like Redis's default.
  - **Events emitted before the subscriber (re)connects are lost.** A node that restarts and
    demotes itself within the first few milliseconds may not appear in the trace.
  - **Each node's timestamps come from its own clock,** fine on one host. Across hosts, traces
    would need clock-offset correction.
- What real Redis does: `MONITOR` streams every command, not internal events. There's no
  built-in stream of cluster-internal events like PFAIL or votes; you read logs or poll
  `CLUSTER NODES`. This is new to Shardis, for observability.
- At bigger scale I'd: Export events as structured logs to a pipeline (OpenTelemetry → a log
  store), with sampling for high-volume types, so one dashboard sees every node's view of a
  failover side by side.
