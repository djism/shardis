# 045: Metrics are read at scrape time; the hot path only counts per connection
- Context: Prometheus metrics are how operators see a cluster: throughput, latency, roles,
  slots, replication lag, failovers. A cache that slows itself down measuring itself defeats the
  point. Every per-command cost multiplies by a million commands a second.
- Options considered:
  1. **Read at scrape time, plus two tiny hot-path hooks.**
     - Almost everything (keys, memory, roles, slots, epochs, PFAIL/FAIL counts, replication
       lag, bus messages, failover counters) comes from state the node already keeps. A custom
       collector reads it when Prometheus scrapes (every 2–15 s).
     - Per command, only a call counter and a sampled latency histogram.
  2. **Prometheus `CounterVec` and `Histogram` updated on every command:** the idiomatic way,
     and simplest.
  3. **Derive metrics from the event bus:** one source of truth, but subscribing turns events
     on, so the store would build an event per write (ADR 041).
- Decision: Option 1, served on an opt-in `--metrics-port` (`/metrics`, `/healthz`, `/readyz`),
  plus the standard Go runtime and process collectors (GC pauses, heap, RSS, CPU).
- Why (measured, [overhead.txt](../benchmarks/iter-15-metrics/overhead.txt)):
  - **The idiomatic version cost 7–9% of throughput:** slower in every round, at ~1.4M ops/s.
    One counter per command and one histogram meant every core did atomic adds to the same
    memory, so the cache lines bounced between cores. It's the same trap Iteration 2 avoided
    for the command count.
  - **The fix:**
    - Each connection counts calls in its own array of atomics, summed at scrape time, so no
      two cores touch the same counter.
    - Latency is timed for the 1st, 17th, 33rd... command of each connection. That's 1/16 of
      the clock reads and histogram updates, and short-lived connections still get sampled.
    - After the fix, the medians over 6 rounds were within noise of the no-metrics build.
  - **Failure handling is counted where it happens** (`failovers_won_total`, `elections_total`,
    `takeovers_total`, `pfail_marks_total`, ...), as plain atomics next to the events.
  - **Verified end to end:** with Prometheus and Grafana scraping a local cluster, killing a
    primary shows up as `is_primary` 0→1 on its replica, `cluster_state_ok` dipping on the
    others, and `failovers_won_total` +1. `TestClusterMetrics` checks the same in-process.
- Tradeoffs we accept:
  - **Latency is a 1-in-16 sample:** the tail is estimated, not complete. A rare slow command
    can be missed.
  - **Scrape-time reads take locks briefly.** `shardis_keys` sums 16384 map lengths under every
    shard lock (microseconds), every scrape.
  - **No per-key or per-slot metrics** (cardinality: 16384 slots × nodes is too many series).
  - Metrics are per node, with no cluster-wide aggregation inside Shardis; that's Prometheus's
    job.
- What real Redis does: Redis has no Prometheus endpoint. People run `redis_exporter`, which
  polls `INFO` and `CLUSTER INFO` over a normal connection. Redis counts per-command stats
  (`INFO commandstats`, `LATENCY` monitor) in its single thread, where counters are free.
  Multi-threaded, we have to avoid shared counters.
- At bigger scale I'd: Add exemplars (trace IDs on slow-latency samples), per-slot hotness
  sampled into a top-k sketch, and SLO alerts ("p99 > 2 ms for 5 min", "any node in a
  minority", "failovers > 0 in 10 min") shipped with the dashboard.
