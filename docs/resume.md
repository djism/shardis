# Resume bullets (every number is measured; sources in docs/benchmarks)

Pick 3–4 for a resume and keep the rest for interviews. They're ordered by how strongly they
signal distributed-systems judgment.

**Shardis: Redis-compatible distributed cache (Go, Kubernetes)** · github.com/djism/shardis · djism.github.io/shardis

- Built a Redis Cluster-compatible distributed cache from scratch in Go (16k lines plus 7k of tests). It has 16,384
  hash slots, async primary/replica replication with PSYNC, gossip-based failure detection, and
  epoch-based replica elections. Unmodified `redis-cli`, `redis-benchmark` and `go-redis` work
  against it.
- Achieved automatic failover in 3.1 s median (20 runs, 2 s node timeout) with no external
  coordinator, and 0 client errors across 1.2M operations in 7 chaos scenarios on real
  processes (crashes, partitions, flapping links, resharding during failover).
- Wrote a consistency checker that counts acknowledged writes rolled back, not just missing
  at the end. It showed a network partition rolling back 2,029 acknowledged writes on
  Kubernetes while the final state looked perfect, and I documented the bound and mitigations
  (`WAIT`, timeouts, Raft).
- Implemented live resharding (MIGRATE + ASK redirects) that rebalanced 4,096 slots under
  load with 0 client errors. Found and fixed a check-then-act race that parallel execution
  introduces (Redis doesn't have it) with a per-slot reader/writer gate, proven by a
  concurrent-INCR test.
- Ran it as a Kubernetes StatefulSet (stable DNS identity, PDB, bootstrap Job,
  NetworkPolicy partitions). A SIGTERM handoff moves a primary's slots to its replica in
  13 ms, so a full rolling restart under load lost 0 of 1.1M writes.
- Reached 1.46M SET/s pipelined on one node, 45% above Redis 8.10 on the same machine, with
  256 lock groups instead of a single thread. Kept per-command atomicity and measured the
  costs: 9% slower unpipelined, ~3× memory per key.
- Built a smart cluster client with a slot map, MOVED/ASK handling, a jittered-backoff retry
  budget, and retry-safety by command. Retrying everything doubled 32 of 32 in-flight INCRs
  in testing, so only idempotent commands retry by default.
- Added group-commit durability: `appendfsync everysec` costs 11% of throughput, and a
  kill -9 lost 0 of 2.1M acknowledged writes. Also added Prometheus metrics read at scrape time,
  a WebSocket event stream, and 51 written design decision records.

**One-liner for a summary line**
> Built a Redis Cluster-compatible distributed cache in Go with automatic failover (3 s),
> live resharding and a chaos-tested consistency checker; 0 client errors in 1.2M chaos ops.
