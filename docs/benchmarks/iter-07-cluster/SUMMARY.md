# Iteration 7: one node vs a 3-primary cluster

**Setup:** Apple M4 (10 cores), macOS 26.6, Go 1.26.1, Redis 8.10.1, persistence off everywhere.
`redis-benchmark -t set,get -n 1000000 -r 1000000 -c 50`, with `--cluster` for the clusters
(it discovers nodes by parsing `CLUSTER NODES`, so this also proves that output is compatible).
Clusters are 3 primaries + 3 replicas as local processes; the Redis cluster was created with
`redis-cli --cluster create --cluster-replicas 1`.
Reproduce: `bash docs/benchmarks/iter-07-cluster/run.sh`. Raw: `raw.csv`.

| Setup | SET (no pipeline) | GET (no pipeline) | SET (-P 16) | GET (-P 16) | SET p99, -P 16 |
|---|---:|---:|---:|---:|---:|
| Shardis, single node | 125k | 119k | 1.49M | 1.45M | 1.1 ms |
| Shardis, 3-primary cluster | 108k | 125k | ~1.0M* | ~1.33M* | 4.4 ms |
| Redis, single node | 137k | 120k | 966k | 1.31M | 1.1 ms |
| Redis, 3-primary cluster | 129k | 121k | ~1.0M* | ~1.33M* | 1.7 ms |

\* Both clusters reported *identical* round numbers for pipelined runs (1,333,333 = 1M requests in
exactly 0.75 s; 998k–999k ≈ 1.0 s). That's the same coarse-timer artifact we caught in
`redis-benchmark --threads` in Iteration 2, apparently also in `--cluster` mode. Read those cells
as ±25%.

## What this tells us

1. **The cluster works with standard tools:** `redis-benchmark --cluster` found all nodes from our
   `CLUSTER NODES` and routed every request to the right primary, with no errors and no redirects.
2. **On one laptop, a cluster doesn't add throughput, for Redis or for us.** One benchmark client
   process drives all three primaries, and it's the bottleneck. A cluster pays off when each
   primary has its own machine (CPU, NIC) and there are many clients. That's the point of
   sharding, and it can't be shown on a single host.
3. **Shardis-as-cluster has a worse tail than Redis-as-cluster** (SET p99 4.4 vs 1.7 ms pipelined,
   max up to 80 ms). Six Go processes, each free to use all 10 cores (`GOMAXPROCS=10`), plus the
   benchmark, oversubscribe the machine. Redis runs one thread per process. On real hardware
   (one node per machine) this goes away. On a shared host, `GOMAXPROCS` should be capped per
   node. Not done yet.
4. **Single node:** unchanged from earlier iterations. About 10% behind Redis without pipelining,
   ahead with it (multi-core).
