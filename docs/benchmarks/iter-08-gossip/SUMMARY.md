# Iteration 8: gossip convergence and bus overhead

**Setup:** Apple M4, macOS 26.6, Go 1.26.1. N fresh nodes in one process (each with its own bus
listener and real TCP links), introduced in a chain (node i MEETs node i+1, the worst case for
spreading news). Node timeout 15 s (Redis's default). Convergence = every node knows all N nodes
and none is still in handshake. Overhead = bus messages and bytes *sent* per node per second,
measured over 15 s once the cluster is stable.
Reproduce: `SHARDIS_GOSSIP_BENCH=$PWD/docs/benchmarks/iter-08-gossip/raw.csv go test ./internal/cluster -run TestGossipScaling -v -timeout 20m`,
then `python3 docs/benchmarks/iter-08-gossip/chart.py`. Raw: `raw.csv`.

![Gossip convergence and overhead](gossip.svg)

| Nodes | Converged (3 runs) | Messages sent / node / s | Bytes sent / node / s | Avg message |
|---:|---|---:|---:|---:|
| 6  | 1.4 – 2.2 s  | 2.0  | 4.8 KB  | ~2.4 KB |
| 20 | 8.2 – 11.8 s | 5.3  | 12.7 KB | ~2.4 KB |
| 50 | 10.8 – 15.9 s | 13.4 | 34.3 KB | ~2.6 KB |

Real processes, measured by hand: 6 `shardis` processes MEET'd in a chain with `redis-cli` knew
all 6 after **1.58 s**.

## What this tells us

1. **Gossip reaches everyone without anyone talking to everyone.** A chain of 5 introductions
   became a fully known 6-node cluster in ~1.5 s, because each PING/PONG carries news about other
   nodes and hearing about a stranger starts a handshake.
2. **Bigger clusters take longer to converge** (~11–16 s at 50 nodes from a chain). Each node
   gossips on about one sampled PING per second, and each message carries only ~10% of the
   cluster (at least 3 entries). Introducing nodes in a star (everyone MEETs one node), as
   `redis-cli --cluster create` does, is much faster than a chain.
3. **Per-node traffic grows linearly with N, so total cluster traffic grows with N².** The term
   that dominates is "ping anyone you haven't heard from in node_timeout/2": every node hears
   from every other at least every 7.5 s, so 50 nodes means ~6.5 PINGs + 6.5 PONGs per node per
   second. This is a known property of Redis Cluster's bus too, and the main reason Redis
   recommends at most ~1,000 nodes. A larger node timeout means less traffic but slower failure
   detection (Iteration 9).
4. **Messages are ~2.4 KB, mostly the 2 KB slot bitmap** every PING/PONG carries. Wasteful when
   nothing changes, but it means slot ownership heals itself on every heartbeat, with no
   separate sync protocol.

**Caveat:** in-process on one machine, there's no real network latency or loss, so these are lower
bounds for convergence. On real networks the message counts are the same; the times grow with
latency.
