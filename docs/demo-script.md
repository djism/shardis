# Shardis: 5-minute demo video script

**Setup before recording**
- Run `make build`, `rm -rf .cluster-local`, then
  `SHARDIS_CLUSTER_NODE_TIMEOUT=2000 make cluster-up-local` (a 2 s node timeout, as in the
  measurements; the default is 15 s).
- Terminal: large font, two panes. Browser: the site, scrolled to the top.
- Pane 2 is ready with `bin/shardis-loadgen -cluster -addr 127.0.0.1:7001 -preload`.
- Practise once. Every number below is a real measurement, so say them exactly.

| Time | On screen | Say (roughly) |
|---|---|---|
| **0:00–0:30** · Hook | Site hero: the animated six-node cluster | "This is Shardis, a distributed cache I built from scratch in Go. It works like Redis Cluster: six processes split the keys, copy every write to a backup, and replace a dead machine on their own. Real `redis-cli` and real Redis client libraries talk to it unchanged. And I measured exactly when it loses data, because Redis Cluster does too." |
| **0:30–1:10** · One key | Scene 1: type `user:42`, then `{user:42}:cart` | "Every key hashes to one of 16,384 slots: CRC16, mod 16384. Each primary owns a range. Curly braces are hash tags: only the part inside is hashed, so a user's keys land together. This runs the same algorithm as the server, checked by a shared test file." |
| **1:10–1:50** · It's real | Terminal: `redis-cli -c -p 7001 set user:42 hi`, then `get user:42`, `cluster nodes` | "Here's the real thing. I asked node 7001; the key lives elsewhere, so the node answered MOVED with the owner's address, and `-c` followed it. Every node knows the whole map, so there's no proxy and no coordinator." |
| **1:50–3:00** · Kill a primary | Start the loadgen in pane 2. Then `kill -9 $(cat .cluster-local/a.pid)` (node a, port 7000, owns slots 0–5460). Watch `cluster nodes` and the loadgen output. | "Now traffic is flowing, and I kill a primary, a hard kill. Its peers stop hearing from it. After the node timeout they mark it 'possibly failed', the suspicion spreads by gossip, and when a majority of primaries agree, it's FAILED. Its replica asks for votes, wins, and takes the slots. Median failover is 3.07 seconds at a 2-second timeout, over 20 runs. The client saw a short stall, not errors: across seven chaos scenarios and 1.2 million operations, zero client errors." |
| **3:00–3:40** · The honest part | Site → Benchmarks: "Acknowledged writes lost" chart; Scorecard "Where it hurts" | "But I don't hide this: replication is asynchronous. The primary says OK before the replica has the write, so a crash loses the in-flight tail, up to 146 writes per failover here. `WAIT` closes that gap at a latency cost. A partition is worse: a cut-off primary keeps accepting writes until it notices it's in the minority. On Kubernetes that rolled back 2,029 acknowledged writes. My checker catches that even when the final state looks perfect." |
| **3:40–4:20** · Kubernetes | Site → Kubernetes: click "kubectl rollout restart", open the StatefulSet card, then the PDB card | "It runs as a StatefulSet, so each pod keeps its name and its disk. On SIGTERM a primary hands its slots to its replica before exiting. That takes 13 milliseconds, and a full rolling restart under load lost 0 of 1.1 million writes. The disruption budget means only one pod is ever down." |
| **4:20–5:00** · Close | Site → Design decisions (search "gate"), then "At Google scale" | "Every real choice is written down: 51 decision records with the options, the tradeoffs and what Redis does. My favourite is the per-slot gate: running commands in parallel created a race Redis doesn't have, so I found it, measured it and closed it. At a thousand nodes I'd move slot ownership to Raft to close the split-brain window. Code, design doc and benchmarks are all linked. Thanks for watching." |

**If something goes wrong on camera**
- Failover looks slow? You probably forgot `SHARDIS_CLUSTER_NODE_TIMEOUT=2000`, so the default
  15 s is in effect. Say so: "the timeout here is 15 seconds, so it takes a bit longer".
- Stale state from an earlier run? `make cluster-down-local && rm -rf .cluster-local`.
