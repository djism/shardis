# Shardis Roadmap

Each iteration is small, ends with something working, and produces:
code + tests + recap (docs/learning-log.md) + decision records + (when relevant) benchmarks.

Rough pace: 1–2 iterations per week part-time, about 10–12 weeks total.
"Done when" = the acceptance check. Don't move on until it passes.

---

## Phase 1: A single fast node (the "Redis" part)

### Iteration 0: Setup
- Go module, Makefile, folder layout from CLAUDE.md, GitHub Actions CI (build + `go test -race`).
- Empty `docs/learning-log.md`, `docs/decisions/`, `docs/benchmarks/`.
- Check tools are installed: Go, Docker, minikube, kubectl, Node 20+, `redis-cli` and
  `redis-benchmark` (from the `redis-tools` package, used only as test clients/baseline).
- **Done when:** `make build` and `make test` pass in CI.

### Iteration 1: Speak the Redis protocol
- RESP2 parser/writer (simple strings, errors, integers, bulk strings, arrays, plus inline
  commands). TCP server with one goroutine per connection.
- Commands: `PING`, `ECHO`, `COMMAND` (minimal stub so redis-cli is happy), `QUIT`.
- Fuzz test for the parser (`go test -fuzz`).
- **Done when:** `redis-cli -p 7000 ping` returns `PONG`.
- **Concepts:** wire protocols, framing, why pipelining works.

### Iteration 2: The keyspace + the concurrency decision
- `GET`, `SET` (with `NX`/`XX`), `DEL`, `EXISTS`, `INCR`/`DECR`, `MGET`/`MSET`, `KEYS`/`SCAN`, `DBSIZE`, `FLUSHALL`.
- **Design choice (benchmark both):**
  A) a single "executor" goroutine owns the data, like Redis's single thread (no locks), vs
  B) a map split into N shards, each with its own lock.
  Compare with `redis-benchmark` (throughput + p99) at 1, 50 and 200 clients. Write the ADR.
- **Done when:** `redis-benchmark -p 7000 -t set,get -n 100000` runs cleanly, and the ADR has numbers.
- **Concepts:** lock contention, why Redis is single-threaded, tail latency.

### Iteration 3: Expiry
- `SET ... EX/PX`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`.
- Two cleanup paths, like Redis: **lazy** (check on read) + **active** (a background loop
  samples ~20 keys with a TTL and repeats if more than 25% were expired).
- Emit `expire` events.
- **Done when:** tests show expired keys vanish, and memory drops after a mass expiry.
- **Concepts:** why scanning every key is too slow, probabilistic cleanup.

### Iteration 4: Memory limits + eviction
- `--maxmemory` and policies: `noeviction`, `allkeys-lru`, `allkeys-random`, `volatile-ttl`.
- Approximate LRU by sampling (like Redis), vs exact LRU (linked list). Benchmark the
  memory overhead and hit rate of both on a skewed (Zipfian) workload. Write the ADR.
- `INFO memory` and `INFO stats` (hits, misses, evictions).
- **Done when:** a chart in docs/benchmarks shows hit rate: approx LRU vs exact LRU vs random.
- **Concepts:** caching tradeoffs, why "good enough" LRU wins.

### Iteration 5: Persistence
- Append-only file (AOF) with fsync policies `always` / `everysec` / `no`, replayed on startup.
- Point-in-time snapshot + AOF rewrite (compaction) without blocking writes for long.
- Crash test: kill -9 the process mid-load, restart, count lost writes per fsync policy.
- **Done when:** a table shows throughput vs writes lost for each fsync policy.
- **Concepts:** durability vs latency, write-ahead logs, fsync.

---

## Phase 2: Copies of the data (replication)

### Iteration 6: Primary → replica replication
- `REPLICAOF host port`. Full sync (send a snapshot, then stream new writes) and partial
  resync after short disconnects (replication ID + offset + backlog buffer, like `PSYNC`).
- Replicas are read-only by default. `INFO replication` shows offsets and lag.
- `WAIT numreplicas timeout`, for "wait until N replicas have this write".
- **The important demo:** show the async replication **lost-write window**. The primary
  acks a write, crashes before the replica gets it, the replica gets promoted, and the
  write is gone. Reproduce it on purpose with fault injection, then show how `WAIT`
  shrinks (but doesn't fully remove) the window.
- **Done when:** an automated test reproduces the lost write, and the recap explains it simply.
- **Concepts:** async vs sync replication, replication lag, why acks can lie.

---

## Phase 3: The cluster (the distributed systems core)

### Iteration 7: Hash slots + redirects (static cluster)
- CRC16 (XMODEM) mod 16384, hash tags (`{user:1}:cart` and `{user:1}:profile` land in the same slot).
- A static config says which node owns which slots. Wrong node → `-MOVED <slot> <host:port>`.
- Commands: `CLUSTER KEYSLOT`, `CLUSTER SLOTS`, `CLUSTER SHARDS`, `CLUSTER NODES`,
  `CLUSTER INFO`, `CLUSTER MYID`. Multi-key commands across slots → `CROSSSLOT` error.
- `deploy/compose/`: a 3-primary + 3-replica cluster.
- **Done when:** `redis-cli -c -p 7000` hops between nodes on MOVED, and the go-redis
  `ClusterClient` works against our cluster in an integration test.
- **Concepts:** sharding, why fixed slots beat hashing keys straight onto nodes, smart clients.

### Iteration 8: Cluster bus + gossip
- Separate cluster-bus port (client port + 10000). Binary messages: PING, PONG, MEET, FAIL, UPDATE.
- `CLUSTER MEET` to join. Each PING/PONG carries info about a few random other nodes (gossip),
  so everyone learns about everyone without all-to-all messages.
- Track per node: last ping sent, last pong received, flags, slot ownership, config epoch.
- Emit `gossip_send` / `gossip_recv` events (the website animates these as moving dots).
- **Done when:** start 6 nodes, MEET them in a chain, and within seconds all 6 know all 6.
- **Concepts:** gossip protocols, how info spreads fast, message overhead vs cluster size.

### Iteration 9: Failure detection + automatic failover
- **PFAIL:** "I can't reach node X" after `--node-timeout`.
- **FAIL:** a majority of primaries report PFAIL for X → mark FAIL and broadcast it.
- **Replica election:** the dead primary's replica waits a short ranked delay, bumps the
  `currentEpoch`, asks the primaries for votes, needs a majority, and each primary votes
  once per epoch. The winner takes a new `configEpoch`, claims the slots and broadcasts.
- **Epochs** settle conflicts: the higher config epoch wins slot ownership.
- Measure **failover time** (primary killed → writes accepted again), over 20 runs → p50/p99.
- **Done when:** kill a primary under load, its replica takes over with no manual steps,
  and docs/benchmarks has the failover timing table.
- **Concepts:** quorum, split votes, epochs as logical clocks, why majority matters.
  Compare with Raft in the recap (similar ideas, different design).

### Iteration 10: Network partitions
- Use fault injection to split the cluster (e.g. 1 primary alone on one side, everyone else on the other).
- The minority-side primary must **stop accepting writes** after node-timeout. The majority
  side fails over. When the partition heals, the old primary becomes a replica of the new one.
- Count acknowledged writes lost on the minority side (the "split-brain window").
- **Done when:** the partition test passes, and the recap explains CAP with our real numbers.
- **Concepts:** partitions, CAP in practice, why "stop writes after timeout" limits damage.

### Iteration 11: Live resharding + adding/removing nodes
- Slot migration: source marks slot MIGRATING, target marks IMPORTING, keys move in
  batches (`MIGRATE`), and clients get `-ASK` for keys already moved (client sends
  `ASKING` first). The slot flips to the target at the end, and the epoch bumps.
- A rebalance tool: add a 4th primary and move slots to it evenly **while a load generator runs**.
- Throughput and error rate measured during migration.
- **Done when:** resharding under load finishes with zero client errors (only redirects), plus a chart.
- **Concepts:** online migration, MOVED vs ASK, why the order of steps matters.

### Iteration 12: Our own smart client
- `pkg/client`: caches the slot map, follows MOVED (and refreshes the map) and ASK (one-shot),
  retries with backoff, pools connections per node, optional read-from-replica.
- **Done when:** the load generator uses it, and it survives failover + resharding without errors.
- **Concepts:** what a cluster puts on the client, retries vs duplicates.

---

## Phase 4: Proving it works (this is what makes the project stand out)

### Iteration 13: Chaos suite + consistency checker
- `cmd/shardis-chaos` runs named scenarios: `kill-primary`, `kill-replica`,
  `partition-minority-primary`, `flapping-network`, `slow-node`, `reshard-during-failover`,
  `restart-all`.
- A checker records every operation (start, end, result), then reports:
  lost acknowledged writes, stale reads, unavailability time, error rate.
  (We're not claiming linearizability. Redis Cluster doesn't promise it. We measure what does hold.)
- **Recorder:** saves the event stream of each scenario as JSON to `website/public/traces/`.
- **Done when:** `make chaos` prints a report table for every scenario, and traces are saved.
- **Concepts:** testing distributed systems, Jepsen-style thinking, honest guarantees.

---

## Phase 5: Kubernetes + observability

### Iteration 14: Run it on minikube
- `deploy/k8s/`: StatefulSet (6 pods), headless Service, PersistentVolumeClaims for AOF,
  ConfigMap, a bootstrap Job that forms the cluster, readiness/liveness probes,
  PodDisruptionBudget, resource requests/limits, graceful shutdown on SIGTERM.
- Handle pod IP changes on restart (announce stable DNS names / update gossip).
- Demo scripts in `deploy/k8s/demos/`:
  `kubectl delete pod shardis-0` (failover), `kubectl scale` + rebalance,
  `kubectl rollout restart` (rolling upgrade with no data loss),
  NetworkPolicy to cut off a pod (partition).
- **Done when:** every demo script runs on minikube and the chaos checker reports results.
- **Concepts:** StatefulSets vs Deployments, stable identity, why stateful apps on K8s are hard.

### Iteration 15: Metrics + live event stream
- Prometheus metrics (ops/sec, latency histograms, replication lag, gossip messages,
  failovers, evictions, slot ownership). A Grafana dashboard JSON in the repo.
- `cmd/shardis-observer`: subscribes to all nodes' event buses and streams to the website
  over WebSocket. Admin actions (kill node, partition, add node, reshard) can be
  triggered from the website **in local/live mode only**.
- **Done when:** the Grafana dashboard shows a failover live, and the observer streams events.

---

## Phase 6: The showcase website

### Iteration 16: Website skeleton + "Where does my key live?"
- `website/`: React + Vite + TypeScript, Framer Motion for motion, D3 for charts, Tailwind.
- Two modes: **Live** (connected to the observer locally) and **Replay** (plays recorded
  traces, which is what the public site uses). Clear label: "Every animation is a replay of a real run."
- Every animation has play / pause / step / speed controls (interviewers will want to step
  through), and respects `prefers-reduced-motion`.
- Section 1, hero: an animated 6-node cluster with the slot map and live dots for traffic and gossip.
- Section 2, "Where does my key live?": type a key, watch CRC16 → slot → node happen
  (CRC16 computed in the browser, same algorithm), with a hash-tag example.
- If an existing site's style should be matched (e.g. the Atlas project), follow its look and feel.

### Iteration 17: The distributed systems story, section by section
Each section = short plain-language explanation + animation from a real trace + "the design decision" card.
1. **Redirects:** MOVED vs ASK, animated client hops.
2. **Replication:** writes flowing with offsets, plus the lost-write window step by step.
3. **Failure detection and failover:** a timeline of PFAIL marks spreading by gossip → FAIL
   quorum → votes → promotion, with real timings.
4. **Network partition:** split view, the minority stops writes, lost-write count.
5. **Live resharding:** slots moving while the throughput chart stays flat.
6. **Persistence and eviction tradeoffs:** charts from Phase 1 benchmarks.
7. **Benchmarks vs real Redis:** honest charts, including where we're slower and why.
8. **Design decisions:** cards generated from `docs/decisions/*.md`.
9. **What I'd change at Google/Salesforce scale:** a short, thoughtful list.
10. **Kubernetes:** a short screen recording/GIF of the kubectl demos.
- Deploy the replay site to GitHub Pages / Vercel and link it from djism.github.io.

### Iteration 18: Final polish
- README: a 30-second pitch, an architecture diagram, a GIF, "run it in 2 commands", links.
- `docs/DESIGN.md`: the full design doc (written like a Google design doc: goals, non-goals,
  design, alternatives, failure modes, testing, future work).
- A 5-minute demo video script.
- Resume bullets with real numbers from the benchmarks.

---

## Stretch goals (only after everything above is solid)
- `MULTI`/`EXEC` transactions within a slot
- Pub/Sub (`PUBLISH`/`SUBSCRIBE`), cluster-wide
- RESP3
- `redis-cli --cluster create/check` compatibility
- Hot-key detection and client-side caching
