# Shardis: a Redis-compatible distributed cache, built from scratch

> "Shardis" is a working name. Rename it anytime.

## What this project is

A distributed in-memory key-value cache written in Go that works like **Redis Cluster**:
- It speaks the real Redis protocol (RESP), so the real `redis-cli`, `redis-benchmark`
  and the `go-redis` cluster client work against it.
- Keys are split across nodes with the same 16384 hash slots Redis uses.
- Every primary has a replica. Nodes find each other and detect failures through gossip.
  When a primary dies, its replica gets elected and takes over automatically.
- Slots can move between nodes while traffic is running (live resharding).
- It runs as a 6-pod StatefulSet on Kubernetes (minikube).
- A showcase website animates what the cluster actually does, using **recorded traces
  from real runs**, not fake animations.

**Goal:** prove to Google / Salesforce-style interviewers that the author (Dhananjay)
understands distributed systems *and* knows how to work around their failure modes.
Every feature should come with: a test, a measurement, a design decision record, and
something that can be visualized.

## Who you're working with

Dhananjay has about 5 years of backend experience (Java/Spring, full-stack) and an MS in
Data Science. He's newer to Go and to low-level distributed systems. **He is building this
to learn, not just to have the code.** Explain things in simple, plain, casual language.
Avoid jargon walls. When a term is unavoidable (e.g. "fencing", "epoch", "quorum"),
explain it in one plain sentence the first time it comes up.

## How we work: the iteration loop (follow this every time)

We build in small iterations listed in `ROADMAP.md`. For each iteration:

1. **Plan first.** Before writing code, give a short plan (5–10 bullets): what you'll
   build, which files, any design choice to make. If there's a real design choice, lay out
   the 2–3 options with tradeoffs and recommend one. **Wait for Dhananjay's OK.**
2. **Build in small steps.** Keep the code compiling and tests passing as you go.
3. **Test it.** Unit tests plus at least one integration test. Always run
   `go test -race ./...`. Where it applies, test with the real `redis-cli`.
4. **Measure it** if the iteration has a performance or reliability angle (benchmarks,
   failover time, lost-write counts). Save the results in `docs/benchmarks/`.
5. **Write the recap** (format below). Show it in chat and append it to
   `docs/learning-log.md`.
6. **Write a design decision record** in `docs/decisions/NNN-short-title.md` (format below)
   for every real choice made.
7. **Stop and wait.** Don't start the next iteration until Dhananjay says so. Offer to
   commit, using a clear message like `iter-07: hash slots + MOVED redirects`.

### Recap format (after every iteration, in chat AND in docs/learning-log.md)

```
## Iteration N: <title>

**What we built**
2–3 plain sentences.

**How it works**
Simple explanation. Add a small ASCII diagram if it helps.

**Why we did it this way**
The choice we made, the alternative we didn't pick, and why.

**What can go wrong**
The failure modes / tradeoffs this design has, honestly.

**Try it yourself**
Exact commands to run and what you should see.

**Interview question you can now answer**
One realistic interview question + a short answer in Dhananjay's voice.

**Check yourself**
2 quick questions. Put answers inside <details><summary>Answer</summary>...</details>.
```

### Design decision record format (docs/decisions/NNN-title.md)

```
# NNN: <decision>
- Context: what problem we were solving
- Options considered: 2–3 options, one line each
- Decision: what we picked
- Why: the reasoning (link benchmarks if any)
- Tradeoffs we accept: what this costs us
- What real Redis does: and whether we match it
- At bigger scale I'd: what would change at Google/Salesforce scale
```

These records feed the "Design decisions" section of the website, so write them well.

## Technical rules

- **Language:** Go (latest stable). Standard library first. Allowed deps: a test helper
  lib, Prometheus client, a WebSocket lib, a CLI flag lib. Ask before adding others.
- **Implement from public specs and docs** (the RESP protocol spec and the Redis Cluster
  spec on redis.io). **Never copy code from the Redis source.** Write it ourselves.
- **Compatibility is a feature:** use CRC16 (XMODEM) mod 16384 for slots, Redis hash tag
  rules (`{...}`), and exact `-MOVED <slot> <host:port>` / `-ASK <slot> <host:port>` error
  formats, so real clients work.
- **Events from day one:** every important internal action (key write, replication ack,
  gossip message, PFAIL/FAIL mark, vote, failover, slot migration step, eviction,
  expiry) emits a structured event to an internal event bus
  (`{ts, node, type, data}`). Later, an observer streams these to the website, and a
  recorder saves them as replay traces. Design this so it costs ~nothing when nobody
  is listening.
- **Deterministic fault injection:** build admin hooks (behind a flag, never on by
  default) to drop or delay messages between specific nodes, pause a node, etc. Chaos
  tests and website traces depend on it.
- **Config by flags/env**, with sensible defaults, so the same binary runs locally, in
  Docker Compose and in Kubernetes.
- **Honesty over hype:** Redis Cluster is *not* strongly consistent. It can lose
  acknowledged writes during failover or partitions. We don't hide that. We measure it,
  explain it, and show where it happens. That's the most impressive part of the project.
- Keep functions small, name things clearly, and add comments that explain *why*,
  not *what*.

## Repo layout (grow into this)

```
cmd/shardis/          server binary
cmd/shardis-loadgen/  Zipf "cache-aside" load generator (hit rate, latency)
cmd/shardis-crashtest/ crash a real server mid-load, count lost acknowledged writes
cmd/shardis-observer/ streams cluster events over WebSocket for the website
cmd/shardis-chaos/    runs chaos scenarios + consistency checker, records traces
internal/resp/        protocol parser/writer
internal/server/      TCP listener, per-connection loop, command table
internal/config/      flags + SHARDIS_* env config
internal/store/       in-memory keyspace (one map per hash slot), engines (sharded locks / executor), expiry, eviction
internal/keyslot/     CRC16 + hash tags → slot (verified against real Redis)
internal/persist/     AOF (effects log, group commit, fsync policies), manifest, fuzzy rewrite
internal/replication/ PSYNC, backlog, full/partial sync, WAIT, promotion
internal/cluster/     topology snapshots, cluster bus (wire, gossip), failure detection + failover, nodes.conf
internal/events/      event bus
internal/faults/      deterministic fault injection (FAULT SET repl.stream pause|delay|sever)
pkg/client/           our own "smart" cluster client
deploy/local/         6-node cluster as local processes (cluster.sh, topology.conf)
deploy/compose/       docker-compose 6-node cluster (Dockerfile at repo root)
deploy/k8s/           Kubernetes manifests (StatefulSet etc.)
website/              showcase site (React + Vite + TypeScript)
docs/decisions/       design decision records
docs/benchmarks/      benchmark results (raw + summary)
docs/learning-log.md  all iteration recaps
ROADMAP.md            iteration plan
```

## Useful commands (keep this section updated as we add things)

```
make help         # list all targets
make build        # build binaries (bin/shardis, bin/shardis-loadgen, bin/shardis-crashtest)
make test         # go test -race ./...
make test-memory  # heap-measuring tests (skipped under -race; CI runs both)
make lint         # go vet + gofmt check (what CI runs first)
make fmt          # format all Go code
make fuzz         # fuzz the RESP parser (FUZZTIME=60s per target)
make bench        # Go microbenchmarks
make run          # single node on 127.0.0.1:7000 (config: --flag > SHARDIS_* env > default)
                  # --engine sharded (default) | executor   (ADR 005)
                  # --hz 10   active-expiry cycles per second (ADR 008)
                  # --maxmemory 64mb --maxmemory-policy allkeys-lru (ADR 011-013)
                  #   (warns below 32mb: the 16384 slot maps alone use ~9-14 MB)
                  # --appendonly yes --appendfsync everysec --dir /data  (ADR 014-016)
                  # --replicaof "host port"  --enable-fault-injection yes (testing only)
                  # --cluster-config deploy/local/topology.conf --cluster-node a   (seed file, ADR 021-023)
                  # --cluster-enabled yes   (fresh node; form with CLUSTER MEET/ADDSLOTS/REPLICATE
                  #   or redis-cli --cluster create; state saved in <dir>/nodes.conf, ADR 024-026)
                  # --cluster-node-timeout 15000 (failure detection, ADR 027-029)
                  # --cluster-require-full-coverage yes|no
SHARDIS_MEMTEST=1 go test ./internal/store -run TestMemoryPerKey -v   # heap bytes per key
bash docs/benchmarks/iter-01-ping/run.sh          # PING benchmark vs real redis-server
bash docs/benchmarks/iter-02-concurrency/run.sh   # engines vs real Redis (~8 min)
bash docs/benchmarks/iter-02-concurrency/gc.sh    # memory per key + GC pauses with ~1M keys
bash docs/benchmarks/iter-03-expiry/storm.sh      # 1M-key expiry storm vs Redis (~2 min)
bash docs/benchmarks/iter-04-eviction/run.sh      # eviction hit rates vs Redis (~8 min)
bin/shardis-loadgen -addr 127.0.0.1:7000          # Zipf cache-aside traffic: hit rate, latency
bin/shardis-crashtest -runs 10                    # kill -9 / simulated power cut, lost acked writes
bash docs/benchmarks/iter-05-persistence/run.sh   # crash table + throughput per fsync policy (~10 min)
bin/shardis-crashtest -failover plain,wait,slowlink,slowlink+wait   # replication lost-write window
bin/shardis-crashtest -cluster-failover 2000,5000 -runs 20          # automatic failover time
bash docs/benchmarks/iter-09-failover/run.sh      # the failover timing table (~7 min)
bash docs/benchmarks/iter-06-replication/run.sh   # the full failover table (~4 min)
bash docs/benchmarks/iter-07-cluster/run.sh       # single node vs cluster, Shardis and Redis (~3 min)
SHARDIS_GOSSIP_BENCH=$PWD/docs/benchmarks/iter-08-gossip/raw.csv go test ./internal/cluster -run TestGossipScaling -v -timeout 20m
```

macOS note: AirPlay Receiver listens on *:7000. Binding 127.0.0.1:7000 (the default) works;
`--host 0.0.0.0` on port 7000 fails. Benchmarks use ports 7100 / 6399 to stay clear of it.
Benchmark note: don't use `redis-benchmark --threads`; it reports throughput in coarse
steps (e.g. always 114,220 req/s) and is slower than the single-threaded client.

```
make cluster-up-local    # 6 local processes on 127.0.0.1:7000-7005 (deploy/local/topology.conf)
make cluster-down-local
make cluster-up          # 6 containers with docker compose (deploy/compose), same ports
make cluster-down
redis-cli -c -p 7000     # -c follows MOVED; use timeouts in scripts (AirPlay owns *:7000)
```
