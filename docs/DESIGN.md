# Shardis: Design Document

**Status:** implemented (Iterations 0–18) · **Code:** `github.com/djism/shardis` ·
**Decision records:** [docs/decisions](decisions/) (51) · **Measurements:** [docs/benchmarks](benchmarks/)

## 1. Context

Redis Cluster is the most widely deployed sharded cache design. Its ideas are simple to
state: hash slots, gossip, replica election. Their consequences are subtle: when exactly is
an acknowledged write lost, and how long does a partition last? Shardis re-implements that
design from the public specs (RESP, the Redis Cluster spec), in Go. It's compatible enough
that unmodified Redis tools and clients work, and it's instrumented so every failure mode can
be reproduced, measured and shown.

## 2. Goals and non-goals

**Goals**
- **Wire compatibility:** RESP2, CRC16-XMODEM mod 16384 with hash tags, exact `-MOVED`/`-ASK`
  formats, and `CLUSTER SLOTS`/`SHARDS`/`NODES`. `redis-cli -c`, `redis-benchmark`, `go-redis`
  and `redis-cli --cluster create` work unchanged.
- **Per-command atomicity** with a multi-core engine (Redis gets it for free from one thread).
- **Automatic failure handling:** detect, agree, elect and promote with no human or external
  coordinator. Detection takes about node-timeout plus 1–2 s.
- **Live resharding** under load with zero client-visible errors.
- **Measured honesty:** every way of losing an acknowledged write is reproduced and counted.
- **Operable:** runs as local processes, under Docker Compose, and as a Kubernetes
  StatefulSet. It has Prometheus metrics and an event stream for visualization.

**Non-goals**
- **Strong consistency or linearizability.** It's the same AP-leaning trade as Redis Cluster
  (§7).
- **Data types beyond strings, MULTI/EXEC, pub/sub, Lua and RESP3.**
- **Auth and TLS:** it assumes a trusted network, like default Redis.
- **Beating Redis in general.** Comparisons exist to explain the costs, not to win.

## 3. Overview

```
             clients (redis-cli, go-redis, pkg/client): learn slot map, follow MOVED/ASK
                    │ RESP over TCP
   ┌────────────────┼──────────────────────────────────────────────┐
   │  primary A     ▼   slots 0–5460       primary B    primary C    │
   │  ┌──────────────────────────────┐                               │
   │  │ RESP ─► dispatch ─► slot gate│◄─ cluster bus (TCP, binary) ─►│  gossip, PFAIL/FAIL,
   │  │            │                 │    every node ↔ some nodes     │  votes, epochs
   │  │   store (16384 maps,         │                               │
   │  │   256 lock groups) ─► AOF    │                               │
   │  │            └─► repl backlog ─┼──► replica A′  (async, PSYNC)  │
   │  └──────────────────────────────┘                               │
   └─────────────────────────────────────────────────────────────────┘
```

One binary (`cmd/shardis`) plays every role. A node is a primary or a replica according to
the cluster state, which is held as a copy-on-write topology snapshot. That snapshot is updated
by gossip and persisted per node in `nodes.conf`.

## 4. Detailed design

### 4.1 Protocol and connections (`internal/resp`, `internal/server`)
- **One goroutine per connection.** Replies are buffered and flushed only before the next
  blocking read, so pipelines cost one syscall per batch, not per command (ADR 003).
- **The parser has hard limits** (bulk size, array length, inline length). A protocol error
  closes the connection instead of trying to resynchronize (ADR 004). Three fuzz targets cover
  it.
- **The command table carries key positions and flags,** so routing, `COMMAND` and the client
  library share one source (ADR 022, 038).

### 4.2 Keyspace and concurrency (`internal/store`)
- **One map per hash slot (ADR 006),** so migrating, counting or dumping a slot never scans the
  whole keyspace.
- **256 lock groups (slot mod 256) instead of a single executor goroutine (ADR 005).** A
  command locks the groups of all its keys in a fixed order, so it's atomic and can't deadlock.
  MSET is all-or-nothing, and INCR never loses an update. Pipelined, this beats Redis by
  1.2–1.4×; unpipelined, it's 9% slower (goroutine wake-up per request).
- **Memory:** 108 Go-heap bytes for a key with 32 bytes of payload; about 3× Redis per key in
  RSS. GC stop-the-world pauses stayed under 0.2 ms with ~1M keys.

### 4.3 Expiry and eviction
- **Expiry:** lazy on access, plus Redis's sampled active cycle (`--hz`). A per-slot TTL index
  makes sampling O(sample), not O(keyspace) (ADR 008, 009). A 1M-key expiry storm peaked GET
  p99 at ~1.7 ms, against Redis's 3.4 ms.
- **Eviction:** maxmemory is enforced against our own size estimate, not the Go heap, which is
  noisy and lags (ADR 011). Eviction uses sampled approximate LRU (ADR 012) and runs
  synchronously before a write takes its locks (ADR 013). Hit rate at 32 MB on Zipf traffic is
  77.2% vs Redis's 77.0%.

### 4.4 Persistence (`internal/persist`)
- **The AOF logs effects, not commands (ADR 014):** `INCR` is logged as the resulting `SET`,
  and a relative TTL becomes an absolute deadline. Replaying is deterministic and idempotent.
- **Group commit (ADR 015):** a write is acknowledged only after its batch reaches the kernel
  (and the disk, for `always`). `everysec` costs ~11% of throughput; `always` costs 20×, the
  same as Redis.
- **Rewrite** takes a fuzzy snapshot while writes continue. It's correct because replaying the
  log tail over the snapshot is idempotent (ADR 016).
- **Measured:** kill -9 with `everysec` lost 0 of 2.1M acknowledged writes. A simulated power
  cut lost 0.3–0.9 s of writes, within the documented "≤ 1 s".

### 4.5 Replication (`internal/replication`)
- **The primary streams its effect log** through one shared backlog. Replicas resume with
  Redis-style PSYNC (replication ID + offset), and fall back to a full sync that streams a
  snapshot straight over the socket (ADR 017, 018).
- **Asynchronous by default:** the primary acknowledges before the replica has the write.
  `WAIT n timeout` lets a caller wait for n replicas.
- **Replicas hide expired keys but never delete them;** only the primary's deletions replicate
  (ADR 019).
- **Measured on failover:** plain async lost 4 of 2.4M acknowledged writes, and `WAIT 1` lost
  0. With a slow replica link, it lost 1.5M of 2.3M, so lag is the real risk.

### 4.6 Cluster membership and gossip (`internal/cluster`)
- **A binary bus protocol on port+10000.** Every message carries the sender's epochs, role and
  full 16,384-bit slot bitmap (ADR 024).
- **Redis's gossip schedule (ADR 025):** once a second, each node samples 5 peers and pings
  the one it heard from least recently. It also pings anyone silent for half the node timeout.
  Every PING/PONG carries gossip about ~1/10 of the cluster (at least 3 nodes), so a new node
  introduced to one member soon becomes known to all.
- **Config epochs resolve slot conflicts:** the higher epoch wins. Formation is done by
  `CLUSTER MEET`/`ADDSLOTS`/`REPLICATE`, `redis-cli --cluster create`, or our
  `shardis-admin create` (ADR 021, 044).

### 4.7 Failure detection and failover
```
 t=0          t≈timeout           t≈timeout+gossip        +rank delay        +vote round
 A dies ──► peers mark PFAIL ──► majority of primaries ──► replica A′ asks ──► majority grants
            (local suspicion)    agree → FAIL broadcast     for votes          → A′ primary,
                                                            (epoch+1)            bumps epoch
```
- **PFAIL → FAIL needs a majority of primaries** reporting within 2 × node timeout (ADR 027).
- **Replicas are ranked by replication offset,** so the most up-to-date one asks first. Each
  primary votes once per epoch (ADR 028). A replica that's been cut off too long won't run
  (ADR 029).
- **Measured (median of 20 runs):** 2.26 / 3.07 / 7.17 s at 1 / 2 / 5 s timeouts.
- **Planned handoff:** on SIGTERM a primary pauses writes, waits for its replica to catch up,
  and triggers `CLUSTER FAILOVER TAKEOVER` before exiting (ADR 043). That took 13 ms, with 0
  writes lost across a full rolling restart (1.1M ops).

### 4.8 Partitions
- **A primary that can't reach a majority of primaries for node-timeout stops serving** and
  waits before serving again after it rejoins (ADR 030). This bounds, but doesn't remove, the
  window in which two primaries both accept writes for the same slots (§7).

### 4.9 Live resharding
- **The classic protocol:** `SETSLOT MIGRATING/IMPORTING`, then `MIGRATE` keys in batches
  (DUMP/RESTORE with a versioned, CRC-64-checked payload, ADR 034), then `SETSLOT NODE`.
  Clients get `-ASK` for moved keys and `-MOVED` afterwards (ADR 032). The importing node
  bumps its epoch unilaterally to claim the slot (ADR 035).
- **The per-slot gate (ADR 033):** concurrent execution creates a race Redis doesn't have.
  A command checks that a key is still here, MIGRATE moves it, and then the command recreates
  it on the old node. So every keyed command holds its slot's gate shared, and MIGRATE holds
  it exclusive. A test with 8 concurrent clients INCRing 200 counters while their slots moved
  found zero lost increments.
- **Measured:** 4,096 slots rebalanced onto a new node under load in 18.9 s with 0 client
  errors. Redis 8.10's atomic slot migration took 3.1 s.

### 4.10 Client (`pkg/client`)
- **The client keeps a slot map** refreshed on MOVED, handles ASK/ASKING, and keeps per-node
  connection pools. It reads key positions from `COMMAND` (ADR 038). Replica reads are opt-in
  (ADR 039).
- **Retry policy by safety (ADR 036):** after an unknown outcome (a timeout or a dropped
  connection), the client resends only idempotent commands by default. Retrying everything
  doubled 32 of 32 in-flight INCRs in the pause experiment.
- **Retries have a time budget,** with exponential backoff and full jitter (ADR 037).

### 4.11 Observability and operations
- **The event bus costs ~nothing when idle** and drops events rather than blocking (ADR 007).
  `EVENTS` streams it as JSON (ADR 041). The observer fans it out to browsers over WebSocket
  (ADR 046), and the chaos tool records it as traces.
- **Prometheus metrics are read at scrape time.** The hot path only bumps per-connection
  counters, and latency is sampled 1 in 16 (ADR 045). There's also a Grafana dashboard.
- **Kubernetes:** a StatefulSet gives stable DNS identities and a PVC per pod. A headless
  Service, a PDB (max 1 unavailable), a bootstrap Job, and a NetworkPolicy for partition demos
  complete the setup (ADR 044).

## 5. Alternatives considered (the big ones)

| Decision | Chosen | Rejected, and why |
|---|---|---|
| Concurrency | 256 lock groups | One executor goroutine: simplest, but it caps throughput at one core (benchmarked, ADR 005) |
| Membership | Gossip + epochs | Raft for slot ownership: no split-brain window, but a different system from Redis Cluster (ADR 027/035; "at bigger scale") |
| Durability unit | Effects log + group commit | Command log: non-deterministic replay (TTL, INCR on missing keys) |
| Resharding | Key batches + ASK | Whole-slot streaming (Redis 8): 6× faster, saved for future work |
| Client retries | Retry-safe by default | Retry everything: duplicates non-idempotent writes, which we measured |
| Website | Labelled simulations + recorded replays | Replays only (unreadable cold) or animations only (unprovable), ADR 050 |

## 6. API and compatibility

Supported: the string, key, TTL and server commands that `redis-benchmark` and cache-aside
clients use, plus `CLUSTER` (SLOTS, SHARDS, NODES, INFO, MYID, MEET, FORGET, ADDSLOTS,
REPLICATE, SETSLOT, FAILOVER, COUNTKEYSINSLOT, GETKEYSINSLOT, KEYSLOT), `MIGRATE`, `DUMP`/`RESTORE`, `ASKING`,
`READONLY`, `WAIT` and `INFO`. Keyslot vectors are checked against real Redis and shared with
the browser (ADR 048).

## 7. Failure modes

| Failure | What happens | Bound / measured | Mitigation |
|---|---|---|---|
| Primary crash | In-flight replication is lost | ≤ 146 acked writes per failover | `WAIT` for writes that matter |
| Lagging replica + crash | Everything not yet replicated is lost | 1.5M of 2.3M in the slow-link test | monitor lag; `WAIT` |
| Partition, primary in minority | It acks writes until it notices; they're rolled back on heal | ~node-timeout of writes (~215k per run locally; 2,029 on k8s) | shorter timeout; `WAIT`; Raft at scale |
| Stale reads after failover | Readers can briefly see the old value | 130 stale reads in the k8s partition | primary reads by default |
| Long GC / SIGSTOP pause | Peers may fail the node over; the client sees timeouts | 1.5 s pause tested | retry-safe client; on resume the higher epoch wins and the old primary steps down |
| Unknown outcome + blind retry | Non-idempotent command applied twice | 32 / 32 INCRs doubled | RetrySafe default |
| Crash mid-reshard | Slot left MIGRATING/IMPORTING | detected by `shardis-admin nodes` | `shardis-admin fix` |
| Power cut | Unsynced AOF tail lost | ≤ 1 s with `everysec` | `appendfsync always` (20× cost) |
| Too-short node timeout | False failovers under load | trade-off measured in iter-09 | default 15 s; 2 s in demos |

The chaos suite's history checker reports **rolled back** writes (an acknowledged write a
later read didn't see), not only final-state losses. In the partition scenario the final
state looks perfect while hundreds of writes were rolled back. That distinction is the most
important number in the project.

## 8. Testing and measurement

- **Unit and integration:** `go test -race ./...` (185 test functions). In-process 3+3
  clusters (`internal/testcluster`) cover failover, partitions and migration.
- **Fuzzing:** RESP parser (3 targets), bus wire format, DUMP decoder.
- **Compatibility:** `redis-cli`, `redis-benchmark` and `go-redis` cluster client in tests and
  benchmarks.
- **Crash and failover testers** (`shardis-crashtest`): kill -9, simulated power cuts,
  failover timing, partition windows, client chaos, all counting acknowledged-but-lost writes.
- **Chaos suite** (`make chaos`): 7 scenarios on real processes with deterministic fault
  injection (ADR 020, 031, 042). Result: 0 client errors in 1.2M operations. Losses appear
  only where the design says they will.
- **Kubernetes demos:** failover, rolling restart, partition and scale, each verified by a
  checker Job.

## 9. Future work

1. **Raft for slot ownership** (or a metadata service), to close the split-brain window.
2. **Whole-slot streaming migration** (snapshot plus change stream), ~6× faster rebalances.
3. **Request IDs with server-side dedup,** so every command is safe to retry.
4. **Delta gossip / SWIM-style partial views** for clusters of hundreds of nodes.
5. **A Kubernetes operator** that rolls pods only when replicas are in sync and finishes
   half-moved slots.
6. **Zone-aware replica placement** and bounded-staleness replica reads.
7. **More data types and MULTI/EXEC within a slot.**
