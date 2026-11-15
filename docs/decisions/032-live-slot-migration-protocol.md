# 032: Live resharding moves keys one batch at a time, with MIGRATING/IMPORTING and -ASK
- Context: Adding a primary means moving about a quarter of the slots, and their keys, to it
  while clients keep reading and writing. During the move, every key has to be reachable at all
  times, and no acknowledged write may be lost.
- Options considered:
  1. **Key-by-key migration (classic Redis Cluster).**
     - The target marks the slot IMPORTING, then the source marks it MIGRATING.
     - Keys move in batches with `MIGRATE`. A client asking the source for a key that's already
       gone gets `-ASK <slot> <target>` and retries once there, sending `ASKING` first.
     - At the end, `SETSLOT NODE` flips ownership (with an epoch bump, ADR 035) and clients get
       `MOVED`.
  2. **Atomic slot migration (Redis 8.2+, what Redis 8.10's `redis-cli --cluster rebalance`
     does).**
     - The source streams a snapshot of a whole *range* of slots to the target over a
       replication-like channel, then streams the writes made since.
     - Writes pause for a moment, and the whole range changes owner in one step.
     - No ASK: a key is always on exactly one node, as far as clients can tell.
  3. **A proxy layer** that forwards requests during the move, so clients never see redirects.
- Decision: Option 1. It's what the Redis Cluster spec describes, what every cluster client
  understands, and it's built only from pieces we already have. The order of the steps is part
  of the design:
  1. **Target IMPORTING before source MIGRATING.** Otherwise the source sends `-ASK` to a target
     that answers "not mine, MOVED back to the source", and the client loops.
  2. **Target `NODE` before source `NODE`.** Then the source keeps sending `-ASK`, which the
     target accepts, until it too says `MOVED`. The other way round, there'd be a moment when
     the source says MOVED-to-target and the target says MOVED-to-source.
  3. **The source refuses `NODE <other>` while it still has keys in the slot**, so a key can't be
     stranded on a node nobody routes to.
- Why:
  - **It works with every client.** go-redis and our `pkg/client` both follow ASK and MOVED.
    `TestMigrateUnderConcurrentWrites` migrates a slot under 8 writers with 0 errors and every
    acknowledged INCR accounted for.
  - **Zero client errors under load.** In the benchmark,
    [iter-11-resharding](../benchmarks/iter-11-resharding/SUMMARY.md) adds a 4th primary to a
    1M-key cluster under 50 clients and moves 4096 slots, about 250k keys, with **0 errors**.
    The clients see a few hundred MOVED and a few dozen ASK redirects.
  - **Each step is a plain command, so a crash in the middle is recoverable:** the markers are
    saved in nodes.conf, and `shardis-admin fix` finishes half-moved slots.
- Tradeoffs we accept:
  - **It's slow.** Ours took 15–19 s for 4096 slots and 250k keys. That's about 10 round trips
    per slot, plus one MIGRATE round trip per 100 keys. Redis 8.10's atomic migration did the
    same rebalance in about 3 s, because it moves whole ranges in one stream.
  - **Multi-key commands can get TRYAGAIN** while their keys are split across two nodes, and
    the client has to retry.
  - **Clients must understand ASK.** Dumb clients (plain `redis-cli` without `-c`) see the
    redirect errors.
  - **Replicas lag behind by a second or two.** They learn new owners only from gossip, so
    `redis-cli --cluster check` briefly reports disagreement right after a rebalance. Redis
    pushes UPDATE messages to stale nodes. We reserved that message type but haven't built it.
  - A failover in the middle of a migration loses the MIGRATING/IMPORTING markers on the
    promoted replica, since they aren't replicated. Redis has the same gap. It's a chaos scenario
    for Iteration 13.
- What real Redis does: Both. The classic protocol (`SETSLOT IMPORTING/MIGRATING`, `MIGRATE`,
  `ASK`) is still supported and is what older tooling uses. Redis 8.2 added atomic slot
  migration (`CLUSTER MIGRATION`), which `redis-cli --cluster rebalance` now uses. We implement
  the classic one, with the same command names and error formats, so `redis-cli -c` and cluster
  clients work.
- At bigger scale I'd: Move to range streaming, like Redis 8.x, CockroachDB's range splits or
  Vitess's resharding: snapshot plus change stream, then a short handoff. That's fewer round
  trips, no ASK, and per-key work stays on the data path instead of the control path. Throttle
  it, so a rebalance can't eat the bandwidth clients need, and let it resume from a checkpoint.
