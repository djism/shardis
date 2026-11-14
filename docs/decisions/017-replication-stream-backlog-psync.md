# 017: Replicate the effect log through one shared backlog, with Redis-style PSYNC
- Context: A replica must end up with exactly the primary's data and stay current. After a short
  disconnect it should catch up cheaply, without re-copying gigabytes. After a failover, the other
  replicas should be able to follow the new primary without a full copy either.
- Options considered:
  1. **What to send:** the effect log (ADR 014) vs commands as received. Effects are
     deterministic (absolute TTLs, `DEL` for every expiry and eviction), so we reuse them as-is.
  2. **How to fan out to replicas:** one shared ring buffer (the backlog) with a sender goroutine
     per replica reading at its own offset, vs a separate output queue per replica.
  3. **How to resume:** a replication ID plus byte offset (PSYNC), vs always full-syncing on
     reconnect.
- Decision: effects; one shared 64 MB backlog (`--repl-backlog-size`); PSYNC with PSYNC2's
  second ID.
  - Every byte of the stream has an offset. The replica re-encodes each record it receives into
    its own backlog, and the encoding is canonical, so it holds the same bytes at the same offsets.
  - `PSYNC <replid> <offset>`: if the replica is on our history and its offset is still in the
    backlog → `+CONTINUE` and just the missing bytes. Otherwise → `+FULLRESYNC <replid> <O>`
    (ADR 018).
  - On promotion (`REPLICAOF NO ONE`), the node keeps its backlog, moves its old ID to `replid2`
    and records the offset where it took over. Replicas of the old primary that are no further
    along can then `CONTINUE` with it.
  - The primary puts a `PING` into the stream every `--repl-ping-ms`, so replicas can tell a quiet
    primary from a dead one. Replicas `REPLCONF ACK <offset>` every second and whenever they see
    `REPLCONF GETACK`.
- Why:
  - **Bounded memory:** one buffer however many replicas. A replica that falls further behind
    than the backlog is dropped and full-resyncs (`TestFullResyncWhenBacklogOverflows`).
  - **Cheap reconnects:** `TestPartialResyncAfterDisconnect` severs the link, writes 100 keys,
    and the replica catches up with a partial resync (still 1 full sync total).
  - **Cheap failovers:** after promotion, the remaining replica partially resyncs to the new
    primary (`TestPromotedReplicaServesPartialResync`: 0 full syncs served).
  - **One format end to end:** AOF, backlog and wire all carry the same bytes.
- Tradeoffs we accept:
  - **Replication is asynchronous.** The primary acknowledges a write before any replica has it.
    If it dies in that gap and a replica is promoted, the write is gone (Demo A). That's by design,
    and it's the main thing this iteration measures.
  - A second global mutex (the backlog) on every write's path, next to the AOF's.
  - A slow replica holds no memory of its own. It just falls behind and eventually needs a full
    resync, which costs the primary a snapshot.
  - No chained replication (a replica of a replica) yet.
- What real Redis does: The same design. A replication ID and offset, `repl-backlog-size`
  (default 1 MB), `PSYNC` with `+CONTINUE` / `+FULLRESYNC`, PSYNC2's `master_replid2` and
  `second_repl_offset`, PING in the stream, `REPLCONF ACK`/`GETACK`. Redis also keeps a
  per-replica output buffer (capped by `client-output-buffer-limit replica`), where we read
  straight from the backlog. Our wire stream carries effects, not Redis's exact commands, so a
  real Redis replica can't follow Shardis (or vice versa).
- At bigger scale I'd: Use per-slot streams (no global sequence), add chained replication to
  offload a primary with many replicas, and compress the stream for cross-region replicas. Size
  the backlog from measured write rate × the longest disconnect we want to survive cheaply.
