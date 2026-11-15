# 028: Fail over by replica election: ranked delay, one vote per primary per epoch
- Context: Once a primary is FAIL, one of its replicas must take over its slots. Exactly one:
  two winners would both accept writes for the same slots (split brain). The choice should be
  the most up-to-date replica, to lose the fewest acknowledged writes (Iteration 6, Demo C).
- Options considered:
  1. **Redis's election:**
     - Replicas of the failed primary wait `500 ms + random(0–500 ms) + rank × 1 s`, where rank 0
       is the replica with the highest replication offset.
     - The replica then bumps `currentEpoch` and asks every primary for a vote.
     - A primary serving slots votes at most once per epoch, and only if the requester's primary
       is FAIL, it hasn't voted for that primary's replicas in 2 × timeout, and the requester's
       view of the slots isn't older than its own.
     - A majority of primaries wins. The winner takes the election epoch as its config epoch,
       promotes, and claims the slots.
  2. **A cluster-wide leader** elected with Raft makes every failover decision. Easier to reason
     about, but a whole consensus layer and log to add.
  3. **An external watcher** (Redis Sentinel, a Kubernetes operator): another highly-available
     system to deploy and trust.
- Decision: Option 1:
  - Replication offsets travel in every bus header, so replicas can rank themselves.
  - The vote is written to `nodes.conf` (fsynced) *before* the ACK is sent, so a primary that
    crashes and restarts can't vote twice in one epoch.
  - The winner announces itself right away. Every node adopts its claim because its epoch is
    higher (the Iteration 8 rule).
  - The old primary's other replicas follow the winner, and the old primary, when it returns,
    finds its slots claimed with a higher epoch and turns itself into a replica.
- Why ([benchmarks](../benchmarks/iter-09-failover/SUMMARY.md)):
  - **Majority + one vote per epoch means at most one winner per epoch.** Two replicas can't both
    get a majority from the same voters in the same epoch. That's the safety argument, and
    `TestVoteOncePerEpochAndPersist` checks it, across a restart too.
  - **The most up-to-date replica wins:** `TestMostUpToDateReplicaWins` gives one replica offset
    5000 and the other 100. The 5000 one runs first and wins, and the other follows it.
  - **Rejoins are automatic:** `TestOldPrimaryComesBackAsReplica`.
  - **End to end with real processes:** a dead primary's slots take writes again ~3 s after
    `kill -9` with a 2 s node timeout (20-run p50/p99 in the benchmark summary).
- Compared with Raft: the same core ideas. Terms ≈ epochs, one vote per term, a majority wins, and
  "most up-to-date log wins" ≈ ranking by replication offset. The difference: Raft elects one
  leader for a replicated *log* and only commits entries a majority has stored, so it never loses
  an acknowledged write. Redis Cluster elects a new owner for a *shard*, and replication is
  asynchronous, so the winner may be missing recent writes. Redis gets fast, simple failover and
  pays with possible write loss. Raft pays with a majority round trip on every write.
- Tradeoffs we accept:
  - **Lost writes on failover:** anything the dead primary acknowledged but hadn't replicated is
    gone (measured per run in the benchmark).
  - **The delay before the election is never below ~500 ms,** and rank adds a second per more
    up-to-date replica.
  - **A failed election waits 2 × auth timeout before retrying.** Two replicas starting at the same
    instant can split the votes so neither wins, and the next round then takes several seconds.
  - **A majority of primaries must be reachable.** With half the primaries down, no failover
    happens.
- What real Redis does: The same algorithm: `clusterHandleReplicaFailover`, rank by replication
  offset, `FAILOVER_AUTH_REQUEST`/`ACK`, `lastVoteEpoch` saved with fsync before voting, the "voted
  for this master recently" guard, and the slot config-epoch check. Redis also has manual
  failover (`CLUSTER FAILOVER`) for zero-loss planned switchovers. We'll want that for rolling
  upgrades in Iteration 14.
- At bigger scale I'd: Add `CLUSTER FAILOVER` (planned, no write loss: pause clients, let the
  replica catch up, then switch) and use it for every deploy. Expose election events and timings
  as metrics. For data that must not be lost, pair it with `WAIT` (Iteration 6), or use a
  consensus-replicated store for that subset.
