# 043: On SIGTERM a primary hands its slots to its replica before exiting
- Context: Kubernetes stops pods all the time: rolling upgrades, node drains, rescheduling. It
  sends SIGTERM, waits `terminationGracePeriodSeconds`, then SIGKILLs. If a primary just exits,
  the cluster sees a crash:
  - Its slots are unavailable for node timeout + an election (Iteration 9: ~3 s at 2 s, ~16 s
    at Redis's default 15 s).
  - Writes still in flight to the replica are lost.

  A rolling restart of 6 pods would do that 3 times, on purpose.
- Options considered:
  1. **Coordinated handoff:**
     1. Pause writes atomically. Every slot gate is taken exclusively, so in-flight writes
        finish, and new ones get `TRYAGAIN`, which clients retry.
     2. `WAIT 1` until the replica has everything.
     3. Tell the replica `CLUSTER FAILOVER TAKEOVER`: it claims the slots with an epoch bump
        and no vote.
     4. Wait until our own view shows the new owner (clients still connected now get MOVED),
        then exit.
  2. **A full manual failover with votes** (Redis's default `CLUSTER FAILOVER`): the replica
     runs an election that the primaries accept even though its primary isn't failed. Safer if
     the old primary isn't trustworthy, but more protocol.
  3. **Do nothing:** let failure detection handle it. Simple, slow, and it loses writes.
- Decision: Option 1, on by default for cluster primaries. If no replica catches up within 5 s,
  the primary shuts down anyway, and ordinary failover follows.
- Why:
  - **Zero acknowledged writes lost, measured.** `TestGracefulHandoff` INCRs counters
    throughout: 0 lost, 0 doubled, 0 errors, 3 of 3 runs. The handoff takes 22–24 ms, and the
    slowest client request 28–37 ms. On minikube, `kubectl rollout restart` of all 6 pods under
    the checker gave 0 rolled-back writes and 0 stale reads over 1.1M operations, with
    `graceful handoff done took=13ms` in each primary's log.
  - **TAKEOVER without votes is safe *here*:** the old primary has stopped accepting writes and
    is about to exit, so there's no second writer to split-brain with. The same move on a live,
    unpaused primary would be a forced split brain, so Shardis only supports TAKEOVER, and only
    the handoff uses it.
  - **The pause is free in normal operation:** it reuses the per-slot gates from ADR 033.
    Taking all 16384 exclusively *is* the pause.
- Tradeoffs we accept:
  - **Writes to the stopping primary's slots get `TRYAGAIN` for a few milliseconds.** Smart
    clients retry (ours and go-redis do). A naive client sees an error.
  - **After a rolling restart, primaries and replicas have swapped places.** Nothing moves them
    back. That's harmless, but unexpected for anyone who assumed `shardis-0` is a primary.
  - **If the replica is lagging badly** (it just restarted and is still syncing), the 5 s wait
    can expire. The primary then exits without a handoff and the cluster falls back to a
    timeout failover.
- What real Redis does: Redis 7 has `shutdown-on-sigterm` with a `failover` option and
  `SHUTDOWN FAILOVER`: pause writes, wait for a replica to catch up, trigger a manual failover,
  then exit. The same idea. Redis's manual failover uses a vote (FORCEACK), ours uses TAKEOVER
  because the old primary is guaranteed paused.
- At bigger scale I'd: Use a vote-based manual failover (Option 2), so even a misbehaving old
  primary can't keep a claim. Add a readiness gate that marks a replica Ready only once it's in
  sync, so a StatefulSet never stops a primary right after restarting its replica. Pick the
  replica with the highest offset when there are several.
