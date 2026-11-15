# 035: The importing node bumps its config epoch without asking anyone
- Context: At the end of a slot migration the target runs `CLUSTER SETSLOT <slot> NODE <itself>`.
  Every other node has to learn that it now owns the slot. Ownership conflicts are settled by
  config epoch (ADR 025): a version number for a primary's slot claims, where the higher number
  wins. So the target's claim has to come with an epoch higher than the old owner's, or gossip
  would ignore it and keep sending clients to the source.
- Options considered:
  1. **Bump without consensus:** if the target's config epoch isn't already the unique highest,
     it sets `currentEpoch + 1` on its own and claims the slot. Gossip spreads it.
  2. **Ask for votes,** like a failover (ADR 028): a majority of primaries must agree to the
     new epoch. Safe even when two nodes bump at once, but it adds a round of votes per migrated
     slot, or per batch.
  3. **No epoch change:** rely on the tool telling every node directly. Any node the tool
     can't reach would never learn.
- Decision: Option 1, like Redis.
- Why:
  - **Migration is coordinated by one tool, not contended.** The source has already agreed to
    give the slot up (MIGRATING, emptied by MIGRATE, then `SETSLOT NODE` on itself). There's
    nobody to outvote.
  - **It only bumps when needed.** After the first imported slot the target holds the unique
    highest epoch, so the next 4095 slots of a rebalance cost no new epochs.
    `TestMigrateSlotStepByStep` checks the bump, and that node `c`, which the test never told,
    learns the new owner by gossip alone.
- Tradeoffs we accept:
  - **Two nodes can pick the same epoch.** For example, a failover and an import can happen at
    once. Then neither claim beats the other. The epoch-collision rule (the node with the smaller
    ID moves to a fresh epoch, ADR 025) breaks the tie, but for a moment two nodes can claim the
    same slot. Redis documents the same risk.
  - Epochs grow with resharding activity, not only with failovers. That's harmless: they're
    64-bit.
- What real Redis does: The same thing (the cluster spec calls it "config epoch bump without
  consensus"), used for `SETSLOT NODE` on the importing node and for manual `CLUSTER FAILOVER
  TAKEOVER`. We match it, including the collision handling.
- At bigger scale I'd: Put ownership changes through a consensus log (Raft-replicated metadata,
  as in CockroachDB range descriptors, or a separate config service), so a slot handoff is one
  committed entry and two owners at once can't happen, even briefly.
