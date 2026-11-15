# 027: Detect failures with a timeout (PFAIL) confirmed by a majority of primaries (FAIL)
- Context: A cluster must notice a dead primary on its own, quickly, and without overreacting. A
  node that is slow, or reachable from most nodes but not one, must not be failed over: a failover
  loses the unreplicated writes (Iteration 6), so a false alarm has a real cost.
- Options considered:
  1. **Redis's two-level design.** Each node marks a peer PFAIL ("I can't reach it") when its
     oldest unanswered PING is older than `node_timeout`. Gossip carries every node's PFAIL/FAIL
     view. When a majority of primaries report a node (counting ourselves if we're a primary), it
     becomes FAIL, and a `FAIL` message makes everyone accept that at once.
  2. **SWIM:** when a direct probe fails, ask k random members to probe indirectly before
     suspecting. Fewer false positives from one bad link, and a constant probe rate per node.
  3. **Phi-accrual** (Cassandra, Akka): track each peer's heartbeat interval distribution and
     suspect when the current silence is statistically unlikely. Adapts to jittery networks with
     no fixed timeout.
- Decision: Option 1:
  - Failure reports come only from primaries and expire after 2 × timeout.
  - A replica, or a primary without slots, has FAIL cleared as soon as it talks again. A primary
    that still owns its slots has FAIL cleared only if nobody took over within 2 × timeout.
- Why:
  - **One bad link can't cause a failover:** `TestNoFailoverWithoutMajority` cuts a ↔ b only. b
    suspects a, nobody else does, and a keeps its slots.
  - **A dead primary is noticed by everyone:** `TestPrimaryFailureTriggersFailover` saw FAIL
    spread and a failover complete ~2.3 s after a kill with a 1 s timeout.
  - **It reuses what we had:** gossip entries already carried PFAIL/FAIL flags (Iteration 8).
- Bugs that taught us something:
  - **A node that dies right after answering a PING never looked unreachable.** Its links broke,
    reconnecting failed, and so no PING was ever left unanswered: there was nothing to time. Fix
    (Redis does this too): when connecting fails, count it as a PING sent now.
  - **Message counters were sized for 5 message types,** so adding the election messages crashed
    the bus on the first vote request. Caught by the first failover test.
- Tradeoffs we accept:
  - **Detection takes at least `node_timeout`,** plus gossip time for reports to reach a majority.
    A smaller timeout detects faster but risks false alarms on a slow network or during a long GC
    pause. 15 s is Redis's default.
  - **A minority of primaries can't declare FAIL alone.** If most primaries are down, nothing fails
    over: the cluster chooses consistency over availability here (more in Iteration 10).
  - **No indirect probing,** so a node with a flaky link to most primaries can still be failed over.
- What real Redis does: The same: PFAIL after `cluster-node-timeout`, failure reports in gossip
  sections from masters, valid for 2 × timeout, FAIL on a master majority, broadcast as a FAIL
  message, and the same FAIL-clearing rules. Including the "count a failed connect as a sent
  ping" detail.
- At bigger scale I'd: Add SWIM-style indirect probes before suspecting. Use phi-accrual instead of
  one fixed timeout, so cross-zone links with more jitter don't need a huge timeout everywhere.
  Feed suspicion events into monitoring: a PFAIL that never becomes FAIL is an early sign of a
  bad network path.
