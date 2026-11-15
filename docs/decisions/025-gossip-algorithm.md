# 025: Redis's gossip schedule: sampled pings, ~10% of the cluster per message
- Context: Every node must learn about every other node (membership) and stay roughly current
  about each one's health, without the message count exploding as the cluster grows. A new
  node introduced to just one member must become known to all.
- Options considered:
  1. **Redis's schedule:**
     - 10 times a second, each node connects to any known node it has no link to.
     - Once a second, it samples 5 random nodes and pings the one it heard from least recently.
     - It pings anyone it hasn't heard from in node_timeout/2.
     - Every PING/PONG carries gossip about ~1/10 of the cluster (at least 3 nodes), and hearing
       about an unknown node starts a handshake with it.
  2. **Everyone pings everyone every period:** simplest and freshest, but N² messages per period.
  3. **SWIM** (used by HashiCorp memberlist, Consul): ping one random member per period; if it
     doesn't answer, ask k others to probe it indirectly before suspecting it; piggyback
     membership changes on probes. A constant message rate per node, and fewer false alarms.
- Decision: Option 1, matching the system we model. Details that turned out to matter:
  - **Reconnect a link whose PING has gone unanswered for node_timeout/2.** Without this, a node
    whose PONG was lost was *never pinged again*, because we only ping when no ping is pending.
    `TestBusDropFault` caught it: after dropping messages, `a` never heard from `b` again, even
    once the fault was cleared. The time of the oldest unanswered PING is kept across
    reconnects, because failure detection (Iteration 9) measures exactly that.
  - **Only a MEET introduces a node directly.** A stray PING from a stranger gets a PONG but
    doesn't join it. Gossip about unknown nodes is only accepted from confirmed members.
  - **`CLUSTER FORGET` keeps a 60 s "forgotten" list,** or the next PING mentioning the node would
    bring it straight back (`TestForgetStaysForgotten`).
  - **Equal config epochs on two primaries:** the one with the smaller node ID bumps the current
    epoch and takes it (Redis's collision rule), so every primary ends up with a distinct epoch
    without coordination (`TestEpochCollisionsAreResolved`).
- Why ([benchmarks](../benchmarks/iter-08-gossip/SUMMARY.md)):
  - A chain of 5 MEETs became a fully known 6-node cluster in ~1.5 s (in-process and as real
    processes).
  - `redis-cli --cluster create` works against Shardis unmodified: it MEETs, waits for agreement
    and checks the result (`TestRedisCLIClusterCreate`).
- Tradeoffs we accept:
  - **Per-node traffic grows with N** (2 → 5.3 → 13.4 msgs/s at 6/20/50 nodes), so the cluster
    total grows with N², dominated by the node_timeout/2 rule. SWIM's constant per-node rate
    scales better.
  - **Convergence slows with size** (~11–16 s for 50 nodes from a chain).
  - **Failure suspicion will be direct only** (Iteration 9): a node that can't reach X marks it,
    with no indirect probe. Redis makes up for that with a majority vote before marking FAIL;
    SWIM uses indirect probes.
- What real Redis does: The same schedule (`clusterCron`, `clusterSendPing` with
  `wanted = max(3, N/10)` gossip entries, the reconnect-after-half-timeout rule, MEET-only
  joining, the forget blacklist, and the epoch collision rule). We reimplemented it from the
  spec's description, not the source.
- At bigger scale I'd: Look hard at SWIM-style indirect probing and a constant fanout. Send deltas
  instead of full slot bitmaps. Make the node timeout a deliberate tradeoff knob: bigger means
  less traffic, but slower failure detection.
