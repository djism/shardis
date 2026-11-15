# 040: The consistency checker uses registers with increasing values, not a linearizability check
- Context: "Did the cluster lose data?" needs an exact answer, not a feeling. The chaos suite
  records every operation the clients ran (start time, end time, result). It then has to decide
  which results are wrong. Redis Cluster doesn't promise linearizability (every operation
  appearing to happen at one instant between its start and end), so a checker that demands it
  would just say "no" every time. We want to measure the specific guarantees that *should* hold,
  and count how often the ones that don't hold actually fail.
- Options considered:
  1. **Registers with increasing values.**
     - Each writer owns its keys and writes 1, 2, 3, ... to each, one at a time, so a key's value
       can only legitimately go up.
     - Then four questions are exact and cheap:
       - **lost:** an acked write whose value isn't in the final state
       - **rolled back:** an acked write that a later read didn't see
       - **stale read:** a read lower than a value acked before the read began
       - **phantom:** a value nobody wrote
  2. **A full linearizability checker** (Knossos / Porcupine style). The most general option, but
     NP-hard in general, slow on 100k+ operations, and it only answers "linearizable or not",
     which we already know is "not".
  3. **Counters (INCR) only:** finds lost or doubled increments (we did this in Iteration 12),
     but can't detect stale reads.
- Decision: Option 1, with per-key sequential writers, concurrent readers, and a final read of
  every key after the cluster has healed.
- Why:
  - **Every number means one thing and can be checked by hand:** `TestCheckerFindsAnomalies`
    builds a history with exactly one lost write, one stale read and one phantom, and the
    checker must find exactly those.
  - **"Rolled back" was added after the first real run fooled us.** The partition scenario
    showed 251 stale reads but **0 lost writes**. The writes acknowledged by the cut-off primary
    were lost, but writers kept writing those keys after the heal, so the final values were
    newer writes and covered the losses. A final-state-only check would have certified a
    split brain as harmless. Counting acked writes that a later read didn't see catches it.
  - **It's linear time** in the history size, so 300k-op runs check in milliseconds.
- Tradeoffs we accept:
  - **Only single-key properties.** No multi-key or transaction anomalies (Shardis has no
    transactions), and no causal chains across keys.
  - **"Stale" is judged against real time on one machine's clock.** Fine here (one host).
    Across hosts, clock skew would need care.
  - **Reads go to primaries,** so stale reads measure failover and split-brain effects, not
    replica lag (that was measured separately in ADR 039).
- What real Redis / others do: Jepsen tested Redis (and Redis-Raft) with similar register and
  set workloads and the Knossos linearizability checker. Our checker is a deliberately smaller,
  faster cousin. We don't run the Jepsen framework itself.
- At bigger scale I'd: Add Elle-style anomaly detection (Jepsen's checker that infers
  dependency cycles from list-append histories), track per-operation causality, and run the
  suite on several hosts with clock-skew injection.
