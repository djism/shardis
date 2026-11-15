# 049: In replays, traffic dots split the recorded request rate by slot ownership
- Context: The hero animation shows client traffic flowing into the primaries. A chaos trace
  records nodes' internal events and the client's *total* operations per 100 ms (the
  timeline), not which node served each request.
- Options considered:
  1. **Split the recorded total across serving primaries in proportion to the slots they
     own,** and say so in a footnote. The workload's keys are uniform over slots, so this is a
     good estimate, but it is an estimate.
  2. **Record per-node samples:** add `EVENTS SAMPLE n` (1 in n per-key write events) and
     re-record the traces, so every dot is a real request a real node served.
- Decision: Option 1, for this iteration (a deliberate call, revisited if per-node truth matters).
- Why:
  - **No server change, no new traces,** and the overall rate, its dips and its errors are all
    real numbers from the run.
  - **Live mode doesn't need the estimate:** the observer reports each node's measured
    commands per second (from `INFO`'s `total_commands_processed`), and the dots there are
    per-node truth.
- Tradeoffs we accept:
  - **The only inferred element on the page:** a hot slot wouldn't show up as one busy node in
    a replay. It's labelled under every replay ("Traffic dots show the recorded request rate,
    split across primaries by the slots they own").
  - During a failover, the split goes to whoever owns the slots *in the replayed state*. Dead
    and failed nodes get no dots, so the picture follows the model, not the client's actual
    retries.
- What real Redis does: Not applicable.
- At bigger scale I'd: Do option 2 (sampled per-node events), so replays and live mode show
  the same, measured thing.
