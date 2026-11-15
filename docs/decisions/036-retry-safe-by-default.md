# 036: The client resends after an unknown outcome only if the command is safe to repeat
- Context: A client sends `INCR k`, and then the connection dies: the node crashed, or it froze
  past our timeout. Did the INCR run? Maybe. The client can't know. Resending risks applying it
  twice, and not resending risks telling the app "failed" when it actually happened. This is the
  oldest problem in distributed systems ("exactly-once is impossible over an unreliable
  network"), and every cluster client has to pick a side. Some failures are *definite*, though:
  - a dial that failed
  - a reply of `MOVED`, `ASK`, `TRYAGAIN`, `CLUSTERDOWN` or `READONLY`

  In those cases the command didn't run, and resending is always safe.
- Options considered:
  1. **RetrySafe:** always retry definite failures. After an unknown outcome, resend only reads
     and *idempotent* writes, meaning running them twice leaves the same state and gets the same
     reply (`SET` without NX/XX/GET, `DEL`, `MSET`, `EXPIREAT`, ...). Everything else returns
     `ErrUnknownOutcome` and the app decides.
  2. **RetryAll (at-least-once):** resend everything. What go-redis does by default (v9.23:
     `MaxRetries` 3; `shouldRetry` retries `io.EOF` and read timeouts for every command).
  3. **RetryNever:** surface every unknown outcome.
- Decision: Option 1 as the default. 2 and 3 are available as `Options.Retry`.
- Why (measured with `shardis-crashtest -client-chaos`, see
  [iter-12-client](../benchmarks/iter-12-client/SUMMARY.md)): 32 writers INCR counters while a
  primary freezes for 1.5 s (SIGSTOP, past the client's 500 ms timeout), another primary is
  killed, it comes back, and the cluster is rebalanced.
  - **RetryAll applied INCRs twice: 32 of the 32 in flight during the freeze, in 5 of 5 runs.** The client timed
    out and resent on a new connection, then the frozen node woke up and ran the original
    command still sitting in its socket buffer. The app saw zero errors and wrong counters.
  - **RetrySafe** reported those 32 INCRs as `ErrUnknownOutcome` and applied nothing twice, in
    5 of 5 runs.
    Every other command (thousands of reads and SETs) was retried invisibly.
  - Some subtle cases the allowlist catches:
    - `SET k v NX` changes no state when repeated, but its *reply* does: the second run says
      "not set", and the app would believe its lock wasn't taken.
    - Relative `EXPIRE k 10` pushes the deadline later each time it runs.

    Both are treated as unsafe.
- Tradeoffs we accept:
  - The app has to handle `ErrUnknownOutcome` for non-idempotent writes. That's honest, but it's
    work: read the value back, use a request ID, or accept the risk.
  - The allowlist is conservative: commands it doesn't know count as unsafe.
  - Duplicates are prevented only where the client can see the risk. A server-side dedup
    (idempotency keys) would cover every command, but Redis has no such feature.
- What real Redis clients do: Mostly at-least-once by default (go-redis, as above). Some offer
  per-command `NoRetry`. Ours inverts the default: safe unless you opt in to at-least-once.
- At bigger scale I'd: Make writes idempotent by design. Clients attach a unique request ID, and
  the server remembers recent IDs per client and drops repeats, the way Kafka's idempotent
  producer or Stripe's idempotency keys work. Then RetryAll is safe for everything.
