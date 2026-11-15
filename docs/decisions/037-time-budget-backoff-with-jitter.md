# 037: Retries use a time budget, exponential backoff and full jitter
- Context: During a failover the cluster says "not now" for several seconds. The dead primary
  refuses connections, other nodes answer `CLUSTERDOWN` until the replica takes over, and then a
  demoted node answers `READONLY`. A client must neither give up too early (an error the app
  didn't need to see) nor hammer the cluster (50 clients retrying in lockstep).
- Options considered:
  1. **A time budget** (`MaxRetryTime`, default 20 s) with exponential backoff (5 ms, 10 ms, ...,
     capped at 500 ms) and **full jitter**: each wait is random between 0 and that cap.
  2. **A retry count** (go-redis v9.23: `MaxRetries` 3, backoff 10 ms–1 s): simple, but "3 tries" means
     a different amount of time depending on why the tries failed.
  3. **Fixed delay:** easy to reason about, but synchronized clients stay synchronized.
- Decision: Option 1. Each attempt also has its own I/O timeout (`Timeout`, default 5 s), capped
  by what's left of the budget, and the caller's `context` deadline always wins.
- Why:
  - **The right unit is time.** A failover takes about node timeout + 1–2 s (Iteration 9: 3.1 s
    at 2 s). A 20 s budget covers a failover at the default 15 s timeout too. In our tests the
    client rode through a 3–4 s failover with zero errors (`TestFailoverIsInvisibleForSafeCommands`:
    163 network errors absorbed).
  - **Full jitter spreads the herd.** When a primary dies, every client fails at the same
    instant. With jitter, their retries arrive spread over the wait window instead of in waves.
    (AWS's "Exponential Backoff and Jitter" analysis is the classic reference.)
  - **It gives up cleanly when it should:** with a shard truly gone (primary and replica dead),
    `TestRetryBudget` gets `ErrRetryBudget` after 1.5 s (its budget) with the last error attached,
    instead of hanging.
- Tradeoffs we accept:
  - **During a failover, requests to that shard are slow, not failed:** up to the failover time.
    Latency-sensitive apps should pass a shorter `context` deadline and fail fast.
  - **Retries add load right when the cluster is weakest.** The 500 ms cap and jitter limit it,
    but there's no global retry budget (a token bucket shared across requests).
- What real Redis clients do: go-redis v9.23 counts retries (3) with 10 ms–1 s backoff. Ours is
  time-based instead.
- At bigger scale I'd: Add a client-wide retry budget (e.g. retries may be at most 10% of
  requests, as in gRPC and Finagle) and circuit breakers per node, so a dying cluster isn't
  finished off by its own clients' retries.
