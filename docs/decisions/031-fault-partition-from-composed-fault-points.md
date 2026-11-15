# 031: FAULT PARTITION is built from the existing per-link fault points
- Context: To test and measure split brain we need repeatable network partitions: cut node `a`
  off from `b`–`f` on the cluster bus *and* the replication stream, while clients can still
  reach `a`. It has to work in Go tests, local processes, Docker and (later) Kubernetes.
- Options considered:
  1. **Compose existing faults:** `FAULT PARTITION <names...>` makes each node add
     `bus.message drop` and `repl.stream sever` rules for every node on the other side.
     `FAULT HEAL` clears them. Client connections are untouched.
  2. **Network-level tools:** iptables/pf rules, `tc netem`, or Toxiproxy between nodes.
     More realistic (kernel-level drops), but needs root or extra containers, and differs per OS.
  3. **A separate in-process "network" for tests only:** precise, but doesn't run against real
     processes.
- Decision: Option 1 now. Network-level faults come in Iteration 14 (Kubernetes chaos).
- Why:
  - **Deterministic and portable:** the same command works on macOS, in Docker and in `go test`,
    with no root. Every node gets the same command, and each decides locally which side it's on.
  - **It's only a few lines,** because Iteration 6 and 8 already built the fault points
    (ADR 020). A partition is just "drop to these IDs, sever to these addresses".
  - Link IDs had to match: replication links are now keyed by the address the replica announces
    (`REPLCONF ip-address`), so `repl.stream sever` targets the same internal address the cluster
    uses.
- Tradeoffs we accept:
  - **It's not a real network fault.** TCP connections stay open, and messages are dropped by our
    own code. Kernel timeouts, half-open connections and asymmetric routes aren't exercised.
  - **Every node has to get the command.** A node that misses it doesn't participate, so the
    tooling sends it to all six and checks each reply.
  - Behind `--enable-fault-injection`, off by default, like every fault hook.
- What real Redis does: Nothing comparable built in. The Redis test suite pauses processes
  (`DEBUG SLEEP`, SIGSTOP) or kills them, and Jepsen-style tests use iptables. Our approach is
  closer to what a test harness does than to anything in Redis itself.
- At bigger scale I'd: Use network-level chaos (Chaos Mesh / Toxiproxy / iptables in Kubernetes)
  for realism, and keep the in-process hooks for fast, deterministic unit tests. Both are worth
  having.
