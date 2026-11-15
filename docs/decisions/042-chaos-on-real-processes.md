# 042: Chaos scenarios run on real processes, with faults from signals and FAULT commands
- Context: The chaos suite has to reproduce the failures that matter (crash, restart, network
  partition, flapping links, a stalled process, a crash in the middle of resharding) in a way
  that's repeatable, runs on a laptop and in CI, and is close enough to real life to trust.
- Options considered:
  1. **Real processes plus in-band faults:**
     - Seven `shardis` processes with AOF on.
     - Crashes are `kill -9`, stalls are `SIGSTOP`/`SIGCONT`.
     - Partitions use `FAULT PARTITION`/`HEAL` (ADR 031), sent to every member.
  2. **The in-process test cluster (`internal/testcluster`):** fast, but "stopping" a node is a
     clean shutdown inside one Go process. No real kill, no SIGSTOP, no AOF replay.
  3. **Containers plus network tools** (Docker / Kubernetes with iptables, `tc`, Chaos Mesh): the
     most realistic network faults, but it needs root or a cluster, and it's slower and harder
     to make deterministic.
- Decision: Option 1 for `make chaos`. Network-level faults on Kubernetes come in Phase 5.
- Why:
  - **The process faults are real:** `kill -9` gives no chance to flush or say goodbye, and
    restarts replay the AOF and reload nodes.conf. `SIGSTOP` freezes every goroutine at once,
    like a long GC pause or a VM being descheduled, which is what produces client timeouts and
    retry duplicates (Iteration 12).
  - **Repeatable:** every scenario is a short script of timed actions, and the actions are
    written into the trace (`chaos` events) next to what each node reported.
  - **It found real problems while being built:**
    - the "rolled back" blind spot in our own checker (ADR 040)
    - a split-brain overlap under load (Iteration 12)
    - `admin.Fix` couldn't finish a slot whose source had died and failed over: it now uses
      the slot's current owner as the source
- Tradeoffs we accept:
  - **The partitions are simulated inside our code:** messages are dropped by the receiving
    node's fault registry, not by the kernel. Half-open TCP connections and asymmetric routes
    aren't exercised.
  - **One host, loopback network:** replication lag is microseconds, so loss windows are much
    smaller than on a real network. The numbers are best cases.
  - **Each scenario runs once per `make chaos`.** The runs are short (about 20 s of traffic), so
    rare races may not show up every time. Run it more often (or in CI) to catch them.
- What real Redis does: Redis's own test suite kills and pauses processes (`DEBUG SLEEP`,
  `kill`) in Tcl tests. Jepsen's Redis analyses used real VMs with iptables partitions. We sit
  between the two.
- At bigger scale I'd: Run the same scenarios continuously against a staging cluster on
  Kubernetes with Chaos Mesh (network faults, disk stalls, clock skew). Gate releases on the
  report (e.g. "rolled-back writes per failover must not grow"), the way Netflix's Chaos
  Monkey and FoundationDB's simulation testing are used.
