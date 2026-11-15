# 029: A replica that's been cut off too long doesn't run for primary
- Context: A replica's data is only as fresh as its last contact with its primary. If it lost the
  link minutes ago and the primary then dies, promoting it would quietly throw away every write
  since. That's worse than staying down, because clients see old data as if it were current.
- Options considered:
  1. **Skip the election** if the replica's replication link has been down for more than
     `node_timeout + 10 × node_timeout` (Redis's `cluster-replica-validity-factor`, default 10).
     The slots stay down until a fresher replica or the primary comes back.
  2. **Always fail over:** maximum availability, unbounded data loss.
  3. **Fail over, but only for reads:** a mode clients don't understand.
- Decision: Option 1, factor 10. The replica logs and emits `failover_skipped` (rate-limited).
- Why:
  - **Failover exists to keep serving the same data,** not to bring back an old copy. Iteration 6
    showed even a fresh replica loses the in-flight window; a stale one loses far more.
  - `TestStaleReplicaDoesNotRunForPrimary`: with the replica's link down for an hour, the primary
    goes FAIL, nobody takes over, and the cluster reports `cluster_state:fail` instead of serving
    an old copy.
- Seen for real: while checking the docs' demo, a replica that had *never* synced (its first
  connection had gone to macOS's AirPlay listener on port 7000) correctly refused to promote
  itself when its primary was killed. The shard stayed down rather than come back empty. A
  never-synced replica is treated as infinitely stale.
- Tradeoffs we accept:
  - **Availability is lost on purpose:** if the only replica is stale, that shard stays down until
    a human acts (bring the primary back, or force a failover).
  - **"Too stale" is measured by link-down time, not by how much data is missing.** A replica with
    a broken link to an idle primary might be perfectly current but still be refused.
- What real Redis does: The same check (`cluster-replica-validity-factor`; 0 disables it, so
  replicas always run). Redis computes the data age as link-down time minus `node_timeout` and
  compares it to `repl-ping-replica-period + node_timeout × factor`.
- At bigger scale I'd: Base it on missing offset when possible (how far behind the primary's last
  known offset), make the factor per-shard (some data prefers availability, some correctness), and
  page a human when a shard stays down because every replica is stale.
