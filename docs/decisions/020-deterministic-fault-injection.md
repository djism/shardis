# 020: Deterministic fault injection through named points and an admin command
- Context: The interesting failures in a distributed system ("primary dies after acking, before
  replicating"; "one replica lags while another is current") almost never happen on demand.
  Tests, the chaos suite (Iteration 13) and the website's live demos all need to cause them on
  purpose, repeatably.
- Options considered:
  1. **Named fault points in our own code** (e.g. `repl.stream`), each checking a registry of
     rules: pause, delay, sever. Rules are set and cleared with an admin `FAULT` command.
  2. **Network-level tools** (`tc netem`, iptables, toxiproxy): no code changes and realistic,
     but they need root or extra processes, they don't run in `go test`, and they can't target
     "the replication stream to replica B" without port tricks.
  3. **Random failures** (drop 1% of messages): finds unknown bugs, but every run is different,
     so a failure can't be replayed or shown step by step.
- Decision: Option 1, deterministic rules:
  - `internal/faults` holds the registry. A nil or empty registry costs one atomic load per check.
  - The registry only exists, and the `FAULT` command only works, with
    `--enable-fault-injection yes`. Off by default, and the server logs a loud warning when on.
  - First point: `repl.stream`, checked by the primary *after* new data arrives and *before*
    sending it to a replica. Rules can target one replica by its `host:port`.
- Why:
  - **The lost-write demos are ordinary, repeatable tests:** pause the stream → write → kill the
    primary → promote → the write is gone (Demo A). Pause only replica B → `WAIT 1` is
    satisfied by A → promote B → the confirmed write is gone too (Demo C).
  - **The website's live mode can trigger the same faults** through the same command.
  - **It caught two real bugs while being built:**
    - The check originally ran *before* waiting for data, so a pause set while the sender waited
      let the next write slip through.
    - The test harness released pauses before closing links, so a "crashing" primary still
      delivered the data it was holding back. A real crash flushes nothing.
- Tradeoffs we accept:
  - Faults only exist where we've placed a check. Unplaced paths (the AOF, client sockets) can't
    be broken this way yet.
  - It's not how real networks fail (no partial packets, no reordering). Network-level chaos
    (option 2) still has value for Kubernetes demos (Iteration 14).
  - Being off by default and loud when on is our only protection against misuse. Never enable it
    in production.
- What real Redis does: Redis has `DEBUG` subcommands (`DEBUG SLEEP`, `DEBUG SET-ACTIVE-EXPIRE`,
  `DEBUG RELOAD`), and its test suite uses them plus process kills and `CLIENT PAUSE`. It has no
  general fault-point registry. Jepsen-style testing of Redis uses network-level partitions (option 2).
- At bigger scale I'd: Add fault points on the cluster bus (Iteration 8: drop or delay gossip
  between specific nodes), on the AOF writer (slow fsync, write errors) and on client
  connections. Record every fault in the event stream, so a chaos trace shows exactly what was
  injected and when. Then pair it with randomized schedules from a seed: random enough to find
  surprises, deterministic enough to replay any failure.
