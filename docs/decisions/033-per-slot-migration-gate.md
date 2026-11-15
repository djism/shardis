# 033: A per-slot gate keeps "is the key still here?" atomic during migration
- Context: While a slot migrates, the source node answers each command one of two ways:
  - It runs the command, if it still has the keys.
  - It replies `-ASK` (try the target), if the keys are gone.

  Redis runs one command at a time, so nothing can happen between "check the key" and "run the
  command". Shardis runs commands in parallel (256 lock groups, ADR 005). That allows this:
  1. A client's `INCR k` checks that `k` is still on the source.
  2. MIGRATE copies `k` to the target and deletes it.
  3. The `INCR` runs and recreates `k` on the source, where nobody will look again.
  The client got "OK", but its write is gone. This is the "keep in mind: atomic ops" constraint,
  in concrete form.
- Options considered:
  1. **A per-slot reader/writer lock (the "gate").** Every keyed command holds its slot's gate
     *shared* from routing to the end of execution. MIGRATE and `CLUSTER SETSLOT NODE` hold it
     *exclusively*.
  2. **Do the key check inside each command's own store transaction.** This is the tightest
     scope, but every command handler needs a hook, and the store's API gets more complicated.
  3. **Hold the store lock across MIGRATE's network round trip.** Simple, but it blocks one lock
     group, which is 64 slots, while waiting on the network, not just the migrating slot.
- Decision: Option 1. That's 16384 `sync.RWMutex` (~384 KB per server). Every keyed command takes
  one in cluster mode, not just commands for migrating slots: a command that routed *just before*
  `SETSLOT MIGRATING` could otherwise still slip through.
- Why:
  - **Measured, not assumed.** `TestMigrateUnderConcurrentWrites` has 8 clients (go-redis and
    `pkg/client`) INCR 200 counters while their slot migrates in batches of 10, then checks
    every counter against the acknowledged INCRs.
    - With the gate: 10 of 10 runs exact, 13–19k INCRs each, 0 lost.
    - Gate switched off: **10 of 10 runs lost acknowledged INCRs** (e.g. "key 3 = 127, but 128
      INCRs were acknowledged").
  - **Cheap.** An uncontended `RLock`/`RUnlock` is two atomic adds. Commands on different slots
    never touch the same lock, and only the migrating slot ever waits.
  - **Lock order is fixed:** gate first, then store locks, everywhere. MIGRATE takes several gates
    in ascending slot order, so two MIGRATEs can't deadlock.
- Tradeoffs we accept:
  - While a batch is in flight, commands for that one slot wait one MIGRATE round trip, about
    100 keys. That's a latency bump on that slot, visible in the resharding benchmark's p99.
  - Every keyed command pays the RLock, even when no migration is running.
  - A hot key in a migrating slot makes MIGRATE wait for that key's readers (Go's RWMutex lets a
    waiting writer in before new readers, so it can't starve).
- What real Redis does: It doesn't need this: one thread runs one command at a time, and
  MIGRATE blocks the whole server during its round trip. Our gate gives the same
  "either here or there, never neither" guarantee, while blocking only one slot instead of the
  whole server.
- At bigger scale I'd: Keep it. It's the standard "readers share, migrator excludes" pattern
  (row/range locks in databases). A system that migrates by streaming whole slots (see ADR 032)
  needs only a short exclusive handoff per range instead of one per batch.
