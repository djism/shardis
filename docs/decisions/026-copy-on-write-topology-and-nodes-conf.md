# 026: A copy-on-write topology snapshot, and each node saves its own view
- Context: Every keyed command reads the slot map to route (Iteration 7), hundreds of thousands of
  times a second across many goroutines. Gossip changes the map rarely (joins, slot moves,
  failovers), but now it *does* change at runtime. Separately, a node that restarts must come
  back as the same node, with its ID, peers and slot claims, or the cluster sees a stranger.
- Options considered:
  1. **Copy-on-write snapshots:** the gossip side keeps mutable state under a mutex and, after each
     change, builds a fresh immutable `Topology` and swaps it in with `atomic.Pointer`. Readers do
     one atomic load and never lock.
  2. **One shared structure behind an RWMutex:** simpler, but every command takes the read lock,
     and RWMutex read locks contend on a shared counter across cores.
  3. **Persistence:** a per-node `nodes.conf` (like Redis) vs keeping only the static seed file.
- Decision: Option 1 for reads; a per-node `nodes.conf` for identity:
  - **Snapshot rebuild:** O(nodes + 16384), triggered by membership, flag, slot or epoch changes,
    and by fresh ping/pong timestamps.
  - **Each command takes one snapshot** for its whole routing decision, so it never sees half an
    update.
  - **`<dir>/nodes.conf`** holds this node's random ID, the current epoch, and every confirmed
    peer with its addresses, role, config epoch and slots. It's saved atomically (temp file,
    fsync, rename, fsync dir) within 100 ms of a change.
  - **On startup:** `nodes.conf` if present, else the seed file, else a fresh node that knows only
    itself. The node's own addresses always come from the command line.
- Why:
  - **Routing stays lock-free:** one atomic load per command, whatever gossip is doing.
  - **Restarts keep identity:** `TestStateSurvivesRestart` stops a node, starts it again from its
    `nodes.conf`, and checks the same ID, all peers, its slots, and links back up.
  - **Its own addresses aren't trusted from disk,** because an IP can change when a pod is
    rescheduled (Iteration 14).
- Tradeoffs we accept:
  - **Snapshots can be stale for a moment:** a command may route on the map from a few
    microseconds ago. That's fine; MOVED corrects it, as it does for client-side caches.
  - **A bug found while building:** at first, ping/pong timestamps changed without publishing a new
    snapshot, so `CLUSTER NODES` showed `pong-recv 0` forever. Timestamps now trigger a publish
    (a few per second, cheap).
  - **Each rebuild allocates a new snapshot.** Fine at a few per second; it would matter at
    thousands.
  - **`nodes.conf` is per node and written by the server.** Editing it by hand while running gets
    overwritten.
- What real Redis does: Redis keeps one mutable cluster state on its single thread (no readers
  to protect), and persists it to `nodes.conf` (the `cluster-config-file`), rewritten after
  changes with fsync. Same file idea and the same "own address from config" rule. The
  copy-on-write part is ours, because we serve commands from many goroutines.
- At bigger scale I'd: Version the snapshot (an epoch number), so clients and logs can tell which
  map a decision used. Persist through the same atomic-write helper as the AOF. In Kubernetes,
  keep `nodes.conf` on the pod's persistent volume, so a rescheduled pod keeps its identity.
