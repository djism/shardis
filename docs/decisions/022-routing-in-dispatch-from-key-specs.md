# 022: Route in command dispatch, using key positions from the command table
- Context: In cluster mode, a command for a slot this node doesn't serve must not run. The client
  gets `-MOVED <slot> <host:port>` instead (or `-CROSSSLOT` if its keys span slots). Every
  command, current and future, must be checked.
- Options considered:
  1. Route centrally in dispatch. Each command-table entry declares where its keys are (first
     key, last key, step, Redis's convention), and dispatch extracts them, computes the slot, and
     redirects before the handler runs.
  2. Check inside each command handler.
  3. Route in a separate proxy in front of the nodes (like Twemproxy or Envoy's Redis filter).
     Clients stay simple, but it's an extra hop, and it isn't how Redis Cluster works.
- Decision: Option 1:
  - Keys spanning slots → `CROSSSLOT Keys in request don't hash to the same slot`.
  - Slot owned by another primary → `MOVED <slot> <client-addr>`.
  - Commands without keys (`KEYS`, `SCAN`, `DBSIZE`, `INFO`) run node-locally.
  - A replica serves reads for its own primary's slots only on connections that sent `READONLY`.
    Writes always get `MOVED` to the primary.
  - Each redirect emits a `moved` event for the website's redirect animation.
- Why:
  - **One place, no forgotten checks.** A new command only needs its key positions to route
    correctly. Forgetting them in a per-handler design (option 2) would silently write a key on
    the wrong node.
  - **The same data powers `COMMAND`,** which now returns real entries (name, arity, flags, first
    key, last key, step). Smart clients like go-redis use it to compute slots themselves and go
    straight to the right node. `TestGoRedisClusterClient` routes 1,000 keys, pipelines and
    hash-tagged `MSET` across 3 primaries.
  - **Cheap:** one CRC16 (~10 ns) per key and an array lookup.
  - Exact error formats, so `redis-cli -c` follows `MOVED` (`TestRedisCLIFollowsMoved`).
- Tradeoffs we accept:
  - **Key positions are declared by hand per command.** A wrong declaration is a routing bug, so
    `TestCommandListsKeyPositions` pins the important ones.
  - **Commands whose keys aren't at fixed positions** (like `EVAL`'s `numkeys`) will need Redis's
    richer "key specs". We have none of those commands yet.
  - Routing happens before the command's own validation, so a malformed command for a foreign
    slot gets `MOVED` rather than its syntax error. Redis behaves the same.
- What real Redis does: The same. `getNodeByQuery` uses each command's key specs to find the slot,
  returns `-CROSSSLOT`/`-MOVED`/`-ASK`, and honors `READONLY` on replicas. `COMMAND` exposes the
  same data (Redis 7 adds detailed key specs; we report the classic first/last/step form, which
  clients still read).
- At bigger scale I'd: Add Redis 7-style key specs for commands with computed key positions, and
  `-ASK` redirects during slot migration (Iteration 11).
