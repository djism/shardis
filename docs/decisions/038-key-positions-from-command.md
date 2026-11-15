# 038: The client learns key positions and read-only flags from COMMAND
- Context: To route a command, the client needs its keys. For `GET k` that's argument 1. For
  `MSET a 1 b 2` it's every other argument, and some commands have no key at all. The client
  also needs to know which commands only read, to send them to replicas (ADR 039) and to resend
  them safely (ADR 036).
- Options considered:
  1. **Ask the server once with `COMMAND`.** It returns every command's name, flags
     (`readonly`/`write`) and first/last/step key positions, in the same format on Shardis and
     on Redis.
  2. **A hardcoded table** in the client: no round trip, but it goes stale when the server adds
     commands.
  3. **Always assume `args[1]`**, which is what the Iteration 11 client did: wrong for MSET (only
     the first key would be checked) and for keyless commands.
- Decision: Option 1, loaded at connect time. If `COMMAND` fails, fall back to option 3.
- Why:
  - The server is the source of truth: Shardis's command table already holds key positions for
    its own routing (ADR 022), and `COMMAND` just publishes them.
  - **Multi-key commands are checked before sending:** `MSET x 1 y 2` with keys in different
    slots fails locally with `ErrCrossSlot`, with no round trip and no server `CROSSSLOT` reply
    (`TestRetryBudget` checks it).
  - The same `readonly` flag drives replica reads and retry safety, so there's one source of
    truth for both.
- Tradeoffs we accept:
  - One extra round trip at startup (~1 ms).
  - Commands whose keys aren't at fixed positions (`EVAL` with `numkeys`, `XREAD ... STREAMS`)
    need Redis 7's "key specs". We don't parse those yet, and Shardis has no such commands.
- What real Redis clients do: The same. go-redis's cluster client loads `COMMAND` into a
  `cmdsInfoCache` and routes by it.
- At bigger scale I'd: Parse Redis 7 key specs for full coverage, and refresh the command table
  when a node reports a newer version (rolling upgrades can add commands mid-flight).
