# 024: A binary cluster bus format with the slot bitmap in every message
- Context: Nodes need their own channel to exchange heartbeats, membership and slot ownership,
  separate from client traffic. Every message is sent many times a second across the cluster,
  so size and parse cost matter. Bytes arrive from the network, so the decoder must survive
  garbage.
- Options considered:
  1. Our own binary layout: a fixed header (sender ID, epochs, flags, primary ID, addresses), a
     2 KB slot bitmap, and length-prefixed gossip entries, framed by magic + version + length.
  2. JSON: readable on the wire, but ~5–10x larger, slower to parse, and awkward for a
     16384-bit bitmap.
  3. Protobuf: compact with schema evolution, but needs a dependency outside our allowed list.
- Decision: Option 1 (layout documented at the top of `internal/cluster/wire.go`), on its own port
  (client port + 10000, as in Redis). Types PING, PONG and MEET are used now; FAIL and UPDATE are
  reserved for Iterations 9–10.
- Why:
  - **Every heartbeat carries the sender's full slot claim** (2 KB bitmap) and config epoch, so
    ownership converges and heals with no separate sync protocol (`TestSlotsSpreadByGossip`).
  - **Small and cheap:** a PING with gossip about 5 nodes is ~2.6 KB, decoded with a few bounds
    checks.
  - **A defensive decoder:** every length is checked against the bytes remaining, total size is
    capped at 1 MB, and the gossip count is checked against what could possibly fit. It was fuzzed
    for 20 s with no panics, and "anything accepted re-encodes to something accepted".
  - **A version byte** leaves room to change the format later.
- Tradeoffs we accept:
  - **Not human-readable on the wire.** The `gossip_send`/`gossip_recv` events give the readable
    view, and they feed the website.
  - **The 2 KB bitmap is sent even when nothing changed,** most of every message. Per-node traffic
    is ~13 messages/s at 50 nodes (~34 KB/s).
  - **No authentication.** Anyone who can reach the bus port can MEET into the cluster. Keep the
    bus on a private network (in Compose it isn't published).
  - **Not compatible with Redis's bus format:** real Redis nodes and Shardis nodes can't form one
    cluster. Client compatibility is what we aim for.
- What real Redis does: The same idea and similar fields. `clusterMsg` has a fixed header (sender
  name, slot bitmap `myslots[16384/8]`, config/current epoch, flags, primary name, IP/ports),
  then gossip sections, with message types PING/PONG/MEET/FAIL/UPDATE/PUBLISH/... and a
  signature + version. Redis 7 added extension fields (hostnames, forgotten nodes, shard IDs).
- At bigger scale I'd: Send the slot bitmap only when it changes (or a hash of it, with the full
  bitmap on mismatch), compress gossip, add TLS and authentication on the bus, and version
  message extensions the way Redis 7 does.
