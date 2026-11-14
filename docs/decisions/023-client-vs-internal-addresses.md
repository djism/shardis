# 023: Separate the address clients are told from the address nodes use
- Context: Clients learn node addresses from the cluster (`MOVED`, `CLUSTER SLOTS`, `CLUSTER NODES`)
  and connect to them directly. In Docker, the address that works *between containers*
  (`shardis-c:7000`) doesn't work from the host, and the address that works from the host
  (`127.0.0.1:7002`, a published port) doesn't work between containers.
- Options considered:
  1. Two addresses per node in the topology: a client address (announced to clients) and an
     internal address (used node to node, for replication now and the cluster bus later).
  2. One address everywhere, with host networking (no Docker network translation). On macOS,
     Docker Desktop's host networking is limited, and it doesn't carry over to Kubernetes.
  3. One address, with clients running inside the Docker network only. Then `redis-cli` and the
     benchmarks on the Mac couldn't reach the nodes.
- Decision: Option 1. Locally both addresses are the same (`deploy/local`). In Compose, clients
  get `127.0.0.1:7000-7005` and nodes use `shardis-a..f:7000` (`deploy/compose`).
- Why:
  - **Both kinds of client work:** `redis-cli -c` on the Mac follows `MOVED 12182 127.0.0.1:7002`,
    while the replica in container `shardis-f` syncs from `shardis-c:7000` (checked by hand on
    the Compose cluster).
  - The same idea will carry Kubernetes: internal = pod DNS name, client = whatever the clients
    can reach.
- Tradeoffs we accept:
  - **A client *inside* the Docker network** gets `127.0.0.1` addresses it can't use. It needs a
    topology that announces container names. One topology file serves one audience.
  - **Two addresses to keep consistent** per node.
  - On macOS, ports are published on `127.0.0.1` only: AirPlay Receiver owns `*:7000`, and also
    accepts connections on it without ever replying. A `redis-cli` that reaches it hangs forever,
    which is why the scripts use timeouts.
- What real Redis does: `cluster-announce-ip`, `cluster-announce-port` and
  `cluster-announce-bus-port` (and, since Redis 7, `cluster-announce-hostname` /
  `cluster-preferred-endpoint-type`) exist for exactly this NAT/container case. Same idea.
- At bigger scale I'd: Announce DNS names rather than IPs (they survive pod restarts), and support
  more than one client-facing endpoint per node (internal vs external clients), as Redis 7's
  hostname endpoints do.
