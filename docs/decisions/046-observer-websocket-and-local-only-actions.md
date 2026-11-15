# 046: An observer process relays events to browsers; admin actions are local-only
- Context: The showcase website (Phase 6) has a live mode: watch a real cluster react as you
  break it. Browsers can't speak RESP, and shouldn't connect to every node. Some actions (kill
  a node, partition) are destructive.
- Options considered:
  1. **A separate observer process (`shardis-observer`):**
     - It discovers nodes via `CLUSTER NODES`, subscribes to every node's `EVENTS` stream
       (ADR 041), and polls a topology snapshot every second.
     - It fans everything out over one WebSocket.
     - Actions go to a small HTTP API that's off by default.
  2. **A WebSocket endpoint in every node:** no extra process, but the browser would juggle N
     connections, and every node would carry HTTP + WebSocket code and a browser-facing attack
     surface.
  3. **Server-Sent Events instead of WebSocket:** simpler, one-way (all we need for the stream),
     but the actions need a second channel anyway, and WebSocket is what the roadmap's website
     uses.
- Decision: Option 1, with `github.com/coder/websocket` (pre-approved: "a WebSocket lib").
- Why:
  - **One stream with everything in order:** `{"kind":"topology"|"event"|"action", ...}`,
    node names from the topology file. `TestObserverStreamsTopologyAndEvents` kills a primary
    and sees PFAIL/FAIL, then `failover_won`, arrive over the WebSocket.
  - **Slow browsers can't hurt anything:** each client has a 1024-message buffer, and overflow
    is dropped and counted, the same rule as the event bus (ADR 007).
  - **Actions are fenced three ways:**
    1. They need `-local-actions` (403 otherwise).
    2. They're refused if any cluster node isn't on a loopback address, so the website can never
       kill pods in a shared Kubernetes cluster.
    3. The observer listens on 127.0.0.1 by default, and WebSocket origins are limited to
       localhost.

    `kill`/`pause` find the process by pid file, and path-like node names are rejected.
  - **Verified live:** `POST /api/actions {"action":"kill","node":"a"}` against the local
    cluster under load. The topology showed `a` FAIL and `d` primary 19 s later (15 s default
    node timeout), and Grafana showed the same.
- Tradeoffs we accept:
  - **One more process to run,** and the observer is a single point of failure for the *view*
    (not for the cluster).
  - **Events from a node are lost while its stream reconnects** (~100 ms after a restart).
  - **Topology is polled every second,** not pushed, so roles in the table lag by up to a
    second. The events themselves are immediate.
  - **No authentication:** it's a local demo tool. Exposing it beyond localhost would need auth
    and TLS.
- What real Redis does: Nothing comparable. Redis tooling (RedisInsight, exporters) polls
  `INFO`/`CLUSTER NODES`, and nothing streams internal cluster events.
- At bigger scale I'd: Put the observer behind auth, make actions go through an audited
  control plane (who killed what, when), and replace polling with change notifications
  from the nodes.
