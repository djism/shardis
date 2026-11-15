# 050: The story is told by labelled simulations; the proof is recorded replays
- Context: The first site (Iteration 16) replayed only real traces. They're accurate, but a
  recruiter landing on the page saw six circles and a log and couldn't tell in five seconds
  what the system is or why it's hard. The page has to explain the ideas (one request's
  journey, millions of requests, TTL and eviction, redirects, a node dying) *and* prove them.
  But nobody records "one shopper adds sneakers to a cart".
- Options considered:
  1. **Replays only:** maximally honest, but hard to read for anyone who doesn't already know
     Redis Cluster. That's the problem we're solving.
  2. **Scripted animations for the story, with no proof:** pretty, and exactly the "is this
     real or fake?" doubt the first draft raised.
  3. **Both, clearly labelled:** five canvas scenes that simulate the mechanism using the real
     algorithm (CRC16 + hash tags in the browser, ADR 048) and *measured* timings (2 s node
     timeout, ~3 s failover, 13 ms handoff), then a "Real runs" section of recorded replays,
     a real terminal session from the Kubernetes demos, and benchmark charts with sources.
- Decision: Option 3.
- Why:
  - **The story makes the proof readable.** After scene 5 ("kill a machine", where you click
    and watch PFAIL → FAIL → vote → promotion), the recorded kill-primary run means
    something: it's the same sequence, from real processes.
  - **Simulations stay honest:** the footer and each scene's note say which parts are
    simulations and which are recordings. Traffic dots say they're scaled. Numbers in the
    scenes come from `docs/benchmarks/`, and the key calculator uses the server's own CRC16
    vectors.
  - **Interactive beats passive:** "type a key, see its slot and owner", "write 40 keys, watch
    TTL and LRU", "kill A / cut B off / heal". A senior engineer pokes at a system; the page
    lets them.
  - **Cheap:** plain canvas 2D, no WebGL. Scenes animate only while on screen
    (IntersectionObserver) and respect `prefers-reduced-motion`.
- Tradeoffs we accept:
  - **Two sources of truth for timing:** if the server's failover got slower, the simulation
    wouldn't notice. Replays would, so they stay on the page as the check.
  - **A simulation is a simplification:** scene 5 shows gossip on a few links, not every
    node's view. The recorded replay is the full version.
  - **The scenes are hand-written canvas code** (`src/scenes/scenes.js`), not React, so
    they're harder to unit-test. We test the parts that carry facts (slot maths, the replay
    model, the decision parser).
- What real Redis does: Not applicable. redis.io explains Redis Cluster with static diagrams
  and prose.
- At bigger scale I'd: Drive the simulations from the same trace format (generate a "story
  trace" from a scripted chaos run), so every animation on the page is replayed from real
  events, and the only thing simulated is the pace.
