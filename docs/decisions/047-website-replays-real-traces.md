# 047: The website is a static replay of recorded runs, with an optional live mode
- Context: The showcase site has to show what the cluster *actually* does (gossip, suspicion,
  FAIL, elections, slot moves), to people who won't run Go, a cluster or Kubernetes. It must
  be cheap to host, honest about what's real, and steppable: an interviewer wants to pause on
  "d wins the election".
- Options considered:
  1. **A static site that replays recorded traces** (`make chaos` writes them; ADR 042),
     plus a **Live mode** that connects to `shardis-observer` on the visitor's own machine
     (ADR 046).
  2. **A hosted live demo cluster** that the public site connects to: always "real", but it
     costs money, can be broken by visitors, and needs auth for the destructive buttons.
  3. **Hand-made animations** (keyframed, scripted): easiest to make pretty, and exactly what
     the roadmap rules out ("not fake animations").
- Decision: Option 1. React + Vite + TypeScript, Tailwind, Framer Motion. (D3 was planned for the
  charts; plain HTML bars turned out to be enough, so it was dropped.)
- Why:
  - **The same model drives both modes.** A trace and the observer's stream are reduced to one
    `ClusterState` (roles, slot owners, PFAIL/FAIL, minority, down). `Replayer` rebuilds the
    state at any time t: forward event by event, or from the start when scrubbing back.
    Unit tests replay the real traces: after `failover_won` in `kill-primary`, d owns a's
    5,461 slots; at the end of `reshard-during-failover`, g owns a quarter.
  - **Steppable:** ⏮ ⏭ jump between key events (the chaos actions, FAIL, election, failover,
    demotion), and the scrubber marks them. **Deep links** (`?scenario=kill-primary&t=6.3`)
    open a run at a moment. `prefers-reduced-motion` turns off autoplay and the flying dots;
    every state is still reachable by stepping.
  - **Honest by construction:** the "Real runs" section is replays only, and the footer
    says which scenes are recordings and which are simulations tuned to measured numbers
    (ADR 050). The one inferred element in a replay, how traffic splits across nodes, is
    footnoted (ADR 049).
  - **Free to host:** the whole site is static files (348 KB of JS, 112 KB gzipped, plus
    traces of 70 KB–3 MB, loaded on demand).
- Tradeoffs we accept:
  - **Replays are only as good as the events recorded:** a node's first milliseconds after a
    restart may be missing (ADR 041).
  - **The model reconstructs each node's view approximately:** one badge per node, not every
    node's opinion of every other. We fixed the worst case: a cut-off node suspects everyone,
    and showing that as fact was wrong. A node is marked SUSPECTED only if a healthy node
    suspects it.
  - **Live mode needs the observer running locally;** on the public site it just says so.
- What real Redis does: Not applicable (Redis has no such site). The closest are Jepsen's
  reports: static, generated from recorded histories.
- At bigger scale I'd: Generate traces in CI from every release (`make chaos` → traces →
  site), so the site can never drift from the code, and diff behaviour between releases
  ("failover got 300 ms slower").
