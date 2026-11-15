# Shardis website

The showcase site: React + Vite + TypeScript, Tailwind, Framer Motion.

- **Replay** (the default, and what the public site uses) plays the traces that `make chaos`
  writes to `public/traces/`. Every animation is a replay of a real run (ADR 047).
- **Live** connects to `bin/shardis-observer` on this machine and animates the local cluster.

```
make web-dev     # http://127.0.0.1:5174   (Live mode: make cluster-up-local && bin/shardis-observer)
make web-test    # typecheck + unit tests (CRC16 vectors shared with the Go server, trace replays)
make web-build   # static site in dist/
```

Deep links: `?scenario=kill-primary&t=6.3` opens a recorded run at a moment; `?mode=live`
opens Live mode.

`src/lib/model.ts` turns a trace (or the observer's stream) into the cluster state at any
moment. `src/lib/keyslot.ts` is CRC16-XMODEM + hash tags, checked against
`src/lib/keyslot.vectors.json`, which the Go tests generate and verify (ADR 048).
