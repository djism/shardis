# Chaos traces

Written by `make chaos` (`cmd/shardis-chaos`, Iteration 13), one file per scenario. The website
replays them. Format:

```
{
  "scenario": "kill-primary",
  "description": "...",
  "recorded": "<UTC time>",
  "duration_s": 17.3,
  "nodes":    [{"name": "a", "role": "primary", "slots": "0-5460"}, {"name": "d", "role": "replica", "primary": "a"}, ...],
  "events":   [{"t": 6.25, "node": "d", "type": "failover_won", "data": {...}}, ...],   // t = seconds since load started
  "timeline": [{"t": 0.0, "ok": 812, "fail": 0}, ...],                                  // client ops per 100 ms
  "report":   {"ops": ..., "errors": ..., "rolled_back_writes": ..., "stale_reads": ..., ...}
}
```

`type: "chaos"` events are what the tool did (`data.action`: kill, start, pause, resume,
partition, heal, fix, rebalance_start, ...). Everything else comes from the nodes themselves
(the `EVENTS` stream, ADR 041).
