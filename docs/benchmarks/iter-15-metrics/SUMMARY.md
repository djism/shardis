# Iteration 15: metrics + live event stream

## Grafana shows a failover live

Setup:
- the local cluster (`make cluster-up-local`, metrics on 9121–9126)
- Prometheus + Grafana from `deploy/observability` (2 s scrape)
- `shardis-loadgen -cluster` traffic
- `shardis-observer -local-actions`

At 08:33:29 UTC primary `a` was killed through the observer: `POST /api/actions {"action":"kill","node":"a"}`.

What Prometheus recorded (`query_range`, 2 s steps):

| series | change |
|---|---|
| `shardis_cluster_state_ok{node="b"}` | 1 → **0 at 08:33:46** → 1 at 08:33:48 (a marked FAIL; full coverage) |
| `shardis_is_primary{node="d"}` | 0 → **1 at 08:33:48** |
| `shardis_cluster_failovers_won_total{node="d"}` | 0 → **1 at 08:33:48** |
| `shardis_cluster_nodes_fail{node="c"}` | 0 → 1 at 08:33:48 |

That's 19 s from kill to new primary: the local cluster's default 15 s node timeout, plus FAIL
agreement and the election.

The dashboard's "Role per node" panel query, run through **Grafana's own API**
(`/api/ds/query`), returned six series: `d` flipping `[0, 1]`, `a` `b` `c` constant 1, and `e`
`f` constant 0. That's what the state-timeline panel draws. (No screenshot: the Grafana image
renderer isn't installed. Open `http://127.0.0.1:3000/d/shardis` and kill a primary to watch it.)

The observer's topology 30 s after the kill: `a` primary `['fail']` unreachable, `d` primary.

## Hot-path cost

See [overhead.txt](overhead.txt).
- **First version** (shared per-command counter + histogram on every command): −7% SET,
  −9% GET, in every round.
- **Fixed** (per-connection counters, latency sampled 1 in 16): no measurable cost.
