# Shardis

A Redis-compatible distributed cache written from scratch in Go, modeled on Redis Cluster:
RESP protocol, 16384 hash slots, replicas, gossip, automatic failover and live resharding,
with honest measurements of where it can lose writes.

> Work in progress. See [ROADMAP.md](ROADMAP.md) and [docs/learning-log.md](docs/learning-log.md).

## Build and test

```
make build   # bin/shardis
make test    # go test -race ./...
make run     # single node on :7000
```

Requires Go 1.26+.
