# Learning log

One recap per iteration, newest at the bottom. Format is defined in CLAUDE.md.

## Iteration 0: Setup

**What we built**
The empty skeleton of the project: a Go module, the folder layout, a Makefile, and a GitHub
Actions pipeline that checks formatting, builds, and runs every test with the race detector
on each push. The server binary exists but only reads its config, logs, and waits to be stopped.

**How it works**
```
 flags ─┐
 env ───┼─► config.Load ─► run(ctx) ─► waits on ctx.Done()
 defaults┘                    ▲
             Ctrl-C / SIGTERM ┘  (signal.NotifyContext cancels ctx)
```
- `cmd/shardis/main.go` is tiny. All the real logic is in `run(ctx, args, getenv)`, which
  takes its inputs as parameters instead of reading `os.Args`/`os.Getenv` directly, so tests
  can call it with fake inputs and cancel it themselves.
- Packages live under `internal/`. Go refuses to let code outside this repo import anything
  in `internal/`, so we can change those APIs freely. `pkg/client` is the one public package.
- `make test` runs `go test -race`. The race detector watches memory accesses while tests run
  and reports when two goroutines touch the same variable without synchronization.

**Why we did it this way**
Standard `flag` plus a tiny env-var helper instead of a big config library (ADR 001), and
`go.mod` as the only place the Go version lives (ADR 002). SIGTERM handling is there from day
one because Kubernetes sends SIGTERM before killing a pod. We'll need a graceful shutdown for
rolling restarts in Iteration 14.

**What can go wrong**
- The race detector only catches races that actually happen during a test run. A race in
  code the tests never execute won't show up. Tests have to exercise the concurrent paths.
- Stub packages have no tests yet, so CI is green partly because there's nothing to break.
- CI runs on Linux and the laptop on macOS. Real differences (fsync behavior, networking)
  will matter in Iteration 5 and later.

**Try it yourself**
```
make help                       # list targets
make test                       # all packages: ok / [no test files]
make run                        # INFO shardis starting host=0.0.0.0 port=7000, Ctrl-C to stop
SHARDIS_PORT=7005 ./bin/shardis # env var works: port=7005
./bin/shardis --port 60000      # shardis: port 60000 out of range 1-55535 (exit 1)
```

**Interview question you can now answer**
*"How does your service shut down cleanly in Kubernetes?"*
"Kubernetes sends SIGTERM, waits for the grace period, then sends SIGKILL. My main function
turns SIGTERM into a cancelled context, and everything long-running watches that context, so
the server stops accepting work and finishes what it's doing. There's a test that cancels the
context and checks `run` returns within two seconds. Later I'll add flushing the AOF and
handing off to the replica before exit."

**Check yourself**
1. Why does `run` take a `getenv func(string) string` instead of calling `os.Getenv`?
<details><summary>Answer</summary>So tests can pass a fake environment backed by a map. The tests don't depend on (or change) the real process environment, so they can run in parallel and give the same result on any machine.</details>

2. If `SHARDIS_PORT=7001` is set and you run `./bin/shardis --port 7002`, which port is used, and why is the port capped at 55535?
<details><summary>Answer</summary>7002. Precedence is flag > env > default. The cap is because the cluster bus will listen on port + 10000, and that has to stay at or below 65535, the highest TCP port.</details>
