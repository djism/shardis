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

## Iteration 1: Speak the Redis protocol

**What we built**
A RESP2 parser and writer (`internal/resp`) and a TCP server (`internal/server`) that answers
`PING`, `ECHO`, `QUIT` and a stub `COMMAND`. The real `redis-cli` and `redis-benchmark` now
talk to Shardis, and its PING throughput is within ~7% of real Redis.

**How it works**
RESP is a "length-prefixed" protocol. For `ECHO hello`, the client sends:
```
*2\r\n          ← "an array of 2 things follows"
$4\r\nECHO\r\n  ← "a 4-byte string": ECHO
$5\r\nhello\r\n ← "a 5-byte string": hello
```
Since every string announces its length, the parser never scans the payload looking for an end
marker. It reads exactly N bytes, so values can contain anything, even `\r\n`. That's "framing":
how you cut a continuous byte stream into separate messages.

Each connection gets its own goroutine:
```
 socket ──► flushBeforeRead ──► bufio.Reader ──► ReadCommand ──► dispatch ──► Writer (buffer)
              │                                                                │
              └──── "about to wait for more bytes? send buffered replies first" ◄┘
```
Pipelining works because replies pile up in the write buffer while more commands are already
waiting in the read buffer. We only flush when the read side runs dry, so 16 pipelined commands
cost one write syscall instead of 16.

**Why we did it this way**
- *Flush timing (ADR 003):* flush right before blocking on a read, not after every reply (100
  syscalls for 100 pipelined commands) and not on a timer (adds delay to every single request).
  Measured: 133k → 1.8M req/s with pipelining.
- *Parser safety (ADR 004):* hard limits plus "allocate as bytes arrive", so a client claiming a
  500 MB value but sending 3 bytes costs us nothing. Bad input means an error reply and a
  disconnect, because after garbage we can't tell where the next command starts.
- *Goroutine per connection* instead of a Redis-style single event loop: much simpler code, and
  it costs us ~7% throughput and a bit of tail latency at 50 clients.
- *Default host is now 127.0.0.1* (amended ADR 001), like Redis: no auth means no listening on
  every interface unless you ask.

**What can go wrong**
- Rare latency spikes: a handful of requests took 6–55 ms (Redis max ≤ 3 ms). Probably GC or
  scheduler hiccups. Not explained yet; we'll trace it once there's data in memory.
- Memory limits are per connection, not global. 10,000 clients each sending big values
  could still hurt us. That's for the maxmemory work later.
- Inline commands don't support quotes (`SET k "a b"` via telnet splits into 4 args).
  redis-cli and libraries always use multibulk, so this only affects hand-typing.
- On macOS, AirPlay Receiver holds `*:7000`. We bind `127.0.0.1:7000`, which works, but if you
  run with `--host 0.0.0.0` you'll get "address already in use". (Fix: System Settings →
  General → AirDrop & Handoff → AirPlay Receiver off.)

**Try it yourself**
```
make run                                   # INFO shardis listening addr=127.0.0.1:7000
redis-cli -p 7000 ping                     # PONG
redis-cli -p 7000 echo "hello shardis"     # hello shardis
redis-cli -p 7000 set a b                  # ERR unknown command 'set', ... (Iteration 2!)
printf 'PING\r\nECHO hi\r\n' | nc 127.0.0.1 7000     # +PONG  $2 hi  (inline commands)
redis-benchmark -p 7000 -t ping -q         # throughput
redis-benchmark -p 7000 -t ping -q -P 16   # ~13x more with pipelining
make fuzz                                  # 60s of random bytes per parser entry point
```

**Interview question you can now answer**
*"Why does pipelining make Redis so much faster, and what does the server need to do to support it?"*
"Without pipelining, every request pays a full network round trip plus a read and a write
syscall, so one client tops out around 37k requests a second even though the server is idle.
With pipelining the client sends a batch without waiting, and the server has to do two things:
answer strictly in order, and not flush after every reply. My server buffers replies and flushes
right before it would block waiting for more input, so a batch of 16 costs one write. I measured
133k to 1.8M requests a second with 50 clients. The catch is memory: buffered replies need a cap,
which the fixed-size write buffer gives me."

**Check yourself**
1. A client sends one complete `PING` plus the first half of a second `PING`, then waits. Why
   would a "flush when `Buffered() == 0`" rule hang here, and why doesn't ours?
<details><summary>Answer</summary>The half-command bytes are sitting in the read buffer, so Buffered() is non-zero and the loop skips the flush. Then the parser blocks waiting for the rest of the second PING, and the first PONG never leaves the buffer. If the client waits for that PONG before sending more, both sides wait forever. Our flush lives inside the socket Read call, which only happens right before we'd block, so the first PONG goes out first. TestPartialCommands covers this.</details>

2. Why does the parser disconnect on a protocol error instead of skipping ahead to the next `*`?
<details><summary>Answer</summary>Once framing is broken, we don't know which bytes are headers and which are payload. A value can contain "*" or "\r\n", so "skipping to the next *" could land in the middle of someone's data and run it as a command. Disconnecting is the only safe reset, and it's what Redis does.</details>
