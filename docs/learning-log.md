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

## Iteration 2: The keyspace + the concurrency decision

**What we built**
Shardis now stores data. GET, SET (NX/XX), DEL, EXISTS, INCR/DECR/INCRBY/DECRBY, MGET/MSET, KEYS,
SCAN, DBSIZE and FLUSHALL all work, with replies checked byte-for-byte against real Redis 8. We
built two engines that keep commands atomic in different ways, benchmarked them against each other
and against Redis, and picked **sharded locks**. We also added the event bus, which costs 3.4 ns
when nobody listens.

**How it works**
Every command follows the same pattern: parse the arguments, then call `engine.Do(keys, fn)`, then
write the reply. Inside `fn`, nobody else can touch those keys.
```
                    ┌─────────── keyspace: 16384 slot maps ───────────┐
 conn goroutine ─┐  │ slot 0 {..}  slot 1 {..}  ...  slot 16383 {..}  │
 conn goroutine ─┼─►│   guarded by 256 locks: lock = slot % 256        │
 conn goroutine ─┘  └──────────────────────────────────────────────────┘
   each command: lock its keys' shards (ascending order) → run → unlock → reply
```
- A key's slot is `CRC16(key) mod 16384`, exactly like Redis Cluster. `{user:1}:cart` and
  `{user:1}:name` share a slot because only the part in `{}` is hashed.
- **Why ascending order?** If command A locks shard 3 then waits for shard 7, while B locks 7 then
  waits for 3, they wait forever. That's a deadlock. If everyone locks low-to-high, it can't happen.
- **Safety net:** a command must declare every key it'll touch. If it touches an undeclared key,
  the `Tx` panics, so the bug shows up in tests and not as a silent race in production.
- **Replies are written after the lock is released.** Writing to a slow client while holding a
  lock would freeze everyone else on that shard.

**Why we did it this way**
We expected the Redis-style single executor to win. It didn't, and the reason is interesting.
Redis's one thread does the network I/O *and* runs the commands, so it never hands work between
threads. In Go each connection already has its own goroutine, so "one goroutine owns the data"
means an extra hand-off per command: wake the executor, then wake the caller back up. Measured:
- **Single client:** +8 µs per request.
- **Pipelined:** capped at about 0.9M req/s, while sharded reached 1.5M (faster than Redis 8's
  1.0–1.4M, because we use several cores and Redis runs commands on one).
- **Engine alone, 10 cores:** 11M vs 1.8M ops/s.

Atomicity is identical either way, and the same tests pass on both: 100k concurrent INCRs lose
nothing, exactly one SET NX winner in 20 rounds of 50 racers, and MSET is never seen half-done.
(ADRs 005, 006, 007.)

**What can go wrong**
- **KEYS, SCAN, DBSIZE and FLUSHALL take all 256 locks** and stall everyone while they run. KEYS
  on a million keys will freeze the server, same as in Redis.
- **Memory: about 3x Redis per key** (250 vs 85 bytes RSS for a 16-byte key and value). Go stores a
  string header, a slice header and map overhead per key, and the GC lets the heap grow to about
  2x live data before collecting.
- **Latency spikes aren't explained yet.** GC is *ruled out*: the longest stop-the-world pause with
  1M keys was 0.26 ms. But our worst requests (49–69 ms) are still worse than Redis's (22 ms) on the
  same laptop.
- **Without pipelining we're about 10% behind Redis.** One goroutine wake-up per request costs more
  than Redis's event loop, which handles many ready sockets per wake-up.

**Try it yourself**
```
make run
redis-cli -p 7000 set greeting hello          # OK
redis-cli -p 7000 set greeting x NX           # (nil): already exists
redis-cli -p 7000 incrby visits 42            # 42
redis-cli -p 7000 mset {user:1}:name dj {user:1}:city nyc
redis-cli -p 7000 mget {user:1}:name {user:1}:city nope    # dj, nyc, (nil)
redis-cli -p 7000 --scan --pattern '{user:1}*'
redis-benchmark -p 7000 -t set,get -n 100000 -r 100000 -q
./bin/shardis --port 7001 --engine executor   # try the other engine and compare
bash docs/benchmarks/iter-02-concurrency/run.sh   # full comparison vs real Redis (~8 min)
```

**Interview question you can now answer**
*"Redis is single-threaded. Why didn't you just copy that?"*
"I built both and measured. Redis's single thread is fast because the same thread reads the socket
and runs the command, with no hand-off and no locks. In Go, connections already live on their own
goroutines, so a single owner adds two goroutine wake-ups per command. That cost me 8 µs per request
and capped pipelined throughput at 0.9M a second. With 256 sharded locks, taken in a fixed order so
deadlock is impossible, I got 1.5M, faster than Redis on the same laptop. And atomicity was the same:
the same concurrency tests pass on both. It also fits the cluster model: Redis Cluster only allows
multi-key ops inside one hash slot, which means exactly one lock for me."

**Check yourself**
1. Two clients run `MSET a 1 b 1` and `MSET b 2 a 2` at the same time, and `a` and `b` are in
   different shards. Why can't this deadlock in our design?
<details><summary>Answer</summary>Do() doesn't lock in the order the keys were given. It builds the set of shards first and always locks them from lowest to highest number. Both commands try to take the lower shard first, so one gets it and the other waits before holding anything. Nobody ever holds one lock while waiting for another that the first is holding.</details>

2. We measured GC stop-the-world pauses under 0.3 ms with 1M keys. Does that mean GC has no effect on latency?
<details><summary>Answer</summary>No. Stop-the-world pauses are short, but the GC's marking phase runs *concurrently* and uses about 25% of CPU while it runs, and it can make a busy goroutine help with marking ("mark assist"), which slows that request. GC also explains why we use about 2.3x more memory than live data. Short pauses rule out GC as the cause of 50 ms spikes, not as a cost.</details>

## Iteration 3: Expiry

**What we built**
Keys can now have a time-to-live: `SET ... EX/PX/EXAT/PXAT/KEEPTTL`, `EXPIRE`/`PEXPIRE`/`EXPIREAT`/
`PEXPIREAT` (with `NX/XX/GT/LT`), `TTL`/`PTTL`/`EXPIRETIME`/`PEXPIRETIME` and `PERSIST`. 63 cases
replayed against real Redis give identical replies. Expired keys are removed two ways: when someone
reads them (lazy), and by a background loop that samples keys with TTLs (active). About a million
keys nobody reads are gone about a second after they expire, and the memory comes back.

**How it works**
Each key stores an absolute deadline ("dies at 12:00:05.250"). Keys with a TTL are also listed in a
small per-slot index so the background loop can find them.
```
 GET k ──► lookup ──► past deadline? ──yes──► delete now ("lazy"), reply (nil)
                                     └─no──► reply value

 every 100 ms (hz=10), for each group of slots, holding its lock ≤ 1 ms:
     sample 20 keys from the TTL index → delete the expired ones
     > 25% were expired?  yes → sample again (probably lots more)
                          no  → good enough, next slot
     slot's map now < 1/4 of its peak size? → rebuild it smaller
 whole cycle stops after 25 ms (25% of one core, at most)
```
The 25% rule makes it self-tuning: when nothing is expiring it glances at each slot once, and in a
storm it works flat out (within its budget).

**Why we did it this way**
- *Sampling, not a heap or timers (ADR 008).* A min-heap would delete exactly on time but costs
  ~40 B per key and O(log n) per update. A timer per key means a million runtime timers. Sampling
  costs nothing extra and keeps up as well as Redis (0.8–1.2 s vs 1.0–1.2 s after the deadline).
- *Absolute deadlines (ADR 009).* "Dies in 5 s" would restart its countdown every time the write
  is replayed or copied to a replica. "Dies at 12:00:05" means the same thing everywhere.
- *A 1 ms cap per lock.* Redis's single thread can spend a full 25 ms on expiry while clients
  wait. We hold any one shard lock for at most ~1 ms, so during the storm our GET p99 peaked at
  ~1–1.7 ms vs Redis's ~1.7–3.4 ms.
- *A fake clock in tests.* "Wait 2 seconds" becomes `clk.Advance(2*time.Second)`: instant and the
  same every run, instead of sleeping and hoping.

**What can go wrong**
- **Go maps never shrink, and we got bitten.** The first version freed a slot's map only when it
  was completely empty. In the storm, survivors shared slots with expired keys, so all 16,384 maps
  stayed at peak size: **102 MB of heap for ~12 MB of data**. The unit test had missed it because
  there *every* key expired. Fix (ADR 010): the background pass rebuilds maps that fall below 1/4
  of their peak. After the fix: 16 MB of heap, and RSS fell from ~330 to ~58 MB. Shrinking on
  every delete also worked but slowed the storm down, because maps got rebuilt over and over.
- **Expired keys linger.** They're invisible right away, but their memory can stay for a while.
  DBSIZE counts them until they're cleaned up, as in Redis.
- **Clock jumps.** Deadlines follow the wall clock. If the clock jumps forward (NTP, a laptop
  waking up), many keys expire at once.
- **Every key pays 16 bytes for TTL support** (124 vs 108 bytes/key), even keys with no TTL.

**Try it yourself**
```
make run
redis-cli -p 7000 set session abc EX 10      # OK
redis-cli -p 7000 ttl session                # 10, then 9, 8, ...
redis-cli -p 7000 set visits 0 EX 60
redis-cli -p 7000 incr visits                # 1, and TTL is still ~60 (INCR keeps it)
redis-cli -p 7000 set visits 5               # a plain SET removes the TTL → ttl = -1
redis-cli -p 7000 expire visits 100 GT       # 0: "no TTL" counts as infinitely long
redis-cli -p 7000 set k v PX 100; sleep 0.2; redis-cli -p 7000 get k   # (nil)
bash docs/benchmarks/iter-03-expiry/storm.sh  # the 1M-key expiry storm vs Redis (~2 min)
```

**Interview question you can now answer**
*"How would you expire millions of keys without scanning all of them?"*
"Lazy plus sampled active expiry, like Redis. Reads check the deadline, so an expired key is never
returned. For keys nobody reads, a background loop runs ten times a second and samples 20 keys
that have a TTL. If more than a quarter are expired, it samples again, otherwise it moves on. So
it's nearly free when nothing's expiring and goes flat out in a storm, with a CPU budget and a
1 ms cap per lock so clients don't stall. I measured a million unread keys gone about a second
after their deadline. The thing that surprised me was memory: Go maps never shrink, so I had to
add a background compaction step. Without it, 95k surviving keys were holding 102 MB."

**Check yourself**
1. Why does the background loop sample *again* when more than 25% of a sample was expired, instead of just moving on?
<details><summary>Answer</summary>A sample is a small random look at the slot. If more than a quarter of 20 random TTL keys are expired, a big share of the whole slot is probably expired too, so it's worth working there more. If few are, the remaining expired keys are rare and it's cheaper to let lazy expiry or a later cycle get them. That's how it adapts to load without counting anything.</details>

2. We store "expires at 12:00:05" instead of "expires in 5 seconds". What breaks with the relative version once we add replication?
<details><summary>Answer</summary>The replica gets the write a bit later (replication lag), or replays it from a log after a restart. With "5 seconds from now", each copy starts its own countdown when it *receives* the write, so the replica would keep the key alive longer than the primary, and a restart would extend every TTL. An absolute deadline means the same moment everywhere.</details>

## Iteration 4: Memory limits + eviction

**What we built**
`--maxmemory` with four Redis policies (`noeviction`, `allkeys-lru`, `allkeys-random`,
`volatile-ttl`), adjustable at runtime with `CONFIG SET`, plus `INFO` (memory, stats, keyspace).
Memory is tracked by an estimate that lands within 1–3% of the real Go heap. We built a load
generator that simulates an app with skewed traffic, and showed that Redis-style *approximate* LRU
is within 0.4 points of *exact* LRU while using less memory and being 5x faster to read.

**How it works**
```
SET k v ──► over maxmemory?  no  ──────────────────────────────► run SET
                │ yes
                ├─ noeviction ─────► -OOM command not allowed...   (GET and DEL still work)
                └─ evict until under the limit, one shard lock at a time:
                     allkeys-lru:    sample 5 random keys → pool of the 16 oldest → evict oldest
                     allkeys-random: sample 1 key → evict it
                     volatile-ttl:   sample keys with a TTL → evict the soonest-expiring
                   ...then run SET
```
- **How we know the memory used:** every write and delete adds or subtracts what that key costs.
  That's its key and value rounded to Go's allocation sizes, plus the slot maps, whose size we
  can work out because Go maps double and never shrink.
- **Why evict before locking:** eviction has to touch whichever shard holds the victim. Doing it
  while holding this command's locks could deadlock with another command doing the same thing.
- **Last-used time** has 1-second resolution, so a hot key's timestamp is rewritten at most once a
  second, not on every read.

**Why we did it this way**
- *An estimate, not the Go heap (ADR 011).* Reading the heap is slow and includes uncollected
  garbage, so it sawtooths. Our estimate costs one atomic add per write and matches real heap
  growth within 1–3% from 20k to 1M keys. The first version was off by **2x** for small keyspaces
  because it ignored the 16,384 mostly-empty slot maps.
- *Approximate LRU (ADR 012).* Exact LRU makes every GET move a node in one shared list behind one
  lock: 5x slower at 10 cores, and ~107 extra bytes per key. With the same memory it fits 26% fewer
  keys and loses to approximate LRU (74.6% vs 77.2%).
- *Synchronous eviction before locking (ADR 013).* The limit holds within one write, and deadlock
  is impossible by construction.

**What can go wrong**
- **Memory floor of ~9 MB.** The 16,384 slot maps cost that once keys spread over every slot (up
  to ~14 MB with TTLs). Below ~10 MB of `maxmemory`, eviction has to empty whole slots to free
  their maps, which kills LRU's quality. Our first eviction tests used a 400 KB limit and LRU
  "kept" 0 of 200 hot keys. Startup now warns below 32 MB.
- **~1.7x Redis's memory per key** (322 vs 188 B for a 100-byte value).
- **The estimate is not RSS.** Go heap runs ~20% above it between GCs, and RSS above that. Give a
  container's limit headroom above `maxmemory`.
- **Writes pay for eviction when full**, and writers line up behind one eviction lock under memory
  pressure. One write (or one per concurrent writer) can land over the limit.

**Try it yourself**
```
make build
./bin/shardis --maxmemory 64mb --maxmemory-policy allkeys-lru
./bin/shardis-loadgen -addr 127.0.0.1:7000 -keys 1000000 -warmup 25s -duration 20s
redis-cli -p 7000 info memory                   # used_memory vs maxmemory, go heap next to it
redis-cli -p 7000 info stats                    # keyspace_hits/misses, evicted_keys
redis-cli -p 7000 config set maxmemory-policy noeviction
redis-cli -p 7000 set x y                       # (error) OOM command not allowed when used memory > 'maxmemory'.
redis-cli -p 7000 config set maxmemory-policy allkeys-random   # rerun loadgen, compare hit rate
bash docs/benchmarks/iter-04-eviction/run.sh    # the full comparison vs Redis (~8 min)
```

**Interview question you can now answer**
*"How would you implement LRU eviction for a cache with millions of keys under heavy concurrency?"*
"Not with a linked list. Exact LRU turns every read into a write to one shared structure. I built
it to check, and at 10 cores reads got 5x slower and adding cores made it worse. It also costs
about 107 bytes per key, so with the same memory it fit 26% fewer keys and ended up with a worse
hit rate than the approximation. I do what Redis does: each key stores a last-used timestamp at
one-second resolution. To evict, sample 5 random keys into a pool of the 16 best candidates and
evict the oldest. On a Zipf workload that's 77.2% hit rate vs 77.6% for exact LRU and 73.7% for
random. I also evict before the write takes its locks, so eviction can't deadlock with commands."

**Check yourself**
1. Exact LRU picks the truly oldest key every time, yet with the same 32 MB it got a *lower* hit rate than approximate LRU. Why?
<details><summary>Answer</summary>Its bookkeeping takes memory: a list node and an index entry, ~107 bytes per key. In the same 32 MB it held 77k keys instead of 104k. Hit rate depends mostly on how many keys fit, so picking the perfect victim couldn't make up for 26% less cache. It only caught up when given 33% more memory.</details>

2. Why does the memory estimate track each slot map's *peak* size instead of its current size?
<details><summary>Answer</summary>Go maps grow by doubling and never shrink. A map that once held 60 keys keeps its 128-slot array even after 55 are deleted. So the memory a map really uses follows from the most it has ever held, until compaction rebuilds it smaller. Using the current size would undercount, and eviction would let real memory blow past the limit.</details>
