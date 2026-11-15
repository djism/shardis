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

## Iteration 5: Persistence

**What we built**
An append-only file (AOF) that records every change, so data survives restarts and crashes, with
Redis's three fsync policies (`always`, `everysec`, `no`). On startup the AOF is replayed. In the
background it's compacted with a snapshot that doesn't stop writes. A crash-test tool kills the
server mid-load 70 times and counts acknowledged writes that went missing, for each policy and for
two kinds of crash.

**How it works**
```
SET k v ─► store records the effect "SET k v" ─► AOF buffer ─► writer goroutine: write() ─┬─► disk
   │                                                                (always: fsync now) │
   └── reply waits here until write() (or fsync, for always) covers it ◄────────────────┘
   everysec: fsync once a second, separately     no: never fsync, the OS decides
```
- **Effects, not commands.** `INCR x` is logged as `SET x 6`, `EXPIRE k 10` as `PEXPIREAT k
  <exact time>`, and every expiry or eviction as `DEL k`. Replaying always gives the same result.
- **Group commit.** One fsync covers every write that's waiting at that moment. 50 waiting clients
  means 1 fsync, not 50.
- **Files:** a base snapshot + incremental files + a manifest listing them, replaced atomically
  (temp file, fsync, rename, fsync the directory).
- **Rewrite without stopping the world:** switch to a new incremental file, then copy the keyspace
  out one shard group at a time. The snapshot isn't from one instant, but every record is absolute
  ("x is now 6"), so replaying a few records that are already in the snapshot gives the same answer.

**Why we did it this way**
- *Effects (ADR 014):* deterministic replay, expiries and evictions logged for free, and the same
  stream feeds replicas in Iteration 6.
- *Reply after write() (ADR 015):* process crashes (by far the most common kind) lose nothing with
  any policy. We measured 0 of ~6.4M acknowledged writes lost in 30 `kill -9` runs.
- *Fuzzy snapshot (ADR 016):* Go can't `fork()` like Redis, so we rely on idempotent records.
  40 out of 40 test rounds with writers hammering keys *during* the rewrite restored the exact state.

**What can go wrong**
- **fsync is about machine crashes, not process crashes.** In a simulated power cut, `everysec`
  lost 0.3–0.9 s of acknowledged writes each time, and `no` lost everything not yet flushed by
  the OS. Only `always` lost nothing, at ~20x lower throughput (5.9k vs 115k writes/s).
- **Tail latency.** With the AOF on, our p99 is 9 ms vs Redis's 1.5 ms (`everysec`). Likely a
  `write()` waiting behind an in-progress fsync, which Redis avoids by postponing writes. Not
  fixed yet.
- **The log is one sequence.** Every write takes the log mutex briefly, which will matter at
  higher core counts. Per-slot logs would fix it.
- **Bugs we hit:** a variadic `...string` helper secretly copied every value (allocation), and
  `Close` panicked when called twice. Both were caught and fixed while building.

**Try it yourself**
```
make build
./bin/shardis --appendonly yes --appendfsync everysec --dir /tmp/shardis
redis-cli -p 7000 set name shardis; redis-cli -p 7000 incrby visits 42
# Ctrl-C, start it again with the same flags:
redis-cli -p 7000 get visits                     # "42"
cat /tmp/shardis/appendonlydir/shardis.aof.1.incr.aof   # the log is readable RESP: SET visits 42
redis-cli -p 7000 bgrewriteaof; redis-cli -p 7000 info persistence
./bin/shardis-crashtest -runs 3                  # kill -9 / power-cut table per policy
bash docs/benchmarks/iter-05-persistence/run.sh  # full measurement (~10 min)
```

**Interview question you can now answer**
*"Your cache has a persistence option. If the server crashes, can it lose writes it already acknowledged?"*
"It depends on what crashes. I measured it. If only the process dies (kill -9, OOM kill, a
panic), we lose nothing under any fsync policy, because a write is acknowledged only after it's
been handed to the kernel: 0 of 6.4 million acknowledged writes lost in 30 crash runs. If the
machine dies, only what was fsynced survives. With `everysec` that's up to about a second of
writes (I measured 0.3 to 0.9 s), with `always` nothing, but `always` is about 20 times slower
even with group commit, because each fsync takes ~8 ms on my disk. In practice I'd use
`everysec` plus a replica: two machines' memory is usually safer and faster than one fsync."

**Check yourself**
1. With `appendfsync no`, a `kill -9` lost zero acknowledged writes. Why doesn't "no fsync" mean "data loss" here?
<details><summary>Answer</summary>Every reply waited until its write was handed to the kernel with write(). After that, the data lives in the kernel's page cache, which belongs to the OS, not our process. Killing the process doesn't touch it, and the OS writes it to disk eventually. fsync only protects against the OS or machine going down before that happens.</details>

2. Why would the fuzzy snapshot break if the AOF logged `INCR x` instead of `SET x 6`?
<details><summary>Answer</summary>The snapshot may already include the result of an INCR that's also in the new incremental file. Replaying "SET x 6" again changes nothing, because it's idempotent. Replaying "INCR x" again would add 1 a second time and give the wrong value. The fuzzy snapshot is only correct because every record states the result, not the change.</details>

## Iteration 6: Primary → replica replication

**What we built**
Replicas: `REPLICAOF host port` turns a node into a live, read-only copy of a primary, and
`REPLICAOF NO ONE` promotes it. Replicas catch up after a disconnect from a backlog instead of
re-copying everything, even after a failover. `WAIT` tells a client how many replicas have its
writes. Plus a fault-injection framework, which we used to reproduce, on purpose, the moment
replication loses an acknowledged write.

**How it works**
```
client ─SET─► primary: apply, AOF, append effect to the backlog ─► reply OK   ◄── acknowledged here
                              │ (ring buffer, every byte numbered by offset)
                              └─ sender goroutine per replica ──► replica: apply, ACK offset
```
- **The stream is the effect log from Iteration 5** (`SET x 6`, `PEXPIREAT k <time>`, `DEL k`),
  so replicas end up byte-for-byte in sync.
- **Reconnect:** "I'm at offset X of history Y" → still in the backlog? Send just the gap. Too old?
  Full sync: stream a fuzzy snapshot, then the stream from where it started.
- **After a promotion,** the new primary remembers the old history ID, so the other replicas
  continue without a full copy (PSYNC2).
- **Replicas never expire keys themselves.** They hide expired keys and wait for the primary's
  `DEL`, so they can't disagree with it.

**Why we did it this way**
- *One shared backlog (ADR 017):* memory stays bounded however many replicas there are. A replica
  that falls too far behind gets a fresh full sync.
- *Diskless fuzzy full sync (ADR 018):* the same idempotency argument as the AOF rewrite.
  No disk, and clients wait on one slot group at a time.
- *Only the primary deletes (ADR 019):* the replica's data is always a copy of the primary's
  history, never a divergent version.
- *Deterministic fault injection (ADR 020):* the rare failure ("primary dies after acking, before
  replicating") becomes an ordinary test you can rerun.

**What can go wrong** (this is the point of the iteration)
- **Asynchronous replication loses acknowledged writes on failover.** Measured: 4 of 2.4M even
  with a sub-millisecond local replica, and up to 68% of writes on a slow link, where lag reached
  200k writes.
- **WAIT shrinks the window but doesn't remove it.** Waiting for 1 replica after each write lost
  nothing in 20 runs, but cost 35% throughput (366x on the slow link). And:
  (B) a WAIT that times out leaves the write applied and visible, with no rollback;
  (C) with two replicas, WAIT 1 can be satisfied by the one that *isn't* promoted, and the
  "confirmed" write is still lost.
- **Bugs fault injection caught while building:** the pause check ran before waiting for data, so
  one write slipped through. And the test's "crash" released pauses before closing links, so the
  dying primary still delivered data, which a real crash never does.

**Try it yourself**
```
make build
./bin/shardis --port 7001 &
./bin/shardis --port 7002 --replicaof "127.0.0.1 7001" &
redis-cli -p 7001 set k v; redis-cli -p 7002 get k     # "v"
redis-cli -p 7002 set x y                              # READONLY You can't write against a read only replica.
redis-cli -p 7001 wait 1 1000                          # 1
redis-cli -p 7001 info replication                     # slave0:...,offset=...,lag=0
./bin/shardis --port 7003 --enable-fault-injection yes &   # then: FAULT SET repl.stream pause
./bin/shardis-crashtest -failover plain,wait -runs 3   # the lost-write table, quick
```

**Interview question you can now answer**
*"Can Redis lose a write it acknowledged? What does WAIT change?"*
"Yes. Replication is asynchronous: the primary says OK before any replica has the write. If it
dies in that gap and a replica takes over, the write is gone. I reproduced it on purpose with
fault injection, and measured it: 4 out of 2.4 million writes even with a local replica, and up
to two-thirds of writes when the replica lagged on a slow link. WAIT makes the client wait until
N replicas have its writes. In my tests that took losses to zero, but it cost 35% throughput, and
it isn't synchronous replication: if WAIT times out, the write is still applied, and with two
replicas WAIT 1 can be satisfied by the replica that doesn't get promoted. So the real fixes are
monitoring lag and having the failover pick the most up-to-date replica."

**Check yourself**
1. Why does a replica that reconnects after a 2-second blip usually *not* need a full copy?
<details><summary>Answer</summary>The primary keeps the last 64 MB of the stream in its backlog, with every byte numbered by offset. The replica says "I have everything up to offset X of history Y". If X is still in the backlog, the primary sends just the bytes after X (a partial resync). A full copy is only needed if the replica missed more than the backlog holds, or is on a different history.</details>

2. In Demo C, WAIT 1 returned 1 and the write was still lost. What would have prevented it?
<details><summary>Answer</summary>Either wait for all replicas (WAIT 2), so every candidate for promotion has the write, or make the failover promote the most up-to-date replica, the one with the highest replication offset. Redis Cluster does the second, roughly, by giving the replica with the highest offset the shortest election delay. That's coming in Iteration 9.</details>

## Iteration 7: Hash slots + redirects (a static cluster)

**What we built**
Shardis runs as a cluster of 3 primaries + 3 replicas that real clients understand. Every node
knows which node owns each of the 16384 hash slots, from a topology file that's the same on every
node. A command for someone else's slot gets `-MOVED <slot> <host:port>`, which `redis-cli -c`
and go-redis's `ClusterClient` follow automatically. Keys in different slots get `-CROSSSLOT`.
It runs as six local processes or six Docker containers.

**How it works**
```
client: SET foo bar ──► node a (slots 0-5460)
                         slot = CRC16("foo") mod 16384 = 12182 → owned by c
          ◄── -MOVED 12182 127.0.0.1:7002
client: SET foo bar ──► node c (slots 10923-16383) ──► OK ──► replicates to its replica f
```
- **Every command declares where its keys are** (`GET`: argument 1; `MSET`: every other argument
  from 1). Dispatch computes the slot *before* running anything, and redirects if it's not ours.
- **Smart clients avoid the redirect.** They ask `CLUSTER SLOTS` (who owns what) and `COMMAND`
  (where each command's keys are), compute the slot themselves, and go straight to the right node.
  `MOVED` is just the correction when their map is out of date.
- **Replicas** redirect to their primary, unless the client sent `READONLY`. Then they serve reads
  for that primary's slots, which is how you scale reads.
- **Hash tags** (`{user:1}:cart`, `{user:1}:profile`) put related keys in one slot, so multi-key
  commands still work.

**Why we did it this way**
- *A static topology file (ADR 021):* deterministic in local processes, Docker and soon Kubernetes,
  and Iteration 8's gossip will start from it.
- *Routing in dispatch from key positions (ADR 022):* one place, no forgotten checks, and the same
  data powers `COMMAND` for smart clients.
- *Client vs internal addresses (ADR 023):* in Docker, containers talk by name (`shardis-c:7000`)
  while your Mac is told `127.0.0.1:7002`.

**What can go wrong**
- **Static means frozen.** No node can join, no failover, no slot moves until gossip (Iteration 8),
  failover (9) and resharding (11). If a primary dies, its slots are down.
- **Multi-key commands across slots fail by design.** Applications need hash tags.
- **A real bug from Iteration 6 surfaced here.** Replica links dropped every 20 s, because a
  handshake deadline also covered *writes* and quietly broke every ACK after 10 s. It's fixed and
  has a regression test (`TestIdleLinkStaysUp`), which fails with the old code.
- **The macOS AirPlay trap.** AirPlay Receiver listens on `*:7000` and *accepts* connections without
  ever replying, so `redis-cli -p 7000` hangs forever if our node isn't up yet. The scripts use
  timeouts and publish ports on `127.0.0.1` only.
- **On one laptop the cluster isn't faster:** one benchmark client is the bottleneck, for Redis too.

**Try it yourself**
```
make cluster-up-local                 # or: make cluster-up  (Docker Compose)
redis-cli -c -p 7000 set foo bar      # OK, after following MOVED 12182 → 127.0.0.1:7002
redis-cli -p 7000 get foo             # MOVED 12182 127.0.0.1:7002   (without -c you see the redirect)
redis-cli -c -p 7000 mset a 1 b 2     # CROSSSLOT Keys in request don't hash to the same slot
redis-cli -c -p 7000 mset {u:1}:a 1 {u:1}:b 2   # OK: same slot thanks to the hash tag
redis-cli -p 7000 cluster nodes       # who owns what
redis-benchmark --cluster -p 7000 -t set,get -q
make cluster-down-local
```

**Interview question you can now answer**
*"Why does Redis Cluster use 16384 hash slots instead of hashing keys straight onto nodes?"*
"Because slots are a fixed layer between keys and nodes. A key's slot never changes (CRC16 mod
16384), and only the slot → node map changes. Adding a node means moving some slots, not
rehashing every key, and clients can cache a small map of 16384 entries. In my implementation
every node knows that map. A request for someone else's slot gets MOVED with the right address,
and smart clients like go-redis cache the map and route directly. Multi-key commands must stay in
one slot, which is why hash tags exist."

**Check yourself**
1. A client sends `MSET {cart:7}:items 3 {cart:7}:total 42` to the wrong node. What does it get back, and why not CROSSSLOT?
<details><summary>Answer</summary>MOVED to the node that owns the slot of "cart:7". With hash tags only the part inside {} is hashed, so both keys are in the same slot: not cross-slot, just the wrong node. The client retries on the owner, which accepts it.</details>

2. Why does a replica answer MOVED for a GET on its own primary's key, even though it has the data?
<details><summary>Answer</summary>Replicas are asynchronous copies and may be slightly behind, so by default Redis Cluster sends every request, reads included, to the primary for consistency. A client that accepts possibly-stale reads sends READONLY first, and then the replica serves reads for its primary's slots. Writes always go to the primary.</details>

## Iteration 8: Cluster bus + gossip

**What we built**
Nodes now find each other and keep a shared picture of the cluster *by themselves*. Each node opens
a second port (client port + 10000) for node-to-node messages (PING, PONG, MEET). Every message
carries news about a few other nodes, so introducing nodes in a chain is enough for everyone to
meet everyone. Clusters can be built at runtime with `CLUSTER MEET / ADDSLOTS / REPLICATE`. The
real `redis-cli --cluster create` can build a Shardis cluster. Each node saves its own view
(`nodes.conf`), so it survives a restart.

**How it works**
```
          MEET            MEET            MEET
   a ───────────► b ───────────► c ───────────► d ...
   every PING/PONG says: "I'm b, epoch 3, I own slots 5461-10922,
                          and I know about c (127.0.0.1:7002) and d (...)"
   a hears about c from b → a starts a handshake with c → soon everyone knows everyone
```
- **Ten times a second each node:** connects to nodes it has no link to. Once a second it pings the
  least recently heard-from of 5 random nodes, and it pings anyone silent for half the node timeout.
- **Each message carries:** the sender's full slot ownership (a 2 KB bitmap of 16384 bits) and
  gossip about ~10% of the cluster.
- **Epochs settle conflicts:** if two nodes claim the same slot, the one with the higher config
  epoch wins. If two primaries have the same epoch, the one with the smaller ID bumps its own.
  Every node applies the same rule, so they all agree without a vote.
- **Routing reads an immutable snapshot of the map,** and gossip swaps in a new one after each
  change, so commands never take a lock.

**Why we did it this way**
- *Binary format with the slot bitmap in every message (ADR 024):* ownership heals itself on
  every heartbeat. It's fuzz-tested, because the bytes come from the network.
- *Redis's gossip schedule (ADR 025):* compared with all-to-all and with SWIM in the ADR.
- *Copy-on-write topology + per-node `nodes.conf` (ADR 026):* lock-free routing, and a restarted
  node keeps its identity.

**What can go wrong**
- **Traffic grows with cluster size:** 2, 5.3 and 13.4 messages per node per second at 6, 20 and
  50 nodes. The whole cluster's traffic grows with N², because every node pings every other at
  least every node_timeout/2.
- **Convergence slows with size:** ~1.5 s for 6 nodes, ~11–16 s for 50 (introduced in a chain).
- **Two bugs the tests caught:**
  - A node whose PONG was lost was *never pinged again*, because we only ping when no ping is
    pending. Fixed by reconnecting links whose ping has waited longer than half the timeout.
  - Ping/pong times changed without publishing a new snapshot, so `CLUSTER NODES` showed
    "never heard from" forever.
- **`ADDSLOTS` only checks the local view.** Two nodes can briefly both claim a slot. Epochs settle
  it, but someone's claim loses.
- **No authentication on the bus.** Anyone who can reach port +10000 can MEET in. Keep it private.
- **Nothing reacts to a dead node yet:** that's Iteration 9 (PFAIL → FAIL → failover).

**Try it yourself**
```
make build
for p in 7200 7201 7202 7203 7204 7205; do
  ./bin/shardis --port $p --cluster-enabled yes --dir /tmp/sh$p & done
redis-cli -p 7200 cluster info | grep known      # 1: it knows only itself
for p in 7200 7201 7202 7203 7204; do redis-cli -p $p cluster meet 127.0.0.1 $((p+1)); done
redis-cli -p 7205 cluster info | grep known      # 6, about a second later
# or let redis-cli build it all:
redis-cli --cluster create 127.0.0.1:7200 ... 127.0.0.1:7205 --cluster-replicas 1
redis-cli -p 7200 cluster nodes                  # live ping/pong times, link state, epochs
```

**Interview question you can now answer**
*"How do nodes in Redis Cluster learn about each other without a central coordinator?"*
"Gossip. Each node keeps a connection to the others it knows, and a few times a second it pings
someone: the least recently heard-from of a random sample, plus anyone silent for half the
timeout. Every ping and pong includes the sender's slot ownership and news about a tenth of the
cluster. When you hear about a node you don't know, you handshake with it. So I can introduce six
nodes in a chain and they all know each other in about 1.5 seconds. Conflicts are settled with
epochs: the higher config epoch wins a slot. The cost is that per-node traffic grows with cluster
size: I measured 13 messages a second per node at 50 nodes."

**Check yourself**
1. Node a was introduced only to b. How does it end up connected to f?
<details><summary>Answer</summary>b's PINGs and PONGs to a include gossip entries about random other nodes, each with its ID and address. When a sees an entry for a node it doesn't know, it creates it in "handshake" state and connects to its bus port. f's PONG then confirms it. Every node does this, so news spreads hop by hop until everyone knows everyone.</details>

2. Two nodes both claim slot 42. How does every node agree on the owner, without a vote?
<details><summary>Answer</summary>Every claim travels with the claimer's config epoch, and every node applies the same rule: a claim replaces the current owner only if its epoch is higher. So every node independently picks the claim with the higher epoch. If the epochs are equal, the collision rule (smaller node ID bumps its epoch) makes them different first.</details>

## Iteration 9: Failure detection + automatic failover

**What we built**
The cluster now heals itself. When a primary dies, the other nodes notice, a majority of primaries
agree it's down, its most up-to-date replica wins an election, takes over its slots, and everyone
reconfigures, all without a human. When the old primary comes back, it rejoins as a replica.
Killing a real primary with `kill -9` under load, writes to its slots work again ~3 s later with a
2 s node timeout, every time, across 60 runs.

**How it works**
```
 t=0      primary a dies
 t≈2s     every node: "no PONG from a for > node_timeout" → PFAIL (suspected)
          gossip carries the suspicion; once a majority of primaries agree → FAIL, broadcast
 +0.5-1s  replica d waits a ranked delay (most up-to-date replica waits least),
          bumps the epoch (a cluster-wide logical clock), asks every primary for its vote
          each primary votes at most once per epoch (saved to disk before replying)
 +~10ms   d has a majority → takes the new epoch, promotes itself, claims a's slots
          everyone accepts the claim: higher epoch wins (Iteration 8's rule)
 later    a comes back, sees its slots taken with a higher epoch → becomes d's replica
```
- **Why a majority to declare FAIL?** So one node with a bad network link can't trigger a failover
  of a healthy primary. We tested that: cut one link, and there's no failover.
- **Why one vote per epoch?** Two replicas can't both collect a majority from the same voters in the
  same epoch, so there's at most one winner per election.
- **Why rank by replication offset?** The replica with the most data loses the fewest writes
  (Iteration 6's Demo C, solved).

**Why we did it this way**
- *Timeout + majority detection (ADR 027),* compared with SWIM and phi-accrual.
- *Replica election with primary voting (ADR 028),* compared with Raft. Same ideas (terms ≈
  epochs, one vote per term, majority, freshest wins), but Raft only acknowledges what a majority
  stored and never loses acknowledged writes. Redis Cluster replicates asynchronously, so it fails
  over faster and can lose the in-flight writes.
- *Stale replicas don't run (ADR 029):* better a shard down than an old copy served as current.

**What can go wrong**
- **Failover loses acknowledged writes:** in 16 of 60 runs, up to 146 per failover, all writes in
  flight to the replica when the primary died.
- **The node timeout is a tradeoff.** Smaller means faster failover (2.3 s at 1 s) but more false
  alarms from GC pauses or network blips, and each false alarm can lose writes.
- **The whole cluster refuses keyed commands during the gap** (full coverage, Redis's default).
  `--cluster-require-full-coverage no` confines it to the failed shard.
- **Clients can lag the cluster.** go-redis by default refreshes its slot map only on `MOVED` or
  every 60 s, so it can keep hitting a dead primary for up to a minute after a 3 s failover.
- **Bugs found while building:**
  - A node that died right after answering a PING was *never* suspected, because reconnecting
    failed and no PING was ever left unanswered. Fixed by counting a failed connect as a sent
    PING, as Redis does.
  - Message counters sized for 5 types crashed the bus on the first vote request.
  - Checking the "Try it yourself" steps by hand, the failover *didn't happen*. Replica d had
    dialed 127.0.0.1:7000 before node a had bound it, reached macOS's AirPlay Receiver (which
    owns *:7000 and never replies), waited 10 s, and a was killed before d ever synced. d then
    correctly refused to promote itself with no data (ADR 029 working as designed). Fixed the
    setup: a 2 s handshake timeout, and the script starts primaries before replicas. After the
    fix, the 15 s-timeout failover took 16.4 s and the old primary rejoined as a replica.

**Try it yourself**
```
make cluster-up-local                          # 3 primaries + 3 replicas (node timeout 15s default)
redis-cli -p 7000 cluster nodes | head -3
kill -9 $(cat .cluster-local/a.pid)            # primary a dies
redis-cli -p 7001 cluster nodes                # a: "master,fail?" then "master,fail"
redis-cli -p 7003 role                         # ~16-18 s later: replica d is now "master"
redis-cli -c -p 7001 set foo bar               # keyed writes work again
make cluster-down-local; make cluster-up-local # a comes back...
redis-cli -p 7000 role                         # ...as a replica ("slave") of d
./bin/shardis-crashtest -cluster-failover 2000 -runs 5   # time it
```

**Interview question you can now answer**
*"Walk me through what happens when a Redis Cluster primary dies."*
"Every node pings its peers. When one hasn't answered for longer than the node timeout, the node
marks it PFAIL, meaning suspected, and gossip carries that suspicion around. When a majority of
primaries suspect the same node, it becomes FAIL and that's broadcast. A majority, so one bad link
can't trigger a failover. Its replicas then wait a delay ranked by replication offset, so the
most up-to-date one goes first, bump the epoch, and ask the primaries for votes. Each primary
votes once per epoch, so there's at most one winner. The winner takes a higher config epoch and
claims the slots, and everyone accepts the higher epoch. I measured about node timeout plus 1 to
2.5 seconds, so 3 seconds with a 2 second timeout. It can lose writes that were in flight to the
replica: I saw up to 146 in one failover. That's the price of asynchronous replication, unlike
Raft."

**Check yourself**
1. Two replicas of the same dead primary start elections at the same moment, in the same epoch. Can both win?
<details><summary>Answer</summary>No. To win, each needs votes from a majority of primaries, and each primary votes at most once per epoch (recorded on disk before it replies). Two majorities of the same set always overlap, so at least one primary would have to vote twice, which it won't. At worst the votes split, nobody wins, and they retry later in a higher epoch. The random part of the delay makes that rare.</details>

2. Why does the cluster refuse to promote a replica whose link to the primary has been down for an hour?
<details><summary>Answer</summary>Its data is an hour old. Promoting it would silently discard an hour of acknowledged writes and serve old data as if it were current. Keeping the shard down until the primary or a fresh replica returns is the lesser evil. That's Redis's cluster-replica-validity-factor (data older than 10 × node_timeout).</details>

## Iteration 10: Network partitions and split brain

**What we built**
A node that can't reach a majority of the primaries now stops serving (`CLUSTERDOWN`). When it
regains the majority, it waits a short rejoin delay before serving again. We added
`FAULT PARTITION`/`FAULT HEAL` to cut the network between groups of nodes on demand,
`--cluster-allow-reads-when-down`, and a crash-tool mode that measures the split-brain window
on 6 real processes.

**How it works**
```
      minority side           │           majority side
  clients ──► a (primary)     │     b   c   (primaries)
              "OK" "OK" ...   │     d (a's replica)   e   f
                              │
 t=0      the cut ────────────┼──────────────────────────────────
 t≈2.1s   a: "I can only      │
          reach 1 of 3        │
          primaries" →        │
          CLUSTERDOWN         │
 t≈3.2s                       │   b,c agree a is FAIL → d elected,
                              │   d takes a's slots (epoch +1)
 heal     a hears d's higher epoch → becomes d's replica,
          drops its own data → every write a acked after t=0 is gone
```
Each node already tracks who it can reach (PFAIL from Iteration 9). The new rule is one line of
logic: if the primaries I can reach (counting me) aren't a majority of the slot-serving
primaries, I'm in the minority and I refuse keyed commands. `FAULT PARTITION a` is sent to every
node. Each one works out which side it's on and adds "drop bus messages to them" and "sever
replication to them" rules for every node on the other side. Clients are unaffected.

**Why we did it this way**
- **Minority stops serving, plus a rejoin delay (ADR 030).** The alternative is a **lease**: the
  primary may serve only while a majority has granted it a time-limited permission, and nobody
  replaces it until that runs out. Leases give a hard "never two primaries" guarantee but need a
  lease protocol and bounded clock drift. Redis uses the simpler timing rule, and so do we.
- **Partitions built from our existing fault points (ADR 031)** rather than iptables or
  Toxiproxy. They're deterministic, need no root, and the same command works in `go test`,
  locally and in Docker. Real network-level chaos comes in Iteration 14.

**What can go wrong**
- **Every write the cut-off primary acknowledged during the split is lost.** At a 2 s node
  timeout, `a` kept saying OK for **2.07 s** (p50). After healing, **2,125,387 of 2,125,779** of
  those writes (99.98%) were gone. The few survivors were already on their way to `d`. Loss ≈
  write rate × node timeout. With Redis's default 15 s, that's ~15 s of acknowledged writes.
- **"No two primaries at once" is a race, not a guarantee.** In all 20 runs `a` stopped before
  `d` started, by 0.2–1.4 s. But one 5 s run had only 199 ms to spare. A long GC pause on `a`
  could make the windows overlap.
- **There's a gap when nobody is writable** for those slots (0.8–1.4 s at 2 s, beyond the
  detection time itself). With full coverage on, the whole majority side refuses keyed commands
  while `a` is FAIL.
- **A partition is worse than a crash.** A crash loses ≤ 146 in-flight writes (Iteration 9). A
  partition loses ~215,000 in the same setup, because the cut-off node is alive and keeps
  acknowledging writes it can't protect.
- **About CAP:** in a partition you must pick consistency (refuse) or availability (answer).
  Redis Cluster is **neither strictly CP nor AP**.
  - It's not AP: the minority refuses after a timeout, and in Scenario B (a primary cut off
    together with its replica) the whole cluster went down.
  - It's not CP: for the first node timeout, the minority answers "OK" to writes it will later
    throw away.

  It's a pragmatic middle: available for a while, then consistent-ish.
- Bug found: the reads-when-down test first passed for the wrong reason, because its probe
  write used the same key it later read. Fixed with a separate probe key in the same slot.

**Try it yourself**
```
# fault injection on, 5 s node timeout (SHARDIS_* env vars reach every node);
# rm -rf .cluster-local first so no earlier failover is remembered in nodes.conf
rm -rf .cluster-local
SHARDIS_ENABLE_FAULT_INJECTION=yes SHARDIS_CLUSTER_NODE_TIMEOUT=5000 make cluster-up-local
for p in 7000 7001 7002 7003 7004 7005; do redis-cli -p $p fault partition a; done
redis-cli -p 7000 set hello world          # slot 866 is a's. "OK": a doesn't know yet (keep trying)
redis-cli -p 7000 set hello world          # after ~5-7 s: (error) CLUSTERDOWN The cluster is down
redis-cli -p 7003 role                     # ~7-9 s after the cut: d is "master"
for p in 7000 7001 7002 7003 7004 7005; do redis-cli -p $p fault heal; done
redis-cli -p 7000 role                     # a is now a "slave" of d
redis-cli -c -p 7001 get hello             # (nil): a's split-brain write is gone
make cluster-down-local
./bin/shardis-crashtest -partition 2000 -runs 3     # measure the window
bash docs/benchmarks/iter-10-partition/run.sh       # the full table (~5 min)
```

**Interview question you can now answer**
*"Can Redis Cluster lose acknowledged writes during a network partition? How much?"*
"Yes, and I measured it. If a primary gets cut off with some of its clients, it keeps
acknowledging writes until it notices it can't reach a majority of the primaries, which takes
about the node timeout. Meanwhile the majority side elects its replica. When the partition
heals, the old primary sees the higher epoch, becomes a replica and throws away everything it
accepted. With a 2 second timeout that was about 2 seconds of writes, around 215,000 in my
setup, or 99.98% of what it acknowledged during the split. A crash loses far less, only what was
in flight, because a dead node can't keep saying OK. The fix depends on the use case: shorter
timeouts, `WAIT` for important writes, or leases or consensus if you truly can't lose
acknowledged writes. For a cache, that tradeoff is usually fine."

**Check yourself**
1. Why does a node wait before serving again after the partition heals, instead of resuming
   right away?
<details><summary>Answer</summary>Right after healing, it can reach the majority again but may not have heard yet that its replica was promoted in a higher epoch. If it resumed instantly, it could accept writes for slots it no longer owns, and they'd be lost a moment later. The rejoin delay (node timeout, clamped to 0.5–5 s) gives gossip time to deliver the news first.</details>

2. A 5-node-primary cluster is split 2 | 3. Which side keeps serving, and what happens to the
   slots of the two primaries on the small side?
<details><summary>Answer</summary>The 3-primary side has a majority and keeps serving. The 2 primaries on the small side can each reach only 2 of 5, so after a node timeout they refuse with CLUSTERDOWN. On the big side, their slots fail over only if their replicas are on the big side. If a replica is stuck on the small side too, those slots stay down, and with full coverage on, the whole big side refuses keyed commands until the heal.</details>

## Iteration 11: Live resharding, adding and removing nodes

**What we built**
Slots can now move between primaries while clients keep reading and writing, using Redis's
classic protocol: MIGRATING/IMPORTING markers, `MIGRATE` in batches, `-ASK` redirects and a final
`SETSLOT NODE`. `shardis-admin` adds and removes nodes and rebalances, and the real
`redis-cli --cluster reshard/rebalance` works too. A minimal `pkg/client` follows MOVED and ASK,
and the loadgen gained a cluster mode with a 100 ms timeline.

**How it works**
Moving slot 100 from `a` to the new node `g`, with clients running:
```
 1. g: SETSLOT 100 IMPORTING a    g serves slot-100 keys, but only after ASKING
 2. a: SETSLOT 100 MIGRATING g    a: key still here? serve it. Gone? "-ASK 100 g"
 3. a: MIGRATE g ... KEYS k1..k100   (repeat until the slot is empty on a)
        client ──GET k1──► a ──"-ASK 100 g"──► client ──ASKING; GET k1──► g ✓
 4. g: SETSLOT 100 NODE g         g owns it, bumps its epoch (no vote needed)
 5. a: SETSLOT 100 NODE g         a now says "-MOVED 100 g" (update your map)
 6. others: SETSLOT 100 NODE g    (gossip would tell them anyway, a bit later)
```
- **ASK vs MOVED:** ASK means "just this once, try over there". MOVED means "it lives there
  now, update your map".
- **Why the order matters:**
  - If `a` sent ASK before `g` was importing, `g` would answer "MOVED back to `a`", and the
    client would loop.
  - If `a` flipped to `g` before `g` did, each node would point at the other.

The hard part was making it **atomic** in a server that runs commands in parallel. Every keyed
command holds a per-slot "gate" (a reader/writer lock) shared from routing to execution.
MIGRATE holds it exclusively while a batch is in flight. So a key is always either still on `a`
or already on `g`, never neither.

**Why we did it this way**
- **Classic MIGRATE + ASK (ADR 032)** rather than streaming whole slot ranges. It's in the
  Redis Cluster spec and every client understands it. Redis 8.10's own `redis-cli` now uses
  "atomic slot migration" (stream a whole range, pause writes briefly, flip it in one step),
  which we measured as 6× faster. That's what I'd build at scale.
- **The per-slot gate (ADR 033)** rather than holding store locks across the network.
  - Switching it off makes `TestMigrateUnderConcurrentWrites` lose acknowledged INCRs in
    10 of 10 runs.
  - On normal traffic its cost was too small to measure.
- **Our own DUMP format with version + CRC-64 (ADR 034),** because both ends are Shardis.
- **The importing node bumps its epoch without a vote (ADR 035),** like Redis: one tool
  coordinates the move, so there's nobody to outvote.

**What can go wrong**
- **It's slow and it costs throughput.** 18.9 s for 4096 slots and 250k keys. Throughput was
  20% lower than the control during the move, and p99 went from 1.3 to 2.2 ms. Redis 8.10 did
  the same move in 3.1 s.
- **Multi-key commands can get `TRYAGAIN`** while their keys are split across two nodes.
- **Replicas learn new owners only by gossip,** so for 1–3 s `redis-cli --cluster check` says
  "nodes don't agree". Redis pushes UPDATE messages; we don't have them yet.
- **A crash mid-move leaves "open" slots.** The markers survive in nodes.conf, and
  `shardis-admin fix` finishes them.
- **A failover mid-move loses the markers** on the promoted replica (Redis has the same gap).
  That's for the Iteration 13 chaos tests.
- **Found along the way:**
  - Our `add-node` first returned before every node knew the newcomer, so the rebalance hit
    "I don't know about node". It now waits for every node.
  - Real Redis's new node answers CLUSTERDOWN for ~1.4 s if you rebalance right after
    `redis-cli --cluster add-node`, because the node is still settling. Wait for its
    `cluster_state:ok` first.
- This Mac was swapping heavily (30 GB of swap in use), which freezes everything for up to 2.5 s.
  That's why every benchmark has a no-rebalance control run next to it.

**Try it yourself**
```
rm -rf .cluster-local && make cluster-up-local
bin/shardis --port 7006 --dir /tmp/g --cluster-enabled yes &         # an empty node
bin/shardis-admin add-node -addr 127.0.0.1:7001 -new 127.0.0.1:7006
bin/shardis-loadgen -cluster -addr 127.0.0.1:7001 -keys 100000 -preload -warmup 0 -duration 30s &
bin/shardis-admin rebalance -addr 127.0.0.1:7001                       # ~4096 slots to the new node
bin/shardis-admin nodes -addr 127.0.0.1:7001                           # 4096 slots each
# the loadgen's summary: "followed N MOVED and M ASK redirects", and no errors line
redis-cli --cluster check 127.0.0.1:7001   # "don't agree" for a few s (replicas catch up by gossip), then [OK]
make cluster-down-local; kill %1
bash docs/benchmarks/iter-11-resharding/run.sh                         # the full comparison (~6 min)
```

**Interview question you can now answer**
*"How do you move data between shards without downtime? What's the difference between MOVED
and ASK?"*
"In Redis Cluster the target marks the slot importing, the source marks it migrating, and keys
move in batches with MIGRATE. During the move the source serves keys it still has, and for
anything already moved it replies ASK, which means 'try the target, just this once'. The client
sends ASKING there so the target accepts it. When the slot's empty, the target takes ownership
with a higher config epoch, and from then on everyone replies MOVED, meaning 'update your map'.
The order matters: target importing before source migrating, and target owning before source
letting go, or clients bounce between them. In my version I also had to make 'is the key still
here?' atomic, because my server runs commands in parallel. I added a per-slot reader/writer
lock, and I proved it matters: without it, a test lost acknowledged writes every run. I moved a
quarter of a 1M-key cluster under load with zero client errors. It cost about 20% throughput for
19 seconds, while Redis 8's newer range-streaming migration took 3. That's the direction I'd go
at scale."

**Check yourself**
1. A client sends `MGET {u1}:a {u1}:b` to the source while slot `{u1}` is migrating, `a` has moved and `b` hasn't. What does it get, and why not just ASK?
<details><summary>Answer</summary>TRYAGAIN. The two keys are on different nodes right now, so neither node can answer the whole command atomically. ASK would send it to the target, which doesn't have b yet. TRYAGAIN tells the client to wait a moment and retry; once b has moved too, the source answers ASK and the target serves both.</details>

2. Why does the target node, not the source, take ownership first (`SETSLOT NODE`)?
<details><summary>Answer</summary>While the source is still MIGRATING, it answers ASK for moved keys, and the target (now the owner) serves them. Everything works. If the source flipped first, it would say MOVED-to-target while the target, still only IMPORTING, says MOVED-back-to-source for clients without ASKING: a redirect loop. Target first means there's always a node willing to serve every key.</details>

## Iteration 12: Our own smart client

**What we built**
`pkg/client` is now a real cluster client:
- It learns every command's key positions from the server (`COMMAND`).
- It follows MOVED and ASK.
- It retries "not now" answers (dead node, CLUSTERDOWN, TRYAGAIN, READONLY) with jittered
  backoff inside a time budget.
- It pools connections per node, and can read from replicas.

Its key feature is that it never applies a write twice behind your back. If a connection dies
after an `INCR` was sent, it returns `ErrUnknownOutcome` instead of guessing.

**How it works**
```
Do(ctx, "INCR", "ctr:7")
  keys from COMMAND → slot 3412 → primary in our map
  ┌── send ──────────────────────────────────────────────────────────────┐
  │ reply OK / value         → done                                      │
  │ MOVED / ASK              → go where it says (MOVED: fix the map)     │
  │ TRYAGAIN / CLUSTERDOWN / READONLY / can't connect                    │
  │                          → it certainly didn't run: back off, retry  │
  │ connection died after sending → it MAY have run:                     │
  │     safe to repeat? (GET, SET, DEL…)  → retry                        │
  │     not safe (INCR…)                  → ErrUnknownOutcome            │
  └── until MaxRetryTime (20 s): a failover becomes a slow call ────────┘
```
"Safe to repeat" (idempotent) means running it twice gives the same state *and* the same reply.
`SET k v` is safe. `SET k v NX` isn't: the second run answers "not set", and the app would think
it failed. `INCR` obviously isn't.

**Why we did it this way**
- **RetrySafe by default (ADR 036),** not at-least-once like go-redis's default (which, per its
  source, retries every command after a lost connection). Measured: during a 1.5 s pause, RetryAll
  applied **32 of 32 in-flight INCRs twice, in 5 of 5 runs**, with zero errors shown to the app.
  RetrySafe reported them and doubled nothing.
- **A time budget with exponential backoff and full jitter (ADR 037),** not a retry count.
  Failovers take seconds, and jitter stops 50 clients from retrying in lockstep.
- **Key positions from `COMMAND` (ADR 038),** so MSET is routed and checked correctly.
- **Replica reads off by default (ADR 039),** because they're stale: 2.6–6.7% of immediate
  read-after-writes returned old data, even on loopback.

**What can go wrong**
- **The app must handle `ErrUnknownOutcome`** for non-idempotent writes: read back, use request
  IDs, or accept the risk.
- **During a failover, requests to that shard are slow (up to ~3.5 s here), not failed.**
  Latency-sensitive callers should pass a shorter context deadline.
- **The client can't undo asynchronous replication.** RetryAll runs still lost up to 9
  acknowledged INCRs when a primary was killed.
- **No global retry budget or circuit breaker:** a dying cluster gets retries from every client.
- **Found along the way:**
  - The race detector caught a data race from Iteration 11: `SETSLOT NODE` read a node's epoch
    for an event after releasing the cluster lock, while gossip could write it. Fixed.
  - Under heavy CPU load, a partition test saw the split-brain overlap that ADR 030 warns about:
    `a` acknowledged a write 0.7 ms after `d` took over. The test now bounds the overlap instead
    of claiming it's always zero.
  - Test packages running in parallel picked the same "free" ports. They now use separate
    ranges.

**Try it yourself**
```
go test ./pkg/client -run 'Failover|Reshard|ReplicaRead' -v     # failover/reshard with 0 errors; staleness %
make build
bin/shardis-crashtest -client-chaos safe,all -runs 1
#  policy,run,pause_unknown,pause_doubled,...  safe: 32 unknown, 0 doubled; all: 0 unknown, 32 doubled
bash docs/benchmarks/iter-12-client/run.sh                       # 5 runs per policy (~8 min)
```

**Interview question you can now answer**
*"Your client times out on a write. Should it retry?"*
"It depends on whether the write is idempotent, because a timeout doesn't tell you whether the
command ran. If the connection was never made, or the server said MOVED or CLUSTERDOWN, it
definitely didn't run, so retrying is always safe. But if I sent it and the connection died, it
might have run. Retrying a SET is fine, since running it twice gives the same result. Retrying
an INCR can count twice. I measured it: with a 1.5 second pause on one node and a 500 ms client
timeout, retry-everything applied all 32 in-flight INCRs twice, every run, and the app saw no
errors at all. So my client retries only idempotent commands after an unknown outcome and
returns a specific error for the rest. The real fix is idempotency keys on the server side,
like Stripe or Kafka's idempotent producer, so every write becomes safe to retry."

**Check yourself**
1. Why is `SET lock me NX` not safe to resend even though running it twice leaves the same state?
<details><summary>Answer</summary>Because its reply changes. If the first SET NX ran and took the lock, the resent one finds the key already set and answers nil ("not set"). The application then believes it didn't get the lock, though it did, and may never release it. "Idempotent" has to cover the reply the caller acts on, not just the stored state.</details>

2. A primary is killed while INCRs are in flight. Why did RetryAll cause *duplicates* during the pause but not during the crash?
<details><summary>Answer</summary>A paused node wakes up and still runs the commands waiting in its socket buffers, so the original and the resent INCR both run. A killed node's in-flight INCRs usually never reached its replica before it died, so the resent INCR on the new primary is the only copy that survives. Duplicates come from nodes that are slow, not dead. (A crash can still lose acknowledged writes, which no retry policy fixes.)</details>

## Iteration 13: Chaos suite + consistency checker

**What we built**
`make chaos` runs seven failure scenarios against seven real Shardis processes while a recorded
workload runs through our client:
- kill a primary, kill a replica
- partition a primary away, flap its network
- freeze a node
- crash a node in the middle of a rebalance
- restart everything

A checker then goes through every recorded operation and reports errors, lost and rolled-back
writes, stale reads, phantom values, the longest stall and p99 latency. Each run is saved as a
trace (node events + chaos actions + client timeline) for the website. A new `EVENTS` command
lets any tool stream a node's internal events live.

**How it works**
```
 writers: SET chaos:k 1, 2, 3, ...  (each key only goes UP)      readers: GET random keys
        │ every op recorded: start, end, ok/failed                         │
        ▼                                                                  ▼
 ┌──── 7 processes ───────────────────────────────┐   EVENTS streams (gossip, PFAIL, FAIL,
 │ a b c (primaries)  d e f (replicas)  g (empty) │──▶ votes, failover_won, slot moves…)
 └────────────────────────────────────────────────┘   + chaos actions (kill, pause, partition)
        fault script: kill -9 / SIGSTOP / FAULT PARTITION / rebalance+kill / restart all
 then the checker:
   rolled back = acked write that a later read didn't see (the value went DOWN)
   stale read  = read lower than a value acked before the read started
   phantom     = a value nobody wrote
```
Because each key's value only ever goes up, "did we lose something?" has an exact, cheap
answer. No linearizability solver needed.

**Why we did it this way**
- **Increasing registers instead of a full linearizability check (ADR 040).** Redis Cluster
  isn't linearizable, so that check would just say "no". We measure the specific things that
  should hold, and count the ones that don't.
- **"Rolled back", not just "lost at the end".** The first real partition run showed 251 stale
  reads and **0** lost writes. The losses were real, but later writes to the same keys hid them.
  Counting acked writes that a later read didn't see caught **3,480** of them.
- **An `EVENTS` command (ADR 041)** rather than log files or a new port. Excluded event types
  aren't even produced (the store stops building per-write events when nobody wants them).
- **Real processes with kill -9, SIGSTOP and FAULT PARTITION (ADR 042),** not the in-process
  test cluster: crashes and AOF replays are real.

**What can go wrong**
- **Partitions lose acknowledged writes:** 3,480 rolled back and 200 stale reads in one run,
  while the final state looked perfect.
- **The numbers are best cases:** one machine, loopback network, paced load. Crashes lost
  nothing here. Iteration 9 lost up to 146 writes under heavier load, and Iteration 5's
  power-cut test showed what `kill -9` hides: a process crash leaves the OS page cache intact.
- **Our simulated partitions are harsher than real ones:** a dropped PING is gone for good,
  where TCP would retransmit it. 1.5 s "flaps" (under the 2 s timeout) still caused a failover.
- **Single runs vary:** `flapping-network` had 103 stale reads in one run and 0 in the next.
- **Fixed along the way:** `shardis-admin fix` couldn't finish a slot whose source died and
  failed over mid-rebalance. It now takes the slot's current owner as the source.

**Try it yourself**
```
make chaos                                    # ~5 min: the table below, plus traces
bin/shardis-chaos -list
bin/shardis-chaos -scenarios partition-minority-primary -csv ""
python3 -c "import json;t=json.load(open('website/public/traces/kill-primary.json'));print([(e['t'],e['node'],e['type']) for e in t['events'] if e['type'] in ('chaos','fail','failover_won')])"
make cluster-up-local && (printf 'EVENTS\r\n'; sleep 5) | nc 127.0.0.1 7001   # live gossip (redis-cli exits after OK)
```

**Interview question you can now answer**
*"How do you test that a distributed database doesn't lose data?"*
"You record a history of every operation (when it started, when it ended, what it returned),
inject real failures while it runs, and check the history afterwards. I made the check exact
by having each writer write increasing numbers to keys only it owns. Any read that's lower than
a write that had already been acknowledged means the system went backwards. On my Redis Cluster
clone, seven scenarios produced zero client errors, but the partition scenario rolled back
3,480 acknowledged writes. A naive check of the final state reported zero, because later writes
covered the gap. Checking only the end state can certify a split brain as harmless. That's the
Jepsen mindset: don't trust the final state, check what clients actually observed."

**Check yourself**
1. Why does the checker need writers to write *increasing* values?
<details><summary>Answer</summary>So a value can only legitimately go up. Then "the value went down" (a read or the final state lower than an acknowledged write) is an exact, local proof of a lost or rolled-back write, checkable in linear time, with no need to search for a valid ordering of all operations the way a linearizability checker does.</details>

2. `restart-all` kills every node with `kill -9` and loses nothing, with fsync only every second. Why isn't that proof the data is safe?
<details><summary>Answer</summary>`kill -9` ends the process but not the OS: data already written to the file is in the kernel's page cache and reaches the disk later anyway. Only a power cut or kernel crash loses that last second. Iteration 5's simulated power loss is the test that shows it.</details>
