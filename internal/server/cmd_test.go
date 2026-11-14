package server

import (
	"fmt"
	"math/rand/v2"
	"net"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djism/shardis/internal/resp"
	"github.com/djism/shardis/internal/store"
)

// testClient speaks RESP over a real TCP connection, like a minimal client library.
type testClient struct {
	t *testing.T
	c net.Conn
	w *resp.Writer
	r *resp.Reader
}

func newClient(t *testing.T, addr string) *testClient {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(30 * time.Second))
	t.Cleanup(func() { c.Close() })
	return &testClient{t: t, c: c, w: resp.NewWriter(c), r: resp.NewReader(c)}
}

// do sends one command and returns the reply rendered as text (see render).
// It reports errors with t.Error (not Fatal) so it's safe from goroutines.
func (tc *testClient) do(args ...string) string {
	bargs := make([][]byte, len(args))
	for i, a := range args {
		bargs[i] = []byte(a)
	}
	tc.w.WriteCommand(bargs...)
	if err := tc.w.Flush(); err != nil {
		tc.t.Error(err)
		return ""
	}
	v, err := tc.r.ReadValue()
	if err != nil {
		tc.t.Errorf("%v: %v", args, err)
		return ""
	}
	return render(v)
}

// render prints a reply compactly, like redis-cli: OK, "v", (nil), (int) 3, [a b].
func render(v resp.Value) string {
	switch {
	case v.Null:
		return "(nil)"
	case v.Kind == resp.SimpleString, v.Kind == resp.Error:
		return string(v.Str)
	case v.Kind == resp.Integer:
		return fmt.Sprintf("(int) %d", v.Int)
	case v.Kind == resp.BulkString:
		return strconv.Quote(string(v.Str))
	default:
		parts := make([]string, len(v.Elems))
		for i, e := range v.Elems {
			parts[i] = render(e)
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
}

func forEachEngine(t *testing.T, test func(t *testing.T, addr string)) {
	for _, kind := range store.Engines {
		t.Run(kind, func(t *testing.T) {
			addr, _ := startServerWith(t, kind)
			test(t, addr)
		})
	}
}

// commandCases are run in order on one connection. They're also replayed
// against real Redis in TestMatchesRealRedis, so the expected strings below
// are what Redis 8 actually returns.
var commandCases = []commandCase{
	{[]string{"GET", "k"}, "(nil)"},
	{[]string{"SET", "k", "v"}, "OK"},
	{[]string{"GET", "k"}, `"v"`},
	{[]string{"SET", "k", "v2", "NX"}, "(nil)"},
	{[]string{"SET", "k", "v2", "xx"}, "OK"},
	{[]string{"SET", "new", "v", "XX"}, "(nil)"},
	{[]string{"SET", "new", "v", "NX"}, "OK"},
	{[]string{"SET", "k", "v", "NX", "XX"}, errSyntax},
	{[]string{"SET", "k", "v", "BOGUS"}, errSyntax},
	{[]string{"GET"}, "ERR wrong number of arguments for 'get' command"},
	{[]string{"EXISTS", "k", "k", "nope"}, "(int) 2"},
	{[]string{"DEL", "k", "new", "nope"}, "(int) 2"},
	{[]string{"MSET", "a", "1", "b", "2"}, "OK"},
	{[]string{"MSET", "a", "1", "b"}, "ERR wrong number of arguments for 'mset' command"},
	{[]string{"MGET", "a", "nope", "b"}, `["1" (nil) "2"]`},
	{[]string{"INCR", "a"}, "(int) 2"},
	{[]string{"INCRBY", "a", "10"}, "(int) 12"},
	{[]string{"DECR", "a"}, "(int) 11"},
	{[]string{"DECRBY", "a", "20"}, "(int) -9"},
	{[]string{"INCR", "fresh"}, "(int) 1"},
	{[]string{"INCRBY", "a", "x"}, errNotInt},
	{[]string{"INCRBY", "a", "+1"}, errNotInt},
	{[]string{"INCRBY", "a", "01"}, errNotInt},
	{[]string{"SET", "s", "hello"}, "OK"},
	{[]string{"INCR", "s"}, errNotInt},
	{[]string{"SET", "s", "-0"}, "OK"},
	{[]string{"INCR", "s"}, errNotInt},
	{[]string{"SET", "s", " 1"}, "OK"},
	{[]string{"INCR", "s"}, errNotInt},
	{[]string{"SET", "max", "9223372036854775807"}, "OK"},
	{[]string{"INCR", "max"}, errOverflow},
	{[]string{"SET", "min", "-9223372036854775808"}, "OK"},
	{[]string{"DECR", "min"}, errOverflow},
	{[]string{"DECRBY", "x", "-9223372036854775808"}, "ERR decrement would overflow"},
	{[]string{"DBSIZE"}, "(int) 6"},
	{[]string{"KEYS", "m*"}, ""}, // order differs from Redis; checked as a set below
	{[]string{"SCAN", "abc"}, "ERR invalid cursor"},
	{[]string{"SCAN", "0", "COUNT", "0"}, errSyntax},
	{[]string{"SCAN", "0", "COUNT"}, errSyntax},
	{[]string{"FLUSHALL", "nope"}, errSyntax},
	{[]string{"FLUSHALL"}, "OK"},
	{[]string{"DBSIZE"}, "(int) 0"},
}

// expiryCases cover TTLs. Like commandCases they also run against real
// Redis. A want of "~(int) N" means "an integer within 1000 of N", for PTTL,
// which ticks down while the test runs. 4102444800 is 2100-01-01 in Unix seconds.
var expiryCases = []commandCase{
	{[]string{"SET", "t", "v", "EX", "100"}, "OK"},
	{[]string{"TTL", "t"}, "(int) 100"},
	{[]string{"PTTL", "t"}, "~(int) 100000"},
	{[]string{"SET", "t", "v2"}, "OK"}, // plain SET drops the TTL
	{[]string{"TTL", "t"}, "(int) -1"},
	{[]string{"TTL", "nope"}, "(int) -2"},
	{[]string{"PTTL", "nope"}, "(int) -2"},
	{[]string{"SET", "c", "5", "PX", "100000"}, "OK"},
	{[]string{"INCR", "c"}, "(int) 6"},
	{[]string{"TTL", "c"}, "(int) 100"}, // INCR keeps it
	{[]string{"SET", "t", "v", "EX", "50"}, "OK"},
	{[]string{"SET", "t", "v3", "KEEPTTL"}, "OK"},
	{[]string{"TTL", "t"}, "(int) 50"},
	{[]string{"GET", "t"}, `"v3"`},
	{[]string{"SET", "k", "v", "EX", "0"}, "ERR invalid expire time in 'set' command"},
	{[]string{"SET", "k", "v", "PX", "-5"}, "ERR invalid expire time in 'set' command"},
	{[]string{"SET", "k", "v", "EX", "abc"}, errNotInt},
	{[]string{"SET", "k", "v", "EX", "9223372036854775807"}, "ERR invalid expire time in 'set' command"},
	{[]string{"SET", "k", "v", "EX", "10", "PX", "100"}, errSyntax},
	{[]string{"SET", "k", "v", "EX"}, errSyntax},
	{[]string{"SET", "k", "v", "KEEPTTL", "EX", "10"}, errSyntax},
	{[]string{"SET", "q", "v", "PXAT", "1"}, "OK"}, // deadline in 1970: born expired
	{[]string{"GET", "q"}, "(nil)"},
	{[]string{"SET", "n", "v", "NX", "EX", "100"}, "OK"},
	{[]string{"TTL", "n"}, "(int) 100"},
	{[]string{"EXPIREAT", "t", "4102444800"}, "(int) 1"},
	{[]string{"EXPIRETIME", "t"}, "(int) 4102444800"},
	{[]string{"PEXPIRETIME", "t"}, "(int) 4102444800000"},
	{[]string{"EXPIRETIME", "nope"}, "(int) -2"},
	{[]string{"PERSIST", "t"}, "(int) 1"},
	{[]string{"PERSIST", "t"}, "(int) 0"},
	{[]string{"PERSIST", "nope"}, "(int) 0"},
	{[]string{"EXPIRETIME", "t"}, "(int) -1"},
	{[]string{"EXPIRE", "nope", "10"}, "(int) 0"},
	{[]string{"EXPIRE", "t", "100", "XX"}, "(int) 0"}, // no TTL yet
	{[]string{"EXPIRE", "t", "100", "NX"}, "(int) 1"},
	{[]string{"EXPIRE", "t", "200", "NX"}, "(int) 0"},
	{[]string{"EXPIRE", "t", "50", "GT"}, "(int) 0"},
	{[]string{"EXPIRE", "t", "200", "gt"}, "(int) 1"},
	{[]string{"EXPIRE", "t", "300", "LT"}, "(int) 0"},
	{[]string{"EXPIRE", "t", "10", "LT"}, "(int) 1"},
	{[]string{"TTL", "t"}, "(int) 10"},
	{[]string{"EXPIRE", "t", "1000", "XX", "GT"}, "(int) 1"}, // has a TTL and 1000s is later
	{[]string{"TTL", "t"}, "(int) 1000"},
	{[]string{"EXPIRE", "t", "10", "NX", "XX"}, "ERR NX and XX, GT or LT options at the same time are not compatible"},
	{[]string{"EXPIRE", "t", "10", "GT", "LT"}, "ERR GT and LT options at the same time are not compatible"},
	{[]string{"EXPIRE", "t", "10", "BOGUS"}, "ERR Unsupported option BOGUS"},
	{[]string{"EXPIRE", "t", "x"}, errNotInt},
	{[]string{"EXPIRE", "t", "9223372036854775807"}, "ERR invalid expire time in 'expire' command"},
	{[]string{"PEXPIRE", "t", "9223372036854775807"}, "ERR invalid expire time in 'pexpire' command"},
	{[]string{"PERSIST", "t"}, "(int) 1"},
	{[]string{"EXPIRE", "t", "100", "GT"}, "(int) 0"}, // no TTL counts as infinite
	{[]string{"EXPIRE", "t", "100", "LT"}, "(int) 1"},
	{[]string{"EXPIRE", "t", "0"}, "(int) 1"}, // past deadline: deleted now
	{[]string{"EXISTS", "t"}, "(int) 0"},
	{[]string{"SET", "p", "v"}, "OK"},
	{[]string{"PEXPIRE", "p", "-1"}, "(int) 1"},
	{[]string{"GET", "p"}, "(nil)"},
	{[]string{"SET", "a", "v"}, "OK"},
	{[]string{"EXPIREAT", "a", "0"}, "(int) 1"},
	{[]string{"EXISTS", "a"}, "(int) 0"},
	{[]string{"TTL"}, "ERR wrong number of arguments for 'ttl' command"},
	{[]string{"FLUSHALL"}, "OK"},
}

type commandCase = struct {
	args []string
	want string
}

// configCases also run against real Redis: same names, same defaults, same
// size parsing ("100mb" = 104857600).
var configCases = []commandCase{
	{[]string{"CONFIG", "GET", "maxmemory"}, `["maxmemory" "0"]`},
	{[]string{"CONFIG", "GET", "maxmemory-policy"}, `["maxmemory-policy" "noeviction"]`},
	{[]string{"CONFIG", "GET", "maxmemory-samples"}, `["maxmemory-samples" "5"]`},
	{[]string{"CONFIG", "GET", "hz"}, `["hz" "10"]`},
	{[]string{"CONFIG", "SET", "maxmemory", "100mb"}, "OK"},
	{[]string{"CONFIG", "GET", "maxmemory"}, `["maxmemory" "104857600"]`},
	{[]string{"CONFIG", "SET", "maxmemory", "1gb", "maxmemory-policy", "allkeys-lru"}, "OK"},
	{[]string{"CONFIG", "GET", "maxmemory"}, `["maxmemory" "1073741824"]`},
	{[]string{"CONFIG", "GET", "maxmemory-policy"}, `["maxmemory-policy" "allkeys-lru"]`},
	{[]string{"CONFIG", "SET", "maxmemory-policy", "VOLATILE-TTL"}, "OK"},
	{[]string{"CONFIG", "GET", "maxmemory-policy"}, `["maxmemory-policy" "volatile-ttl"]`},
	{[]string{"CONFIG", "SET", "maxmemory", "0", "maxmemory-policy", "noeviction"}, "OK"},
	{[]string{"CONFIG", "SET", "maxmemory"}, "ERR wrong number of arguments for 'config|set' command"},
}

func checkCases(t *testing.T, c *testClient, label string) {
	t.Helper()
	runCases(t, c, label, commandCases)
	runCases(t, c, label, expiryCases)
	runCases(t, c, label, configCases)
}

func runCases(t *testing.T, c *testClient, label string, cases []commandCase) {
	t.Helper()
	for _, tc := range cases {
		got := c.do(tc.args...)
		switch {
		case tc.args[0] == "KEYS":
			if !sameSet(got, `["max" "min"]`) {
				t.Errorf("%s %v = %s", label, tc.args, got)
			}
		case strings.HasPrefix(tc.want, "~"):
			if !closeInt(got, tc.want[1:], 1000) {
				t.Errorf("%s %v = %s, want about %s", label, tc.args, got, tc.want[1:])
			}
		case got != tc.want:
			t.Errorf("%s %v = %s, want %s", label, tc.args, got, tc.want)
		}
	}
}

func closeInt(got, want string, tolerance int64) bool {
	var g, w int64
	if _, err := fmt.Sscanf(got, "(int) %d", &g); err != nil {
		return false
	}
	fmt.Sscanf(want, "(int) %d", &w)
	return g <= w && g >= w-tolerance
}

func sameSet(a, b string) bool {
	split := func(s string) []string {
		f := strings.Fields(strings.Trim(s, "[]"))
		slices.Sort(f)
		return f
	}
	return slices.Equal(split(a), split(b))
}

func TestDataCommands(t *testing.T) {
	forEachEngine(t, func(t *testing.T, addr string) {
		checkCases(t, newClient(t, addr), "shardis")
	})
}

// Differential test: replay the same commands against a real redis-server
// and check it gives exactly the replies we expect, i.e. that our expected
// values are really Redis's behavior, not our guesses. Skipped without redis-server.
func TestMatchesRealRedis(t *testing.T) {
	bin, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not installed")
	}
	port := strconv.Itoa(20000 + rand.IntN(20000))
	dir := t.TempDir()
	cmd := exec.Command(bin, "--port", port, "--save", "", "--appendonly", "no", "--dir", dir,
		"--logfile", filepath.Join(dir, "redis.log"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	addr := "127.0.0.1:" + port
	for range 100 {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	checkCases(t, newClient(t, addr), "redis")
}

func TestScanReturnsEveryKeyOnce(t *testing.T) {
	forEachEngine(t, func(t *testing.T, addr string) {
		c := newClient(t, addr)
		var args []string
		for i := range 1000 {
			args = append(args, fmt.Sprint("user:", i), "x")
		}
		c.do(append([]string{"MSET"}, args...)...)
		c.do("SET", "other", "x")

		seen := map[string]int{}
		cursor := "0"
		for calls := 0; ; calls++ {
			if calls > 20000 {
				t.Fatal("SCAN never finished")
			}
			v := scan(t, c, cursor, "MATCH", "user:*", "COUNT", "50")
			cursor = v.cursor
			for _, k := range v.keys {
				seen[k]++
			}
			if cursor == "0" {
				break
			}
		}
		if len(seen) != 1000 {
			t.Fatalf("saw %d distinct keys, want 1000", len(seen))
		}
		for k, n := range seen {
			if n != 1 || !strings.HasPrefix(k, "user:") {
				t.Fatalf("key %s seen %d times", k, n)
			}
		}
	})
}

type scanResult struct {
	cursor string
	keys   []string
}

func scan(t *testing.T, c *testClient, args ...string) scanResult {
	t.Helper()
	bargs := [][]byte{[]byte("SCAN")}
	for _, a := range args {
		bargs = append(bargs, []byte(a))
	}
	c.w.WriteCommand(bargs...)
	c.w.Flush()
	v, err := c.r.ReadValue()
	if err != nil || v.Kind != resp.Array || len(v.Elems) != 2 {
		t.Fatalf("SCAN reply %v, %v", v, err)
	}
	res := scanResult{cursor: string(v.Elems[0].Str)}
	for _, e := range v.Elems[1].Elems {
		res.keys = append(res.keys, string(e.Str))
	}
	return res
}

// Atomicity through the real network path: 50 clients x 200 INCRs each.
func TestConcurrentIncrLosesNothing(t *testing.T) {
	forEachEngine(t, func(t *testing.T, addr string) {
		const clients, each = 50, 200
		var wg sync.WaitGroup
		for range clients {
			c := newClient(t, addr)
			wg.Go(func() {
				for range each {
					c.do("INCR", "counter")
				}
			})
		}
		wg.Wait()
		if got := newClient(t, addr).do("GET", "counter"); got != strconv.Quote(strconv.Itoa(clients*each)) {
			t.Fatalf("counter = %s, want %d", got, clients*each)
		}
	})
}

// SET NX is how people build simple locks. Exactly one racer may win.
func TestSetNXHasExactlyOneWinner(t *testing.T) {
	forEachEngine(t, func(t *testing.T, addr string) {
		for round := range 20 {
			key := fmt.Sprint("lock:", round)
			var wg sync.WaitGroup
			var mu sync.Mutex
			wins := 0
			start := make(chan struct{})
			for i := range 50 {
				c := newClient(t, addr)
				wg.Go(func() {
					<-start // release all racers at once
					if c.do("SET", key, strconv.Itoa(i), "NX") == "OK" {
						mu.Lock()
						wins++
						mu.Unlock()
					}
				})
			}
			close(start)
			wg.Wait()
			if wins != 1 {
				t.Fatalf("round %d: %d winners, want 1", round, wins)
			}
		}
	})
}

// MSET writes both keys together; MGET must never see one updated without the other.
func TestMSetIsNeverSeenHalfDone(t *testing.T) {
	forEachEngine(t, func(t *testing.T, addr string) {
		writer := newClient(t, addr)
		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			defer close(done)
			for i := range 3000 {
				v := strconv.Itoa(i)
				writer.do("MSET", "a", v, "{other}b", v)
			}
		})
		for range 4 {
			c := newClient(t, addr)
			wg.Go(func() {
				for {
					select {
					case <-done:
						return
					default:
					}
					got := c.do("MGET", "a", "{other}b")
					parts := strings.Fields(strings.Trim(got, "[]"))
					if len(parts) != 2 || parts[0] != parts[1] {
						t.Errorf("torn read: %s", got)
						return
					}
				}
			})
		}
		wg.Wait()
	})
}

// Real clock, real network: a key with a 100ms TTL is gone after it passes.
func TestKeyExpiresOverTheNetwork(t *testing.T) {
	forEachEngine(t, func(t *testing.T, addr string) {
		c := newClient(t, addr)
		if got := c.do("SET", "short", "v", "PX", "100"); got != "OK" {
			t.Fatal(got)
		}
		if got := c.do("GET", "short"); got != `"v"` {
			t.Fatalf("GET before expiry = %s", got)
		}
		time.Sleep(150 * time.Millisecond)
		if got := c.do("GET", "short"); got != "(nil)" {
			t.Fatalf("GET after expiry = %s", got)
		}
	})
}

// Keys that are never read must still disappear: only active expiry can
// remove them. DBSIZE counts stored keys (expired or not), so it only drops
// to 0 if the background loop deleted them.
func TestUnreadKeysAreCleanedUpInBackground(t *testing.T) {
	forEachEngine(t, func(t *testing.T, addr string) {
		c := newClient(t, addr)
		var args []string
		for i := range 2000 {
			args = append(args, fmt.Sprint("tmp:", i), "x")
		}
		c.do(append([]string{"MSET"}, args...)...)
		for i := range 2000 {
			c.do("PEXPIRE", fmt.Sprint("tmp:", i), "50")
		}
		c.do("SET", "keeper", "v")
		deadline := time.Now().Add(5 * time.Second)
		for c.do("DBSIZE") != "(int) 1" {
			if time.Now().After(deadline) {
				t.Fatalf("DBSIZE still %s after 5s", c.do("DBSIZE"))
			}
			time.Sleep(20 * time.Millisecond)
		}
		if got := c.do("GET", "keeper"); got != `"v"` {
			t.Fatalf("persistent key affected: %s", got)
		}
	})
}

// parseInfo turns an INFO reply into field → value.
func parseInfo(t *testing.T, raw string) map[string]string {
	t.Helper()
	s, err := strconv.Unquote(raw)
	if err != nil {
		t.Fatalf("INFO reply %q is not a bulk string", raw)
	}
	out := map[string]string{}
	for _, line := range strings.Split(s, "\r\n") {
		if k, v, ok := strings.Cut(line, ":"); ok && !strings.HasPrefix(line, "#") {
			out[k] = v
		}
	}
	return out
}

func infoInt(t *testing.T, info map[string]string, field string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(info[field], 10, 64)
	if err != nil {
		t.Fatalf("INFO %s = %q", field, info[field])
	}
	return n
}

func TestInfo(t *testing.T) {
	addr, _ := startServer(t)
	c := newClient(t, addr)
	c.do("SET", "a", "1")
	c.do("SET", "b", "2", "EX", "100")
	c.do("GET", "a")
	c.do("GET", "nope")
	all := parseInfo(t, c.do("INFO"))
	for _, f := range []string{"redis_version", "tcp_port", "connected_clients", "used_memory", "maxmemory_policy", "keyspace_hits", "evicted_keys"} {
		if _, ok := all[f]; !ok {
			t.Errorf("INFO missing %s", f)
		}
	}
	if all["db0"] != "keys=2,expires=1,avg_ttl=0" {
		t.Errorf("db0 = %q", all["db0"])
	}
	if infoInt(t, all, "keyspace_hits") != 1 || infoInt(t, all, "keyspace_misses") != 1 {
		t.Errorf("hits/misses = %s/%s", all["keyspace_hits"], all["keyspace_misses"])
	}
	if infoInt(t, all, "total_commands_processed") < 5 {
		t.Errorf("total_commands_processed = %s", all["total_commands_processed"])
	}
	mem := parseInfo(t, c.do("INFO", "memory"))
	if _, ok := mem["keyspace_hits"]; ok {
		t.Error("INFO memory included the stats section")
	}
	if infoInt(t, mem, "used_memory") <= 0 {
		t.Error("used_memory should be positive with keys stored")
	}
}

// The whole memory-limit story over the network: refuse writes when full
// (noeviction), keep reads and DEL working, then switch policy at runtime
// and watch eviction keep memory at the limit.
func TestMaxMemoryOverTheNetwork(t *testing.T) {
	forEachEngine(t, func(t *testing.T, addr string) {
		c := newClient(t, addr)
		const limit = 16 << 20
		if got := c.do("CONFIG", "SET", "maxmemory", "16mb"); got != "OK" {
			t.Fatal(got)
		}
		val := strings.Repeat("x", 1000)
		i, reply := 0, ""
		for ; i < 100000; i++ {
			if reply = c.do("SET", fmt.Sprint("k:", i), val); reply != "OK" {
				break
			}
		}
		if reply != "OOM command not allowed when used memory > 'maxmemory'." {
			t.Fatalf("after %d writes got %q, want the OOM error", i, reply)
		}
		if got := c.do("GET", "k:0"); got != strconv.Quote(val) {
			t.Fatalf("GET when full = %.20s", got)
		}
		if got := c.do("DEL", "k:0", "k:1"); got != "(int) 2" {
			t.Fatalf("DEL when full = %s", got)
		}

		c.do("CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
		for j := i; j < i+20000; j++ {
			if got := c.do("SET", fmt.Sprint("k:", j), val); got != "OK" {
				t.Fatalf("SET with allkeys-lru = %s", got)
			}
		}
		info := parseInfo(t, c.do("INFO"))
		if infoInt(t, info, "evicted_keys") == 0 {
			t.Fatal("no keys evicted")
		}
		if used := infoInt(t, info, "used_memory"); used > limit+64<<10 {
			t.Fatalf("used_memory %d over the %d limit", used, limit)
		}
	})
}
