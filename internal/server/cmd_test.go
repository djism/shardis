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
var commandCases = []struct {
	args []string
	want string
}{
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

func checkCases(t *testing.T, c *testClient, label string) {
	t.Helper()
	for _, tc := range commandCases {
		got := c.do(tc.args...)
		if tc.args[0] == "KEYS" {
			if !sameSet(got, `["max" "min"]`) {
				t.Errorf("%s %v = %s", label, tc.args, got)
			}
			continue
		}
		if got != tc.want {
			t.Errorf("%s %v = %s, want %s", label, tc.args, got, tc.want)
		}
	}
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
