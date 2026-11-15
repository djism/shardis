package server

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/djism/shardis/internal/cluster"
	"github.com/djism/shardis/internal/keyslot"
	"github.com/djism/shardis/internal/resp"
	shardisclient "github.com/djism/shardis/pkg/client"
)

// raw sends a command with binary arguments and returns the parsed reply.
func (tc *testClient) raw(args ...[]byte) resp.Value {
	tc.w.WriteCommand(args...)
	if err := tc.w.Flush(); err != nil {
		tc.t.Fatal(err)
	}
	v, err := tc.r.ReadValue()
	if err != nil {
		tc.t.Fatal(err)
	}
	return v
}

func bs(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func TestDumpRestore(t *testing.T) {
	addr, _ := startServer(t)
	c := newClient(t, addr)
	c.do("SET", "src", "hello\x00world")
	payload := c.raw(bs("DUMP", "src")...).Str
	expect := func(got, want string) {
		t.Helper()
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	expect(render(c.raw([]byte("RESTORE"), []byte("dst"), []byte("0"), payload)), "OK")
	expect(c.do("GET", "dst"), strconv.Quote("hello\x00world"))
	expect(render(c.raw([]byte("RESTORE"), []byte("dst"), []byte("0"), payload)), "BUSYKEY Target key name already exists.")
	expect(render(c.raw([]byte("RESTORE"), []byte("dst"), []byte("5000"), payload, []byte("REPLACE"))), "OK")
	if ttl := c.do("PTTL", "dst"); ttl < "(int) 4000" || ttl > "(int) 5000" {
		t.Fatalf("PTTL after RESTORE with ttl 5000 = %s", ttl)
	}
	past := strconv.FormatInt(time.Now().Add(-time.Second).UnixMilli(), 10)
	expect(render(c.raw([]byte("RESTORE"), []byte("gone"), []byte(past), payload, []byte("ABSTTL"))), "OK")
	expect(c.do("EXISTS", "gone"), "(int) 0") // already expired: nothing stored
	bad := append([]byte(nil), payload...)
	bad[3] ^= 1
	expect(render(c.raw([]byte("RESTORE"), []byte("x"), []byte("0"), bad)), "ERR DUMP payload version or checksum are wrong")
	expect(c.do("DUMP", "missing"), "(nil)")
}

// tagIn returns a hash tag whose slot primary name owns, so "{tag}:..." keys
// all live in one slot.
func (tc *testCluster) tagIn(name string) (string, uint16) {
	for i := 0; ; i++ {
		tag := fmt.Sprint("t", i)
		s := keyslot.Of([]byte(tag))
		if tc.topo.Owner(s).Name == name {
			return tag, s
		}
	}
}

// migrateSlot moves one slot the way shardis-admin and redis-cli do, in the
// order that matters: target IMPORTING, source MIGRATING, keys in batches,
// then target NODE, source NODE, everyone else NODE.
func migrateSlot(t *testing.T, tc *testCluster, from, to string, slot uint16, batch int) {
	t.Helper()
	src, dst := newClient(t, tc.nodes[from].addr), newClient(t, tc.nodes[to].addr)
	srcID, dstID := cluster.NodeID(from), cluster.NodeID(to)
	s := strconv.Itoa(int(slot))
	must := func(c *testClient, args ...string) {
		t.Helper()
		if got := c.do(args...); got != "OK" {
			t.Fatalf("%v = %q", args, got)
		}
	}
	must(dst, "CLUSTER", "SETSLOT", s, "IMPORTING", srcID)
	must(src, "CLUSTER", "SETSLOT", s, "MIGRATING", dstID)
	host, port, _ := strings.Cut(tc.nodes[to].addr, ":")
	for {
		keys := src.raw(bs("CLUSTER", "GETKEYSINSLOT", s, strconv.Itoa(batch))...)
		if len(keys.Elems) == 0 {
			break
		}
		args := bs("MIGRATE", host, port, "", "0", "5000", "KEYS")
		for _, k := range keys.Elems {
			args = append(args, k.Str)
		}
		if v := src.raw(args...); render(v) != "OK" {
			t.Fatalf("MIGRATE = %s", render(v))
		}
	}
	must(dst, "CLUSTER", "SETSLOT", s, "NODE", dstID)
	must(src, "CLUSTER", "SETSLOT", s, "NODE", dstID)
	for _, n := range []string{"a", "b", "c"} {
		if n != from && n != to {
			must(newClient(t, tc.nodes[n].addr), "CLUSTER", "SETSLOT", s, "NODE", dstID)
		}
	}
}

// One slot, a → b, one step at a time, checking what clients see at each.
func TestMigrateSlotStepByStep(t *testing.T) {
	tc := startCluster(t)
	tag, slot := tc.tagIn("a")
	k1, k2, missing := "{"+tag+"}:1", "{"+tag+"}:2", "{"+tag+"}:missing"
	a, b, c := newClient(t, tc.nodes["a"].addr), newClient(t, tc.nodes["b"].addr), newClient(t, tc.nodes["c"].addr)
	a.do("SET", k1, "one")
	a.do("SET", k2, "two", "PX", "100000")
	s := strconv.Itoa(int(slot))
	aID, bID := cluster.NodeID("a"), cluster.NodeID("b")
	expect := func(c *testClient, want string, args ...string) {
		t.Helper()
		if got := c.do(args...); got != want {
			t.Fatalf("%v = %q, want %q", args, got, want)
		}
	}
	ask := fmt.Sprintf("ASK %d %s", slot, tc.nodes["b"].addr)
	movedA := fmt.Sprintf("MOVED %d %s", slot, tc.nodes["a"].addr)
	movedB := fmt.Sprintf("MOVED %d %s", slot, tc.nodes["b"].addr)

	expect(b, "OK", "CLUSTER", "SETSLOT", s, "IMPORTING", aID)
	expect(b, movedA, "GET", k1) // importing alone doesn't serve: the client must say ASKING
	expect(a, "OK", "CLUSTER", "SETSLOT", s, "MIGRATING", bID)
	if nodes := a.do("CLUSTER", "NODES"); !strings.Contains(nodes, fmt.Sprintf("[%d->-%s]", slot, bID)) {
		t.Fatalf("CLUSTER NODES on a has no migrating marker:\n%s", nodes)
	}
	expect(a, `"one"`, "GET", k1)  // still here: served
	expect(a, ask, "GET", missing) // not here: ask the target
	expect(a, ask, "SET", missing, "x")

	host, port, _ := strings.Cut(tc.nodes["b"].addr, ":")
	expect(a, "OK", "MIGRATE", host, port, k1, "0", "5000")
	expect(a, ask, "GET", k1)              // moved
	expect(a, errTryAgain, "MGET", k1, k2) // split across two nodes right now
	expect(b, movedA, "GET", k1)           // without ASKING, b still says "ask a"
	expect(b, "OK", "ASKING")
	expect(b, `"one"`, "GET", k1) // with ASKING: served
	expect(b, movedA, "GET", k1)  // ASKING lasted one command
	expect(b, "OK", "ASKING")
	expect(b, errTryAgain, "MGET", k1, k2) // k2 hasn't arrived
	expect(a, "ERR Can't assign hashslot "+s+" to a different node while I still hold keys for this hash slot.",
		"CLUSTER", "SETSLOT", s, "NODE", bID)

	expect(a, "OK", "MIGRATE", host, port, "", "0", "5000", "KEYS", k2)
	expect(a, "NOKEY", "MIGRATE", host, port, "", "0", "5000", "KEYS", k2)
	b.do("ASKING")
	if ttl := b.do("PTTL", k2); ttl < "(int) 90000" || len(ttl) != len("(int) 90000") {
		t.Fatalf("TTL not carried over: PTTL = %s", ttl)
	}
	epochBefore := clusterInfoField(t, b, "cluster_my_epoch")
	expect(b, "OK", "CLUSTER", "SETSLOT", s, "NODE", bID)
	if e := clusterInfoField(t, b, "cluster_my_epoch"); e == epochBefore {
		t.Fatalf("b's config epoch wasn't bumped (still %s)", e)
	}
	expect(a, ask, "GET", k1) // a hasn't been told yet: still migrating, key not here
	expect(a, "OK", "CLUSTER", "SETSLOT", s, "NODE", bID)
	expect(a, movedB, "GET", k1)
	expect(b, `"one"`, "GET", k1)
	expect(b, `["one" "two"]`, "MGET", k1, k2)
	if nodes := a.do("CLUSTER", "NODES"); strings.Contains(nodes, "->-") {
		t.Fatalf("migrating marker left behind:\n%s", nodes)
	}
	// c was never told: gossip carries b's higher epoch to it.
	eventually(t, "c learns the new owner by gossip", func() bool { return c.do("GET", k1) == movedB })
	// Replication: b's replica e gets the keys, a's replica d loses them.
	e, d := newClient(t, tc.nodes["e"].addr), newClient(t, tc.nodes["d"].addr)
	e.do("READONLY")
	d.do("READONLY")
	eventually(t, "b's replica has the migrated key", func() bool { return e.do("GET", k1) == `"one"` })
	eventually(t, "a's replica dropped it", func() bool {
		return !strings.Contains(d.do("KEYS", "*"), tag)
	})
}

// The real test of the gate (ADR 033): clients INCR counters in a slot while
// it migrates in small batches. go-redis follows MOVED and ASK on its own.
// If any INCR ran on a key that had just been migrated away (or was lost
// in flight), a counter would come out lower than the number of acks.
func TestMigrateUnderConcurrentWrites(t *testing.T) {
	tc := startCluster(t)
	tag, slot := tc.tagIn("a")
	ctx := context.Background()
	rc := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{tc.nodes["a"].addr}, MaxRedirects: 16})
	defer rc.Close()
	const nKeys, writers = 200, 8
	for i := range nKeys {
		if err := rc.Set(ctx, fmt.Sprintf("{%s}:%d", tag, i), 0, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	ours, err := shardisclient.NewCluster([]string{tc.nodes["a"].addr}, shardisclient.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer ours.Close()
	// Half the writers use go-redis, half our own client (pkg/client).
	incr := []func(key string) error{
		func(key string) error { return rc.Incr(ctx, key).Err() },
		func(key string) error {
			v, err := ours.Do(ctx, []byte("INCR"), []byte(key))
			if err == nil && v.Kind == resp.Error {
				err = fmt.Errorf("%s", v.Str)
			}
			return err
		},
	}
	var acks [nKeys]atomic.Int64
	var errs atomic.Int64
	var firstErr atomic.Value
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			do := incr[w%2]
			for i := w; ; i = (i + 7) % nKeys {
				select {
				case <-stop:
					return
				default:
				}
				if err := do(fmt.Sprintf("{%s}:%d", tag, i)); err != nil {
					errs.Add(1)
					firstErr.CompareAndSwap(nil, err.Error())
					continue
				}
				acks[i].Add(1)
			}
		})
	}
	time.Sleep(200 * time.Millisecond)
	migrateSlot(t, tc, "a", "b", slot, 10)
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	if errs.Load() > 0 {
		t.Fatalf("%d client errors during migration, first: %v", errs.Load(), firstErr.Load())
	}
	b := newClient(t, tc.nodes["b"].addr)
	var total int64
	for i := range nKeys {
		got := b.do("GET", fmt.Sprintf("{%s}:%d", tag, i))
		want := strconv.Quote(strconv.FormatInt(acks[i].Load(), 10))
		if got != want {
			t.Fatalf("key %d = %s on b, but %s INCRs were acknowledged", i, got, want)
		}
		total += acks[i].Load()
	}
	if n := newClient(t, tc.nodes["a"].addr).do("CLUSTER", "COUNTKEYSINSLOT", strconv.Itoa(int(slot))); n != "(int) 0" {
		t.Fatalf("keys left behind on a: %s", n)
	}
	t.Logf("%d INCRs acknowledged during the migration, all accounted for, 0 errors; our client saw %d MOVED, %d ASK, %d TRYAGAIN",
		total, ours.Stats.Moved.Load(), ours.Stats.Ask.Load(), ours.Stats.TryAgain.Load())
	if ours.Stats.Ask.Load() == 0 || ours.Stats.Moved.Load() == 0 {
		t.Fatalf("our client should have followed both ASK and MOVED during the migration")
	}
}
