package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djism/shardis/internal/admin"
	"github.com/djism/shardis/internal/resp"
	"github.com/djism/shardis/internal/testcluster"
)

func b(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func newClient(t *testing.T, tc *testcluster.Cluster, o Options) *Cluster {
	t.Helper()
	c, err := NewCluster([]string{tc.Nodes["b"].Addr}, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// counters runs writers that INCR their own counters until stop, and
// records per counter how many INCRs were acknowledged and how many ended
// in ErrUnknownOutcome. Any other error is counted separately.
type counters struct {
	acked, unknown [400]atomic.Int64
	otherErrs      atomic.Int64
	firstErr       atomic.Value
}

func (cs *counters) run(c *Cluster, writers int, stop chan struct{}, wg *sync.WaitGroup) {
	for w := range writers {
		wg.Go(func() {
			for i := w; ; i = (i + writers) % len(cs.acked) {
				select {
				case <-stop:
					return
				default:
				}
				v, err := c.Do(context.Background(), b("INCR", fmt.Sprint("ctr:", i))...)
				switch {
				case errors.Is(err, ErrUnknownOutcome):
					cs.unknown[i].Add(1)
				case err != nil || v.Kind == resp.Error:
					cs.otherErrs.Add(1)
					cs.firstErr.CompareAndSwap(nil, fmt.Sprint(err, string(v.Str)))
				default:
					cs.acked[i].Add(1)
				}
			}
		})
	}
}

// check reads every counter and returns totals: lost = acknowledged INCRs
// that aren't there; dup = increments beyond acked+unknown (applied twice).
func (cs *counters) check(t *testing.T, c *Cluster) (acked, unknown, lost, dup int64) {
	t.Helper()
	for i := range cs.acked {
		v, err := c.Do(context.Background(), b("GET", fmt.Sprint("ctr:", i))...)
		if err != nil {
			t.Fatalf("reading counter %d: %v", i, err)
		}
		got, _ := strconv.ParseInt(string(v.Str), 10, 64)
		a, u := cs.acked[i].Load(), cs.unknown[i].Load()
		acked, unknown = acked+a, unknown+u
		if got < a {
			lost += a - got
		}
		if got > a+u {
			dup += got - a - u
		}
	}
	return
}

func waitPrimary(t *testing.T, tc *testcluster.Cluster, name string) {
	t.Helper()
	testcluster.Eventually(t, 15*time.Second, name+" promoted", func() bool {
		return strings.Contains(testcluster.Do(t, tc.Nodes[name].Addr, "ROLE").String(), "master")
	})
}

// A primary dies mid-traffic. With idempotent writes (SET) the client hides
// the whole failover: zero errors, just slower requests.
func TestFailoverIsInvisibleForSafeCommands(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{})
	c := newClient(t, tc, Options{})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ok, errs atomic.Int64
	var first atomic.Value
	for w := range 6 {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("w%d:%d", w, i%500)
				v, err := c.Do(context.Background(), b("SET", k, strconv.Itoa(i))...)
				if err == nil && v.Kind != resp.Error {
					v, err = c.Do(context.Background(), b("GET", k)...)
				}
				if err != nil || v.Kind == resp.Error {
					errs.Add(1)
					first.CompareAndSwap(nil, fmt.Sprint(err, string(v.Str)))
					continue
				}
				ok.Add(1)
			}
		})
	}
	time.Sleep(300 * time.Millisecond)
	killed := time.Now()
	tc.Nodes["a"].Stop()
	waitPrimary(t, tc, "d")
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	t.Logf("failover took %v; %d SET+GET pairs ok, %d errors; client: %d net errors, %d retries, %d CLUSTERDOWN, %d MOVED",
		time.Since(killed).Round(time.Millisecond), ok.Load(), errs.Load(), c.Stats.NetErrors.Load(),
		c.Stats.Retries.Load(), c.Stats.ClusterDown.Load(), c.Stats.Moved.Load())
	if errs.Load() > 0 {
		t.Fatalf("%d errors during failover, first: %v", errs.Load(), first.Load())
	}
}

// INCR isn't safe to resend. Under RetrySafe, the commands in flight when the
// primary died come back as ErrUnknownOutcome (never anything else), and no
// counter is ever higher than acked + unknown: nothing applied twice. Under
// RetryAll there are no errors at all, but an INCR can be applied twice.
func TestFailoverRetryPolicies(t *testing.T) {
	for _, tcase := range []struct {
		name   string
		policy RetryPolicy
	}{{"safe", RetrySafe}, {"all", RetryAll}} {
		t.Run(tcase.name, func(t *testing.T) {
			tc := testcluster.Start(t, testcluster.Options{})
			c := newClient(t, tc, Options{Retry: tcase.policy})
			var cs counters
			stop := make(chan struct{})
			var wg sync.WaitGroup
			cs.run(c, 6, stop, &wg)
			time.Sleep(300 * time.Millisecond)
			tc.Nodes["a"].Stop()
			waitPrimary(t, tc, "d")
			time.Sleep(300 * time.Millisecond)
			close(stop)
			wg.Wait()
			acked, unknown, lost, dup := cs.check(t, c)
			t.Logf("policy %s: %d INCRs acked, %d unknown outcome, %d other errors; %d acked-but-lost (async replication), %d applied twice",
				tcase.name, acked, unknown, cs.otherErrs.Load(), lost, dup)
			if cs.otherErrs.Load() > 0 {
				t.Fatalf("unexpected errors: %v", cs.firstErr.Load())
			}
			if tcase.policy == RetrySafe && dup > 0 {
				t.Fatalf("RetrySafe applied %d INCRs twice", dup)
			}
			if tcase.policy == RetryAll && unknown > 0 {
				t.Fatalf("RetryAll returned %d unknown outcomes", unknown)
			}
		})
	}
}

// Resharding under load: a new primary joins and takes 1000 slots from a.
// Counters must come out exact: zero errors, nothing lost, nothing doubled
// (no connection breaks, so no unknown outcomes either). The full 4096-slot
// rebalance is in the benchmark; under -race it takes over a minute.
func TestReshardIsInvisible(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{})
	c := newClient(t, tc, Options{})
	var cs counters
	stop := make(chan struct{})
	var wg sync.WaitGroup
	cs.run(c, 6, stop, &wg)
	g := tc.AddEmpty(t, "g")
	if _, err := admin.AddNode(tc.Nodes["b"].Addr, g.Addr, ""); err != nil {
		t.Fatal(err)
	}
	if err := admin.Reshard(tc.Nodes["b"].Addr, tc.Nodes["a"].ID(), g.ID(), 1000, admin.MoveOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	acked, unknown, lost, dup := cs.check(t, c)
	t.Logf("%d INCRs during the rebalance; client followed %d MOVED, %d ASK, %d TRYAGAIN", acked, c.Stats.Moved.Load(), c.Stats.Ask.Load(), c.Stats.TryAgain.Load())
	if cs.otherErrs.Load() > 0 || unknown > 0 || lost > 0 || dup > 0 {
		t.Fatalf("errors %d (first %v), unknown %d, lost %d, doubled %d", cs.otherErrs.Load(), cs.firstErr.Load(), unknown, lost, dup)
	}
	if c.Stats.Moved.Load() == 0 {
		t.Fatal("expected MOVED redirects during the rebalance")
	}
}

// Replica reads: reads go to replicas; when a replica dies they fall back
// to the primary without errors.
func TestReplicaReads(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{})
	c := newClient(t, tc, Options{ReadFromReplicas: true})
	k := tc.KeyIn("a")
	if v, err := c.Do(context.Background(), b("SET", k, "v1")...); err != nil || string(v.Str) != "OK" {
		t.Fatalf("SET: %v %s", err, v.Str)
	}
	testcluster.Eventually(t, 5*time.Second, "replica has the value", func() bool {
		v, err := c.Do(context.Background(), b("GET", k)...)
		return err == nil && string(v.Str) == "v1"
	})
	if c.Stats.ReplicaReads.Load() == 0 {
		t.Fatal("no read went to a replica")
	}
	tc.Nodes["d"].Stop() // a's only replica
	for range 50 {
		v, err := c.Do(context.Background(), b("GET", k)...)
		if err != nil || string(v.Str) != "v1" {
			t.Fatalf("read after the replica died: %v %q", err, v.Str)
		}
	}
}

// When a shard is gone for good (primary and replica dead), commands for
// it fail after MaxRetryTime instead of hanging, and the error says why.
func TestRetryBudget(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{})
	c := newClient(t, tc, Options{MaxRetryTime: 1500 * time.Millisecond})
	tc.Nodes["d"].Stop()
	tc.Nodes["a"].Stop()
	start := time.Now()
	_, err := c.Do(context.Background(), b("GET", tc.KeyIn("a"))...)
	if !errors.Is(err, ErrRetryBudget) || time.Since(start) > 3*time.Second {
		t.Fatalf("after %v: err = %v, want ErrRetryBudget after ~1.5s", time.Since(start), err)
	}
	t.Logf("gave up after %v: %v", time.Since(start).Round(time.Millisecond), err)
	if _, err := c.Do(context.Background(), b("MSET", "x", "1", "y", "2")...); !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("MSET across slots: %v", err)
	}
}

// How stale are replica reads? Write through the primary, then read the same
// key right away through a replica-reading client. Logged, not asserted: it
// depends on replication lag, which is the point of ADR 039.
func TestReplicaReadStaleness(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{})
	w := newClient(t, tc, Options{})
	r := newClient(t, tc, Options{ReadFromReplicas: true})
	const n = 2000
	stale := 0
	for i := range n {
		k := fmt.Sprint("fresh:", i%50)
		if _, err := w.Do(context.Background(), b("SET", k, strconv.Itoa(i))...); err != nil {
			t.Fatal(err)
		}
		v, err := r.Do(context.Background(), b("GET", k)...)
		if err != nil {
			t.Fatal(err)
		}
		if string(v.Str) != strconv.Itoa(i) {
			stale++
		}
	}
	t.Logf("read-after-write through a replica: %d of %d reads stale (%.1f%%); %d went to replicas",
		stale, n, 100*float64(stale)/n, r.Stats.ReplicaReads.Load())
}
