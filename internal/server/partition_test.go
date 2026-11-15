package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// all sends a command to every live node (FAULT PARTITION must reach each one).
func (tc *testCluster) all(t *testing.T, skip string, args ...string) {
	t.Helper()
	for name, n := range tc.nodes {
		if name == skip {
			continue
		}
		if got := newClient(t, n.addr).do(args...); got != "OK" {
			t.Fatalf("%v on %s: %s", args, name, got)
		}
	}
}

// writeLoop writes key-0, key-1, ... to addr every few ms until stop, and
// records which writes were acknowledged and when the node first refused.
type writeLog struct {
	mu          sync.Mutex
	acked       map[string]time.Time
	firstRefuse time.Time
}

func writeLoop(t *testing.T, addr, prefix string, stop chan struct{}) *writeLog {
	wl := &writeLog{acked: map[string]time.Time{}}
	c := newClient(t, addr)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			k := fmt.Sprintf("%s:%d", prefix, i)
			got := c.do("SET", k, "v")
			wl.mu.Lock()
			if got == "OK" {
				wl.acked[k] = time.Now()
			} else if strings.HasPrefix(got, "CLUSTERDOWN") && wl.firstRefuse.IsZero() {
				wl.firstRefuse = time.Now()
			}
			wl.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return wl
}

// Scenario A: primary a is cut off from everyone else (its clients can still
// reach it). It keeps accepting writes until it notices it's in a minority
// (~node timeout), then refuses. The majority fails over to d. When the
// partition heals, a becomes d's replica and the writes it accepted during
// the split are gone.
func TestPartitionIsolatedPrimary(t *testing.T) {
	tc := startCluster(t)
	tag := "{" + tc.keyIn("a") + "}"
	a := newClient(t, tc.nodes["a"].addr)
	a.do("SET", tag+":before", "safe")
	if got := a.do("WAIT", "1", "2000"); got != "(int) 1" {
		t.Fatalf("pre-partition write not replicated: WAIT = %s", got)
	}

	stop := make(chan struct{})
	defer close(stop)
	split := time.Now()
	tc.all(t, "", "FAULT", "PARTITION", "a")
	wl := writeLoop(t, tc.nodes["a"].addr, tag+":during", stop)

	// The majority elects d.
	d := newClient(t, tc.nodes["d"].addr)
	var dFirstOK time.Time
	eventually(t, "d takes over on the majority side", func() bool {
		if d.do("SET", tag+":majority", "x") == "OK" {
			dFirstOK = time.Now()
			return true
		}
		return false
	})
	eventually(t, "a refuses writes (minority)", func() bool {
		wl.mu.Lock()
		defer wl.mu.Unlock()
		return !wl.firstRefuse.IsZero()
	})
	wl.mu.Lock()
	var lastAOK time.Time
	for _, at := range wl.acked {
		if at.After(lastAOK) {
			lastAOK = at
		}
	}
	during := len(wl.acked)
	refusedAfter := wl.firstRefuse.Sub(split)
	wl.mu.Unlock()
	t.Logf("a accepted %d writes for %v after the split, then refused; d took over after %v",
		during, refusedAfter.Round(time.Millisecond), dFirstOK.Sub(split).Round(time.Millisecond))
	if during == 0 {
		t.Fatal("a should have accepted writes before noticing the partition")
	}
	// Usually a stops well before d starts (ADR 030: a needs one node
	// timeout, d needs that plus FAIL agreement and an election). But that's
	// two timers racing, not a guarantee: under heavy CPU load this test
	// once saw a acknowledge a write 0.7 ms after d's first. So we bound the
	// overlap instead of demanding zero.
	if overlap := lastAOK.Sub(dFirstOK); overlap > 0 {
		t.Logf("split-brain overlap: a acknowledged writes for %v after d started taking them", overlap)
		if overlap > maxOverlap {
			t.Fatalf("split brain lasted %v (> %v): a kept acknowledging writes long after d took over", overlap, maxOverlap)
		}
	}

	// Heal: a must turn into d's replica, and its during-split writes are lost.
	tc.all(t, "", "FAULT", "HEAL")
	eventuallyWithin(t, healWait, "a becomes d's replica", func() bool {
		return strings.HasPrefix(a.do("ROLE"), fmt.Sprintf(`["slave" "127.0.0.1" (int) %d "connected"`, tc.nodes["d"].port))
	})
	d.do("READONLY")
	lost := 0
	wl.mu.Lock()
	for k := range wl.acked {
		if d.do("GET", k) == "(nil)" {
			lost++
		}
	}
	wl.mu.Unlock()
	t.Logf("after healing: %d of %d writes a acknowledged during the split are lost", lost, during)
	if lost != during {
		t.Fatalf("expected all %d split-brain writes lost, %d lost", during, lost)
	}
	if got := d.do("GET", tag+":before"); got != `"safe"` {
		t.Fatalf("pre-partition (replicated) write = %s", got)
	}
	a.do("READONLY")
	eventually(t, "a's data now matches d", func() bool { return a.do("GET", tag+":majority") == `"x"` && a.do("GET", tag+":during:0") == "(nil)" })
}

// healWait bounds recovery after FAULT HEAL: noticing the others are
// reachable again can take up to a node timeout (2 s here: a PING already
// pending from the partition has to time out before the link is redialed),
// then a minority node waits the rejoin delay (another 2 s) before serving.
// Plus slack for the race detector.
const healWait = 10 * time.Second

// maxOverlap bounds how long both sides may accept writes in scenario A.
// Zero in practice; anything near a node timeout would mean the minority
// check is broken.
const maxOverlap = 500 * time.Millisecond

// Scenario B: a is cut off together with its own replica d. The majority
// can't fail over (no replica on its side), and the minority can't serve
// (no majority). Those slots, and with full coverage the whole cluster, are
// down until the partition heals. CAP: no availability for that data.
func TestPartitionPrimaryWithItsReplica(t *testing.T) {
	tc := startCluster(t)
	ka := tc.keyIn("a")
	b, a := newClient(t, tc.nodes["b"].addr), newClient(t, tc.nodes["a"].addr)
	a.do("SET", ka, "v")
	tc.all(t, "", "FAULT", "PARTITION", "a", "d")
	eventually(t, "majority marks a FAIL and goes down", func() bool {
		return clusterInfoField(t, b, "cluster_state") == "fail"
	})
	eventually(t, "minority a refuses too", func() bool { return strings.HasPrefix(a.do("SET", ka, "x"), "CLUSTERDOWN") })
	time.Sleep(3 * time.Second) // no failover may happen: d can't win votes from the majority
	if got := b.do("SET", tc.keyIn("b"), "x"); !strings.HasPrefix(got, "CLUSTERDOWN") {
		t.Fatalf("majority with a failed shard (full coverage) = %q", got)
	}
	if role := newClient(t, tc.nodes["d"].addr).do("ROLE"); !strings.HasPrefix(role, `["slave"`) {
		t.Fatalf("d was promoted without a majority: %s", role)
	}
	tc.all(t, "", "FAULT", "HEAL")
	eventuallyWithin(t, healWait, "everything back, a still primary", func() bool {
		return clusterInfoField(t, b, "cluster_state") == "ok" && a.do("GET", ka) == `"v"` && strings.HasPrefix(a.do("ROLE"), `["master"`)
	})
}

// Scenario C: a lone replica is cut off. Nothing fails over; it refuses to
// serve (minority) and catches up when the partition heals.
func TestPartitionLoneReplica(t *testing.T) {
	tc := startCluster(t)
	kc := tc.keyIn("c")
	f, c := newClient(t, tc.nodes["f"].addr), newClient(t, tc.nodes["c"].addr)
	f.do("READONLY")
	tc.all(t, "", "FAULT", "PARTITION", "f")
	eventually(t, "f refuses reads (minority)", func() bool { return strings.HasPrefix(f.do("GET", kc), "CLUSTERDOWN") })
	if got := c.do("SET", kc, "written-during"); got != "OK" {
		t.Fatalf("majority write = %q", got)
	}
	tc.all(t, "", "FAULT", "HEAL")
	eventuallyWithin(t, healWait, "f catches up", func() bool { return f.do("GET", kc) == `"written-during"` })
}

// With cluster-allow-reads-when-down, the minority side keeps serving
// (possibly stale) reads but refuses writes.
func TestReadsWhenDown(t *testing.T) {
	tc := startClusterWith(t, true)
	ka := tc.keyIn("a")
	a := newClient(t, tc.nodes["a"].addr)
	a.do("SET", ka, "v")
	tc.all(t, "", "FAULT", "PARTITION", "a")
	probe := "{" + ka + "}:probe" // same slot, different key, so probing doesn't change ka
	eventually(t, "a refuses writes", func() bool { return strings.HasPrefix(a.do("SET", probe, "x"), "CLUSTERDOWN") })
	if got := a.do("GET", ka); got != `"v"` {
		t.Fatalf("read on minority with reads-when-down = %q", got)
	}
	tc.all(t, "", "FAULT", "HEAL")
}
