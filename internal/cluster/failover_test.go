package cluster

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djism/shardis/internal/faults"
)

// seeded is a cluster started from a seed file: primaries a b c, replicas d
// e f (of a b c), plus optionally g as a second replica of a.
type seeded struct {
	t      *testing.T
	nodes  map[string]*testNode
	ports  map[string]int
	dir    string
	text   string
	faults map[string]*faults.Registry
}

const failTimeout = time.Second

func startSeeded(t *testing.T, withG bool) *seeded {
	t.Helper()
	names := []string{"a", "b", "c", "d", "e", "f"}
	if withG {
		names = append(names, "g")
	}
	s := &seeded{t: t, nodes: map[string]*testNode{}, ports: map[string]int{}, dir: t.TempDir(), faults: map[string]*faults.Registry{}}
	used := map[int]bool{}
	for _, n := range names {
		for s.ports[n] == 0 || used[s.ports[n]] {
			s.ports[n] = freePort(t)
		}
		used[s.ports[n]] = true
	}
	var b strings.Builder
	slots := map[string]string{"a": "0-5460", "b": "5461-10922", "c": "10923-16383"}
	primaryOf := map[string]string{"d": "a", "e": "b", "f": "c", "g": "a"}
	for _, n := range names {
		addr := fmt.Sprintf("127.0.0.1:%d", s.ports[n])
		if r, ok := slots[n]; ok {
			fmt.Fprintf(&b, "node %s %s %s primary %s\n", n, addr, addr, r)
		} else {
			fmt.Fprintf(&b, "node %s %s %s replica %s\n", n, addr, addr, primaryOf[n])
		}
	}
	s.text = b.String()
	for _, n := range names {
		s.start(n)
	}
	waitFor(t, "seeded cluster linked up", 10*time.Second, func() bool {
		for _, x := range s.nodes {
			for _, k := range x.View().Nodes {
				if !k.LinkUp || (k != x.View().Me && k.PongRecv.IsZero()) {
					return false
				}
			}
		}
		return true
	})
	return s
}

// start (or restart) node n, keeping its nodes.conf.
func (s *seeded) start(n string) *testNode {
	s.t.Helper()
	seed, err := Parse(strings.NewReader(s.text), n)
	if err != nil {
		s.t.Fatal(err)
	}
	if s.faults[n] == nil {
		s.faults[n] = faults.New()
	}
	addr := fmt.Sprintf("127.0.0.1:%d", s.ports[n])
	fr := &fakeReplicator{}
	c, err := New(Options{ClientAddr: addr, InternalAddr: addr, Seed: seed, NodeTimeout: failTimeout, Log: quiet,
		StateFile: filepath.Join(s.dir, n+".conf"), Replicator: fr, Faults: s.faults[n]})
	if err != nil {
		s.t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(c.Close)
	s.nodes[n] = &testNode{Cluster: c, port: s.ports[n], addr: addr, repl: fr}
	return s.nodes[n]
}

func (s *seeded) id(n string) string { return NodeID(n) }

// ownerEverywhere: every live node (except skip) sees `want` as the owner of slot.
func (s *seeded) ownerEverywhere(slot uint16, want string, skip ...string) bool {
	for name, x := range s.nodes {
		if contains(skip, name) {
			continue
		}
		o := x.View().Owner(slot)
		if o == nil || o.ID != s.id(want) {
			return false
		}
	}
	return true
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// The heart of Iteration 9: a primary dies, its replica is elected and takes
// over its slots, with no manual steps.
func TestPrimaryFailureTriggersFailover(t *testing.T) {
	s := startSeeded(t, false)
	killedAt := time.Now()
	s.nodes["a"].Close()
	took := waitFor(t, "d owns a's slots on every live node", 20*time.Second, func() bool {
		return s.ownerEverywhere(0, "d", "a") && s.ownerEverywhere(5460, "d", "a")
	})
	t.Logf("failover done %v after the primary died (node timeout %v)", took, failTimeout)
	_ = killedAt
	d := s.nodes["d"].View().Me
	if !d.IsPrimary() || d.Epoch <= 3 {
		t.Fatalf("d after failover: primary=%v epoch=%d (want a new, higher epoch)", d.IsPrimary(), d.Epoch)
	}
	if _, _, promoted := s.nodes["d"].repl.state(); !promoted {
		t.Fatal("replication was never told to promote d")
	}
	for _, n := range []string{"b", "c", "e", "f"} {
		v := s.nodes[n].View()
		if a := v.ByID(s.id("a")); a == nil || !a.Has(FlagFail) {
			t.Errorf("%s doesn't see a as FAIL", n)
		}
	}
	waitFor(t, "cluster_state ok again", 5*time.Second, func() bool { return s.nodes["b"].View().StateOK() })
}

// With two replicas, the one with more replicated data waits less and wins.
func TestMostUpToDateReplicaWins(t *testing.T) {
	s := startSeeded(t, true)
	s.nodes["d"].repl.set(100, 0)
	s.nodes["g"].repl.set(5000, 0)
	time.Sleep(3 * failTimeout) // let the offsets spread in headers
	s.nodes["a"].Close()
	waitFor(t, "a replica owns a's slots everywhere", 20*time.Second, func() bool {
		o := s.nodes["b"].View().Owner(0)
		return o != nil && o.ID != s.id("a") && (s.ownerEverywhere(0, "d", "a") || s.ownerEverywhere(0, "g", "a"))
	})
	if !s.ownerEverywhere(0, "g", "a") {
		t.Fatal("the less up-to-date replica won")
	}
	// The loser follows the winner.
	waitFor(t, "d replicates from g", 10*time.Second, func() bool {
		_, port, _ := s.nodes["d"].repl.state()
		me := s.nodes["d"].View().Me
		return port == s.ports["g"] && me.Primary != nil && me.Primary.ID == s.id("g")
	})
}

// If only one node can't reach a primary (a bad link, not a dead node),
// there's no majority, so no FAIL and no failover.
func TestNoFailoverWithoutMajority(t *testing.T) {
	s := startSeeded(t, false)
	// Cut a <-> b in both directions; everyone else still reaches a.
	s.faults["a"].Set(faults.Rule{Point: faults.BusMessage, Kind: faults.Drop, Target: "b"})
	s.faults["b"].Set(faults.Rule{Point: faults.BusMessage, Kind: faults.Drop, Target: "a"})
	waitFor(t, "b suspects a", 10*time.Second, func() bool {
		x := s.nodes["b"].View().ByID(s.id("a"))
		return x.Has(FlagPFail) || x.Has(FlagFail)
	})
	time.Sleep(5 * failTimeout)
	for name, x := range s.nodes {
		if a := x.View().ByID(s.id("a")); a.Has(FlagFail) {
			t.Fatalf("%s marked a FAIL with only one node unable to reach it", name)
		}
	}
	if !s.ownerEverywhere(0, "a") {
		t.Fatal("a lost its slots without a majority agreeing it was down")
	}
}

// After a failover, the old primary comes back, sees its slots taken with a
// higher epoch, and turns itself into a replica of the new primary.
func TestOldPrimaryComesBackAsReplica(t *testing.T) {
	s := startSeeded(t, false)
	s.nodes["a"].Close()
	waitFor(t, "failover to d", 20*time.Second, func() bool { return s.ownerEverywhere(0, "d", "a") })
	a := s.start("a") // restarts from its nodes.conf, still believing it owns 0-5460
	waitFor(t, "a demotes itself to replica of d", 10*time.Second, func() bool {
		me := a.View().Me
		_, port, _ := a.repl.state()
		return !me.IsPrimary() && me.Primary != nil && me.Primary.ID == s.id("d") && port == s.ports["d"]
	})
	waitFor(t, "everyone sees a as healthy replica of d", 10*time.Second, func() bool {
		for _, x := range s.nodes {
			n := x.View().ByID(s.id("a"))
			if n == nil || n.Has(FlagFail) || n.IsPrimary() {
				return false
			}
		}
		return s.ownerEverywhere(0, "d")
	})
}

// A replica that's been cut off from its primary for too long has stale
// data and must not run (ADR 029): the slots stay down instead.
func TestStaleReplicaDoesNotRunForPrimary(t *testing.T) {
	s := startSeeded(t, false)
	s.nodes["d"].repl.set(0, time.Hour)
	s.nodes["a"].Close()
	waitFor(t, "a FAIL", 15*time.Second, func() bool {
		x := s.nodes["b"].View().ByID(s.id("a"))
		return x.Has(FlagFail)
	})
	time.Sleep(4 * failTimeout)
	if !s.ownerEverywhere(0, "a", "a") {
		t.Fatal("a stale replica took over")
	}
	if s.nodes["b"].View().StateOK() {
		t.Fatal("cluster says ok with a failed primary and no failover")
	}
}

// A primary votes at most once per epoch, and remembers it across restarts.
func TestVoteOncePerEpochAndPersist(t *testing.T) {
	text := "node a 127.0.0.1:1 127.0.0.1:1 primary 0-5460\nnode b 127.0.0.1:2 127.0.0.1:2 primary 5461-16383\n" +
		"node d 127.0.0.1:4 127.0.0.1:4 replica a\nnode g 127.0.0.1:5 127.0.0.1:5 replica a\n"
	seed, _ := Parse(strings.NewReader(text), "b")
	state := filepath.Join(t.TempDir(), "b.conf")
	c, err := New(Options{ClientAddr: "127.0.0.1:2", InternalAddr: "127.0.0.1:2", Seed: seed, NodeTimeout: failTimeout,
		StateFile: state, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	a := c.nodes[NodeID("a")]
	c.setFail(a, time.Now().Add(-time.Minute))
	c.currentEpoch = 10
	req := func(from string) *message {
		m := &message{typ: msgAuthReq, sender: NodeID(from), currentEpoch: 10, configEpoch: a.configEpoch,
			flags: wireReplica, primaryID: NodeID("a")}
		for s := 0; s <= 5460; s++ {
			m.setSlot(s)
		}
		return m
	}
	first := c.handleAuthRequest(c.nodes[NodeID("d")], req("d"), time.Now())
	second := c.handleAuthRequest(c.nodes[NodeID("g")], req("g"), time.Now())
	c.mu.Unlock()
	if first == nil || second != nil {
		t.Fatalf("first vote=%v second vote=%v: want exactly one vote in epoch 10", first != nil, second != nil)
	}
	c.saveIfDirty()
	again, err := New(Options{ClientAddr: "127.0.0.1:2", InternalAddr: "127.0.0.1:2", NodeTimeout: failTimeout, StateFile: state, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	if again.lastVoteEpoch != 10 {
		t.Fatalf("after restart lastVoteEpoch = %d, want 10", again.lastVoteEpoch)
	}
}

// A node cut off from the majority marks itself as minority (stops serving);
// once it can reach the majority again it waits a rejoin delay before
// serving, giving gossip time to deliver any slot changes first.
func TestMinorityThenRejoinDelay(t *testing.T) {
	s := startSeeded(t, false)
	for name := range s.nodes { // isolate replica e from everyone (both directions)
		if name == "e" {
			for other := range s.nodes {
				if other != "e" {
					s.faults["e"].Set(faults.Rule{Point: faults.BusMessage, Kind: faults.Drop, Target: other})
				}
			}
		} else {
			s.faults[name].Set(faults.Rule{Point: faults.BusMessage, Kind: faults.Drop, Target: "e"})
		}
	}
	e := s.nodes["e"]
	waitFor(t, "e sees itself in a minority", 10*time.Second, func() bool { return e.View().Minority() })
	if e.View().Serves(0) {
		t.Fatal("a minority node still serves")
	}
	if !s.nodes["b"].View().StateOK() {
		t.Fatal("the majority went down because one replica was cut off")
	}
	for _, r := range s.faults {
		r.Clear("")
	}
	waitFor(t, "e out of the minority", 10*time.Second, func() bool { return !e.View().Minority() })
	if v := e.View(); !v.Rejoining() || v.Serves(0) {
		t.Fatalf("right after rejoining: rejoining=%v serves=%v (want a delay before serving)", v.Rejoining(), v.Serves(0))
	}
	took := waitFor(t, "e serves again", 10*time.Second, func() bool { return e.View().Serves(0) })
	t.Logf("served again %v after regaining the majority (rejoin delay %v)", took.Round(time.Millisecond), e.rejoinDelay())
}
