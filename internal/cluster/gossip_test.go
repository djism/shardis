package cluster

import (
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djism/shardis/internal/faults"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// freePorts returns a port p where both p and p+10000 (the bus) are free.
func freePort(t testing.TB) int {
	t.Helper()
	for range 200 {
		p := 20000 + rand.IntN(20000)
		a, err1 := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		b, err2 := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p+10000))
		if a != nil {
			a.Close()
		}
		if b != nil {
			b.Close()
		}
		if err1 == nil && err2 == nil {
			return p
		}
	}
	t.Fatal("no free port pair")
	return 0
}

type fakeReplicator struct {
	mu       sync.Mutex
	host     string
	port     int
	promoted bool
	offset   int64
	linkDown time.Duration
}

func (f *fakeReplicator) ReplicaOf(host string, port int) error {
	f.mu.Lock()
	f.host, f.port = host, port
	f.promoted = false
	f.mu.Unlock()
	return nil
}

func (f *fakeReplicator) Promote() {
	f.mu.Lock()
	f.promoted = true
	f.mu.Unlock()
}

func (f *fakeReplicator) Offset() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.offset
}

func (f *fakeReplicator) LinkDownFor() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.linkDown
}

func (f *fakeReplicator) set(offset int64, linkDown time.Duration) {
	f.mu.Lock()
	f.offset, f.linkDown = offset, linkDown
	f.mu.Unlock()
}

func (f *fakeReplicator) state() (string, int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.host, f.port, f.promoted
}

type testNode struct {
	*Cluster
	port  int
	addr  string
	state string
	repl  *fakeReplicator
}

func startNode(t testing.TB, opts Options) *testNode {
	t.Helper()
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	if opts.ClientAddr == "" {
		opts.ClientAddr, opts.InternalAddr = addr, addr
	}
	if opts.NodeTimeout == 0 {
		opts.NodeTimeout = 2 * time.Second
	}
	if opts.Log == nil {
		opts.Log = quiet
	}
	fr := &fakeReplicator{}
	if opts.Replicator == nil {
		opts.Replicator = fr
	}
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return &testNode{Cluster: c, port: port, addr: addr, state: opts.StateFile, repl: fr}
}

func waitFor(t testing.TB, what string, timeout time.Duration, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for !cond() {
		if time.Since(start) > timeout {
			t.Fatalf("timed out after %v waiting for: %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return time.Since(start)
}

// converged: every node knows exactly n confirmed nodes.
func converged(nodes []*testNode, n int) bool {
	for _, x := range nodes {
		v := x.View()
		if len(v.Nodes) != n {
			return false
		}
		for _, k := range v.Nodes {
			if k.Has(FlagHandshake) {
				return false
			}
		}
	}
	return true
}

func meetChain(t testing.TB, nodes []*testNode) {
	t.Helper()
	for i := 0; i+1 < len(nodes); i++ {
		if err := nodes[i].Meet("127.0.0.1", nodes[i+1].port, 0); err != nil {
			t.Fatal(err)
		}
	}
}

func startNodes(t testing.TB, n int) []*testNode {
	nodes := make([]*testNode, n)
	for i := range nodes {
		nodes[i] = startNode(t, Options{})
	}
	return nodes
}

// "Done when": 6 nodes introduced in a chain all know all 6 within seconds.
func TestMeetChainConverges(t *testing.T) {
	nodes := startNodes(t, 6)
	meetChain(t, nodes)
	took := waitFor(t, "all 6 nodes know all 6", 15*time.Second, func() bool { return converged(nodes, 6) })
	t.Logf("6 nodes met in a chain (a-b, b-c, ...) converged in %v", took)
	ids := map[string]bool{}
	for _, x := range nodes {
		ids[x.View().Me.ID] = true
	}
	for _, x := range nodes {
		for _, k := range x.View().Nodes {
			if !ids[k.ID] {
				t.Fatalf("unknown ID %s in a view", k.ID)
			}
		}
	}
}

func TestSlotsSpreadByGossip(t *testing.T) {
	nodes := startNodes(t, 4)
	meetChain(t, nodes)
	waitFor(t, "convergence", 15*time.Second, func() bool { return converged(nodes, 4) })
	ranges := [][2]int{{0, 5460}, {5461, 10922}, {10923, 16383}}
	for i, r := range ranges {
		var slots []uint16
		for s := r[0]; s <= r[1]; s++ {
			slots = append(slots, uint16(s))
		}
		if err := nodes[i].AddSlots(slots); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "everyone agrees on all slots", 10*time.Second, func() bool {
		for _, x := range nodes {
			v := x.View()
			if !v.StateOK() {
				return false
			}
			for i, r := range ranges {
				if v.Owner(uint16(r[0])).ID != nodes[i].View().Me.ID || v.Owner(uint16(r[1])).ID != nodes[i].View().Me.ID {
					return false
				}
			}
		}
		return true
	})
	if err := nodes[3].AddSlots([]uint16{0}); err == nil || !strings.Contains(err.Error(), "already busy") {
		t.Fatalf("claiming a taken slot: %v", err)
	}
}

// Two nodes claim the same slot before they know each other: once they
// meet, the claim with the higher config epoch wins on both.
func TestHigherConfigEpochWins(t *testing.T) {
	a, b := startNode(t, Options{}), startNode(t, Options{})
	a.SetConfigEpoch(1)
	b.SetConfigEpoch(5)
	a.AddSlots([]uint16{42})
	b.AddSlots([]uint16{42})
	a.Meet("127.0.0.1", b.port, 0)
	bID := b.View().Me.ID
	waitFor(t, "both agree b owns slot 42", 10*time.Second, func() bool {
		return a.View().Owner(42) != nil && a.View().Owner(42).ID == bID && b.View().Owner(42).ID == bID
	})
}

// Primaries that start with the same config epoch (0) end up with distinct
// ones: the node with the smaller ID moves, by a rule both sides apply.
func TestEpochCollisionsAreResolved(t *testing.T) {
	nodes := startNodes(t, 4)
	meetChain(t, nodes)
	waitFor(t, "convergence", 15*time.Second, func() bool { return converged(nodes, 4) })
	waitFor(t, "distinct config epochs", 15*time.Second, func() bool {
		seen := map[uint64]bool{}
		for _, x := range nodes {
			e := x.View().Me.Epoch
			if seen[e] {
				return false
			}
			seen[e] = true
		}
		return true
	})
}

func TestReplicateAndRoleSpreads(t *testing.T) {
	nodes := startNodes(t, 3)
	meetChain(t, nodes)
	waitFor(t, "convergence", 15*time.Second, func() bool { return converged(nodes, 3) })
	primaryID := nodes[0].View().Me.ID
	if err := nodes[2].Replicate(primaryID); err != nil {
		t.Fatal(err)
	}
	nodes[2].repl.mu.Lock()
	host, port := nodes[2].repl.host, nodes[2].repl.port
	nodes[2].repl.mu.Unlock()
	if host != "127.0.0.1" || port != nodes[0].port {
		t.Fatalf("replication started to %s:%d", host, port)
	}
	replicaID := nodes[2].View().Me.ID
	waitFor(t, "everyone sees the replica", 10*time.Second, func() bool {
		for _, x := range nodes {
			r := x.View().ByID(replicaID)
			if r == nil || r.Primary == nil || r.Primary.ID != primaryID {
				return false
			}
		}
		return true
	})
	if err := nodes[2].Replicate(replicaID); err == nil {
		t.Fatal("replicating myself should fail")
	}
}

func TestForgetStaysForgotten(t *testing.T) {
	nodes := startNodes(t, 3)
	meetChain(t, nodes)
	waitFor(t, "convergence", 15*time.Second, func() bool { return converged(nodes, 3) })
	gone := nodes[2].View().Me.ID
	if err := nodes[0].Forget(gone); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second) // plenty of gossip from node 1, which still knows it
	if nodes[0].View().ByID(gone) != nil {
		t.Fatal("forgotten node came back through gossip")
	}
}

// A restarted node keeps its identity and its peers from nodes.conf.
func TestStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	nodes := []*testNode{startNode(t, Options{StateFile: filepath.Join(dir, "a.conf")}), startNode(t, Options{}), startNode(t, Options{})}
	meetChain(t, nodes)
	waitFor(t, "convergence", 15*time.Second, func() bool { return converged(nodes, 3) })
	nodes[0].AddSlots([]uint16{1, 2, 3})
	waitFor(t, "slots spread", 10*time.Second, func() bool { return nodes[2].View().Owner(2) != nil })
	id := nodes[0].View().Me.ID
	nodes[0].Close()

	again := startNodeAt(t, nodes[0].port, Options{StateFile: filepath.Join(dir, "a.conf")})
	v := again.View()
	if v.Me.ID != id || len(v.Nodes) != 3 || v.Owner(2) == nil || v.Owner(2).ID != id {
		t.Fatalf("after restart: id %s (want %s), %d nodes, owner(2)=%v", v.Me.ID, id, len(v.Nodes), v.Owner(2))
	}
	waitFor(t, "links back up", 10*time.Second, func() bool {
		for _, n := range again.View().Nodes {
			if !n.LinkUp {
				return false
			}
		}
		return true
	})
}

func startNodeAt(t testing.TB, port int, opts Options) *testNode {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	opts.ClientAddr, opts.InternalAddr, opts.NodeTimeout, opts.Log = addr, addr, 2*time.Second, quiet
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return &testNode{Cluster: c, port: port, addr: addr}
}

// A static seed file still works: everyone starts knowing everyone.
func TestStaticSeedLinksUp(t *testing.T) {
	ports := []int{freePort(t), freePort(t), freePort(t)}
	text := fmt.Sprintf("node a 127.0.0.1:%[1]d 127.0.0.1:%[1]d primary 0-8191\nnode b 127.0.0.1:%[2]d 127.0.0.1:%[2]d primary 8192-16383\nnode c 127.0.0.1:%[3]d 127.0.0.1:%[3]d replica a\n", ports[0], ports[1], ports[2])
	var nodes []*testNode
	for i, name := range []string{"a", "b", "c"} {
		seed, err := Parse(strings.NewReader(text), name)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, startNodeAt(t, ports[i], Options{Seed: seed}))
	}
	defer func() {
		if t.Failed() {
			for _, x := range nodes {
				v := x.View()
				for _, n := range v.Nodes {
					t.Logf("view of %s: %s link=%v pong=%v ping=%v flags=%b slots=%v", v.Me.Name, n.Label(), n.LinkUp, n.PongRecv.Format("15:04:05.000"), n.PingSent.Format("15:04:05.000"), n.Flags, n.Slots)
				}
				t.Logf("view of %s: stateOK=%v", v.Me.Name, v.StateOK())
			}
		}
	}()
	waitFor(t, "all links up, state ok", 10*time.Second, func() bool {
		for _, x := range nodes {
			v := x.View()
			if !v.StateOK() {
				return false
			}
			for _, n := range v.Nodes {
				if !n.LinkUp || n.PongRecv.IsZero() && n != v.Me {
					return false
				}
			}
		}
		return true
	})
	if c := nodes[2].View().Me; c.Primary == nil || c.Primary.Name != "a" {
		t.Fatal("seeded replica lost its primary")
	}
}

// Fault injection on the bus: dropping every message to a node means we
// stop hearing back from it (its PONGs never get triggered).
func TestBusDropFault(t *testing.T) {
	reg := faults.New()
	a := startNode(t, Options{Faults: reg})
	b := startNode(t, Options{})
	a.Meet("127.0.0.1", b.port, 0)
	waitFor(t, "a and b met", 10*time.Second, func() bool { return converged([]*testNode{a, b}, 2) })
	bID := b.View().Me.ID
	reg.Set(faults.Rule{Point: faults.BusMessage, Kind: faults.Drop, Target: bID})
	time.Sleep(200 * time.Millisecond)
	before := a.View().ByID(bID).PongRecv
	time.Sleep(2 * time.Second) // a keeps pinging b, but the pings are dropped
	if after := a.View().ByID(bID).PongRecv; after.After(before) {
		t.Fatalf("a still hears from b (pong %v → %v) with messages dropped", before, after)
	}
	if a.Stats().Dropped.Load() == 0 {
		t.Fatal("no messages counted as dropped")
	}
	reg.Clear("")
	waitFor(t, "a hears from b again", 10*time.Second, func() bool { return a.View().ByID(bID).PongRecv.After(before) })
}
