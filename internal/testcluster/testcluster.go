// Package testcluster runs a real Shardis cluster inside a test process:
// 3 primaries + 3 replicas on free ports, wired exactly like separate
// processes (TCP, cluster bus, replication). Tests outside internal/server
// (the client, chaos experiments) use it to get a cluster in a second.
package testcluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djism/shardis/internal/cluster"
	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/faults"
	"github.com/djism/shardis/internal/keyslot"
	"github.com/djism/shardis/internal/replication"
	"github.com/djism/shardis/internal/resp"
	"github.com/djism/shardis/internal/server"
	"github.com/djism/shardis/internal/store"
)

// Options configures Start.
type Options struct {
	NodeTimeout time.Duration // default 2s (fast failover for tests)
}

// Cluster is a running test cluster.
type Cluster struct {
	Nodes map[string]*Node // a b c (primaries), d e f (replicas of a b c), plus any AddEmpty'd
	Topo  *cluster.Topology
	opts  Options
	quiet *slog.Logger
}

// Node is one member.
type Node struct {
	Name, Addr string
	Port       int
	Faults     *faults.Registry
	Cluster    *cluster.Cluster
	Server     *server.Server
	Engine     store.Engine
	Repl       *replication.Node
	Stop       func() // like the process dying (idempotent)
}

// ID is the node's cluster ID.
func (n *Node) ID() string { return n.Cluster.View().Me.ID }

// Start runs the cluster and waits until every replica is in sync.
func Start(t testing.TB, o Options) *Cluster {
	t.Helper()
	if o.NodeTimeout == 0 {
		o.NodeTimeout = 2 * time.Second
	}
	names := []string{"a", "b", "c", "d", "e", "f"}
	ports, used := map[string]int{}, map[int]bool{}
	for _, n := range names {
		for ports[n] == 0 || used[ports[n]] {
			ports[n] = FreePort(t)
		}
		used[ports[n]] = true
	}
	var text strings.Builder
	slots := []string{"0-5460", "5461-10922", "10923-16383"}
	for i, n := range names {
		addr := fmt.Sprintf("127.0.0.1:%d", ports[n])
		if i < 3 {
			fmt.Fprintf(&text, "node %s %s %s primary %s\n", n, addr, addr, slots[i])
		} else {
			fmt.Fprintf(&text, "node %s %s %s replica %s\n", n, addr, addr, names[i-3])
		}
	}
	tc := &Cluster{Nodes: map[string]*Node{}, opts: o, quiet: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, n := range names {
		seed, err := cluster.Parse(strings.NewReader(text.String()), n)
		if err != nil {
			t.Fatal(err)
		}
		if n == "a" {
			tc.Topo = seed
		}
		tc.Nodes[n] = tc.startNode(t, n, ports[n], seed)
	}
	for _, n := range []string{"d", "e", "f"} {
		addr := tc.Nodes[n].Addr
		Eventually(t, 10*time.Second, "replica "+n+" in sync", func() bool {
			return strings.Contains(Do(t, addr, "INFO", "replication").Text(), "master_link_status:up")
		})
	}
	return tc
}

func (tc *Cluster) startNode(t testing.TB, name string, port int, seed *cluster.Topology) *Node {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	bus := events.NewBus(addr) // inactive (free) until someone subscribes with EVENTS
	engine, _ := store.New(store.EngineSharded, store.Options{Bus: bus})
	reg := faults.New()
	repl := replication.NewNode(replication.Options{Engine: engine, PingPeriod: 100 * time.Millisecond,
		Timeout: 2 * time.Second, AnnouncePort: port, AnnounceHost: "127.0.0.1", Faults: reg, Bus: bus, Log: tc.quiet})
	cl, err := cluster.New(cluster.Options{ClientAddr: addr, InternalAddr: addr, Seed: seed,
		NodeTimeout: tc.opts.NodeTimeout, Replicator: repl, Faults: reg, Bus: bus, Log: tc.quiet})
	if err != nil {
		t.Fatal(err)
	}
	s := server.New(server.Options{Engine: engine, Repl: repl, Cluster: cl, Port: port, Hz: 10, Log: tc.quiet, Faults: reg, Bus: bus})
	if err := s.Listen(addr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Serve(ctx); close(done) }()
	var once sync.Once
	stop := func() { once.Do(func() { cl.Close(); repl.Close(); cancel(); <-done; engine.Close() }) }
	t.Cleanup(stop)
	if err := cl.Start(); err != nil {
		t.Fatal(err)
	}
	return &Node{Name: name, Addr: addr, Port: port, Faults: reg, Cluster: cl, Server: s, Engine: engine, Repl: repl, Stop: stop}
}

// AddEmpty starts a fresh node that knows nobody (like `--cluster-enabled
// yes`). Join it with admin.AddNode.
func (tc *Cluster) AddEmpty(t testing.TB, name string) *Node {
	t.Helper()
	n := tc.startNode(t, name, FreePort(t), nil)
	tc.Nodes[name] = n
	return n
}

// KeyIn returns a key whose slot primary `name` owned at the start.
func (tc *Cluster) KeyIn(name string) string {
	for i := 0; ; i++ {
		k := fmt.Sprint("key-", i)
		if tc.Topo.Owner(keyslot.Of([]byte(k))).Name == name {
			return k
		}
	}
}

// FreePort returns a port p where p and p+10000 (its cluster bus) are free.
func FreePort(t testing.TB) int {
	t.Helper()
	for range 200 {
		// 40000-55000, so it can't collide with internal/server's tests
		// (20000-40000), which run in parallel with ours.
		p := 40000 + rand.IntN(15000)
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
	t.Fatal("no free port")
	return 0
}

// Reply wraps a RESP value for tests.
type Reply struct{ resp.Value }

// Text is the reply's text (bulk/simple string or error).
func (r Reply) Text() string { return string(r.Str) }

// Do sends one command to addr on a fresh connection.
func Do(t testing.TB, addr string, args ...string) Reply {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return Reply{resp.Value{Kind: resp.Error, Str: []byte(err.Error())}}
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	w, r := resp.NewWriter(c), resp.NewReader(c)
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	w.WriteCommand(b...)
	if err := w.Flush(); err != nil {
		return Reply{resp.Value{Kind: resp.Error, Str: []byte(err.Error())}}
	}
	v, err := r.ReadValue()
	if err != nil {
		return Reply{resp.Value{Kind: resp.Error, Str: []byte(err.Error())}}
	}
	return Reply{v}
}

// Eventually polls cond every 20 ms until it's true or d passes.
func Eventually(t testing.TB, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for: %s", d, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
