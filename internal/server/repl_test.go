package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/djism/shardis/internal/faults"
	"github.com/djism/shardis/internal/replication"
	"github.com/djism/shardis/internal/store"
)

// replNode is one in-process server with replication and fault injection on.
type replNode struct {
	addr, id string
	port     int
	node     *replication.Node
	faults   *faults.Registry
	stop     func()
}

func startReplNode(t *testing.T, backlog int64) *replNode {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine, err := store.New(store.EngineSharded, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var ln net.Listener
	port := 0
	for range 100 { // a port our config would accept (≤ 55535), like the real server
		port = 20000 + rand.IntN(20000)
		if ln, err = net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port)); err == nil {
			ln.Close()
			break
		}
	}
	reg := faults.New()
	node := replication.NewNode(replication.Options{
		Engine: engine, BacklogSize: backlog, PingPeriod: 100 * time.Millisecond, Timeout: 2 * time.Second,
		AnnouncePort: port, Faults: reg, Log: quiet,
	})
	s := New(Options{Engine: engine, Repl: node, Faults: reg, Port: port, Hz: 100, Log: quiet})
	if err := s.Listen("127.0.0.1:" + strconv.Itoa(port)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	expCtx, stopExp := context.WithCancel(context.Background())
	served, expired := make(chan struct{}), make(chan struct{})
	go func() { s.Serve(ctx); close(served) }()
	go func() { store.NewActiveExpirer(engine, 100).Run(expCtx); close(expired) }()
	stopped := false
	n := &replNode{addr: s.Addr().String(), id: "127.0.0.1:" + strconv.Itoa(port), port: port, node: node, faults: reg}
	n.stop = func() { // like the process dying: links drop, data is gone
		if stopped {
			return
		}
		stopped = true
		// Close the links *before* releasing any paused fault: a crashing
		// process doesn't get to flush what it was holding back. (Clearing
		// the pause first let a paused sender deliver its data in the gap.)
		node.Close()
		reg.Clear("")
		cancel()
		<-served
		stopExp()
		<-expired
		engine.Close()
	}
	t.Cleanup(n.stop)
	return n
}

func (n *replNode) client(t *testing.T) *testClient { return newClient(t, n.addr) }

// replicate makes r a replica of p and waits until it's in sync.
func replicate(t *testing.T, r, p *replNode) {
	t.Helper()
	if got := r.client(t).do("REPLICAOF", "127.0.0.1", strconv.Itoa(p.port)); got != "OK" {
		t.Fatal(got)
	}
	eventually(t, "replica link up", func() bool {
		return parseInfo(t, r.client(t).do("INFO", "replication"))["master_link_status"] == "up"
	})
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// dump returns key → value|absolute-deadline, which must match exactly
// between a primary and an in-sync replica.
func dump(t *testing.T, c *testClient) map[string]string {
	t.Helper()
	out := map[string]string{}
	keys := c.do("KEYS", "*")
	for _, k := range strings.Fields(strings.Trim(keys, "[]")) {
		k, _ = strconv.Unquote(k)
		out[k] = c.do("GET", k) + "|" + c.do("PEXPIRETIME", k)
	}
	return out
}

func inSync(t *testing.T, p, r *replNode) {
	t.Helper()
	pc, rc := p.client(t), r.client(t)
	eventually(t, "replica matches primary", func() bool { return maps.Equal(dump(t, pc), dump(t, rc)) })
}

func TestFullSyncThenStream(t *testing.T) {
	p, r := startReplNode(t, 1<<20), startReplNode(t, 1<<20)
	pc := p.client(t)
	for i := range 500 { // data that exists before the replica: arrives via snapshot
		pc.do("SET", fmt.Sprint("pre:", i), fmt.Sprint(i))
	}
	pc.do("SET", "ttl", "x", "EX", "1000")
	replicate(t, r, p)
	inSync(t, p, r)

	// Every kind of effect, after the sync: arrives via the stream.
	pc.do("INCRBY", "counter", "41")
	pc.do("INCR", "counter")
	pc.do("MSET", "a", "1", "b", "2")
	pc.do("DEL", "pre:1", "pre:2")
	pc.do("EXPIRE", "a", "500")
	pc.do("PERSIST", "ttl")
	pc.do("SET", "pre:3", "changed", "KEEPTTL")
	inSync(t, p, r)
	pc.do("FLUSHALL")
	pc.do("SET", "after-flush", "1")
	inSync(t, p, r)

	info := parseInfo(t, pc.do("INFO", "replication"))
	if info["connected_slaves"] != "1" || info["sync_full"] != "1" {
		t.Fatalf("primary info: %v", info)
	}
}

func TestReplicaIsReadOnly(t *testing.T) {
	p, r := startReplNode(t, 1<<20), startReplNode(t, 1<<20)
	replicate(t, r, p)
	rc := r.client(t)
	if got := rc.do("SET", "x", "y"); got != "READONLY You can't write against a read only replica." {
		t.Fatal(got)
	}
	if got := rc.do("WAIT", "1", "10"); !strings.HasPrefix(got, "ERR WAIT cannot be used with replica") {
		t.Fatal(got)
	}
	if got := rc.do("ROLE"); !strings.HasPrefix(got, `["slave" "127.0.0.1" (int) `+strconv.Itoa(p.port)+` "connected"`) {
		t.Fatalf("ROLE = %s", got)
	}
}

// Replicas hide expired keys but only the primary deletes them (ADR 019).
func TestExpiryOnReplicaFollowsPrimary(t *testing.T) {
	p, r := startReplNode(t, 1<<20), startReplNode(t, 1<<20)
	replicate(t, r, p)
	p.client(t).do("SET", "short", "v", "PX", "100")
	rc := r.client(t)
	eventually(t, "key on replica", func() bool { return rc.do("EXISTS", "short") == "(int) 1" })
	time.Sleep(150 * time.Millisecond)
	if got := rc.do("GET", "short"); got != "(nil)" {
		t.Fatalf("replica returned an expired key: %s", got)
	}
	// The primary's active expiry deletes it and replicates the DEL.
	eventually(t, "DEL reaches the replica", func() bool { return rc.do("DBSIZE") == "(int) 0" })
}

// A short disconnect resumes from the backlog: no second full sync.
func TestPartialResyncAfterDisconnect(t *testing.T) {
	p, r := startReplNode(t, 1<<20), startReplNode(t, 1<<20)
	replicate(t, r, p)
	pc := p.client(t)
	pc.do("SET", "a", "1")
	inSync(t, p, r)
	p.faults.Set(faults.Rule{Point: faults.ReplStream, Kind: faults.Sever})
	for i := range 100 {
		pc.do("SET", fmt.Sprint("during:", i), "x") // written while the link is down
	}
	time.Sleep(200 * time.Millisecond)
	p.faults.Clear("")
	inSync(t, p, r)
	info := parseInfo(t, pc.do("INFO", "replication"))
	if info["sync_full"] != "1" || infoInt(t, info, "sync_partial_ok") < 1 {
		t.Fatalf("want 1 full sync and partial resyncs after, got %v", info)
	}
}

// If the replica falls further behind than the backlog holds, it can't
// partially resync: it gets a fresh full sync, and still ends up identical.
func TestFullResyncWhenBacklogOverflows(t *testing.T) {
	p, r := startReplNode(t, 4096), startReplNode(t, 4096)
	replicate(t, r, p)
	pc := p.client(t)
	p.faults.Set(faults.Rule{Point: faults.ReplStream, Kind: faults.Pause})
	for i := range 300 { // ~10 KB of stream > 4 KB backlog
		pc.do("SET", fmt.Sprint("k", i), strings.Repeat("v", 20))
	}
	p.faults.Clear("")
	inSync(t, p, r)
	eventually(t, "second full sync", func() bool {
		return parseInfo(t, pc.do("INFO", "replication"))["sync_full"] == "2"
	})
}

// After a failover, the other replicas continue from the new primary's
// backlog instead of copying everything again (PSYNC2).
func TestPromotedReplicaServesPartialResync(t *testing.T) {
	p, r1, r2 := startReplNode(t, 1<<20), startReplNode(t, 1<<20), startReplNode(t, 1<<20)
	replicate(t, r1, p)
	replicate(t, r2, p)
	pc := p.client(t)
	for i := range 50 {
		pc.do("SET", fmt.Sprint("k", i), "v")
	}
	inSync(t, p, r1)
	inSync(t, p, r2)
	p.stop() // primary dies

	r1.client(t).do("REPLICAOF", "NO", "ONE")
	replicate(t, r2, r1)
	r1.client(t).do("SET", "after-failover", "1")
	inSync(t, r1, r2)
	info := parseInfo(t, r1.client(t).do("INFO", "replication"))
	if info["sync_full"] != "0" || info["sync_partial_ok"] != "1" {
		t.Fatalf("new primary should have served a partial resync: %v", info)
	}
	if info["master_replid2"] == strings.Repeat("0", 40) {
		t.Fatal("promoted replica didn't keep its old replication ID")
	}
}

// ---- The lost-write demonstrations (Iteration 6's main point) ----

// Demo A: async replication can lose an acknowledged write. The primary says
// OK, dies before the replica has the write, the replica is promoted: gone.
func TestDemoA_AcknowledgedWriteLostOnFailover(t *testing.T) {
	p, r := startReplNode(t, 1<<20), startReplNode(t, 1<<20)
	replicate(t, r, p)
	pc := p.client(t)
	pc.do("SET", "k1", "safe")
	if got := pc.do("WAIT", "1", "1000"); got != "(int) 1" {
		t.Fatalf("k1 not replicated: WAIT = %s", got)
	}
	p.faults.Set(faults.Rule{Point: faults.ReplStream, Kind: faults.Pause}) // the replica stops receiving
	if got := pc.do("SET", "k2", "acknowledged"); got != "OK" {
		t.Fatal(got) // the client is told its write succeeded...
	}
	p.stop() // ...and the primary dies before replicating it

	rc := r.client(t)
	rc.do("REPLICAOF", "NO", "ONE") // failover
	if got := rc.do("GET", "k1"); got != `"safe"` {
		t.Fatalf("k1 = %s", got)
	}
	if got := rc.do("GET", "k2"); got != "(nil)" {
		t.Fatalf("expected the acknowledged write to be lost, got %s", got)
	}
}

// Demo B: WAIT tells the client the write isn't replicated, but can't undo
// it: the primary has applied it, and other clients can already read it.
func TestDemoB_WaitRevealsButDoesNotUndo(t *testing.T) {
	p, r := startReplNode(t, 1<<20), startReplNode(t, 1<<20)
	replicate(t, r, p)
	pc := p.client(t)
	p.faults.Set(faults.Rule{Point: faults.ReplStream, Kind: faults.Pause})
	pc.do("SET", "k3", "v")
	if got := pc.do("WAIT", "1", "200"); got != "(int) 0" {
		t.Fatalf("WAIT = %s, want 0: the replica can't have it", got)
	}
	if got := p.client(t).do("GET", "k3"); got != `"v"` {
		t.Fatalf("the write should still be visible on the primary, got %s", got)
	}
	if got := r.client(t).do("GET", "k3"); got != "(nil)" {
		t.Fatalf("replica has k3 = %s while paused", got)
	}
	p.faults.Clear("")
	if got := pc.do("WAIT", "1", "2000"); got != "(int) 1" {
		t.Fatalf("after resuming, WAIT = %s", got)
	}
}

// Demo C: WAIT shrinks the window but doesn't close it. With two replicas,
// WAIT 1 can be satisfied by replica A while replica B lags; if B is the one
// promoted, the "confirmed" write is lost anyway.
func TestDemoC_WaitCanStillLoseIfTheWrongReplicaIsPromoted(t *testing.T) {
	p, a, b := startReplNode(t, 1<<20), startReplNode(t, 1<<20), startReplNode(t, 1<<20)
	replicate(t, a, p)
	replicate(t, b, p)
	pc := p.client(t)
	p.faults.Set(faults.Rule{Point: faults.ReplStream, Kind: faults.Pause, Target: b.id}) // only B lags
	pc.do("SET", "k4", "confirmed")
	if got := pc.do("WAIT", "1", "1000"); got != "(int) 1" {
		t.Fatalf("WAIT 1 = %s, want 1 (replica A has it)", got)
	}
	p.stop()
	bc := b.client(t)
	bc.do("REPLICAOF", "NO", "ONE") // the lagging replica wins the failover
	if got := bc.do("GET", "k4"); got != "(nil)" {
		t.Fatalf("expected k4 lost on B, got %s", got)
	}
	if got := a.client(t).do("GET", "k4"); got != `"confirmed"` {
		t.Fatalf("A should have k4, got %s", got)
	}
}

func TestWaitWithNoReplicasReturnsZero(t *testing.T) {
	p := startReplNode(t, 1<<20)
	c := p.client(t)
	c.do("SET", "x", "y")
	start := time.Now()
	if got := c.do("WAIT", "1", "5000"); got != "(int) 0" || time.Since(start) > time.Second {
		t.Fatalf("WAIT with no replicas = %s after %v", got, time.Since(start))
	}
}

func TestFaultCommand(t *testing.T) {
	p := startReplNode(t, 1<<20)
	c := p.client(t)
	if got := c.do("FAULT", "SET", "repl.stream", "delay", "50", "1.2.3.4:5"); got != "OK" {
		t.Fatal(got)
	}
	if got := c.do("FAULT", "LIST"); got != `["repl.stream delay 50 1.2.3.4:5"]` {
		t.Fatal(got)
	}
	if got := c.do("FAULT", "SET", "nope", "pause"); !strings.HasPrefix(got, "ERR unknown fault point") {
		t.Fatal(got)
	}
	c.do("FAULT", "CLEAR")
	if got := c.do("FAULT", "LIST"); got != "[]" {
		t.Fatal(got)
	}
	plain, _ := startServer(t) // no fault registry: the command refuses
	if got := newClient(t, plain).do("FAULT", "LIST"); !strings.HasPrefix(got, "ERR fault injection is disabled") {
		t.Fatal(got)
	}
}
