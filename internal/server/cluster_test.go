package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/djism/shardis/internal/cluster"
	"github.com/djism/shardis/internal/faults"
	"github.com/djism/shardis/internal/keyslot"
	"github.com/djism/shardis/internal/replication"
	"github.com/djism/shardis/internal/store"
)

// testCluster is 3 primaries + 3 replicas running in-process on free ports,
// configured from a generated topology file, exactly like real nodes.
type testCluster struct {
	nodes map[string]*clusterNode // by name: a b c (primaries), d e f (replicas of a b c)
	topo  *cluster.Topology       // as seen by node a
}

type clusterNode struct {
	name, addr string
	port       int
	srv        *Server
	stop       func() // like the process dying
}

func startCluster(t *testing.T) *testCluster { return startClusterWith(t, false) }

// startClusterWith: readsWhenDown sets AllowReadsWhenDown on every node.
func startClusterWith(t *testing.T, readsWhenDown bool) *testCluster {
	t.Helper()
	names := []string{"a", "b", "c", "d", "e", "f"}
	ports, used := map[string]int{}, map[int]bool{}
	for _, n := range names {
		for ports[n] == 0 || used[ports[n]] {
			ports[n] = freePort(t)
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
	tc := &testCluster{nodes: map[string]*clusterNode{}}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, n := range names {
		topo, err := cluster.Parse(strings.NewReader(text.String()), n)
		if err != nil {
			t.Fatal(err)
		}
		if n == "a" {
			tc.topo = topo
		}
		engine, _ := store.New(store.EngineSharded, store.Options{})
		reg := faults.New()
		node := replication.NewNode(replication.Options{Engine: engine, PingPeriod: 100 * time.Millisecond,
			Timeout: 2 * time.Second, AnnouncePort: ports[n], AnnounceHost: "127.0.0.1", Faults: reg, Log: quiet})
		addr := fmt.Sprintf("127.0.0.1:%d", ports[n])
		cl, err := cluster.New(cluster.Options{ClientAddr: addr, InternalAddr: addr, Seed: topo,
			NodeTimeout: 2 * time.Second, Replicator: node, Faults: reg, Log: quiet})
		if err != nil {
			t.Fatal(err)
		}
		s := New(Options{Engine: engine, Repl: node, Cluster: cl, Port: ports[n], Hz: 10, Log: quiet,
			Faults: reg, AllowReadsWhenDown: readsWhenDown})
		if err := s.Listen(fmt.Sprintf("127.0.0.1:%d", ports[n])); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { s.Serve(ctx); close(done) }()
		var once sync.Once
		stop := func() { once.Do(func() { cl.Close(); node.Close(); cancel(); <-done; engine.Close() }) }
		t.Cleanup(stop)
		if err := cl.Start(); err != nil { // replicas start replicating from here
			t.Fatal(err)
		}
		tc.nodes[n] = &clusterNode{name: n, addr: fmt.Sprintf("127.0.0.1:%d", ports[n]), port: ports[n], srv: s, stop: stop}
	}
	for _, n := range []string{"d", "e", "f"} {
		c := newClient(t, tc.nodes[n].addr)
		eventually(t, "replica "+n+" up", func() bool {
			return parseInfo(t, c.do("INFO", "replication"))["master_link_status"] == "up"
		})
	}
	return tc
}

// freePort returns a port p where p and p+10000 (its cluster bus) are both free.
func freePort(t *testing.T) int {
	t.Helper()
	for range 200 {
		p := 20000 + rand.IntN(20000) // a real random source: clock-based ports collided
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

// keyIn returns a key that hashes to a slot owned by primary name.
func (tc *testCluster) keyIn(name string) string {
	for i := 0; ; i++ {
		k := fmt.Sprint("key-", i)
		if tc.topo.Owner(keyslot.Of([]byte(k))).Name == name {
			return k
		}
	}
}

func TestMovedAndCrossSlot(t *testing.T) {
	tc := startCluster(t)
	a := newClient(t, tc.nodes["a"].addr)
	kc := tc.keyIn("c")
	slot := keyslot.Of([]byte(kc))
	want := fmt.Sprintf("MOVED %d %s", slot, tc.nodes["c"].addr)
	if got := a.do("SET", kc, "v"); got != want {
		t.Fatalf("SET on wrong node = %q, want %q", got, want)
	}
	if got := a.do("GET", kc); got != want {
		t.Fatalf("GET on wrong node = %q", got)
	}
	ka := tc.keyIn("a")
	if got := a.do("SET", ka, "v"); got != "OK" {
		t.Fatalf("SET on owner = %q", got)
	}
	if got := a.do("MSET", ka, "1", kc, "2"); got != errCrossSlot {
		t.Fatalf("MSET across slots = %q", got)
	}
	// Hash tags put keys in one slot; they go to that slot's owner.
	tag := "{" + ka + "}"
	if got := a.do("MSET", tag+":x", "1", tag+":y", "2"); got != "OK" {
		t.Fatalf("MSET with hash tag = %q", got)
	}
	if got := a.do("DBSIZE"); got != "(int) 3" { // keyless commands are node-local
		t.Fatalf("DBSIZE on a = %s", got)
	}
}

func TestReplicaRedirectsUnlessReadOnly(t *testing.T) {
	tc := startCluster(t)
	ka := tc.keyIn("a")
	newClient(t, tc.nodes["a"].addr).do("SET", ka, "v")
	d := newClient(t, tc.nodes["d"].addr) // replica of a
	movedToA := fmt.Sprintf("MOVED %d %s", keyslot.Of([]byte(ka)), tc.nodes["a"].addr)
	if got := d.do("GET", ka); got != movedToA {
		t.Fatalf("GET on replica without READONLY = %q", got)
	}
	d.do("READONLY")
	eventually(t, "replica serves the read", func() bool { return d.do("GET", ka) == `"v"` })
	if got := d.do("SET", ka, "x"); got != movedToA {
		t.Fatalf("SET on replica after READONLY = %q (writes always go to the primary)", got)
	}
	if got := d.do("GET", tc.keyIn("b")); !strings.HasPrefix(got, "MOVED") { // not its primary's slot
		t.Fatalf("GET of another shard's key on replica = %q", got)
	}
	d.do("READWRITE")
	if got := d.do("GET", ka); got != movedToA {
		t.Fatalf("after READWRITE = %q", got)
	}
}

func TestClusterCommands(t *testing.T) {
	tc := startCluster(t)
	a := newClient(t, tc.nodes["a"].addr)
	if got := a.do("CLUSTER", "KEYSLOT", "foo"); got != "(int) 12182" {
		t.Fatalf("KEYSLOT = %s", got)
	}
	if got := a.do("CLUSTER", "MYID"); got != strconv.Quote(cluster.NodeID("a")) {
		t.Fatalf("MYID = %s", got)
	}
	slots := a.do("CLUSTER", "SLOTS")
	wantFirst := fmt.Sprintf(`[[(int) 0 (int) 5460 ["127.0.0.1" (int) %d %q] ["127.0.0.1" (int) %d %q]]`,
		tc.nodes["a"].port, cluster.NodeID("a"), tc.nodes["d"].port, cluster.NodeID("d"))
	if !strings.HasPrefix(slots, wantFirst) {
		t.Fatalf("SLOTS = %s\nwant prefix %s", slots, wantFirst)
	}
	nodes, _ := strconv.Unquote(a.do("CLUSTER", "NODES"))
	lines := strings.Split(strings.TrimSpace(nodes), "\n")
	mine := ""
	for _, l := range lines {
		if strings.Contains(l, "myself") {
			mine = l
		}
	}
	if len(lines) != 6 || !strings.Contains(mine, "myself,master") || !strings.HasSuffix(mine, "0-5460") {
		t.Fatalf("NODES:\n%s", nodes)
	}
	info, _ := strconv.Unquote(a.do("CLUSTER", "INFO"))
	if !strings.Contains(info, "cluster_state:ok") || !strings.Contains(info, "cluster_size:3") {
		t.Fatalf("INFO:\n%s", info)
	}
	ka := tc.keyIn("a")
	a.do("SET", ka, "v")
	slot := strconv.Itoa(int(keyslot.Of([]byte(ka))))
	if got := a.do("CLUSTER", "COUNTKEYSINSLOT", slot); got != "(int) 1" {
		t.Fatalf("COUNTKEYSINSLOT = %s", got)
	}
	if got := a.do("CLUSTER", "GETKEYSINSLOT", slot, "10"); got != fmt.Sprintf("[%q]", ka) {
		t.Fatalf("GETKEYSINSLOT = %s", got)
	}
	if got := a.do("CLUSTER", "SHARDS"); !strings.Contains(got, `"slots" [(int) 0 (int) 5460] "nodes"`) {
		t.Fatalf("SHARDS = %s", got)
	}
}

// The real thing: go-redis's ClusterClient discovers the cluster (CLUSTER
// SLOTS), learns key positions (COMMAND), and routes every command itself.
func TestGoRedisClusterClient(t *testing.T) {
	tc := startCluster(t)
	ctx := context.Background()
	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{tc.nodes["a"].addr}})
	defer rdb.Close()

	for i := range 1000 {
		if err := rdb.Set(ctx, fmt.Sprint("user:", i), i, 0).Err(); err != nil {
			t.Fatalf("SET user:%d: %v", i, err)
		}
	}
	for i := range 1000 {
		if v, err := rdb.Get(ctx, fmt.Sprint("user:", i)).Int(); err != nil || v != i {
			t.Fatalf("GET user:%d = %d, %v", i, v, err)
		}
	}
	// The keys really are spread over all three primaries.
	for _, n := range []string{"a", "b", "c"} {
		size := newClient(t, tc.nodes[n].addr).do("DBSIZE")
		if size == "(int) 0" {
			t.Errorf("primary %s got no keys", n)
		}
	}
	// Multi-key with a hash tag: one slot, one node.
	if err := rdb.MSet(ctx, "{cart:7}:items", "3", "{cart:7}:total", "42").Err(); err != nil {
		t.Fatal(err)
	}
	if vals, err := rdb.MGet(ctx, "{cart:7}:items", "{cart:7}:total").Result(); err != nil || vals[1] != "42" {
		t.Fatalf("MGET = %v, %v", vals, err)
	}
	// Across slots: the server refuses, and the client surfaces it.
	if err := rdb.MSet(ctx, "a", "1", "b", "2").Err(); err == nil || !strings.Contains(err.Error(), "CROSSSLOT") {
		t.Fatalf("cross-slot MSET error = %v", err)
	}
	// Pipelines get split per node by the client.
	pipe := rdb.Pipeline()
	for i := range 100 {
		pipe.Incr(ctx, fmt.Sprint("counter:", i))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if v, _ := rdb.Get(ctx, "counter:57").Int(); v != 1 {
		t.Fatalf("counter:57 = %d", v)
	}
	if err := rdb.Expire(ctx, "user:1", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if ttl, _ := rdb.TTL(ctx, "user:1").Result(); ttl < 59*time.Second {
		t.Fatalf("TTL = %v", ttl)
	}
}

// And the real redis-cli in cluster mode (-c) follows MOVED on its own.
func TestRedisCLIFollowsMoved(t *testing.T) {
	cli, err := exec.LookPath("redis-cli")
	if err != nil {
		t.Skip("redis-cli not installed")
	}
	tc := startCluster(t)
	kc := tc.keyIn("c")
	a := strconv.Itoa(tc.nodes["a"].port)
	run := func(args ...string) string {
		out, err := exec.Command(cli, append([]string{"-c", "-p", a}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("redis-cli %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	if got := run("SET", kc, "hello"); got != "OK" {
		t.Fatalf("SET via node a = %q", got)
	}
	if got := run("GET", kc); got != "hello" {
		t.Fatalf("GET via node a = %q", got)
	}
	if got := newClient(t, tc.nodes["c"].addr).do("GET", kc); got != `"hello"` {
		t.Fatalf("the key should live on node c, got %s", got)
	}
}

// End to end: a real primary dies; its replica wins the election and serves
// the slots; data written before the crash (and replicated) is there; a
// go-redis ClusterClient keeps working after refreshing its slot map.
func TestFailoverEndToEnd(t *testing.T) {
	tc := startCluster(t)
	ctx := context.Background()
	// go-redis refreshes its slot map only on MOVED, or every
	// ClusterStateReloadInterval (default 60 s). A connection error to a dead
	// primary does NOT trigger a refresh, so with defaults it can keep failing
	// for up to a minute after the cluster has already failed over. Our own
	// client (Iteration 12) will refresh on connection errors.
	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{tc.nodes["b"].addr}, MaxRedirects: 8,
		ClusterStateReloadInterval: time.Second})
	defer rdb.Close()
	ka := tc.keyIn("a")
	if err := rdb.Set(ctx, ka, "before", 0).Err(); err != nil {
		t.Fatal(err)
	}
	d := newClient(t, tc.nodes["d"].addr)
	d.do("READONLY")
	eventually(t, "replicated to d", func() bool { return d.do("GET", ka) == `"before"` })

	start := time.Now()
	tc.nodes["a"].stop() // the primary dies
	moved := fmt.Sprintf("MOVED %d %s", keyslot.Of([]byte(ka)), tc.nodes["a"].addr)
	deadline := time.Now().Add(20 * time.Second)
	for { // probe the replica until it takes writes for a's slots
		got := d.do("SET", ka, "after")
		if got == "OK" {
			break
		}
		if got != moved && !strings.HasPrefix(got, "CLUSTERDOWN") {
			t.Fatalf("unexpected reply during failover: %q", got)
		}
		if time.Now().After(deadline) {
			t.Fatalf("d never took over (last reply %q)", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("writes to the dead primary's slots accepted again after %v (node timeout 2s)", time.Since(start))
	if role := d.do("ROLE"); !strings.HasPrefix(role, `["master"`) {
		t.Fatalf("d's role after failover: %s", role)
	}
	b := newClient(t, tc.nodes["b"].addr)
	eventually(t, "cluster ok on b with d owning a's slots", func() bool {
		return clusterInfoField(t, b, "cluster_state") == "ok" &&
			strings.Contains(b.do("CLUSTER", "SLOTS"), fmt.Sprintf(`(int) %d`, tc.nodes["d"].port))
	})
	// The client refreshes its map (MOVED/connection errors) and carries on.
	var lastErr error
	var lastVal string
	defer func() {
		if t.Failed() {
			t.Logf("last go-redis result: %q, %v", lastVal, lastErr)
		}
	}()
	eventually(t, "go-redis reads through the new primary", func() bool {
		lastVal, lastErr = rdb.Get(ctx, ka).Result()
		return lastErr == nil && lastVal == "after"
	})
}
