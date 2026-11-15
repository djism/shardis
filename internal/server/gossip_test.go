package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/djism/shardis/internal/cluster"
	"github.com/djism/shardis/internal/replication"
	"github.com/djism/shardis/internal/store"
)

// startFreshNode is a cluster-enabled node that knows only itself, like
// `shardis --cluster-enabled yes`: the cluster is formed with commands.
func startFreshNode(t *testing.T) *clusterNode {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	engine, _ := store.New(store.EngineSharded, store.Options{})
	node := replication.NewNode(replication.Options{Engine: engine, PingPeriod: 100 * time.Millisecond,
		Timeout: 2 * time.Second, AnnouncePort: port, Log: quiet})
	cl, err := cluster.New(cluster.Options{ClientAddr: addr, InternalAddr: addr, NodeTimeout: 2 * time.Second,
		StateFile: filepath.Join(t.TempDir(), "nodes.conf"), Replicator: node, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	s := New(Options{Engine: engine, Repl: node, Cluster: cl, Port: port, Hz: 10, Log: quiet})
	if err := s.Listen(addr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Serve(ctx); close(done) }()
	t.Cleanup(func() { cl.Close(); node.Close(); cancel(); <-done; engine.Close() })
	if err := cl.Start(); err != nil {
		t.Fatal(err)
	}
	return &clusterNode{addr: addr, port: port}
}

func clusterInfoField(t *testing.T, c *testClient, field string) string {
	t.Helper()
	raw := c.do("CLUSTER", "INFO")
	s := strings.Trim(raw, `"`)
	for _, line := range strings.Split(s, `\r\n`) {
		if k, v, ok := strings.Cut(line, ":"); ok && k == field {
			return v
		}
	}
	return ""
}

// The Redis way to build a cluster, with nothing but commands: MEET in a
// chain, ADDSLOTSRANGE on three primaries, REPLICATE on three replicas.
// Then a real client uses it.
func TestFormClusterWithCommands(t *testing.T) {
	nodes := make([]*clusterNode, 6)
	clients := make([]*testClient, 6)
	for i := range nodes {
		nodes[i] = startFreshNode(t)
		clients[i] = newClient(t, nodes[i].addr)
	}
	if got := clients[0].do("SET", "x", "1"); got != "CLUSTERDOWN Hash slot not served" {
		t.Fatalf("before slots are assigned: %q", got)
	}
	for i := 0; i < 5; i++ {
		if got := clients[i].do("CLUSTER", "MEET", "127.0.0.1", fmt.Sprint(nodes[i+1].port)); got != "OK" {
			t.Fatal(got)
		}
	}
	eventually(t, "all 6 know all 6", func() bool {
		for _, c := range clients {
			if clusterInfoField(t, c, "cluster_known_nodes") != "6" {
				return false
			}
		}
		return true
	})
	for i, r := range [][2]string{{"0", "5460"}, {"5461", "10922"}, {"10923", "16383"}} {
		if got := clients[i].do("CLUSTER", "ADDSLOTSRANGE", r[0], r[1]); got != "OK" {
			t.Fatal(got)
		}
	}
	// ADDSLOTS only checks this node's own view (as in Redis). Once gossip has
	// told node 3 that slot 0 is taken, it refuses. (Before that, it would
	// accept, and the two claims would be settled by config epoch.)
	eventually(t, "node 3 learns about slot 0's owner", func() bool {
		return strings.Contains(clients[3].do("CLUSTER", "SLOTS"), "(int) 0 (int) 5460")
	})
	if got := clients[3].do("CLUSTER", "ADDSLOTS", "0"); !strings.Contains(got, "already busy") {
		t.Fatalf("taking a busy slot: %q", got)
	}
	for i := range 3 {
		id := strings.Trim(clients[i].do("CLUSTER", "MYID"), `"`)
		if got := clients[3+i].do("CLUSTER", "REPLICATE", id); got != "OK" {
			t.Fatal(got)
		}
	}
	eventually(t, "cluster_state:ok everywhere", func() bool {
		for _, c := range clients {
			if clusterInfoField(t, c, "cluster_state") != "ok" || clusterInfoField(t, c, "cluster_size") != "3" {
				return false
			}
		}
		return true
	})

	ctx := context.Background()
	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{nodes[4].addr}})
	defer rdb.Close()
	for i := range 300 {
		if err := rdb.Set(ctx, fmt.Sprint("k", i), i, 0).Err(); err != nil {
			t.Fatalf("SET k%d: %v", i, err)
		}
	}
	if v, err := rdb.Get(ctx, "k123").Int(); err != nil || v != 123 {
		t.Fatalf("GET k123 = %d, %v", v, err)
	}
	// Replicas got their data through replication started by CLUSTER REPLICATE.
	eventually(t, "replicas have the keys", func() bool {
		total := 0
		for _, c := range clients[3:] {
			var n int
			fmt.Sscanf(c.do("DBSIZE"), "(int) %d", &n)
			total += n
		}
		return total == 300
	})
}

// The real redis-cli --cluster create can build a Shardis cluster: it uses
// ADDSLOTS, SET-CONFIG-EPOCH, MEET and REPLICATE, then checks that every
// node agrees about the slots.
func TestRedisCLIClusterCreate(t *testing.T) {
	cli, err := exec.LookPath("redis-cli")
	if err != nil {
		t.Skip("redis-cli not installed")
	}
	args := []string{"--cluster", "create"}
	for range 6 {
		args = append(args, startFreshNode(t).addr)
	}
	args = append(args, "--cluster-replicas", "1", "--cluster-yes")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, cli, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("redis-cli --cluster create: %v\n%s", err, out)
	}
	for _, want := range []string{"[OK] All nodes agree about slots configuration.", "[OK] All 16384 slots covered."} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("output lacks %q:\n%s", want, out)
		}
	}
}
