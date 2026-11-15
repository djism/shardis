package admin

import (
	"strings"
	"testing"

	"github.com/djism/shardis/internal/testcluster"
)

// Create forms a cluster from six fresh nodes, and running it again is a
// no-op (the Kubernetes bootstrap Job may run more than once).
func TestCreate(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{}) // a seeded cluster we don't use; AddEmpty needs it
	var addrs []string
	for _, n := range []string{"p1", "p2", "p3", "r1", "r2", "r3"} {
		addrs = append(addrs, tc.AddEmpty(t, n).Addr)
	}
	var log strings.Builder
	out := func(f string, a ...any) { log.WriteString(strings.TrimSpace(strings.ReplaceAll(f, "%s", "x")) + "\n") }
	if err := Create(addrs, 1, out); err != nil {
		t.Fatalf("create: %v", err)
	}
	v, err := load(addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(v.primaries) != 3 {
		t.Fatalf("%d primaries, want 3", len(v.primaries))
	}
	total := 0
	for _, p := range v.primaries {
		total += len(p.slots)
		replicas := 0
		for _, n := range v.nodes {
			if n.primaryID == p.id {
				replicas++
			}
		}
		if replicas != 1 {
			t.Fatalf("%s has %d replicas, want 1", p.addr, replicas)
		}
	}
	if total != 16384 {
		t.Fatalf("%d slots assigned, want 16384", total)
	}
	log.Reset()
	if err := Create(addrs, 1, func(f string, a ...any) { log.WriteString(f) }); err != nil || !strings.Contains(log.String(), "already formed") {
		t.Fatalf("second create: err %v, log %q", err, log.String())
	}
	if _, err := AddNode("127.0.0.1:1", "10.0.0.5:7000", ""); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("add-node with a loopback -addr for a remote node: %v", err)
	}
}
