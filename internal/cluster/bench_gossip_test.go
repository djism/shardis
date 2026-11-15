package cluster

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestGossipScaling measures convergence time and steady-state bus overhead
// for growing clusters. Slow, so it only runs when asked:
//
//	SHARDIS_GOSSIP_BENCH=docs/benchmarks/iter-08-gossip/raw.csv go test ./internal/cluster -run TestGossipScaling -v -timeout 20m
func TestGossipScaling(t *testing.T) {
	out := os.Getenv("SHARDIS_GOSSIP_BENCH")
	if out == "" {
		t.Skip("set SHARDIS_GOSSIP_BENCH=<csv path> to run")
	}
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fmt.Fprintf(f, "# %s, node timeout 15s (Redis default), nodes MEET'd in a chain\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintln(f, "nodes,run,converge_ms,msgs_per_node_per_s,kbytes_per_node_per_s,avg_msg_bytes")
	sizes := []int{6, 20, 50}
	if s := os.Getenv("SHARDIS_GOSSIP_SIZES"); s != "" {
		sizes = nil
		for _, x := range splitComma(s) {
			n, _ := strconv.Atoi(x)
			sizes = append(sizes, n)
		}
	}
	for _, n := range sizes {
		for run := 1; run <= 3; run++ {
			nodes := make([]*testNode, n)
			for i := range nodes {
				nodes[i] = startNode(t, Options{NodeTimeout: 15 * time.Second})
			}
			meetChain(t, nodes)
			took := waitFor(t, fmt.Sprintf("%d nodes converge", n), 120*time.Second, func() bool { return converged(nodes, n) })
			var msgs, bytes float64
			window := 0 * time.Second
			if run == 1 { // steady-state overhead: measure once per size
				window = 15 * time.Second
				before := make([][2]int64, n)
				for i, x := range nodes {
					before[i] = [2]int64{x.Stats().Sent.Load(), x.Stats().BytesSent.Load()}
				}
				time.Sleep(window)
				for i, x := range nodes {
					msgs += float64(x.Stats().Sent.Load() - before[i][0])
					bytes += float64(x.Stats().BytesSent.Load() - before[i][1])
				}
				msgs /= float64(n) * window.Seconds()
				bytes /= float64(n) * window.Seconds()
			}
			avg := 0.0
			if msgs > 0 {
				avg = bytes / msgs
			}
			fmt.Fprintf(f, "%d,%d,%d,%.1f,%.1f,%.0f\n", n, run, took.Milliseconds(), msgs, bytes/1024, avg)
			t.Logf("%d nodes run %d: converged in %v; %.1f msgs/s and %.1f KB/s sent per node", n, run, took, msgs, bytes/1024)
			for _, x := range nodes {
				x.Close()
			}
		}
	}
}

func splitComma(s string) []string {
	var out []string
	cur := ""
	for _, c := range s + "," {
		if c == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
		} else {
			cur += string(c)
		}
	}
	return out
}
