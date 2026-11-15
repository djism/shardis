package metrics

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/djism/shardis/internal/testcluster"
)

// value returns a metric's value (summed over label sets matching want).
func value(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	found := false
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	next:
		for _, m := range mf.GetMetric() {
			for k, v := range want {
				ok := false
				for _, lp := range m.GetLabel() {
					if lp.GetName() == k && lp.GetValue() == v {
						ok = true
					}
				}
				if !ok {
					continue next
				}
			}
			found = true
			sum += metricValue(m)
		}
	}
	if !found {
		t.Fatalf("metric %s%v not found", name, want)
	}
	return sum
}

func metricValue(m *dto.Metric) float64 {
	switch {
	case m.Gauge != nil:
		return m.Gauge.GetValue()
	case m.Counter != nil:
		return m.Counter.GetValue()
	case m.Histogram != nil:
		return float64(m.Histogram.GetSampleCount())
	}
	return 0
}

func TestClusterMetrics(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{})
	reg := func(name string) *prometheus.Registry {
		n := tc.Nodes[name]
		return NewRegistry(Sources{Server: n.Server, Engine: n.Engine, Repl: n.Repl, Cluster: n.Cluster})
	}
	a, b := reg("a"), reg("b")
	k := tc.KeyIn("a")
	for range 5 {
		testcluster.Do(t, tc.Nodes["a"].Addr, "SET", k, "v")
	}
	testcluster.Do(t, tc.Nodes["a"].Addr, "GET", k)
	if got := value(t, a, "shardis_commands_total", map[string]string{"cmd": "set"}); got != 5 {
		t.Fatalf("commands_total{cmd=set} = %v, want 5", got)
	}
	if got := value(t, a, "shardis_command_duration_seconds", nil); got < 6 {
		t.Fatalf("latency histogram count = %v, want >= 6", got)
	}
	checks := []struct {
		reg  *prometheus.Registry
		name string
		want float64
	}{
		{a, "shardis_is_primary", 1}, {a, "shardis_cluster_slots_owned", 5461}, {a, "shardis_keys", 1},
		{a, "shardis_cluster_state_ok", 1}, {a, "shardis_cluster_known_nodes", 6}, {a, "shardis_cluster_minority", 0},
		{reg("d"), "shardis_is_primary", 0}, {reg("d"), "shardis_replication_link_up", 1},
	}
	for _, c := range checks {
		if got := value(t, c.reg, c.name, nil); got != c.want {
			t.Errorf("%s = %v, want %v", c.name, got, c.want)
		}
	}
	// A failover shows up: b counts a FAIL mark, d counts a won election.
	tc.Nodes["a"].Stop()
	d := reg("d")
	testcluster.Eventually(t, 15*time.Second, "failover visible in metrics", func() bool {
		return value(t, d, "shardis_cluster_failovers_won_total", nil) == 1 && value(t, d, "shardis_is_primary", nil) == 1
	})
	if got := value(t, b, "shardis_cluster_pfail_marks_total", nil); got < 1 {
		t.Errorf("b's pfail_marks_total = %v after a died", got)
	}
	if got := value(t, b, "shardis_cluster_bus_messages_sent_total", map[string]string{"type": "ping"}); got < 1 {
		t.Errorf("no pings counted: %v", got)
	}
}

// The HTTP side: /metrics in Prometheus text format, /healthz, /readyz.
func TestServe(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{})
	n := tc.Nodes["b"]
	reg := NewRegistry(Sources{Server: n.Server, Engine: n.Engine, Repl: n.Repl, Cluster: n.Cluster})
	port := testcluster.FreePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Serve(ctx, fmt.Sprint("127.0.0.1:", port), reg)
	get := func(path string) string {
		var body []byte
		testcluster.Eventually(t, 3*time.Second, "GET "+path, func() bool {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d%s", port, path))
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			body, _ = io.ReadAll(resp.Body)
			return resp.StatusCode == 200
		})
		return string(body)
	}
	m := get("/metrics")
	for _, want := range []string{"shardis_cluster_slots_owned 5462", "shardis_is_primary 1", "go_gc_duration_seconds", "process_resident_memory_bytes"} {
		if !strings.Contains(m, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	if get("/healthz") != "ok\n" || get("/readyz") != "ok\n" {
		t.Error("health endpoints")
	}
}
