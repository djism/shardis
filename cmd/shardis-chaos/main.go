// Command shardis-chaos runs named failure scenarios against a real Shardis
// cluster (7 processes) under a recorded workload, checks the history for
// lost writes and stale reads, and saves each run as a trace the website
// replays (Iteration 13, ADR 040-042).
//
//	shardis-chaos                                   # every scenario
//	shardis-chaos -scenarios kill-primary,slow-node
//	shardis-chaos -list
//	shardis-chaos -external shardis-0.shardis:7000 -duration 90s   # checker only (Kubernetes demos)
//
// We don't claim linearizability: Redis Cluster doesn't promise it. We
// measure what does hold, and how often what doesn't fails.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/djism/shardis/pkg/client"
)

func main() {
	names := flag.String("scenarios", "all", "comma-separated scenario names, or all")
	bin := flag.String("bin", "bin/shardis", "shardis binary")
	traces := flag.String("traces", "website/public/traces", "where to write trace JSON")
	csvPath := flag.String("csv", "docs/benchmarks/iter-13-chaos/report.csv", "report CSV ('' = none)")
	list := flag.Bool("list", false, "list scenarios and exit")
	external := flag.String("external", "", "checker-only mode: run the workload against this existing cluster (no faults; something else injects them)")
	duration := flag.Duration("duration", 60*time.Second, "external mode: how long to run the workload")
	flag.Parse()
	if *external != "" {
		r, err := runExternal(*external, *duration)
		if err != nil {
			fmt.Fprintln(os.Stderr, "shardis-chaos:", err)
			os.Exit(1)
		}
		printTable(os.Stdout, []report{r})
		return
	}
	if *list {
		for _, s := range scenarios {
			fmt.Printf("%-28s %s\n", s.name, s.desc)
		}
		return
	}
	var chosen []scenario
	for _, s := range scenarios {
		if *names == "all" || strings.Contains(","+*names+",", ","+s.name+",") {
			chosen = append(chosen, s)
		}
	}
	if len(chosen) == 0 {
		fmt.Fprintln(os.Stderr, "no such scenario; -list shows them")
		os.Exit(2)
	}
	os.MkdirAll(*traces, 0o755)
	var reports []report
	failed := false
	for _, s := range chosen {
		fmt.Fprintf(os.Stderr, "== %s: %s\n", s.name, s.desc)
		r, err := runScenario(*bin, *traces, s)
		if err != nil {
			fmt.Fprintf(os.Stderr, "   FAILED: %v\n", err)
			failed = true
			continue
		}
		reports = append(reports, r)
	}
	printTable(os.Stdout, reports)
	if *csvPath != "" && len(reports) > 0 {
		writeCSV(*csvPath, reports)
	}
	if failed {
		os.Exit(1)
	}
}

func runScenario(bin, traceDir string, s scenario) (report, error) {
	rec := newRecorder()
	h, err := newHarness(bin, rec)
	if err != nil {
		return report{}, err
	}
	defer h.close()
	if err := h.startAll(); err != nil {
		return report{}, err
	}
	seeds := []string{h.addr("a"), h.addr("b"), h.addr("c")}
	cl, err := client.NewCluster(seeds, client.Options{MaxRetryTime: 30 * time.Second})
	if err != nil {
		return report{}, err
	}
	defer cl.Close()

	rec.t0 = time.Now() // the trace's clock starts with the workload
	rec.mu.Lock()
	rec.events = nil // drop startup notes
	rec.mu.Unlock()
	for _, n := range allNames {
		rec.follow(h.addr(n), n)
	}
	hist := &history{t0: rec.t0}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { runWorkload(cl, hist, stop); close(done) }()

	time.Sleep(3 * time.Second)
	rec.note("scenario_start", "", "name", s.name)
	faultErr := s.run(h)
	rec.note("recovered", "")
	time.Sleep(3 * time.Second)
	close(stop)
	<-done
	end := time.Since(rec.t0)
	if faultErr != nil {
		rec.close()
		return report{}, faultErr
	}

	final := make([]int64, writers*keysPerWriter)
	for k := range final {
		v, err := cl.Do(context.Background(), []byte("GET"), keyName(k))
		if err != nil {
			rec.close()
			return report{}, fmt.Errorf("final read of key %d: %w", k, err)
		}
		final[k], _ = strconv.ParseInt(string(v.Str), 10, 64)
	}
	time.Sleep(300 * time.Millisecond) // let the last events arrive
	rec.close()

	r := check(hist, final, end)
	r.Scenario = s.name
	r.Events = len(rec.events)
	r.EventsDropped = rec.dropped
	if err := writeTrace(filepath.Join(traceDir, s.name+".json"), s, h, rec, hist, end, r); err != nil {
		return r, err
	}
	return r, nil
}

// trace is the file the website replays: what the nodes reported, what
// the chaos tool did, what clients saw (per 100 ms), and the verdict.
type trace struct {
	Scenario    string       `json:"scenario"`
	Description string       `json:"description"`
	Recorded    time.Time    `json:"recorded"`
	DurationS   float64      `json:"duration_s"`
	Nodes       []traceNode  `json:"nodes"`
	Events      []traceEvent `json:"events"`
	Timeline    []bucket     `json:"timeline"`
	Report      report       `json:"report"`
}

type traceNode struct {
	Name    string `json:"name"`
	Role    string `json:"role"`
	Primary string `json:"primary,omitempty"`
	Slots   string `json:"slots,omitempty"`
}

func writeTrace(path string, s scenario, h *harness, rec *recorder, hist *history, end time.Duration, r report) error {
	t := trace{Scenario: s.name, Description: s.desc, Recorded: rec.t0.UTC(), DurationS: end.Seconds(),
		Timeline: timeline(hist, end), Report: r}
	for _, n := range allNames {
		tn := traceNode{Name: n, Role: "primary", Slots: seedSlots[n]}
		if p, ok := replicaOf[n]; ok {
			tn.Role, tn.Primary = "replica", p
		}
		if n == "g" {
			tn.Role = "empty"
		}
		t.Nodes = append(t.Nodes, tn)
	}
	for _, e := range rec.events {
		if e.T >= 0 {
			t.Events = append(t.Events, e)
		}
	}
	// Events arrive from seven streams; put them in time order.
	slices.SortStableFunc(t.Events, func(a, b traceEvent) int { return cmp.Compare(a.T, b.T) })
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// runExternal runs the recorded workload against a cluster someone else is
// breaking (the Kubernetes demo scripts: kubectl delete pod, rollout
// restart, NetworkPolicy), then checks the history. No traces: the nodes
// come and go, and the demo prints its own timeline.
func runExternal(addr string, d time.Duration) (report, error) {
	var cl *client.Cluster
	var err error
	for range 60 { // the cluster may still be forming
		if cl, err = client.NewCluster([]string{addr}, client.Options{MaxRetryTime: 60 * time.Second}); err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return report{}, err
	}
	defer cl.Close()
	hist := &history{t0: time.Now()}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { runWorkload(cl, hist, stop); close(done) }()
	tick := time.NewTicker(5 * time.Second)
	end := time.After(d)
loop:
	for {
		select {
		case <-end:
			break loop
		case <-tick.C:
			fmt.Fprintf(os.Stderr, "%4.0fs  moved=%d ask=%d retries=%d net_errors=%d\n", time.Since(hist.t0).Seconds(),
				cl.Stats.Moved.Load(), cl.Stats.Ask.Load(), cl.Stats.Retries.Load(), cl.Stats.NetErrors.Load())
		}
	}
	close(stop)
	<-done
	elapsed := time.Since(hist.t0)
	final := make([]int64, writers*keysPerWriter)
	for k := range final {
		v, err := cl.Do(context.Background(), []byte("GET"), keyName(k))
		if err != nil {
			return report{}, fmt.Errorf("final read of key %d: %w", k, err)
		}
		final[k], _ = strconv.ParseInt(string(v.Str), 10, 64)
	}
	r := check(hist, final, elapsed)
	r.Scenario = "external " + addr
	fmt.Fprintf(os.Stderr, "longest stall: %.0f ms, ending %.1f s after start (%s)\n", r.MaxStallMs, r.MaxStallAtS,
		hist.t0.Add(time.Duration(r.MaxStallAtS*float64(time.Second))).UTC().Format("15:04:05.000"))
	return r, nil
}
