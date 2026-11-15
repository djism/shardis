package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/djism/shardis/internal/admin"
	"github.com/djism/shardis/internal/keyslot"
	"github.com/djism/shardis/internal/resp"
	"github.com/djism/shardis/pkg/client"
)

// Client chaos mode (Iteration 12): what the application sees through our
// smart client while the cluster goes through, under load:
//
//  1. a pause: primary b freezes for 1.5 s (SIGSTOP, like a long GC pause or
//     a network hiccup), shorter than the node timeout, so no failover. The
//     client's 500 ms per-attempt timeout fires, and it decides whether to resend.
//  2. a crash: primary a is killed (kill -9) and its replica d takes over.
//  3. a comes back (and rejoins as d's replica).
//  4. a rebalance onto a new, empty node g.
//
// Writers INCR counters; afterwards every counter is
// compared with what the client reported:
//
//	unknown  INCRs that failed with ErrUnknownOutcome (sent, then the connection died)
//	errors   any other failure the application saw
//	lost     acknowledged INCRs missing at the end (async replication, ADR 017)
//	doubled  increments beyond acked + unknown: an INCR applied twice (a retry)
//
//	shardis-crashtest -client-chaos safe,all,never -runs 5
func runClientChaos(o options, policies []string) {
	fmt.Println("policy,run,pause_unknown,pause_doubled,failover_ms,rebalance_s,acked,unknown,errors,lost,doubled,max_latency_ms")
	for _, name := range policies {
		policy, ok := map[string]client.RetryPolicy{"safe": client.RetrySafe, "all": client.RetryAll, "never": client.RetryNever}[name]
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown policy %q (safe, all, never)\n", name)
			os.Exit(2)
		}
		for run := 1; run <= o.runs; run++ {
			r, err := clientChaosRun(o, policy)
			if err != nil {
				fmt.Fprintf(os.Stderr, "policy %s run %d: %v\n", name, run, err)
				os.Exit(1)
			}
			fmt.Printf("%s,%d,%d,%d,%.0f,%.1f,%d,%d,%d,%d,%d,%.0f\n", name, run, r.pauseUnknown, r.pauseDoubled, r.failoverMs, r.rebalanceS,
				r.acked, r.unknown, r.errors, r.lost, r.doubled, r.maxLatencyMs)
			if r.firstErr != "" {
				fmt.Fprintf(os.Stderr, "  policy %s run %d first error: %s\n", name, run, r.firstErr)
			}
		}
	}
}

type chaosResult struct {
	failoverMs, rebalanceS, maxLatencyMs  float64
	acked, unknown, errors, lost, doubled int64
	pauseUnknown, pauseDoubled            int64 // the same, for the counters b serves (the paused node)
	firstErr                              string
}

const nCounters = 2000

func clientChaosRun(o options, policy client.RetryPolicy) (chaosResult, error) {
	dir, err := os.MkdirTemp("", "shardis-chaos-*")
	if err != nil {
		return chaosResult{}, err
	}
	defer os.RemoveAll(dir)
	names := []string{"a", "b", "c", "d", "e", "f"}
	ports, used := map[string]int{}, map[int]bool{}
	var topo strings.Builder
	slots := map[string]string{"a": "0-5460", "b": "5461-10922", "c": "10923-16383"}
	primaryOf := map[string]string{"d": "a", "e": "b", "f": "c"}
	for _, n := range append(names, "g") {
		ports[n] = clusterPort(used)
	}
	for _, n := range names {
		addr := fmt.Sprintf("127.0.0.1:%d", ports[n])
		if r, ok := slots[n]; ok {
			fmt.Fprintf(&topo, "node %s %s %s primary %s\n", n, addr, addr, r)
		} else {
			fmt.Fprintf(&topo, "node %s %s %s replica %s\n", n, addr, addr, primaryOf[n])
		}
	}
	topoFile := filepath.Join(dir, "topology.conf")
	os.WriteFile(topoFile, []byte(topo.String()), 0o644)
	var mu sync.Mutex
	procs := map[string]*os.Process{}
	defer func() {
		for _, p := range procs {
			p.Signal(syscall.SIGKILL)
			p.Wait()
		}
	}()
	startNode := func(n string, args ...string) error {
		nodeDir := filepath.Join(dir, n)
		os.MkdirAll(nodeDir, 0o755)
		all := append([]string{"--port", strconv.Itoa(ports[n]), "--dir", nodeDir, "--cluster-node-timeout", "2000"}, args...)
		cmd, err := start(o.bin, all, ports[n])
		if err != nil {
			return fmt.Errorf("start %s: %w", n, err)
		}
		mu.Lock()
		procs[n] = cmd.Process
		mu.Unlock()
		return nil
	}
	for _, n := range names {
		if err := startNode(n, "--cluster-config", topoFile, "--cluster-node", n); err != nil {
			return chaosResult{}, err
		}
	}
	if err := startNode("g", "--cluster-enabled", "yes"); err != nil {
		return chaosResult{}, err
	}
	for _, n := range []string{"d", "e", "f"} {
		if err := waitLinkUp(ports[n]); err != nil {
			return chaosResult{}, err
		}
	}
	if err := waitClusterOK(ports["b"], 15*time.Second); err != nil {
		return chaosResult{}, err
	}

	addrOf := func(n string) string { return fmt.Sprintf("127.0.0.1:%d", ports[n]) }
	cl, err := client.NewCluster([]string{addrOf("b"), addrOf("c")},
		client.Options{Retry: policy, MaxRetryTime: 30 * time.Second, Timeout: 500 * time.Millisecond})
	if err != nil {
		return chaosResult{}, err
	}
	defer cl.Close()
	var acked, unknown [nCounters]atomic.Int64
	var errs, maxLat atomic.Int64
	var firstErr atomic.Value
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range o.writers {
		wg.Go(func() {
			for i := w; ; i = (i + o.writers) % nCounters {
				select {
				case <-stop:
					return
				default:
				}
				t0 := time.Now()
				v, err := cl.Do(context.Background(), []byte("INCR"), []byte(fmt.Sprint("ctr:", i)))
				if lat := time.Since(t0).Microseconds(); lat > maxLat.Load() {
					maxLat.Store(lat) // racy max is fine for a report
				}
				switch {
				case errors.Is(err, client.ErrUnknownOutcome):
					unknown[i].Add(1)
				case err != nil || v.Kind == resp.Error:
					errs.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Sprint(err, " ", string(v.Str)))
				default:
					acked[i].Add(1)
				}
			}
		})
	}
	finish := func() { close(stop); wg.Wait() }

	time.Sleep(time.Second)
	mu.Lock()
	procs["b"].Signal(syscall.SIGSTOP)
	mu.Unlock()
	time.Sleep(1500 * time.Millisecond)
	mu.Lock()
	procs["b"].Signal(syscall.SIGCONT)
	mu.Unlock()
	time.Sleep(time.Second)
	pauseUnknown := make([]int64, nCounters) // unknown outcomes so far: all from the pause
	for i := range nCounters {
		pauseUnknown[i] = unknown[i].Load()
	}

	killed := time.Now()
	mu.Lock()
	procs["a"].Signal(syscall.SIGKILL)
	procs["a"].Wait()
	delete(procs, "a")
	mu.Unlock()
	for {
		if role, err := command(ports["d"], "ROLE"); err == nil && strings.Contains(role, "master") {
			break
		}
		info, _ := command(ports["d"], "INFO", "replication")
		if strings.Contains(info, "role:master") {
			break
		}
		if time.Since(killed) > 30*time.Second {
			finish()
			return chaosResult{}, errors.New("d never took over")
		}
		time.Sleep(20 * time.Millisecond)
	}
	r := chaosResult{failoverMs: float64(time.Since(killed).Microseconds()) / 1000}
	time.Sleep(time.Second)
	if err := startNode("a", "--cluster-config", topoFile, "--cluster-node", "a"); err != nil {
		finish()
		return chaosResult{}, err
	}
	time.Sleep(2 * time.Second)
	if _, err := admin.AddNode(addrOf("b"), addrOf("g"), ""); err != nil {
		finish()
		return chaosResult{}, fmt.Errorf("add-node: %w", err)
	}
	if err := waitClusterOK(ports["g"], 15*time.Second); err != nil {
		finish()
		return chaosResult{}, err
	}
	t0 := time.Now()
	if err := admin.Rebalance(addrOf("b"), admin.MoveOptions{}); err != nil {
		finish()
		return chaosResult{}, fmt.Errorf("rebalance: %w", err)
	}
	r.rebalanceS = time.Since(t0).Seconds()
	time.Sleep(time.Second)
	finish()

	for i := range nCounters {
		v, err := cl.Do(context.Background(), []byte("GET"), []byte(fmt.Sprint("ctr:", i)))
		if err != nil {
			return r, fmt.Errorf("reading counter %d: %w", i, err)
		}
		got, _ := strconv.ParseInt(string(v.Str), 10, 64)
		a, u := acked[i].Load(), unknown[i].Load()
		r.acked += a
		r.unknown += u
		if got < a {
			r.lost += a - got
		}
		if got > a+u {
			r.doubled += got - a - u
			if servedByB(i) {
				r.pauseDoubled += got - a - u
			}
		}
		if servedByB(i) {
			r.pauseUnknown += pauseUnknown[i]
		}
	}
	r.errors = errs.Load()
	r.maxLatencyMs = float64(maxLat.Load()) / 1000
	if s, ok := firstErr.Load().(string); ok {
		r.firstErr = s
	}
	return r, nil
}

// servedByB: is counter i in primary b's slot range (the node we pause)?
// The rebalance later moves some of them, but the pause happens first.
func servedByB(i int) bool {
	s := keyslot.Of([]byte(fmt.Sprint("ctr:", i)))
	return s >= 5461 && s <= 10922
}
