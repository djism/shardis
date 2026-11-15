package main

import (
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/djism/shardis/internal/keyslot"
	"github.com/djism/shardis/internal/resp"
)

// Cluster failover mode (Iteration 9): how long until a dead primary's slots
// accept writes again, with no manual steps.
//
// One run: start 3 primaries + 3 replicas as real processes from a seed
// topology; 8 writers write keys that live on primary a (straight to a);
// after 1 s, kill -9 a; probe a's replica d every 10 ms with a write to one
// of a's slots until it says OK. Failover time = kill → first OK. Then count
// writes a acknowledged that d doesn't have (the async-replication window).
//
//	shardis-crashtest -cluster-failover 2000,5000 -runs 20
func runClusterFailover(o options, timeouts []string) {
	fmt.Println("node_timeout_ms,run,failover_ms,acked,lost")
	for _, ts := range timeouts {
		timeout, err := strconv.Atoi(ts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad timeout %q\n", ts)
			os.Exit(2)
		}
		var times []float64
		for run := 1; run <= o.runs; run++ {
			ms, acked, lost, err := clusterFailoverRun(o, timeout)
			if err != nil {
				fmt.Fprintf(os.Stderr, "timeout %d run %d: %v\n", timeout, run, err)
				os.Exit(1)
			}
			times = append(times, ms)
			fmt.Printf("%d,%d,%.0f,%d,%d\n", timeout, run, ms, acked, lost)
		}
		sort.Float64s(times)
		fmt.Fprintf(os.Stderr, "node timeout %d ms: failover p50 %.0f ms, p99 %.0f ms, min %.0f, max %.0f\n",
			timeout, percentile(times, 0.5), percentile(times, 0.99), times[0], times[len(times)-1])
	}
}

func percentile(sorted []float64, q float64) float64 {
	i := int(q * float64(len(sorted)-1))
	return sorted[i]
}

// clusterPort picks a port whose bus port (+10000) is free too.
func clusterPort(used map[int]bool) int {
	for {
		p := 20000 + rand.IntN(20000)
		if used[p] {
			continue
		}
		a, err1 := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		b, err2 := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p+10000))
		if a != nil {
			a.Close()
		}
		if b != nil {
			b.Close()
		}
		if err1 == nil && err2 == nil {
			used[p] = true
			return p
		}
	}
}

func clusterFailoverRun(o options, timeoutMs int) (failoverMs float64, acked, lost int, err error) {
	dir, err := os.MkdirTemp("", "shardis-failover-*")
	if err != nil {
		return 0, 0, 0, err
	}
	defer os.RemoveAll(dir)
	names := []string{"a", "b", "c", "d", "e", "f"}
	ports, used := map[string]int{}, map[int]bool{}
	var topo strings.Builder
	slots := map[string]string{"a": "0-5460", "b": "5461-10922", "c": "10923-16383"}
	primaryOf := map[string]string{"d": "a", "e": "b", "f": "c"}
	for _, n := range names {
		ports[n] = clusterPort(used)
		addr := fmt.Sprintf("127.0.0.1:%d", ports[n])
		if r, ok := slots[n]; ok {
			fmt.Fprintf(&topo, "node %s %s %s primary %s\n", n, addr, addr, r)
		} else {
			fmt.Fprintf(&topo, "node %s %s %s replica %s\n", n, addr, addr, primaryOf[n])
		}
	}
	topoFile := filepath.Join(dir, "topology.conf")
	if err := os.WriteFile(topoFile, []byte(topo.String()), 0o644); err != nil {
		return 0, 0, 0, err
	}
	procs := map[string]*os.Process{}
	defer func() {
		for _, p := range procs {
			p.Signal(syscall.SIGKILL)
			p.Wait()
		}
	}()
	for _, n := range names {
		nodeDir := filepath.Join(dir, n)
		os.MkdirAll(nodeDir, 0o755)
		cmd, err := start(o.bin, []string{"--port", strconv.Itoa(ports[n]), "--dir", nodeDir,
			"--cluster-config", topoFile, "--cluster-node", n, "--cluster-node-timeout", strconv.Itoa(timeoutMs)}, ports[n])
		if err != nil {
			return 0, 0, 0, fmt.Errorf("start %s: %w", n, err)
		}
		procs[n] = cmd.Process
	}
	for _, n := range []string{"d", "e", "f"} {
		if err := waitLinkUp(ports[n]); err != nil {
			return 0, 0, 0, err
		}
	}
	if err := waitClusterOK(ports["b"], 15*time.Second); err != nil {
		return 0, 0, 0, err
	}

	// Load: writers SET keys that live on a, straight to a.
	tags := make([]string, o.writers) // one hash tag per writer that lands in a's slots
	for w := range tags {
		for i := 0; ; i++ {
			tag := fmt.Sprintf("w%d-%d", w, i)
			if keyslot.Of([]byte(tag)) <= 5460 {
				tags[w] = tag
				break
			}
		}
	}
	ackedBy := make([][]int, o.writers)
	var wg sync.WaitGroup
	for w := range o.writers {
		wg.Go(func() { ackedBy[w] = taggedWriter(ports["a"], tags[w]) })
	}
	time.Sleep(time.Second)

	killedAt := time.Now()
	procs["a"].Signal(syscall.SIGKILL) // the primary dies
	procs["a"].Wait()
	delete(procs, "a")
	wg.Wait()

	probe := "{" + tags[0] + "}:probe"
	deadline := time.Now().Add(time.Duration(timeoutMs)*time.Millisecond*6 + 20*time.Second)
	for {
		reply, err := command(ports["d"], "SET", probe, "x")
		if err == nil && reply == "OK" {
			break
		}
		if time.Now().After(deadline) {
			return 0, 0, 0, fmt.Errorf("d never accepted writes (last: %q, %v)", reply, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	failoverMs = float64(time.Since(killedAt).Microseconds()) / 1000

	for w := range o.writers {
		acked += len(ackedBy[w])
		l, err := countMissingTagged(ports["d"], tags[w], ackedBy[w])
		if err != nil {
			return 0, 0, 0, err
		}
		lost += l
	}
	return failoverMs, acked, lost, nil
}

func taggedWriter(port int, tag string) (acked []int) {
	conn, err := net.Dial("tcp", fmt.Sprint("127.0.0.1:", port))
	if err != nil {
		return nil
	}
	defer conn.Close()
	wr, rd := resp.NewWriter(conn), resp.NewReader(conn)
	for n := 0; ; n++ {
		wr.WriteCommand([]byte("SET"), []byte(fmt.Sprintf("{%s}:%d", tag, n)), []byte(strconv.Itoa(n)))
		if wr.Flush() != nil {
			return acked
		}
		v, err := rd.ReadValue()
		if err != nil || string(v.Str) != "OK" {
			return acked
		}
		acked = append(acked, n)
	}
}

func countMissingTagged(port int, tag string, seqs []int) (int, error) {
	conn, err := net.Dial("tcp", fmt.Sprint("127.0.0.1:", port))
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	wr, rd := resp.NewWriter(conn), resp.NewReader(conn)
	lost := 0
	for i := 0; i < len(seqs); i += 500 {
		args := [][]byte{[]byte("MGET")}
		for _, n := range seqs[i:min(i+500, len(seqs))] {
			args = append(args, []byte(fmt.Sprintf("{%s}:%d", tag, n)))
		}
		wr.WriteCommand(args...)
		if err := wr.Flush(); err != nil {
			return 0, err
		}
		v, err := rd.ReadValue()
		if err != nil {
			return 0, err
		}
		if v.Kind == resp.Error {
			return 0, fmt.Errorf("MGET on new primary: %s", v.Str)
		}
		for _, e := range v.Elems {
			if e.Null {
				lost++
			}
		}
	}
	return lost, nil
}

func waitClusterOK(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if info, err := command(port, "CLUSTER", "INFO"); err == nil && strings.Contains(info, "cluster_state:ok") {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("cluster on %d never reached state ok", port)
}
