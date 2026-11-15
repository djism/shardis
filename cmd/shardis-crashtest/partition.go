package main

import (
	"fmt"
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

// Partition mode (Iteration 10): the split-brain window.
//
// One run: 3 primaries + 3 replicas as real processes with fault injection
// on. Writers keep writing keys that live on primary a, straight to a (as if
// they sit on a's side of the network). After 1 s, every node gets
// FAULT PARTITION a: a is cut off from the other five (cluster bus and
// replication), but its clients can still reach it. We record:
//
//	minority_writable  how long a kept acknowledging writes after the split
//	majority_writable  how long until a's replica d accepted writes for a's slots
//	gap                time in between, when no node accepted writes for a's slots
//	lost               writes a acknowledged after the split that are gone once
//	                   the partition heals and a rejoins as d's replica
//
//	shardis-crashtest -partition 2000,5000 -runs 10
func runPartition(o options, timeouts []string) {
	fmt.Println("node_timeout_ms,run,minority_writable_ms,majority_writable_ms,gap_ms,acked_during_split,lost_during_split,lost_before_split")
	for _, ts := range timeouts {
		timeout, err := strconv.Atoi(ts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad timeout %q\n", ts)
			os.Exit(2)
		}
		var minority, majority []float64
		for run := 1; run <= o.runs; run++ {
			r, err := partitionRun(o, timeout)
			if err != nil {
				fmt.Fprintf(os.Stderr, "timeout %d run %d: %v\n", timeout, run, err)
				os.Exit(1)
			}
			minority, majority = append(minority, r.minorityMs), append(majority, r.majorityMs)
			fmt.Printf("%d,%d,%.0f,%.0f,%.0f,%d,%d,%d\n", timeout, run, r.minorityMs, r.majorityMs,
				r.majorityMs-r.minorityMs, r.ackedDuring, r.lostDuring, r.lostBefore)
		}
		sort.Float64s(minority)
		sort.Float64s(majority)
		fmt.Fprintf(os.Stderr, "node timeout %d ms: minority kept writing p50 %.0f ms (max %.0f); majority took over p50 %.0f ms (max %.0f)\n",
			timeout, percentile(minority, 0.5), minority[len(minority)-1], percentile(majority, 0.5), majority[len(majority)-1])
	}
}

type partitionResult struct {
	minorityMs, majorityMs              float64
	ackedDuring, lostDuring, lostBefore int
}

type ackedWrite struct {
	n  int
	at time.Time
}

func partitionRun(o options, timeoutMs int) (partitionResult, error) {
	dir, err := os.MkdirTemp("", "shardis-partition-*")
	if err != nil {
		return partitionResult{}, err
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
	os.WriteFile(topoFile, []byte(topo.String()), 0o644)
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
			"--cluster-config", topoFile, "--cluster-node", n, "--cluster-node-timeout", strconv.Itoa(timeoutMs),
			"--enable-fault-injection", "yes"}, ports[n])
		if err != nil {
			return partitionResult{}, fmt.Errorf("start %s: %w", n, err)
		}
		procs[n] = cmd.Process
	}
	for _, n := range []string{"d", "e", "f"} {
		if err := waitLinkUp(ports[n]); err != nil {
			return partitionResult{}, err
		}
	}
	if err := waitClusterOK(ports["b"], 15*time.Second); err != nil {
		return partitionResult{}, err
	}

	tags := make([]string, o.writers)
	for w := range tags {
		for i := 0; ; i++ {
			if tag := fmt.Sprintf("p%d-%d", w, i); keyslot.Of([]byte(tag)) <= 5460 {
				tags[w] = tag
				break
			}
		}
	}
	stop := make(chan struct{})
	logs := make([][]ackedWrite, o.writers)
	var wg sync.WaitGroup
	for w := range o.writers {
		wg.Go(func() { logs[w] = partitionWriter(ports["a"], tags[w], stop) })
	}
	time.Sleep(time.Second)

	split := time.Now()
	for _, n := range names {
		if _, err := command(ports[n], "FAULT", "PARTITION", "a"); err != nil {
			return partitionResult{}, fmt.Errorf("partition on %s: %w", n, err)
		}
	}
	probe := "{" + tags[0] + "}:majority"
	deadline := time.Now().Add(time.Duration(timeoutMs)*6*time.Millisecond + 20*time.Second)
	var dOK time.Time
	for dOK.IsZero() {
		if reply, err := command(ports["d"], "SET", probe, "x"); err == nil && reply == "OK" {
			dOK = time.Now()
		} else if time.Now().After(deadline) {
			return partitionResult{}, fmt.Errorf("majority never failed over (last %q, %v)", reply, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(time.Second) // a has surely stopped by now; then heal
	close(stop)
	wg.Wait()
	for _, n := range names {
		if _, err := command(ports[n], "FAULT", "HEAL"); err != nil {
			return partitionResult{}, fmt.Errorf("heal on %s: %w", n, err)
		}
	}
	wantRole := fmt.Sprintf("%d", ports["d"])
	healDeadline := time.Now().Add(30 * time.Second)
	for {
		if info, err := command(ports["a"], "INFO", "replication"); err == nil &&
			strings.Contains(info, "role:slave") && strings.Contains(info, "master_port:"+wantRole) && strings.Contains(info, "master_link_status:up") {
			break
		}
		if time.Now().After(healDeadline) {
			return partitionResult{}, fmt.Errorf("a never rejoined as d's replica")
		}
		time.Sleep(50 * time.Millisecond)
	}

	var r partitionResult
	var lastAck time.Time
	for w, log := range logs {
		var before, during []int
		for _, a := range log {
			if a.at.After(split) {
				during = append(during, a.n)
				if a.at.After(lastAck) {
					lastAck = a.at
				}
			} else {
				before = append(before, a.n)
			}
		}
		r.ackedDuring += len(during)
		lb, err := countMissingTagged(ports["d"], tags[w], before)
		if err != nil {
			return partitionResult{}, err
		}
		ld, err := countMissingTagged(ports["d"], tags[w], during)
		if err != nil {
			return partitionResult{}, err
		}
		r.lostBefore += lb
		r.lostDuring += ld
	}
	if !lastAck.IsZero() {
		r.minorityMs = float64(lastAck.Sub(split).Microseconds()) / 1000
	}
	r.majorityMs = float64(dOK.Sub(split).Microseconds()) / 1000
	return r, nil
}

// partitionWriter keeps writing until stop, recording every acknowledged
// write and when. CLUSTERDOWN replies are expected (a in the minority) and
// just skipped.
func partitionWriter(port int, tag string, stop chan struct{}) (acked []ackedWrite) {
	conn, err := net.Dial("tcp", fmt.Sprint("127.0.0.1:", port))
	if err != nil {
		return nil
	}
	defer conn.Close()
	wr, rd := resp.NewWriter(conn), resp.NewReader(conn)
	for n := 0; ; n++ {
		select {
		case <-stop:
			return acked
		default:
		}
		wr.WriteCommand([]byte("SET"), []byte(fmt.Sprintf("{%s}:%d", tag, n)), []byte(strconv.Itoa(n)))
		if wr.Flush() != nil {
			return acked
		}
		v, err := rd.ReadValue()
		if err != nil {
			return acked
		}
		if string(v.Str) == "OK" {
			acked = append(acked, ackedWrite{n, time.Now()})
		} else {
			time.Sleep(2 * time.Millisecond) // refused: don't spin
		}
	}
}
