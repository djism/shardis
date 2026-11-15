// Command shardis-crashtest measures how many *acknowledged* writes survive
// a crash, for each AOF fsync policy.
//
// One run: start a real shardis process, let several writers send numbered
// SETs as fast as they can (each records which ones got "OK"), crash the
// server mid-load, restart it on the same files, and count acknowledged keys
// that are missing.
//
// Two kinds of crash:
//
//	process  kill -9. The process dies; the OS keeps everything already
//	         handed to it with write(). This is what most crashes look like
//	         (panic, OOM kill, kubectl delete --force).
//	power    kill -9 plus a simulated power cut: the AOF is truncated to the
//	         last length known to be fsynced (the server's
//	         --debug-record-synced fault hook writes that down after each
//	         fsync). This is the worst case for a machine losing power: bytes
//	         not yet fsynced are gone. (A real power cut might save some of
//	         them; we assume none.)
//
//	shardis-crashtest -runs 10 -policies off,no,everysec,always -modes process,power
//
// With -failover it instead measures replication's lost-write window: kill
// the primary, promote its replica, count acknowledged writes the replica
// lacks (see failover.go).
//
//	shardis-crashtest -failover plain,wait,slowlink,slowlink+wait -runs 10
//
// With -cluster-failover it measures automatic failover in a 6-node cluster:
// kill a primary, time how long until its replica accepts writes for its
// slots (see cluster_failover.go).
//
//	shardis-crashtest -cluster-failover 2000,5000 -runs 20
//
// With -partition it cuts a primary off from the rest of the cluster while
// its clients keep writing to it, and measures the split-brain window (see
// partition.go).
//
//	shardis-crashtest -partition 2000,5000 -runs 10
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/djism/shardis/internal/resp"
)

type options struct {
	bin      string
	policies []string
	modes    []string
	runs     int
	writers  int
	minLoad  time.Duration
	maxLoad  time.Duration
}

func main() {
	var o options
	var policies, modes string
	flag.StringVar(&o.bin, "bin", "./bin/shardis", "shardis binary")
	flag.StringVar(&policies, "policies", "off,no,everysec,always", "fsync policies to test (off = no AOF)")
	flag.StringVar(&modes, "modes", "process,power", "crash kinds: process, power")
	flag.IntVar(&o.runs, "runs", 10, "runs per policy and mode")
	flag.IntVar(&o.writers, "writers", 16, "concurrent writers")
	flag.DurationVar(&o.minLoad, "min-load", 1500*time.Millisecond, "shortest time under load before the crash")
	flag.DurationVar(&o.maxLoad, "max-load", 3*time.Second, "longest time under load before the crash")
	failover := flag.String("failover", "", "replication failover test instead: variants plain,wait,slowlink,slowlink+wait")
	clusterFailover := flag.String("cluster-failover", "", "cluster failover timing instead: node timeouts in ms, e.g. 2000,5000")
	partition := flag.String("partition", "", "network partition (split-brain window) test instead: node timeouts in ms")
	flag.Parse()
	o.policies, o.modes = strings.Split(policies, ","), strings.Split(modes, ",")
	if *failover != "" {
		runFailover(o, strings.Split(*failover, ","))
		return
	}
	if *clusterFailover != "" {
		runClusterFailover(o, strings.Split(*clusterFailover, ","))
		return
	}
	if *partition != "" {
		runPartition(o, strings.Split(*partition, ","))
		return
	}

	fmt.Println("policy,mode,run,load_s,acked,lost,lost_pct,acked_writes_per_s,unacked_but_present")
	for _, mode := range o.modes {
		for _, policy := range o.policies {
			if policy == "off" && mode == "power" {
				continue // nothing on disk either way
			}
			for run := 1; run <= o.runs; run++ {
				r, err := crashRun(o, policy, mode)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s/%s run %d: %v\n", policy, mode, run, err)
					os.Exit(1)
				}
				fmt.Printf("%s,%s,%d,%.2f,%d,%d,%.3f,%.0f,%d\n", policy, mode, run, r.load.Seconds(),
					r.acked, r.lost, 100*float64(r.lost)/float64(max(r.acked, 1)), float64(r.acked)/r.load.Seconds(), r.phantom)
			}
		}
	}
}

type runResult struct {
	load        time.Duration
	acked, lost int
	phantom     int // keys present that never got an OK (written, reply lost in the crash)
}

func crashRun(o options, policy, mode string) (runResult, error) {
	dir, err := os.MkdirTemp("", "shardis-crash-*")
	if err != nil {
		return runResult{}, err
	}
	defer os.RemoveAll(dir)
	port := freePort()
	args := []string{"--port", strconv.Itoa(port), "--dir", dir}
	if policy != "off" {
		args = append(args, "--appendonly", "yes", "--appendfsync", policy)
	}
	if mode == "power" {
		args = append(args, "--debug-record-synced", "yes")
	}
	srv, err := start(o.bin, args, port)
	if err != nil {
		return runResult{}, err
	}

	// Load: each writer sends SET c:<writer>:<n> <n>, n = 0, 1, 2, ... one at a
	// time, and remembers the highest n that got "OK".
	acked := make([]int, o.writers) // writer w got OK for keys 0..acked[w]-1
	var wg sync.WaitGroup
	for w := range o.writers {
		wg.Go(func() { acked[w] = writer(port, w) })
	}
	load := o.minLoad + time.Duration(rand.Int64N(int64(o.maxLoad-o.minLoad)+1))
	time.Sleep(load)
	srv.Process.Signal(syscall.SIGKILL) // the crash
	srv.Wait()
	wg.Wait()

	if mode == "power" {
		if err := simulatePowerLoss(dir); err != nil {
			return runResult{}, err
		}
	}

	// Restart on the same files and count what's missing.
	srv2, err := start(o.bin, args[:len(args)-boolInt(mode == "power")*2], port)
	if err != nil {
		return runResult{}, fmt.Errorf("restart: %w", err)
	}
	defer func() { srv2.Process.Signal(syscall.SIGTERM); srv2.Wait() }()
	res := runResult{load: load}
	for w, n := range acked {
		res.acked += n
		lost, phantom, err := check(port, w, n)
		if err != nil {
			return runResult{}, err
		}
		res.lost += lost
		res.phantom += phantom
	}
	return res, nil
}

// writer loops until the connection breaks (the crash) and returns how many
// consecutive writes were acknowledged.
func writer(port, w int) int {
	conn, err := net.Dial("tcp", fmt.Sprint("127.0.0.1:", port))
	if err != nil {
		return 0
	}
	defer conn.Close()
	wr, rd := resp.NewWriter(conn), resp.NewReader(conn)
	for n := 0; ; n++ {
		v := strconv.Itoa(n)
		wr.WriteCommand([]byte("SET"), []byte(fmt.Sprintf("c:%d:%d", w, n)), []byte(v))
		if wr.Flush() != nil {
			return n
		}
		reply, err := rd.ReadValue()
		if err != nil || string(reply.Str) != "OK" {
			return n // no OK: this write was never acknowledged
		}
	}
}

// check counts missing acknowledged keys (0..n-1) and present unacknowledged
// ones (n, n+1, ...) for writer w.
func check(port, w, n int) (lost, phantom int, err error) {
	conn, err := net.Dial("tcp", fmt.Sprint("127.0.0.1:", port))
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()
	wr, rd := resp.NewWriter(conn), resp.NewReader(conn)
	mget := func(from, to int) ([]bool, error) {
		args := [][]byte{[]byte("MGET")}
		for i := from; i < to; i++ {
			args = append(args, []byte(fmt.Sprintf("c:%d:%d", w, i)))
		}
		wr.WriteCommand(args...)
		if err := wr.Flush(); err != nil {
			return nil, err
		}
		v, err := rd.ReadValue()
		if err != nil {
			return nil, err
		}
		present := make([]bool, len(v.Elems))
		for i, e := range v.Elems {
			present[i] = !e.Null
		}
		return present, nil
	}
	for from := 0; from < n; from += 500 {
		present, err := mget(from, min(from+500, n))
		if err != nil {
			return 0, 0, err
		}
		for _, p := range present {
			if !p {
				lost++
			}
		}
	}
	// A write can reach the AOF and then the crash kills the reply. Those
	// show up as present-but-unacknowledged. That's fine (the client just
	// doesn't know it succeeded); count a few for the record.
	present, err := mget(n, n+5)
	if err != nil {
		return 0, 0, err
	}
	for _, p := range present {
		if p {
			phantom++
		}
	}
	return lost, phantom, nil
}

// simulatePowerLoss truncates the current incremental AOF file to the length
// the server recorded after its last successful fsync.
func simulatePowerLoss(dir string) error {
	aofDir := filepath.Join(dir, "appendonlydir")
	data, err := os.ReadFile(filepath.Join(aofDir, "shardis.aof.manifest"))
	if err != nil {
		return err
	}
	var current string
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) == 6 && f[5] == "i" {
			current = f[1] // the last incr file is the one being appended to
		}
	}
	synced := int64(0) // no record = nothing was ever fsynced
	if b, err := os.ReadFile(filepath.Join(aofDir, current+".synced")); err == nil {
		synced, _ = strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	}
	path := filepath.Join(aofDir, current)
	if st, err := os.Stat(path); err == nil && st.Size() > synced {
		return os.Truncate(path, synced)
	} else if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return nil
}

func start(bin string, args []string, port int) (*exec.Cmd, error) {
	cmd := exec.Command(bin, args...)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(30 * time.Second) // loading a big AOF takes a moment
	for time.Now().Before(deadline) {
		if c, err := net.Dial("tcp", fmt.Sprint("127.0.0.1:", port)); err == nil {
			c.Close()
			return cmd, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	cmd.Process.Kill()
	return nil, errors.New("server didn't start")
}

func freePort() int {
	for {
		p := 20000 + rand.IntN(20000)
		if ln, err := net.Listen("tcp", fmt.Sprint("127.0.0.1:", p)); err == nil {
			ln.Close()
			return p
		}
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
