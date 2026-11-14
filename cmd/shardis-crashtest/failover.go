package main

import (
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/djism/shardis/internal/resp"
)

// Failover mode: the replication lost-write window (Iteration 6).
//
// A primary and a replica run as real processes. Writers send numbered SETs;
// the primary is killed with kill -9 mid-load; the replica is promoted
// (REPLICAOF NO ONE); we count acknowledged writes the new primary lacks.
//
// Variants:
//
//	plain          a write counts as acknowledged when SET returns OK
//	wait           after each SET, WAIT 1 1000; acknowledged only if WAIT returned 1
//	slowlink       plain, with a slow replication link: fault injection holds
//	               back each chunk sent (up to 64 KB) by 50 ms, capping the
//	               stream at ~1.3 MB/s. The primary writes faster than that, so
//	               the replica falls further and further behind, like a
//	               congested network.
//	slowlink+wait  both
type variant struct {
	name  string
	wait  bool
	lagMs int
}

var failoverVariants = map[string]variant{
	"plain":         {"plain", false, 0},
	"wait":          {"wait", true, 0},
	"slowlink":      {"slowlink", false, 50},
	"slowlink+wait": {"slowlink+wait", true, 50},
}

type failoverResult struct {
	load        time.Duration
	acked, lost int
	unconfirmed int // WAIT returned 0: the client knew it wasn't safe
}

func failoverRun(o options, v variant) (failoverResult, error) {
	pPort, rPort := freePort(), freePort()
	common := []string{"--repl-ping-ms", "100", "--repl-timeout-ms", "2000"}
	primary, err := start(o.bin, append([]string{"--port", strconv.Itoa(pPort), "--enable-fault-injection", "yes"}, common...), pPort)
	if err != nil {
		return failoverResult{}, err
	}
	defer func() { primary.Process.Kill(); primary.Wait() }()
	replica, err := start(o.bin, append([]string{"--port", strconv.Itoa(rPort), "--replicaof", fmt.Sprintf("127.0.0.1 %d", pPort)}, common...), rPort)
	if err != nil {
		return failoverResult{}, err
	}
	defer func() { replica.Process.Signal(syscall.SIGTERM); replica.Wait() }()
	if err := waitLinkUp(rPort); err != nil {
		return failoverResult{}, err
	}
	if v.lagMs > 0 {
		if _, err := command(pPort, "FAULT", "SET", "repl.stream", "delay", strconv.Itoa(v.lagMs)); err != nil {
			return failoverResult{}, err
		}
	}

	acked := make([][]int, o.writers) // per writer: sequence numbers acknowledged
	unconfirmed := make([]int, o.writers)
	var wg sync.WaitGroup
	for w := range o.writers {
		wg.Go(func() { acked[w], unconfirmed[w] = failoverWriter(pPort, w, v.wait) })
	}
	load := o.minLoad + time.Duration(rand.Int64N(int64(o.maxLoad-o.minLoad)+1))
	time.Sleep(load)
	primary.Process.Signal(syscall.SIGKILL) // the primary dies
	primary.Wait()
	wg.Wait()
	if _, err := command(rPort, "REPLICAOF", "NO", "ONE"); err != nil { // failover
		return failoverResult{}, err
	}

	res := failoverResult{load: load}
	for w := range o.writers {
		res.acked += len(acked[w])
		res.unconfirmed += unconfirmed[w]
		lost, err := countMissing(rPort, w, acked[w])
		if err != nil {
			return failoverResult{}, err
		}
		res.lost += lost
	}
	return res, nil
}

func failoverWriter(port, w int, wait bool) (acked []int, unconfirmed int) {
	conn, err := net.Dial("tcp", fmt.Sprint("127.0.0.1:", port))
	if err != nil {
		return nil, 0
	}
	defer conn.Close()
	wr, rd := resp.NewWriter(conn), resp.NewReader(conn)
	for n := 0; ; n++ {
		wr.WriteCommand([]byte("SET"), []byte(fmt.Sprintf("c:%d:%d", w, n)), []byte(strconv.Itoa(n)))
		if wait {
			wr.WriteCommand([]byte("WAIT"), []byte("1"), []byte("1000"))
		}
		if wr.Flush() != nil {
			return acked, unconfirmed
		}
		v, err := rd.ReadValue()
		if err != nil || string(v.Str) != "OK" {
			return acked, unconfirmed
		}
		if wait {
			v, err = rd.ReadValue()
			if err != nil {
				return acked, unconfirmed // primary died before answering WAIT: not confirmed
			}
			if v.Int < 1 {
				unconfirmed++ // the client was told: not replicated
				continue
			}
		}
		acked = append(acked, n)
	}
}

func countMissing(port, w int, seqs []int) (int, error) {
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
			args = append(args, []byte(fmt.Sprintf("c:%d:%d", w, n)))
		}
		wr.WriteCommand(args...)
		if err := wr.Flush(); err != nil {
			return 0, err
		}
		v, err := rd.ReadValue()
		if err != nil {
			return 0, err
		}
		for _, e := range v.Elems {
			if e.Null {
				lost++
			}
		}
	}
	return lost, nil
}

// command runs one command and returns its reply as text.
func command(port int, args ...string) (string, error) {
	conn, err := net.Dial("tcp", fmt.Sprint("127.0.0.1:", port))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	wr, rd := resp.NewWriter(conn), resp.NewReader(conn)
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	wr.WriteCommand(b...)
	if err := wr.Flush(); err != nil {
		return "", err
	}
	v, err := rd.ReadValue()
	if err != nil {
		return "", err
	}
	if v.Kind == resp.Error {
		return "", fmt.Errorf("%s: %s", args[0], v.Str)
	}
	return string(v.Str), nil
}

func waitLinkUp(port int) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := command(port, "INFO", "replication"); err == nil && strings.Contains(info, "master_link_status:up") {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("replica on %d never came up", port)
}

func runFailover(o options, variants []string) {
	fmt.Println("variant,run,load_s,acked,lost,lost_pct,acked_writes_per_s,unconfirmed_by_wait")
	for _, name := range variants {
		v, ok := failoverVariants[name]
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown variant %q\n", name)
			os.Exit(2)
		}
		for run := 1; run <= o.runs; run++ {
			r, err := failoverRun(o, v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s run %d: %v\n", name, run, err)
				os.Exit(1)
			}
			fmt.Printf("%s,%d,%.2f,%d,%d,%.3f,%.0f,%d\n", v.name, run, r.load.Seconds(), r.acked, r.lost,
				100*float64(r.lost)/float64(max(r.acked, 1)), float64(r.acked)/r.load.Seconds(), r.unconfirmed)
		}
	}
}
