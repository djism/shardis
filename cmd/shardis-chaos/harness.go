package main

import (
	"errors"
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

// harness runs a real cluster as separate processes: primaries a b c,
// replicas d e f (of a b c), and an empty node g for resharding. Every node
// has fault injection on and an AOF (everysec), so crashes and restarts
// behave like production, losses included.
type harness struct {
	bin, dir string
	topo     string // seed topology file
	mu       sync.Mutex
	nodes    map[string]*proc
	members  map[string]bool // part of the cluster (g only after add-node)
	rec      *recorder
}

type proc struct {
	name string
	port int
	args []string
	cmd  *exec.Cmd
}

var (
	primaryNames = []string{"a", "b", "c"}
	replicaOf    = map[string]string{"d": "a", "e": "b", "f": "c"}
	seedSlots    = map[string]string{"a": "0-5460", "b": "5461-10922", "c": "10923-16383"}
	allNames     = []string{"a", "b", "c", "d", "e", "f", "g"}
)

const nodeTimeoutMs = 2000

func newHarness(bin string, rec *recorder) (*harness, error) {
	dir, err := os.MkdirTemp("", "shardis-chaos-*")
	if err != nil {
		return nil, err
	}
	h := &harness{bin: bin, dir: dir, nodes: map[string]*proc{}, rec: rec,
		members: map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true, "f": true}}
	used := map[int]bool{}
	for _, n := range allNames {
		h.nodes[n] = &proc{name: n, port: freePort(used)}
	}
	var topo strings.Builder
	for _, n := range allNames[:6] {
		addr := h.addr(n)
		if r, ok := seedSlots[n]; ok {
			fmt.Fprintf(&topo, "node %s %s %s primary %s\n", n, addr, addr, r)
		} else {
			fmt.Fprintf(&topo, "node %s %s %s replica %s\n", n, addr, addr, replicaOf[n])
		}
	}
	h.topo = filepath.Join(dir, "topology.conf")
	if err := os.WriteFile(h.topo, []byte(topo.String()), 0o644); err != nil {
		return nil, err
	}
	for _, n := range allNames {
		p := h.nodes[n]
		nodeDir := filepath.Join(dir, n)
		os.MkdirAll(nodeDir, 0o755)
		p.args = []string{"--port", strconv.Itoa(p.port), "--dir", nodeDir,
			"--cluster-node-timeout", strconv.Itoa(nodeTimeoutMs), "--enable-fault-injection", "yes",
			"--appendonly", "yes", "--appendfsync", "everysec"}
		if n == "g" {
			p.args = append(p.args, "--cluster-enabled", "yes")
		} else {
			p.args = append(p.args, "--cluster-config", h.topo, "--cluster-node", n)
		}
	}
	return h, nil
}

// freePort picks a port whose cluster bus port (+10000) is free too.
func freePort(used map[int]bool) int {
	for {
		p := 21000 + rand.IntN(30000)
		if used[p] {
			continue
		}
		a, err1 := net.Listen("tcp", fmt.Sprint("127.0.0.1:", p))
		b, err2 := net.Listen("tcp", fmt.Sprint("127.0.0.1:", p+10000))
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

func (h *harness) addr(n string) string { return fmt.Sprint("127.0.0.1:", h.nodes[n].port) }

// startAll starts primaries first, then replicas, then g.
func (h *harness) startAll() error {
	for _, n := range allNames {
		if err := h.start(n); err != nil {
			return err
		}
	}
	for _, n := range []string{"d", "e", "f"} {
		if err := h.waitInfo(n, "master_link_status:up", 15*time.Second); err != nil {
			return err
		}
	}
	return h.waitOK("b", 15*time.Second)
}

func (h *harness) start(n string) error {
	p := h.nodes[n]
	cmd := exec.Command(h.bin, p.args...)
	cmd.Stdout, cmd.Stderr = nil, nil
	if err := cmd.Start(); err != nil {
		return err
	}
	h.mu.Lock()
	p.cmd = cmd
	h.mu.Unlock()
	deadline := time.Now().Add(30 * time.Second) // AOF replay can take a moment
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", h.addr(n), time.Second); err == nil {
			c.Close()
			h.rec.note("start", n)
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("node %s didn't start", n)
}

// kill is kill -9: no shutdown, no final fsync.
func (h *harness) kill(n string) {
	h.mu.Lock()
	p := h.nodes[n]
	cmd := p.cmd
	p.cmd = nil
	h.mu.Unlock()
	if cmd != nil {
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()
		h.rec.note("kill", n)
	}
}

func (h *harness) signal(n string, sig syscall.Signal, action string) {
	h.mu.Lock()
	cmd := h.nodes[n].cmd
	h.mu.Unlock()
	if cmd != nil {
		cmd.Process.Signal(sig)
		h.rec.note(action, n)
	}
}

func (h *harness) alive(n string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodes[n].cmd != nil
}

// all sends a command to every live cluster member (FAULT PARTITION / HEAL).
func (h *harness) all(args ...string) error {
	for _, n := range allNames {
		h.mu.Lock()
		member := h.members[n]
		h.mu.Unlock()
		if !member || !h.alive(n) {
			continue
		}
		if _, err := command(h.addr(n), args...); err != nil {
			return fmt.Errorf("%s on %s: %w", args[0], n, err)
		}
	}
	return nil
}

func (h *harness) close() {
	for _, n := range allNames {
		h.kill(n)
	}
	os.RemoveAll(h.dir)
}

func (h *harness) waitOK(n string, timeout time.Duration) error {
	return h.waitInfoCmd(n, []string{"CLUSTER", "INFO"}, "cluster_state:ok", timeout)
}

func (h *harness) waitInfo(n, want string, timeout time.Duration) error {
	return h.waitInfoCmd(n, []string{"INFO", "replication"}, want, timeout)
}

func (h *harness) waitInfoCmd(n string, cmd []string, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if s, err := command(h.addr(n), cmd...); err == nil && strings.Contains(s, want) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("node %s: %v never showed %q", n, cmd, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// command sends one command on a fresh connection; server errors become Go errors.
func command(addr string, args ...string) (string, error) {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	w, r := resp.NewWriter(c), resp.NewReader(c)
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	w.WriteCommand(b...)
	if err := w.Flush(); err != nil {
		return "", err
	}
	v, err := r.ReadValue()
	if err != nil {
		return "", err
	}
	if v.Kind == resp.Error {
		return "", errors.New(string(v.Str))
	}
	return string(v.Str), nil
}

func (h *harness) join(n string) {
	h.mu.Lock()
	h.members[n] = true
	h.mu.Unlock()
}
