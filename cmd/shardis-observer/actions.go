package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/djism/shardis/internal/admin"
)

// Admin actions the website can trigger: local/live mode only (ADR 046).
// They kill and pause real processes, so they're off unless the observer
// was started with -local-actions, and even then they're refused if any
// cluster node isn't on a loopback address: the website must never be able
// to kill pods in a shared Kubernetes cluster.
type actions struct {
	enabled bool
	pidDir  string // deploy/local/cluster.sh writes <name>.pid here
	script  string // deploy/local/cluster.sh (restarts stopped nodes)
	o       *observer
}

type actionReq struct {
	Action string   `json:"action"` // kill, pause, resume, restart, partition, heal, add-node, rebalance
	Node   string   `json:"node,omitempty"`
	Nodes  []string `json:"nodes,omitempty"` // partition: the side to cut off
	Addr   string   `json:"addr,omitempty"`  // add-node: the empty node's address
}

func (a *actions) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !a.enabled {
		http.Error(w, "admin actions are disabled (start the observer with -local-actions, against a local cluster)", http.StatusForbidden)
		return
	}
	if err := a.allLocal(); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	var req actionReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		http.Error(w, "bad JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	err := a.do(req)
	result := map[string]any{"kind": "action", "t": time.Now().UnixMilli(), "action": req.Action, "node": req.Node,
		"nodes": req.Nodes, "ok": err == nil}
	if err != nil {
		result["error"] = err.Error()
	}
	a.o.hub.broadcast(result) // the website's timeline shows what was done
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}
	json.NewEncoder(w).Encode(result)
}

// allLocal refuses actions unless every known node is on loopback.
func (a *actions) allLocal() error {
	nodes := a.o.topology()
	if len(nodes) == 0 {
		return errors.New("no cluster discovered yet")
	}
	for _, n := range nodes {
		host, _, _ := net.SplitHostPort(n.Addr)
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return fmt.Errorf("refusing admin actions: node %s is not on this machine", n.Addr)
		}
	}
	return nil
}

func (a *actions) do(req actionReq) error {
	seed := a.o.seeds[0]
	switch req.Action {
	case "kill":
		return a.signal(req.Node, syscall.SIGKILL)
	case "pause":
		return a.signal(req.Node, syscall.SIGSTOP)
	case "resume":
		return a.signal(req.Node, syscall.SIGCONT)
	case "restart":
		out, err := exec.Command(a.script, "up").CombinedOutput()
		if err != nil {
			return fmt.Errorf("%v: %s", err, out)
		}
		return nil
	case "partition":
		if len(req.Nodes) == 0 {
			return errors.New("partition needs nodes")
		}
		return a.all(append([]string{"FAULT", "PARTITION"}, req.Nodes...)...)
	case "heal":
		return a.all("FAULT", "HEAL")
	case "add-node":
		if req.Addr == "" {
			return errors.New("add-node needs addr")
		}
		_, err := admin.AddNode(seed, req.Addr, "")
		return err
	case "rebalance":
		return admin.Rebalance(seed, admin.MoveOptions{})
	}
	return fmt.Errorf("unknown action %q", req.Action)
}

func (a *actions) signal(name string, sig syscall.Signal) error {
	if name == "" || strings.ContainsAny(name, "/.") {
		return errors.New("bad node name")
	}
	b, err := os.ReadFile(filepath.Join(a.pidDir, name+".pid"))
	if err != nil {
		return fmt.Errorf("no pid file for %q (is this the local cluster from deploy/local/cluster.sh?)", name)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return err
	}
	return syscall.Kill(pid, sig)
}

// all sends a command to every reachable node (FAULT needs every node:
// each decides which side it's on). Nodes need --enable-fault-injection.
func (a *actions) all(args ...string) error {
	var errs []string
	for _, n := range a.o.topology() {
		if _, err := command(n.Addr, args...); err != nil {
			errs = append(errs, n.Name+": "+err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}
