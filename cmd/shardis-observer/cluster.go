package main

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/djism/shardis/internal/resp"
)

// Node is one cluster member as the website sees it.
type Node struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"` // from the topology file if given, else the address
	Addr    string   `json:"addr"`
	Role    string   `json:"role"` // primary | replica
	Primary string   `json:"primary,omitempty"`
	Slots   [][2]int `json:"slots,omitempty"`
	Flags   []string `json:"flags,omitempty"` // fail?, fail, handshake, myself is dropped
	Epoch   int64    `json:"epoch"`
	Up      bool     `json:"up"` // the observer reached it in the last round
	Keys    int64    `json:"keys"`
	Ops     float64  `json:"ops"` // commands per second since the last snapshot (from INFO)
}

// observer keeps the current topology and follows every node's events.
type observer struct {
	seeds  []string
	names  map[string]string // addr → name
	hub    *hub
	mu     sync.Mutex
	nodes  []Node
	follow map[string]bool // addrs we already stream events from
	stop   chan struct{}
	prev   map[string]opsSample // addr → last total_commands_processed
}

type opsSample struct {
	total int64
	at    time.Time
}

// loadNames reads a seed topology file (deploy/local/topology.conf) for
// friendly node names. Optional.
func loadNames(path string) map[string]string {
	names := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return names
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fl := strings.Fields(sc.Text())
		if len(fl) >= 3 && fl[0] == "node" {
			names[fl[2]] = fl[1]
		}
	}
	return names
}

func (o *observer) name(addr string) string {
	if n, ok := o.names[addr]; ok {
		return n
	}
	return addr
}

// run refreshes the topology every second and broadcasts it.
func (o *observer) run() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		o.refresh()
		select {
		case <-o.stop:
			return
		case <-tick.C:
		}
	}
}

func (o *observer) refresh() {
	var nodes []Node
	o.mu.Lock()
	candidates := append([]string(nil), o.seeds...)
	for _, n := range o.nodes {
		candidates = append(candidates, n.Addr)
	}
	o.mu.Unlock()
	for _, a := range candidates {
		if v, err := command(a, "CLUSTER", "NODES"); err == nil {
			nodes = parseNodes(string(v.Str), o)
			break
		}
	}
	if nodes == nil {
		return
	}
	for i := range nodes {
		if v, err := command(nodes[i].Addr, "DBSIZE"); err == nil {
			nodes[i].Keys, nodes[i].Up = v.Int, true
		}
		if v, err := command(nodes[i].Addr, "INFO", "stats"); err == nil {
			nodes[i].Ops = o.opsRate(nodes[i].Addr, string(v.Str))
		}
		o.mu.Lock()
		if !o.follow[nodes[i].Addr] {
			o.follow[nodes[i].Addr] = true
			go o.events(nodes[i].Addr)
		}
		o.mu.Unlock()
	}
	o.mu.Lock()
	o.nodes = nodes
	o.mu.Unlock()
	o.hub.broadcast(map[string]any{"kind": "topology", "t": time.Now().UnixMilli(), "nodes": nodes})
}

func (o *observer) topology() []Node {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]Node(nil), o.nodes...)
}

func parseNodes(text string, o *observer) []Node {
	byID := map[string]string{}
	var out []Node
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		f := strings.Fields(line)
		if len(f) < 8 {
			continue
		}
		addr, _, _ := strings.Cut(f[1], "@")
		n := Node{ID: f[0], Addr: addr, Name: o.name(addr), Role: "primary"}
		for _, fl := range strings.Split(f[2], ",") {
			switch fl {
			case "slave":
				n.Role = "replica"
			case "master", "myself", "noflags":
			default:
				n.Flags = append(n.Flags, fl)
			}
		}
		n.Epoch, _ = strconv.ParseInt(f[6], 10, 64)
		if f[3] != "-" {
			n.Primary = f[3]
		}
		for _, s := range f[8:] {
			if strings.HasPrefix(s, "[") {
				continue
			}
			lo, hi, isRange := strings.Cut(s, "-")
			if !isRange {
				hi = lo
			}
			a, _ := strconv.Atoi(lo)
			b, _ := strconv.Atoi(hi)
			n.Slots = append(n.Slots, [2]int{a, b})
		}
		byID[n.ID] = n.Name
		out = append(out, n)
	}
	for i := range out { // primary by name, which is what the website shows
		if p, ok := byID[out[i].Primary]; ok {
			out[i].Primary = p
		}
	}
	return out
}

// events streams one node's EVENTS (ADR 041) to the hub, reconnecting
// after the node restarts, until the observer stops.
func (o *observer) events(addr string) {
	name := o.name(addr)
	for {
		select {
		case <-o.stop:
			return
		default:
		}
		o.streamOnce(addr, name)
		select {
		case <-o.stop:
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (o *observer) streamOnce(addr, name string) {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return
	}
	defer c.Close()
	go func() { <-o.stop; c.Close() }()
	w, r := resp.NewWriter(c), resp.NewReader(c)
	w.WriteCommand([]byte("EVENTS"))
	if w.Flush() != nil {
		return
	}
	if v, err := r.ReadValue(); err != nil || string(v.Str) != "OK" {
		return
	}
	for {
		v, err := r.ReadValue()
		if err != nil {
			return
		}
		var e struct {
			TS   time.Time      `json:"ts"`
			Type string         `json:"type"`
			Data map[string]any `json:"data"`
		}
		if json.Unmarshal(v.Str, &e) != nil {
			continue
		}
		o.hub.broadcast(map[string]any{"kind": "event", "t": e.TS.UnixMilli(), "node": name, "type": e.Type, "data": e.Data})
	}
}

// command sends one command on a fresh connection.
func command(addr string, args ...string) (resp.Value, error) {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return resp.Value{}, err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	w, r := resp.NewWriter(c), resp.NewReader(c)
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	w.WriteCommand(b...)
	if err := w.Flush(); err != nil {
		return resp.Value{}, err
	}
	v, err := r.ReadValue()
	if err == nil && v.Kind == resp.Error {
		return v, errString(string(v.Str))
	}
	return v, err
}

type errString string

func (e errString) Error() string { return string(e) }

// opsRate turns INFO's total_commands_processed into commands per second
// since the previous snapshot. Our own DBSIZE/INFO polls are 2 commands a
// second; small enough to ignore.
func (o *observer) opsRate(addr, info string) float64 {
	var total int64
	for _, line := range strings.Split(info, "\r\n") {
		if v, ok := strings.CutPrefix(line, "total_commands_processed:"); ok {
			total, _ = strconv.ParseInt(v, 10, 64)
		}
	}
	now := time.Now()
	o.mu.Lock()
	defer o.mu.Unlock()
	p, seen := o.prev[addr]
	o.prev[addr] = opsSample{total, now}
	if !seen || total < p.total {
		return 0 // first sample, or the node restarted
	}
	return float64(total-p.total) / now.Sub(p.at).Seconds()
}
