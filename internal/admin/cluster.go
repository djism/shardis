package admin

import (
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/djism/shardis/internal/resp"
)

// conn is a plain RESP connection to one node.
type conn struct {
	c net.Conn
	w *resp.Writer
	r *resp.Reader
}

func dial(addr string) (*conn, error) {
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	return &conn{c, resp.NewWriter(c), resp.NewReader(c)}, nil
}

func (c *conn) close() { c.c.Close() }

// do sends a command; a server error is returned as a Go error.
func (c *conn) do(args ...string) (resp.Value, error) {
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	return c.doBytes(b...)
}

func (c *conn) doBytes(args ...[]byte) (resp.Value, error) {
	c.c.SetDeadline(time.Now().Add(30 * time.Second))
	c.w.WriteCommand(args...)
	if err := c.w.Flush(); err != nil {
		return resp.Value{}, err
	}
	v, err := c.r.ReadValue()
	if err != nil {
		return v, err
	}
	if v.Kind == resp.Error {
		return v, fmt.Errorf("%s: %s", args[0], v.Str)
	}
	return v, nil
}

// node is one line of CLUSTER NODES.
type node struct {
	id, addr  string
	flags     []string
	primaryID string
	slots     []int
	migrating map[int]string // slot → target ID (only on the reporting node's own line)
	importing map[int]string // slot → source ID
}

func (n *node) has(flag string) bool { return slices.Contains(n.flags, flag) }
func (n *node) primary() bool        { return n.has("master") }

func (n *node) short() string { return n.id[:8] + " " + n.addr }

// clusterNodes asks addr for CLUSTER NODES and parses it.
func clusterNodes(addr string) ([]*node, error) {
	c, err := dial(addr)
	if err != nil {
		return nil, err
	}
	defer c.close()
	v, err := c.do("CLUSTER", "NODES")
	if err != nil {
		return nil, err
	}
	var out []*node
	for _, line := range strings.Split(strings.TrimSpace(string(v.Str)), "\n") {
		f := strings.Fields(line)
		if len(f) < 8 {
			continue
		}
		hostPort, _, _ := strings.Cut(f[1], "@")
		n := &node{id: f[0], addr: hostPort, flags: strings.Split(f[2], ","), primaryID: f[3],
			migrating: map[int]string{}, importing: map[int]string{}}
		for _, s := range f[8:] {
			if strings.HasPrefix(s, "[") {
				body := strings.Trim(s, "[]")
				if sl, id, ok := strings.Cut(body, "->-"); ok {
					n.migrating[atoi(sl)] = id
				} else if sl, id, ok := strings.Cut(body, "-<-"); ok {
					n.importing[atoi(sl)] = id
				}
				continue
			}
			lo, hi, isRange := strings.Cut(s, "-")
			if !isRange {
				hi = lo
			}
			for x := atoi(lo); x <= atoi(hi); x++ {
				n.slots = append(n.slots, x)
			}
		}
		out = append(out, n)
	}
	return out, nil
}

func atoi(s string) int { n, _ := strconv.Atoi(s); return n }

// view is the cluster as every node reports it: the primaries (from the
// entry node's view) and each primary's own open-slot markers, which only
// appear on that node's own "myself" line.
type view struct {
	nodes     []*node
	byID      map[string]*node
	primaries []*node
	conns     map[string]*conn // one cached connection per node
}

func (v *view) conn(addr string) (*conn, error) {
	if c := v.conns[addr]; c != nil {
		return c, nil
	}
	c, err := dial(addr)
	if err == nil {
		v.conns[addr] = c
	}
	return c, err
}

func (v *view) drop(addr string) {
	if c := v.conns[addr]; c != nil {
		c.close()
		delete(v.conns, addr)
	}
}

func load(addr string) (*view, error) {
	nodes, err := clusterNodes(addr)
	if err != nil {
		return nil, err
	}
	v := &view{nodes: nodes, byID: map[string]*node{}, conns: map[string]*conn{}}
	for _, n := range nodes {
		v.byID[n.id] = n
		if n.primary() && !n.has("fail") && !n.has("handshake") {
			v.primaries = append(v.primaries, n)
		}
	}
	for _, p := range v.primaries {
		own, err := clusterNodes(p.addr)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p.short(), err)
		}
		for _, o := range own {
			if o.has("myself") {
				p.migrating, p.importing = o.migrating, o.importing
			}
		}
	}
	return v, nil
}

func (v *view) find(id string) (*node, error) {
	if n := v.byID[id]; n != nil {
		return n, nil
	}
	for _, n := range v.nodes {
		if strings.HasPrefix(n.id, id) && len(id) >= 6 {
			return n, nil
		}
	}
	return nil, fmt.Errorf("no node %q", id)
}

// PrintNodes writes who owns how many slots, and open slots, to w.
func PrintNodes(addr string, w io.Writer) error {
	v, err := load(addr)
	if err != nil {
		return err
	}
	for _, p := range v.primaries {
		replicas := 0
		for _, n := range v.nodes {
			if n.primaryID == p.id {
				replicas++
			}
		}
		fmt.Fprintf(w, "%s  %5d slots  %d replica(s)", p.short(), len(p.slots), replicas)
		for s, id := range p.migrating {
			fmt.Fprintf(w, "  [%d -> %s]", s, id[:8])
		}
		for s, id := range p.importing {
			fmt.Fprintf(w, "  [%d <- %s]", s, id[:8])
		}
		fmt.Fprintln(w)
	}
	return nil
}

// AddNode introduces a new (empty) node to the cluster: the new node MEETs
// an existing one, and gossip introduces it to everyone else. It returns
// once every node knows the newcomer, with the newcomer's ID. replicaOf may
// be a primary's node ID or its address (host:port).
func AddNode(existing, newAddr, replicaOf string) (string, error) {
	if newAddr == "" {
		return "", fmt.Errorf("-new is required")
	}
	host, port, err := net.SplitHostPort(existing)
	if err != nil {
		return "", err
	}
	newHost, _, _ := net.SplitHostPort(newAddr)
	if isLoopback(host) && !isLoopback(newHost) {
		// The new node is told to MEET this address. From its side,
		// localhost is itself.
		return "", fmt.Errorf("-addr %s is a loopback address, but the new node (%s) must be able to reach it: use the node's real name or IP", existing, newAddr)
	}
	c, err := dial(newAddr)
	if err != nil {
		return "", err
	}
	defer c.close()
	idv, err := c.do("CLUSTER", "MYID")
	if err != nil {
		return "", err
	}
	newID := string(idv.Str)
	if _, err := c.do("CLUSTER", "MEET", host, port); err != nil {
		return "", err
	}
	// Wait until *every* node knows the new one, not just the one we met:
	// the others hear about it by gossip, a moment later. A reshard started
	// before then fails ("I don't know about node ...") on whichever source
	// hasn't heard yet.
	deadline := time.Now().Add(30 * time.Second)
	for !everyoneKnows(existing, newID) {
		if time.Now().After(deadline) {
			return "", fmt.Errorf("the cluster never fully learned about %s", newAddr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if replicaOf != "" {
		if strings.Contains(replicaOf, ":") { // an address: look up its node ID
			pc, err := dial(replicaOf)
			if err != nil {
				return "", err
			}
			v, err := pc.do("CLUSTER", "MYID")
			pc.close()
			if err != nil {
				return "", err
			}
			replicaOf = string(v.Str)
		}
		if _, err := c.do("CLUSTER", "REPLICATE", replicaOf); err != nil {
			return "", err
		}
	}
	return newID, nil
}

// everyoneKnows reports whether every node reachable from addr lists id as
// a confirmed (non-handshake) member.
func everyoneKnows(addr, id string) bool {
	nodes, err := clusterNodes(addr)
	if err != nil {
		return false
	}
	for _, n := range nodes {
		if n.has("fail") || n.has("handshake") {
			continue
		}
		view, err := clusterNodes(n.addr)
		if err != nil || !slices.ContainsFunc(view, func(m *node) bool { return m.id == id && !m.has("handshake") }) {
			return false
		}
	}
	return slices.ContainsFunc(nodes, func(n *node) bool { return n.id == id })
}

// waitAgreement waits until every primary knows every other primary, the
// precondition for moving slots between any two of them.
func (v *view) waitAgreement(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		missing := ""
		for _, p := range v.primaries {
			known, err := clusterNodes(p.addr)
			if err != nil {
				return err
			}
			for _, q := range v.primaries {
				if !slices.ContainsFunc(known, func(m *node) bool { return m.id == q.id && !m.has("handshake") }) {
					missing = p.short() + " doesn't know " + q.short() + " yet"
				}
			}
		}
		if missing == "" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cluster not in agreement: %s", missing)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// RemoveNode makes every other node forget an empty node. Stop its process
// right after: a node that keeps running would keep trying to talk to the
// others.
func RemoveNode(addr, id string) error {
	v, err := load(addr)
	if err != nil {
		return err
	}
	gone, err := v.find(id)
	if err != nil {
		return err
	}
	if len(gone.slots) > 0 {
		return fmt.Errorf("%s still owns %d slots: reshard them away first", gone.short(), len(gone.slots))
	}
	for _, n := range v.nodes {
		if n == gone {
			continue
		}
		if n.primaryID == gone.id {
			return fmt.Errorf("%s is a replica of the node being removed: remove or re-point it first", n.short())
		}
	}
	for _, n := range v.nodes {
		if n == gone || n.has("fail") {
			continue
		}
		c, err := dial(n.addr)
		if err != nil {
			return err
		}
		_, err = c.do("CLUSTER", "FORGET", gone.id)
		c.close()
		if err != nil {
			return fmt.Errorf("%s: %w", n.short(), err)
		}
	}
	return nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
