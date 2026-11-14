// Package cluster holds the cluster topology: which node owns which of the
// 16384 hash slots, and who replicates whom.
//
// Iteration 7 reads it from a static file that is the same on every node
// (ADR 021). Iteration 8's gossip will start from this file and keep it up
// to date as nodes join, fail and take over slots.
//
// File format, one node per line ('#' starts a comment):
//
//	node <name> <client-addr> <internal-addr> primary <slots>...
//	node <name> <client-addr> <internal-addr> replica <primary-name>
//
// <slots> are ranges like 0-5460, or single slots. <client-addr> is what
// clients are told (in MOVED, CLUSTER SLOTS, ...); <internal-addr> is how
// nodes reach each other (replication). They differ in Docker, where
// containers talk over the Docker network but clients connect from the host
// (ADR 023).
package cluster

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/djism/shardis/internal/keyslot"
)

// Node is one member of the cluster.
type Node struct {
	Name     string
	ID       string // 40 hex chars, derived from Name, like a Redis node ID
	Addr     string // client-facing host:port
	Internal string // node-to-node host:port
	Primary  *Node  // nil for primaries
	Slots    []Range
	Replicas []*Node
	Epoch    int // config epoch; static for now (primaries numbered 1..N)
}

func (n *Node) IsPrimary() bool { return n.Primary == nil }

// HostPort splits the client address.
func (n *Node) HostPort() (string, int) {
	h, p, _ := net.SplitHostPort(n.Addr)
	port, _ := strconv.Atoi(p)
	return h, port
}

// Range is an inclusive slot range.
type Range struct{ Start, End uint16 }

// Topology is the whole cluster as seen by one node.
type Topology struct {
	Nodes []*Node // in file order
	Me    *Node
	owner [keyslot.Count]*Node // slot → primary that serves it
}

// NodeID derives a stable 40-hex-char ID from a node name.
func NodeID(name string) string {
	sum := sha1.Sum([]byte("shardis-node:" + name))
	return hex.EncodeToString(sum[:])
}

// Load reads a topology file; me is this node's name.
func Load(path, me string) (*Topology, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	t, err := Parse(f, me)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return t, nil
}

// Parse reads a topology and checks it: unique names, every slot owned by
// exactly one primary, replicas pointing at primaries, and me present.
func Parse(r io.Reader, me string) (*Topology, error) {
	t := &Topology{}
	byName := map[string]*Node{}
	type pending struct {
		n       *Node
		primary string
		line    int
	}
	var replicas []pending
	sc := bufio.NewScanner(r)
	for line := 1; sc.Scan(); line++ {
		text, _, _ := strings.Cut(sc.Text(), "#")
		f := strings.Fields(text)
		if len(f) == 0 {
			continue
		}
		if f[0] != "node" || len(f) < 6 {
			return nil, fmt.Errorf("line %d: want: node <name> <client-addr> <internal-addr> primary|replica ...", line)
		}
		n := &Node{Name: f[1], ID: NodeID(f[1]), Addr: f[2], Internal: f[3]}
		if byName[n.Name] != nil {
			return nil, fmt.Errorf("line %d: duplicate node %q", line, n.Name)
		}
		for _, a := range []string{n.Addr, n.Internal} {
			if _, _, err := net.SplitHostPort(a); err != nil {
				return nil, fmt.Errorf("line %d: bad address %q", line, a)
			}
		}
		switch f[4] {
		case "primary":
			for _, s := range f[5:] {
				rg, err := parseRange(s)
				if err != nil {
					return nil, fmt.Errorf("line %d: %w", line, err)
				}
				n.Slots = append(n.Slots, rg)
			}
		case "replica":
			if len(f) != 6 {
				return nil, fmt.Errorf("line %d: replica takes exactly one primary name", line)
			}
			replicas = append(replicas, pending{n, f[5], line})
		default:
			return nil, fmt.Errorf("line %d: role must be primary or replica, got %q", line, f[4])
		}
		byName[n.Name] = n
		t.Nodes = append(t.Nodes, n)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	for _, p := range replicas {
		prim := byName[p.primary]
		if prim == nil || !prim.IsPrimary() {
			return nil, fmt.Errorf("line %d: %q is not a primary", p.line, p.primary)
		}
		p.n.Primary = prim
		prim.Replicas = append(prim.Replicas, p.n)
	}
	epoch := 0
	for _, n := range t.Nodes {
		if !n.IsPrimary() {
			continue
		}
		epoch++
		n.Epoch = epoch
		for _, rg := range n.Slots {
			for s := int(rg.Start); s <= int(rg.End); s++ {
				if prev := t.owner[s]; prev != nil {
					return nil, fmt.Errorf("slot %d is owned by both %s and %s", s, prev.Name, n.Name)
				}
				t.owner[s] = n
			}
		}
	}
	for _, n := range t.Nodes {
		if !n.IsPrimary() {
			n.Epoch = n.Primary.Epoch
		}
	}
	for s, o := range t.owner {
		if o == nil {
			return nil, fmt.Errorf("slot %d has no owner (all 16384 slots must be assigned)", s)
		}
	}
	if t.Me = byName[me]; t.Me == nil {
		return nil, fmt.Errorf("this node (%q) is not in the topology", me)
	}
	return t, nil
}

func parseRange(s string) (Range, error) {
	lo, hi, isRange := strings.Cut(s, "-")
	if !isRange {
		hi = lo
	}
	a, err1 := strconv.Atoi(lo)
	b, err2 := strconv.Atoi(hi)
	if err1 != nil || err2 != nil || a < 0 || b >= keyslot.Count || a > b {
		return Range{}, fmt.Errorf("bad slot range %q", s)
	}
	return Range{uint16(a), uint16(b)}, nil
}

// Owner is the primary that serves slot s.
func (t *Topology) Owner(s uint16) *Node { return t.owner[s] }

// Primaries in file order.
func (t *Topology) Primaries() []*Node {
	var out []*Node
	for _, n := range t.Nodes {
		if n.IsPrimary() {
			out = append(out, n)
		}
	}
	return out
}

// SlotRanges returns every contiguous range with its owner, sorted by slot,
// which is the shape CLUSTER SLOTS and SHARDS report.
func (t *Topology) SlotRanges() []OwnedRange {
	var out []OwnedRange
	for s := 0; s < keyslot.Count; {
		o, start := t.owner[s], s
		for s < keyslot.Count && t.owner[s] == o {
			s++
		}
		out = append(out, OwnedRange{Range{uint16(start), uint16(s - 1)}, o})
	}
	return out
}

// OwnedRange is a slot range and the primary serving it.
type OwnedRange struct {
	Range
	Owner *Node
}

// MaxEpoch is the highest config epoch (CLUSTER INFO cluster_current_epoch).
func (t *Topology) MaxEpoch() int {
	return slices.MaxFunc(t.Nodes, func(a, b *Node) int { return a.Epoch - b.Epoch }).Epoch
}
