// Package cluster holds the cluster topology (which node owns which of the
// 16384 hash slots, who replicates whom) and the cluster bus that keeps it
// up to date by gossip.
//
// A Topology is an immutable snapshot: the Cluster builds a new one whenever
// something changes and swaps it in atomically, so every command can read it
// without a lock (ADR 026). It can be seeded from a static file that is the
// same on every node (ADR 021), or built at runtime with CLUSTER MEET /
// ADDSLOTS / REPLICATE, the way Redis clusters are formed.
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
	"strconv"
	"strings"
	"time"

	"github.com/djism/shardis/internal/keyslot"
)

// Node is one member of the cluster, as of one snapshot.
type Node struct {
	Name        string // from the seed file; "" for nodes met at runtime
	ID          string // 40 hex chars
	Addr        string // client-facing host:port
	Internal    string // node-to-node host:port (replication)
	BusPort     int    // cluster bus port on the Internal host
	Primary     *Node  // nil for primaries
	Slots       []Range
	Replicas    []*Node
	Epoch       uint64 // config epoch: the version of this node's slot claims
	Flags       uint16 // Flag* bits
	PingSent    time.Time
	PongRecv    time.Time
	LinkUp      bool  // we have a working outbound bus connection to it
	ReplOffset  int64 // its replication offset, as last advertised
	FailReports int   // primaries currently reporting it unreachable
}

// Node flags.
const (
	FlagMyself uint16 = 1 << iota
	FlagPrimary
	FlagReplica
	FlagPFail     // we can't reach it (Iteration 9)
	FlagFail      // the majority agrees it's down (Iteration 9)
	FlagHandshake // met but not yet confirmed by a PONG
)

func (n *Node) IsPrimary() bool      { return n.Primary == nil && n.Flags&FlagReplica == 0 }
func (n *Node) Has(flag uint16) bool { return n.Flags&flag != 0 } // any of the given bits

// Label is the name if there is one, else a short ID (for logs and events).
func (n *Node) Label() string {
	if n.Name != "" {
		return n.Name
	}
	return n.ID[:8]
}

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
	Nodes        []*Node
	Me           *Node
	CurrentEpoch uint64
	owner        [keyslot.Count]*Node // slot → primary that serves it (nil = nobody)

	// Computed once per snapshot, so per-command checks are O(1).
	minority      bool // we can't reach a majority of the slot-serving primaries
	rejoining     bool // we just got a majority back and are waiting before serving
	stateOK       bool
	fullCoverage  bool
	pfailSlots    int
	failSlots     int
	assignedSlots int
}

func (t *Topology) computeState(fullCoverage bool) {
	t.fullCoverage = fullCoverage
	t.stateOK = true
	// Minority check (ADR 030): of the primaries that serve slots, how many
	// can we reach (not PFAIL/FAIL from our point of view, us included)?
	// Without a majority we're on the losing side of a partition: stop
	// serving, because the majority side may be electing our replacement.
	serving := map[*Node]bool{}
	for _, o := range t.owner {
		if o != nil {
			serving[o] = true
		}
	}
	reachable := 0
	for o := range serving {
		if o == t.Me || !o.Has(FlagPFail|FlagFail) {
			reachable++
		}
	}
	t.minority = len(serving) > 0 && reachable < len(serving)/2+1
	if t.minority {
		t.stateOK = false
	}
	for _, o := range t.owner {
		switch {
		case o == nil:
			t.stateOK = false
			continue
		case o.Has(FlagFail):
			t.stateOK = false
			t.failSlots++
		case o.Has(FlagPFail):
			t.pfailSlots++
		}
		t.assignedSlots++
	}
}

// Minority reports whether this node can't reach a majority of the
// slot-serving primaries; Rejoining whether it just regained one and is
// waiting before serving again.
func (t *Topology) Minority() bool  { return t.minority }
func (t *Topology) Rejoining() bool { return t.rejoining }

// SlotHealth reports slots whose owner is suspected (PFAIL) or failed (FAIL).
func (t *Topology) SlotHealth() (pfail, fail int) { return t.pfailSlots, t.failSlots }

// Serves reports whether slot s can be served right now. With full coverage
// required (the default), any unserved or failed slot makes the whole
// cluster refuse keyed commands; otherwise only that slot does.
func (t *Topology) Serves(s uint16) bool {
	if t.minority || t.rejoining {
		return false // on the minority side, nothing is served
	}
	if t.fullCoverage {
		return t.stateOK
	}
	o := t.owner[s]
	return o != nil && !o.Has(FlagFail)
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
		n := &Node{Name: f[1], ID: NodeID(f[1]), Addr: f[2], Internal: f[3], Flags: FlagPrimary}
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
		p.n.Flags = FlagReplica
		prim.Replicas = append(prim.Replicas, p.n)
	}
	for _, n := range t.Nodes {
		_, port, _ := net.SplitHostPort(n.Internal)
		p, _ := strconv.Atoi(port)
		n.BusPort = p + 10000
	}
	var epoch uint64
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
	t.Me.Flags |= FlagMyself
	t.CurrentEpoch = t.MaxEpoch()
	t.computeState(true)
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

// Owner is the primary that serves slot s, or nil if no node does.
func (t *Topology) Owner(s uint16) *Node { return t.owner[s] }

// StateOK is Redis's cluster_state:ok: every slot is served by a node that
// isn't marked as failed.
func (t *Topology) StateOK() bool { return t.stateOK }

// AssignedSlots counts slots with an owner.
func (t *Topology) AssignedSlots() int { return t.assignedSlots }

// ByID finds a node.
func (t *Topology) ByID(id string) *Node {
	for _, n := range t.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

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

// SlotRanges returns every contiguous owned range with its owner, sorted
// by slot, which is the shape CLUSTER SLOTS reports.
func (t *Topology) SlotRanges() []OwnedRange {
	var out []OwnedRange
	for s := 0; s < keyslot.Count; {
		o, start := t.owner[s], s
		for s < keyslot.Count && t.owner[s] == o {
			s++
		}
		if o != nil {
			out = append(out, OwnedRange{Range{uint16(start), uint16(s - 1)}, o})
		}
	}
	return out
}

// OwnedRange is a slot range and the primary serving it.
type OwnedRange struct {
	Range
	Owner *Node
}

// MaxEpoch is the highest config epoch any node has.
func (t *Topology) MaxEpoch() uint64 {
	var m uint64
	for _, n := range t.Nodes {
		m = max(m, n.Epoch)
	}
	return m
}
