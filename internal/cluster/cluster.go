package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/faults"
	"github.com/djism/shardis/internal/keyslot"
)

// Replicator is what the cluster needs from replication. *replication.Node
// implements it.
type Replicator interface {
	// ReplicaOf starts following a primary (seed file, CLUSTER REPLICATE,
	// or a failover that moved our primary's slots elsewhere).
	ReplicaOf(host string, port int) error
	// Promote stops replicating and makes this node a primary (it won a failover).
	Promote()
	// Offset is our replication offset, advertised in every bus header.
	Offset() int64
	// LinkDownFor is how long we've been cut off from our primary (0 = connected).
	LinkDownFor() time.Duration
}

// Options configures a Cluster.
type Options struct {
	// AllowPartialCoverage: by default (false, like Redis's
	// cluster-require-full-coverage yes), if any slot is unserved or its owner
	// is FAIL, every keyed command gets CLUSTERDOWN. true: only the affected
	// slots fail and the rest of the cluster keeps serving.
	AllowPartialCoverage bool
	ClientAddr           string    // how clients reach this node (MOVED, CLUSTER SLOTS)
	InternalAddr         string    // how other nodes reach its client port (replication)
	BusPort              int       // cluster bus port; 0 = internal port + 10000
	Seed                 *Topology // static topology to start from (optional)
	StateFile            string    // nodes.conf path; "" = don't persist
	NodeTimeout          time.Duration
	Replicator           Replicator
	Faults               *faults.Registry
	Bus                  *events.Bus
	Log                  *slog.Logger
}

// node is the mutable state behind a Node snapshot. Guarded by Cluster.mu.
type node struct {
	id, name     string
	clientAddr   string
	internalAddr string
	busPort      int
	flags        uint16
	primaryID    string
	configEpoch  uint64
	pingSent     time.Time // zero = no ping waiting for a PONG
	pongRecv     time.Time
	created      time.Time
	link         *link
	dialing      bool
	meet         bool // send MEET instead of PING on the first message (we introduced it)

	// Failure detection and failover (failover.go).
	replOffset  int64                // its replication offset, from its last header
	failReports map[string]time.Time // primary ID → when it last told us "I can't reach this node"
	failTime    time.Time            // when we marked it FAIL
	votedTime   time.Time            // when we last voted for one of its replicas (it being the failed primary)
}

func (n *node) busAddr() string {
	host, _, _ := net.SplitHostPort(n.internalAddr)
	return net.JoinHostPort(host, strconv.Itoa(n.busPort))
}

func (n *node) label() string {
	if n.name != "" {
		return n.name
	}
	return n.id[:8]
}

// Cluster is this node's view of the cluster and its bus.
type Cluster struct {
	opts Options
	log  *slog.Logger

	mu            sync.Mutex
	myself        *node
	nodes         map[string]*node
	owner         [keyslot.Count]*node
	migrating     map[uint16]*node // slot → node we're moving it to (ADR 032)
	importing     map[uint16]*node // slot → node we're taking it from
	currentEpoch  uint64
	forgotten     map[string]time.Time // CLUSTER FORGET'd: ignore gossip about them until then
	lastVoteEpoch uint64               // the epoch we last voted in: at most one vote per epoch, saved in nodes.conf
	wasMinority   bool                 // we were cut off from the majority (rejoin delay pending)
	rejoinAt      time.Time            // when we may serve again after regaining a majority
	election      election             // our own run for primary, when our primary has failed
	dirty         bool                 // nodes.conf needs saving
	rng           *mrand.Rand

	view atomic.Pointer[Topology]

	ln      net.Listener
	inbound map[net.Conn]struct{}
	quit    chan struct{}
	wg      sync.WaitGroup

	stats Stats
}

// Stats are bus message counters (CLUSTER INFO, benchmarks).
type Stats struct {
	Sent, Received, BytesSent, BytesReceived, Dropped atomic.Int64
	SentByType, ReceivedByType                        [numMsgTypes]atomic.Int64
}

func randomID() string {
	b := make([]byte, 20)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// New builds the cluster state: from nodes.conf if this node has run before
// (so it keeps its identity), else from the seed file, else as a fresh node
// that knows only itself. Call Start to open the bus.
func New(opts Options) (*Cluster, error) {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.NodeTimeout <= 0 {
		opts.NodeTimeout = 15 * time.Second
	}
	if opts.BusPort == 0 {
		_, p, err := net.SplitHostPort(opts.InternalAddr)
		if err != nil {
			return nil, fmt.Errorf("internal address %q: %w", opts.InternalAddr, err)
		}
		port, _ := strconv.Atoi(p)
		opts.BusPort = port + 10000
	}
	c := &Cluster{
		opts: opts, log: opts.Log, nodes: map[string]*node{}, forgotten: map[string]time.Time{},
		migrating: map[uint16]*node{}, importing: map[uint16]*node{},
		rng: mrand.New(mrand.NewPCG(mrand.Uint64(), mrand.Uint64())), inbound: map[net.Conn]struct{}{},
		quit: make(chan struct{}),
	}
	loaded, err := c.loadState()
	if err != nil {
		return nil, err
	}
	switch {
	case loaded:
		c.log.Info("cluster state loaded", "file", opts.StateFile, "id", c.myself.id, "nodes", len(c.nodes))
	case opts.Seed != nil:
		c.seed(opts.Seed)
	default:
		c.myself = &node{id: randomID(), flags: FlagPrimary, created: time.Now()}
		c.nodes[c.myself.id] = c.myself
	}
	// Our own addresses come from the command line, not from old state: an
	// IP can change across restarts (a rescheduled pod).
	c.myself.flags |= FlagMyself
	c.myself.clientAddr, c.myself.internalAddr, c.myself.busPort = opts.ClientAddr, opts.InternalAddr, opts.BusPort
	c.dirty = true
	c.publish()
	return c, nil
}

func (c *Cluster) seed(t *Topology) {
	byID := map[string]*node{}
	for _, sn := range t.Nodes {
		n := &node{id: sn.ID, name: sn.Name, clientAddr: sn.Addr, internalAddr: sn.Internal, busPort: sn.BusPort,
			flags: sn.Flags &^ FlagMyself, configEpoch: sn.Epoch, created: time.Now()}
		if sn.Primary != nil {
			n.primaryID = sn.Primary.ID
		}
		byID[n.id] = n
		c.nodes[n.id] = n
	}
	for s := range keyslot.Count {
		if o := t.Owner(uint16(s)); o != nil {
			c.owner[s] = byID[o.ID]
		}
	}
	c.myself = byID[t.Me.ID]
	c.currentEpoch = t.CurrentEpoch
}

// View is the current topology snapshot. Lock-free; never modify it.
func (c *Cluster) View() *Topology { return c.view.Load() }

// Stats returns the bus counters.
func (c *Cluster) Stats() *Stats { return &c.stats }

// publish builds a new immutable snapshot from the mutable state and swaps
// it in (ADR 026). Called with c.mu held after every change. Readers holding
// the old snapshot keep a consistent (if slightly stale) picture.
func (c *Cluster) publish() {
	t := &Topology{CurrentEpoch: c.currentEpoch}
	snap := map[*node]*Node{}
	ids := make([]string, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	slices.Sort(ids) // stable order for CLUSTER NODES
	for _, id := range ids {
		n := c.nodes[id]
		sn := &Node{Name: n.name, ID: n.id, Addr: n.clientAddr, Internal: n.internalAddr, BusPort: n.busPort,
			Epoch: n.configEpoch, Flags: n.flags, PingSent: n.pingSent, PongRecv: n.pongRecv,
			LinkUp: n.link != nil || n == c.myself, ReplOffset: n.replOffset, FailReports: len(n.failReports)}
		snap[n] = sn
		t.Nodes = append(t.Nodes, sn)
	}
	for n, sn := range snap {
		if p := c.nodes[n.primaryID]; p != nil && n.flags&FlagReplica != 0 {
			sn.Primary = snap[p]
			snap[p].Replicas = append(snap[p].Replicas, sn)
		}
	}
	for s := 0; s < keyslot.Count; {
		o, start := c.owner[s], s
		for s < keyslot.Count && c.owner[s] == o {
			t.owner[s] = snap[o]
			s++
		}
		if o != nil {
			snap[o].Slots = append(snap[o].Slots, Range{uint16(start), uint16(s - 1)})
		}
	}
	t.Me = snap[c.myself]
	if len(c.migrating) > 0 || len(c.importing) > 0 {
		t.migrating, t.importing = map[uint16]*Node{}, map[uint16]*Node{}
		for s, n := range c.migrating {
			t.migrating[s] = snap[n]
		}
		for s, n := range c.importing {
			t.importing[s] = snap[n]
		}
	}
	t.computeState(!c.opts.AllowPartialCoverage)
	c.applyRejoinDelay(t, time.Now())
	c.view.Store(t)
}

func (c *Cluster) emit(typ string, data map[string]any) {
	if c.opts.Bus.Active() {
		c.opts.Bus.Emit(typ, data)
	}
}

// ---- Runtime commands (CLUSTER MEET / ADDSLOTS / REPLICATE / FORGET / SET-CONFIG-EPOCH) ----

// Meet introduces another node at host:port (its internal address; the bus
// is busPort, default port + 10000). We don't know its ID yet: it's a
// handshake node until its PONG tells us who it is.
func (c *Cluster) Meet(host string, port, busPort int) error {
	if busPort == 0 {
		busPort = port + 10000
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		if n.internalAddr == addr && n.busPort == busPort {
			return nil // already known (or already meeting)
		}
	}
	n := &node{id: randomID(), clientAddr: addr, internalAddr: addr, busPort: busPort,
		flags: FlagHandshake | FlagPrimary, created: time.Now(), meet: true}
	c.nodes[n.id] = n
	c.publish()
	c.log.Info("cluster meet", "addr", addr)
	return nil
}

// AddSlots makes this node the owner of the given (currently unowned) slots.
func (c *Cluster) AddSlots(slots []uint16) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.myself.flags&FlagReplica != 0 {
		return errors.New("a replica can't own slots")
	}
	for _, s := range slots {
		if c.owner[s] != nil {
			return fmt.Errorf("Slot %d is already busy", s)
		}
	}
	for _, s := range slots {
		c.owner[s] = c.myself
	}
	c.dirty = true
	c.publish()
	c.emit("slots_assigned", map[string]any{"node": c.myself.label(), "count": len(slots)})
	return nil
}

// Replicate makes this node a replica of the node with the given ID.
func (c *Cluster) Replicate(id string) error {
	c.mu.Lock()
	target := c.nodes[id]
	switch {
	case target == nil:
		c.mu.Unlock()
		return fmt.Errorf("Unknown node %s", id)
	case target == c.myself:
		c.mu.Unlock()
		return errors.New("Can't replicate myself")
	case target.flags&FlagReplica != 0:
		c.mu.Unlock()
		return errors.New("I can only replicate a master, not a replica.")
	}
	for _, o := range c.owner {
		if o == c.myself {
			c.mu.Unlock()
			return errors.New("To set a master the node must be empty and without assigned slots.")
		}
	}
	c.myself.flags = c.myself.flags&^FlagPrimary | FlagReplica
	c.myself.primaryID = id
	c.dirty = true
	c.publish()
	internal := target.internalAddr
	c.mu.Unlock()
	c.emit("role_change", map[string]any{"node": c.myself.label(), "role": "replica", "primary": target.label()})
	return c.startReplication(internal)
}

func (c *Cluster) startReplication(internal string) error {
	if c.opts.Replicator == nil {
		return nil
	}
	host, port, err := net.SplitHostPort(internal)
	if err != nil {
		return err
	}
	p, _ := strconv.Atoi(port)
	return c.opts.Replicator.ReplicaOf(host, p)
}

// Forget removes a node from our view, and ignores gossip about it for a
// minute; otherwise the next PING that mentions it would bring it back.
func (c *Cluster) Forget(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.nodes[id]
	switch {
	case n == nil:
		return fmt.Errorf("Unknown node %s", id)
	case n == c.myself:
		return errors.New("I tried hard but I can't forget myself...")
	case c.myself.primaryID == id:
		return errors.New("Can't forget my master!")
	}
	c.removeNode(n)
	c.forgotten[id] = time.Now().Add(time.Minute)
	c.dirty = true
	c.publish()
	return nil
}

func (c *Cluster) removeNode(n *node) {
	delete(c.nodes, n.id)
	for s, o := range c.owner {
		if o == n {
			c.owner[s] = nil
		}
	}
	for s, o := range c.migrating {
		if o == n {
			delete(c.migrating, s)
		}
	}
	for s, o := range c.importing {
		if o == n {
			delete(c.importing, s)
		}
	}
	if n.link != nil {
		n.link.close()
	}
}

// SetConfigEpoch is used once, when a cluster is first created, to give
// every primary a distinct epoch (redis-cli --cluster create does this).
func (c *Cluster) SetConfigEpoch(e uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.nodes) > 1 {
		return errors.New("The user can assign a config epoch only when the node does not know any other node.")
	}
	if c.myself.configEpoch != 0 {
		return errors.New("Node config epoch is already non-zero")
	}
	c.myself.configEpoch = e
	c.currentEpoch = max(c.currentEpoch, e)
	c.dirty = true
	c.publish()
	return nil
}

// sampleGossip picks the nodes a message will tell its receiver about:
// about a tenth of the cluster, at least 3, never the receiver or ourselves,
// and not nodes still in handshake. Called with c.mu held.
func (c *Cluster) sampleGossip(to *node) []gossipEntry {
	var pool []*node
	for _, n := range c.nodes {
		if n != c.myself && n != to && n.flags&FlagHandshake == 0 {
			pool = append(pool, n)
		}
	}
	want := min(max(3, len(c.nodes)/10), len(pool))
	c.rng.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	now := time.Now()
	out := make([]gossipEntry, 0, want)
	for _, n := range pool[:want] {
		g := gossipEntry{id: n.id, flags: wireFlags(n.flags), busPort: uint16(n.busPort),
			clientAddr: n.clientAddr, internalAddr: n.internalAddr}
		if !n.pingSent.IsZero() {
			g.pingSentAgo = uint32(now.Sub(n.pingSent).Milliseconds())
		}
		if !n.pongRecv.IsZero() {
			g.pongRecvAgo = uint32(now.Sub(n.pongRecv).Milliseconds())
		}
		out = append(out, g)
	}
	return out
}

func wireFlags(f uint16) uint16 {
	var w uint16
	if f&FlagPrimary != 0 {
		w |= wirePrimary
	}
	if f&FlagReplica != 0 {
		w |= wireReplica
	}
	if f&FlagPFail != 0 {
		w |= wirePFail
	}
	if f&FlagFail != 0 {
		w |= wireFail
	}
	return w
}

// buildMessage creates a PING/PONG/MEET from us to `to`. Called with c.mu held.
func (c *Cluster) buildMessage(typ uint8, to *node) *message {
	me := c.myself
	m := &message{typ: typ, sender: me.id, currentEpoch: c.currentEpoch, configEpoch: me.configEpoch,
		flags: wireFlags(me.flags), primaryID: me.primaryID, busPort: uint16(me.busPort),
		clientAddr: me.clientAddr, internalAddr: me.internalAddr, offset: uint64(max(c.myOffset(), 0))}
	if me.flags&FlagReplica != 0 {
		// Like Redis, a replica advertises its primary's config epoch.
		if p := c.nodes[me.primaryID]; p != nil {
			m.configEpoch = p.configEpoch
		}
	} else {
		for s, o := range c.owner {
			if o == me {
				m.setSlot(s)
			}
		}
	}
	m.gossip = c.sampleGossip(to)
	return m
}

// rejoinDelay is how long a node that was in a minority waits after
// regaining a majority before serving again (Redis: node timeout, clamped to
// 0.5-5 s). It gives gossip time to tell it "your slots belong to someone
// else now" before it accepts a write it would later throw away.
func (c *Cluster) rejoinDelay() time.Duration {
	return min(max(c.opts.NodeTimeout, 500*time.Millisecond), 5*time.Second)
}

// applyRejoinDelay holds a node that just left the minority in a
// "rejoining" state for rejoinDelay. Called with c.mu held, from publish.
func (c *Cluster) applyRejoinDelay(t *Topology, now time.Time) {
	switch {
	case t.minority:
		if !c.wasMinority {
			c.log.Warn("minority partition: can't reach a majority of primaries; refusing commands")
			c.emit("minority", map[string]any{"node": c.myself.label()})
		}
		c.wasMinority, c.rejoinAt = true, time.Time{}
	case c.wasMinority:
		if c.rejoinAt.IsZero() {
			c.rejoinAt = now.Add(c.rejoinDelay())
			c.log.Info("majority reachable again; waiting before serving", "delay", c.rejoinDelay())
		}
		if now.Before(c.rejoinAt) {
			t.rejoining, t.stateOK = true, false
			return
		}
		c.wasMinority, c.rejoinAt = false, time.Time{}
		c.log.Info("rejoined the majority; serving again")
		c.emit("rejoined", map[string]any{"node": c.myself.label()})
	}
}
