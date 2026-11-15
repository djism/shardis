// Package replication keeps replicas in sync with a primary (ADR 017-019).
//
// The stream is the effect log the store already produces for the AOF
// (SET x 6, PEXPIREAT k <unix ms>, DEL k, ...). Every byte of it has an
// offset. A replica asks for "history <replid>, from offset X" (PSYNC):
//
//	X still in the primary's backlog   → +CONTINUE, send just the missing bytes
//	otherwise                          → +FULLRESYNC <replid> <O>, stream a
//	                                     snapshot, then the stream from O
//
// Replication is asynchronous: the primary acknowledges a write as soon as it
// has applied it (and written its AOF), before any replica has it. If the
// primary dies in that gap and a replica is promoted, the write is gone.
// WAIT lets a client find out whether N replicas have its writes, which
// shrinks that window but can't close it (see the tests and docs).
package replication

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/faults"
	"github.com/djism/shardis/internal/store"
)

// Role is primary or replica.
type Role int32

const (
	RolePrimary Role = iota
	RoleReplica
)

func (r Role) String() string {
	if r == RoleReplica {
		return "slave" // Redis's word in INFO/ROLE; clients parse it
	}
	return "master"
}

// Options configures a Node.
type Options struct {
	Engine store.Engine
	// AOF is this node's own persistence (nil if off). On a primary the
	// engine's effects go to both the AOF and the replication backlog; on a
	// replica only to the AOF (the backlog then holds the primary's bytes).
	AOF          store.Log
	BacklogSize  int64
	PingPeriod   time.Duration // primary → replica heartbeat in the stream
	Timeout      time.Duration // link considered dead after this much silence
	AnnouncePort int           // our client port, sent as REPLCONF listening-port
	Faults       *faults.Registry
	Bus          *events.Bus
	Log          *slog.Logger
}

// Node is this server's replication state: a primary with links to its
// replicas, or a replica with a link to its primary.
type Node struct {
	opts Options
	log  *slog.Logger
	role atomic.Int32

	mu      sync.Mutex
	ackCond *sync.Cond // broadcast when a replica's ack offset changes (WAIT)
	replid  string     // current history
	replid2 string     // previous history (after a promotion), for PSYNC2
	second  int64      // replicas of replid2 can continue up to this offset; -1 = none
	links   map[*link]struct{}
	closed  bool

	backlog *backlog

	// replica side
	masterHost string
	masterPort int
	replStop   chan struct{}
	replDone   chan struct{}
	linkStatus atomic.Value // string: "connecting", "sync", "up", "down"
	lastIO     atomic.Int64 // unix ns of last byte from the primary

	// Syncs this node has *served* as a primary (INFO sync_full etc.).
	fullSyncs, partialOK, partialErr atomic.Int64

	quit chan struct{}
	done sync.WaitGroup
}

func newReplID() string {
	b := make([]byte, 20)
	rand.Read(b)
	return hex.EncodeToString(b) // 40 hex chars, like Redis
}

// NewNode creates a node in the primary role and hooks it into the engine.
func NewNode(opts Options) *Node {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.BacklogSize <= 0 {
		opts.BacklogSize = 64 << 20
	}
	if opts.PingPeriod <= 0 {
		opts.PingPeriod = time.Second
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}
	n := &Node{
		opts: opts, log: opts.Log, replid: newReplID(), second: -1,
		links: map[*link]struct{}{}, backlog: newBacklog(opts.BacklogSize), quit: make(chan struct{}),
	}
	n.ackCond = sync.NewCond(&n.mu)
	n.linkStatus.Store("")
	opts.Engine.SetLog(store.Tee(opts.AOF, n))
	n.done.Add(1)
	go n.heartbeat()
	return n
}

// Append is the store.Log the engine writes effects to while we're primary.
func (n *Node) Append(p []byte) { n.backlog.append(p) }

func (n *Node) Role() Role      { return Role(n.role.Load()) }
func (n *Node) IsReplica() bool { return n.Role() == RoleReplica }

// ReplicaOf makes this node a replica of host:port (REPLICAOF host port).
// Its data is replaced by the primary's on the first sync.
func (n *Node) ReplicaOf(host string, port int) error {
	n.stopReplicaLoop()
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return errors.New("shutting down")
	}
	for l := range n.links { // we can't be a primary and a replica at once
		l.close()
	}
	n.role.Store(int32(RoleReplica))
	n.masterHost, n.masterPort = host, port
	n.replStop, n.replDone = make(chan struct{}), make(chan struct{})
	stop, done := n.replStop, n.replDone
	n.mu.Unlock()

	n.opts.Engine.SetReplicaMode(true)
	n.opts.Engine.SetLog(n.opts.AOF) // effects only to our own AOF now
	n.linkStatus.Store("connecting")
	n.emit("repl_role", map[string]any{"role": "replica", "master": hostPort(host, port)})
	go n.replicaLoop(stop, done)
	return nil
}

// Promote makes this node a primary (REPLICAOF NO ONE). It keeps its data
// and its backlog, and remembers the old history ID: replicas of the old
// primary that are no further along than us can then partially resync to us
// instead of copying everything again (Redis's PSYNC2).
func (n *Node) Promote() {
	n.stopReplicaLoop()
	n.mu.Lock()
	if n.Role() == RolePrimary {
		n.mu.Unlock()
		return
	}
	_, end := n.backlog.offsets()
	n.replid2, n.second, n.replid = n.replid, end, newReplID()
	n.role.Store(int32(RolePrimary))
	n.masterHost, n.masterPort = "", 0
	n.mu.Unlock()
	n.linkStatus.Store("")
	n.opts.Engine.SetReplicaMode(false)
	n.opts.Engine.SetLog(store.Tee(n.opts.AOF, n))
	n.emit("repl_role", map[string]any{"role": "primary", "offset": end})
	n.log.Info("promoted to primary", "replid", n.replid, "replid2", n.replid2, "offset", end)
}

func (n *Node) stopReplicaLoop() {
	n.mu.Lock()
	stop, done := n.replStop, n.replDone
	n.replStop, n.replDone = nil, nil
	n.mu.Unlock()
	if stop != nil {
		close(stop)
		<-done
	}
}

// Close stops all links and background work.
func (n *Node) Close() {
	n.stopReplicaLoop()
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.closed = true
	for l := range n.links {
		l.close()
	}
	n.ackCond.Broadcast()
	n.mu.Unlock()
	close(n.quit)
	n.backlog.wake()
	n.opts.Faults.Wake() // senders paused by a fault see their link closed and exit
	n.done.Wait()
}

func (n *Node) emit(typ string, data map[string]any) {
	if n.opts.Bus.Active() {
		n.opts.Bus.Emit(typ, data)
	}
}

// ReplicaInfo describes one connected replica.
type ReplicaInfo struct {
	ID      string // host:port it announced
	Offset  int64  // last acknowledged offset
	Lag     time.Duration
	Syncing bool // still receiving the snapshot
}

// Info is what INFO replication and ROLE report.
type Info struct {
	Role         Role
	ReplID       string
	ReplID2      string
	Offset       int64
	SecondOffset int64
	BacklogOn    bool
	BacklogSize  int64
	BacklogStart int64
	Replicas     []ReplicaInfo
	MasterHost   string
	MasterPort   int
	LinkStatus   string
	LastIOAgo    time.Duration
	FullSyncs    int64
	PartialOK    int64
	PartialErr   int64
}

func (n *Node) Info() Info {
	n.mu.Lock()
	defer n.mu.Unlock()
	start, end := n.backlog.offsets()
	in := Info{
		Role: n.Role(), ReplID: n.replid, ReplID2: n.replid2, Offset: end, SecondOffset: n.second,
		BacklogOn: n.backlog.active(), BacklogSize: n.backlog.size, BacklogStart: start,
		MasterHost: n.masterHost, MasterPort: n.masterPort, LinkStatus: n.linkStatus.Load().(string),
		FullSyncs: n.fullSyncs.Load(), PartialOK: n.partialOK.Load(), PartialErr: n.partialErr.Load(),
	}
	if ns := n.lastIO.Load(); ns > 0 {
		in.LastIOAgo = time.Since(time.Unix(0, ns))
	}
	for l := range n.links {
		in.Replicas = append(in.Replicas, ReplicaInfo{
			ID: l.id, Offset: l.ack.Load(), Lag: time.Since(time.Unix(0, l.lastAck.Load())), Syncing: l.syncing.Load(),
		})
	}
	return in
}

// Offset is this node's replication offset: on a primary, everything it has
// streamed; on a replica, everything it has received. Replicas compare
// offsets to decide which of them is most up to date (failover ranking).
func (n *Node) Offset() int64 {
	_, end := n.backlog.offsets()
	return end
}

// LinkDownFor is how long a replica has been cut off from its primary
// (0 while the link is up, or on a primary). A replica cut off for too long
// has stale data and must not win a failover (ADR 029).
func (n *Node) LinkDownFor() time.Duration {
	if n.Role() != RoleReplica || n.linkStatus.Load().(string) == "up" {
		return 0
	}
	last := n.lastIO.Load()
	if last == 0 {
		return time.Duration(1 << 62) // never synced: infinitely stale
	}
	return time.Since(time.Unix(0, last))
}
