package replication

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djism/shardis/internal/faults"
	"github.com/djism/shardis/internal/resp"
	"github.com/djism/shardis/internal/store"
)

// link is the primary's side of one replica connection.
type link struct {
	id      string
	conn    net.Conn
	ack     atomic.Int64 // last offset the replica confirmed
	lastAck atomic.Int64 // unix ns
	syncing atomic.Bool
	stopped atomic.Bool
	stopCh  chan struct{}
	once    sync.Once
}

func (l *link) close() {
	l.once.Do(func() {
		l.stopped.Store(true)
		close(l.stopCh)
		l.conn.Close()
	})
}

// ServePSYNC takes over a client connection that sent PSYNC and turns it into
// a replication link. It runs until the link ends. replid/offset are what the
// replica asked for ("?" / -1 for a fresh replica).
func (n *Node) ServePSYNC(conn net.Conn, r *resp.Reader, replid string, offset int64, replicaPort int, replicaHost string) {
	host := replicaHost
	if host == "" {
		host, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}
	l := &link{id: hostPort(host, replicaPort), conn: conn, stopCh: make(chan struct{})}
	l.lastAck.Store(time.Now().UnixNano())
	w := bufio.NewWriterSize(conn, 64<<10)

	// Fault injection first: during a simulated partition, not even the
	// PSYNC reply (let alone a snapshot) may cross to this replica.
	if n.opts.Faults.Check(faults.ReplStream, l.id, l.stopCh) {
		conn.Close()
		return
	}
	n.mu.Lock()
	if n.closed || n.Role() != RolePrimary {
		n.mu.Unlock()
		fmt.Fprintf(w, "-NOMASTERLINK this node is not a primary\r\n")
		w.Flush()
		conn.Close()
		return
	}
	n.backlog.enable()
	full := !n.canContinue(replid, offset)
	if full {
		_, offset = n.backlog.offsets() // the snapshot starts here; the stream continues from here
		fmt.Fprintf(w, "+FULLRESYNC %s %d\r\n", n.replid, offset)
		l.syncing.Store(true)
		n.fullSyncs.Add(1)
		if replid != "?" {
			n.partialErr.Add(1)
		}
	} else {
		fmt.Fprintf(w, "+CONTINUE %s\r\n", n.replid)
		n.partialOK.Add(1)
	}
	l.ack.Store(offset)
	n.links[l] = struct{}{}
	n.mu.Unlock()
	// Send the reply now: on an idle primary nothing else would flush it,
	// and the replica would wait for +CONTINUE until it timed out.
	if err := w.Flush(); err != nil {
		conn.Close()
		n.mu.Lock()
		delete(n.links, l)
		n.mu.Unlock()
		return
	}
	n.emit("repl_replica_connected", map[string]any{"replica": l.id, "full_sync": full, "offset": offset})
	n.log.Info("replica connected", "replica", l.id, "full_sync", full, "offset", offset)

	defer func() {
		l.close()
		n.mu.Lock()
		delete(n.links, l)
		n.ackCond.Broadcast()
		n.mu.Unlock()
		n.emit("repl_replica_disconnected", map[string]any{"replica": l.id})
		n.log.Info("replica disconnected", "replica", l.id)
	}()
	go n.readAcks(l, r)

	if full {
		start := time.Now()
		if err := n.sendSnapshot(l, w); err != nil {
			return
		}
		l.syncing.Store(false)
		n.emit("repl_full_sync_sent", map[string]any{"replica": l.id, "took_ms": time.Since(start).Milliseconds()})
	}
	n.stream(l, w, offset)
}

// canContinue decides PSYNC: continue if the replica is on our history (or
// our previous one, up to where we took over) and its offset is still in the
// backlog. Callers hold n.mu.
func (n *Node) canContinue(replid string, off int64) bool {
	if replid == n.replid || (replid == n.replid2 && n.replid2 != "" && off <= n.second) {
		return n.backlog.covers(off)
	}
	return false
}

// sendSnapshot streams every key as a SET record, one slot group at a time
// under that group's lock (the same fuzzy snapshot as the AOF rewrite,
// ADR 016/018), then a SNAPSHOT-END marker. Changes made meanwhile are in the
// backlog from the snapshot's start offset, and records are idempotent, so
// replaying them on top of the snapshot is correct.
func (n *Node) sendSnapshot(l *link, w *bufio.Writer) error {
	var buf []byte
	for g := range store.NumGroups {
		n.opts.Engine.DoGroup(g, func(tx *store.Tx, slots []uint16) { buf = tx.AppendSnapshot(buf[:0], slots) })
		l.conn.SetWriteDeadline(time.Now().Add(n.opts.Timeout))
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	w.Write(snapshotEndRecord)
	return w.Flush()
}

// stream sends backlog bytes from off onward until the link ends.
func (n *Node) stream(l *link, w *bufio.Writer, off int64) {
	buf := make([]byte, 64<<10)
	for !l.stopped.Load() {
		got, err := n.backlog.read(off, buf, l.stopped.Load)
		if err == nil {
			// The fault check goes here, between "new data arrived" and "send
			// it". Checked before the read, a pause set while we were waiting
			// for data would let the very next write slip through.
			if n.opts.Faults.Check(faults.ReplStream, l.id, l.stopCh) {
				n.log.Info("fault injection: severing replication link", "replica", l.id)
				return
			}
			// Re-read: while we were paused the backlog may have moved on
			// (more data, or our offset overwritten).
			got, err = n.backlog.read(off, buf, l.stopped.Load)
		}
		if errors.Is(err, errTooOld) {
			n.log.Warn("replica fell behind the backlog; it must full-resync", "replica", l.id, "offset", off)
			return
		}
		if err != nil {
			return
		}
		l.conn.SetWriteDeadline(time.Now().Add(n.opts.Timeout))
		if _, err := w.Write(buf[:got]); err != nil {
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
		off += int64(got)
	}
}

// readAcks handles what the replica sends back: REPLCONF ACK <offset>.
func (n *Node) readAcks(l *link, r *resp.Reader) {
	defer n.opts.Faults.Wake() // ...including a sender paused by a fault
	defer n.backlog.wake()     // let the sender notice the link is gone
	defer l.close()
	for {
		args, err := r.ReadCommand()
		if err != nil {
			return
		}
		if len(args) == 3 && strings.EqualFold(string(args[0]), "REPLCONF") && strings.EqualFold(string(args[1]), "ACK") {
			off, err := strconv.ParseInt(string(args[2]), 10, 64)
			if err != nil {
				return
			}
			l.ack.Store(off)
			l.lastAck.Store(time.Now().UnixNano())
			n.mu.Lock()
			n.ackCond.Broadcast()
			n.mu.Unlock()
		}
	}
}

// heartbeat puts a PING into the stream every PingPeriod while there are
// replicas (so they can tell a quiet primary from a dead one), and drops
// replicas that haven't acknowledged anything for Timeout.
func (n *Node) heartbeat() {
	defer n.done.Done()
	t := time.NewTicker(n.opts.PingPeriod)
	defer t.Stop()
	for {
		select {
		case <-n.quit:
			return
		case <-t.C:
		}
		n.mu.Lock()
		if n.Role() == RolePrimary && len(n.links) > 0 {
			n.backlog.append(pingRecord)
		}
		for l := range n.links {
			if !l.syncing.Load() && time.Since(time.Unix(0, l.lastAck.Load())) > n.opts.Timeout {
				n.log.Warn("replica timed out", "replica", l.id)
				l.close()
			}
		}
		n.mu.Unlock()
	}
}

// Wait blocks until numReplicas replicas have acknowledged everything
// written so far, or until timeout (0 = forever), and returns how many have.
// It asks replicas to ack right away (REPLCONF GETACK in the stream) rather
// than waiting for their once-a-second ack.
//
// What WAIT does NOT do: undo anything. If it returns fewer than asked, the
// write is still applied on the primary and visible to other clients; the
// caller just knows it isn't safely replicated yet.
func (n *Node) Wait(numReplicas int, timeout time.Duration) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, target := n.backlog.offsets()
	count := n.ackedLocked(target)
	if count >= numReplicas || len(n.links) == 0 {
		return count
	}
	n.backlog.append(getackRecord)
	timedOut := false
	if timeout > 0 {
		timer := time.AfterFunc(timeout, func() {
			n.mu.Lock()
			timedOut = true
			n.ackCond.Broadcast()
			n.mu.Unlock()
		})
		defer timer.Stop()
	}
	for count < numReplicas && !timedOut && !n.closed {
		n.ackCond.Wait()
		count = n.ackedLocked(target)
	}
	return count
}

func (n *Node) ackedLocked(target int64) int {
	c := 0
	for l := range n.links {
		if !l.syncing.Load() && l.ack.Load() >= target {
			c++
		}
	}
	return c
}

func hostPort(host string, port int) string { return net.JoinHostPort(host, strconv.Itoa(port)) }
