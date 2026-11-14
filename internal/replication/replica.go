package replication

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/djism/shardis/internal/resp"
	"github.com/djism/shardis/internal/store"
)

// replicaLoop keeps a link to the primary, reconnecting with backoff, until
// stop is closed.
func (n *Node) replicaLoop(stop, done chan struct{}) {
	defer close(done)
	backoff := 100 * time.Millisecond
	for {
		err := n.syncOnce(stop)
		select {
		case <-stop:
			n.linkStatus.Store("down")
			return
		default:
		}
		n.linkStatus.Store("down")
		n.log.Warn("replication link down; retrying", "err", err, "in", backoff)
		n.emit("repl_link_down", map[string]any{"err": fmt.Sprint(err)})
		select {
		case <-stop:
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 2*time.Second)
	}
}

// replicaConn is a connection to the primary. Writes come from two
// goroutines (the apply loop answering GETACK, the ack ticker), so they're
// serialized by mu.
type replicaConn struct {
	conn    net.Conn
	r       *resp.Reader
	mu      sync.Mutex
	w       *resp.Writer
	timeout time.Duration
}

// send writes one command with its own write deadline. (A deadline set once
// for the handshake would expire later and silently break every ACK; that
// bug made links drop every 20 s until Iteration 7 caught it.)
func (c *replicaConn) send(args ...string) error {
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	c.w.WriteCommand(b...)
	return c.w.Flush()
}

// call sends a command and reads a simple-string reply.
func (c *replicaConn) call(args ...string) (string, error) {
	if err := c.send(args...); err != nil {
		return "", err
	}
	v, err := c.r.ReadValue()
	if err != nil {
		return "", err
	}
	if v.Kind == resp.Error {
		return "", fmt.Errorf("primary replied: %s", v.Str)
	}
	return string(v.Str), nil
}

// syncOnce connects, handshakes, syncs (full or partial) and then applies
// the stream until the connection fails or stop is closed.
func (n *Node) syncOnce(stop chan struct{}) error {
	n.mu.Lock()
	addr := hostPort(n.masterHost, n.masterPort)
	n.mu.Unlock()
	conn, err := net.DialTimeout("tcp", addr, n.opts.Timeout)
	if err != nil {
		return err
	}
	c := &replicaConn{conn: conn, r: resp.NewReader(conn), w: resp.NewWriter(conn), timeout: n.opts.Timeout}
	finished := make(chan struct{})
	defer close(finished)
	go func() { // unblock reads when told to stop (and don't outlive this attempt)
		select {
		case <-stop:
			conn.Close()
		case <-finished:
		}
	}()
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(n.opts.Timeout))

	// Handshake, the same steps a Redis replica takes.
	if _, err := c.call("PING"); err != nil {
		return err
	}
	if _, err := c.call("REPLCONF", "listening-port", strconv.Itoa(n.opts.AnnouncePort)); err != nil {
		return err
	}
	if _, err := c.call("REPLCONF", "capa", "psync2"); err != nil {
		return err
	}
	n.mu.Lock()
	replid, off := n.replid, int64(-1)
	if _, end := n.backlog.offsets(); n.backlog.active() {
		off = end
	} else {
		replid = "?"
	}
	n.mu.Unlock()
	n.linkStatus.Store("sync")
	reply, err := c.call("PSYNC", replid, strconv.FormatInt(off, 10))
	if err != nil {
		return err
	}
	f := strings.Fields(reply)
	switch {
	case len(f) == 3 && f[0] == "FULLRESYNC":
		start, err := strconv.ParseInt(f[2], 10, 64)
		if err != nil {
			return fmt.Errorf("bad FULLRESYNC reply %q", reply)
		}
		if err := n.loadSnapshot(c, stop); err != nil {
			return err
		}
		n.mu.Lock()
		n.replid, n.replid2, n.second = f[1], "", -1
		n.backlog.reset(start)
		n.mu.Unlock()
		off = start
		n.log.Info("full resync done", "primary", addr, "offset", start)
	case len(f) == 2 && f[0] == "CONTINUE":
		n.mu.Lock()
		if f[1] != n.replid { // the primary was promoted: adopt its new history
			n.replid2, n.second, n.replid = n.replid, off, f[1]
		}
		n.mu.Unlock()
		n.log.Info("partial resync", "primary", addr, "offset", off)
	default:
		return fmt.Errorf("unexpected PSYNC reply %q", reply)
	}
	n.linkStatus.Store("up")
	n.emit("repl_link_up", map[string]any{"primary": addr, "offset": off})
	return n.applyStream(c, off, stop)
}

// loadSnapshot replaces our data with the primary's: flush, then apply SET
// records until SNAPSHOT-END. Snapshot bytes don't count toward the offset.
func (n *Node) loadSnapshot(c *replicaConn, stop chan struct{}) error {
	n.opts.Engine.DoAll(func(tx *store.Tx) { tx.Flush() })
	for {
		c.conn.SetReadDeadline(time.Now().Add(n.opts.Timeout))
		rec, err := c.r.ReadCommand()
		if err != nil {
			return err
		}
		n.lastIO.Store(time.Now().UnixNano())
		if strings.EqualFold(string(rec[0]), "SNAPSHOT-END") {
			return nil
		}
		if err := n.apply(rec); err != nil {
			return err
		}
	}
}

// applyStream applies stream records, appends them to our own backlog (so
// after a promotion we can serve partial resyncs at the same offsets), and
// acknowledges our offset once a second and whenever the primary asks.
func (n *Node) applyStream(c *replicaConn, off int64, stop chan struct{}) error {
	var mu sync.Mutex // guards off between the apply loop and the ack ticker
	ackStop := make(chan struct{})
	defer close(ackStop)
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ackStop:
				return
			case <-t.C:
			}
			mu.Lock()
			o := off
			mu.Unlock()
			if err := c.send("REPLCONF", "ACK", strconv.FormatInt(o, 10)); err != nil {
				c.conn.Close() // a link we can't ack on is dead: reconnect
				return
			}
		}
	}()
	c.send("REPLCONF", "ACK", strconv.FormatInt(off, 10))
	var raw []byte
	for {
		c.conn.SetReadDeadline(time.Now().Add(n.opts.Timeout))
		rec, err := c.r.ReadCommand()
		if err != nil {
			select {
			case <-stop:
				return errors.New("stopped")
			default:
			}
			return err
		}
		n.lastIO.Store(time.Now().UnixNano())
		raw = encode(raw[:0], rec...)
		getack := false
		switch cmd := strings.ToUpper(string(rec[0])); {
		case cmd == "PING":
		case cmd == "REPLCONF":
			getack = len(rec) >= 2 && strings.EqualFold(string(rec[1]), "GETACK")
		default:
			if err := n.apply(rec); err != nil {
				return err
			}
		}
		n.backlog.append(raw)
		mu.Lock()
		off += int64(len(raw))
		o := off
		mu.Unlock()
		if getack {
			c.send("REPLCONF", "ACK", strconv.FormatInt(o, 10))
		}
	}
}

// apply runs one effect record against the engine, locking only its key.
func (n *Node) apply(rec [][]byte) error {
	var err error
	fn := func(tx *store.Tx) { err = store.ApplyEffect(tx, rec) }
	if keys, all := store.EffectKeys(rec); all {
		n.opts.Engine.DoAll(fn)
	} else {
		n.opts.Engine.Do(keys, fn)
	}
	return err
}
