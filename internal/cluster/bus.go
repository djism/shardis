package cluster

import (
	"bufio"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/djism/shardis/internal/faults"
)

// The cluster bus (ADR 024, 025). Each node listens on its bus port and
// keeps one outbound connection ("link") to every node it knows. PINGs go
// out on our links and PONGs come back on them; other nodes' PINGs arrive
// on inbound connections and we answer with a PONG on the same connection.
//
// Ten times a second (cron), each node:
//   - connects to known nodes it has no link to;
//   - once a second, samples 5 random nodes and pings the one it heard from
//     least recently;
//   - pings any node it hasn't heard from in NodeTimeout/2;
//   - forgets handshakes that never completed, and saves nodes.conf if dirty.
//
// Every PING/PONG carries the sender's slots and gossip about ~10% of the
// cluster. A node that hears about someone it doesn't know starts a
// handshake with it: that's how a chain of MEETs reaches everyone.
const cronPeriod = 100 * time.Millisecond

// link is our outbound connection to one node.
type link struct {
	c     *Cluster
	n     *node
	since time.Time
	conn  net.Conn
	out   chan []byte
	once  sync.Once
	close func()
}

// Start opens the bus port and starts the background loops. If this node is
// a replica in the loaded/seeded state, replication starts too.
func (c *Cluster) Start() error {
	host, _, _ := net.SplitHostPort(c.opts.InternalAddr)
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		host = "" // listen on every interface (containers, pods); loopback stays loopback
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(c.opts.BusPort)))
	if err != nil {
		return err
	}
	c.ln = ln
	c.wg.Add(2)
	go c.acceptLoop()
	go c.cron()
	c.mu.Lock()
	var primary string
	if c.myself.flags&FlagReplica != 0 {
		if p := c.nodes[c.myself.primaryID]; p != nil {
			primary = p.internalAddr
		}
	}
	c.mu.Unlock()
	if primary != "" {
		return c.startReplication(primary)
	}
	return nil
}

// Close stops the bus and saves the state one last time.
func (c *Cluster) Close() {
	select {
	case <-c.quit:
		return
	default:
	}
	close(c.quit)
	if c.ln != nil {
		c.ln.Close()
	}
	c.mu.Lock()
	for _, n := range c.nodes {
		if n.link != nil {
			n.link.close()
		}
	}
	for conn := range c.inbound {
		conn.Close()
	}
	c.mu.Unlock()
	c.wg.Wait()
	c.saveIfDirty()
}

func (c *Cluster) acceptLoop() {
	defer c.wg.Done()
	for {
		conn, err := c.ln.Accept()
		if err != nil {
			return
		}
		c.mu.Lock()
		c.inbound[conn] = struct{}{}
		c.mu.Unlock()
		c.wg.Add(1)
		go c.serveInbound(conn)
	}
}

// serveInbound handles another node's PINGs/MEETs and answers with PONGs.
func (c *Cluster) serveInbound(conn net.Conn) {
	defer c.wg.Done()
	defer func() {
		c.mu.Lock()
		delete(c.inbound, conn)
		c.mu.Unlock()
		conn.Close()
	}()
	r := bufio.NewReader(conn)
	for {
		m, size, err := readMessage(r)
		if err != nil {
			return
		}
		c.countRecv(m.typ, size)
		reply, to := c.process(m, nil)
		if reply == nil {
			continue
		}
		if reply.typ == msgAuthAck {
			c.saveIfDirty() // the vote must be on disk before anyone hears about it
		}
		if c.dropTo(to, m.sender) {
			c.stats.Dropped.Add(1)
			continue
		}
		b := reply.encode()
		conn.SetWriteDeadline(time.Now().Add(c.opts.NodeTimeout))
		if _, err := conn.Write(b); err != nil {
			return
		}
		c.countSent(reply.typ, len(b))
	}
}

// dropTo consults fault injection for a message to a node (by name or ID).
func (c *Cluster) dropTo(n *node, id string) bool {
	if c.opts.Faults == nil {
		return false
	}
	if n != nil && n.name != "" && c.opts.Faults.Check(faults.BusMessage, n.name, c.quit) {
		return true
	}
	return c.opts.Faults.Check(faults.BusMessage, id, c.quit)
}

func (c *Cluster) countSent(typ uint8, size int) {
	c.stats.Sent.Add(1)
	c.stats.BytesSent.Add(int64(size))
	c.stats.SentByType[typ].Add(1)
}

func (c *Cluster) countRecv(typ uint8, size int) {
	c.stats.Received.Add(1)
	c.stats.BytesReceived.Add(int64(size))
	c.stats.ReceivedByType[typ].Add(1)
}

// cron is the heartbeat of the bus.
func (c *Cluster) cron() {
	defer c.wg.Done()
	t := time.NewTicker(cronPeriod)
	defer t.Stop()
	for tick := 0; ; tick++ {
		select {
		case <-c.quit:
			return
		case <-t.C:
		}
		c.mu.Lock()
		now := time.Now()
		changed := false
		for _, n := range c.nodes {
			if n == c.myself {
				continue
			}
			if n.flags&FlagHandshake != 0 && now.Sub(n.created) > max(c.opts.NodeTimeout, time.Second) {
				c.log.Info("handshake timed out", "addr", n.internalAddr)
				c.removeNode(n)
				changed = true
				continue
			}
			if n.link == nil && !n.dialing {
				n.dialing = true
				go c.dial(n)
			}
		}
		for id, until := range c.forgotten {
			if now.After(until) {
				delete(c.forgotten, id)
			}
		}
		// Once a second: of 5 random nodes, ping the one we heard from least recently.
		if tick%10 == 0 {
			if n := c.oldestOfSample(5); n != nil {
				c.ping(n, now)
			}
		}
		// A PING unanswered for half the timeout: the link may be broken in a
		// way TCP hasn't noticed (or its messages are being lost). Drop it so
		// cron reconnects and pings again. Without this, a node whose PONG was
		// lost would never be pinged again. pingSent is kept: it's how long
		// we've been waiting, which failure detection (Iteration 9) needs.
		for _, n := range c.nodes {
			if n.link != nil && !n.pingSent.IsZero() && now.Sub(n.pingSent) > c.opts.NodeTimeout/2 &&
				now.Sub(n.link.since) > c.opts.NodeTimeout/2 {
				n.link.close()
			}
		}
		// Anyone we haven't heard from in half the timeout gets pinged now.
		for _, n := range c.nodes {
			if n != c.myself && n.link != nil && n.pingSent.IsZero() &&
				(n.flags&FlagHandshake != 0 || now.Sub(n.pongRecv) > c.opts.NodeTimeout/2) {
				c.ping(n, now)
			}
		}
		if c.detectFailures(now) {
			changed = true
		}
		if c.considerFailover(now) {
			changed = true
		}
		if c.wasMinority && !c.rejoinAt.IsZero() && !now.Before(c.rejoinAt) {
			changed = true // the rejoin delay is over: republish so we serve again
		}
		if changed {
			c.dirty = true
			c.publish()
		}
		c.mu.Unlock()
		c.saveIfDirty()
	}
}

func (c *Cluster) oldestOfSample(k int) *node {
	var pool []*node
	for _, n := range c.nodes {
		if n != c.myself && n.link != nil && n.pingSent.IsZero() && n.flags&FlagHandshake == 0 {
			pool = append(pool, n)
		}
	}
	c.rng.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	var best *node
	for _, n := range pool[:min(k, len(pool))] {
		if best == nil || n.pongRecv.Before(best.pongRecv) {
			best = n
		}
	}
	return best
}

// ping sends a PING (or MEET) to n on its link. Called with c.mu held.
func (c *Cluster) ping(n *node, now time.Time) {
	typ := msgPing
	if n.meet {
		typ = msgMeet
	}
	if n.pingSent.IsZero() { // keep the oldest unanswered ping time
		n.pingSent = now
		c.publish()
	}
	m := c.buildMessage(typ, n)
	n.link.send(m)
	if c.opts.Bus.Active() {
		c.opts.Bus.Emit("gossip_send", map[string]any{"to": n.label(), "type": msgNames[typ], "gossip": len(m.gossip)})
	}
}

func (c *Cluster) dial(n *node) {
	conn, err := net.DialTimeout("tcp", n.busAddr(), time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	n.dialing = false
	if err != nil {
		// Can't even connect. Count it as a PING sent now (if none is
		// pending), or a node that dies right after answering a PING would
		// never look unreachable: no PING could ever go unanswered.
		if n.pingSent.IsZero() && n.flags&FlagHandshake == 0 {
			n.pingSent = time.Now()
			c.publish()
		}
		return // cron retries next tick
	}
	if c.nodes[n.id] != n || n.link != nil {
		conn.Close() // forgotten or renamed meanwhile, or already linked
		return
	}
	select {
	case <-c.quit:
		conn.Close()
		return
	default:
	}
	l := &link{c: c, n: n, since: time.Now(), conn: conn, out: make(chan []byte, 64)}
	done := make(chan struct{})
	l.close = func() { l.once.Do(func() { close(done); conn.Close() }) }
	n.link = l
	c.publish()
	c.emit("bus_link_up", map[string]any{"to": n.label()})
	c.wg.Add(2)
	go l.writeLoop(done)
	go l.readLoop()
	c.ping(n, time.Now()) // say hello right away (MEET for nodes we introduced)
}

// send queues a message; a full queue means the peer is too slow and the
// message is dropped (the next heartbeat carries fresher news anyway).
func (l *link) send(m *message) {
	select {
	case l.out <- m.encode():
	default:
		l.c.stats.Dropped.Add(1)
	}
}

func (l *link) writeLoop(done chan struct{}) {
	defer l.c.wg.Done()
	for {
		select {
		case <-done:
			return
		case b := <-l.out:
			l.c.mu.Lock()
			id := l.n.id
			l.c.mu.Unlock()
			if l.c.dropTo(l.n, id) { // fault injection, outside the cluster lock
				l.c.stats.Dropped.Add(1)
				continue
			}
			l.conn.SetWriteDeadline(time.Now().Add(l.c.opts.NodeTimeout))
			if _, err := l.conn.Write(b); err != nil {
				l.close()
				return
			}
			l.c.countSent(b[5], len(b))
		}
	}
}

func (l *link) readLoop() {
	defer l.c.wg.Done()
	defer l.c.linkDown(l)
	r := bufio.NewReader(l.conn)
	for {
		m, size, err := readMessage(r)
		if err != nil {
			return
		}
		l.c.countRecv(m.typ, size)
		l.c.process(m, l)
	}
}

func (c *Cluster) linkDown(l *link) {
	l.close()
	c.mu.Lock()
	defer c.mu.Unlock()
	if l.n.link == l {
		l.n.link = nil // pingSent stays: we're still waiting to hear from it
		c.publish()
		c.emit("bus_link_down", map[string]any{"to": l.n.label()})
	}
}

// process applies one received message to our state. For PING/MEET it
// returns the PONG to send back (and the node it goes to). l is the link it
// arrived on if it's a reply on one of our outbound links, else nil.
func (c *Cluster) process(m *message, l *link) (*message, *node) {
	reply, to, replicateFrom := c.processLocked(m, l)
	if replicateFrom != "" { // our slots (or our primary's) moved: follow the new owner
		go func() {
			if err := c.startReplication(replicateFrom); err != nil {
				c.log.Error("couldn't start replicating from the new primary", "addr", replicateFrom, "err", err)
			}
		}()
	}
	return reply, to
}

func (c *Cluster) processLocked(m *message, l *link) (reply *message, to *node, replicateFrom string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if until, ok := c.forgotten[m.sender]; ok && now.Before(until) {
		return nil, nil, ""
	}
	changed := false
	if m.currentEpoch > c.currentEpoch {
		c.currentEpoch, changed = m.currentEpoch, true
	}
	sender := c.nodes[m.sender]

	// We MEET'd our own address: drop that handshake instead of linking to ourselves.
	if l != nil && m.sender == c.myself.id {
		if l.n != c.myself {
			delete(c.nodes, l.n.id)
			c.publish()
		}
		l.close()
		return nil, nil, ""
	}
	// A PONG on a link to a handshake node tells us who that node really is.
	if l != nil && m.typ == msgPong && l.n.flags&FlagHandshake != 0 && l.n.id != m.sender {
		hs := l.n
		delete(c.nodes, hs.id)
		if sender != nil { // we'd already learned about it by gossip: keep that one
			if sender.link == nil {
				sender.link, l.n = l, sender
			} else {
				l.close()
			}
		} else {
			hs.id = m.sender
			c.nodes[hs.id] = hs
			sender = hs
		}
		changed = true
	}
	if sender == nil {
		if m.typ != msgMeet {
			// Only a MEET may introduce a new node directly; a stray PING
			// from a stranger gets a PONG but doesn't join it to our view.
			if m.typ == msgPing {
				return c.buildMessage(msgPong, nil), nil, ""
			}
			return nil, nil, ""
		}
		sender = &node{id: m.sender, flags: FlagHandshake, created: time.Now()}
		c.nodes[sender.id] = sender
		changed = true
		c.log.Info("met by a new node", "id", m.sender[:8], "addr", m.internalAddr)
	}
	sender.replOffset = int64(m.offset)
	upd, addr := c.updateFromHeader(sender, m)
	if upd {
		changed = true
	}
	replicateFrom = addr
	if m.typ == msgPing || m.typ == msgPong || m.typ == msgMeet {
		if c.heardFrom(sender, now) {
			changed = true
		}
	}
	if m.typ == msgPong && l != nil && l.n == sender {
		sender.pongRecv, sender.pingSent = now, time.Time{}
		changed = true // fresh timestamps belong in the snapshot (CLUSTER NODES, failure detection)
		if sender.flags&FlagHandshake != 0 {
			sender.flags &^= FlagHandshake
			sender.meet = false
			changed = true
			c.emit("node_joined", map[string]any{"node": sender.label(), "addr": sender.clientAddr})
			c.log.Info("node joined", "id", sender.id[:8], "addr", sender.clientAddr)
		}
	}
	if c.handleGossip(sender, m.gossip, now) {
		changed = true
	}
	switch m.typ {
	case msgFail: // "this node is FAIL": accepted from any confirmed node, no vote needed
		if sender.flags&FlagHandshake == 0 && len(m.gossip) == 1 {
			if x := c.nodes[m.gossip[0].id]; x != nil && x != c.myself && x.flags&FlagFail == 0 {
				c.setFail(x, now)
				changed = true
				c.log.Warn("node FAIL (told by peer)", "node", x.label(), "by", sender.label())
			}
		}
	case msgAuthReq:
		if ack := c.handleAuthRequest(sender, m, now); ack != nil {
			changed = true
			c.publish()
			return ack, sender, replicateFrom
		}
	case msgAuthAck:
		if c.handleAuthAck(sender, m) {
			changed = true
		}
	}
	if c.opts.Bus.Active() {
		ids := make([]string, len(m.gossip))
		for i, g := range m.gossip {
			ids[i] = g.id[:8]
		}
		c.opts.Bus.Emit("gossip_recv", map[string]any{"from": sender.label(), "type": msgNames[m.typ], "about": ids})
	}
	if changed {
		c.dirty = true
		c.publish()
	}
	if m.typ == msgPing || m.typ == msgMeet {
		return c.buildMessage(msgPong, sender), sender, replicateFrom
	}
	return nil, nil, replicateFrom
}

// updateFromHeader copies what a node says about itself: addresses, role,
// config epoch, and the slots it claims. A claim wins a slot only if the
// slot is free or the claimer's config epoch is higher than the current
// owner's: epochs settle conflicts (the rule failover relies on).
func (c *Cluster) updateFromHeader(n *node, m *message) (changed bool, replicateFrom string) {
	if n.clientAddr != m.clientAddr || n.internalAddr != m.internalAddr || n.busPort != int(m.busPort) {
		n.clientAddr, n.internalAddr, n.busPort = m.clientAddr, m.internalAddr, int(m.busPort)
		changed = true
	}
	role := uint16(FlagPrimary)
	if m.flags&wireReplica != 0 {
		role = FlagReplica
	}
	if n.flags&(FlagPrimary|FlagReplica) != role || n.primaryID != m.primaryID {
		n.flags = n.flags&^(FlagPrimary|FlagReplica) | role
		n.primaryID = m.primaryID
		changed = true
	}
	if role == FlagReplica || m.typ == msgAuthReq {
		return changed, "" // a replica's header carries its primary's epoch, not a claim of its own
	}
	if n.configEpoch != m.configEpoch {
		n.configEpoch = m.configEpoch
		changed = true
	}
	myPrimary := c.nodes[c.myself.primaryID]
	iLost, primaryLost := false, false
	for s := range len(c.owner) {
		if !m.hasSlot(s) || c.owner[s] == n {
			continue
		}
		if cur := c.owner[s]; cur == nil || n.configEpoch > cur.configEpoch {
			if cur == c.myself {
				iLost = true
			}
			if cur != nil && cur == myPrimary {
				primaryLost = true
			}
			c.owner[s] = n
			changed = true
		}
	}
	if iLost || primaryLost {
		replicateFrom = c.adoptNewOwner(n, iLost, primaryLost)
	}
	if c.myself.flags&FlagPrimary != 0 && n.configEpoch == c.myself.configEpoch && n != c.myself {
		c.resolveEpochCollision(n)
		changed = true
	}
	return changed, replicateFrom
}

// resolveEpochCollision: two primaries with the same config epoch can't be
// ordered, so one must move. By Redis's rule, the node with the smaller ID
// bumps the current epoch and takes it as its new config epoch. Both nodes
// run this and agree on who moves without talking about it.
func (c *Cluster) resolveEpochCollision(other *node) {
	if c.myself.id >= other.id {
		return // the other node has the smaller ID: it moves
	}
	c.currentEpoch++
	c.myself.configEpoch = c.currentEpoch
	c.log.Info("config epoch collision resolved", "with", other.label(), "new_epoch", c.myself.configEpoch)
}

// handleGossip records failure reports (what a primary says about other
// nodes' health) and starts a handshake with any node the sender knows that
// we don't.
func (c *Cluster) handleGossip(sender *node, entries []gossipEntry, now time.Time) bool {
	if sender.flags&FlagHandshake != 0 {
		return false // don't take introductions from a node we haven't confirmed
	}
	changed := false
	for _, g := range entries {
		if g.id == c.myself.id {
			continue
		}
		if known := c.nodes[g.id]; known != nil {
			if c.noteFailureReport(known, sender, g.flags, now) {
				changed = true
			}
			continue
		}
		if until, ok := c.forgotten[g.id]; ok && time.Now().Before(until) {
			continue
		}
		if g.internalAddr == "" || g.busPort == 0 {
			continue
		}
		c.nodes[g.id] = &node{id: g.id, clientAddr: g.clientAddr, internalAddr: g.internalAddr,
			busPort: int(g.busPort), flags: FlagHandshake | FlagPrimary, created: time.Now()}
		changed = true
	}
	return changed
}
