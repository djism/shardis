package cluster

import (
	"time"
)

// Failure detection and automatic failover (ADR 027-029), following the
// Redis Cluster spec:
//
//	PFAIL      I've waited longer than node_timeout for a PONG from X.
//	reports    Gossip carries each node's PFAIL/FAIL view; when a *primary*
//	           says it can't reach X, we remember that (for 2 x timeout).
//	FAIL       I think X is PFAIL and a majority of primaries agree (me
//	           included, if I'm a primary): mark X FAIL and broadcast FAIL,
//	           which everyone accepts at once.
//	election   X's replicas wait 500 ms + random(0-500 ms) + rank x 1 s
//	           (rank 0 = highest replication offset = most up to date), bump
//	           the current epoch, and ask every primary for its vote.
//	vote       A primary serving slots votes at most once per epoch, only if
//	           X really is FAIL, it hasn't voted for one of X's replicas in
//	           the last 2 x timeout, and the requester's view of the slots
//	           isn't older than its own. The vote is saved to disk first.
//	win        Votes from a majority of primaries: take the election epoch as
//	           our config epoch, promote, claim X's slots. Everyone adopts the
//	           claim because the epoch is higher (the Iteration 8 rule).
//	afterwards X's other replicas follow the new primary; X itself, when it
//	           returns, finds its slots taken with a higher epoch and becomes
//	           a replica. FAIL flags clear once nodes answer again.
const (
	failReportValidity = 2  // x node timeout: how long a failure report counts
	failUndoTime       = 2  // x node timeout: a FAIL primary that still owns its slots gets cleared after this
	replicaValidity    = 10 // x node timeout: a replica cut off longer than this won't run (ADR 029)
)

// election is our own run for primary.
type election struct {
	startAt  time.Time       // when to send the vote requests (after the ranked delay)
	sentAt   time.Time       // when we sent them; zero = not yet
	epoch    uint64          // the epoch we're asking votes for
	votes    map[string]bool // primary IDs that voted for us
	rank     int
	nextTry  time.Time // after a failed election, don't retry before this
	staleLog time.Time // rate-limits the "too stale to run" log
}

func (c *Cluster) authTimeout() time.Duration { return max(2*c.opts.NodeTimeout, 2*time.Second) }

func (c *Cluster) myOffset() int64 {
	if c.opts.Replicator == nil {
		return 0
	}
	return c.opts.Replicator.Offset()
}

// quorum is how many primaries must agree: a majority of the primaries that
// serve at least one slot. Called with c.mu held.
func (c *Cluster) quorum() int {
	serving := map[*node]bool{}
	for _, o := range c.owner {
		if o != nil {
			serving[o] = true
		}
	}
	return len(serving)/2 + 1
}

func (c *Cluster) servesSlots(n *node) bool {
	for _, o := range c.owner {
		if o == n {
			return true
		}
	}
	return false
}

// ---- failure detection ----

// detectFailures runs every cron tick. Called with c.mu held.
func (c *Cluster) detectFailures(now time.Time) (changed bool) {
	validity := failReportValidity * c.opts.NodeTimeout
	for _, n := range c.nodes {
		if n == c.myself || n.flags&FlagHandshake != 0 {
			continue
		}
		for reporter, at := range n.failReports {
			if now.Sub(at) > validity {
				delete(n.failReports, reporter)
			}
		}
		if n.flags&(FlagPFail|FlagFail) == 0 && !n.pingSent.IsZero() && now.Sub(n.pingSent) > c.opts.NodeTimeout {
			n.flags |= FlagPFail
			changed = true
			c.log.Warn("node unreachable (PFAIL)", "node", n.label(), "waited", now.Sub(n.pingSent).Round(time.Millisecond))
			c.stats.PFailMarks.Add(1)
			c.emit("pfail", map[string]any{"node": n.label(), "by": c.myself.label()})
		}
		if n.flags&FlagPFail != 0 && c.markFailIfMajority(n, now) {
			changed = true
		}
	}
	return changed
}

// markFailIfMajority turns our PFAIL into FAIL once enough primaries agree.
func (c *Cluster) markFailIfMajority(n *node, now time.Time) bool {
	votes := len(n.failReports)
	if c.myself.flags&FlagPrimary != 0 {
		votes++ // our own suspicion counts if we're a primary
	}
	if votes < c.quorum() {
		return false
	}
	c.setFail(n, now)
	c.log.Warn("node marked FAIL by majority", "node", n.label(), "reports", votes, "quorum", c.quorum())
	// Tell everyone right away; FAIL needs no further agreement.
	m := c.buildMessage(msgFail, nil)
	m.gossip = []gossipEntry{{id: n.id, flags: wireFlags(n.flags), busPort: uint16(n.busPort),
		clientAddr: n.clientAddr, internalAddr: n.internalAddr}}
	for _, o := range c.nodes {
		if o != c.myself && o.link != nil {
			o.link.send(m)
		}
	}
	return true
}

func (c *Cluster) setFail(n *node, now time.Time) {
	n.flags = n.flags&^FlagPFail | FlagFail
	n.failTime = now
	c.stats.FailMarks.Add(1)
	c.emit("fail", map[string]any{"node": n.label(), "by": c.myself.label(), "reports": len(n.failReports)})
}

// noteFailureReport records what a primary told us about a node in gossip.
func (c *Cluster) noteFailureReport(about, reporter *node, flags uint16, now time.Time) bool {
	if reporter.flags&FlagPrimary == 0 || about == c.myself {
		return false
	}
	if flags&(wirePFail|wireFail) != 0 {
		if about.failReports == nil {
			about.failReports = map[string]time.Time{}
		}
		_, had := about.failReports[reporter.id]
		about.failReports[reporter.id] = now
		if !had {
			c.emit("failure_report", map[string]any{"about": about.label(), "from": reporter.label()})
		}
		return !had
	}
	if _, had := about.failReports[reporter.id]; had {
		delete(about.failReports, reporter.id)
		return true
	}
	return false
}

// heardFrom clears PFAIL and, where safe, FAIL when a node talks to us again.
func (c *Cluster) heardFrom(n *node, now time.Time) bool {
	changed := false
	if n.flags&FlagPFail != 0 {
		n.flags &^= FlagPFail
		changed = true
	}
	if n.flags&FlagFail != 0 {
		// A replica, or a primary whose slots are already elsewhere, can be
		// cleared at once. A primary that still owns slots is cleared only if
		// nobody took over within 2 x timeout (it came back on its own).
		if n.flags&FlagReplica != 0 || !c.servesSlots(n) || now.Sub(n.failTime) > failUndoTime*c.opts.NodeTimeout {
			n.flags &^= FlagFail
			changed = true
			c.log.Info("FAIL cleared", "node", n.label())
			c.emit("fail_cleared", map[string]any{"node": n.label()})
		}
	}
	return changed
}

// ---- failover: the replica side ----

// considerFailover runs every cron tick on a replica. Called with c.mu held.
func (c *Cluster) considerFailover(now time.Time) (changed bool) {
	me := c.myself
	primary := c.nodes[me.primaryID]
	e := &c.election
	if me.flags&FlagReplica == 0 || primary == nil || primary.flags&FlagFail == 0 || !c.servesSlots(primary) {
		e.startAt, e.sentAt = time.Time{}, time.Time{} // nothing to do (or it recovered)
		return false
	}
	// A replica cut off from its primary for too long has stale data.
	// Better no failover than promoting a copy missing minutes of writes.
	if c.opts.Replicator != nil {
		down := c.opts.Replicator.LinkDownFor() - c.opts.NodeTimeout
		if down > replicaValidity*c.opts.NodeTimeout {
			if now.Sub(e.staleLog) > 10*time.Second {
				e.staleLog = now
				c.log.Warn("not running for primary: my data is too old", "link_down_for", down.Round(time.Second))
				c.emit("failover_skipped", map[string]any{"node": me.label(), "reason": "stale replica"})
			}
			return false
		}
	}
	switch {
	case e.startAt.IsZero():
		if now.Before(e.nextTry) {
			return false
		}
		// Rank: replicas of the same primary that have more data than us
		// go first. Each rank adds a second of delay.
		e.rank = 0
		my := c.myOffset()
		for _, n := range c.nodes {
			if n != me && n.primaryID == primary.id && n.flags&FlagReplica != 0 && n.replOffset > my {
				e.rank++
			}
		}
		delay := 500*time.Millisecond + time.Duration(c.rng.Int64N(int64(500*time.Millisecond))) + time.Duration(e.rank)*time.Second
		e.startAt, e.sentAt = now.Add(delay), time.Time{}
		c.log.Info("primary failed; will run for election", "primary", primary.label(), "rank", e.rank, "in", delay.Round(time.Millisecond))
	case e.sentAt.IsZero() && !now.Before(e.startAt):
		c.currentEpoch++
		e.epoch, e.sentAt, e.votes = c.currentEpoch, now, map[string]bool{}
		changed = true
		c.dirty = true
		req := c.buildMessage(msgAuthReq, nil)
		req.configEpoch = primary.configEpoch // we claim the slots at the primary's last config...
		req.slots = [slotBytes]byte{}
		for s, o := range c.owner {
			if o == primary {
				req.setSlot(s) // ...for exactly the slots it served
			}
		}
		for _, n := range c.nodes {
			if n != me && n.flags&FlagPrimary != 0 && n.link != nil {
				n.link.send(req)
			}
		}
		c.log.Info("election started", "epoch", e.epoch, "quorum", c.quorum())
		c.stats.Elections.Add(1)
		c.emit("election_start", map[string]any{"node": me.label(), "epoch": e.epoch, "primary": primary.label(), "rank": e.rank})
	case !e.sentAt.IsZero() && now.Sub(e.sentAt) > c.authTimeout():
		c.log.Warn("election timed out; will retry", "epoch", e.epoch, "votes", len(e.votes), "quorum", c.quorum())
		c.emit("election_timeout", map[string]any{"node": me.label(), "epoch": e.epoch, "votes": len(e.votes)})
		e.startAt, e.sentAt = time.Time{}, time.Time{}
		e.nextTry = now.Add(2 * c.authTimeout())
	}
	return changed
}

// handleAuthAck counts a vote for our election.
func (c *Cluster) handleAuthAck(sender *node, m *message) bool {
	e := &c.election
	if e.sentAt.IsZero() || m.currentEpoch < e.epoch || sender.flags&FlagPrimary == 0 || !c.servesSlots(sender) {
		return false
	}
	e.votes[sender.id] = true
	c.emit("vote_received", map[string]any{"node": c.myself.label(), "from": sender.label(), "epoch": e.epoch, "votes": len(e.votes)})
	if len(e.votes) < c.quorum() {
		return false
	}
	c.winElection()
	return true
}

// winElection: we're the new primary for our old primary's slots.
func (c *Cluster) winElection() {
	me := c.myself
	old := c.nodes[me.primaryID]
	me.flags = me.flags&^FlagReplica | FlagPrimary
	me.primaryID = ""
	me.configEpoch = c.election.epoch
	taken := 0
	for s, o := range c.owner {
		if o == old {
			c.owner[s] = me
			taken++
		}
	}
	c.log.Warn("won the election: now primary", "epoch", me.configEpoch, "slots", taken, "votes", len(c.election.votes))
	c.stats.FailoversWon.Add(1)
	c.emit("failover_won", map[string]any{"node": me.label(), "epoch": me.configEpoch, "slots": taken,
		"votes": len(c.election.votes), "old_primary": old.label()})
	c.election = election{}
	c.dirty = true
	if c.opts.Replicator != nil {
		go c.opts.Replicator.Promote()
	}
	// Announce right away instead of waiting for the next heartbeats.
	for _, n := range c.nodes {
		if n != me && n.link != nil {
			n.link.send(c.buildMessage(msgPong, n))
		}
	}
}

// ---- failover: the voter side ----

// handleAuthRequest decides whether to vote for a replica. Called with c.mu
// held. Returns the ACK to send (or nil). The caller saves nodes.conf
// before sending it, so a restart can never make us vote twice in an epoch.
func (c *Cluster) handleAuthRequest(req *node, m *message, now time.Time) *message {
	me := c.myself
	deny := func(why string) *message {
		c.emit("vote_denied", map[string]any{"node": me.label(), "to": req.label(), "epoch": m.currentEpoch, "why": why})
		return nil
	}
	if me.flags&FlagPrimary == 0 || !c.servesSlots(me) {
		return nil // only primaries serving slots vote
	}
	primary := c.nodes[m.primaryID]
	switch {
	case m.currentEpoch < c.currentEpoch:
		return deny("old epoch")
	case c.lastVoteEpoch >= c.currentEpoch:
		return deny("already voted this epoch")
	case m.flags&wireReplica == 0 || primary == nil:
		return deny("not a replica of a known primary")
	case primary.flags&FlagFail == 0:
		return deny("its primary is not FAIL")
	case now.Sub(primary.votedTime) < 2*c.opts.NodeTimeout:
		return deny("voted for a replica of that primary recently")
	}
	for s := range len(c.owner) {
		if m.hasSlot(s) && c.owner[s] != nil && c.owner[s].configEpoch > m.configEpoch {
			return deny("requester has an older view of the slots")
		}
	}
	c.lastVoteEpoch = c.currentEpoch
	primary.votedTime = now
	c.dirty = true
	c.log.Info("voted for failover", "replica", req.label(), "epoch", c.currentEpoch)
	c.emit("vote_granted", map[string]any{"node": me.label(), "to": req.label(), "epoch": c.currentEpoch})
	return c.buildMessage(msgAuthAck, req)
}

// ---- reconfiguration when slots move ----

// adoptNewOwner is called when slots we (or our primary) owned were claimed
// by `taker` with a higher epoch. If we've lost all our slots, we become its
// replica; if our primary lost all its slots, we follow `taker` instead.
// Called with c.mu held; returns the internal address to replicate from,
// if we must start replicating (the caller does that after unlocking).
func (c *Cluster) adoptNewOwner(taker *node, iLost, primaryLost bool) string {
	me := c.myself
	switch {
	case iLost && me.flags&FlagPrimary != 0 && !c.servesSlots(me):
		c.log.Warn("my slots were taken over (higher epoch); becoming a replica", "new_primary", taker.label())
		c.emit("demoted", map[string]any{"node": me.label(), "new_primary": taker.label()})
	case primaryLost && me.flags&FlagReplica != 0 && !c.servesSlots(c.nodes[me.primaryID]):
		c.log.Info("my primary's slots moved; following the new primary", "new_primary", taker.label())
	default:
		return ""
	}
	me.flags = me.flags&^FlagPrimary | FlagReplica
	me.primaryID = taker.id
	c.election = election{}
	c.dirty = true
	return taker.internalAddr
}
