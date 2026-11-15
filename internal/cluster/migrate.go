package cluster

import (
	"errors"
	"fmt"
)

// Slot migration state (Iteration 11, ADR 032). Moving slot S from node A to
// node B is driven by a tool (shardis-admin, or redis-cli --cluster), step by
// step:
//
//	B: SETSLOT S IMPORTING A   B will serve S's keys, but only to clients that sent ASKING
//	A: SETSLOT S MIGRATING B   A serves the keys it still has; missing ones get -ASK S B
//	A: MIGRATE ... (batches)   keys move from A to B
//	B: SETSLOT S NODE B        B owns S (bumping its config epoch)
//	A: SETSLOT S NODE B        A stops serving S and sends MOVED
//
// The MIGRATING/IMPORTING markers are local to each node, like in Redis:
// they're not gossiped, only saved in nodes.conf. The final ownership
// change spreads by gossip: B's higher config epoch wins the slot everywhere.

// SetSlotMigrating marks a slot we own as moving to node id.
func (c *Cluster) SetSlotMigrating(slot uint16, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.owner[slot] != c.myself {
		return fmt.Errorf("I'm not the owner of hash slot %d", slot)
	}
	to := c.nodes[id]
	if err := c.checkMigrationPeer(to, id); err != nil {
		return err
	}
	c.migrating[slot] = to
	c.dirty = true
	c.publish()
	c.emit("slot_migrating", map[string]any{"node": c.myself.label(), "slot": slot, "to": to.label()})
	return nil
}

// SetSlotImporting marks a slot that node id owns as moving to us.
func (c *Cluster) SetSlotImporting(slot uint16, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.owner[slot] == c.myself {
		return fmt.Errorf("I'm already the owner of hash slot %d", slot)
	}
	from := c.nodes[id]
	if err := c.checkMigrationPeer(from, id); err != nil {
		return err
	}
	c.importing[slot] = from
	c.dirty = true
	c.publish()
	c.emit("slot_importing", map[string]any{"node": c.myself.label(), "slot": slot, "from": from.label()})
	return nil
}

func (c *Cluster) checkMigrationPeer(n *node, id string) error {
	switch {
	case c.myself.flags&FlagReplica != 0:
		return errors.New("Please use SETSLOT only with masters.")
	case n == nil:
		return fmt.Errorf("I don't know about node %s", id)
	case n == c.myself:
		return errors.New("I can't migrate a slot to or from myself")
	case n.flags&FlagReplica != 0:
		return fmt.Errorf("Target node %s is not a master", id)
	}
	return nil
}

// SetSlotStable clears both markers (aborting a migration).
func (c *Cluster) SetSlotStable(slot uint16) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.myself.flags&FlagReplica != 0 {
		return errors.New("Please use SETSLOT only with masters.")
	}
	delete(c.migrating, slot)
	delete(c.importing, slot)
	c.dirty = true
	c.publish()
	return nil
}

// SetSlotNode assigns slot to node id, ending a migration. keysHere is how
// many keys we still hold in the slot: an owner must not give a slot away
// while it has keys in it, or they'd be stranded where nobody looks.
//
// If we're the importing node and take the slot ourselves, we bump our
// config epoch without asking anyone (ADR 035): our claim must beat the old
// owner's in gossip, and a higher config epoch is how claims are ordered.
func (c *Cluster) SetSlotNode(slot uint16, id string, keysHere int) error {
	c.mu.Lock()
	n := c.nodes[id]
	switch {
	case c.myself.flags&FlagReplica != 0:
		c.mu.Unlock()
		return errors.New("Please use SETSLOT only with masters.")
	case n == nil:
		c.mu.Unlock()
		return fmt.Errorf("Unknown node %s", id)
	case n.flags&FlagReplica != 0:
		c.mu.Unlock()
		return fmt.Errorf("Target node %s is not a master", id)
	case c.owner[slot] == c.myself && n != c.myself && keysHere > 0:
		c.mu.Unlock()
		return fmt.Errorf("Can't assign hashslot %d to a different node while I still hold keys for this hash slot.", slot)
	}
	prev := c.owner[slot]
	var replicateFrom string
	if n == c.myself {
		if c.importing[slot] != nil {
			c.bumpEpochWithoutConsensus()
		}
		delete(c.importing, slot)
	} else {
		delete(c.migrating, slot)
	}
	c.owner[slot] = n
	if prev == c.myself && n != c.myself {
		// Giving away our last slot makes us a replica of the node that took
		// it, as in Redis (cluster-allow-replica-migration, default yes).
		replicateFrom = c.adoptNewOwner(n, true, false)
	}
	c.dirty = true
	c.publish()
	// Built under the lock: gossip may update n.configEpoch the moment we
	// unlock (the race detector caught us reading it after).
	ev := map[string]any{"node": c.myself.label(), "slot": slot, "owner": n.label(), "epoch": n.configEpoch}
	c.mu.Unlock()
	c.emit("slot_owner", ev)
	if replicateFrom != "" {
		return c.startReplication(replicateFrom)
	}
	return nil
}

// bumpEpochWithoutConsensus gives us a config epoch higher than any other
// node's, unless we already have the unique highest. No vote: two nodes
// doing this at once can pick the same epoch, which the epoch-collision
// rule then resolves (ADR 035). Called with c.mu held.
func (c *Cluster) bumpEpochWithoutConsensus() {
	unique := c.myself.configEpoch > 0
	for _, n := range c.nodes {
		if n != c.myself && n.configEpoch >= c.myself.configEpoch {
			unique = false
		}
	}
	if unique {
		return
	}
	for _, n := range c.nodes {
		c.currentEpoch = max(c.currentEpoch, n.configEpoch)
	}
	c.currentEpoch++
	c.myself.configEpoch = c.currentEpoch
	c.log.Info("config epoch bumped for slot import", "epoch", c.myself.configEpoch)
	c.emit("epoch_bump", map[string]any{"node": c.myself.label(), "epoch": c.myself.configEpoch, "reason": "import"})
}

// dropStaleMarkers forgets migration markers that no longer make sense
// after ownership changed by gossip: MIGRATING a slot we don't own, or
// IMPORTING one we do. Called with c.mu held.
func (c *Cluster) dropStaleMarkers() {
	for s := range c.migrating {
		if c.owner[s] != c.myself {
			delete(c.migrating, s)
			c.dirty = true
		}
	}
	for s := range c.importing {
		if c.owner[s] == c.myself {
			delete(c.importing, s)
			c.dirty = true
		}
	}
}

// TakeOver is CLUSTER FAILOVER TAKEOVER: this replica becomes the primary
// of its primary's slots right now, with no election. It bumps its config
// epoch without consensus (ADR 035) and claims the slots; gossip carries the
// higher epoch to everyone, and the old primary demotes itself on seeing it.
//
// It's only safe when the old primary has stopped accepting writes and
// this replica has everything it acknowledged, which is exactly the
// graceful-shutdown handoff (ADR 043). Used blindly it's a forced split
// brain, which is why Redis documents TAKEOVER as a last resort.
func (c *Cluster) TakeOver() error {
	c.mu.Lock()
	me := c.myself
	old := c.nodes[me.primaryID]
	if me.flags&FlagReplica == 0 || old == nil {
		c.mu.Unlock()
		return errors.New("You should send CLUSTER FAILOVER to a replica")
	}
	me.flags = me.flags&^FlagReplica | FlagPrimary
	me.primaryID = ""
	c.bumpEpochWithoutConsensus()
	taken := 0
	for s, o := range c.owner {
		if o == old {
			c.owner[s] = me
			taken++
		}
	}
	c.election = election{}
	c.dirty = true
	c.publish()
	ev := map[string]any{"node": me.label(), "epoch": me.configEpoch, "slots": taken, "old_primary": old.label()}
	for _, n := range c.nodes { // announce now, not at the next heartbeat
		if n != me && n.link != nil {
			n.link.send(c.buildMessage(msgPong, n))
		}
	}
	c.mu.Unlock()
	c.log.Warn("took over my primary's slots (CLUSTER FAILOVER TAKEOVER)", "epoch", ev["epoch"], "slots", taken)
	c.emit("failover_takeover", ev)
	if c.opts.Replicator != nil {
		c.opts.Replicator.Promote()
	}
	return nil
}
