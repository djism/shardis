package cluster

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/djism/shardis/internal/keyslot"
)

// nodes.conf: this node's own view of the cluster, saved so a restart keeps
// its identity (random node ID), its peers, slot ownership and epochs
// (ADR 026). One line per node:
//
//	myself <id>
//	epoch <currentEpoch>
//	vote <lastVoteEpoch>    (a primary votes at most once per epoch, even across restarts)
//	node <id> <name|-> <client-addr> <internal-addr> <bus-port> <primary|replica> <primary-id|-> <config-epoch> <slots...>
//	migrating <slot> <node-id>   (a slot we're moving away, ADR 032)
//	importing <slot> <node-id>   (a slot we're taking over)
//
// It's rewritten atomically (temp file, fsync, rename, fsync dir), like the
// AOF manifest, so a crash never leaves a half-written file.

func (c *Cluster) saveIfDirty() {
	if c.opts.StateFile == "" {
		return
	}
	c.mu.Lock()
	if !c.dirty {
		c.mu.Unlock()
		return
	}
	c.dirty = false
	data := c.encodeState()
	c.mu.Unlock()
	if err := writeFileAtomic(c.opts.StateFile, data); err != nil {
		c.log.Error("saving cluster state failed", "file", c.opts.StateFile, "err", err)
		c.mu.Lock()
		c.dirty = true
		c.mu.Unlock()
	}
}

func (c *Cluster) encodeState() []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "# Shardis cluster state. Written by the server; don't edit while it runs.\n")
	fmt.Fprintf(&b, "myself %s\nepoch %d\nvote %d\n", c.myself.id, c.currentEpoch, c.lastVoteEpoch)
	for _, n := range c.nodes {
		if n.flags&FlagHandshake != 0 {
			continue // unconfirmed; we'll meet it again if it's real
		}
		role, primary, name := "primary", "-", n.name
		if n.flags&FlagReplica != 0 {
			role, primary = "replica", n.primaryID
		}
		if name == "" {
			name = "-"
		}
		fmt.Fprintf(&b, "node %s %s %s %s %d %s %s %d", n.id, name, n.clientAddr, n.internalAddr, n.busPort, role, primary, n.configEpoch)
		for s := 0; s < keyslot.Count; {
			if c.owner[s] != n {
				s++
				continue
			}
			start := s
			for s < keyslot.Count && c.owner[s] == n {
				s++
			}
			fmt.Fprintf(&b, " %d-%d", start, s-1)
		}
		b.WriteByte('\n')
	}
	for s, n := range c.migrating {
		fmt.Fprintf(&b, "migrating %d %s\n", s, n.id)
	}
	for s, n := range c.importing {
		fmt.Fprintf(&b, "importing %d %s\n", s, n.id)
	}
	return []byte(b.String())
}

// loadState reads nodes.conf if it exists. ok is false for a fresh node.
func (c *Cluster) loadState() (bool, error) {
	if c.opts.StateFile == "" {
		return false, nil
	}
	f, err := os.Open(c.opts.StateFile)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	var myID string
	type slotClaim struct {
		n      *node
		ranges []string
	}
	var claims []slotClaim
	type marker struct {
		importing bool
		slot      uint16
		id        string
	}
	var markers []marker
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for line := 1; sc.Scan(); line++ {
		f := strings.Fields(sc.Text())
		if len(f) == 0 || strings.HasPrefix(f[0], "#") {
			continue
		}
		bad := fmt.Errorf("%s line %d: malformed %q", c.opts.StateFile, line, sc.Text())
		switch f[0] {
		case "myself":
			if len(f) != 2 {
				return false, bad
			}
			myID = f[1]
		case "epoch":
			if len(f) != 2 {
				return false, bad
			}
			e, err := strconv.ParseUint(f[1], 10, 64)
			if err != nil {
				return false, bad
			}
			c.currentEpoch = e
		case "vote":
			if len(f) != 2 {
				return false, bad
			}
			v, err := strconv.ParseUint(f[1], 10, 64)
			if err != nil {
				return false, bad
			}
			c.lastVoteEpoch = v
		case "node":
			if len(f) < 9 {
				return false, bad
			}
			bus, err1 := strconv.Atoi(f[5])
			epoch, err2 := strconv.ParseUint(f[8], 10, 64)
			if err1 != nil || err2 != nil || len(f[1]) != idLen {
				return false, bad
			}
			n := &node{id: f[1], clientAddr: f[3], internalAddr: f[4], busPort: bus, configEpoch: epoch, created: time.Now()}
			if f[2] != "-" {
				n.name = f[2]
			}
			if f[6] == "replica" {
				n.flags, n.primaryID = FlagReplica, f[7]
			} else {
				n.flags = FlagPrimary
			}
			c.nodes[n.id] = n
			claims = append(claims, slotClaim{n, f[9:]})
		case "migrating", "importing":
			if len(f) != 3 {
				return false, bad
			}
			s, err := strconv.Atoi(f[1])
			if err != nil || s < 0 || s >= keyslot.Count {
				return false, bad
			}
			markers = append(markers, marker{f[0] == "importing", uint16(s), f[2]})
		default:
			return false, bad
		}
	}
	if err := sc.Err(); err != nil {
		return false, err
	}
	for _, cl := range claims {
		for _, r := range cl.ranges {
			rg, err := parseRange(r)
			if err != nil {
				return false, fmt.Errorf("%s: %w", c.opts.StateFile, err)
			}
			for s := int(rg.Start); s <= int(rg.End); s++ {
				c.owner[s] = cl.n
			}
		}
	}
	for _, m := range markers {
		n := c.nodes[m.id]
		switch {
		case n == nil:
			c.log.Warn("nodes.conf: migration marker for an unknown node; dropped", "slot", m.slot, "node", m.id)
		case m.importing:
			c.importing[m.slot] = n
		default:
			c.migrating[m.slot] = n
		}
	}
	if c.myself = c.nodes[myID]; c.myself == nil {
		return false, fmt.Errorf("%s: no entry for myself %q", c.opts.StateFile, myID)
	}
	return true, nil
}

func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
