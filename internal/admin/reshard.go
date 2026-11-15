package admin

import (
	"fmt"
	"io"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

// MoveOptions tunes slot moves.
type MoveOptions struct {
	Batch     int       // keys per MIGRATE (default 100)
	TimeoutMs int       // MIGRATE timeout (default 10000)
	Verbose   bool      // log every slot
	Fix       bool      // MIGRATE with REPLACE: finishing a crashed run (see Fix)
	Out       io.Writer // progress messages; nil = discard
}

func (o *MoveOptions) defaults() {
	if o.Batch == 0 {
		o.Batch = 100
	}
	if o.TimeoutMs == 0 {
		o.TimeoutMs = 10000
	}
	if o.Out == nil {
		o.Out = io.Discard
	}
}

// moveSlot moves one slot from src to dst while clients keep using it. The
// order of the steps is the whole trick (ADR 032):
//
//  1. dst IMPORTING first. If src started sending -ASK before dst was ready,
//     dst would answer MOVED-back-to-src, and the client would bounce.
//  2. src MIGRATING: from now on, keys src no longer has get -ASK dst.
//  3. MIGRATE the keys in batches until the slot is empty on src.
//  4. dst NODE dst first: dst owns the slot (and bumps its epoch, so its
//     claim beats src's in gossip). Clients still sent to src get -ASK, and
//     dst serves them.
//  5. src NODE dst: src now answers MOVED dst.
//  6. Every other primary NODE dst, so they stop sending clients to src. (Gossip
//     would get there too, but this is faster.)
func moveSlot(v *view, src, dst *node, slot int, o MoveOptions) (keys int, err error) {
	s := strconv.Itoa(slot)
	sc, err := v.conn(src.addr)
	if err != nil {
		return 0, err
	}
	dc, err := v.conn(dst.addr)
	if err != nil {
		return 0, err
	}
	if src == dst || slices.Contains(dst.slots, slot) {
		// A run crashed after step 4: dst already owns the slot. Only
		// steps 5-6 are left.
		return 0, finishSlot(v, sc, src, dst, s)
	}
	if _, err := dc.do("CLUSTER", "SETSLOT", s, "IMPORTING", src.id); err != nil {
		return 0, err
	}
	if _, err := sc.do("CLUSTER", "SETSLOT", s, "MIGRATING", dst.id); err != nil {
		return 0, err
	}
	host, port, _ := net.SplitHostPort(dst.addr)
	for {
		kv, err := sc.do("CLUSTER", "GETKEYSINSLOT", s, strconv.Itoa(o.Batch))
		if err != nil {
			return keys, err
		}
		if len(kv.Elems) == 0 {
			break
		}
		args := [][]byte{[]byte("MIGRATE"), []byte(host), []byte(port), {}, []byte("0"), []byte(strconv.Itoa(o.TimeoutMs))}
		if o.Fix {
			args = append(args, []byte("REPLACE"))
		}
		args = append(args, []byte("KEYS"))
		for _, k := range kv.Elems {
			args = append(args, k.Str)
		}
		if _, err := sc.doBytes(args...); err != nil {
			if strings.Contains(err.Error(), "BUSYKEY") && !o.Fix {
				return keys, fmt.Errorf("slot %d: %w (the target already has some of these keys; run `shardis-admin fix` to overwrite them with the source's copy)", slot, err)
			}
			return keys, fmt.Errorf("slot %d: %w", slot, err)
		}
		keys += len(kv.Elems)
	}
	if _, err := dc.do("CLUSTER", "SETSLOT", s, "NODE", dst.id); err != nil {
		return keys, err
	}
	if err := finishSlot(v, sc, src, dst, s); err != nil {
		return keys, err
	}
	if o.Verbose {
		fmt.Fprintf(o.Out, "slot %d: %s -> %s, %d keys\n", slot, src.short(), dst.short(), keys)
	}
	return keys, nil
}

// finishSlot is steps 5 and 6: the source, then everyone else, learn the
// new owner.
func finishSlot(v *view, sc *conn, src, dst *node, s string) error {
	if _, err := sc.do("CLUSTER", "SETSLOT", s, "NODE", dst.id); err != nil {
		return err
	}
	for _, p := range v.primaries {
		if p == src || p == dst {
			continue
		}
		c, err := v.conn(p.addr)
		if err != nil {
			continue // it'll learn by gossip
		}
		if _, err := c.do("CLUSTER", "SETSLOT", s, "NODE", dst.id); err != nil && !strings.Contains(err.Error(), "SETSLOT") {
			v.drop(p.addr) // a network error: reconnect next time
		}
	}
	return nil
}

type move struct {
	src, dst *node
	slot     int
}

func run(v *view, moves []move, o MoveOptions) error {
	total := 0
	for i, m := range moves {
		k, err := moveSlot(v, m.src, m.dst, m.slot, o)
		total += k
		if err != nil {
			return fmt.Errorf("after %d of %d slots: %w", i, len(moves), err)
		}
		if !o.Verbose && (i+1)%256 == 0 {
			fmt.Fprintf(o.Out, "%d/%d slots moved (%d keys)\n", i+1, len(moves), total)
		}
	}
	fmt.Fprintf(o.Out, "moved %d slots, %d keys\n", len(moves), total)
	return nil
}

// Reshard moves n slots from one primary to another.
func Reshard(addr, fromID, toID string, n int, o MoveOptions) error {
	o.defaults()
	v, err := load(addr)
	if err != nil {
		return err
	}
	if err := refuseOpenSlots(v); err != nil {
		return err
	}
	if err := v.waitAgreement(30 * time.Second); err != nil {
		return err
	}
	src, err := v.find(fromID)
	if err != nil {
		return err
	}
	dst, err := v.find(toID)
	if err != nil {
		return err
	}
	if n <= 0 || n > len(src.slots) {
		return fmt.Errorf("-slots must be 1..%d (what %s owns)", len(src.slots), src.short())
	}
	var moves []move
	for _, s := range src.slots[len(src.slots)-n:] {
		moves = append(moves, move{src, dst, s})
	}
	return run(v, moves, o)
}

// Rebalance gives every primary (empty ones included) an equal share of the
// 16384 slots, moving as few slots as possible: primaries above their share
// give their highest slots to primaries below it.
func Rebalance(addr string, o MoveOptions) error {
	o.defaults()
	v, err := load(addr)
	if err != nil {
		return err
	}
	if err := refuseOpenSlots(v); err != nil {
		return err
	}
	if err := v.waitAgreement(30 * time.Second); err != nil {
		return err
	}
	moves := plan(v.primaries)
	if len(moves) == 0 {
		fmt.Fprintln(o.Out, "already balanced")
		return nil
	}
	fmt.Fprintf(o.Out, "moving %d slots across %d primaries\n", len(moves), len(v.primaries))
	return run(v, moves, o)
}

// plan computes the moves for rebalance. A pure function (no network), so
// it's unit-tested on its own.
func plan(primaries []*node) []move {
	ps := slices.Clone(primaries)
	slices.SortFunc(ps, func(a, b *node) int { return strings.Compare(a.id, b.id) })
	want := map[*node]int{}
	for i, p := range ps {
		want[p] = 16384 / len(ps)
		if i < 16384%len(ps) {
			want[p]++
		}
	}
	owned := map[*node][]int{}
	for _, p := range ps {
		owned[p] = slices.Clone(p.slots)
	}
	var moves []move
	for _, dst := range ps {
		for len(owned[dst]) < want[dst] {
			// Take from whoever is furthest above their share.
			var src *node
			for _, p := range ps {
				if len(owned[p]) > want[p] && (src == nil || len(owned[p])-want[p] > len(owned[src])-want[src]) {
					src = p
				}
			}
			if src == nil {
				break
			}
			last := owned[src][len(owned[src])-1]
			owned[src] = owned[src][:len(owned[src])-1]
			owned[dst] = append(owned[dst], last)
			moves = append(moves, move{src, dst, last})
		}
	}
	return moves
}

func refuseOpenSlots(v *view) error {
	for _, p := range v.primaries {
		if len(p.migrating)+len(p.importing) > 0 {
			return fmt.Errorf("%s has slots half-moved (a previous run was interrupted): run `shardis-admin fix` first", p.short())
		}
	}
	return nil
}

// Fix finishes every half-moved slot. A slot is "open" if its owner is
// MIGRATING it or some node is IMPORTING it. We finish the move toward the
// node that was importing (or the migration target): keys left on the
// source are moved with REPLACE, because the source's copy is the newest. While
// the slot was MIGRATING, the source served every key it still had, so a
// copy the target holds from an interrupted MIGRATE can only be older.
//
// The source is always the slot's *current* owner, not the node named in
// the marker. If the original source died mid-move and its replica took
// over (reshard-during-failover), the replica owns the slot now. It has
// the source's unmigrated keys (by replication) but none of its markers,
// since markers aren't replicated (ADR 032).
func Fix(addr string, o MoveOptions) error {
	o.defaults()
	o.Fix = true
	v, err := load(addr)
	if err != nil {
		return err
	}
	owner := map[int]*node{}
	for _, p := range v.primaries {
		for _, s := range p.slots {
			owner[s] = p
		}
	}
	type open struct{ src, dst *node }
	slotsOpen := map[int]open{}
	for _, p := range v.primaries {
		for s, id := range p.migrating {
			dst := v.byID[id]
			if dst == nil || !slices.Contains(v.primaries, dst) {
				// The target is gone. Cancel the move: the slot stays where it is.
				// Keys already sent to the dead target are only on its
				// replica now, which doesn't own the slot: reported, not recovered.
				fmt.Fprintf(o.Out, "slot %d: target %s is gone; cancelling the move (keys already moved there may be stranded)\n", s, id[:8])
				if c, err := v.conn(p.addr); err == nil {
					c.do("CLUSTER", "SETSLOT", strconv.Itoa(s), "STABLE")
				}
				continue
			}
			slotsOpen[s] = open{p, dst}
		}
		for s := range p.importing {
			src := owner[s]
			if src == nil {
				fmt.Fprintf(o.Out, "slot %d: no healthy owner; skipped\n", s)
				continue
			}
			slotsOpen[s] = open{src, p}
		}
	}
	if len(slotsOpen) == 0 {
		fmt.Fprintln(o.Out, "no open slots")
		return nil
	}
	var moves []move
	for _, s := range slices.Sorted(maps.Keys(slotsOpen)) {
		moves = append(moves, move{slotsOpen[s].src, slotsOpen[s].dst, s})
	}
	fmt.Fprintf(o.Out, "finishing %d half-moved slot(s)\n", len(moves))
	return run(v, moves, o)
}
