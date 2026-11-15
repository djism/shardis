package main

import (
	"fmt"
	"testing"
)

func TestPlanEvensOutSlots(t *testing.T) {
	// The default 3-primary layout plus one empty primary.
	ranges := [][2]int{{0, 5460}, {5461, 10922}, {10923, 16383}, {1, 0}}
	var ps []*node
	for i, r := range ranges {
		n := &node{id: fmt.Sprintf("%040d", i), addr: fmt.Sprint("n", i)}
		for s := r[0]; s <= r[1]; s++ {
			n.slots = append(n.slots, s)
		}
		ps = append(ps, n)
	}
	moves := plan(ps)
	if len(moves) != 4096 {
		t.Fatalf("moved %d slots, want exactly 4096 (the new node's share, nothing more)", len(moves))
	}
	count := map[*node]int{}
	for _, p := range ps {
		count[p] = len(p.slots)
	}
	seen := map[int]bool{}
	for _, m := range moves {
		if m.dst != ps[3] {
			t.Fatalf("a slot moved to %s, not to the empty node", m.dst.addr)
		}
		if seen[m.slot] {
			t.Fatalf("slot %d moved twice", m.slot)
		}
		seen[m.slot] = true
		count[m.src]--
		count[m.dst]++
	}
	for _, p := range ps {
		if count[p] != 4096 {
			t.Fatalf("%s ends with %d slots, want 4096", p.addr, count[p])
		}
	}
	if again := plan([]*node{{id: "x", slots: make([]int, 8192)}, {id: "y", slots: make([]int, 8192)}}); len(again) != 0 {
		t.Fatalf("balanced cluster: %d moves planned", len(again))
	}
}
