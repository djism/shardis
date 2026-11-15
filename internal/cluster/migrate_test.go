package cluster

import (
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

// newOffline builds node `me` from the sample topology without opening the bus.
func newOffline(t *testing.T, me, state string) *Cluster {
	t.Helper()
	seed, err := Parse(strings.NewReader(sample), me)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(Options{ClientAddr: seed.Me.Addr, InternalAddr: "127.0.0.1:1", Seed: seed, StateFile: state,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSetSlotRules(t *testing.T) {
	a := newOffline(t, "a", "")
	b, d := NodeID("b"), NodeID("d")
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"migrate a slot we don't own", a.SetSlotMigrating(6000, b), "not the owner"},
		{"import a slot we own", a.SetSlotImporting(100, b), "already the owner"},
		{"migrate to a replica", a.SetSlotMigrating(100, d), "not a master"},
		{"migrate to an unknown node", a.SetSlotMigrating(100, NodeID("zz")), "don't know"},
		{"give away a slot that still has keys", a.SetSlotNode(100, b, 3), "still hold keys"},
	} {
		if tc.err == nil || !strings.Contains(tc.err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", tc.name, tc.err, tc.want)
		}
	}
	if a.View().MigratingTo(100) != nil {
		t.Fatal("a refused SETSLOT still left a marker")
	}
}

// Markers are saved in nodes.conf, so a restarted node is still mid-migration
// (and `shardis-admin fix` can find and finish the slot).
func TestMigrationMarkersSurviveRestart(t *testing.T) {
	state := filepath.Join(t.TempDir(), "nodes.conf")
	a := newOffline(t, "a", state)
	if err := a.SetSlotMigrating(100, NodeID("b")); err != nil {
		t.Fatal(err)
	}
	if err := a.SetSlotImporting(6000, NodeID("b")); err != nil {
		t.Fatal(err)
	}
	a.saveIfDirty()
	again := newOffline(t, "a", state)
	v := again.View()
	if to := v.MigratingTo(100); to == nil || to.Name != "b" {
		t.Fatalf("after restart, slot 100 migrating to %v", to)
	}
	if from := v.ImportingFrom(6000); from == nil || from.Name != "b" {
		t.Fatalf("after restart, slot 6000 importing from %v", from)
	}
	if err := again.SetSlotStable(100); err != nil || again.View().MigratingTo(100) != nil {
		t.Fatalf("STABLE didn't clear the marker (err %v)", err)
	}
}

// Taking an imported slot bumps our config epoch above everyone's, once.
func TestImportBumpsEpochOnce(t *testing.T) {
	b := newOffline(t, "b", "")
	a := NodeID("a")
	before := b.View().Me.Epoch
	for _, s := range []uint16{100, 101} {
		if err := b.SetSlotImporting(s, a); err != nil {
			t.Fatal(err)
		}
		if err := b.SetSlotNode(s, NodeID("b"), 0); err != nil {
			t.Fatal(err)
		}
	}
	v := b.View()
	var others uint64
	for _, n := range v.Nodes {
		if n != v.Me {
			others = max(others, n.Epoch)
		}
	}
	// Sample epochs are a=1 b=2 c=3: b jumps to 4 (above c) on the first
	// import and stays there on the second.
	if v.Me.Epoch != 4 || before != 2 || others != 3 {
		t.Fatalf("epoch went %d -> %d (others' max %d), want 2 -> 4", before, v.Me.Epoch, others)
	}
	if v.Owner(100) != v.Me || v.Owner(101) != v.Me || v.ImportingFrom(100) != nil {
		t.Fatal("slots not owned / importing marker left")
	}
}
