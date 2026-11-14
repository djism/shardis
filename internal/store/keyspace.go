// Package store is the in-memory keyspace and the engines that control
// concurrent access to it.
//
// Layout (ADR 006): keys are grouped by hash slot, one small map per slot,
// created only when the first key arrives. That gives SCAN a stable cursor
// (the slot number), and later lets the cluster count, list and migrate the
// keys of a single slot without touching the rest.
//
// Concurrency (ADR 005): commands never touch the keyspace directly. They
// call Engine.Do(keys, fn) and fn gets a *Tx. The engine guarantees nothing
// else touches those keys while fn runs, so every command is atomic, the way
// it is in Redis. Two engines implement that guarantee differently:
// a single executor goroutine, or 256 locks.
package store

import (
	"fmt"

	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/keyslot"
)

// entry is what we store per key. A struct (not a bare []byte) so expiry
// and LRU fields can be added later without changing every call site.
type entry struct {
	// val is immutable once stored. Commands copy the slice header out of
	// the critical section and write it to the client after the engine lets
	// go, so nobody may modify these bytes in place. Updates replace the slice.
	val []byte
}

// keyspace is all the data on this node.
type keyspace struct {
	slots [keyslot.Count]map[string]entry
}

// Tx is a command's view of the keyspace while it holds the engine's
// guarantee. It is only valid inside the fn passed to Do/DoAll.
type Tx struct {
	ks  *keyspace
	bus *events.Bus

	// locked is the set of shards this Tx may touch, or nil for "all".
	// The sharded engine sets it; touching a key outside it panics, so a
	// command that forgets to declare a key fails loudly in tests instead
	// of silently racing in production.
	locked *shardSet
}

func (t *Tx) slotOf(key []byte) uint16 {
	s := keyslot.Of(key)
	if t.locked != nil && !t.locked.has(shardOf(s)) {
		panic(fmt.Sprintf("store: key %q (slot %d) touched without being declared to Do", key, s))
	}
	return s
}

func (t *Tx) requireAll() {
	if t.locked != nil {
		panic("store: whole-keyspace operation inside Do; use DoAll")
	}
}

// Get returns the value for key. The returned slice must not be modified.
func (t *Tx) Get(key []byte) ([]byte, bool) {
	e, ok := t.ks.slots[t.slotOf(key)][string(key)] // a nil map reads as empty
	return e.val, ok
}

func (t *Tx) Exists(key []byte) bool {
	_, ok := t.ks.slots[t.slotOf(key)][string(key)]
	return ok
}

// Set stores val under key. The store keeps val, so the caller must not
// modify it afterwards (RESP arguments are freshly allocated, so that holds).
func (t *Tx) Set(key, val []byte) {
	s := t.slotOf(key)
	m := t.ks.slots[s]
	if m == nil {
		m = make(map[string]entry)
		t.ks.slots[s] = m
	}
	m[string(key)] = entry{val: val}
	t.emitWrite("set", key, s)
}

// Delete removes key and reports whether it existed.
func (t *Tx) Delete(key []byte) bool {
	s := t.slotOf(key)
	m := t.ks.slots[s]
	if _, ok := m[string(key)]; !ok {
		return false
	}
	delete(m, string(key))
	if len(m) == 0 {
		// Go maps never shrink, so drop empty ones to give memory back
		// after mass deletes.
		t.ks.slots[s] = nil
	}
	t.emitWrite("del", key, s)
	return true
}

// Len counts all keys. Needs DoAll.
func (t *Tx) Len() int {
	t.requireAll()
	n := 0
	for _, m := range t.ks.slots {
		n += len(m)
	}
	return n
}

// ForEach calls fn for every key. Needs DoAll. Blocks the whole keyspace for
// as long as it runs, which is why KEYS is dangerous on big datasets.
func (t *Tx) ForEach(fn func(key string)) {
	t.requireAll()
	for _, m := range t.ks.slots {
		for k := range m {
			fn(k)
		}
	}
}

// Scan visits whole slots starting at cursor until it has seen at least
// count keys, and returns the cursor to continue from (0 = finished).
// Because a key's slot never changes, any key that exists for the whole
// scan is visited exactly once. Needs DoAll.
func (t *Tx) Scan(cursor uint16, count int, fn func(key string)) (next uint16) {
	t.requireAll()
	seen := 0
	for s := int(cursor); s < keyslot.Count; s++ {
		for k := range t.ks.slots[s] {
			fn(k)
		}
		seen += len(t.ks.slots[s])
		if seen >= count && s+1 < keyslot.Count {
			return uint16(s + 1)
		}
	}
	return 0
}

// Flush deletes everything. Needs DoAll.
func (t *Tx) Flush() {
	t.requireAll()
	for i := range t.ks.slots {
		t.ks.slots[i] = nil
	}
	if t.bus.Active() {
		t.bus.Emit("flushall", nil)
	}
}

func (t *Tx) emitWrite(op string, key []byte, slot uint16) {
	if t.bus.Active() { // one atomic load when nobody's listening
		t.bus.Emit("key_write", map[string]any{"op": op, "key": string(key), "slot": slot})
	}
}
