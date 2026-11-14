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
//
// Expiry (ADR 008, 009): a key with a TTL stores an absolute deadline. Reads
// delete it lazily once the deadline has passed, and a background loop
// (expire.go) samples keys with TTLs to clean up the ones nobody reads.
package store

import (
	"fmt"

	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/keyslot"
)

// entry is what we store per key.
type entry struct {
	// val is immutable once stored. Commands copy the slice header out of
	// the critical section and write it to the client after the engine lets
	// go, so nobody may modify these bytes in place. Updates replace the slice.
	val []byte

	// expireAt is the absolute deadline in Unix milliseconds, 0 = never.
	// Absolute rather than "N ms from now" so the deadline means the same
	// thing after a restart, or when forwarded to a replica (ADR 009).
	expireAt int64
}

func (e entry) expiredAt(now int64) bool { return e.expireAt != 0 && e.expireAt <= now }

// keyspace is all the data on this node.
type keyspace struct {
	slots [keyslot.Count]map[string]entry

	// volatile[s] holds the keys in slot s that have a TTL. Only the active
	// expiry loop reads it, to sample keys that might be expired without
	// wading through the (often much larger) set of keys that never expire.
	// Its strings share memory with the keys in slots[s].
	volatile [keyslot.Count]map[string]struct{}

	// peak[s] is the most keys slots[s] has held since it was last rebuilt
	// (vpeak[s] the same for volatile[s]). Go maps never shrink: a map that
	// once held 1000 keys keeps the memory for 1000 even after 990 are
	// deleted. The background loop (expire.go) calls compact, which rebuilds
	// a map smaller once it has fallen below 1/4 of its peak. Doing it there
	// rather than on every delete means a slot being drained by an expiry
	// storm is rebuilt once, after it settles, not over and over mid-storm.
	peak, vpeak [keyslot.Count]int32
}

// Maps smaller than this aren't worth rebuilding.
const shrinkMinPeak = 16

// shrunk returns a right-sized copy of m if it has fallen below a quarter of
// its peak, and the new peak. Copying is O(len), and it can only happen after
// at least 3/4 of peak deletions, so the cost per delete stays O(1) on average.
// An empty map is dropped entirely (nil).
func shrunk[V any](m map[string]V, peak int32) (map[string]V, int32) {
	if len(m) == 0 {
		return nil, 0
	}
	if peak < shrinkMinPeak || int32(len(m))*4 >= peak {
		return m, peak
	}
	nm := make(map[string]V, len(m))
	for k, v := range m {
		nm[k] = v
	}
	return nm, int32(len(nm))
}

// Tx is a command's view of the keyspace while it holds the engine's
// guarantee. It is only valid inside the fn passed to Do/DoAll/DoGroup.
type Tx struct {
	ks  *keyspace
	bus *events.Bus

	// now is read once per Tx, so time stands still for the length of a
	// command: a key can't be alive at the start of MGET and expired by
	// the end. Redis caches the time per command for the same reason.
	now int64

	// locked is the set of shards this Tx may touch, or nil for "all".
	// The sharded engine sets it; touching a key outside it panics, so a
	// command that forgets to declare a key fails loudly in tests instead
	// of silently racing in production.
	locked *shardSet
}

// Now is the current time (Unix ms) as seen by this command.
func (t *Tx) Now() int64 { return t.now }

func (t *Tx) slotOf(key []byte) uint16 {
	s := keyslot.Of(key)
	t.checkSlot(s)
	return s
}

func (t *Tx) checkSlot(s uint16) {
	if t.locked != nil && !t.locked.has(shardOf(s)) {
		panic(fmt.Sprintf("store: slot %d touched without holding its lock", s))
	}
}

func (t *Tx) requireAll() {
	if t.locked != nil {
		panic("store: whole-keyspace operation inside Do; use DoAll")
	}
}

// lookup finds a live entry, deleting it first if it has expired (lazy expiry).
func (t *Tx) lookup(key []byte) (uint16, entry, bool) {
	s := t.slotOf(key)
	e, ok := t.ks.slots[s][string(key)] // a nil map reads as empty
	if ok && e.expiredAt(t.now) {
		t.expire(s, string(key), "lazy")
		return s, entry{}, false
	}
	return s, e, ok
}

// Get returns the value for key. The returned slice must not be modified.
func (t *Tx) Get(key []byte) ([]byte, bool) {
	_, e, ok := t.lookup(key)
	return e.val, ok
}

func (t *Tx) Exists(key []byte) bool {
	_, _, ok := t.lookup(key)
	return ok
}

// ExpireAt returns the key's deadline (0 = no TTL) and whether the key exists.
func (t *Tx) ExpireAt(key []byte) (int64, bool) {
	_, e, ok := t.lookup(key)
	return e.expireAt, ok
}

// Set stores val under key and removes any TTL, like a plain Redis SET.
// The store keeps val, so the caller must not modify it afterwards (RESP
// arguments are freshly allocated, so that holds).
func (t *Tx) Set(key, val []byte) { t.SetWithExpire(key, val, 0) }

// SetKeepTTL stores val but keeps the key's existing TTL (INCR, SET KEEPTTL).
func (t *Tx) SetKeepTTL(key, val []byte) {
	at, _ := t.ExpireAt(key) // 0 if missing or expired
	t.SetWithExpire(key, val, at)
}

// SetWithExpire stores val with deadline at (Unix ms, 0 = none).
func (t *Tx) SetWithExpire(key, val []byte, at int64) {
	s := t.slotOf(key)
	m := t.ks.slots[s]
	if m == nil {
		m = make(map[string]entry)
		t.ks.slots[s] = m
	}
	k := string(key) // one allocation, shared by both maps
	m[k] = entry{val: val, expireAt: at}
	t.ks.peak[s] = max(t.ks.peak[s], int32(len(m)))
	t.setVolatile(s, k, at != 0)
	t.emitWrite("set", key, s)
}

// SetExpire changes the deadline of an existing key (0 removes the TTL) and
// reports whether the key existed. A deadline at or before now deletes the
// key right away, which is what EXPIRE with a past time does in Redis.
func (t *Tx) SetExpire(key []byte, at int64) bool {
	s, e, ok := t.lookup(key)
	if !ok {
		return false
	}
	k := string(key)
	if at != 0 && at <= t.now {
		t.remove(s, k)
		t.emitWrite("del", key, s)
		return true
	}
	e.expireAt = at
	t.ks.slots[s][k] = e
	t.setVolatile(s, k, at != 0)
	return true
}

// Delete removes key and reports whether it existed.
func (t *Tx) Delete(key []byte) bool {
	s, _, ok := t.lookup(key)
	if !ok {
		return false
	}
	t.remove(s, string(key))
	t.emitWrite("del", key, s)
	return true
}

// expire removes an expired key. Every expiry, lazy or active, goes through
// here, so this is the one place replication will hook into later: Redis
// replicas don't expire keys on their own clock, they wait for the primary
// to send a DEL.
func (t *Tx) expire(s uint16, k string, how string) {
	t.remove(s, k)
	if t.bus.Active() {
		t.bus.Emit("expire", map[string]any{"key": k, "slot": s, "how": how})
	}
}

func (t *Tx) remove(s uint16, k string) {
	m := t.ks.slots[s]
	delete(m, k)
	if len(m) == 0 { // cheap and always worth it; partial shrinking waits for compact
		t.ks.slots[s], t.ks.peak[s] = nil, 0
	}
	t.setVolatile(s, k, false)
}

// compact rebuilds slot s's maps if they've become mostly empty space.
func (t *Tx) compact(s uint16) {
	t.checkSlot(s)
	t.ks.slots[s], t.ks.peak[s] = shrunk(t.ks.slots[s], t.ks.peak[s])
	t.ks.volatile[s], t.ks.vpeak[s] = shrunk(t.ks.volatile[s], t.ks.vpeak[s])
}

func (t *Tx) setVolatile(s uint16, k string, on bool) {
	vm := t.ks.volatile[s]
	if on {
		if vm == nil {
			vm = make(map[string]struct{})
			t.ks.volatile[s] = vm
		}
		vm[k] = struct{}{}
		t.ks.vpeak[s] = max(t.ks.vpeak[s], int32(len(vm)))
		return
	}
	if vm != nil {
		delete(vm, k)
		if len(vm) == 0 {
			t.ks.volatile[s], t.ks.vpeak[s] = nil, 0
		}
	}
}

// Len counts all stored keys, including expired ones not yet cleaned up
// (Redis's DBSIZE counts those too). Needs DoAll.
func (t *Tx) Len() int {
	t.requireAll()
	n := 0
	for _, m := range t.ks.slots {
		n += len(m)
	}
	return n
}

// ForEach calls fn for every live key. Needs DoAll. Blocks the whole
// keyspace for as long as it runs, which is why KEYS is dangerous on big datasets.
func (t *Tx) ForEach(fn func(key string)) {
	t.requireAll()
	for _, m := range t.ks.slots {
		for k, e := range m {
			if !e.expiredAt(t.now) {
				fn(k)
			}
		}
	}
}

// Scan visits whole slots starting at cursor until it has seen at least
// count keys, and returns the cursor to continue from (0 = finished).
// Because a key's slot never changes, any key that exists for the whole
// scan is visited exactly once. Expired keys are skipped. Needs DoAll.
func (t *Tx) Scan(cursor uint16, count int, fn func(key string)) (next uint16) {
	t.requireAll()
	seen := 0
	for s := int(cursor); s < keyslot.Count; s++ {
		for k, e := range t.ks.slots[s] {
			if !e.expiredAt(t.now) {
				fn(k)
			}
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
		t.ks.slots[i], t.ks.peak[i] = nil, 0
		t.ks.volatile[i], t.ks.vpeak[i] = nil, 0
	}
	if t.bus.Active() {
		t.bus.Emit("flushall", nil)
	}
}

// sampleExpired is one round of active expiry for slot s: look at up to max
// keys that have a TTL and delete the expired ones. Go randomizes where map
// iteration starts, so "the first max keys we range over" is a cheap random
// sample. Deleting from a map while ranging over it is allowed in Go.
func (t *Tx) sampleExpired(s uint16, max int) (sampled, expired int) {
	t.checkSlot(s)
	for k := range t.ks.volatile[s] {
		if sampled == max {
			break
		}
		sampled++
		if t.ks.slots[s][k].expiredAt(t.now) {
			t.expire(s, k, "active")
			expired++
		}
	}
	return sampled, expired
}

// hasVolatile reports whether slot s has any keys with a TTL.
func (t *Tx) hasVolatile(s uint16) bool { return len(t.ks.volatile[s]) > 0 }

func (t *Tx) emitWrite(op string, key []byte, slot uint16) {
	if t.bus.Active() { // one atomic load when nobody's listening
		t.bus.Emit("key_write", map[string]any{"op": op, "key": string(key), "slot": slot})
	}
}
