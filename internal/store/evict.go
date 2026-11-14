package store

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// Policy says which keys to throw away when memory is full.
type Policy int32

const (
	NoEviction      Policy = iota // refuse writes when full (Redis's default)
	AllKeysLRU                    // evict the least recently used key (approximate)
	AllKeysRandom                 // evict any key
	VolatileTTL                   // evict the key with a TTL that expires soonest
	AllKeysLRUExact               // exact LRU, for comparison only (not a Redis policy; ADR 012)
)

var policyNames = [...]string{"noeviction", "allkeys-lru", "allkeys-random", "volatile-ttl", "allkeys-lru-exact"}

func (p Policy) String() string { return policyNames[p] }

// ParsePolicy accepts the Redis policy names (case-insensitive).
func ParsePolicy(s string) (Policy, error) {
	for i, n := range policyNames {
		if strings.EqualFold(s, n) {
			return Policy(i), nil
		}
	}
	return 0, fmt.Errorf("unknown maxmemory policy %q (want one of %s)", s, strings.Join(policyNames[:], ", "))
}

// ErrOOM is returned when a write can't run because memory is full and
// nothing can (or may) be evicted. Its text is Redis's exact error.
var ErrOOM = errors.New("OOM command not allowed when used memory > 'maxmemory'.")

// evictionPoolSize is how many good candidates we remember between
// evictions, like Redis's eviction pool. Each eviction adds a fresh sample;
// the pool lets a great candidate found earlier still be used later.
const evictionPoolSize = 16

// Evictor enforces maxmemory (ADR 013). The server calls Reserve before
// every command that may grow memory. If we're over the limit, it evicts
// keys *before* the command takes its locks. Eviction must touch other
// shards, and doing that while holding a command's locks could deadlock.
//
// Settings can change at runtime (CONFIG SET), so they're atomics.
type Evictor struct {
	engine Engine
	ks     *keyspace

	maxmemory atomic.Int64 // bytes, 0 = no limit
	policy    atomic.Int32
	samples   atomic.Int32

	mu   sync.Mutex // one eviction at a time; guards pool and rng
	pool []candidate
	rng  *rand.Rand
}

type candidate struct {
	key   string
	slot  uint16
	score int64 // higher = better to evict
}

// NewEvictor creates an evictor for e. AllKeysLRUExact needs an engine
// created with Options.ExactLRU.
func NewEvictor(e Engine, maxmemory int64, p Policy, samples int) (*Evictor, error) {
	x := &Evictor{engine: e, ks: e.keyspace(), rng: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))}
	x.SetMaxMemory(maxmemory)
	if err := x.SetPolicy(p); err != nil {
		return nil, err
	}
	if err := x.SetSamples(samples); err != nil {
		return nil, err
	}
	return x, nil
}

func (x *Evictor) SetMaxMemory(n int64) { x.maxmemory.Store(max(n, 0)) }
func (x *Evictor) MaxMemory() int64     { return x.maxmemory.Load() }
func (x *Evictor) Policy() Policy       { return Policy(x.policy.Load()) }
func (x *Evictor) Samples() int         { return int(x.samples.Load()) }

func (x *Evictor) SetPolicy(p Policy) error {
	if p < NoEviction || int(p) >= len(policyNames) {
		return fmt.Errorf("invalid policy %d", p)
	}
	if (p == AllKeysLRUExact) != (x.ks.exact != nil) {
		// The exact list must track every key from the start; it can't be
		// switched on (or usefully off) later.
		return errors.New("allkeys-lru-exact can only be chosen at startup (--maxmemory-policy)")
	}
	x.policy.Store(int32(p))
	return nil
}

func (x *Evictor) SetSamples(n int) error {
	if n < 1 || n > 64 {
		return fmt.Errorf("maxmemory-samples %d out of range 1-64", n)
	}
	x.samples.Store(int32(n))
	return nil
}

// Reserve makes room for a write. It returns nil if we're under the limit
// (or got there by evicting), and ErrOOM if the policy forbids eviction or
// there's nothing left to evict. The common case, no limit or plenty of room,
// is two atomic loads.
func (x *Evictor) Reserve() error {
	limit := x.maxmemory.Load()
	if limit == 0 || x.ks.mem.used.Load() <= limit {
		return nil
	}
	p := x.Policy()
	if p == NoEviction {
		return ErrOOM
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	for x.ks.mem.used.Load() > limit {
		if !x.evictOne(p) {
			return ErrOOM // e.g. volatile-ttl with no TTL keys left
		}
	}
	return nil
}

// evictOne frees one key. Callers hold x.mu.
func (x *Evictor) evictOne(p Policy) bool {
	switch p {
	case AllKeysLRUExact:
		return x.evictExact()
	case AllKeysRandom:
		return x.evictRandom()
	default: // AllKeysLRU, VolatileTTL: sample into the pool, evict the best
		for range 3 { // a pool entry may be stale (key deleted since); retry a few times
			x.sampleIntoPool(p)
			if len(x.pool) == 0 {
				return false // nothing to sample: keyspace (or TTL index) is empty
			}
			best := x.pool[len(x.pool)-1]
			x.pool = x.pool[:len(x.pool)-1]
			if x.evictKey(best.slot, best.key, p) {
				return true
			}
		}
		return false
	}
}

func (x *Evictor) evictKey(slot uint16, key string, p Policy) (ok bool) {
	x.engine.DoGroup(shardOf(slot), func(tx *Tx, _ []uint16) { ok = tx.evict(slot, key, p) })
	return ok
}

func (x *Evictor) evictExact() bool {
	for range 3 {
		it, ok := x.ks.exact.oldest()
		if !ok {
			return false
		}
		if x.evictKey(it.slot, it.key, AllKeysLRUExact) {
			return true
		}
	}
	return false
}

func (x *Evictor) evictRandom() bool {
	var slot uint16
	var key string
	found := x.sample(AllKeysRandom, 1, func(k string, s uint16, _ int64) { key, slot = k, s })
	return found && x.evictKey(slot, key, AllKeysRandom)
}

// sampleIntoPool takes one fresh sample and merges it into the pool.
func (x *Evictor) sampleIntoPool(p Policy) {
	x.sample(p, x.Samples(), func(k string, s uint16, score int64) { x.offer(candidate{k, s, score}) })
}

// sample finds a random non-empty slot and reports up to n keys from it with
// their eviction score. It locks one group at a time and starts at a random
// group and slot, so samples are spread over the whole keyspace. Returns
// false if there's nothing to sample.
func (x *Evictor) sample(p Policy, n int, fn func(key string, slot uint16, score int64)) bool {
	start := x.rng.IntN(NumGroups)
	for i := range NumGroups {
		g := (start + i) % NumGroups
		found := false
		x.engine.DoGroup(g, func(tx *Tx, slots []uint16) {
			off := x.rng.IntN(len(slots))
			for j := range slots {
				s := slots[(off+j)%len(slots)]
				if tx.sampleForEviction(s, p, n, func(k string, score int64) { fn(k, s, score) }) > 0 {
					found = true
					return
				}
			}
		})
		if found {
			return true
		}
	}
	return false
}

// offer adds c to the pool, which stays sorted by score (best last) and
// holds at most evictionPoolSize candidates.
func (x *Evictor) offer(c candidate) {
	if i := slices.IndexFunc(x.pool, func(o candidate) bool { return o.key == c.key }); i >= 0 {
		x.pool = slices.Delete(x.pool, i, i+1) // re-insert with the fresh score
	}
	if len(x.pool) == evictionPoolSize {
		if c.score <= x.pool[0].score {
			return // worse than everything we already have
		}
		x.pool = x.pool[1:]
	}
	i, _ := slices.BinarySearchFunc(x.pool, c.score, func(o candidate, s int64) int {
		switch {
		case o.score < s:
			return -1
		case o.score > s:
			return 1
		}
		return 0
	})
	x.pool = slices.Insert(x.pool, i, c)
}

// sampleForEviction reports up to n keys from slot s with their eviction
// score (higher = better victim), and returns how many it reported.
//   - LRU: seconds since last use.
//   - volatile-ttl: sooner deadline = higher score; only keys with a TTL.
//   - random: score unused.
func (t *Tx) sampleForEviction(s uint16, p Policy, n int, fn func(key string, score int64)) int {
	t.checkSlot(s)
	got := 0
	if p == VolatileTTL {
		for k := range t.ks.volatile[s] {
			if got == n {
				break
			}
			fn(k, -t.ks.slots[s][k].expireAt)
			got++
		}
		return got
	}
	now := t.nowSec()
	for k, e := range t.ks.slots[s] {
		if got == n {
			break
		}
		fn(k, int64(now)-int64(e.lru))
		got++
	}
	return got
}
