package store

import (
	"math/bits"
	"sync"

	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/keyslot"
)

// numShards locks cover the 16384 slots: slot s is guarded by lock s % 256.
// More shards = less contention but a costlier DoAll (it takes every lock).
const numShards = 256

func shardOf(slot uint16) int { return int(slot) % numShards }

// shardSet is a 256-bit set of shard numbers.
type shardSet [numShards / 64]uint64

func (s *shardSet) add(i int)      { s[i/64] |= 1 << (i % 64) }
func (s *shardSet) has(i int) bool { return s[i/64]&(1<<(i%64)) != 0 }

// paddedMutex keeps each lock on its own cache line. Without padding, 8-16
// mutexes share one line, and cores fighting over *different* locks still
// slow each other down ("false sharing"). 128 bytes covers Apple M-series lines.
type paddedMutex struct {
	sync.Mutex
	_ [128 - 8]byte
}

// sharded runs commands directly on the calling connection's goroutine,
// under per-shard locks. Many cores work at once, but atomicity now depends
// on locking correctly, which is where the subtle bugs live:
//
//   - Multi-key commands lock several shards. Two commands locking the same
//     two shards in opposite orders would deadlock (each holds one, waits for
//     the other), so we always lock in ascending shard order.
//   - A command must lock every key it touches. Tx panics if it doesn't.
type sharded struct {
	ks    keyspace
	bus   *events.Bus
	locks [numShards]paddedMutex
}

func newSharded(bus *events.Bus) *sharded { return &sharded{bus: bus} }

func (s *sharded) Do(keys [][]byte, fn func(*Tx)) {
	var set shardSet
	for _, k := range keys {
		set.add(shardOf(keyslot.Of(k)))
	}
	s.lockSet(&set)
	defer s.unlockSet(&set)
	tx := Tx{ks: &s.ks, bus: s.bus, locked: &set}
	fn(&tx)
}

func (s *sharded) DoAll(fn func(*Tx)) {
	for i := range s.locks {
		s.locks[i].Lock() // ascending, same order as Do, so no deadlock
	}
	defer func() {
		for i := range s.locks {
			s.locks[i].Unlock()
		}
	}()
	tx := Tx{ks: &s.ks, bus: s.bus}
	fn(&tx)
}

// lockSet takes the locks in ascending shard order. Every caller uses the
// same global order, so no two callers can each hold a lock the other needs.
func (s *sharded) lockSet(set *shardSet) {
	for w, word := range set {
		for word != 0 {
			i := bits.TrailingZeros64(word)
			s.locks[w*64+i].Lock()
			word &= word - 1 // clear lowest set bit
		}
	}
}

func (s *sharded) unlockSet(set *shardSet) {
	for w, word := range set {
		for word != 0 {
			i := bits.TrailingZeros64(word)
			s.locks[w*64+i].Unlock()
			word &= word - 1
		}
	}
}

func (s *sharded) Close()       {}
func (s *sharded) Name() string { return EngineSharded }
