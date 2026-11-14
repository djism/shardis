package store

import (
	"math/bits"
	"sort"
	"sync/atomic"
)

// Memory accounting (ADR 011). Asking the Go runtime how big the heap is
// costs a lot (runtime.ReadMemStats pauses the program) and the answer
// includes garbage not yet collected, so it jumps around. Instead every write
// and delete adds or subtracts an *estimate*, built from two parts:
//
//  1. Each key's own allocations: the key string and the value bytes, rounded
//     up to Go's allocator size classes (+ an exact-LRU node if enabled).
//  2. The slot maps themselves. A Go map's capacity is a power of two (at
//     least 8) and never shrinks, so it follows from the most entries the map
//     has held (its peak, which we track anyway for compaction, ADR 010).
//     Modeling this matters: with few keys per slot, the 16,384 mostly-empty
//     maps cost more than the keys in them.
//
// The constants are measured, not guessed: TestMemoryEstimateMatchesHeap and
// TestMemoryEstimateAtOtherSizes compare the estimate with real heap growth.
const (
	// Bytes per slot in a map: string key (16) + entry (40), plus 1 control byte.
	entrySlotBytes = 16 + 40 + 1
	// Bytes per slot in a TTL-index map (map[string]struct{}): the string key
	// (16) padded to 24, because Go pads a struct whose last field has size
	// zero so a pointer to that field can't point past the allocation.
	// Measured: 27.9 B/slot incl. headers. + 1 control byte.
	volatileSlotBytes = 24 + 1
	// Fixed cost per map: map header, table and directory for larger maps.
	mapHeaderBytes = 64
	// Exact LRU: a list element (~48 B) plus its index map entry and spare room.
	exactOverhead = 107
)

// mapCapacity is the slot count of a Go map that has held at most peak
// entries: small maps hold 8, larger ones double whenever they'd be more
// than 7/8 full.
func mapCapacity(peak int32) int64 {
	if peak <= 0 {
		return 0
	}
	if peak <= 8 {
		return 8
	}
	need := (int64(peak)*8 + 6) / 7 // ceil(peak / (7/8))
	return 1 << bits.Len64(uint64(need-1))
}

// mapBytes estimates a map's memory. Its slots live in one array that the
// allocator rounds up to a size class too: 128 slots of 57 B = 7,296 B, which
// really takes 8,192 B.
func mapBytes(peak int32, slotBytes int64) int64 {
	if peak <= 0 {
		return 0
	}
	return mapHeaderBytes + allocSize(int(mapCapacity(peak)*slotBytes))
}

// memoryStats are per-keyspace counters. Hits/misses happen on every read
// from every core, so they're split per shard (each on its own cache line):
// one shared counter would make every GET contend on the same memory.
type memoryStats struct {
	used    atomic.Int64 // estimated bytes
	expired atomic.Int64
	evicted atomic.Int64
	byShard [numShards]shardCounters
}

type shardCounters struct {
	hits, misses atomic.Int64
	_            [128 - 16]byte // pad to a cache line (see paddedMutex)
}

func (m *memoryStats) add(delta int64) { m.used.Add(delta) }

func (m *memoryStats) countLookup(s uint16, hit bool) {
	c := &m.byShard[shardOf(s)]
	if hit {
		c.hits.Add(1)
	} else {
		c.misses.Add(1)
	}
}

// entrySize is what one key costs beyond its map slot.
func (ks *keyspace) entrySize(k string, e entry) int64 {
	n := allocSize(len(k)) + allocSize(cap(e.val))
	if ks.exact != nil {
		n += exactOverhead
	}
	return n
}

// setPeak records a new peak for slot s's map and accounts for the change
// in the map's estimated size (growing, being rebuilt, or being dropped).
func (ks *keyspace) setPeak(s uint16, peak int32) {
	ks.mem.add(mapBytes(peak, entrySlotBytes) - mapBytes(ks.peak[s], entrySlotBytes))
	ks.peak[s] = peak
}

func (ks *keyspace) setVPeak(s uint16, peak int32) {
	ks.mem.add(mapBytes(peak, volatileSlotBytes) - mapBytes(ks.vpeak[s], volatileSlotBytes))
	ks.vpeak[s] = peak
}

// Stats is a snapshot of the keyspace counters, for INFO.
type Stats struct {
	UsedMemory     int64 // estimate, bytes
	ExpiredKeys    int64
	EvictedKeys    int64
	KeyspaceHits   int64
	KeyspaceMisses int64
}

func (ks *keyspace) stats() Stats {
	st := Stats{
		UsedMemory:  ks.mem.used.Load(),
		ExpiredKeys: ks.mem.expired.Load(),
		EvictedKeys: ks.mem.evicted.Load(),
	}
	for i := range ks.mem.byShard {
		st.KeyspaceHits += ks.mem.byShard[i].hits.Load()
		st.KeyspaceMisses += ks.mem.byShard[i].misses.Load()
	}
	return st
}

// Go's allocator rounds small allocations up to one of these size classes
// (runtime/sizeclasses.go). A 100-byte value really takes 112 bytes.
var sizeClasses = []int{
	8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256,
	288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024, 1152, 1280,
	1408, 1536, 1792, 2048, 2304, 2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528,
	6784, 6912, 8192, 9472, 9728, 10240, 10880, 12288, 13568, 14336, 16384, 18432,
	19072, 20480, 21760, 24576, 27264, 28672, 32768,
}

// allocSize estimates the bytes the allocator really uses for n bytes.
func allocSize(n int) int64 {
	switch {
	case n == 0:
		return 0
	case n < 16:
		// Go packs tiny pointer-free objects (like short strings) together
		// into 16-byte blocks, so they cost about their own size.
		return int64(n)
	case n > 32768:
		return int64((n + 8191) &^ 8191) // large objects: whole 8 KB pages
	}
	i := sort.SearchInts(sizeClasses, n)
	return int64(sizeClasses[i])
}
