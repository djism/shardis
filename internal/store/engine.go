package store

import (
	"fmt"
	"time"

	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/keyslot"
)

// Engine runs functions against the keyspace atomically.
//
// Rules for fn (both engines rely on them):
//   - Only touch the keys you declared. The sharded engine enforces this.
//   - Do no I/O and don't block: in the executor, a slow fn stalls every
//     client. Compute the result inside fn; write the reply after Do returns.
type Engine interface {
	// Do runs fn with exclusive access to keys.
	Do(keys [][]byte, fn func(tx *Tx))
	// DoAll runs fn with exclusive access to the whole keyspace.
	DoAll(fn func(tx *Tx))
	// DoGroup runs fn with exclusive access to one group of slots (the
	// slots behind one lock). Background work such as active expiry uses
	// it to process the keyspace a slice at a time, so clients only ever
	// wait for one small group, never for the whole keyspace.
	DoGroup(g int, fn func(tx *Tx, slots []uint16))
	// Close stops the engine. No calls may happen after it.
	Close()
	Name() string
	// Stats returns memory and hit/miss counters. Safe to call any time.
	Stats() Stats
	// SetLog starts (or, with nil, stops) recording effects to l. Call it
	// after loading the AOF, so replayed effects aren't logged again.
	SetLog(l Log)
	// SetReplicaMode turns replica behavior on or off (ADR 019).
	SetReplicaMode(on bool)

	keyspace() *keyspace // for the Evictor, which lives in this package
}

// Options configures an engine. The zero value is valid.
type Options struct {
	Bus *events.Bus // nil = no events
	// Clock returns the current time in Unix milliseconds. Tests swap in a
	// fake clock so they can jump time forward instead of sleeping.
	Clock func() int64
	// ExactLRU keeps an exact LRU list of every key, needed for the
	// allkeys-lru-exact policy. Costs ~112 bytes per key and a global lock
	// on every access (ADR 012); off by default.
	ExactLRU bool
}

func (o Options) newKeyspace() *keyspace {
	ks := new(keyspace)
	if o.ExactLRU {
		ks.exact = newExactLRU()
	}
	return ks
}

func (o Options) clock() func() int64 {
	if o.Clock != nil {
		return o.Clock
	}
	return func() int64 { return time.Now().UnixMilli() }
}

// Engine names accepted by New (and by the --engine flag).
const (
	EngineExecutor = "executor"
	EngineSharded  = "sharded"
)

// Engines lists the valid engine names.
var Engines = []string{EngineExecutor, EngineSharded}

func New(kind string, opts Options) (Engine, error) {
	switch kind {
	case EngineExecutor:
		return newExecutor(opts), nil
	case EngineSharded:
		return newSharded(opts), nil
	default:
		return nil, fmt.Errorf("unknown engine %q (want one of %v)", kind, Engines)
	}
}

// NumGroups is how many slot groups DoGroup accepts. Both engines use the
// same split as the sharded engine's locks: group g holds slots g, g+256, ...
const NumGroups = numShards

var groupSlots = func() (gs [NumGroups][]uint16) {
	for s := range keyslot.Count {
		g := shardOf(uint16(s))
		gs[g] = append(gs[g], uint16(s))
	}
	return gs
}()
