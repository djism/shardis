package store

import (
	"fmt"

	"github.com/djism/shardis/internal/events"
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
	// Close stops the engine. No Do/DoAll calls may happen after it.
	Close()
	Name() string
}

// Engine names accepted by New (and by the --engine flag).
const (
	EngineExecutor = "executor"
	EngineSharded  = "sharded"
)

// Engines lists the valid engine names.
var Engines = []string{EngineExecutor, EngineSharded}

func New(kind string, bus *events.Bus) (Engine, error) {
	switch kind {
	case EngineExecutor:
		return newExecutor(bus), nil
	case EngineSharded:
		return newSharded(bus), nil
	default:
		return nil, fmt.Errorf("unknown engine %q (want one of %v)", kind, Engines)
	}
}
