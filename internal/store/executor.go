package store

import (
	"sync"

	"github.com/djism/shardis/internal/events"
)

// executor is Redis's model: one goroutine owns the data and runs every
// command, one after another. Atomicity is free (nothing runs concurrently)
// and there are no locks. The cost is a hand-off per command (send to the
// executor, wait for it to finish) and that data work uses a single core.
//
//	conn goroutines ──fn──►  ┌──────────┐
//	conn goroutines ──fn──►  │ executor │  runs fn(tx), signals done
//	conn goroutines ──fn──►  └──────────┘
type executor struct {
	tx   Tx // locked == nil: the executor may touch anything
	reqs chan *request
	done chan struct{}
}

type request struct {
	fn   func(*Tx)
	done chan struct{}
}

// Requests are pooled so a command doesn't allocate a fresh channel each time.
var requestPool = sync.Pool{New: func() any { return &request{done: make(chan struct{}, 1)} }}

func newExecutor(bus *events.Bus) *executor {
	e := &executor{
		tx: Tx{ks: new(keyspace), bus: bus},
		// Buffered so many connections can queue work without each one
		// waiting for the executor to pick it up individually.
		reqs: make(chan *request, 1024),
		done: make(chan struct{}),
	}
	go e.loop()
	return e
}

func (e *executor) loop() {
	defer close(e.done)
	for r := range e.reqs {
		r.fn(&e.tx)
		r.done <- struct{}{}
	}
}

// Do ignores keys: with one goroutine, every command already has exclusive access.
func (e *executor) Do(_ [][]byte, fn func(*Tx)) { e.run(fn) }
func (e *executor) DoAll(fn func(*Tx))          { e.run(fn) }

func (e *executor) run(fn func(*Tx)) {
	r := requestPool.Get().(*request)
	r.fn = fn
	e.reqs <- r
	<-r.done // also makes everything fn wrote visible to us (happens-before)
	r.fn = nil
	requestPool.Put(r)
}

func (e *executor) Close() {
	close(e.reqs)
	<-e.done
}

func (e *executor) Name() string { return EngineExecutor }
