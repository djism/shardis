package store

import "sync"

// executor is Redis's model: one goroutine owns the data and runs every
// command, one after another. Atomicity is free (nothing runs concurrently)
// and there are no locks. The cost is a hand-off per command (send to the
// executor, wait for it to finish) and that data work uses a single core.
// ADR 005 measured it against the sharded engine, which won.
//
//	conn goroutines ──fn──►  ┌──────────┐
//	conn goroutines ──fn──►  │ executor │  runs fn(tx), signals done
//	conn goroutines ──fn──►  └──────────┘
type executor struct {
	tx    Tx // locked == nil: the executor may touch anything
	log   logRef
	clock func() int64
	reqs  chan *request
	done  chan struct{}
}

type request struct {
	fn   func(*Tx)
	done chan struct{}
}

// Requests are pooled so a command doesn't allocate a fresh channel each time.
var requestPool = sync.Pool{New: func() any { return &request{done: make(chan struct{}, 1)} }}

func newExecutor(opts Options) *executor {
	e := &executor{
		tx:    Tx{ks: opts.newKeyspace(), bus: opts.Bus},
		clock: opts.clock(),
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
		l := e.log.get()
		e.tx.now, e.tx.logging = e.clock(), l != nil
		r.fn(&e.tx)
		e.tx.flushEffects(l)
		r.done <- struct{}{}
	}
}

// Do ignores keys: with one goroutine, every command already has exclusive access.
func (e *executor) Do(_ [][]byte, fn func(*Tx)) { e.run(fn) }
func (e *executor) DoAll(fn func(*Tx))          { e.run(fn) }

func (e *executor) DoGroup(g int, fn func(*Tx, []uint16)) {
	e.run(func(tx *Tx) { fn(tx, groupSlots[g]) })
}

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

func (e *executor) Name() string        { return EngineExecutor }
func (e *executor) Stats() Stats        { return e.tx.ks.stats() }
func (e *executor) SetLog(l Log)        { e.log.set(l) }
func (e *executor) keyspace() *keyspace { return e.tx.ks }
