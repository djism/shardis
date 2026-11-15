// Package events is the node's internal event bus. Components report what
// they're doing (key writes now; gossip, votes, failovers later), and
// observers (the website stream, the trace recorder, tests) subscribe.
//
// Two rules shape the design (ADR 007):
//  1. Free when nobody listens. Callers guard with `if bus.Active()`, a
//     single atomic load, so they don't even build the event otherwise.
//  2. Never slow the cache down. Delivery is non-blocking: if a subscriber's
//     buffer is full, the event is dropped and counted, not waited on.
package events

import (
	"sync"
	"sync/atomic"
	"time"
)

// Event is one thing that happened on a node. The JSON shape is what the
// website and the trace files consume.
type Event struct {
	TS   time.Time      `json:"ts"`
	Node string         `json:"node"`
	Type string         `json:"type"`
	Data map[string]any `json:"data,omitempty"`
}

// Bus fans events out to subscribers. A nil *Bus is valid and inactive,
// so components can be built without one (handy in tests).
type Bus struct {
	node string

	active     atomic.Int32 // number of subscribers; read on every Active() call
	keyWriters atomic.Int32 // subscribers that want key_write (the one high-volume type)

	mu   sync.RWMutex // guards subs; Emit holds it for reading while sending
	subs map[*Subscription]struct{}
}

func NewBus(node string) *Bus {
	return &Bus{node: node, subs: make(map[*Subscription]struct{})}
}

// Active reports whether anyone is subscribed. Call it before building an
// event so the no-listener path costs one atomic load and no allocations.
func (b *Bus) Active() bool { return b != nil && b.active.Load() > 0 }

// KeyWrite is the per-key write event, emitted on every write. It's the one
// type worth filtering at the source: a stream that doesn't want it
// mustn't make the store build 100k events a second just to drop them.
const KeyWrite = "key_write"

// WantsKeyWrites reports whether any subscriber wants key_write events.
// The store checks this instead of Active().
func (b *Bus) WantsKeyWrites() bool { return b != nil && b.keyWriters.Load() > 0 }

// Emit delivers an event to every subscriber without blocking.
func (b *Bus) Emit(typ string, data map[string]any) {
	if !b.Active() {
		return
	}
	e := Event{TS: time.Now(), Node: b.node, Type: typ, Data: data}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for s := range b.subs {
		if s.exclude[typ] {
			continue
		}
		select {
		case s.ch <- e:
		default:
			s.dropped.Add(1) // slow subscriber: drop rather than stall the cache
		}
	}
}

// Subscription is one listener. Read events from C().
type Subscription struct {
	bus     *Bus
	ch      chan Event
	exclude map[string]bool // types this subscriber doesn't want (fixed at Subscribe)
	dropped atomic.Uint64
	once    sync.Once
}

// Subscribe registers a listener for every event type, with room for
// buffer pending events.
func (b *Bus) Subscribe(buffer int) *Subscription { return b.SubscribeExcept(buffer, nil) }

// SubscribeExcept registers a listener for every type except those in
// exclude. Excluding KeyWrite also stops the store from producing them, if
// no other subscriber wants them.
func (b *Bus) SubscribeExcept(buffer int, exclude map[string]bool) *Subscription {
	s := &Subscription{bus: b, ch: make(chan Event, buffer), exclude: exclude}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.active.Add(1)
	if !exclude[KeyWrite] {
		b.keyWriters.Add(1)
	}
	b.mu.Unlock()
	return s
}

func (s *Subscription) C() <-chan Event { return s.ch }

// Dropped is how many events this subscriber missed because it fell behind.
// Recorders report it so a trace with gaps says so.
func (s *Subscription) Dropped() uint64 { return s.dropped.Load() }

// Close unsubscribes and closes C(). Safe to call more than once. Taking the
// write lock guarantees no Emit is mid-send to this channel when we close it.
func (s *Subscription) Close() {
	s.once.Do(func() {
		s.bus.mu.Lock()
		delete(s.bus.subs, s)
		s.bus.active.Add(-1)
		if !s.exclude[KeyWrite] {
			s.bus.keyWriters.Add(-1)
		}
		s.bus.mu.Unlock()
		close(s.ch)
	})
}
