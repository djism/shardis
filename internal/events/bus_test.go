package events

import (
	"sync"
	"testing"
)

func TestDeliverAndClose(t *testing.T) {
	b := NewBus("n1")
	if b.Active() {
		t.Fatal("active with no subscribers")
	}
	s := b.Subscribe(4)
	if !b.Active() {
		t.Fatal("not active after Subscribe")
	}
	b.Emit("key_write", map[string]any{"key": "k"})
	e := <-s.C()
	if e.Type != "key_write" || e.Node != "n1" || e.Data["key"] != "k" || e.TS.IsZero() {
		t.Fatalf("bad event %+v", e)
	}
	s.Close()
	s.Close() // idempotent
	if b.Active() {
		t.Fatal("still active after Close")
	}
	if _, ok := <-s.C(); ok {
		t.Fatal("channel not closed")
	}
	b.Emit("key_write", nil) // no subscribers: must not panic
}

func TestSlowSubscriberDropsInsteadOfBlocking(t *testing.T) {
	b := NewBus("n1")
	s := b.Subscribe(2)
	defer s.Close()
	for range 10 {
		b.Emit("x", nil) // would deadlock here if Emit blocked
	}
	if s.Dropped() != 8 {
		t.Fatalf("dropped = %d, want 8", s.Dropped())
	}
}

func TestNilBus(t *testing.T) {
	var b *Bus
	if b.Active() {
		t.Fatal("nil bus active")
	}
	b.Emit("x", nil)
}

// Emitting while subscribers come and go must never send on a closed channel.
func TestConcurrentEmitAndClose(t *testing.T) {
	b := NewBus("n1")
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					b.Emit("x", nil)
				}
			}
		})
	}
	for range 200 {
		s := b.Subscribe(1)
		s.Close()
	}
	close(stop)
	wg.Wait()
}

// The cost callers pay on every key write when nobody is watching.
func BenchmarkGuardedEmitNoSubscribers(b *testing.B) {
	bus := NewBus("n1")
	b.ReportAllocs()
	for b.Loop() {
		if bus.Active() {
			bus.Emit("key_write", map[string]any{"key": "k"})
		}
	}
}

func BenchmarkEmitOneSubscriber(b *testing.B) {
	bus := NewBus("n1")
	s := bus.Subscribe(1024)
	defer s.Close()
	go func() {
		for range s.C() {
		}
	}()
	b.ReportAllocs()
	for b.Loop() {
		if bus.Active() {
			bus.Emit("key_write", map[string]any{"key": "k"})
		}
	}
}
