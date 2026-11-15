package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/store"
)

func startServerWithBus(t *testing.T) (string, *events.Bus) {
	t.Helper()
	bus := events.NewBus("test-node")
	engine, _ := store.New(store.EngineSharded, store.Options{Bus: bus})
	s := New(Options{Engine: engine, Bus: bus})
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done; engine.Close() })
	return s.Addr().String(), bus
}

// nextEvent reads one streamed event (a JSON bulk string).
func nextEvent(t *testing.T, c *testClient) events.Event {
	t.Helper()
	c.c.SetReadDeadline(time.Now().Add(3 * time.Second))
	v, err := c.r.ReadValue()
	if err != nil {
		t.Fatalf("reading event: %v", err)
	}
	var e events.Event
	if err := json.Unmarshal(v.Str, &e); err != nil {
		t.Fatalf("event %q: %v", v.Str, err)
	}
	return e
}

func TestEventsStream(t *testing.T) {
	addr, bus := startServerWithBus(t)
	sub, cmd := newClient(t, addr), newClient(t, addr)
	if got := sub.do("EVENTS"); got != "OK" {
		t.Fatalf("EVENTS = %q", got)
	}
	time.Sleep(50 * time.Millisecond) // let the takeover subscribe
	if bus.WantsKeyWrites() {
		t.Fatal("a default EVENTS stream made the store produce key_write events")
	}
	cmd.do("SET", "k", "v") // key_write: excluded by default
	cmd.do("FLUSHALL")      // flushall: streamed
	if e := nextEvent(t, sub); e.Type != "flushall" || e.Node != "test-node" {
		t.Fatalf("first event = %+v, want flushall (key_write must be filtered)", e)
	}

	all := newClient(t, addr)
	all.do("EVENTS", "ALL")
	time.Sleep(50 * time.Millisecond)
	if !bus.WantsKeyWrites() {
		t.Fatal("EVENTS ALL didn't turn key_write events on")
	}
	cmd.do("SET", "k2", "v")
	if e := nextEvent(t, all); e.Type != events.KeyWrite || !strings.Contains(e.Data["key"].(string), "k2") {
		t.Fatalf("EVENTS ALL got %+v, want key_write for k2", e)
	}
	all.c.Close()
	time.Sleep(100 * time.Millisecond)
	if bus.WantsKeyWrites() {
		t.Fatal("key_write still produced after the only ALL subscriber hung up")
	}
	if got := cmd.do("EVENTS", "BOGUS"); !strings.HasPrefix(got, "ERR syntax") {
		t.Fatalf("EVENTS BOGUS = %q", got)
	}
}
