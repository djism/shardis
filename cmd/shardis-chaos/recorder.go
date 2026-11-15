package main

import (
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/djism/shardis/internal/resp"
)

// recorder collects every node's event stream (EVENTS, ADR 041) plus the
// chaos actions themselves (kill, pause, partition, ...) into one timeline.
// That timeline is the trace the website replays.
type recorder struct {
	t0      time.Time
	mu      sync.Mutex
	events  []traceEvent
	dropped int64
	stop    chan struct{}
	wg      sync.WaitGroup
}

type traceEvent struct {
	T    float64        `json:"t"` // seconds since the scenario started
	Node string         `json:"node"`
	Type string         `json:"type"`
	Data map[string]any `json:"data,omitempty"`
}

func newRecorder() *recorder { return &recorder{t0: time.Now(), stop: make(chan struct{})} }

func (r *recorder) since(t time.Time) float64 { return float64(t.Sub(r.t0).Microseconds()) / 1e6 }

// note records an action taken by the chaos tool.
func (r *recorder) note(action, node string, kv ...any) {
	d := map[string]any{"action": action}
	for i := 0; i+1 < len(kv); i += 2 {
		d[kv[i].(string)] = kv[i+1]
	}
	r.mu.Lock()
	r.events = append(r.events, traceEvent{T: r.since(time.Now()), Node: node, Type: "chaos", Data: d})
	r.mu.Unlock()
}

// follow subscribes to one node's events, reconnecting after it restarts,
// until the recorder stops. name maps the node's address to its name.
func (r *recorder) follow(addr, name string) {
	r.wg.Go(func() {
		for {
			select {
			case <-r.stop:
				return
			default:
			}
			r.stream(addr, name)
			select {
			case <-r.stop:
				return
			case <-time.After(50 * time.Millisecond): // reconnect fast: a restarted node reports its first events within ms
			}
		}
	})
}

func (r *recorder) stream(addr, name string) {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return
	}
	defer c.Close()
	go func() { <-r.stop; c.Close() }() // unblock the read when we stop
	w, rd := resp.NewWriter(c), resp.NewReader(c)
	w.WriteCommand([]byte("EVENTS"))
	if w.Flush() != nil {
		return
	}
	if v, err := rd.ReadValue(); err != nil || string(v.Str) != "OK" {
		return
	}
	for {
		v, err := rd.ReadValue()
		if err != nil {
			return
		}
		var e struct {
			TS   time.Time      `json:"ts"`
			Type string         `json:"type"`
			Data map[string]any `json:"data"`
		}
		if json.Unmarshal(v.Str, &e) != nil {
			continue
		}
		r.mu.Lock()
		if e.Type == "events_dropped" {
			if n, ok := e.Data["count"].(float64); ok {
				r.dropped += int64(n)
			}
		}
		r.events = append(r.events, traceEvent{T: r.since(e.TS), Node: name, Type: e.Type, Data: e.Data})
		r.mu.Unlock()
	}
}

func (r *recorder) close() {
	close(r.stop)
	r.wg.Wait()
}
