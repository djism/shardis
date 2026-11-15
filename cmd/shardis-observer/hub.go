package main

import (
	"encoding/json"
	"sync"
	"sync/atomic"
)

// hub fans messages out to every WebSocket client. A client that can't keep
// up loses messages (counted) rather than slowing the others or the
// observer down, the same rule as the node's event bus (ADR 007).
type hub struct {
	mu      sync.Mutex
	clients map[*wsClient]struct{}
	dropped atomic.Int64
}

type wsClient struct{ send chan []byte }

func newHub() *hub { return &hub{clients: map[*wsClient]struct{}{}} }

func (h *hub) add() *wsClient {
	c := &wsClient{send: make(chan []byte, 1024)}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	return c
}

func (h *hub) remove(c *wsClient) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

func (h *hub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// broadcast sends v (marshalled once) to every client without blocking.
func (h *hub) broadcast(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c.send <- b:
		default:
			h.dropped.Add(1)
		}
	}
}
