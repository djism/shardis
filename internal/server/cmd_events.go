package server

import (
	"encoding/json"
	"net"
	"strings"
	"time"

	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/resp"
)

// EVENTS [ALL | EXCLUDE type ...]
//
// Turns the connection into a live stream of this node's internal events
// (gossip, PFAIL/FAIL, votes, failovers, slot moves, replication links, ...),
// one JSON object per RESP bulk string, like Redis's MONITOR. The chaos
// recorder and (later) the website observer subscribe to every node this
// way (ADR 041).
//
// By default the per-key event (key_write) is left out: at 100k ops/s it
// would swamp the stream, and the recorder measures client operations
// itself. ALL includes it; EXCLUDE drops more types.
//
// Subscribing is free for the node when nobody does (ADR 007), and a slow
// subscriber loses events (counted, reported as an "events_dropped" event)
// rather than slowing the cache down.
func cmdEvents(cl *client, args [][]byte) {
	exclude := map[string]bool{events.KeyWrite: true}
	for i := 1; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "ALL":
			exclude = map[string]bool{}
		case "EXCLUDE":
			for _, t := range args[i+1:] {
				exclude[strings.ToLower(string(t))] = true
			}
			i = len(args)
		default:
			cl.w.WriteError("ERR syntax error: EVENTS [ALL | EXCLUDE type ...]")
			return
		}
	}
	if cl.srv.bus == nil {
		cl.w.WriteError("ERR this node has no event bus")
		return
	}
	cl.w.WriteSimple("OK")
	bus := cl.srv.bus
	cl.takeover = func(c net.Conn, r *resp.Reader) { streamEvents(c, r, bus.SubscribeExcept(16384, exclude)) }
}

func streamEvents(c net.Conn, r *resp.Reader, sub *events.Subscription) {
	defer sub.Close()
	// The subscriber sends nothing more; a read returning means it hung up
	// (or the server is shutting down and closed the connection).
	gone := make(chan struct{})
	go func() {
		for {
			if _, err := r.ReadCommand(); err != nil {
				close(gone)
				return
			}
		}
	}()
	w := resp.NewWriter(c)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var reported uint64
	for {
		select {
		case <-gone:
			return
		case e, ok := <-sub.C():
			if !ok {
				return
			}
			b, err := json.Marshal(e)
			if err != nil {
				continue
			}
			w.WriteBulk(b)
			if len(sub.C()) == 0 { // batch writes while events are queued
				c.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if w.Flush() != nil {
					return
				}
			}
		case <-tick.C:
			if d := sub.Dropped(); d > reported {
				b, _ := json.Marshal(map[string]any{"ts": time.Now(), "type": "events_dropped", "data": map[string]any{"count": d - reported}})
				reported = d
				w.WriteBulk(b)
				c.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if w.Flush() != nil {
					return
				}
			}
		}
	}
}
