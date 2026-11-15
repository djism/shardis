// Command shardis-observer watches a running cluster and streams what it
// sees to browsers over WebSocket (Iteration 15, ADR 046): every node's
// internal events (via EVENTS) plus a topology snapshot every second. The
// showcase website's live mode connects here. In local mode it can also
// trigger admin actions (kill, pause, partition, add node, rebalance).
//
//	shardis-observer -seed 127.0.0.1:7001 -topology deploy/local/topology.conf
//	shardis-observer -seed 127.0.0.1:7001 -local-actions    # allow actions (local cluster only)
//
//	GET  /ws            WebSocket: {"kind":"topology"|"event"|"action", ...}
//	GET  /api/topology  the latest topology snapshot
//	POST /api/actions   {"action":"kill","node":"a"}   (403 unless -local-actions)
//	GET  /              a tiny live page (events + topology), until the website exists
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

func main() {
	seed := flag.String("seed", "127.0.0.1:7001", "any cluster node (comma-separated for several)")
	listen := flag.String("listen", "127.0.0.1:8080", "HTTP/WebSocket address (loopback by default)")
	topo := flag.String("topology", "deploy/local/topology.conf", "seed topology file for friendly node names (optional)")
	local := flag.Bool("local-actions", false, "allow admin actions (kill, pause, partition, add node, rebalance) on a LOCAL cluster")
	pidDir := flag.String("pid-dir", ".cluster-local", "where deploy/local/cluster.sh writes pid files")
	script := flag.String("cluster-script", "deploy/local/cluster.sh", "local cluster script (restart action)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	o := newObserver(strings.Split(*seed, ","), loadNames(*topo))
	go o.run()
	srv := &http.Server{Addr: *listen, Handler: handler(o, &actions{enabled: *local, pidDir: *pidDir, script: *script, o: o}),
		ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); close(o.stop); srv.Close() }()
	slog.Info("observer listening", "addr", "http://"+*listen, "local_actions", *local)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, "shardis-observer:", err)
		os.Exit(1)
	}
}

func newObserver(seeds []string, names map[string]string) *observer {
	return &observer{seeds: seeds, names: names, hub: newHub(), follow: map[string]bool{}, stop: make(chan struct{})}
}

func handler(o *observer, a *actions) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) { serveWS(o, w, r) })
	mux.HandleFunc("/api/topology", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(o.topology())
	})
	mux.HandleFunc("/api/actions", a.handle)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(page))
	})
	return mux
}

// serveWS sends the current topology, then everything the hub broadcasts.
func serveWS(o *observer, w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// The website's dev server and this page; not arbitrary sites.
		OriginPatterns: []string{"localhost:*", "127.0.0.1:*"},
	})
	if err != nil {
		return
	}
	defer c.CloseNow()
	ctx := c.CloseRead(r.Context()) // we only send; this handles pings and close
	client := o.hub.add()
	defer o.hub.remove(client)
	first, _ := json.Marshal(map[string]any{"kind": "topology", "t": time.Now().UnixMilli(), "nodes": o.topology()})
	if c.Write(ctx, websocket.MessageText, first) != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-client.send:
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.Write(wctx, websocket.MessageText, b)
			cancel()
			if err != nil {
				return
			}
		}
	}
}
