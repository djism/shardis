package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/djism/shardis/internal/testcluster"
)

type msg struct {
	Kind  string         `json:"kind"`
	Type  string         `json:"type"`
	Node  string         `json:"node"`
	Nodes []Node         `json:"nodes"`
	Data  map[string]any `json:"data"`
}

func TestObserverStreamsTopologyAndEvents(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{})
	names := map[string]string{}
	for n, node := range tc.Nodes {
		names[node.Addr] = n
	}
	o := newObserver([]string{tc.Nodes["b"].Addr}, names)
	go o.run()
	defer close(o.stop)
	srv := httptest.NewServer(handler(o, &actions{o: o}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	c.SetReadLimit(1 << 20)
	read := func() msg {
		_, b, err := c.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var m msg
		json.Unmarshal(b, &m)
		return m
	}
	// First a topology with all six nodes, by name.
	for {
		m := read()
		if m.Kind == "topology" && len(m.Nodes) == 6 {
			primaries := 0
			for _, n := range m.Nodes {
				if n.Role == "primary" && len(n.Slots) > 0 {
					primaries++
				}
			}
			if primaries != 3 || names[m.Nodes[0].Addr] != m.Nodes[0].Name {
				t.Fatalf("topology: %+v", m.Nodes)
			}
			break
		}
	}
	// Kill primary a: the stream must show the failover as it happens.
	time.Sleep(500 * time.Millisecond) // let the event streams connect
	tc.Nodes["a"].Stop()
	seen := map[string]bool{}
	for !seen["failover_won"] {
		m := read()
		if m.Kind == "event" {
			seen[m.Type] = true
		}
	}
	if !seen["pfail"] && !seen["fail"] {
		t.Fatalf("failover_won without pfail/fail first: %v", seen)
	}
}

func TestActionsAreOffByDefault(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{})
	o := newObserver([]string{tc.Nodes["b"].Addr}, nil)
	o.refresh()
	off := httptest.NewServer(handler(o, &actions{o: o}))
	defer off.Close()
	resp, err := http.Post(off.URL+"/api/actions", "application/json", strings.NewReader(`{"action":"heal"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("actions without -local-actions: status %d, want 403", resp.StatusCode)
	}
	on := httptest.NewServer(handler(o, &actions{o: o, enabled: true}))
	defer on.Close()
	for _, body := range []string{`{"action":"partition","nodes":["` + tc.Nodes["c"].Cluster.View().Me.ID + `"]}`, `{"action":"heal"}`} {
		resp, err := http.Post(on.URL+"/api/actions", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		var r map[string]any
		json.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		if r["ok"] != true {
			t.Fatalf("%s: %v", body, r)
		}
	}
	resp, _ = http.Post(on.URL+"/api/actions", "application/json", strings.NewReader(`{"action":"kill","node":"../../etc"}`))
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("path-like node name accepted: %d", resp.StatusCode)
	}
}
