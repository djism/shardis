package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djism/shardis/internal/persist"
	"github.com/djism/shardis/internal/store"
)

// startAOFServer runs a server with persistence in dir. stop shuts it down
// cleanly (as SIGTERM would): server, then AOF, then engine.
func startAOFServer(t *testing.T, dir string, fsync persist.Fsync) (addr string, stop func()) {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine, err := store.New(store.EngineSharded, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	aof, err := persist.Open(persist.Options{Dir: dir, Fsync: fsync, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := aof.Load(engine); err != nil {
		t.Fatal(err)
	}
	if err := aof.Start(engine); err != nil {
		t.Fatal(err)
	}
	s := New(Options{Engine: engine, AOF: aof, Hz: 10, Log: quiet})
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Serve(ctx); close(done) }()
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		<-done
		aof.Close()
		engine.Close()
	}
	t.Cleanup(stop)
	return s.Addr().String(), stop
}

func TestDataSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	addr, stop := startAOFServer(t, dir, persist.FsyncEverySec)
	c := newClient(t, addr)
	c.do("SET", "name", "shardis")
	c.do("SET", "temp", "x", "EX", "1000")
	c.do("INCRBY", "counter", "41")
	c.do("INCR", "counter")
	c.do("MSET", "a", "1", "b", "2")
	c.do("DEL", "b")
	stop()

	addr2, _ := startAOFServer(t, dir, persist.FsyncEverySec)
	c2 := newClient(t, addr2)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"GET", "name"}, `"shardis"`},
		{[]string{"GET", "counter"}, `"42"`},
		{[]string{"GET", "a"}, `"1"`},
		{[]string{"GET", "b"}, "(nil)"},
		{[]string{"TTL", "temp"}, "(int) 1000"},
	} {
		if got := c2.do(tc.args...); got != tc.want {
			t.Errorf("%v after restart = %s, want %s", tc.args, got, tc.want)
		}
	}
}

// ADR 015, checked directly: the moment a client gets "OK", the write is
// already in the AOF file (handed to the kernel), whatever the policy.
func TestReplyMeansWrittenToAOF(t *testing.T) {
	for _, f := range []persist.Fsync{persist.FsyncNo, persist.FsyncEverySec, persist.FsyncAlways} {
		t.Run(f.String(), func(t *testing.T) {
			dir := t.TempDir()
			addr, _ := startAOFServer(t, dir, f)
			c := newClient(t, addr)
			for i := range 200 {
				key := fmt.Sprintf("durable-%d", i)
				if got := c.do("SET", key, "v"); got != "OK" {
					t.Fatal(got)
				}
				data, err := os.ReadFile(filepath.Join(dir, "appendonlydir", "shardis.aof.1.incr.aof"))
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(data), key) {
					t.Fatalf("got OK for %s but it isn't in the AOF yet", key)
				}
			}
		})
	}
}

func TestBgRewriteAOFAndInfo(t *testing.T) {
	dir := t.TempDir()
	addr, stop := startAOFServer(t, dir, persist.FsyncEverySec)
	c := newClient(t, addr)
	for i := range 100 {
		for k := range 20 {
			c.do("SET", fmt.Sprint("k", k), fmt.Sprint(i))
		}
	}
	before := infoInt(t, parseInfo(t, c.do("INFO", "persistence")), "aof_current_size")
	if got := c.do("BGREWRITEAOF"); got != "Background append only file rewriting started" {
		t.Fatal(got)
	}
	deadline := time.Now().Add(10 * time.Second)
	var info map[string]string
	for {
		info = parseInfo(t, c.do("INFO", "persistence"))
		if info["aof_rewrite_in_progress"] == "0" && info["aof_rewrites"] == "1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rewrite didn't finish: %v", info)
		}
		time.Sleep(10 * time.Millisecond)
	}
	after := infoInt(t, info, "aof_current_size")
	if after >= before/5 || info["aof_last_bgrewrite_status"] != "ok" || info["aof_enabled"] != "1" {
		t.Fatalf("size %d → %d, info %v", before, after, info)
	}
	if got := c.do("CONFIG", "GET", "appendonly"); got != `["appendonly" "yes"]` {
		t.Errorf("CONFIG GET appendonly = %s", got)
	}
	if got := c.do("CONFIG", "SET", "appendfsync", "always"); got != "OK" {
		t.Errorf("CONFIG SET appendfsync = %s", got)
	}
	c.do("SET", "after-rewrite", "yes")
	stop()
	addr2, _ := startAOFServer(t, dir, persist.FsyncEverySec)
	c2 := newClient(t, addr2)
	if got := c2.do("GET", "k7"); got != `"99"` {
		t.Errorf("k7 after rewrite + restart = %s", got)
	}
	if got := c2.do("GET", "after-rewrite"); got != `"yes"` {
		t.Errorf("after-rewrite = %s", got)
	}
}

func TestBgRewriteWithoutAOF(t *testing.T) {
	addr, _ := startServer(t)
	if got := newClient(t, addr).do("BGREWRITEAOF"); !strings.HasPrefix(got, "ERR AOF is not enabled") {
		t.Fatal(got)
	}
}
