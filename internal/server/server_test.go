package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djism/shardis/internal/store"
)

// startServer runs a real server on a random localhost port and stops it when
// the test ends. It returns the address and a function that triggers shutdown
// early and reports Serve's result.
func startServer(t *testing.T) (addr string, stop func() error) {
	t.Helper()
	return startServerWith(t, store.EngineSharded)
}

func startServerWith(t *testing.T, engineKind string) (addr string, stop func() error) {
	t.Helper()
	engine, err := store.New(engineKind, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Active expiry at 100 cycles/s so tests that wait for TTLs are quick.
	expCtx, stopExpirer := context.WithCancel(context.Background())
	expDone := make(chan struct{})
	go func() { store.NewActiveExpirer(engine, 100).Run(expCtx); close(expDone) }()
	evictor, err := store.NewEvictor(engine, 0, store.NoEviction, 5)
	if err != nil {
		t.Fatal(err)
	}
	s := New(Options{Engine: engine, Evictor: evictor, Hz: 10})
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()

	var once sync.Once
	var serveErr error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case serveErr = <-done:
			case <-time.After(5 * time.Second):
				serveErr = errors.New("Serve did not return after shutdown")
			}
		})
		return serveErr
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
		stopExpirer()
		<-expDone
		engine.Close()
	})
	return s.Addr().String(), stop
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(5 * time.Second)) // a hung test fails instead of hanging CI
	t.Cleanup(func() { c.Close() })
	return c
}

// roundTrip sends raw bytes and reads exactly len(want) bytes back.
func roundTrip(t *testing.T, c net.Conn, send, want string) {
	t.Helper()
	if _, err := io.WriteString(c, send); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("send %q: reading reply: %v (got %q so far)", send, err, got)
	}
	if string(got) != want {
		t.Fatalf("send %q: got %q, want %q", send, got, want)
	}
}

func expectClosed(t *testing.T, c net.Conn) {
	t.Helper()
	if n, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("expected server to close the connection, got n=%d err=%v", n, err)
	}
}

func TestCommands(t *testing.T) {
	addr, _ := startServer(t)
	c := dial(t, addr)
	tests := []struct{ name, send, want string }{
		{"ping", "*1\r\n$4\r\nPING\r\n", "+PONG\r\n"},
		{"ping lowercase", "*1\r\n$4\r\nping\r\n", "+PONG\r\n"},
		{"ping with message", "*2\r\n$4\r\nPING\r\n$2\r\nhi\r\n", "$2\r\nhi\r\n"},
		{"ping too many args", "*3\r\n$4\r\nPING\r\n$1\r\na\r\n$1\r\nb\r\n", "-ERR wrong number of arguments for 'ping' command\r\n"},
		{"inline ping", "PING\r\n", "+PONG\r\n"},
		{"inline echo", "ECHO hello\r\n", "$5\r\nhello\r\n"},
		{"echo binary", "*2\r\n$4\r\nECHO\r\n$3\r\na\x00b\r\n", "$3\r\na\x00b\r\n"},
		{"echo no args", "*1\r\n$4\r\nECHO\r\n", "-ERR wrong number of arguments for 'echo' command\r\n"},
		{"unknown", "*2\r\n$3\r\nFOO\r\n$3\r\nbar\r\n", "-ERR unknown command 'FOO', with args beginning with: 'bar' \r\n"},
		{"command", "COMMAND\r\n", "*0\r\n"},
		{"command docs", "COMMAND DOCS\r\n", "*0\r\n"},
		{"command count", "COMMAND COUNT\r\n", fmt.Sprintf(":%d\r\n", len(New(Options{}).commands))},
		{"config get", "CONFIG GET save\r\n", "*2\r\n$4\r\nsave\r\n$0\r\n\r\n"},
		{"config get glob", "CONFIG GET APPEND*\r\n", "*4\r\n$10\r\nappendonly\r\n$2\r\nno\r\n$11\r\nappendfsync\r\n$8\r\neverysec\r\n"},
		{"config get none", "CONFIG GET nope\r\n", "*0\r\n"},
		{"config set unknown", "CONFIG SET a b\r\n", "-ERR Unknown option or number of arguments for CONFIG SET - 'a'\r\n"},
		{"command bad sub", "COMMAND NOPE\r\n", "-ERR unknown subcommand 'NOPE'. Try COMMAND HELP.\r\n"},
	}
	// All on one connection, in order: also proves a connection stays usable
	// after an error reply.
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { roundTrip(t, c, tc.send, tc.want) })
	}
}

// Pipelining: the client sends many commands without waiting for replies.
// We must answer every one, in order.
func TestPipelining(t *testing.T) {
	addr, _ := startServer(t)
	c := dial(t, addr)
	const n = 1000
	var send, want strings.Builder
	for i := range n {
		fmt.Fprintf(&send, "*2\r\n$4\r\nECHO\r\n$%d\r\n%d\r\n", len(fmt.Sprint(i)), i)
		fmt.Fprintf(&want, "$%d\r\n%d\r\n", len(fmt.Sprint(i)), i)
	}
	roundTrip(t, c, send.String(), want.String())
}

// A command trickling in one byte at a time must still work, and a complete
// command followed by a partial one must get its reply without waiting for
// the partial one to finish (that's the flush-before-blocking rule).
func TestPartialCommands(t *testing.T) {
	addr, _ := startServer(t)
	c := dial(t, addr)
	for _, b := range []byte("*1\r\n$4\r\nPING\r\n") {
		c.Write([]byte{b})
		time.Sleep(time.Millisecond)
	}
	roundTrip(t, c, "", "+PONG\r\n")

	// One full PING plus the first half of the next.
	roundTrip(t, c, "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPI", "+PONG\r\n")
	roundTrip(t, c, "NG\r\n", "+PONG\r\n")
}

func TestQuit(t *testing.T) {
	addr, _ := startServer(t)
	c := dial(t, addr)
	roundTrip(t, c, "PING\r\nQUIT\r\n", "+PONG\r\n+OK\r\n")
	expectClosed(t, c)
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	addr, _ := startServer(t)
	c := dial(t, addr)
	roundTrip(t, c, "*1\r\n$abc\r\n", "-ERR Protocol error: invalid bulk length\r\n")
	expectClosed(t, c)
}

func TestShutdownClosesIdleConnections(t *testing.T) {
	addr, stop := startServer(t)
	c := dial(t, addr)
	roundTrip(t, c, "PING\r\n", "+PONG\r\n")
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	expectClosed(t, c)
	if _, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		t.Fatal("server still accepting after shutdown")
	}
}

// Many clients at once, so the race detector can watch the shared parts
// (the connection set, the command table).
func TestConcurrentClients(t *testing.T) {
	addr, _ := startServer(t)
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			c, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(5 * time.Second))
			br := bufio.NewReader(c)
			for range 100 {
				io.WriteString(c, "PING\r\n")
				line, err := br.ReadString('\n')
				if err != nil || line != "+PONG\r\n" {
					t.Errorf("got %q, %v", line, err)
					return
				}
			}
		})
	}
	wg.Wait()
}

// The real redis-cli against our server. Skipped when it isn't installed (e.g. in CI).
func TestRedisCLI(t *testing.T) {
	cli, err := exec.LookPath("redis-cli")
	if err != nil {
		t.Skip("redis-cli not installed")
	}
	addr, _ := startServer(t)
	host, port, _ := net.SplitHostPort(addr)
	run := func(args ...string) string {
		out, err := exec.Command(cli, append([]string{"-h", host, "-p", port}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("redis-cli %v: %v\n%s", args, err, out)
		}
		return string(bytes.TrimSpace(out))
	}
	if got := run("ping"); got != "PONG" {
		t.Errorf("ping: %q", got)
	}
	if got := run("echo", "hello world"); got != "hello world" {
		t.Errorf("echo: %q", got)
	}
	// No "(error)" prefix: redis-cli only adds that when printing to a terminal.
	if got := run("foo"); !strings.HasPrefix(got, "ERR unknown command 'foo'") {
		t.Errorf("unknown: %q", got)
	}
}
