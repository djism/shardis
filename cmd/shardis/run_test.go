package main

import (
	"context"
	"io"
	"math/rand/v2"
	"net"
	"strconv"
	"testing"
	"time"
)

func noEnv(string) string { return "" }

// freePort finds an unused port that our config accepts. We can't just use
// port 0: the OS picks from its ephemeral range (49152-65535 on macOS), which
// is mostly above our 55535 cap (the cluster bus needs port+10000 to fit).
// Small race (someone could grab it before run does), fine for tests.
func freePort(t *testing.T) string {
	t.Helper()
	for range 100 {
		port := 20000 + rand.IntN(20000)
		ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err == nil {
			ln.Close()
			return strconv.Itoa(port)
		}
	}
	t.Fatal("no free port found in 20000-39999")
	return ""
}

// End to end through run(): it serves PING, and returns promptly once its
// context is cancelled, which is the contract Kubernetes relies on with SIGTERM.
func TestRunServesAndStopsOnCancel(t *testing.T) {
	port := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"--host", "127.0.0.1", "--port", port}, noEnv) }()

	// Wait for the listener. Under -race with all packages testing in
	// parallel, startup can take a while, so allow 5s, and fail fast with
	// the real reason if run exits early (e.g. the port got taken).
	var c net.Conn
	var err error
	deadline := time.Now().Add(5 * time.Second)
	for {
		if c, err = net.Dial("tcp", "127.0.0.1:"+port); err == nil {
			break
		}
		select {
		case runErr := <-done:
			t.Fatalf("run exited before serving: %v", runErr)
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never came up: %v", err)
		}
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	io.WriteString(c, "PING\r\n")
	reply := make([]byte, 7)
	if _, err := io.ReadFull(c, reply); err != nil || string(reply) != "+PONG\r\n" {
		t.Fatalf("reply %q, err %v", reply, err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not stop after cancel")
	}
}

// If the port is taken, run must return the error, not hang.
func TestRunReturnsWhenPortBusy(t *testing.T) {
	port := freePort(t)
	ln, err := net.Listen("tcp", "127.0.0.1:"+port) // occupy it
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan error, 1)
	go func() { done <- run(context.Background(), []string{"--port", port}, noEnv) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an address-in-use error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run hung after Listen failed")
	}
}

func TestRunRejectsBadConfig(t *testing.T) {
	if err := run(context.Background(), []string{"--port", "0"}, noEnv); err == nil {
		t.Fatal("expected config error")
	}
}
