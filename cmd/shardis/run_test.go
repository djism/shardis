package main

import (
	"context"
	"testing"
	"time"
)

func noEnv(string) string { return "" }

// The server must return promptly once its context is cancelled. This is the
// contract Kubernetes relies on when it sends SIGTERM during a rolling restart.
func TestRunStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, nil, noEnv) }()

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

func TestRunRejectsBadConfig(t *testing.T) {
	if err := run(context.Background(), []string{"--port", "0"}, noEnv); err == nil {
		t.Fatal("expected config error")
	}
}
