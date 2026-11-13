// Command shardis is the cache server. In Iteration 0 it only loads config,
// logs that it started, and waits for Ctrl-C / SIGTERM. Iteration 1 adds the
// RESP listener.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// SIGTERM matters as much as Ctrl-C: it's what Kubernetes sends before killing a pod.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, "shardis:", err)
		os.Exit(1)
	}
}
