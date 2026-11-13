package main

import (
	"context"
	"log/slog"

	"github.com/djism/shardis/internal/config"
)

// run is main without the os.* globals, so tests can drive it with a fake
// environment and a context they cancel themselves.
func run(ctx context.Context, args []string, getenv func(string) string) error {
	cfg, err := config.Load(args, getenv)
	if err != nil {
		return err
	}
	slog.Info("shardis starting", "host", cfg.Host, "port", cfg.Port)

	<-ctx.Done() // nothing to serve yet; just wait to be told to stop
	slog.Info("shardis shutting down")
	return nil
}
