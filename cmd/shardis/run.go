package main

import (
	"context"
	"log/slog"
	"net"
	"strconv"

	"github.com/djism/shardis/internal/config"
	"github.com/djism/shardis/internal/server"
)

// run is main without the os.* globals, so tests can drive it with a fake
// environment and a context they cancel themselves.
func run(ctx context.Context, args []string, getenv func(string) string) error {
	cfg, err := config.Load(args, getenv)
	if err != nil {
		return err
	}
	srv := server.New(slog.Default())
	if err := srv.Listen(net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))); err != nil {
		return err
	}
	slog.Info("shardis listening", "addr", srv.Addr().String())

	err = srv.Serve(ctx) // returns once ctx is cancelled and all connections are closed
	slog.Info("shardis stopped")
	return err
}
