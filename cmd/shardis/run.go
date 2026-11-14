package main

import (
	"context"
	"log/slog"
	"net"
	"strconv"

	"github.com/djism/shardis/internal/config"
	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/server"
	"github.com/djism/shardis/internal/store"
)

// run is main without the os.* globals, so tests can drive it with a fake
// environment and a context they cancel themselves.
func run(ctx context.Context, args []string, getenv func(string) string) error {
	cfg, err := config.Load(args, getenv)
	if err != nil {
		return err
	}
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))

	bus := events.NewBus(addr) // node identity for events; a real node ID arrives with the cluster
	engine, err := store.New(cfg.Engine, store.Options{Bus: bus})
	if err != nil {
		return err
	}
	// Closed after Serve returns, i.e. after every connection is gone, so no
	// command can be mid-flight when the engine stops.
	defer engine.Close()

	// Active expiry runs until run returns. Defers run last-in-first-out:
	// stopExpiry, then wait for it to exit, then (above) engine.Close, so it
	// never touches a closed engine. Its own context means an early return
	// (e.g. Listen failing) still stops it instead of waiting forever.
	expCtx, stopExpiry := context.WithCancel(ctx)
	expDone := make(chan struct{})
	go func() {
		store.NewActiveExpirer(engine, cfg.Hz).Run(expCtx)
		close(expDone)
	}()
	defer func() { <-expDone }()
	defer stopExpiry()

	srv := server.New(slog.Default(), engine)
	if err := srv.Listen(addr); err != nil {
		return err
	}
	slog.Info("shardis listening", "addr", srv.Addr().String(), "engine", engine.Name(), "hz", cfg.Hz)

	err = srv.Serve(ctx) // returns once ctx is cancelled and all connections are closed
	slog.Info("shardis stopped")
	return err
}
