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
	policy, err := store.ParsePolicy(cfg.MaxMemoryPolicy)
	if err != nil {
		return err
	}
	engine, err := store.New(cfg.Engine, store.Options{Bus: bus, ExactLRU: policy == store.AllKeysLRUExact})
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

	evictor, err := store.NewEvictor(engine, cfg.MaxMemory, policy, cfg.MaxMemorySamples)
	if err != nil {
		return err
	}
	if cfg.MaxMemory > 0 && cfg.MaxMemory < 32<<20 {
		// The 16,384 slot maps alone take ~9 MB once keys are spread over
		// every slot (up to ~14 MB with TTLs), so a tiny limit leaves little
		// room for data and makes eviction very aggressive (ADR 011).
		slog.Warn("maxmemory is below 32 MB; the hash-slot maps alone use ~9-14 MB", "maxmemory", cfg.MaxMemory)
	}

	srv := server.New(server.Options{Log: slog.Default(), Engine: engine, Evictor: evictor, Hz: cfg.Hz})
	if err := srv.Listen(addr); err != nil {
		return err
	}
	slog.Info("shardis listening", "addr", srv.Addr().String(), "engine", engine.Name(), "hz", cfg.Hz,
		"maxmemory", cfg.MaxMemory, "policy", policy)

	err = srv.Serve(ctx) // returns once ctx is cancelled and all connections are closed
	slog.Info("shardis stopped")
	return err
}
