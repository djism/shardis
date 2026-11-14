package main

import (
	"context"
	"log/slog"
	"net"
	"strconv"

	"github.com/djism/shardis/internal/config"
	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/persist"
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

	// Persistence: replay the AOF into the engine *before* serving, and only
	// then start logging new effects (so replayed ones aren't logged twice).
	var aof *persist.AOF
	if cfg.AppendOnly {
		if aof, err = openAOF(cfg, engine); err != nil {
			return err
		}
		// Runs after Serve returns (defers are last-in-first-out): every
		// pending write is written and fsynced, so a clean shutdown loses nothing.
		defer aof.Close()
	}

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

	srv := server.New(server.Options{Log: slog.Default(), Engine: engine, Evictor: evictor, AOF: aof, Hz: cfg.Hz})
	if err := srv.Listen(addr); err != nil {
		return err
	}
	slog.Info("shardis listening", "addr", srv.Addr().String(), "engine", engine.Name(), "hz", cfg.Hz,
		"maxmemory", cfg.MaxMemory, "policy", policy)

	err = srv.Serve(ctx) // returns once ctx is cancelled and all connections are closed
	slog.Info("shardis stopped")
	return err
}

func openAOF(cfg config.Config, engine store.Engine) (*persist.AOF, error) {
	fsync, err := persist.ParseFsync(cfg.AppendFsync)
	if err != nil {
		return nil, err
	}
	if cfg.DebugRecordSynced {
		slog.Warn("FAULT INJECTION ON: --debug-record-synced (crash testing only)")
	}
	aof, err := persist.Open(persist.Options{
		Dir: cfg.Dir, Fsync: fsync, Log: slog.Default(),
		RewritePercent: cfg.AutoAOFRewritePercent, RewriteMinSize: cfg.AutoAOFRewriteMinSize,
		RecordSynced: cfg.DebugRecordSynced,
	})
	if err != nil {
		return nil, err
	}
	st, err := aof.Load(engine)
	if err != nil {
		return nil, err
	}
	slog.Info("AOF loaded", "files", st.Files, "records", st.Records, "bytes", st.Bytes, "took", st.Took, "truncated", st.Truncated)
	if err := aof.Start(engine); err != nil {
		return nil, err
	}
	return aof, nil
}
