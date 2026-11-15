package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/djism/shardis/internal/cluster"
	"github.com/djism/shardis/internal/config"
	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/faults"
	"github.com/djism/shardis/internal/persist"
	"github.com/djism/shardis/internal/replication"
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

	// Replication: every node can be a primary (replicas connect with PSYNC)
	// and can become a replica with REPLICAOF. The node feeds the engine's
	// effects to the AOF and to the replication backlog.
	var reg *faults.Registry
	if cfg.EnableFaultInjection {
		reg = faults.New()
		slog.Warn("FAULT INJECTION ENABLED: the FAULT command can break replication on purpose (testing only)")
	}
	var aofLog store.Log
	if aof != nil {
		aofLog = aof
	}
	repl := replication.NewNode(replication.Options{
		Engine: engine, AOF: aofLog, BacklogSize: cfg.ReplBacklogSize,
		PingPeriod:   time.Duration(cfg.ReplPingMs) * time.Millisecond,
		Timeout:      time.Duration(cfg.ReplTimeoutMs) * time.Millisecond,
		AnnouncePort: cfg.Port, Faults: reg, Bus: bus, Log: slog.Default(),
	})
	defer repl.Close() // runs before aof.Close (defers are last-in-first-out)
	var cl *cluster.Cluster
	if cfg.ClusterEnabled {
		if cl, err = openCluster(cfg, repl, reg, bus); err != nil {
			return err
		}
		defer cl.Close() // runs first at shutdown: leave the bus, save nodes.conf
	}
	if cfg.ReplicaOf != "" {
		f := strings.Fields(cfg.ReplicaOf)
		port, err := strconv.Atoi(f[1])
		if err != nil {
			return fmt.Errorf("replicaof: bad port %q", f[1])
		}
		if err := repl.ReplicaOf(f[0], port); err != nil {
			return err
		}
	}

	srv := server.New(server.Options{Log: slog.Default(), Engine: engine, Evictor: evictor, AOF: aof,
		Repl: repl, Faults: reg, Cluster: cl, Bus: bus, Port: cfg.Port, Hz: cfg.Hz,
		AllowReadsWhenDown: cfg.ClusterReadsWhenDown})
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

// openCluster builds this node's cluster state (from nodes.conf if it has
// run before, else the seed file, else fresh) and opens the cluster bus.
func openCluster(cfg config.Config, repl *replication.Node, reg *faults.Registry, bus *events.Bus) (*cluster.Cluster, error) {
	var seed *cluster.Topology
	var err error
	clientAddr, internalAddr := cfg.ClusterAnnounceAddr, cfg.ClusterAnnounceInternal
	if cfg.ClusterConfig != "" {
		if seed, err = cluster.Load(cfg.ClusterConfig, cfg.ClusterNode); err != nil {
			return nil, err
		}
		if clientAddr == "" {
			clientAddr = seed.Me.Addr
		}
		if internalAddr == "" {
			internalAddr = seed.Me.Internal
		}
	}
	if clientAddr == "" {
		host := cfg.Host
		if host == "0.0.0.0" || host == "" || host == "::" {
			host = "127.0.0.1"
		}
		clientAddr = net.JoinHostPort(host, strconv.Itoa(cfg.Port))
	}
	if internalAddr == "" {
		internalAddr = clientAddr
	}
	// Announce our internal host to our primary, so its replication link to
	// us is named the way the cluster names us (FAULT PARTITION needs that).
	if h, _, err := net.SplitHostPort(internalAddr); err == nil {
		repl.SetAnnounceHost(h)
	}
	cl, err := cluster.New(cluster.Options{
		ClientAddr: clientAddr, InternalAddr: internalAddr, Seed: seed,
		StateFile:            filepath.Join(cfg.Dir, "nodes.conf"),
		AllowPartialCoverage: !cfg.ClusterFullCoverage,
		NodeTimeout:          time.Duration(cfg.ClusterNodeTimeoutMs) * time.Millisecond,
		Replicator:           repl, Faults: reg, Bus: bus, Log: slog.Default(),
	})
	if err != nil {
		return nil, err
	}
	if err := cl.Start(); err != nil {
		return nil, err
	}
	me := cl.View().Me
	slog.Info("cluster mode", "id", me.ID, "name", me.Name, "client", me.Addr, "internal", me.Internal,
		"bus_port", me.BusPort, "known_nodes", len(cl.View().Nodes))
	return cl, nil
}
