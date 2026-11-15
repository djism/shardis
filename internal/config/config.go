// Package config loads server settings from command-line flags and
// environment variables.
//
// Precedence, highest first: flag > env var > built-in default.
// That lets the same binary run locally (flags), in Docker Compose and in
// Kubernetes (env vars from a ConfigMap) without code changes.
package config

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// EnvPrefix is put in front of every env var name, so "port" becomes SHARDIS_PORT.
const EnvPrefix = "SHARDIS_"

// Config holds every setting the server needs. It grows as iterations add features.
type Config struct {
	Host string // interface to listen on
	Port int    // client (RESP) port; the cluster bus will use Port+10000

	// Engine picks how commands get exclusive access to data: "sharded"
	// (256 locks, the default, chosen by benchmark) or "executor" (one
	// goroutine owns everything, like Redis). See ADR 005. Unknown names are
	// rejected when the store is created.
	Engine string

	// Hz is how many active-expiry cycles run per second (Redis's "hz",
	// default 10). Higher = expired keys are freed sooner, at more CPU cost.
	Hz int

	// Memory limit (ADR 011-013). MaxMemory 0 = unlimited. Policy names are
	// Redis's (noeviction, allkeys-lru, allkeys-random, volatile-ttl), plus
	// allkeys-lru-exact, which exists only for the ADR 012 comparison.
	MaxMemory        int64
	MaxMemoryPolicy  string
	MaxMemorySamples int

	// Persistence (ADR 014-016).
	Dir                   string // data directory (AOF files go in Dir/appendonlydir)
	AppendOnly            bool
	AppendFsync           string // always, everysec, no
	AutoAOFRewritePercent int
	AutoAOFRewriteMinSize int64
	DebugRecordSynced     bool // fault injection for the crash test; never in production

	// Replication (ADR 017-020).
	ReplicaOf            string // "host port" to start as a replica, "" = primary
	ReplBacklogSize      int64
	ReplPingMs           int // primary → replica heartbeat period
	ReplTimeoutMs        int // a silent link is dead after this long
	EnableFaultInjection bool

	// Cluster (ADR 021-026). Cluster mode is on if ClusterEnabled, or if a
	// seed topology file is given (then ClusterNode names this node in it).
	ClusterEnabled          bool
	ClusterConfig           string // path to a seed topology file (optional)
	ClusterNode             string // this node's name in the seed file
	ClusterAnnounceAddr     string // host:port clients are told; default: from seed, else listen addr
	ClusterAnnounceInternal string // host:port other nodes use; default: from seed, else = announce addr
	ClusterNodeTimeoutMs    int
	// ClusterFullCoverage: refuse every keyed command while any slot is
	// unserved or failed (Redis's cluster-require-full-coverage, default yes).
	ClusterFullCoverage bool
	// ClusterReadsWhenDown: serve reads while the cluster is down or this
	// node is in a minority (cluster-allow-reads-when-down, default no).
	ClusterReadsWhenDown bool
}

// Load builds a Config from args (usually os.Args[1:]) and getenv (usually os.Getenv).
// Taking getenv as a parameter keeps tests independent of the real environment.
func Load(args []string, getenv func(string) string) (Config, error) {
	fs := flag.NewFlagSet("shardis", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // the caller decides how to print errors

	var cfg Config
	// Loopback by default, like Redis: there's no auth, so listening on every
	// interface must be an explicit choice (containers set SHARDIS_HOST=0.0.0.0).
	str(fs, getenv, &cfg.Host, "host", "127.0.0.1", "interface to listen on")
	str(fs, getenv, &cfg.Engine, "engine", "sharded", "concurrency engine: sharded (default) or executor")
	if err := integer(fs, getenv, &cfg.Port, "port", 7000, "client port"); err != nil {
		return Config{}, err
	}
	if err := integer(fs, getenv, &cfg.Hz, "hz", 10, "active expiry cycles per second (1-500)"); err != nil {
		return Config{}, err
	}
	var maxmem string
	str(fs, getenv, &maxmem, "maxmemory", "0", "memory limit, e.g. 100mb or 2gb (0 = none)")
	str(fs, getenv, &cfg.MaxMemoryPolicy, "maxmemory-policy", "noeviction", "what to do when full: noeviction, allkeys-lru, allkeys-random, volatile-ttl")
	if err := integer(fs, getenv, &cfg.MaxMemorySamples, "maxmemory-samples", 5, "keys sampled per eviction (approximate LRU / TTL)"); err != nil {
		return Config{}, err
	}
	var appendonly, rewriteMin, recordSynced string
	str(fs, getenv, &cfg.Dir, "dir", ".", "data directory")
	str(fs, getenv, &appendonly, "appendonly", "no", "persist writes to an append-only file: yes or no")
	str(fs, getenv, &cfg.AppendFsync, "appendfsync", "everysec", "fsync policy: always, everysec or no")
	if err := integer(fs, getenv, &cfg.AutoAOFRewritePercent, "auto-aof-rewrite-percentage", 100, "rewrite the AOF when it grows by this % (0 = never)"); err != nil {
		return Config{}, err
	}
	str(fs, getenv, &rewriteMin, "auto-aof-rewrite-min-size", "64mb", "don't auto-rewrite an AOF smaller than this")
	str(fs, getenv, &recordSynced, "debug-record-synced", "no", "FAULT INJECTION (crash test only): record fsynced AOF length")
	var backlog, faultsOn string
	str(fs, getenv, &cfg.ReplicaOf, "replicaof", "", `start as a replica of "host port"`)
	str(fs, getenv, &backlog, "repl-backlog-size", "64mb", "replication backlog (how far behind a replica can be and still partially resync)")
	if err := integer(fs, getenv, &cfg.ReplPingMs, "repl-ping-ms", 1000, "primary heartbeat period in the replication stream (ms)"); err != nil {
		return Config{}, err
	}
	if err := integer(fs, getenv, &cfg.ReplTimeoutMs, "repl-timeout-ms", 10000, "replication link timeout (ms)"); err != nil {
		return Config{}, err
	}
	str(fs, getenv, &faultsOn, "enable-fault-injection", "no", "enable the FAULT admin command (testing only)")
	var clusterOn string
	str(fs, getenv, &clusterOn, "cluster-enabled", "no", "run as a cluster node (form the cluster with CLUSTER MEET/ADDSLOTS/REPLICATE)")
	str(fs, getenv, &cfg.ClusterConfig, "cluster-config", "", "seed topology file (enables cluster mode)")
	str(fs, getenv, &cfg.ClusterAnnounceAddr, "cluster-announce-addr", "", "host:port clients are told to use (MOVED, CLUSTER SLOTS)")
	str(fs, getenv, &cfg.ClusterAnnounceInternal, "cluster-announce-internal", "", "host:port other nodes use to reach this one")
	if err := integer(fs, getenv, &cfg.ClusterNodeTimeoutMs, "cluster-node-timeout", 15000, "ms without a reply before a node is suspected down"); err != nil {
		return Config{}, err
	}
	var fullCoverage, readsDown string
	str(fs, getenv, &fullCoverage, "cluster-require-full-coverage", "yes", "refuse all keyed commands while any slot is down (no = only those slots)")
	str(fs, getenv, &readsDown, "cluster-allow-reads-when-down", "no", "serve (possibly stale) reads while the cluster is down or this node is in a minority")
	str(fs, getenv, &cfg.ClusterNode, "cluster-node", "", "this node's name in the cluster topology file")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if cfg.Port < 1 || cfg.Port > 55535 {
		// 55535 because the cluster bus port (Port+10000) must still fit in 65535.
		return Config{}, fmt.Errorf("port %d out of range 1-55535", cfg.Port)
	}
	var err error
	if cfg.MaxMemory, err = ParseMemory(maxmem); err != nil {
		return Config{}, err
	}
	if cfg.AutoAOFRewriteMinSize, err = ParseMemory(rewriteMin); err != nil {
		return Config{}, err
	}
	if cfg.AppendOnly, err = yesNo("appendonly", appendonly); err != nil {
		return Config{}, err
	}
	if cfg.DebugRecordSynced, err = yesNo("debug-record-synced", recordSynced); err != nil {
		return Config{}, err
	}
	if cfg.ReplBacklogSize, err = ParseMemory(backlog); err != nil {
		return Config{}, err
	}
	if cfg.EnableFaultInjection, err = yesNo("enable-fault-injection", faultsOn); err != nil {
		return Config{}, err
	}
	if cfg.ClusterEnabled, err = yesNo("cluster-enabled", clusterOn); err != nil {
		return Config{}, err
	}
	if cfg.ClusterFullCoverage, err = yesNo("cluster-require-full-coverage", fullCoverage); err != nil {
		return Config{}, err
	}
	if cfg.ClusterReadsWhenDown, err = yesNo("cluster-allow-reads-when-down", readsDown); err != nil {
		return Config{}, err
	}
	if (cfg.ClusterConfig == "") != (cfg.ClusterNode == "") {
		return Config{}, fmt.Errorf("a seed topology needs both --cluster-config and --cluster-node")
	}
	if cfg.ClusterConfig != "" {
		cfg.ClusterEnabled = true
	}
	if cfg.ClusterEnabled && cfg.ReplicaOf != "" {
		return Config{}, fmt.Errorf("--replicaof can't be used in cluster mode; the topology file says who replicates whom")
	}
	if cfg.ReplicaOf != "" && len(strings.Fields(cfg.ReplicaOf)) != 2 {
		return Config{}, fmt.Errorf(`replicaof: want "host port", got %q`, cfg.ReplicaOf)
	}
	if cfg.Hz < 1 || cfg.Hz > 500 { // Redis's allowed range
		return Config{}, fmt.Errorf("hz %d out of range 1-500", cfg.Hz)
	}
	return cfg, nil
}

// yesNo parses redis.conf-style booleans.
func yesNo(name, v string) (bool, error) {
	switch strings.ToLower(v) {
	case "yes", "true", "1":
		return true, nil
	case "no", "false", "0":
		return false, nil
	}
	return false, fmt.Errorf("%s: want yes or no, got %q", name, v)
}

// EnvName turns a flag name like "node-timeout" into "SHARDIS_NODE_TIMEOUT".
func EnvName(flagName string) string {
	return EnvPrefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

// str registers a string flag whose default comes from the env var if set.
func str(fs *flag.FlagSet, getenv func(string) string, p *string, name, def, usage string) {
	if v := getenv(EnvName(name)); v != "" {
		def = v
	}
	fs.StringVar(p, name, def, usage+" (env "+EnvName(name)+")")
}

// integer is like str but for ints. A malformed env var is an error rather than
// being silently ignored, because a typo in a k8s manifest should fail loudly.
func integer(fs *flag.FlagSet, getenv func(string) string, p *int, name string, def int, usage string) error {
	if v := getenv(EnvName(name)); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("env %s=%q: not an integer", EnvName(name), v)
		}
		def = n
	}
	fs.IntVar(p, name, def, usage+" (env "+EnvName(name)+")")
	return nil
}
