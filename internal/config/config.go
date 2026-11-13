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
	if err := integer(fs, getenv, &cfg.Port, "port", 7000, "client port"); err != nil {
		return Config{}, err
	}
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if cfg.Port < 1 || cfg.Port > 55535 {
		// 55535 because the cluster bus port (Port+10000) must still fit in 65535.
		return Config{}, fmt.Errorf("port %d out of range 1-55535", cfg.Port)
	}
	return cfg, nil
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
