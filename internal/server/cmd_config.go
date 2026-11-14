package server

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/djism/shardis/internal/config"
	"github.com/djism/shardis/internal/persist"
	"github.com/djism/shardis/internal/store"
)

// configParam is one setting CONFIG GET can read and (if set != nil) CONFIG
// SET can change at runtime.
type configParam struct {
	name string
	get  func(s *Server) string
	set  func(s *Server, v string) error // nil = read-only
}

// configParams lists the settings we expose. save/appendonly are reported
// honestly as "off" (no persistence yet); redis-benchmark reads them at start.
var configParams = []configParam{
	{name: "maxmemory",
		get: func(s *Server) string { return strconv.FormatInt(s.maxmemory(), 10) },
		set: func(s *Server, v string) error {
			n, err := config.ParseMemory(v)
			if err != nil {
				return err
			}
			s.evictor.SetMaxMemory(n)
			return nil
		}},
	{name: "maxmemory-policy",
		get: func(s *Server) string { return s.policy().String() },
		set: func(s *Server, v string) error {
			p, err := store.ParsePolicy(v)
			if err != nil {
				return err
			}
			return s.evictor.SetPolicy(p)
		}},
	{name: "maxmemory-samples",
		get: func(s *Server) string { return strconv.Itoa(s.samples()) },
		set: func(s *Server, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("not an integer")
			}
			return s.evictor.SetSamples(n)
		}},
	{name: "hz", get: func(s *Server) string { return strconv.Itoa(s.hz) }},
	{name: "save", get: func(*Server) string { return "" }}, // no RDB snapshots; the AOF base plays that role
	{name: "appendonly", get: func(s *Server) string {
		if s.aof != nil {
			return "yes"
		}
		return "no"
	}},
	{name: "appendfsync",
		get: func(s *Server) string {
			if s.aof != nil {
				return s.aof.Fsync().String()
			}
			return persist.FsyncEverySec.String()
		},
		set: func(s *Server, v string) error {
			f, err := persist.ParseFsync(v)
			if err != nil {
				return err
			}
			if s.aof == nil {
				return errors.New("AOF is not enabled")
			}
			s.aof.SetFsync(f)
			return nil
		}},
	{name: "dir", get: func(s *Server) string {
		if s.aof != nil {
			return s.aof.Dir()
		}
		return "."
	}},
}

func (s *Server) maxmemory() int64 {
	if s.evictor == nil {
		return 0
	}
	return s.evictor.MaxMemory()
}

func (s *Server) policy() store.Policy {
	if s.evictor == nil {
		return store.NoEviction
	}
	return s.evictor.Policy()
}

func (s *Server) samples() int {
	if s.evictor == nil {
		return 5
	}
	return s.evictor.Samples()
}

// CONFIG GET pattern [pattern ...]   → flat list of name, value pairs
// CONFIG SET name value [name value ...]
func cmdConfig(cl *client, args [][]byte) {
	switch strings.ToUpper(string(args[1])) {
	case "GET":
		if len(args) >= 3 {
			configGet(cl, args[2:])
			return
		}
	case "SET":
		if len(args) >= 4 && len(args)%2 == 0 {
			configSet(cl, args[2:])
			return
		}
		cl.w.WriteError("ERR wrong number of arguments for 'config|set' command")
		return
	}
	cl.w.WriteError(fmt.Sprintf("ERR unknown subcommand '%s'. Try CONFIG HELP.", truncate(args[1], 128)))
}

func configGet(cl *client, patterns [][]byte) {
	var out []string
	for _, p := range configParams {
		for _, pat := range patterns {
			if store.Match(strings.ToLower(string(pat)), p.name) {
				out = append(out, p.name, p.get(cl.srv))
				break
			}
		}
	}
	cl.w.WriteArrayHeader(len(out))
	for _, s := range out {
		cl.w.WriteBulkString(s)
	}
}

// configSet validates every name first, then applies them in order, like
// Redis 7's multi-parameter CONFIG SET.
func configSet(cl *client, pairs [][]byte) {
	type change struct {
		p *configParam
		v string
	}
	var changes []change
	for i := 0; i < len(pairs); i += 2 {
		name := strings.ToLower(string(pairs[i]))
		j := slices.IndexFunc(configParams, func(p configParam) bool { return p.name == name })
		if j < 0 || configParams[j].set == nil || (cl.srv.evictor == nil && strings.HasPrefix(name, "maxmemory")) {
			cl.w.WriteError(fmt.Sprintf("ERR Unknown option or number of arguments for CONFIG SET - '%s'", truncate(pairs[i], 128)))
			return
		}
		changes = append(changes, change{&configParams[j], string(pairs[i+1])})
	}
	for _, c := range changes {
		if err := c.p.set(cl.srv, c.v); err != nil {
			cl.w.WriteError(fmt.Sprintf("ERR CONFIG SET failed (possibly related to argument '%s') - %v", c.p.name, err))
			return
		}
	}
	cl.w.WriteSimple("OK")
}
