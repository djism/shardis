package server

import (
	"fmt"
	"strings"

	"github.com/djism/shardis/internal/store"
)

// command describes one command the server understands.
type command struct {
	// arity uses Redis's convention, counting the command name itself:
	// N > 0 means exactly N arguments, N < 0 means at least -N.
	arity int
	run   func(cl *client, args [][]byte)
}

// commandTable is built per Server (not as a package-level var) because
// COMMAND COUNT needs to look at the table, and Go forbids a package-level
// variable whose initializer refers back to itself.
func (s *Server) commandTable() map[string]command {
	return map[string]command{
		// connection
		"ping":    {arity: -1, run: cmdPing},
		"echo":    {arity: 2, run: cmdEcho},
		"quit":    {arity: -1, run: cmdQuit},
		"command": {arity: -1, run: cmdCommand},
		"config":  {arity: -2, run: cmdConfig},

		// strings (cmd_strings.go)
		"get":    {arity: 2, run: cmdGet},
		"set":    {arity: -3, run: cmdSet},
		"mget":   {arity: -2, run: cmdMGet},
		"mset":   {arity: -3, run: cmdMSet},
		"incr":   {arity: 2, run: cmdIncr},
		"decr":   {arity: 2, run: cmdDecr},
		"incrby": {arity: 3, run: cmdIncrBy},
		"decrby": {arity: 3, run: cmdDecrBy},

		// keyspace (cmd_keys.go)
		"del":      {arity: -2, run: cmdDel},
		"exists":   {arity: -2, run: cmdExists},
		"keys":     {arity: 2, run: cmdKeys},
		"scan":     {arity: -2, run: cmdScan},
		"dbsize":   {arity: 1, run: cmdDBSize},
		"flushall": {arity: -1, run: cmdFlushAll},
	}
}

func (s *Server) dispatch(cl *client, args [][]byte) {
	name := strings.ToLower(string(args[0]))
	cmd, ok := s.commands[name]
	if !ok {
		cl.w.WriteError(unknownCommandMsg(args))
		return
	}
	if !arityOK(cmd.arity, len(args)) {
		cl.w.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", name))
		return
	}
	cmd.run(cl, args)
}

func arityOK(arity, n int) bool {
	if arity >= 0 {
		return n == arity
	}
	return n >= -arity
}

// unknownCommandMsg matches Redis's wording, since some tools match on it:
//
//	ERR unknown command 'FOO', with args beginning with: 'a' 'b'
//
// Names and args are truncated so a huge bogus command can't produce a huge error.
func unknownCommandMsg(args [][]byte) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ERR unknown command '%s', with args beginning with: ", truncate(args[0], 128))
	for _, a := range args[1:] {
		if b.Len() > 256 {
			break
		}
		fmt.Fprintf(&b, "'%s' ", truncate(a, 128))
	}
	return b.String()
}

func truncate(b []byte, n int) []byte {
	if len(b) > n {
		return b[:n]
	}
	return b
}

// PING → +PONG.  PING msg → msg as a bulk string (Redis's behavior).
func cmdPing(cl *client, args [][]byte) {
	switch len(args) {
	case 1:
		cl.w.WriteSimple("PONG")
	case 2:
		cl.w.WriteBulk(args[1])
	default:
		cl.w.WriteError("ERR wrong number of arguments for 'ping' command")
	}
}

func cmdEcho(cl *client, args [][]byte) { cl.w.WriteBulk(args[1]) }

func cmdQuit(cl *client, _ [][]byte) {
	cl.w.WriteSimple("OK")
	cl.quit = true
}

// COMMAND is a minimal stub. redis-cli calls COMMAND DOCS at startup to power
// its typing hints; an empty reply just means "no hints" and keeps it happy.
// Clients like go-redis also call COMMAND to learn key positions; we'll
// return real metadata once commands take keys (Iteration 2+).
func cmdCommand(cl *client, args [][]byte) {
	if len(args) == 1 {
		cl.w.WriteArrayHeader(0)
		return
	}
	switch sub := strings.ToLower(string(args[1])); sub {
	case "docs", "info", "list":
		cl.w.WriteArrayHeader(0)
	case "count":
		cl.w.WriteInt(int64(len(cl.srv.commands)))
	default:
		cl.w.WriteError(fmt.Sprintf("ERR unknown subcommand '%s'. Try COMMAND HELP.", truncate(args[1], 128)))
	}
}

// configValues are the settings CONFIG GET can report. They're the truth
// about this build: there's no persistence yet, so save is "" and
// appendonly is "no". redis-benchmark reads these two at startup and warns
// if it can't. CONFIG SET and real tunables arrive with Iteration 4.
var configValues = [][2]string{
	{"save", ""},
	{"appendonly", "no"},
}

// CONFIG GET pattern [pattern ...] → flat list of name, value pairs.
func cmdConfig(cl *client, args [][]byte) {
	if !strings.EqualFold(string(args[1]), "GET") || len(args) < 3 {
		cl.w.WriteError(fmt.Sprintf("ERR unknown subcommand '%s'. Try CONFIG HELP.", truncate(args[1], 128)))
		return
	}
	var out [][2]string
	for _, kv := range configValues {
		for _, pat := range args[2:] {
			if store.Match(strings.ToLower(string(pat)), kv[0]) {
				out = append(out, kv)
				break
			}
		}
	}
	cl.w.WriteArrayHeader(2 * len(out))
	for _, kv := range out {
		cl.w.WriteBulkString(kv[0])
		cl.w.WriteBulkString(kv[1])
	}
}
