package server

import (
	"fmt"
	"slices"
	"strings"
)

// command describes one command the server understands.
type command struct {
	// arity uses Redis's convention, counting the command name itself:
	// N > 0 means exactly N arguments, N < 0 means at least -N.
	arity int
	flags cmdFlags
	// Key positions, Redis-style: the first key's argument index, the last
	// key's (negative = counted from the end, -1 = last argument), and the
	// step between keys. 0,0,0 = no keys. Cluster routing uses them to find
	// a command's slot before running it, and COMMAND tells clients, so
	// smart clients can route without asking (ADR 022).
	firstKey, lastKey, step int
	run                     func(cl *client, args [][]byte)
}

// keysOf returns the key arguments of a command.
func (c command) keysOf(args [][]byte) [][]byte {
	if c.firstKey == 0 || c.firstKey >= len(args) {
		return nil
	}
	last := c.lastKey
	if last < 0 {
		last = len(args) + last
	}
	var keys [][]byte
	for i := c.firstKey; i <= last && i < len(args); i += c.step {
		keys = append(keys, args[i])
	}
	return keys
}

// Command flags, the same idea as Redis's command flags.
type cmdFlags uint8

const (
	// flagWrite: changes data. Replicas will refuse these (Iteration 6).
	flagWrite cmdFlags = 1 << iota
	// flagDenyOOM: may grow memory, so it's refused when memory is full and
	// nothing can be evicted. DEL is a write but not denyoom: deleting is
	// how you get out of an out-of-memory situation.
	flagDenyOOM
)

const wd = flagWrite | flagDenyOOM

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
		"info":    {arity: -1, run: cmdInfo},

		"client": {arity: -2, run: cmdClient},

		// cluster (cmd_cluster.go)
		"cluster":   {arity: -2, run: cmdCluster},
		"readonly":  {arity: 1, run: cmdReadOnly},
		"readwrite": {arity: 1, run: cmdReadWrite},

		// persistence (cmd_persist.go)
		"bgrewriteaof": {arity: 1, run: cmdBgRewriteAOF},

		// replication (cmd_repl.go)
		"replicaof": {arity: 3, run: cmdReplicaOf},
		"slaveof":   {arity: 3, run: cmdReplicaOf},
		"replconf":  {arity: -1, run: cmdReplConf},
		"psync":     {arity: 3, run: cmdPSync},
		"wait":      {arity: 3, run: cmdWait},
		"role":      {arity: 1, run: cmdRole},
		"fault":     {arity: -2, run: cmdFault},

		// strings (cmd_strings.go)
		"get":    {arity: 2, firstKey: 1, lastKey: 1, step: 1, run: cmdGet},
		"set":    {arity: -3, flags: wd, firstKey: 1, lastKey: 1, step: 1, run: cmdSet},
		"mget":   {arity: -2, firstKey: 1, lastKey: -1, step: 1, run: cmdMGet},
		"mset":   {arity: -3, flags: wd, firstKey: 1, lastKey: -1, step: 2, run: cmdMSet},
		"incr":   {arity: 2, flags: wd, firstKey: 1, lastKey: 1, step: 1, run: cmdIncr},
		"decr":   {arity: 2, flags: wd, firstKey: 1, lastKey: 1, step: 1, run: cmdDecr},
		"incrby": {arity: 3, flags: wd, firstKey: 1, lastKey: 1, step: 1, run: cmdIncrBy},
		"decrby": {arity: 3, flags: wd, firstKey: 1, lastKey: 1, step: 1, run: cmdDecrBy},

		// keyspace (cmd_keys.go)
		"del":      {arity: -2, flags: flagWrite, firstKey: 1, lastKey: -1, step: 1, run: cmdDel},
		"exists":   {arity: -2, firstKey: 1, lastKey: -1, step: 1, run: cmdExists},
		"keys":     {arity: 2, run: cmdKeys},
		"scan":     {arity: -2, run: cmdScan},
		"dbsize":   {arity: 1, run: cmdDBSize},
		"flushall": {arity: -1, flags: flagWrite, run: cmdFlushAll},

		// expiry (cmd_expire.go)
		"expire":      {arity: -3, flags: flagWrite, firstKey: 1, lastKey: 1, step: 1, run: expireCommand("expire", 1000, false)},
		"pexpire":     {arity: -3, flags: flagWrite, firstKey: 1, lastKey: 1, step: 1, run: expireCommand("pexpire", 1, false)},
		"expireat":    {arity: -3, flags: flagWrite, firstKey: 1, lastKey: 1, step: 1, run: expireCommand("expireat", 1000, true)},
		"pexpireat":   {arity: -3, flags: flagWrite, firstKey: 1, lastKey: 1, step: 1, run: expireCommand("pexpireat", 1, true)},
		"ttl":         {arity: 2, firstKey: 1, lastKey: 1, step: 1, run: ttlCommand(false, false)},
		"pttl":        {arity: 2, firstKey: 1, lastKey: 1, step: 1, run: ttlCommand(true, false)},
		"expiretime":  {arity: 2, firstKey: 1, lastKey: 1, step: 1, run: ttlCommand(false, true)},
		"pexpiretime": {arity: 2, firstKey: 1, lastKey: 1, step: 1, run: ttlCommand(true, true)},
		"persist":     {arity: 2, flags: flagWrite, firstKey: 1, lastKey: 1, step: 1, run: cmdPersist},
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
	cl.cmds.Add(1)
	// Cluster routing comes first: a command for a slot we don't serve gets
	// redirected before anything else looks at it (ADR 022).
	if s.topo != nil {
		if keys := cmd.keysOf(args); len(keys) > 0 {
			if redirect := s.route(cl, cmd, keys); redirect != "" {
				cl.w.WriteError(redirect)
				return
			}
		}
	}
	if cmd.flags&flagWrite != 0 && s.repl != nil && s.repl.IsReplica() {
		cl.w.WriteError("READONLY You can't write against a read only replica.")
		return
	}
	if cmd.flags&flagWrite != 0 && s.aof != nil {
		// Once the AOF has failed (disk full, I/O error), refuse writes:
		// we couldn't make them durable. Like Redis's MISCONF error.
		if err := s.aof.Err(); err != nil {
			cl.w.WriteError("MISCONF Errors writing to the AOF file: " + err.Error())
			return
		}
		defer func() { cl.needDurable = true }()
	}
	// Make room *before* the command takes its locks: eviction has to lock
	// other shards, and doing that while holding this command's locks could
	// deadlock (ADR 013).
	if cmd.flags&flagDenyOOM != 0 && s.evictor != nil {
		if err := s.evictor.Reserve(); err != nil {
			cl.w.WriteError(err.Error()) // "OOM command not allowed when used memory > 'maxmemory'."
			return
		}
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

// COMMAND describes commands the way Redis does, so cluster clients (go-redis
// and others) can find each command's keys and route it to the right node
// themselves. Each entry: [name, arity, flags, first key, last key, step,
// ACL categories, tips, key specs, subcommands]; the last four are empty.
//
//	COMMAND                  every command
//	COMMAND INFO name ...    just those (nil for unknown names)
//	COMMAND COUNT | LIST     how many / their names
//	COMMAND DOCS             empty: redis-cli asks at startup for typing hints
func cmdCommand(cl *client, args [][]byte) {
	table := cl.srv.commands
	if len(args) == 1 {
		names := make([]string, 0, len(table))
		for n := range table {
			names = append(names, n)
		}
		slices.Sort(names)
		cl.w.WriteArrayHeader(len(names))
		for _, n := range names {
			writeCommandInfo(cl, n, table[n])
		}
		return
	}
	switch sub := strings.ToLower(string(args[1])); sub {
	case "docs":
		cl.w.WriteArrayHeader(0)
	case "count":
		cl.w.WriteInt(int64(len(table)))
	case "list":
		cl.w.WriteArrayHeader(len(table))
		for n := range table {
			cl.w.WriteBulkString(n)
		}
	case "info":
		cl.w.WriteArrayHeader(len(args) - 2)
		for _, a := range args[2:] {
			name := strings.ToLower(string(a))
			if c, ok := table[name]; ok {
				writeCommandInfo(cl, name, c)
			} else {
				cl.w.WriteNullArray()
			}
		}
	default:
		cl.w.WriteError(fmt.Sprintf("ERR unknown subcommand '%s'. Try COMMAND HELP.", truncate(args[1], 128)))
	}
}

func writeCommandInfo(cl *client, name string, c command) {
	var flags []string
	if c.flags&flagWrite != 0 {
		flags = append(flags, "write")
	} else if c.firstKey > 0 {
		flags = append(flags, "readonly")
	}
	if c.flags&flagDenyOOM != 0 {
		flags = append(flags, "denyoom")
	}
	cl.w.WriteArrayHeader(10)
	cl.w.WriteBulkString(name)
	cl.w.WriteInt(int64(c.arity))
	cl.w.WriteArrayHeader(len(flags))
	for _, f := range flags {
		cl.w.WriteSimple(f)
	}
	cl.w.WriteInt(int64(c.firstKey))
	cl.w.WriteInt(int64(c.lastKey))
	cl.w.WriteInt(int64(c.step))
	for range 4 { // ACL categories, tips, key specs, subcommands
		cl.w.WriteArrayHeader(0)
	}
}
