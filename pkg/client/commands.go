package client

import (
	"bytes"
	"strings"

	"github.com/djism/shardis/internal/resp"
)

// cmdInfo is what the client knows about one command: where its keys are
// (to route it) and whether it only reads (replica reads, retry safety).
// It comes from the server's COMMAND reply (ADR 038), which Shardis and
// Redis both answer in the same format.
type cmdInfo struct {
	first, last, step int
	readonly          bool
}

// parseCommandTable reads a COMMAND reply:
// [[name, arity, [flags...], first-key, last-key, step, ...], ...]
func parseCommandTable(v resp.Value) map[string]cmdInfo {
	out := map[string]cmdInfo{}
	for _, e := range v.Elems {
		if len(e.Elems) < 6 {
			continue
		}
		ci := cmdInfo{first: int(e.Elems[3].Int), last: int(e.Elems[4].Int), step: int(e.Elems[5].Int)}
		for _, f := range e.Elems[2].Elems {
			if string(f.Str) == "readonly" {
				ci.readonly = true
			}
		}
		out[strings.ToLower(string(e.Elems[0].Str))] = ci
	}
	return out
}

// keys returns a command's key arguments.
func (ci cmdInfo) keys(args [][]byte) [][]byte {
	if ci.first <= 0 || ci.first >= len(args) {
		return nil
	}
	last := ci.last
	if last < 0 {
		last += len(args)
	}
	step := max(ci.step, 1)
	var out [][]byte
	for i := ci.first; i <= last && i < len(args); i += step {
		out = append(out, args[i])
	}
	return out
}

// idempotentWrites are writes that leave the same state, and send the same
// reply, when run twice. Only these (and read-only commands) are resent
// after an unknown outcome under RetrySafe (ADR 036). It's an allowlist:
// a command we don't know is assumed unsafe.
var idempotentWrites = map[string]bool{
	"set": true, "mset": true, "del": true, "unlink": true,
	"expireat": true, "pexpireat": true, "persist": true,
	"hset": true, "hdel": true, "sadd": true, "srem": true,
}

// safeToResend reports whether running args twice can't be told apart from
// running it once. SET is safe, unless an option makes its reply depend on
// the previous state: SET k v NX would answer "not set" the second time,
// although the first one set it, and the application would believe it
// failed. Relative EXPIRE is not safe either: a second EXPIRE k 10 pushes
// the deadline later.
func safeToResend(name string, ci cmdInfo, args [][]byte) bool {
	if ci.readonly {
		return true
	}
	if !idempotentWrites[name] {
		return false
	}
	if name == "set" {
		for _, a := range args[3:] {
			switch strings.ToUpper(string(a)) {
			case "NX", "XX", "GET":
				return false
			}
		}
	}
	return true
}

// lowerName returns the command name in lower case without allocating
// for names that are already lower case.
func lowerName(b []byte) string {
	if bytes.IndexFunc(b, func(r rune) bool { return r >= 'A' && r <= 'Z' }) < 0 {
		return string(b)
	}
	return strings.ToLower(string(b))
}
