package server

import (
	"bytes"
	"strconv"

	"github.com/djism/shardis/internal/keyslot"
	"github.com/djism/shardis/internal/store"
)

// DEL key [key ...]: returns how many existed. All deleted in one Do.
func cmdDel(cl *client, args [][]byte) {
	keys := args[1:]
	n := 0
	cl.do(keys, func(tx *store.Tx) {
		for _, k := range keys {
			if tx.Delete(k) {
				n++
			}
		}
	})
	cl.w.WriteInt(int64(n))
}

// EXISTS key [key ...]: counts repeats, so EXISTS a a returns 2 if a exists (as Redis does).
func cmdExists(cl *client, args [][]byte) {
	keys := args[1:]
	n := 0
	cl.do(keys, func(tx *store.Tx) {
		for _, k := range keys {
			if tx.Exists(k) {
				n++
			}
		}
	})
	cl.w.WriteInt(int64(n))
}

// KEYS pattern: walks the whole keyspace while holding it. With a million
// keys that blocks every other client for a long time, which is why Redis
// docs say never to use it in production. SCAN is the safe alternative.
func cmdKeys(cl *client, args [][]byte) {
	pattern := string(args[1])
	var out []string
	cl.srv.engine.DoAll(func(tx *store.Tx) {
		tx.ForEach(func(k string) {
			if pattern == "*" || store.Match(pattern, k) {
				out = append(out, k)
			}
		})
	})
	cl.w.WriteArrayHeader(len(out))
	for _, k := range out {
		cl.w.WriteBulkString(k)
	}
}

// SCAN cursor [MATCH pattern] [COUNT n]
// Our cursor is simply the next hash slot to visit (ADR 006). Clients treat
// cursors as opaque, so they don't care that it differs from Redis's.
// COUNT is how much work to do per call, not how many results to return:
// with MATCH, a call can return fewer keys (even zero) with a non-zero cursor.
func cmdScan(cl *client, args [][]byte) {
	cur, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		cl.w.WriteError("ERR invalid cursor")
		return
	}
	pattern, count := "", 10
	for i := 2; i < len(args); i += 2 {
		if i+1 >= len(args) {
			cl.w.WriteError(errSyntax)
			return
		}
		switch opt := args[i]; {
		case bytes.EqualFold(opt, []byte("MATCH")):
			pattern = string(args[i+1])
		case bytes.EqualFold(opt, []byte("COUNT")):
			n, ok := parseStrictInt(args[i+1])
			if !ok {
				cl.w.WriteError(errNotInt)
				return
			}
			if n < 1 {
				cl.w.WriteError(errSyntax)
				return
			}
			count = int(min(n, 1<<20))
		default:
			cl.w.WriteError(errSyntax)
			return
		}
	}

	var keys []string
	var next uint16
	if cur < keyslot.Count { // a cursor past the last slot means "done"
		cl.srv.engine.DoAll(func(tx *store.Tx) {
			next = tx.Scan(uint16(cur), count, func(k string) {
				if pattern == "" || store.Match(pattern, k) {
					keys = append(keys, k)
				}
			})
		})
	}
	cl.w.WriteArrayHeader(2)
	cl.w.WriteBulkString(strconv.Itoa(int(next)))
	cl.w.WriteArrayHeader(len(keys))
	for _, k := range keys {
		cl.w.WriteBulkString(k)
	}
}

func cmdDBSize(cl *client, _ [][]byte) {
	var n int
	cl.srv.engine.DoAll(func(tx *store.Tx) { n = tx.Len() })
	cl.w.WriteInt(int64(n))
}

// FLUSHALL [ASYNC | SYNC]: we always flush synchronously; both are accepted.
func cmdFlushAll(cl *client, args [][]byte) {
	if len(args) > 2 || (len(args) == 2 &&
		!bytes.EqualFold(args[1], []byte("ASYNC")) && !bytes.EqualFold(args[1], []byte("SYNC"))) {
		cl.w.WriteError(errSyntax)
		return
	}
	cl.srv.engine.DoAll(func(tx *store.Tx) { tx.Flush() })
	cl.w.WriteSimple("OK")
}
