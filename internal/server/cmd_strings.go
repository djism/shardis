package server

import (
	"bytes"
	"math"
	"strconv"

	"github.com/djism/shardis/internal/store"
)

// Error texts match Redis exactly; clients and tests compare them.
const (
	errSyntax   = "ERR syntax error"
	errNotInt   = "ERR value is not an integer or out of range"
	errOverflow = "ERR increment or decrement would overflow"
)

// The pattern for every data command:
//  1. Parse and validate arguments *before* touching the store.
//  2. Inside engine.Do: read/modify data, save the result in local variables.
//  3. After Do returns: write the reply.
//
// Step 3 is outside on purpose. Writing to a socket can block (slow client,
// full buffer), and blocking while holding the store would stall every
// other client (executor) or every client on that shard (sharded).

func (cl *client) do(keys [][]byte, fn func(tx *store.Tx)) { cl.srv.engine.Do(keys, fn) }

// GET key
func cmdGet(cl *client, args [][]byte) {
	key := args[1]
	var val []byte
	var ok bool
	cl.do(args[1:2], func(tx *store.Tx) { val, ok = tx.Get(key) })
	if !ok {
		cl.w.WriteNull()
		return
	}
	cl.w.WriteBulk(val) // safe after Do: stored values are never modified in place
}

// SET key value [NX | XX]
// NX = only if the key doesn't exist, XX = only if it does. The check and the
// write happen inside one Do, so two racing SET NX calls can't both win.
func cmdSet(cl *client, args [][]byte) {
	key, val := args[1], args[2]
	var nx, xx bool
	for _, opt := range args[3:] {
		switch {
		case bytes.EqualFold(opt, []byte("NX")):
			nx = true
		case bytes.EqualFold(opt, []byte("XX")):
			xx = true
		default:
			cl.w.WriteError(errSyntax) // EX/PX/KEEPTTL arrive with expiry in Iteration 3
			return
		}
	}
	if nx && xx {
		cl.w.WriteError(errSyntax)
		return
	}
	written := false
	cl.do(args[1:2], func(tx *store.Tx) {
		if nx || xx {
			exists := tx.Exists(key)
			if (nx && exists) || (xx && !exists) {
				return
			}
		}
		tx.Set(key, val)
		written = true
	})
	if written {
		cl.w.WriteSimple("OK")
	} else {
		cl.w.WriteNull() // condition not met
	}
}

// MGET key [key ...]
func cmdMGet(cl *client, args [][]byte) {
	keys := args[1:]
	vals := make([][]byte, len(keys))
	found := make([]bool, len(keys))
	cl.do(keys, func(tx *store.Tx) {
		for i, k := range keys {
			vals[i], found[i] = tx.Get(k)
		}
	})
	cl.w.WriteArrayHeader(len(keys))
	for i := range keys {
		if found[i] {
			cl.w.WriteBulk(vals[i])
		} else {
			cl.w.WriteNull()
		}
	}
}

// MSET key value [key value ...]
// All pairs are written in one Do, so no client ever sees some set and others not.
func cmdMSet(cl *client, args [][]byte) {
	if (len(args)-1)%2 != 0 {
		cl.w.WriteError("ERR wrong number of arguments for 'mset' command")
		return
	}
	keys := make([][]byte, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		keys = append(keys, args[i])
	}
	cl.do(keys, func(tx *store.Tx) {
		for i := 1; i < len(args); i += 2 {
			tx.Set(args[i], args[i+1])
		}
	})
	cl.w.WriteSimple("OK")
}

func cmdIncr(cl *client, args [][]byte) { incrBy(cl, args[1], 1) }
func cmdDecr(cl *client, args [][]byte) { incrBy(cl, args[1], -1) }

func cmdIncrBy(cl *client, args [][]byte) {
	delta, ok := parseStrictInt(args[2])
	if !ok {
		cl.w.WriteError(errNotInt)
		return
	}
	incrBy(cl, args[1], delta)
}

func cmdDecrBy(cl *client, args [][]byte) {
	delta, ok := parseStrictInt(args[2])
	if !ok {
		cl.w.WriteError(errNotInt)
		return
	}
	if delta == math.MinInt64 { // -MinInt64 doesn't fit in an int64
		cl.w.WriteError("ERR decrement would overflow")
		return
	}
	incrBy(cl, args[1], -delta)
}

// incrBy is the read-modify-write behind INCR/DECR/INCRBY/DECRBY. Doing the
// read, the math and the write inside one Do is what makes concurrent INCRs
// never lose an update.
func incrBy(cl *client, key []byte, delta int64) {
	var result int64
	var errMsg string
	cl.do([][]byte{key}, func(tx *store.Tx) {
		var cur int64
		if v, ok := tx.Get(key); ok {
			n, valid := parseStrictInt(v)
			if !valid {
				errMsg = errNotInt
				return
			}
			cur = n
		}
		if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
			errMsg = errOverflow
			return
		}
		result = cur + delta
		tx.Set(key, strconv.AppendInt(nil, result, 10)) // a new slice: stored values are immutable
	})
	if errMsg != "" {
		cl.w.WriteError(errMsg)
		return
	}
	cl.w.WriteInt(result)
}

// parseStrictInt accepts only the canonical form of an int64, like Redis:
// "0", "42", "-7". It rejects "+1", "01", "-0", " 1" and anything that
// overflows. Redis is strict here so that INCR on "01" doesn't silently turn
// a string the user cared about into a different-looking number.
func parseStrictInt(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 20 {
		return 0, false
	}
	if len(b) == 1 && b[0] == '0' {
		return 0, true
	}
	digits := b
	if b[0] == '-' {
		digits = b[1:]
	}
	if len(digits) == 0 || digits[0] < '1' || digits[0] > '9' {
		return 0, false // empty, leading zero, or not a digit
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	return n, err == nil
}
