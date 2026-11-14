package server

import (
	"fmt"
	"math"
	"strings"

	"github.com/djism/shardis/internal/store"
)

func invalidExpireMsg(cmd string) string {
	return fmt.Sprintf("ERR invalid expire time in '%s' command", cmd)
}

// ttlSpec is a TTL given as a command option, e.g. EX 10 or PXAT 1700000000000.
// Relative ones are turned into an absolute deadline inside Do, using the
// command's own clock reading, so "now" is the same moment the key is written.
type ttlSpec struct {
	set      bool
	ms       int64 // milliseconds: a duration, or a Unix-ms timestamp if absolute
	absolute bool
}

// newTTLSpec converts n in the unit named by opt (EX, PX, EXAT, PXAT) to
// milliseconds. ok is false if that overflows an int64.
func newTTLSpec(opt string, n int64) (ttlSpec, bool) {
	spec := ttlSpec{set: true, absolute: strings.HasSuffix(opt, "AT")}
	if strings.HasPrefix(opt, "E") { // seconds
		ms, ok := mulOK(n, 1000)
		spec.ms = ms
		return spec, ok
	}
	spec.ms = n
	return spec, true
}

// deadline returns the absolute Unix-ms deadline. ok is false on overflow.
func (t ttlSpec) deadline(now int64) (int64, bool) {
	if t.absolute {
		return t.ms, true
	}
	return addOK(now, t.ms)
}

func mulOK(a, b int64) (int64, bool) {
	if a > math.MaxInt64/b || a < math.MinInt64/b {
		return 0, false
	}
	return a * b, true
}

func addOK(a, b int64) (int64, bool) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, false
	}
	return a + b, true
}

// expireCommand builds EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT:
//
//	EXPIRE key seconds [NX | XX | GT | LT]
//
// NX: only if the key has no TTL.   XX: only if it has one.
// GT: only if the new deadline is later.   LT: only if it's earlier.
// For GT/LT, "no TTL" counts as an infinitely late deadline, so GT never
// applies to such a key and LT always does. A deadline in the past deletes
// the key. Reply: 1 if the TTL was set (or the key deleted), 0 otherwise.
func expireCommand(name string, unitMs int64, absolute bool) func(*client, [][]byte) {
	return func(cl *client, args [][]byte) {
		key := args[1]
		n, ok := parseStrictInt(args[2])
		if !ok {
			cl.w.WriteError(errNotInt)
			return
		}
		var nx, xx, gt, lt bool
		for _, a := range args[3:] {
			switch strings.ToUpper(string(a)) {
			case "NX":
				nx = true
			case "XX":
				xx = true
			case "GT":
				gt = true
			case "LT":
				lt = true
			default:
				cl.w.WriteError("ERR Unsupported option " + string(truncate(a, 128)))
				return
			}
		}
		if nx && (xx || gt || lt) {
			cl.w.WriteError("ERR NX and XX, GT or LT options at the same time are not compatible")
			return
		}
		if gt && lt {
			cl.w.WriteError("ERR GT and LT options at the same time are not compatible")
			return
		}
		ms, ok := mulOK(n, unitMs)
		if !ok {
			cl.w.WriteError(invalidExpireMsg(name))
			return
		}

		var reply int64
		var errMsg string
		cl.do(args[1:2], func(tx *store.Tx) {
			at := ms
			if !absolute {
				if at, ok = addOK(tx.Now(), ms); !ok {
					errMsg = invalidExpireMsg(name)
					return
				}
			}
			cur, exists := tx.ExpireAt(key)
			switch {
			case !exists:
				return
			case nx && cur != 0, xx && cur == 0:
				return
			case gt && (cur == 0 || at <= cur):
				return
			case lt && cur != 0 && at >= cur:
				return
			}
			if at <= tx.Now() {
				tx.Delete(key) // a deadline in the past: Redis deletes right away
			} else {
				tx.SetExpire(key, at)
			}
			reply = 1
		})
		if errMsg != "" {
			cl.w.WriteError(errMsg)
			return
		}
		cl.w.WriteInt(reply)
	}
}

// ttlCommand builds TTL, PTTL (time left) and EXPIRETIME, PEXPIRETIME (the
// absolute deadline). All reply -2 if the key doesn't exist and -1 if it has
// no TTL. TTL rounds to the nearest second, as Redis does.
func ttlCommand(inMs, absolute bool) func(*client, [][]byte) {
	return func(cl *client, args [][]byte) {
		key := args[1]
		var reply int64
		cl.do(args[1:2], func(tx *store.Tx) {
			at, exists := tx.ExpireAt(key)
			switch {
			case !exists:
				reply = -2
			case at == 0:
				reply = -1
			case absolute && inMs:
				reply = at
			case absolute:
				reply = at / 1000
			default:
				left := max(at-tx.Now(), 0)
				if inMs {
					reply = left
				} else {
					reply = (left + 500) / 1000
				}
			}
		})
		cl.w.WriteInt(reply)
	}
}

// PERSIST key: remove the TTL. 1 if there was one, 0 if missing or no TTL.
func cmdPersist(cl *client, args [][]byte) {
	key := args[1]
	var reply int64
	cl.do(args[1:2], func(tx *store.Tx) {
		if at, exists := tx.ExpireAt(key); exists && at != 0 {
			tx.SetExpire(key, 0)
			reply = 1
		}
	})
	cl.w.WriteInt(reply)
}
