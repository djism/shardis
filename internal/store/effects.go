package store

import (
	"strconv"
	"sync/atomic"
)

// Effect logging (ADR 014). Every change to the keyspace is recorded as its
// *effect*, not as the command that caused it:
//
//	INCR x            → SET x 6
//	EXPIRE k 10       → PEXPIREAT k 1731600005250   (absolute time)
//	SET k v EX 10     → SET k v PXAT 1731600005250
//	expiry / eviction → DEL k
//
// Effects are deterministic: replaying them gives the same state no matter
// when or where they're replayed. They're also idempotent per key (each one
// sets a key's value or TTL outright), which is what makes the fuzzy
// snapshot in ADR 016 correct. The same stream will feed replicas in
// Iteration 6.
//
// The encoding is RESP, the same format clients send, so the AOF can be read
// back with our normal parser (and inspected with any text editor).

// Log receives effect records. The engine calls Append while the command
// still holds its locks, so effects on the same key reach the log in the
// order they happened. Append must copy p and must not block for long
// (it's on every write's path).
type Log interface {
	Append(p []byte)
}

// logRef holds the engine's current Log; nil until SetLog is called.
type logRef struct{ p atomic.Pointer[Log] }

func (r *logRef) get() Log {
	if l := r.p.Load(); l != nil {
		return *l
	}
	return nil
}

func (r *logRef) set(l Log) {
	if l == nil {
		r.p.Store(nil)
		return
	}
	r.p.Store(&l)
}

// flushEffects hands this Tx's effects to the log (if any) and resets them.
// Engines call it after fn returns, while still holding the locks.
func (t *Tx) flushEffects(l Log) {
	if len(t.out) > 0 && l != nil {
		l.Append(t.out)
	}
	t.out = t.out[:0]
}

// The record* methods are no-ops unless logging is on.

func (t *Tx) recordSet(k string, e entry) {
	if t.logging {
		t.out = appendSet(t.out, k, e)
	}
}

func (t *Tx) recordDel(k string) {
	if t.logging {
		t.out = appendArrayHeader(t.out, 2)
		t.out = appendBulk(t.out, "DEL")
		t.out = appendBulk(t.out, k)
	}
}

func (t *Tx) recordExpire(k string, at int64) {
	if !t.logging {
		return
	}
	if at == 0 {
		t.out = appendArrayHeader(t.out, 2)
		t.out = appendBulk(t.out, "PERSIST")
		t.out = appendBulk(t.out, k)
		return
	}
	t.out = appendArrayHeader(t.out, 3)
	t.out = appendBulk(t.out, "PEXPIREAT")
	t.out = appendBulk(t.out, k)
	t.out = appendBulkInt(t.out, at)
}

func (t *Tx) recordFlush() {
	if t.logging {
		t.out = appendArrayHeader(t.out, 1)
		t.out = appendBulk(t.out, "FLUSHALL")
	}
}

// appendSet encodes SET k v [PXAT at]. Separate helpers per argument type
// (rather than one variadic ...string function) so values are copied
// straight from the entry into the buffer with no intermediate allocation.
func appendSet(dst []byte, k string, e entry) []byte {
	if e.expireAt == 0 {
		dst = appendArrayHeader(dst, 3)
	} else {
		dst = appendArrayHeader(dst, 5)
	}
	dst = appendBulk(dst, "SET")
	dst = appendBulk(dst, k)
	dst = appendBulk(dst, e.val)
	if e.expireAt != 0 {
		dst = appendBulk(dst, "PXAT")
		dst = appendBulkInt(dst, e.expireAt)
	}
	return dst
}

func appendArrayHeader(dst []byte, n int) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return append(dst, '\r', '\n')
}

func appendBulk[T string | []byte](dst []byte, b T) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(b)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, b...)
	return append(dst, '\r', '\n')
}

func appendBulkInt(dst []byte, n int64) []byte {
	var tmp [20]byte
	return appendBulk(dst, strconv.AppendInt(tmp[:0], n, 10))
}

// AppendSnapshot encodes every live key in slots as a SET effect (with PXAT
// if it has a TTL) and appends it to dst. The AOF rewrite calls it one slot
// group at a time, holding just that group's lock (ADR 016).
func (t *Tx) AppendSnapshot(dst []byte, slots []uint16) []byte {
	for _, s := range slots {
		t.checkSlot(s)
		for k, e := range t.ks.slots[s] {
			if !e.expiredAt(t.now) {
				dst = appendSet(dst, k, e)
			}
		}
	}
	return dst
}
