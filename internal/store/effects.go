package store

import (
	"bytes"
	"fmt"
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

// Tee returns a Log that appends to every non-nil log, in order. The
// primary uses it to feed the same effects to its AOF and to replicas.
func Tee(logs ...Log) Log {
	var out multiLog
	for _, l := range logs {
		if l != nil {
			out = append(out, l)
		}
	}
	switch len(out) {
	case 0:
		return nil
	case 1:
		return out[0]
	}
	return out
}

type multiLog []Log

func (m multiLog) Append(p []byte) {
	for _, l := range m {
		l.Append(p)
	}
}

// EffectKeys returns the keys an effect record touches, for Engine.Do.
// all is true for records that touch everything (FLUSHALL): use DoAll.
func EffectKeys(rec [][]byte) (keys [][]byte, all bool) {
	switch string(bytes.ToUpper(rec[0])) {
	case "FLUSHALL":
		return nil, true
	case "DEL":
		return rec[1:], false
	}
	if len(rec) < 2 {
		return nil, false
	}
	return rec[1:2], false
}

// ApplyEffect replays one effect record (the records effects.go writes).
// The AOF loader and replicas both use it, so a replayed log and a replica
// end up in exactly the state the primary had.
//
// Deadlines are absolute: a SET whose deadline has passed is not stored.
// DEL removes the key even if it has (logically) expired: on a replica,
// expired keys are hidden but kept until the primary's DEL arrives (ADR 019).
func ApplyEffect(tx *Tx, rec [][]byte) error {
	if len(rec) == 0 {
		return fmt.Errorf("empty record")
	}
	cmd := string(bytes.ToUpper(rec[0]))
	switch {
	case cmd == "SET" && (len(rec) == 3 || len(rec) == 5):
		var at int64
		if len(rec) == 5 {
			n, err := strconv.ParseInt(string(rec[4]), 10, 64)
			if err != nil || !bytes.EqualFold(rec[3], []byte("PXAT")) {
				return fmt.Errorf("bad SET record")
			}
			at = n
		}
		if at != 0 && at <= tx.Now() {
			tx.Remove(rec[1]) // already expired: same as not having it
			return nil
		}
		tx.SetWithExpire(rec[1], rec[2], at)
	case cmd == "DEL" && len(rec) >= 2:
		for _, k := range rec[1:] {
			tx.Remove(k)
		}
	case cmd == "PEXPIREAT" && len(rec) == 3:
		at, err := strconv.ParseInt(string(rec[2]), 10, 64)
		if err != nil {
			return fmt.Errorf("bad PEXPIREAT record")
		}
		tx.SetExpire(rec[1], max(at, 1)) // a past deadline deletes the key
	case cmd == "PERSIST" && len(rec) == 2:
		tx.SetExpire(rec[1], 0)
	case cmd == "FLUSHALL" && len(rec) == 1:
		tx.Flush()
	default:
		return fmt.Errorf("unknown effect record %q with %d args", rec[0], len(rec))
	}
	return nil
}
