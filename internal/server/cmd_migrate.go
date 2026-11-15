package server

import (
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/djism/shardis/internal/cluster"
	"github.com/djism/shardis/internal/keyslot"
	"github.com/djism/shardis/internal/resp"
	"github.com/djism/shardis/internal/store"
)

// Live slot migration (Iteration 11): ASKING, CLUSTER SETSLOT, DUMP, RESTORE,
// MIGRATE, and the per-slot gate that keeps them atomic (ADR 032-035).
//
// The gate. In Redis, "is the key still here? then run the command" can't be
// interrupted by MIGRATE, because Redis runs one command at a time. We run
// commands in parallel, so a key could be migrated away between the check
// and the command, and the command would then run on a key that's gone (a
// write would recreate it on the old owner, where nobody will look). So
// every keyed command holds its slot's gate shared, from routing through
// execution. MIGRATE and SETSLOT NODE hold it exclusively. Different slots
// never wait for each other, and an uncontended RLock is two atomic adds.

// slotGates is one reader/writer lock per hash slot (ADR 033).
type slotGates [keyslot.Count]sync.RWMutex

// lockSlots takes the gates of every slot the keys hash to, exclusively, in
// ascending slot order (so two MIGRATEs can't deadlock), and returns the
// unlock function.
func (s *Server) lockSlots(keys [][]byte) func() {
	slots := make([]uint16, 0, 1)
	for _, k := range keys {
		slots = append(slots, keyslot.Of(k))
	}
	slices.Sort(slots)
	slots = slices.Compact(slots)
	for _, sl := range slots {
		s.gates[sl].Lock()
	}
	return func() {
		for _, sl := range slots {
			s.gates[sl].Unlock()
		}
	}
}

const errTryAgain = "TRYAGAIN Multiple keys request during rehashing of slot"

// routeMigrating decides for a slot we own that's being moved away: keys we
// still have are served here; if none are here, they've moved (or never
// existed), so -ASK sends the client to the target for this one command. A
// mix means the keys are split across two nodes right now: TRYAGAIN.
// Runs with the slot's gate held, so MIGRATE can't move keys while we look.
func (s *Server) routeMigrating(slot uint16, keys [][]byte, to *cluster.Node) string {
	here := s.countExisting(keys)
	switch {
	case here == len(keys):
		return ""
	case here == 0:
		if s.bus.Active() {
			s.bus.Emit("ask", map[string]any{"slot": slot, "to": to.Label()})
		}
		return fmt.Sprintf("ASK %d %s", slot, to.Addr)
	default:
		return errTryAgain
	}
}

func (s *Server) countExisting(keys [][]byte) int {
	n := 0
	s.engine.Do(keys, func(tx *store.Tx) {
		for _, k := range keys {
			if tx.Exists(k) {
				n++
			}
		}
	})
	return n
}

// ASKING: the next command on this connection may be served for a slot
// we're importing (one command only, then the flag resets).
func cmdAsking(cl *client, _ [][]byte) {
	if clusterDisabled(cl) {
		return
	}
	cl.asking = true
	cl.w.WriteSimple("OK")
}

// CLUSTER SETSLOT <slot> IMPORTING <id> | MIGRATING <id> | STABLE | NODE <id>
func clusterSetSlot(cl *client, args [][]byte) {
	c := cl.srv.cluster
	slot, ok := parseSlot(cl, args[2])
	if !ok {
		return
	}
	sub := strings.ToUpper(string(args[3]))
	switch {
	case sub == "MIGRATING" && len(args) == 5:
		replyOK(cl, c.SetSlotMigrating(slot, string(args[4])))
	case sub == "IMPORTING" && len(args) == 5:
		replyOK(cl, c.SetSlotImporting(slot, string(args[4])))
	case sub == "STABLE" && len(args) == 4:
		replyOK(cl, c.SetSlotStable(slot))
	case sub == "NODE" && len(args) == 5:
		// Exclusive gate: no command can add a key to the slot between
		// counting its keys and giving it away.
		g := &cl.srv.gates[slot]
		g.Lock()
		var n int
		cl.srv.engine.DoGroup(store.GroupOf(slot), func(tx *store.Tx, _ []uint16) { n = tx.SlotLen(slot) })
		err := c.SetSlotNode(slot, string(args[4]), n)
		g.Unlock()
		replyOK(cl, err)
	default:
		cl.w.WriteError("ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP")
	}
}

// DUMP key
func cmdDump(cl *client, args [][]byte) {
	var val []byte
	var ok bool
	cl.do(args[1:2], func(tx *store.Tx) { val, ok = tx.Get(args[1]) })
	if !ok {
		cl.w.WriteNull()
		return
	}
	cl.w.WriteBulk(store.Dump(val))
}

// RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME s] [FREQ f]
// ttl is in ms, relative unless ABSTTL; 0 = no expiry. IDLETIME and FREQ
// are accepted (redis-cli may send them) and ignored.
func cmdRestore(cl *client, args [][]byte) {
	key := args[1]
	ttl, ok := parseStrictInt(args[2])
	if !ok || ttl < 0 {
		cl.w.WriteError("ERR Invalid TTL value, must be >= 0")
		return
	}
	var replace, absTTL bool
	for i := 4; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "REPLACE":
			replace = true
		case "ABSTTL":
			absTTL = true
		case "IDLETIME", "FREQ":
			if i+1 >= len(args) {
				cl.w.WriteError("ERR syntax error")
				return
			}
			i++
		default:
			cl.w.WriteError("ERR syntax error")
			return
		}
	}
	val, err := store.ParseDump(args[3])
	if err != nil {
		cl.w.WriteError("ERR " + err.Error())
		return
	}
	busy := false
	cl.do(args[1:2], func(tx *store.Tx) {
		if tx.Exists(key) && !replace {
			busy = true
			return
		}
		at := ttl
		if ttl != 0 && !absTTL {
			at = tx.Now() + ttl
		}
		if at != 0 && at <= tx.Now() {
			tx.Delete(key) // already expired: like Redis, OK and nothing stored
			return
		}
		tx.SetWithExpire(key, val, at)
	})
	if busy {
		cl.w.WriteError("BUSYKEY Target key name already exists.")
		return
	}
	cl.w.WriteSimple("OK")
}

// MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE] [AUTH pw] [AUTH2 user pw] [KEYS key...]
//
// Moves keys to another node: send them (RESTORE-ASKING, pipelined), wait for
// every reply, then delete the ones the target accepted. The slots' gates
// are held exclusively for the whole round trip, so no command can change or
// read these keys while they're in flight. To a client, each key is either
// still here or already there, never neither (ADR 033).
func cmdMigrate(cl *client, args [][]byte) {
	s := cl.srv
	port, ok := parseStrictInt(args[2])
	timeout, ok2 := parseStrictInt(args[5])
	if !ok || port < 1 || port > 65535 || !ok2 || timeout < 0 {
		cl.w.WriteError("ERR value is not an integer or out of range")
		return
	}
	if db, ok := parseStrictInt(args[4]); !ok || db != 0 {
		cl.w.WriteError("ERR DB index is out of range")
		return
	}
	if timeout == 0 {
		timeout = 1000
	}
	var copyKeys, replace bool
	var keys [][]byte
	for i := 6; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "COPY":
			copyKeys = true
		case "REPLACE":
			replace = true
		case "AUTH":
			i++ // no auth in Shardis; accept and ignore
		case "AUTH2":
			i += 2
		case "KEYS":
			if len(args[3]) != 0 {
				cl.w.WriteError("ERR When using MIGRATE KEYS option, the key argument must be set to the empty string")
				return
			}
			keys = args[i+1:]
			i = len(args)
		default:
			cl.w.WriteError("ERR syntax error")
			return
		}
	}
	if keys == nil {
		keys = args[3:4]
	}
	if len(keys) == 0 || len(keys[0]) == 0 && len(keys) == 1 {
		cl.w.WriteSimple("NOKEY")
		return
	}

	unlock := s.lockSlots(keys)
	defer unlock()
	type item struct {
		key, payload []byte
		ttl          int64
	}
	var items []item
	s.engine.Do(keys, func(tx *store.Tx) {
		for _, k := range keys {
			val, ok := tx.Get(k)
			if !ok {
				continue
			}
			at, _ := tx.ExpireAt(k)
			ttl := int64(0)
			if at != 0 {
				ttl = max(at-tx.Now(), 1) // relative, like Redis: immune to clock skew between nodes
			}
			items = append(items, item{k, store.Dump(val), ttl})
		}
	})
	if len(items) == 0 {
		cl.w.WriteSimple("NOKEY")
		return
	}

	addr := net.JoinHostPort(string(args[1]), strconv.FormatInt(port, 10))
	deadline := time.Duration(timeout) * time.Millisecond
	mc, err := s.migrateConns.get(addr, deadline)
	if err != nil {
		cl.w.WriteError(fmt.Sprintf("IOERR error or timeout connecting to the client: %v", err))
		return
	}
	restore := []byte("RESTORE-ASKING")
	mc.conn.SetDeadline(time.Now().Add(deadline))
	for _, it := range items {
		a := [][]byte{restore, it.key, strconv.AppendInt(nil, it.ttl, 10), it.payload}
		if replace {
			a = append(a, []byte("REPLACE"))
		}
		mc.w.WriteCommand(a...)
	}
	if err := mc.w.Flush(); err != nil {
		mc.conn.Close()
		cl.w.WriteError("IOERR error or timeout writing to target instance")
		return
	}
	var accepted [][]byte
	var firstErr string
	for _, it := range items {
		v, err := mc.r.ReadValue()
		if err != nil {
			// We don't know which keys made it. Keep them all here: at worst
			// the target has a copy too, and the next MIGRATE (with REPLACE)
			// overwrites it. Deleting a key the target never got would lose it.
			mc.conn.Close()
			cl.w.WriteError("IOERR error or timeout reading to target instance")
			return
		}
		if v.Kind == resp.Error {
			if firstErr == "" {
				firstErr = string(v.Str)
			}
			continue
		}
		accepted = append(accepted, it.key)
	}
	s.migrateConns.put(addr, mc)
	if !copyKeys && len(accepted) > 0 {
		s.engine.Do(accepted, func(tx *store.Tx) {
			for _, k := range accepted {
				tx.Delete(k) // logged and replicated like any DEL
			}
		})
	}
	if s.bus.Active() {
		s.bus.Emit("migrate_batch", map[string]any{"to": addr, "keys": len(accepted), "slot": keyslot.Of(keys[0])})
	}
	if firstErr != "" {
		cl.w.WriteError("ERR Target instance replied with error: " + firstErr)
		return
	}
	cl.w.WriteSimple("OK")
}

// migrateConns caches one idle connection per target, like Redis's
// migrate_cached_sockets: a resharding sends thousands of MIGRATEs to the
// same node, and a TCP handshake each would double their cost.
type migrateConns struct {
	mu   sync.Mutex
	idle map[string]*migrateConn
}

type migrateConn struct {
	conn net.Conn
	w    *resp.Writer
	r    *resp.Reader
	used time.Time
}

func (m *migrateConns) get(addr string, timeout time.Duration) (*migrateConn, error) {
	m.mu.Lock()
	mc := m.idle[addr]
	delete(m.idle, addr)
	m.mu.Unlock()
	if mc != nil && time.Since(mc.used) < 10*time.Second {
		return mc, nil
	}
	if mc != nil {
		mc.conn.Close() // idle too long; the target may have dropped it
	}
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &migrateConn{conn: c, w: resp.NewWriter(c), r: resp.NewReader(c)}, nil
}

func (m *migrateConns) put(addr string, mc *migrateConn) {
	mc.used = time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.idle == nil {
		m.idle = map[string]*migrateConn{}
	}
	if old := m.idle[addr]; old != nil {
		old.conn.Close() // two MIGRATEs ran at once; keep one connection
	}
	m.idle[addr] = mc
}

func (m *migrateConns) closeAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for a, mc := range m.idle {
		mc.conn.Close()
		delete(m.idle, a)
	}
}
