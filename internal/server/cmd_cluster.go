package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/djism/shardis/internal/cluster"
	"github.com/djism/shardis/internal/keyslot"
	"github.com/djism/shardis/internal/store"
)

const errCrossSlot = "CROSSSLOT Keys in request don't hash to the same slot"

// route decides whether this node may run a command with these keys. It
// returns "" if so, otherwise the error to send: CROSSSLOT if the keys span
// slots, or MOVED <slot> <host:port> pointing at the slot's owner. Clients
// follow MOVED and (smart ones) update their slot map.
//
// A replica serves reads for its primary's slots only to connections that
// sent READONLY; everything else is redirected to the primary.
func (s *Server) route(cl *client, cmd command, keys [][]byte) string {
	slot := keyslot.Of(keys[0])
	for _, k := range keys[1:] {
		if keyslot.Of(k) != slot {
			return errCrossSlot
		}
	}
	t := s.cluster.View() // one lock-free snapshot for the whole decision (ADR 026)
	owner, me := t.Owner(slot), t.Me
	if owner == nil {
		return "CLUSTERDOWN Hash slot not served"
	}
	if owner == me {
		return ""
	}
	if cl.readonly && cmd.flags&flagWrite == 0 && me.Primary == owner {
		return "" // a read on a replica, which the client asked for
	}
	if s.bus.Active() {
		s.bus.Emit("moved", map[string]any{"slot": slot, "to": owner.Label(), "addr": owner.Addr})
	}
	return fmt.Sprintf("MOVED %d %s", slot, owner.Addr)
}

func cmdReadOnly(cl *client, _ [][]byte) {
	if clusterDisabled(cl) {
		return
	}
	cl.readonly = true
	cl.w.WriteSimple("OK")
}

func cmdReadWrite(cl *client, _ [][]byte) {
	if clusterDisabled(cl) {
		return
	}
	cl.readonly = false
	cl.w.WriteSimple("OK")
}

func clusterDisabled(cl *client) bool {
	if cl.srv.cluster == nil {
		cl.w.WriteError("ERR This instance has cluster support disabled")
		return true
	}
	return false
}

// CLUSTER <subcommand>: the read-only views of the topology that clients
// and tools use. Output formats follow Redis exactly; clients parse them.
func cmdCluster(cl *client, args [][]byte) {
	if clusterDisabled(cl) {
		return
	}
	c := cl.srv.cluster
	t := c.View()
	sub := strings.ToUpper(string(args[1]))
	switch {
	case sub == "MEET" && (len(args) == 4 || len(args) == 5):
		port, err := strconv.Atoi(string(args[3]))
		bus := 0
		if len(args) == 5 {
			bus, _ = strconv.Atoi(string(args[4]))
		}
		if err != nil || port < 1 || port > 65535 {
			cl.w.WriteError("ERR Invalid base port specified: " + string(args[3]))
			return
		}
		replyOK(cl, c.Meet(string(args[2]), port, bus))
	case sub == "ADDSLOTS" && len(args) >= 3:
		slots := make([]uint16, 0, len(args)-2)
		for _, a := range args[2:] {
			s, ok := parseSlot(cl, a)
			if !ok {
				return
			}
			slots = append(slots, s)
		}
		replyOK(cl, c.AddSlots(slots))
	case sub == "ADDSLOTSRANGE" && len(args) >= 4 && len(args)%2 == 0:
		var slots []uint16
		for i := 2; i < len(args); i += 2 {
			lo, ok1 := parseSlot(cl, args[i])
			if !ok1 {
				return
			}
			hi, ok2 := parseSlot(cl, args[i+1])
			if !ok2 {
				return
			}
			if lo > hi {
				cl.w.WriteError(fmt.Sprintf("ERR start slot number %d is greater than end slot number %d", lo, hi))
				return
			}
			for s := int(lo); s <= int(hi); s++ {
				slots = append(slots, uint16(s))
			}
		}
		replyOK(cl, c.AddSlots(slots))
	case sub == "REPLICATE" && len(args) == 3:
		replyOK(cl, c.Replicate(string(args[2])))
	case sub == "FORGET" && len(args) == 3:
		replyOK(cl, c.Forget(string(args[2])))
	case sub == "SET-CONFIG-EPOCH" && len(args) == 3:
		e, err := strconv.ParseUint(string(args[2]), 10, 64)
		if err != nil {
			cl.w.WriteError("ERR Invalid config epoch specified: " + string(args[2]))
			return
		}
		replyOK(cl, c.SetConfigEpoch(e))
	case sub == "KEYSLOT" && len(args) == 3:
		cl.w.WriteInt(int64(keyslot.Of(args[2])))
	case sub == "MYID" && len(args) == 2:
		cl.w.WriteBulkString(t.Me.ID)
	case sub == "SLOTS" && len(args) == 2:
		clusterSlots(cl, t)
	case sub == "SHARDS" && len(args) == 2:
		clusterShards(cl, t)
	case sub == "NODES" && len(args) == 2:
		cl.w.WriteBulkString(clusterNodes(t))
	case sub == "INFO" && len(args) == 2:
		cl.w.WriteBulkString(clusterInfo(t, c.Stats()))
	case sub == "COUNTKEYSINSLOT" && len(args) == 3:
		slot, ok := parseSlot(cl, args[2])
		if !ok {
			return
		}
		var n int
		cl.srv.engine.DoGroup(store.GroupOf(slot), func(tx *store.Tx, _ []uint16) { n = tx.SlotLen(slot) })
		cl.w.WriteInt(int64(n))
	case sub == "GETKEYSINSLOT" && len(args) == 4:
		slot, ok := parseSlot(cl, args[2])
		if !ok {
			return
		}
		count, okc := parseStrictInt(args[3])
		if !okc || count < 0 {
			cl.w.WriteError("ERR Invalid number of keys")
			return
		}
		var keys []string
		cl.srv.engine.DoGroup(store.GroupOf(slot), func(tx *store.Tx, _ []uint16) { keys = tx.SlotKeys(slot, int(count)) })
		cl.w.WriteArrayHeader(len(keys))
		for _, k := range keys {
			cl.w.WriteBulkString(k)
		}
	default:
		cl.w.WriteError(fmt.Sprintf("ERR unknown subcommand or wrong number of arguments for '%s'. Try CLUSTER HELP.", truncate(args[1], 128)))
	}
}

func replyOK(cl *client, err error) {
	if err != nil {
		cl.w.WriteError("ERR " + err.Error())
		return
	}
	cl.w.WriteSimple("OK")
}

func parseSlot(cl *client, b []byte) (uint16, bool) {
	n, ok := parseStrictInt(b)
	if !ok || n < 0 || n >= keyslot.Count {
		cl.w.WriteError("ERR Invalid slot")
		return 0, false
	}
	return uint16(n), true
}

// CLUSTER SLOTS: [[start, end, [ip, port, id], replica...], ...]
func clusterSlots(cl *client, t *cluster.Topology) {
	ranges := t.SlotRanges()
	cl.w.WriteArrayHeader(len(ranges))
	for _, r := range ranges {
		cl.w.WriteArrayHeader(3 + len(r.Owner.Replicas))
		cl.w.WriteInt(int64(r.Start))
		cl.w.WriteInt(int64(r.End))
		for _, n := range append([]*cluster.Node{r.Owner}, r.Owner.Replicas...) {
			host, port := n.HostPort()
			cl.w.WriteArrayHeader(3)
			cl.w.WriteBulkString(host)
			cl.w.WriteInt(int64(port))
			cl.w.WriteBulkString(n.ID)
		}
	}
}

// CLUSTER SHARDS (Redis 7): one entry per primary with its slot ranges and
// every node of the shard, as flat field/value lists.
func clusterShards(cl *client, t *cluster.Topology) {
	prims := t.Primaries()
	cl.w.WriteArrayHeader(len(prims))
	for _, p := range prims {
		cl.w.WriteArrayHeader(4)
		cl.w.WriteBulkString("slots")
		cl.w.WriteArrayHeader(2 * len(p.Slots))
		for _, r := range p.Slots {
			cl.w.WriteInt(int64(r.Start))
			cl.w.WriteInt(int64(r.End))
		}
		cl.w.WriteBulkString("nodes")
		nodes := append([]*cluster.Node{p}, p.Replicas...)
		cl.w.WriteArrayHeader(len(nodes))
		for _, n := range nodes {
			host, port := n.HostPort()
			role := "master"
			if !n.IsPrimary() {
				role = "replica"
			}
			cl.w.WriteArrayHeader(14)
			for _, kv := range [][2]string{{"id", n.ID}, {"ip", host}, {"endpoint", host}, {"role", role}, {"health", "online"}} {
				cl.w.WriteBulkString(kv[0])
				cl.w.WriteBulkString(kv[1])
			}
			cl.w.WriteBulkString("port")
			cl.w.WriteInt(int64(port))
			cl.w.WriteBulkString("replication-offset")
			cl.w.WriteInt(0)
		}
	}
}

// clusterNodes is the CLUSTER NODES text: one line per node,
//
//	<id> <ip:port@busport> <flags> <primary-id|-> <ping-sent-ms> <pong-recv-ms> <config-epoch> <link-state> <slots...>
//
// redis-cli --cluster and redis-benchmark --cluster parse this. ping-sent is
// when our current unanswered PING went out (0 = none), pong-recv when we
// last heard from the node, both in Unix ms, as in Redis.
func clusterNodes(t *cluster.Topology) string {
	var b strings.Builder
	for _, n := range t.Nodes {
		host, port := n.HostPort()
		var flags []string
		if n == t.Me {
			flags = append(flags, "myself")
		}
		primary := "-"
		if n.IsPrimary() {
			flags = append(flags, "master")
		} else {
			flags = append(flags, "slave")
			if n.Primary != nil {
				primary = n.Primary.ID
			}
		}
		for _, f := range []struct {
			bit  uint16
			name string
		}{{cluster.FlagPFail, "fail?"}, {cluster.FlagFail, "fail"}, {cluster.FlagHandshake, "handshake"}} {
			if n.Has(f.bit) {
				flags = append(flags, f.name)
			}
		}
		link := "connected"
		if !n.LinkUp {
			link = "disconnected"
		}
		fmt.Fprintf(&b, "%s %s:%d@%d %s %s %d %d %d %s", n.ID, host, port, n.BusPort, strings.Join(flags, ","),
			primary, unixMs(n.PingSent), unixMs(n.PongRecv), n.Epoch, link)
		for _, r := range n.Slots {
			if r.Start == r.End {
				fmt.Fprintf(&b, " %d", r.Start)
			} else {
				fmt.Fprintf(&b, " %d-%d", r.Start, r.End)
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func unixMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func clusterInfo(t *cluster.Topology, st *cluster.Stats) string {
	state := "fail"
	if t.StateOK() {
		state = "ok"
	}
	size := 0
	for _, n := range t.Primaries() {
		if len(n.Slots) > 0 {
			size++
		}
	}
	var b strings.Builder
	for _, kv := range [][2]string{
		{"cluster_state", state},
		{"cluster_slots_assigned", strconv.Itoa(t.AssignedSlots())},
		{"cluster_slots_ok", strconv.Itoa(t.AssignedSlots())},
		{"cluster_slots_pfail", "0"},
		{"cluster_slots_fail", "0"},
		{"cluster_known_nodes", strconv.Itoa(len(t.Nodes))},
		{"cluster_size", strconv.Itoa(size)},
		{"cluster_current_epoch", strconv.FormatUint(t.CurrentEpoch, 10)},
		{"cluster_my_epoch", strconv.FormatUint(t.Me.Epoch, 10)},
		{"cluster_stats_messages_sent", strconv.FormatInt(st.Sent.Load(), 10)},
		{"cluster_stats_messages_received", strconv.FormatInt(st.Received.Load(), 10)},
		{"cluster_stats_messages_dropped", strconv.FormatInt(st.Dropped.Load(), 10)},
	} {
		fmt.Fprintf(&b, "%s:%s\r\n", kv[0], kv[1])
	}
	return b.String()
}

// CLIENT: just enough for client libraries that announce themselves on
// connect (go-redis sends CLIENT SETINFO lib-name/lib-ver).
func cmdClient(cl *client, args [][]byte) {
	switch strings.ToUpper(string(args[1])) {
	case "SETINFO", "SETNAME":
		cl.w.WriteSimple("OK")
	case "GETNAME":
		cl.w.WriteNull()
	case "ID":
		cl.w.WriteInt(cl.id)
	default:
		cl.w.WriteError(fmt.Sprintf("ERR unknown subcommand '%s'. Try CLIENT HELP.", truncate(args[1], 128)))
	}
}
