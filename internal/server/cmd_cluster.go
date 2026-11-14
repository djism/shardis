package server

import (
	"fmt"
	"strconv"
	"strings"

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
	owner, me := s.topo.Owner(slot), s.topo.Me
	if owner == me {
		return ""
	}
	if cl.readonly && cmd.flags&flagWrite == 0 && me.Primary == owner {
		return "" // a read on a replica, which the client asked for
	}
	if s.bus.Active() {
		s.bus.Emit("moved", map[string]any{"slot": slot, "to": owner.Name, "addr": owner.Addr})
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
	if cl.srv.topo == nil {
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
	t := cl.srv.topo
	sub := strings.ToUpper(string(args[1]))
	switch {
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
		cl.w.WriteBulkString(clusterInfo(t))
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
//	<id> <ip:port@busport> <flags> <primary-id|-> <ping-sent> <pong-recv> <config-epoch> <link-state> <slots...>
//
// redis-cli --cluster and redis-benchmark --cluster parse this to find nodes.
// The bus port (port + 10000) isn't listening yet; that's Iteration 8.
func clusterNodes(t *cluster.Topology) string {
	var b strings.Builder
	for _, n := range t.Nodes {
		host, port := n.HostPort()
		flags := "master"
		primary := "-"
		if !n.IsPrimary() {
			flags, primary = "slave", n.Primary.ID
		}
		if n == t.Me {
			flags = "myself," + flags
		}
		fmt.Fprintf(&b, "%s %s:%d@%d %s %s 0 0 %d connected", n.ID, host, port, port+10000, flags, primary, n.Epoch)
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

func clusterInfo(t *cluster.Topology) string {
	var b strings.Builder
	for _, kv := range [][2]string{
		{"cluster_state", "ok"},
		{"cluster_slots_assigned", strconv.Itoa(keyslot.Count)},
		{"cluster_slots_ok", strconv.Itoa(keyslot.Count)},
		{"cluster_slots_pfail", "0"},
		{"cluster_slots_fail", "0"},
		{"cluster_known_nodes", strconv.Itoa(len(t.Nodes))},
		{"cluster_size", strconv.Itoa(len(t.Primaries()))},
		{"cluster_current_epoch", strconv.Itoa(t.MaxEpoch())},
		{"cluster_my_epoch", strconv.Itoa(t.Me.Epoch)},
		{"cluster_stats_messages_sent", "0"},
		{"cluster_stats_messages_received", "0"},
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
