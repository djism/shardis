package server

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/djism/shardis/internal/cluster"
	"github.com/djism/shardis/internal/faults"
	"github.com/djism/shardis/internal/replication"
	"github.com/djism/shardis/internal/resp"
)

func replDisabled(cl *client) bool {
	if cl.srv.repl == nil {
		cl.w.WriteError("ERR replication is not enabled on this server")
		return true
	}
	return false
}

// REPLICAOF host port | REPLICAOF NO ONE
func cmdReplicaOf(cl *client, args [][]byte) {
	if replDisabled(cl) {
		return
	}
	if strings.EqualFold(string(args[1]), "NO") && strings.EqualFold(string(args[2]), "ONE") {
		cl.srv.repl.Promote()
		cl.w.WriteSimple("OK")
		return
	}
	port, err := strconv.Atoi(string(args[2]))
	if err != nil || port < 1 || port > 65535 {
		cl.w.WriteError("ERR Invalid master port")
		return
	}
	if err := cl.srv.repl.ReplicaOf(string(args[1]), port); err != nil {
		cl.w.WriteError("ERR " + err.Error())
		return
	}
	cl.w.WriteSimple("OK")
}

// REPLCONF is the replica's side of the handshake: listening-port <port>,
// capa <capability>. (ACKs arrive on the replication link, not here.)
func cmdReplConf(cl *client, args [][]byte) {
	if len(args) >= 3 {
		switch strings.ToLower(string(args[1])) {
		case "listening-port":
			p, err := strconv.Atoi(string(args[2]))
			if err != nil {
				cl.w.WriteError(errNotInt)
				return
			}
			cl.replPort = p
		case "ip-address":
			cl.replHost = string(args[2])
		}
	}
	cl.w.WriteSimple("OK")
}

// PSYNC <replid> <offset>: hand the connection to the replication node.
func cmdPSync(cl *client, args [][]byte) {
	if replDisabled(cl) {
		return
	}
	off, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		cl.w.WriteError(errNotInt)
		return
	}
	replid, port, host, node := string(args[1]), cl.replPort, cl.replHost, cl.srv.repl
	cl.takeover = func(c net.Conn, r *resp.Reader) { node.ServePSYNC(c, r, replid, off, port, host) }
}

// WAIT numreplicas timeout-ms: block until that many replicas have this
// connection's writes (everything written so far), or the timeout passes.
// Returns how many do. It never undoes a write.
func cmdWait(cl *client, args [][]byte) {
	if replDisabled(cl) {
		return
	}
	if cl.srv.repl.IsReplica() {
		cl.w.WriteError("ERR WAIT cannot be used with replica instances")
		return
	}
	n, ok1 := parseStrictInt(args[1])
	ms, ok2 := parseStrictInt(args[2])
	if !ok1 || !ok2 || ms < 0 {
		cl.w.WriteError(errNotInt)
		return
	}
	cl.w.WriteInt(int64(cl.srv.repl.Wait(int(n), time.Duration(ms)*time.Millisecond)))
}

// ROLE, in Redis's shape:
//
//	primary: ["master", offset, [[host, port, ack-offset], ...]]
//	replica: ["slave", host, port, state, offset]
func cmdRole(cl *client, _ [][]byte) {
	if replDisabled(cl) {
		return
	}
	in := cl.srv.repl.Info()
	if in.Role == replication.RolePrimary {
		cl.w.WriteArrayHeader(3)
		cl.w.WriteBulkString("master")
		cl.w.WriteInt(in.Offset)
		cl.w.WriteArrayHeader(len(in.Replicas))
		for _, r := range in.Replicas {
			host, port, _ := net.SplitHostPort(r.ID)
			cl.w.WriteArrayHeader(3)
			cl.w.WriteBulkString(host)
			cl.w.WriteBulkString(port)
			cl.w.WriteBulkString(strconv.FormatInt(r.Offset, 10))
		}
		return
	}
	state := map[string]string{"up": "connected", "sync": "sync", "connecting": "connecting"}[in.LinkStatus]
	if state == "" {
		state = "connect"
	}
	cl.w.WriteArrayHeader(5)
	cl.w.WriteBulkString("slave")
	cl.w.WriteBulkString(in.MasterHost)
	cl.w.WriteInt(int64(in.MasterPort))
	cl.w.WriteBulkString(state)
	cl.w.WriteInt(in.Offset)
}

func infoReplication(b *strings.Builder, s *Server) {
	if s.repl == nil {
		field(b, "role", "master")
		field(b, "connected_slaves", 0)
		return
	}
	in := s.repl.Info()
	field(b, "role", in.Role)
	if in.Role == replication.RoleReplica {
		field(b, "master_host", in.MasterHost)
		field(b, "master_port", in.MasterPort)
		status := "down"
		if in.LinkStatus == "up" {
			status = "up"
		}
		field(b, "master_link_status", status)
		field(b, "master_last_io_seconds_ago", int64(in.LastIOAgo.Seconds()))
		field(b, "master_sync_in_progress", boolInt(in.LinkStatus == "sync"))
		field(b, "slave_repl_offset", in.Offset)
		field(b, "slave_read_only", 1)
	}
	field(b, "connected_slaves", len(in.Replicas))
	for i, r := range in.Replicas {
		host, port, _ := net.SplitHostPort(r.ID)
		state := "online"
		if r.Syncing {
			state = "wait_bgsave"
		}
		fmt.Fprintf(b, "slave%d:ip=%s,port=%s,state=%s,offset=%d,lag=%d\r\n", i, host, port, state, r.Offset, int64(r.Lag.Seconds()))
	}
	field(b, "master_replid", in.ReplID)
	replid2 := in.ReplID2
	if replid2 == "" {
		replid2 = strings.Repeat("0", 40)
	}
	field(b, "master_replid2", replid2)
	field(b, "master_repl_offset", in.Offset)
	field(b, "second_repl_offset", in.SecondOffset)
	field(b, "repl_backlog_active", boolInt(in.BacklogOn))
	field(b, "repl_backlog_size", in.BacklogSize)
	field(b, "repl_backlog_first_byte_offset", in.BacklogStart)
	field(b, "sync_full", in.FullSyncs)
	field(b, "sync_partial_ok", in.PartialOK)
	field(b, "sync_partial_err", in.PartialErr)
}

// FAULT SET <point> <kind> [ms] [target] | FAULT CLEAR [point] | FAULT LIST
// Only exists when the server was started with --enable-fault-injection.
func cmdFault(cl *client, args [][]byte) {
	reg := cl.srv.faults
	if reg == nil {
		cl.w.WriteError("ERR fault injection is disabled (start with --enable-fault-injection yes)")
		return
	}
	words := make([]string, len(args)-1)
	for i, a := range args[1:] {
		words[i] = string(a)
	}
	switch strings.ToUpper(words[0]) {
	case "SET":
		rule, err := faults.Parse(words[1:])
		if err != nil {
			cl.w.WriteError("ERR " + err.Error())
			return
		}
		reg.Set(rule)
		cl.srv.log.Warn("fault injected", "rule", rule.String())
		cl.w.WriteSimple("OK")
	case "CLEAR":
		var p faults.Point
		if len(words) > 1 {
			p = faults.Point(strings.ToLower(words[1]))
		}
		reg.Clear(p)
		cl.w.WriteSimple("OK")
	case "PARTITION":
		partition(cl, words[1:])
	case "HEAL":
		reg.Clear("")
		cl.srv.log.Warn("fault injection: partition healed (all rules cleared)")
		cl.w.WriteSimple("OK")
	case "LIST":
		rules := reg.List()
		cl.w.WriteArrayHeader(len(rules))
		for _, r := range rules {
			cl.w.WriteBulkString(r.String())
		}
	default:
		cl.w.WriteError("ERR usage: FAULT SET|CLEAR|LIST|PARTITION|HEAL")
	}
}

// partition implements FAULT PARTITION <node>...: the listed nodes (names or
// IDs) form one side, everyone else the other. Send the same command to
// every node: each one cuts what it sends across the split, which is cluster
// bus messages (dropped) and the replication stream (severed), so together
// they cut both directions (ADR 031). Client connections are left alone: a
// client on either side can still reach the nodes on its side... and the
// nodes on the other side, which is the split-brain risk we measure.
func partition(cl *client, names []string) {
	if cl.srv.cluster == nil {
		cl.w.WriteError("ERR FAULT PARTITION needs cluster mode")
		return
	}
	if len(names) == 0 {
		cl.w.WriteError("ERR usage: FAULT PARTITION <node> [node ...]")
		return
	}
	t := cl.srv.cluster.View()
	side := map[*cluster.Node]bool{}
	for _, name := range names {
		var found *cluster.Node
		for _, n := range t.Nodes {
			if n.Name == name || n.ID == name || (len(name) >= 8 && strings.HasPrefix(n.ID, name)) {
				found = n
			}
		}
		if found == nil {
			cl.w.WriteError("ERR unknown node " + name)
			return
		}
		side[found] = true
	}
	reg, rules := cl.srv.faults, 0
	for _, n := range t.Nodes {
		if n == t.Me || side[n] == side[t.Me] {
			continue // same side as us: keep talking
		}
		reg.Set(faults.Rule{Point: faults.BusMessage, Kind: faults.Drop, Target: n.ID})
		reg.Set(faults.Rule{Point: faults.ReplStream, Kind: faults.Sever, Target: n.Internal})
		rules += 2
	}
	cl.srv.log.Warn("fault injection: partition", "side", names, "rules", rules)
	if cl.srv.bus.Active() {
		cl.srv.bus.Emit("partition", map[string]any{"node": t.Me.Label(), "side": names, "cut_links": rules / 2})
	}
	cl.w.WriteSimple("OK")
}
