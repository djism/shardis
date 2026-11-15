package server

import (
	"fmt"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"time"

	"github.com/djism/shardis/internal/store"
)

// Version is the Shardis version reported by INFO.
const Version = "0.4.0-dev"

// redisVersion is the Redis feature level we emulate. Some clients read
// redis_version from INFO to decide which commands to use.
const redisVersion = "7.4.0"

var infoSections = []struct {
	name  string
	write func(b *strings.Builder, s *Server)
}{
	{"server", infoServer},
	{"clients", infoClients},
	{"memory", infoMemory},
	{"persistence", infoPersistence},
	{"replication", infoReplication},
	{"cluster", func(b *strings.Builder, s *Server) { field(b, "cluster_enabled", boolInt(s.cluster != nil)) }},
	{"stats", infoStats},
	{"keyspace", infoKeyspace},
}

// INFO [section ...]: plain-text "field:value" lines grouped under "# Section"
// headers, as one bulk string. No argument (or "default"/"all"/"everything")
// means every section.
func cmdInfo(cl *client, args [][]byte) {
	want := map[string]bool{}
	for _, a := range args[1:] {
		want[strings.ToLower(string(a))] = true
	}
	all := len(want) == 0 || want["default"] || want["all"] || want["everything"]
	var b strings.Builder
	for _, sec := range infoSections {
		if !all && !want[sec.name] {
			continue
		}
		if b.Len() > 0 {
			b.WriteString("\r\n")
		}
		fmt.Fprintf(&b, "# %s%s\r\n", strings.ToUpper(sec.name[:1]), sec.name[1:])
		sec.write(&b, cl.srv)
	}
	cl.w.WriteBulkString(b.String())
}

func field(b *strings.Builder, name string, v any) { fmt.Fprintf(b, "%s:%v\r\n", name, v) }

func infoServer(b *strings.Builder, s *Server) {
	field(b, "redis_version", redisVersion)
	field(b, "shardis_version", Version)
	field(b, "go_version", runtime.Version())
	field(b, "process_id", os.Getpid())
	field(b, "tcp_port", portOf(s))
	field(b, "uptime_in_seconds", int64(time.Since(s.started).Seconds()))
	field(b, "hz", s.hz)
	field(b, "engine", s.engine.Name())
}

func portOf(s *Server) string {
	if s.ln == nil {
		return "0"
	}
	addr := s.ln.Addr().String()
	return addr[strings.LastIndexByte(addr, ':')+1:]
}

func infoClients(b *strings.Builder, s *Server) {
	open, _ := s.clientStats()
	field(b, "connected_clients", open)
}

// infoMemory reports our own estimate (what maxmemory is enforced against)
// next to what the Go runtime actually has, so the gap is visible (ADR 011).
func infoMemory(b *strings.Builder, s *Server) {
	st := s.engine.Stats()
	field(b, "used_memory", st.UsedMemory)
	field(b, "used_memory_human", humanBytes(st.UsedMemory))
	heap, total := goMemory()
	field(b, "go_heap_objects_bytes", heap) // live + not-yet-collected objects
	field(b, "go_total_bytes", total)       // everything the Go runtime has mapped and not released
	if rss, ok := rssBytes(); ok {
		field(b, "used_memory_rss", rss)
		field(b, "used_memory_rss_human", humanBytes(rss))
	}
	field(b, "maxmemory", s.maxmemory())
	field(b, "maxmemory_human", humanBytes(s.maxmemory()))
	field(b, "maxmemory_policy", s.policy())
	field(b, "maxmemory_samples", s.samples())
}

func infoStats(b *strings.Builder, s *Server) {
	st := s.engine.Stats()
	_, cmds := s.clientStats()
	field(b, "total_connections_received", s.connsTotal.Load())
	field(b, "total_commands_processed", cmds)
	field(b, "keyspace_hits", st.KeyspaceHits)
	field(b, "keyspace_misses", st.KeyspaceMisses)
	field(b, "expired_keys", st.ExpiredKeys)
	field(b, "evicted_keys", st.EvictedKeys)
}

// infoKeyspace takes the whole-keyspace lock to count keys, like DBSIZE.
func infoKeyspace(b *strings.Builder, s *Server) {
	var keys, expires int
	s.engine.DoAll(func(tx *store.Tx) { keys, expires = tx.Len(), tx.LenVolatile() })
	if keys > 0 {
		fmt.Fprintf(b, "db0:keys=%d,expires=%d,avg_ttl=0\r\n", keys, expires)
	}
}

// goMemory reads the Go runtime's own numbers via runtime/metrics, which,
// unlike runtime.ReadMemStats, doesn't pause the program.
func goMemory() (heapObjects, total uint64) {
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
	}
	metrics.Read(samples)
	get := func(i int) uint64 {
		if samples[i].Value.Kind() == metrics.KindUint64 {
			return samples[i].Value.Uint64()
		}
		return 0
	}
	return get(0), get(1) - get(2)
}

// rssBytes is the resident memory the OS reports, where that's cheap to read
// (Linux, i.e. Docker and Kubernetes). Elsewhere it's left out of INFO.
func rssBytes() (int64, bool) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	f := strings.Fields(string(data))
	if len(f) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseInt(f[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * int64(os.Getpagesize()), true
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2fG", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2fM", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2fK", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%dB", n)
}
