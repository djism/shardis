// Package metrics exposes a node's state to Prometheus (Iteration 15,
// ADR 045). Almost everything is read at scrape time from counters and
// snapshots the node already keeps, so the cache pays nothing between
// scrapes. The two exceptions live in the server's hot path: a per-command
// call counter and the command latency histogram.
package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/djism/shardis/internal/cluster"
	"github.com/djism/shardis/internal/replication"
	"github.com/djism/shardis/internal/server"
	"github.com/djism/shardis/internal/store"
)

// Sources are the parts of a node the metrics read. Repl and Cluster may
// be nil (single node without replication, tests).
type Sources struct {
	Server  *server.Server
	Engine  store.Engine
	Repl    *replication.Node
	Cluster *cluster.Cluster
}

// NewRegistry builds a registry with Shardis's metrics plus the standard Go
// runtime and process ones (GC pauses, heap, goroutines, CPU, RSS).
func NewRegistry(src Sources) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		src.Server.Latency(), &collector{src: src})
	return reg
}

func desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc("shardis_"+name, help, labels, nil)
}

var (
	dCommands      = desc("commands_total", "Commands run, by command.", "cmd")
	dClients       = desc("connected_clients", "Open client connections.")
	dKeys          = desc("keys", "Keys in this node's keyspace.")
	dMemory        = desc("used_memory_bytes", "Estimated memory used by the keyspace (ADR 011).")
	dExpired       = desc("expired_keys_total", "Keys removed because their TTL passed.")
	dEvicted       = desc("evicted_keys_total", "Keys evicted to stay under maxmemory.")
	dHits          = desc("keyspace_hits_total", "Reads that found their key.")
	dMisses        = desc("keyspace_misses_total", "Reads that didn't.")
	dPrimary       = desc("is_primary", "1 if this node is a primary, 0 if a replica.")
	dReplOffset    = desc("replication_offset_bytes", "This node's replication offset.")
	dReplicaLag    = desc("replica_lag_bytes", "How far each replica's acknowledged offset trails ours (primaries only).", "replica")
	dLinkUp        = desc("replication_link_up", "Replicas: 1 if the link to the primary is up.")
	dStateOK       = desc("cluster_state_ok", "1 if this node considers the cluster healthy (cluster_state:ok).")
	dMinority      = desc("cluster_minority", "1 if this node can't reach a majority of primaries (ADR 030).")
	dSlots         = desc("cluster_slots_owned", "Hash slots this node serves.")
	dKnown         = desc("cluster_known_nodes", "Nodes in this node's view.")
	dPFailNodes    = desc("cluster_nodes_pfail", "Nodes this node suspects (PFAIL).")
	dFailNodes     = desc("cluster_nodes_fail", "Nodes marked FAIL in this node's view.")
	dCurrentEpoch  = desc("cluster_current_epoch", "The cluster's current epoch, as this node knows it.")
	dMyEpoch       = desc("cluster_config_epoch", "This node's config epoch (its primary's, for a replica).")
	dOpenSlots     = desc("cluster_open_slots", "Slots this node is migrating or importing (ADR 032).")
	dBusSent       = desc("cluster_bus_messages_sent_total", "Cluster bus messages sent, by type.", "type")
	dBusRecv       = desc("cluster_bus_messages_received_total", "Cluster bus messages received, by type.", "type")
	dPFailMarks    = desc("cluster_pfail_marks_total", "Times this node marked another node PFAIL.")
	dFailMarks     = desc("cluster_fail_marks_total", "Times this node marked another node FAIL (majority).")
	dElections     = desc("cluster_elections_total", "Elections this node started as a replica.")
	dFailoversWon  = desc("cluster_failovers_won_total", "Elections this node won (became primary).")
	dTakeovers     = desc("cluster_takeovers_total", "CLUSTER FAILOVER TAKEOVERs (graceful handoffs) this node performed.")
	dEpochBumps    = desc("cluster_epoch_bumps_total", "Config epoch bumps without consensus (slot imports, ADR 035).")
	allDescriptors = []*prometheus.Desc{dCommands, dClients, dKeys, dMemory, dExpired, dEvicted, dHits, dMisses,
		dPrimary, dReplOffset, dReplicaLag, dLinkUp, dStateOK, dMinority, dSlots, dKnown, dPFailNodes, dFailNodes,
		dCurrentEpoch, dMyEpoch, dOpenSlots, dBusSent, dBusRecv, dPFailMarks, dFailMarks, dElections,
		dFailoversWon, dTakeovers, dEpochBumps}
)

type collector struct{ src Sources }

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range allDescriptors {
		ch <- d
	}
}

func gauge(ch chan<- prometheus.Metric, d *prometheus.Desc, v float64, labels ...string) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
}

func counter(ch chan<- prometheus.Metric, d *prometheus.Desc, v float64, labels ...string) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// Collect runs at scrape time (every few seconds), never on the data path.
func (c *collector) Collect(ch chan<- prometheus.Metric) {
	for cmd, n := range c.src.Server.CommandCalls() {
		counter(ch, dCommands, float64(n), cmd)
	}
	gauge(ch, dClients, float64(c.src.Server.Clients()))
	var keys int
	c.src.Engine.DoAll(func(tx *store.Tx) { keys = tx.Len() }) // sums 16384 map lengths: microseconds
	gauge(ch, dKeys, float64(keys))
	st := c.src.Engine.Stats()
	gauge(ch, dMemory, float64(st.UsedMemory))
	counter(ch, dExpired, float64(st.ExpiredKeys))
	counter(ch, dEvicted, float64(st.EvictedKeys))
	counter(ch, dHits, float64(st.KeyspaceHits))
	counter(ch, dMisses, float64(st.KeyspaceMisses))

	if r := c.src.Repl; r != nil {
		in := r.Info()
		gauge(ch, dPrimary, b2f(!r.IsReplica()))
		gauge(ch, dReplOffset, float64(in.Offset))
		if r.IsReplica() {
			gauge(ch, dLinkUp, b2f(in.LinkStatus == "up"))
		}
		for _, rep := range in.Replicas {
			gauge(ch, dReplicaLag, float64(max(in.Offset-rep.Offset, 0)), rep.ID)
		}
	}

	cl := c.src.Cluster
	if cl == nil {
		return
	}
	t := cl.View()
	gauge(ch, dStateOK, b2f(t.StateOK()))
	gauge(ch, dMinority, b2f(t.Minority()))
	slots := 0
	for _, r := range t.Me.Slots {
		slots += int(r.End-r.Start) + 1
	}
	gauge(ch, dSlots, float64(slots))
	gauge(ch, dKnown, float64(len(t.Nodes)))
	var pfail, fail int
	for _, n := range t.Nodes {
		if n.Has(cluster.FlagFail) {
			fail++
		} else if n.Has(cluster.FlagPFail) {
			pfail++
		}
	}
	gauge(ch, dPFailNodes, float64(pfail))
	gauge(ch, dFailNodes, float64(fail))
	gauge(ch, dCurrentEpoch, float64(t.CurrentEpoch))
	gauge(ch, dMyEpoch, float64(t.Me.Epoch))
	mig, imp := t.Migrations()
	gauge(ch, dOpenSlots, float64(len(mig)+len(imp)))
	s := cl.Stats()
	for i, name := range cluster.MessageTypes() {
		counter(ch, dBusSent, float64(s.SentByType[i].Load()), name)
		counter(ch, dBusRecv, float64(s.ReceivedByType[i].Load()), name)
	}
	counter(ch, dPFailMarks, float64(s.PFailMarks.Load()))
	counter(ch, dFailMarks, float64(s.FailMarks.Load()))
	counter(ch, dElections, float64(s.Elections.Load()))
	counter(ch, dFailoversWon, float64(s.FailoversWon.Load()))
	counter(ch, dTakeovers, float64(s.Takeovers.Load()))
	counter(ch, dEpochBumps, float64(s.EpochBumps.Load()))
}

// Serve runs the metrics HTTP server until ctx is done:
//
//	/metrics  Prometheus text format
//	/healthz  200 while the process runs (liveness)
//	/readyz   200 once the node serves clients (readiness)
func Serve(ctx context.Context, addr string, reg *prometheus.Registry) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	ok := func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) }
	mux.HandleFunc("/healthz", ok)
	mux.HandleFunc("/readyz", ok) // reached only after the client port is listening (see run.go)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
