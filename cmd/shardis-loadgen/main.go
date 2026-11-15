// Command shardis-loadgen generates realistic cache traffic against Shardis
// or Redis and reports hit rate, throughput and latency.
//
// The default mode is "cache-aside", what an app in front of a database
// does: GET the key; on a miss, "fetch it from the database" and SET it. Key
// popularity follows a Zipf distribution (a few keys are very hot), so with a
// memory limit the hit rate shows how well the eviction policy keeps the
// right keys.
//
//	shardis-loadgen -addr 127.0.0.1:7000 -keys 1000000 -theta 0.99 -duration 20s
//
// With -cluster it talks to a whole cluster through pkg/client (following
// MOVED and ASK), and -timeline writes per-interval throughput, errors,
// redirects and latency to a CSV, to watch a resharding or failover happen:
//
//	shardis-loadgen -cluster -addr 127.0.0.1:7000 -timeline 100ms -timeline-file t.csv
package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/djism/shardis/internal/resp"
	"github.com/djism/shardis/pkg/client"
)

type options struct {
	addr      string
	clients   int
	warmup    time.Duration
	duration  time.Duration
	keys      uint64
	theta     float64
	valueSize int
	label     string
	csv       bool
	seed      uint64
	cluster   bool
	every     time.Duration // timeline interval (0 = no timeline)
	tlFile    string
	preload   bool // SET every key once before starting (a full cache)
}

func main() {
	var o options
	flag.StringVar(&o.addr, "addr", "127.0.0.1:7000", "server address")
	flag.IntVar(&o.clients, "clients", 50, "concurrent connections")
	flag.DurationVar(&o.warmup, "warmup", 10*time.Second, "run this long before measuring (fills the cache)")
	flag.DurationVar(&o.duration, "duration", 20*time.Second, "measured run length")
	flag.Uint64Var(&o.keys, "keys", 1_000_000, "distinct keys in the workload")
	flag.Float64Var(&o.theta, "theta", 0.99, "Zipf skew (0 = uniform, 0.99 = YCSB default)")
	flag.IntVar(&o.valueSize, "value", 100, "value size in bytes")
	flag.StringVar(&o.label, "label", "", "label for the CSV row")
	flag.BoolVar(&o.csv, "csv", false, "print one CSV row (with -csv-header for the header)")
	header := flag.Bool("csv-header", false, "print the CSV header and exit")
	flag.Uint64Var(&o.seed, "seed", 1, "random seed (same seed = same request sequence per worker)")
	flag.BoolVar(&o.cluster, "cluster", false, "cluster mode: -addr is any node; follow MOVED/ASK with pkg/client")
	flag.DurationVar(&o.every, "timeline", 0, "write a per-interval timeline (e.g. 100ms) to -timeline-file")
	flag.StringVar(&o.tlFile, "timeline-file", "timeline.csv", "where -timeline goes")
	flag.BoolVar(&o.preload, "preload", false, "SET every key once before starting")
	flag.Parse()
	if *header {
		fmt.Println(csvHeader)
		return
	}
	res, err := run(o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
	if o.csv {
		fmt.Println(res.csv(o))
	} else {
		fmt.Print(res.human(o))
	}
}

// keyName maps a popularity rank to a key. Hashing scatters hot keys over
// the whole keyspace (and all hash slots) instead of making key:0..key:99
// the hot ones (YCSB calls this "scrambled Zipfian").
func keyName(rank uint64) []byte {
	h := fnv.New64a()
	var b [8]byte
	for i := range b {
		b[i] = byte(rank >> (8 * i))
	}
	h.Write(b[:])
	return strconv.AppendUint([]byte("key:"), h.Sum64(), 10)
}

type counts struct {
	hits, misses, sets, errors int64
	lat                        histogram // GET latency
}

func (c *counts) merge(o *counts) {
	c.hits += o.hits
	c.misses += o.misses
	c.sets += o.sets
	c.errors += o.errors
	c.lat.merge(&o.lat)
}

type result struct {
	counts
	moved, ask   int64 // cluster mode: redirects the client followed
	elapsed      time.Duration
	residentKeys int64
	evicted      int64
	usedMemory   int64
	firstError   string
}

func run(o options) (*result, error) {
	base := newZipf(o.keys, o.theta, rand.New(rand.NewPCG(o.seed, 0)))
	value := []byte(strings.Repeat("v", o.valueSize))
	var cc *client.Cluster
	if o.cluster {
		var err error
		if cc, err = client.NewCluster([]string{o.addr}, client.Options{PoolSize: o.clients}); err != nil {
			return nil, err
		}
		defer cc.Close()
	}
	if o.preload {
		if err := preload(o, cc, value); err != nil {
			return nil, err
		}
	}
	start := time.Now()
	var tl *timeline
	if o.every > 0 {
		tl = newTimeline(start, o.every, o.warmup+o.duration)
		if cc != nil {
			stop := make(chan struct{})
			defer close(stop)
			go tl.sampleRedirects(&cc.Stats, stop)
		}
	}
	measureFrom := start.Add(o.warmup)
	end := measureFrom.Add(o.duration)

	var mu sync.Mutex
	total := &result{}
	var wg sync.WaitGroup
	errc := make(chan error, o.clients)
	for w := range o.clients {
		wg.Go(func() {
			c, firstErr, err := worker(o, cc, tl, base.copy(rand.New(rand.NewPCG(o.seed, uint64(w)+1))), value, measureFrom, end)
			if err != nil {
				errc <- err
				return
			}
			mu.Lock()
			total.merge(c)
			if total.firstError == "" {
				total.firstError = firstErr
			}
			mu.Unlock()
		})
	}
	wg.Wait()
	close(errc)
	if err := <-errc; err != nil {
		return nil, err
	}
	total.elapsed = o.duration
	if tl != nil {
		f, err := os.Create(o.tlFile)
		if err != nil {
			return nil, err
		}
		tl.write(f, end)
		f.Close()
	}
	if cc != nil {
		total.moved, total.ask = cc.Stats.Moved.Load(), cc.Stats.Ask.Load()
		return total, nil // DBSIZE/INFO of one node would mislead
	}
	total.residentKeys, total.evicted, total.usedMemory = serverStats(o.addr)
	return total, nil
}

// preload SETs every key once (64 workers in parallel), so the run
// starts with a full cache (and a resharding has real data to move).
func preload(o options, cc *client.Cluster, value []byte) error {
	var wg sync.WaitGroup
	errc := make(chan error, 64)
	for w := range uint64(64) {
		wg.Go(func() {
			do, closeFn, err := connect(o, cc)
			if err != nil {
				errc <- err
				return
			}
			defer closeFn()
			for r := w; r < o.keys; r += 64 {
				if v, err := do([]byte("SET"), keyName(r), value); err != nil || v.Kind == resp.Error {
					errc <- fmt.Errorf("preload SET: %v %s", err, v.Str)
					return
				}
			}
		})
	}
	wg.Wait()
	close(errc)
	return <-errc
}

// connect returns a function that runs one command: through the cluster
// client, or on a dedicated connection to a single server.
func connect(o options, cc *client.Cluster) (func(args ...[]byte) (resp.Value, error), func(), error) {
	if cc != nil {
		return cc.Do, func() {}, nil
	}
	conn, err := net.Dial("tcp", o.addr)
	if err != nil {
		return nil, nil, err
	}
	w, r := resp.NewWriter(conn), resp.NewReader(conn)
	return func(args ...[]byte) (resp.Value, error) {
		w.WriteCommand(args...)
		if err := w.Flush(); err != nil {
			return resp.Value{}, err
		}
		return r.ReadValue()
	}, func() { conn.Close() }, nil
}

func worker(o options, cc *client.Cluster, tl *timeline, z *zipf, value []byte, measureFrom, end time.Time) (*counts, string, error) {
	rawDo, closeFn, err := connect(o, cc)
	if err != nil {
		return nil, "", err
	}
	defer closeFn()
	c := &counts{}
	// Every command goes on the timeline. In cluster mode a failed command
	// (network error, redirects exhausted) is counted, not fatal: the point
	// is to see errors during a resharding, if there are any.
	do := func(args ...[]byte) (resp.Value, error) {
		t0 := time.Now()
		v, err := rawDo(args...)
		now := time.Now()
		failed := err != nil || v.Kind == resp.Error
		tl.record(now, now.Sub(t0), failed)
		if err != nil && cc != nil {
			time.Sleep(time.Millisecond) // the caller counts it as an error reply
			return resp.Value{Kind: resp.Error, Str: []byte(err.Error())}, nil
		}
		return v, err
	}
	var firstErr string
	get, set := []byte("GET"), []byte("SET")
	for {
		now := time.Now()
		if now.After(end) {
			return c, firstErr, nil
		}
		measuring := now.After(measureFrom)
		key := keyName(z.next())
		t0 := time.Now()
		v, err := do(get, key)
		if err != nil {
			return nil, "", err
		}
		if measuring {
			c.lat.record(time.Since(t0))
		}
		if v.Kind == resp.Error {
			if measuring {
				c.errors++
			}
			if firstErr == "" {
				firstErr = string(v.Str)
			}
			continue
		}
		if !v.Null {
			if measuring {
				c.hits++
			}
			continue
		}
		// Miss: the app would load it from the database, then cache it.
		if measuring {
			c.misses++
		}
		v, err = do(set, key, value)
		if err != nil {
			return nil, "", err
		}
		if measuring {
			c.sets++
		}
		if v.Kind == resp.Error {
			if measuring {
				c.errors++
			}
			if firstErr == "" {
				firstErr = string(v.Str)
			}
		}
	}
}

// serverStats reads DBSIZE and INFO from the server after the run. Works on
// both Shardis and Redis.
func serverStats(addr string) (keys, evicted, used int64) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return
	}
	defer conn.Close()
	w, r := resp.NewWriter(conn), resp.NewReader(conn)
	w.WriteCommand([]byte("DBSIZE"))
	w.WriteCommand([]byte("INFO"))
	w.Flush()
	if v, err := r.ReadValue(); err == nil {
		keys = v.Int
	}
	v, err := r.ReadValue()
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(v.Str), "\r\n") {
		k, val, _ := strings.Cut(line, ":")
		n, _ := strconv.ParseInt(val, 10, 64)
		switch k {
		case "evicted_keys":
			evicted = n
		case "used_memory":
			used = n
		}
	}
	return
}

const csvHeader = "label,keys,theta,value_bytes,clients,duration_s,gets,hits,misses,hit_rate,gets_per_s,p50_ms,p99_ms,p999_ms,errors,resident_keys,evicted_keys,used_memory"

func (r *result) gets() int64 { return r.hits + r.misses }

func (r *result) hitRate() float64 {
	if r.gets() == 0 {
		return 0
	}
	return float64(r.hits) / float64(r.gets())
}

func (r *result) csv(o options) string {
	return fmt.Sprintf("%s,%d,%.2f,%d,%d,%.0f,%d,%d,%d,%.4f,%.0f,%.3f,%.3f,%.3f,%d,%d,%d,%d",
		o.label, o.keys, o.theta, o.valueSize, o.clients, r.elapsed.Seconds(),
		r.gets(), r.hits, r.misses, r.hitRate(), float64(r.gets())/r.elapsed.Seconds(),
		ms(r.lat.quantile(0.5)), ms(r.lat.quantile(0.99)), ms(r.lat.quantile(0.999)),
		r.errors, r.residentKeys, r.evicted, r.usedMemory)
}

func (r *result) human(o options) string {
	var b strings.Builder
	fmt.Fprintf(&b, "workload: %d keys, zipf theta=%.2f, %d-byte values, %d clients, %v measured after %v warmup\n",
		o.keys, o.theta, o.valueSize, o.clients, o.duration, o.warmup)
	fmt.Fprintf(&b, "GETs:     %d (%.0f/s)\n", r.gets(), float64(r.gets())/r.elapsed.Seconds())
	fmt.Fprintf(&b, "hit rate: %.2f%% (%d hits, %d misses)\n", 100*r.hitRate(), r.hits, r.misses)
	fmt.Fprintf(&b, "GET latency: p50 %.3f ms, p99 %.3f ms, p99.9 %.3f ms\n",
		ms(r.lat.quantile(0.5)), ms(r.lat.quantile(0.99)), ms(r.lat.quantile(0.999)))
	if !o.cluster { // in cluster mode one node's numbers would mislead
		fmt.Fprintf(&b, "server:   %d keys resident, %d evicted, used_memory %d\n", r.residentKeys, r.evicted, r.usedMemory)
	}
	if r.errors > 0 {
		fmt.Fprintf(&b, "errors:   %d commands failed, first: %s\n", r.errors, r.firstError)
	}
	if o.cluster {
		fmt.Fprintf(&b, "cluster:  followed %d MOVED and %d ASK redirects\n", r.moved, r.ask)
	}
	return b.String()
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
