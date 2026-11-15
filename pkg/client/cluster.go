package client

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djism/shardis/internal/keyslot"
	"github.com/djism/shardis/internal/resp"
)

// Options configures a Cluster client. The zero value is usable.
type Options struct {
	MaxRedirects int           // MOVED/ASK/TRYAGAIN hops per command (default 16)
	PoolSize     int           // idle connections kept per node (default 64)
	Timeout      time.Duration // dial and per-command I/O timeout (default 5s)
}

// Stats counts what the cluster made the client do.
type Stats struct {
	Moved, Ask, TryAgain, ClusterDown, Refreshes atomic.Int64
}

// Cluster sends commands to the node that owns each key's slot.
type Cluster struct {
	opts  Options
	seeds []string
	Stats Stats

	mu    sync.RWMutex
	slots [keyslot.Count]string // slot → owner's address ("" = unknown)
	pools map[string]chan *conn

	refreshing atomic.Bool
	lastFull   atomic.Int64 // unix ns of the last CLUSTER SLOTS refresh
}

type conn struct {
	c net.Conn
	w *resp.Writer
	r *resp.Reader
}

// ErrTooManyRedirects means a command bounced between nodes more than
// MaxRedirects times (a cluster that keeps changing, or a bug).
var ErrTooManyRedirects = errors.New("client: too many cluster redirects")

// NewCluster connects to any of the seed nodes and loads the slot map.
func NewCluster(seeds []string, opts Options) (*Cluster, error) {
	if opts.MaxRedirects == 0 {
		opts.MaxRedirects = 16
	}
	if opts.PoolSize == 0 {
		opts.PoolSize = 64
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	c := &Cluster{opts: opts, seeds: seeds, pools: map[string]chan *conn{}}
	if err := c.refresh(); err != nil {
		return nil, err
	}
	return c, nil
}

// Do runs one command, routed by its first key (args[1]; commands without
// keys go to any node). Server errors come back as a resp.Value of kind
// Error; the Go error is for network failures and exhausted redirects.
func (c *Cluster) Do(args ...[]byte) (resp.Value, error) {
	var slot uint16
	addr := ""
	if len(args) > 1 {
		slot = keyslot.Of(args[1])
		addr = c.owner(slot)
	}
	asking := false
	for hop := 0; ; hop++ {
		if addr == "" {
			addr = c.seeds[0]
		}
		v, err := c.send(addr, asking, args)
		if err != nil {
			return v, err
		}
		if v.Kind != resp.Error || hop >= c.opts.MaxRedirects {
			if v.Kind == resp.Error && isRedirect(v.Str) {
				return v, fmt.Errorf("%w (last reply: %s)", ErrTooManyRedirects, v.Str)
			}
			return v, nil
		}
		asking = false
		switch {
		case bytes.HasPrefix(v.Str, []byte("MOVED ")):
			// The slot lives elsewhere now: remember it, and refresh the
			// whole map in the background (if one slot moved, others did too).
			c.Stats.Moved.Add(1)
			s, to, ok := parseRedirect(v.Str)
			if !ok {
				return v, nil
			}
			c.setOwner(s, to)
			c.refreshSoon()
			addr = to
		case bytes.HasPrefix(v.Str, []byte("ASK ")):
			// Mid-migration: this key is on the target already. Ask there,
			// once, without changing the map (the slot hasn't moved yet).
			c.Stats.Ask.Add(1)
			_, to, ok := parseRedirect(v.Str)
			if !ok {
				return v, nil
			}
			addr, asking = to, true
		case bytes.HasPrefix(v.Str, []byte("TRYAGAIN")):
			c.Stats.TryAgain.Add(1)
			time.Sleep(time.Duration(hop+1) * time.Millisecond)
		case bytes.HasPrefix(v.Str, []byte("CLUSTERDOWN")):
			c.Stats.ClusterDown.Add(1)
			time.Sleep(time.Duration(hop+1) * 10 * time.Millisecond)
			c.refreshSoon()
			addr = c.owner(slot)
		default:
			return v, nil // a real error for the application
		}
	}
}

func isRedirect(msg []byte) bool {
	for _, p := range []string{"MOVED ", "ASK ", "TRYAGAIN", "CLUSTERDOWN"} {
		if bytes.HasPrefix(msg, []byte(p)) {
			return true
		}
	}
	return false
}

// parseRedirect reads "MOVED 3999 127.0.0.1:6381" / "ASK 3999 ...".
func parseRedirect(msg []byte) (uint16, string, bool) {
	f := bytes.Fields(msg)
	if len(f) != 3 {
		return 0, "", false
	}
	s, err := strconv.Atoi(string(f[1]))
	if err != nil || s < 0 || s >= keyslot.Count {
		return 0, "", false
	}
	return uint16(s), string(f[2]), true
}

// send runs one command on addr (preceded by ASKING if asked to, in the
// same write so it costs no extra round trip).
func (c *Cluster) send(addr string, asking bool, args [][]byte) (resp.Value, error) {
	cn, err := c.get(addr)
	if err != nil {
		return resp.Value{}, err
	}
	cn.c.SetDeadline(time.Now().Add(c.opts.Timeout))
	if asking {
		cn.w.WriteCommand([]byte("ASKING"))
	}
	cn.w.WriteCommand(args...)
	if err := cn.w.Flush(); err != nil {
		cn.c.Close()
		return resp.Value{}, err
	}
	if asking {
		if _, err := cn.r.ReadValue(); err != nil {
			cn.c.Close()
			return resp.Value{}, err
		}
	}
	v, err := cn.r.ReadValue()
	if err != nil {
		cn.c.Close()
		return resp.Value{}, err
	}
	c.put(addr, cn)
	return v, nil
}

func (c *Cluster) pool(addr string) chan *conn {
	c.mu.RLock()
	p := c.pools[addr]
	c.mu.RUnlock()
	if p != nil {
		return p
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if p = c.pools[addr]; p == nil {
		p = make(chan *conn, c.opts.PoolSize)
		c.pools[addr] = p
	}
	return p
}

func (c *Cluster) get(addr string) (*conn, error) {
	select {
	case cn := <-c.pool(addr):
		return cn, nil
	default:
	}
	nc, err := net.DialTimeout("tcp", addr, c.opts.Timeout)
	if err != nil {
		return nil, err
	}
	return &conn{c: nc, w: resp.NewWriter(nc), r: resp.NewReader(nc)}, nil
}

func (c *Cluster) put(addr string, cn *conn) {
	select {
	case c.pool(addr) <- cn:
	default:
		cn.c.Close() // pool full
	}
}

func (c *Cluster) owner(s uint16) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.slots[s]
}

func (c *Cluster) setOwner(s uint16, addr string) {
	c.mu.Lock()
	c.slots[s] = addr
	c.mu.Unlock()
}

// refreshSoon reloads the slot map in the background, at most every 100 ms.
func (c *Cluster) refreshSoon() {
	if time.Now().UnixNano()-c.lastFull.Load() < int64(100*time.Millisecond) || !c.refreshing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.refreshing.Store(false)
		c.refresh()
	}()
}

// refresh loads CLUSTER SLOTS from the first node that answers.
func (c *Cluster) refresh() error {
	c.lastFull.Store(time.Now().UnixNano())
	c.Stats.Refreshes.Add(1)
	var lastErr error
	for _, addr := range c.candidates() {
		v, err := c.send(addr, false, [][]byte{[]byte("CLUSTER"), []byte("SLOTS")})
		if err != nil {
			lastErr = err
			continue
		}
		if v.Kind != resp.Array {
			lastErr = fmt.Errorf("CLUSTER SLOTS from %s: %s", addr, v.Str)
			continue
		}
		var slots [keyslot.Count]string
		for _, r := range v.Elems {
			if len(r.Elems) < 3 || len(r.Elems[2].Elems) < 2 {
				continue
			}
			host, port := r.Elems[2].Elems[0].Str, r.Elems[2].Elems[1].Int
			a := net.JoinHostPort(string(host), strconv.FormatInt(port, 10))
			for s := r.Elems[0].Int; s <= r.Elems[1].Int && s < keyslot.Count; s++ {
				slots[s] = a
			}
		}
		c.mu.Lock()
		c.slots = slots
		c.mu.Unlock()
		return nil
	}
	return fmt.Errorf("client: no node answered CLUSTER SLOTS: %w", lastErr)
}

// candidates are the seeds plus every node in the current map.
func (c *Cluster) candidates() []string {
	seen := map[string]bool{}
	var out []string
	add := func(a string) {
		if a != "" && !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	for _, s := range c.seeds {
		add(s)
	}
	c.mu.RLock()
	for _, a := range c.slots {
		add(a)
	}
	c.mu.RUnlock()
	return out
}

// Close closes idle connections.
func (c *Cluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for a, p := range c.pools {
		close(p)
		for cn := range p {
			cn.c.Close()
		}
		delete(c.pools, a)
	}
}
