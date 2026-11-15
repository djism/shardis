package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djism/shardis/internal/keyslot"
	"github.com/djism/shardis/internal/resp"
)

// RetryPolicy says what to do when a command's outcome is unknown: the
// connection broke after the command was sent (ADR 036).
type RetryPolicy int

const (
	// RetrySafe (default) resends only commands that can't do harm twice:
	// reads, and idempotent writes like SET and DEL. INCR and friends fail
	// with ErrUnknownOutcome instead. Nothing is applied twice behind the
	// application's back.
	RetrySafe RetryPolicy = iota
	// RetryAll resends everything (at-least-once). Fewer errors, but an
	// INCR can be applied twice. What most client libraries do by default.
	RetryAll
	// RetryNever returns every unknown outcome as an error.
	RetryNever
)

// Options configures a Cluster client. Zero values get sensible defaults.
type Options struct {
	// MaxRetryTime bounds how long one command keeps retrying while the
	// cluster says "not now" (node down, CLUSTERDOWN, TRYAGAIN). Longer
	// than a failover (node timeout + a few seconds), so a failover looks
	// like a slow request, not an error. Default 20s.
	MaxRetryTime           time.Duration
	MinBackoff, MaxBackoff time.Duration // default 5ms and 500ms (ADR 037)
	PoolSize               int           // max connections in use per node, default 64
	IdleTimeout            time.Duration // idle connections older than this are closed, default 30s
	Timeout                time.Duration // dial and per-attempt I/O timeout, default 5s
	MaxRedirects           int           // MOVED/ASK hops per command, default 32
	// ReadFromReplicas sends read-only commands to a replica of the slot's
	// primary (falling back to the primary if that fails). Spreads read
	// load, but a replica can be slightly behind: reads may be stale (ADR 039).
	ReadFromReplicas bool
	Retry            RetryPolicy
}

func (o *Options) defaults() {
	if o.MaxRetryTime == 0 {
		o.MaxRetryTime = 20 * time.Second
	}
	if o.MinBackoff == 0 {
		o.MinBackoff = 5 * time.Millisecond
	}
	if o.MaxBackoff == 0 {
		o.MaxBackoff = 500 * time.Millisecond
	}
	if o.PoolSize == 0 {
		o.PoolSize = 64
	}
	if o.IdleTimeout == 0 {
		o.IdleTimeout = 30 * time.Second
	}
	if o.Timeout == 0 {
		o.Timeout = 5 * time.Second
	}
	if o.MaxRedirects == 0 {
		o.MaxRedirects = 32
	}
}

// Stats counts what the cluster made the client do.
type Stats struct {
	Moved, Ask, TryAgain, ClusterDown, ReadOnly atomic.Int64 // replies that redirect or say "not now"
	NetErrors, Retries, Unknown                 atomic.Int64 // network failures, attempts after the first, unknown outcomes returned
	Refreshes, ReplicaReads                     atomic.Int64
}

// Cluster sends each command to the node that serves its keys.
type Cluster struct {
	opts  Options
	seeds []string
	Stats Stats

	mu    sync.RWMutex
	slots [keyslot.Count]*shard
	pools map[string]*pool
	down  map[string]time.Time // nodes that just failed: replica reads avoid them

	cmds       atomic.Pointer[map[string]cmdInfo]
	refreshing atomic.Bool
	lastFull   atomic.Int64
	rnd        func(int64) int64
}

// shard is one slot's primary and its replicas (client addresses).
type shard struct {
	primary  string
	replicas []string
}

var (
	ErrTooManyRedirects = errors.New("client: too many cluster redirects")
	ErrCrossSlot        = errors.New("client: CROSSSLOT keys in request don't hash to the same slot")
)

// NewCluster connects to any of the seed nodes, loads the slot map and the
// command table.
func NewCluster(seeds []string, opts Options) (*Cluster, error) {
	opts.defaults()
	c := &Cluster{opts: opts, seeds: seeds, pools: map[string]*pool{}, down: map[string]time.Time{}, rnd: jitter}
	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()
	if err := c.refresh(ctx); err != nil {
		return nil, err
	}
	c.loadCommands(ctx)
	return c, nil
}

// loadCommands asks a node for COMMAND (key positions and flags of every
// command, ADR 038). Without it we fall back to "the key is args[1]".
func (c *Cluster) loadCommands(ctx context.Context) {
	for _, addr := range c.candidates() {
		v, _, err := c.send(ctx, addr, "", [][]byte{[]byte("COMMAND")})
		if err == nil && v.Kind == resp.Array {
			t := parseCommandTable(v)
			c.cmds.Store(&t)
			return
		}
	}
}

func (c *Cluster) info(name string) (cmdInfo, bool) {
	if t := c.cmds.Load(); t != nil {
		ci, ok := (*t)[name]
		return ci, ok
	}
	return cmdInfo{}, false
}

// Do runs one command on the right node and returns its reply. Redirects,
// "not now" replies and network errors are retried within MaxRetryTime
// (and ctx). Errors the application should see (WRONGTYPE, a bad
// argument, ...) come back as a resp.Value of kind Error. The Go error is
// for exhausted retries, unknown outcomes and cancellation.
func (c *Cluster) Do(ctx context.Context, args ...[]byte) (resp.Value, error) {
	name := lowerName(args[0])
	ci, known := c.info(name)
	if !known && len(args) > 1 {
		ci = cmdInfo{first: 1, last: 1, step: 1} // unknown command: guess the key is args[1]
	}
	keys := ci.keys(args)
	slot, keyed := uint16(0), len(keys) > 0
	if keyed {
		slot = keyslot.Of(keys[0])
		for _, k := range keys[1:] {
			if keyslot.Of(k) != slot {
				return resp.Value{}, ErrCrossSlot
			}
		}
	}
	safe := safeToResend(name, ci, args)
	ctx, cancel := context.WithTimeout(ctx, c.opts.MaxRetryTime)
	defer cancel()

	var (
		askTo       string
		redirects   int
		attempt     int
		primaryOnly bool // after a replica fails us, read from the primary
		last        string
	)
	for {
		addr := askTo
		if addr == "" {
			addr = c.pick(slot, keyed, ci.readonly && c.opts.ReadFromReplicas && !primaryOnly)
		}
		v, sent, err := c.send(ctx, addr, askTo, args)
		askTo = ""
		retry := false
		switch {
		case err != nil && ctx.Err() != nil:
			return resp.Value{}, fmt.Errorf("%w (last: %v)", ErrRetryBudget, coalesce(last, err.Error()))
		case err != nil:
			c.Stats.NetErrors.Add(1)
			c.nodeFailed(addr)
			last = err.Error()
			if sent && !c.mayResend(safe) {
				c.Stats.Unknown.Add(1)
				return resp.Value{}, fmt.Errorf("%w: %v", ErrUnknownOutcome, err)
			}
			retry, primaryOnly = true, true
		case v.Kind != resp.Error:
			if addr != c.primaryOf(slot) && keyed && ci.readonly && c.opts.ReadFromReplicas {
				c.Stats.ReplicaReads.Add(1)
			}
			return v, nil
		default:
			msg := v.Str
			switch {
			case bytes.HasPrefix(msg, []byte("MOVED ")):
				// The slot lives elsewhere now: remember it, and refresh the
				// whole map soon (if one slot moved, others did too).
				c.Stats.Moved.Add(1)
				s, to, ok := parseRedirect(msg)
				if !ok {
					return v, nil
				}
				c.setPrimary(s, to)
				c.refreshSoon()
				primaryOnly = true
				if redirects++; redirects > c.opts.MaxRedirects {
					return v, fmt.Errorf("%w (last reply: %s)", ErrTooManyRedirects, msg)
				}
				continue
			case bytes.HasPrefix(msg, []byte("ASK ")):
				// Mid-migration: just this key is on the target already.
				// Ask there once, without changing the map.
				c.Stats.Ask.Add(1)
				_, to, ok := parseRedirect(msg)
				if !ok {
					return v, nil
				}
				askTo = to
				if redirects++; redirects > c.opts.MaxRedirects {
					return v, fmt.Errorf("%w (last reply: %s)", ErrTooManyRedirects, msg)
				}
				continue
			case bytes.HasPrefix(msg, []byte("TRYAGAIN")):
				c.Stats.TryAgain.Add(1)
			case bytes.HasPrefix(msg, []byte("CLUSTERDOWN")):
				c.Stats.ClusterDown.Add(1)
				c.refreshSoon()
				primaryOnly = true
			case bytes.HasPrefix(msg, []byte("READONLY")):
				// A write reached a node that's a replica now (it was
				// demoted after a failover). It didn't run: safe to retry
				// once our map catches up.
				c.Stats.ReadOnly.Add(1)
				c.refreshSoon()
			case bytes.HasPrefix(msg, []byte("LOADING")), bytes.HasPrefix(msg, []byte("MASTERDOWN")):
			default:
				return v, nil // a real error for the application
			}
			last = string(msg)
			retry = true
		}
		if retry {
			c.Stats.Retries.Add(1)
			if !sleep(ctx, backoff(attempt, c.opts.MinBackoff, c.opts.MaxBackoff, c.rnd)) {
				return resp.Value{}, fmt.Errorf("%w (last: %s)", ErrRetryBudget, last)
			}
			attempt++
		}
	}
}

func coalesce(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// mayResend applies the retry policy to an unknown outcome.
func (c *Cluster) mayResend(safe bool) bool {
	switch c.opts.Retry {
	case RetryAll:
		return true
	case RetryNever:
		return false
	default:
		return safe
	}
}

// send runs one command on addr. sent reports whether the command may have
// reached the server: false only when we never got a connection, which
// makes a retry always safe.
func (c *Cluster) send(ctx context.Context, addr, asking string, args [][]byte) (v resp.Value, sent bool, err error) {
	p := c.pool(addr)
	cn, _, err := p.get(ctx)
	if err != nil {
		return resp.Value{}, false, err
	}
	deadline := time.Now().Add(c.opts.Timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	cn.c.SetDeadline(deadline)
	if asking != "" {
		cn.w.WriteCommand([]byte("ASKING"))
	}
	cn.w.WriteCommand(args...)
	if err := cn.w.Flush(); err != nil {
		p.discard(cn)
		return resp.Value{}, true, err
	}
	if asking != "" {
		if _, err := cn.r.ReadValue(); err != nil {
			p.discard(cn)
			return resp.Value{}, true, err
		}
	}
	v, err = cn.r.ReadValue()
	if err != nil {
		p.discard(cn)
		return resp.Value{}, true, err
	}
	p.put(cn)
	return v, true, nil
}

// pick chooses the node for a slot: its primary, or a random healthy
// replica for reads when allowed. Keyless commands go to any known node.
func (c *Cluster) pick(slot uint16, keyed, replica bool) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !keyed {
		for _, s := range c.slots {
			if s != nil {
				return s.primary
			}
		}
		return c.seeds[0]
	}
	sh := c.slots[slot]
	if sh == nil {
		return c.seeds[0]
	}
	if replica && len(sh.replicas) > 0 {
		start := int(c.rnd(int64(len(sh.replicas))))
		for i := range sh.replicas {
			r := sh.replicas[(start+i)%len(sh.replicas)]
			if time.Now().After(c.down[r]) {
				return r
			}
		}
	}
	return sh.primary
}

func (c *Cluster) primaryOf(slot uint16) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if sh := c.slots[slot]; sh != nil {
		return sh.primary
	}
	return ""
}

// nodeFailed reacts to a network error talking to addr: its other idle
// connections are probably dead too, replica reads avoid it for a second,
// and the slot map gets refreshed (a failover may be under way).
func (c *Cluster) nodeFailed(addr string) {
	c.pool(addr).closeIdle()
	c.mu.Lock()
	c.down[addr] = time.Now().Add(time.Second)
	c.mu.Unlock()
	c.refreshSoon()
}

func (c *Cluster) pool(addr string) *pool {
	c.mu.RLock()
	p := c.pools[addr]
	c.mu.RUnlock()
	if p != nil {
		return p
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if p = c.pools[addr]; p == nil {
		p = newPool(addr, c.opts.PoolSize, c.opts)
		c.pools[addr] = p
	}
	return p
}

// setPrimary records a MOVED: slot s is now served by addr. Replicas are
// unknown until the next full refresh.
func (c *Cluster) setPrimary(s uint16, addr string) {
	c.mu.Lock()
	c.slots[s] = &shard{primary: addr}
	c.mu.Unlock()
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

// refreshSoon reloads the slot map in the background, at most every 100 ms.
func (c *Cluster) refreshSoon() {
	if time.Now().UnixNano()-c.lastFull.Load() < int64(100*time.Millisecond) || !c.refreshing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.refreshing.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), c.opts.Timeout)
		defer cancel()
		c.refresh(ctx)
	}()
}

// refresh loads CLUSTER SLOTS from the first node that answers, trying
// nodes that haven't just failed first.
func (c *Cluster) refresh(ctx context.Context) error {
	c.lastFull.Store(time.Now().UnixNano())
	c.Stats.Refreshes.Add(1)
	var lastErr error
	for _, addr := range c.candidates() {
		v, _, err := c.send(ctx, addr, "", [][]byte{[]byte("CLUSTER"), []byte("SLOTS")})
		if err != nil {
			lastErr = err
			continue
		}
		if v.Kind != resp.Array || len(v.Elems) == 0 {
			lastErr = fmt.Errorf("CLUSTER SLOTS from %s: %s", addr, v.Str)
			continue
		}
		var slots [keyslot.Count]*shard
		for _, r := range v.Elems {
			if len(r.Elems) < 3 {
				continue
			}
			sh := &shard{}
			for i, n := range r.Elems[2:] {
				if len(n.Elems) < 2 {
					continue
				}
				a := net.JoinHostPort(string(n.Elems[0].Str), strconv.FormatInt(n.Elems[1].Int, 10))
				if i == 0 {
					sh.primary = a
				} else {
					sh.replicas = append(sh.replicas, a)
				}
			}
			for s := r.Elems[0].Int; s <= r.Elems[1].Int && s < keyslot.Count; s++ {
				slots[s] = sh
			}
		}
		c.mu.Lock()
		c.slots = slots
		c.mu.Unlock()
		return nil
	}
	return fmt.Errorf("client: no node answered CLUSTER SLOTS: %w", lastErr)
}

// candidates: every known node and the seeds, healthy ones first, in random
// order (so a refresh doesn't always ask the same, possibly stale, node).
func (c *Cluster) candidates() []string {
	seen := map[string]bool{}
	var healthy, sick []string
	now := time.Now()
	c.mu.RLock()
	add := func(a string) {
		if a == "" || seen[a] {
			return
		}
		seen[a] = true
		if now.Before(c.down[a]) {
			sick = append(sick, a)
		} else {
			healthy = append(healthy, a)
		}
	}
	for _, s := range c.slots {
		if s != nil {
			add(s.primary)
			for _, r := range s.replicas {
				add(r)
			}
		}
	}
	for _, s := range c.seeds {
		add(s)
	}
	c.mu.RUnlock()
	rand.Shuffle(len(healthy), func(i, j int) { healthy[i], healthy[j] = healthy[j], healthy[i] })
	return append(healthy, sick...)
}

// Close closes idle connections. Call it after all Do calls have returned.
func (c *Cluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.pools {
		p.closeIdle()
	}
}
