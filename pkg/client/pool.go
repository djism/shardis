package client

import (
	"context"
	"net"
	"time"

	"github.com/djism/shardis/internal/resp"
)

// pool holds connections to one node. At most `size` are in use at once
// (a slow node can't swallow every goroutine's connection), and idle
// connections older than idleTimeout are closed instead of reused: the
// server, or a NAT box in between, may have dropped them, and a write on a
// dead connection turns a clean failure into an unknown outcome.
type pool struct {
	addr        string
	slots       chan struct{} // one token per connection allowed to be in use
	idle        chan *conn
	idleTimeout time.Duration
	dialTimeout time.Duration
	readonly    bool // send READONLY on new connections (replica reads)
}

type conn struct {
	c    net.Conn
	w    *resp.Writer
	r    *resp.Reader
	used time.Time
}

func newPool(addr string, size int, o Options) *pool {
	return &pool{addr: addr, slots: make(chan struct{}, size), idle: make(chan *conn, size),
		idleTimeout: o.IdleTimeout, dialTimeout: o.Timeout, readonly: o.ReadFromReplicas}
}

// get returns a connection, waiting for a free slot until ctx is done.
// fresh reports whether it was just dialed (a dial error is a clean "this
// command never left", which is always safe to retry).
func (p *pool) get(ctx context.Context) (cn *conn, fresh bool, err error) {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	for {
		select {
		case cn := <-p.idle:
			if time.Since(cn.used) > p.idleTimeout {
				cn.c.Close()
				continue
			}
			return cn, false, nil
		default:
		}
		break
	}
	cn, err = p.dial(ctx)
	if err != nil {
		<-p.slots
		return nil, true, err
	}
	return cn, true, nil
}

func (p *pool) dial(ctx context.Context) (*conn, error) {
	d := net.Dialer{Timeout: p.dialTimeout}
	nc, err := d.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return nil, err
	}
	cn := &conn{c: nc, w: resp.NewWriter(nc), r: resp.NewReader(nc)}
	if p.readonly {
		// READONLY lets this connection read from a replica. On a primary
		// it's harmless, so every connection gets it when replica reads are on.
		nc.SetDeadline(time.Now().Add(p.dialTimeout))
		cn.w.WriteCommand([]byte("READONLY"))
		if err := cn.w.Flush(); err != nil {
			nc.Close()
			return nil, err
		}
		if _, err := cn.r.ReadValue(); err != nil {
			nc.Close()
			return nil, err
		}
	}
	return cn, nil
}

// put returns a healthy connection to the pool.
func (p *pool) put(cn *conn) {
	cn.used = time.Now()
	select {
	case p.idle <- cn:
	default:
		cn.c.Close()
	}
	<-p.slots
}

// discard closes a broken connection.
func (p *pool) discard(cn *conn) {
	cn.c.Close()
	<-p.slots
}

// closeIdle closes every idle connection. Called when one connection to
// the node failed: the others were opened to the same (probably dead)
// process, and reusing them would turn more requests into unknown outcomes.
func (p *pool) closeIdle() {
	for {
		select {
		case cn := <-p.idle:
			cn.c.Close()
		default:
			return
		}
	}
}
