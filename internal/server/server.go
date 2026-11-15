// Package server accepts TCP connections and runs RESP commands on them.
//
// Model: one goroutine per connection. Each one loops read → execute → reply.
// Goroutines are cheap (a few KB of stack), so thousands of clients are fine,
// and the code reads top to bottom with no callbacks.
package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djism/shardis/internal/cluster"
	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/faults"
	"github.com/djism/shardis/internal/persist"
	"github.com/djism/shardis/internal/replication"
	"github.com/djism/shardis/internal/resp"
	"github.com/djism/shardis/internal/store"
)

// Options configures a Server. Engine is required.
type Options struct {
	Log     *slog.Logger      // nil = discard
	Engine  store.Engine      // the caller owns it and closes it after Serve returns
	Evictor *store.Evictor    // nil = no memory limit
	AOF     *persist.AOF      // nil = no persistence; the caller closes it after Serve returns
	Repl    *replication.Node // nil = no replication (tests); the caller closes it
	Faults  *faults.Registry  // nil = FAULT command disabled (the default)
	Cluster *cluster.Cluster  // nil = not in cluster mode; the caller starts and closes it
	Bus     *events.Bus       // nil = no events
	// AllowReadsWhenDown: while the cluster is down (or this node is in a
	// minority), still serve reads (possibly stale). Writes are refused.
	AllowReadsWhenDown bool
	Port               int // our client port, for INFO
	Hz                 int // for INFO and CONFIG GET hz
}

type Server struct {
	log       *slog.Logger
	engine    store.Engine
	evictor   *store.Evictor
	aof       *persist.AOF
	repl      *replication.Node
	faults    *faults.Registry
	cluster   *cluster.Cluster
	bus       *events.Bus
	readsDown bool
	hz        int
	commands  map[string]command
	started   time.Time

	ln net.Listener

	mu      sync.Mutex
	conns   map[net.Conn]*client // open connections (client set once its goroutine starts)
	closing bool

	wg sync.WaitGroup // one per connection goroutine

	connsTotal atomic.Int64 // connections ever accepted
	cmdsDone   atomic.Int64 // commands run by connections that have since closed
}

func New(opts Options) *Server {
	log := opts.Log
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s := &Server{
		log: log, engine: opts.Engine, evictor: opts.Evictor, aof: opts.AOF, repl: opts.Repl, faults: opts.Faults, cluster: opts.Cluster, bus: opts.Bus, readsDown: opts.AllowReadsWhenDown, hz: opts.Hz,
		conns: make(map[net.Conn]*client), started: time.Now(),
	}
	s.commands = s.commandTable()
	return s
}

// Listen binds the address. It's separate from Serve so callers (and tests
// using port 0) can read the real address before serving starts.
func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln
	return nil
}

// Addr is the bound address, e.g. 127.0.0.1:54321 when listening on port 0.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Serve accepts connections until ctx is cancelled, then closes the listener
// and every open connection, and waits for their goroutines to finish.
func (s *Server) Serve(ctx context.Context) error {
	stopWatch := context.AfterFunc(ctx, s.shutdown)
	defer stopWatch()

	var acceptErr error
	for {
		c, err := s.ln.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				acceptErr = err // a real failure, not our own shutdown
				s.shutdown()
			}
			break
		}
		if !s.track(c) {
			c.Close() // arrived while shutting down
			continue
		}
		s.wg.Add(1)
		go s.handle(c)
	}
	s.wg.Wait()
	return acceptErr
}

func (s *Server) shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return
	}
	s.closing = true
	s.ln.Close()
	// Closing a conn makes its goroutine's blocked Read return an error,
	// which is how we get every handler to exit.
	for c := range s.conns {
		c.Close()
	}
}

func (s *Server) track(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.conns[c] = nil
	s.connsTotal.Add(1)
	return true
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	if cl := s.conns[c]; cl != nil {
		s.cmdsDone.Add(cl.cmds.Load())
	}
	delete(s.conns, c)
	s.mu.Unlock()
	c.Close()
}

// clientStats returns the number of open connections and commands run so far.
func (s *Server) clientStats() (open int, commands int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	commands = s.cmdsDone.Load()
	for _, cl := range s.conns {
		if cl != nil {
			commands += cl.cmds.Load()
		}
	}
	return len(s.conns), commands
}

// client is per-connection state that command handlers can see.
type client struct {
	srv  *Server
	w    *resp.Writer
	quit bool // set by QUIT: flush the reply, then hang up

	// readonly is set by READONLY: in cluster mode, this connection may read
	// from a replica (for the replica's primary's slots).
	readonly bool
	id       int64

	// replPort/replHost are what a replica announced with REPLCONF
	// listening-port / ip-address.
	replPort int
	replHost string
	// takeover, set by PSYNC, turns the connection into a replication link
	// after the current command.
	takeover func(net.Conn, *resp.Reader)

	// needDurable is set after a write command; the next bytes sent to the
	// socket wait for the AOF first (durableConn).
	needDurable bool

	// cmds counts this connection's commands. Per connection rather than one
	// server-wide counter, so 10 cores running commands don't all fight over
	// the same memory. INFO adds them up.
	cmds atomic.Int64
}

func (s *Server) handle(c net.Conn) {
	defer s.wg.Done()
	defer s.untrack(c)
	log := s.log.With("remote", c.RemoteAddr().String())
	log.Debug("conn open")

	cl := &client{srv: s, id: s.connsTotal.Load()}
	// Every byte headed for the socket goes through durableConn, which waits
	// for the AOF first if this connection ran a write (ADR 015).
	w := resp.NewWriter(durableConn{conn: c, cl: cl})
	cl.w = w
	// Replies are flushed right before we'd block waiting for more input
	// (see flushBeforeRead), so a pipelined batch gets one write syscall.
	r := resp.NewReader(flushBeforeRead{conn: c, w: w})
	s.mu.Lock()
	s.conns[c] = cl
	s.mu.Unlock()

	for {
		args, err := r.ReadCommand()
		if err != nil {
			s.endConn(log, w, err)
			return
		}
		s.dispatch(cl, args)
		if cl.quit {
			w.Flush()
			log.Debug("conn quit")
			return
		}
		if cl.takeover != nil {
			// PSYNC: this connection is now a replication link. Send any
			// pending replies, then hand over the raw connection.
			if w.Flush() == nil {
				cl.takeover(c, r)
			}
			return
		}
	}
}

// endConn decides what, if anything, to tell the client as it goes away.
func (s *Server) endConn(log *slog.Logger, w *resp.Writer, err error) {
	var pe *resp.ProtocolError
	switch {
	case errors.As(err, &pe):
		// Same text Redis sends. After a framing error we can't find the
		// start of the next command, so the only safe move is to disconnect.
		w.WriteError("ERR Protocol error: " + pe.Msg)
		w.Flush()
		log.Debug("conn protocol error", "err", pe.Msg)
	case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
		log.Debug("conn closed")
	default:
		log.Debug("conn read error", "err", err)
	}
}

// flushBeforeRead sits between the bufio.Reader and the socket. bufio only
// calls Read on us when its buffer is empty, i.e. when we've handled every
// command the client has sent so far and are about to wait for more. That is
// exactly the moment to send the replies we've been holding (ADR 003).
//
// Doing it here, instead of checking Buffered() in the loop, also covers a
// partial command sitting in the buffer: we still flush before blocking on
// its remaining bytes.
// durableConn is the socket as the reply writer sees it. If the connection
// has run a write since its last reply went out, it first waits until the
// AOF covers that write (written to the kernel, or fsynced for "always").
// Putting the wait here, rather than at our explicit flush points, means no
// path can send an "OK" early: not even the reply buffer filling up and
// flushing itself in the middle of a big pipeline.
//
// If the AOF has failed, we return the error instead of sending the
// replies: the connection drops, and the client never hears "OK" for a
// write that may not be on disk.
type durableConn struct {
	conn net.Conn
	cl   *client
}

func (d durableConn) Write(p []byte) (int, error) {
	if d.cl.needDurable {
		d.cl.needDurable = false
		if aof := d.cl.srv.aof; aof != nil {
			if err := aof.WaitDurable(); err != nil {
				return 0, err
			}
		}
	}
	return d.conn.Write(p)
}

type flushBeforeRead struct {
	conn net.Conn
	w    *resp.Writer
}

func (f flushBeforeRead) Read(p []byte) (int, error) {
	if f.w.Buffered() > 0 {
		if err := f.w.Flush(); err != nil {
			return 0, err
		}
	}
	return f.conn.Read(p)
}
