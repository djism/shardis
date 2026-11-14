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

	"github.com/djism/shardis/internal/resp"
	"github.com/djism/shardis/internal/store"
)

type Server struct {
	log      *slog.Logger
	engine   store.Engine
	commands map[string]command

	ln net.Listener

	mu      sync.Mutex
	conns   map[net.Conn]struct{} // open connections, so shutdown can close them
	closing bool

	wg sync.WaitGroup // one per connection goroutine
}

// New creates a server that runs commands against engine. The caller owns
// the engine and closes it after Serve returns.
func New(log *slog.Logger, engine store.Engine) *Server {
	s := &Server{log: log, engine: engine, conns: make(map[net.Conn]struct{})}
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
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	c.Close()
}

// client is per-connection state that command handlers can see.
type client struct {
	srv  *Server
	w    *resp.Writer
	quit bool // set by QUIT: flush the reply, then hang up
}

func (s *Server) handle(c net.Conn) {
	defer s.wg.Done()
	defer s.untrack(c)
	log := s.log.With("remote", c.RemoteAddr().String())
	log.Debug("conn open")

	w := resp.NewWriter(c)
	// Replies are flushed right before we'd block waiting for more input
	// (see flushBeforeRead), so a pipelined batch gets one write syscall.
	r := resp.NewReader(flushBeforeRead{conn: c, w: w})
	cl := &client{srv: s, w: w}

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
