// Package persist writes the append-only file (AOF) and loads it back.
//
// The store records every change as an effect (store/effects.go) and calls
// AOF.Append while the command still holds its locks. A single writer
// goroutine moves appended bytes to the current file. The server waits
// (WaitDurable) before sending replies to write commands, so a client never
// hears "OK" for a write that isn't at least in the kernel (ADR 015):
//
//	command ──Append──► buffer ──writer goroutine──► write() ──fsync?──► disk
//	   │                                                │
//	   └── reply waits until write() (or fsync, for "always") covers it ──┘
//
// fsync policies:
//
//	always   fsync before replying. Group commit: one fsync covers every
//	         write waiting at that moment, so throughput doesn't fall to
//	         one write per fsync.
//	everysec fsync once a second, in the background. A machine crash loses
//	         up to ~1 s of acknowledged writes; a process crash loses none.
//	no       never fsync; the OS flushes when it likes (typically within
//	         ~30 s on Linux). A process crash still loses nothing.
package persist

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djism/shardis/internal/store"
)

// Fsync is the fsync policy.
type Fsync int32

const (
	FsyncAlways Fsync = iota
	FsyncEverySec
	FsyncNo
)

var fsyncNames = [...]string{"always", "everysec", "no"}

func (f Fsync) String() string { return fsyncNames[f] }

func ParseFsync(s string) (Fsync, error) {
	for i, n := range fsyncNames {
		if strings.EqualFold(s, n) {
			return Fsync(i), nil
		}
	}
	return 0, fmt.Errorf("invalid appendfsync %q (want always, everysec or no)", s)
}

// ErrRewriteInProgress is returned by Rewrite if one is already running.
var ErrRewriteInProgress = errors.New("Background append only file rewriting already in progress")

// Options configures an AOF.
type Options struct {
	Dir   string // data directory; files go in Dir/appendonlydir
	Fsync Fsync
	// Automatic rewrite: when the AOF has grown by RewritePercent since the
	// last rewrite and is at least RewriteMinSize bytes. 0 percent = off.
	RewritePercent int
	RewriteMinSize int64
	// RecordSynced is a fault-injection hook for the crash test (off by
	// default): after every fsync, write the number of bytes known to be on
	// disk to "<incr file>.synced". The crash test truncates the file to that
	// length to simulate a power cut, where unsynced bytes are lost.
	RecordSynced bool
	Log          *slog.Logger
}

// AOF is an open append-only file set. Use: Open, Load, Start, then Append
// (via the engine) and WaitDurable; Close on shutdown.
type AOF struct {
	opts  Options
	dir   string
	log   *slog.Logger
	fsync atomic.Int32

	// Appended bytes waiting for the writer. offsets are logical: total bytes
	// ever appended, across files, so they keep growing through rewrites.
	mu       sync.Mutex
	buf      []byte
	appended int64

	// Progress, for WaitDurable. Guarded by stateMu; cond is broadcast on change.
	stateMu sync.Mutex
	cond    *sync.Cond
	written int64 // logical offset handed to write()
	synced  int64 // logical offset known fsynced
	err     error // sticky: once a write fails, the AOF is broken

	// The current file. fileMu: the writer holds it exclusively to switch
	// files; write() and fsync hold it shared (they may run concurrently).
	fileMu     sync.RWMutex
	man        *manifest
	f          *os.File
	fileBytes  atomic.Int64 // bytes written to the current incr file
	incrBytes  atomic.Int64 // bytes in all incr files of the manifest
	baseBytes  atomic.Int64 // size of the base file
	lastSyncNs atomic.Int64 // wall time of the last successful fsync
	fsyncs     atomic.Int64 // fsync calls, to show group commit at work

	wake     chan struct{}
	switchCh chan chan error
	quit     chan struct{}
	done     sync.WaitGroup

	rewriting      atomic.Bool
	rewrites       atomic.Int64
	lastRewriteErr atomic.Value // string
	lastRewriteDur atomic.Int64 // ns
	loaded         LoadStats
	started        bool
	engine         store.Engine // for automatic rewrites
	closeOnce      sync.Once
	closeErr       error
}

// Open prepares the data directory and reads (or creates) the manifest. It
// doesn't load anything yet: call Load, then Start.
func Open(opts Options) (*AOF, error) {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	dir := filepath.Join(opts.Dir, subdir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	a := &AOF{
		opts: opts, dir: dir, log: opts.Log,
		wake: make(chan struct{}, 1), switchCh: make(chan chan error), quit: make(chan struct{}),
	}
	a.cond = sync.NewCond(&a.stateMu)
	a.fsync.Store(int32(opts.Fsync))
	a.lastRewriteErr.Store("")

	m, ok, err := loadManifest(dir)
	if err != nil {
		return nil, fmt.Errorf("aof: %w", err)
	}
	if !ok { // fresh directory: one empty incremental file
		m = &manifest{incrs: []aofFile{{name: incrName(1), seq: 1}}}
		if err := writeManifest(dir, m); err != nil {
			return nil, fmt.Errorf("aof: creating manifest: %w", err)
		}
	}
	a.man = m
	return a, nil
}

func (a *AOF) Fsync() Fsync      { return Fsync(a.fsync.Load()) }
func (a *AOF) SetFsync(f Fsync)  { a.fsync.Store(int32(f)) }
func (a *AOF) Dir() string       { return a.opts.Dir }
func (a *AOF) Loaded() LoadStats { return a.loaded }

// Append queues effect bytes. Called by the engine with locks held, so it
// only copies and pokes the writer; it never does I/O itself.
func (a *AOF) Append(p []byte) {
	a.mu.Lock()
	a.buf = append(a.buf, p...)
	a.appended += int64(len(p))
	a.mu.Unlock()
	select {
	case a.wake <- struct{}{}:
	default: // writer already has a wake-up pending
	}
}

// WaitDurable blocks until everything appended so far is written (or fsynced,
// for "always"). The server calls it before sending replies to writes.
func (a *AOF) WaitDurable() error {
	a.mu.Lock()
	target := a.appended
	a.mu.Unlock()
	always := a.Fsync() == FsyncAlways
	a.stateMu.Lock()
	defer a.stateMu.Unlock()
	for a.err == nil {
		done := a.written
		if always {
			done = a.synced
		}
		if done >= target {
			return nil
		}
		a.cond.Wait()
	}
	return a.err
}

// Err is the sticky write error, if any. Writes are refused once it's set.
func (a *AOF) Err() error {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()
	return a.err
}

// Start opens the current incremental file for appending, starts the
// background goroutines, and makes e record its effects to this AOF. Call it
// after Load.
func (a *AOF) Start(e store.Engine) error {
	f, err := os.OpenFile(filepath.Join(a.dir, a.man.current().name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	a.f = f
	a.fileBytes.Store(st.Size())
	a.recordSizes()
	a.started = true
	a.engine = e
	a.done.Add(3)
	go a.writer()
	go a.syncer()
	go a.autoRewrite()
	e.SetLog(a)
	return nil
}

// recordSizes recomputes the base/incr totals from the files on disk.
func (a *AOF) recordSizes() {
	var incr int64
	for _, f := range a.man.incrs {
		if st, err := os.Stat(filepath.Join(a.dir, f.name)); err == nil {
			incr += st.Size()
		}
	}
	a.incrBytes.Store(incr)
	a.baseBytes.Store(0)
	if a.man.base != nil {
		if st, err := os.Stat(filepath.Join(a.dir, a.man.base.name)); err == nil {
			a.baseBytes.Store(st.Size())
		}
	}
}

// writer is the only goroutine that writes to the file. Each loop it takes
// everything appended so far and writes it in one write() call. For
// "always" it then fsyncs: every client whose write was in that batch is
// covered by that single fsync (group commit).
func (a *AOF) writer() {
	defer a.done.Done()
	var spare []byte
	for {
		select {
		case <-a.wake:
		case resp := <-a.switchCh:
			spare = a.writeBatch(spare)
			resp <- a.switchFile()
			continue
		case <-a.quit:
			a.writeBatch(spare)
			a.syncNow()
			return
		}
		spare = a.writeBatch(spare)
		if a.Fsync() == FsyncAlways {
			a.syncNow()
		}
	}
}

// writeBatch writes all pending bytes and returns a buffer to reuse.
func (a *AOF) writeBatch(spare []byte) []byte {
	a.mu.Lock()
	data, target := a.buf, a.appended
	a.buf = spare[:0]
	a.mu.Unlock()
	if len(data) == 0 {
		return data
	}
	a.fileMu.RLock()
	_, err := a.f.Write(data)
	a.fileMu.RUnlock()
	if err != nil {
		a.fail(fmt.Errorf("writing AOF: %w", err))
		return data
	}
	a.fileBytes.Add(int64(len(data)))
	a.incrBytes.Add(int64(len(data)))
	a.stateMu.Lock()
	a.written = target
	a.cond.Broadcast()
	a.stateMu.Unlock()
	return data
}

// syncNow fsyncs the current file and records how far that covers.
func (a *AOF) syncNow() {
	a.stateMu.Lock()
	target, already := a.written, a.synced
	a.stateMu.Unlock()
	if target <= already {
		return
	}
	a.fileMu.RLock()
	fileLen := a.fileBytes.Load()
	err := a.f.Sync()
	name := a.man.current().name
	a.fileMu.RUnlock()
	if err != nil {
		a.fail(fmt.Errorf("fsync AOF: %w", err))
		return
	}
	a.lastSyncNs.Store(time.Now().UnixNano())
	a.fsyncs.Add(1)
	if a.opts.RecordSynced {
		// Fault-injection only: no fsync on this file on purpose. A process
		// crash keeps it (the kernel has it), and that's all the test needs.
		os.WriteFile(filepath.Join(a.dir, name+".synced"), []byte(fmt.Sprint(fileLen)), 0o644)
	}
	a.stateMu.Lock()
	a.synced = max(a.synced, target)
	a.cond.Broadcast()
	a.stateMu.Unlock()
}

// syncer fsyncs once a second for "everysec". It runs separately from the
// writer so a slow fsync never holds up write() calls (and thus replies).
func (a *AOF) syncer() {
	defer a.done.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-a.quit:
			return
		case <-t.C:
			if a.Fsync() == FsyncEverySec {
				a.syncNow()
			}
		}
	}
}

func (a *AOF) fail(err error) {
	a.log.Error("AOF failure: refusing writes from now on", "err", err)
	a.stateMu.Lock()
	if a.err == nil {
		a.err = err
	}
	a.cond.Broadcast()
	a.stateMu.Unlock()
}

// switchFile starts a new incremental file (for a rewrite). Runs on the
// writer goroutine after it has written everything pending, so every byte
// appended before this point is in the old file and every byte after it
// goes to the new one. The manifest lists the new file *before* anything is
// written to it; otherwise a crash could lose writes that landed in a file
// the manifest doesn't know about.
func (a *AOF) switchFile() error {
	a.syncNow() // the old file is complete and durable
	seq := a.man.maxSeq() + 1
	name := incrName(seq)
	f, err := os.OpenFile(filepath.Join(a.dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	next := &manifest{base: a.man.base, incrs: append(append([]aofFile(nil), a.man.incrs...), aofFile{name: name, seq: seq})}
	if err := writeManifest(a.dir, next); err != nil {
		f.Close()
		os.Remove(filepath.Join(a.dir, name))
		return err
	}
	a.fileMu.Lock()
	old := a.f
	a.f, a.man = f, next
	a.fileBytes.Store(0)
	a.fileMu.Unlock()
	return old.Close()
}

// Close writes and fsyncs everything pending and stops the goroutines. A
// clean shutdown (SIGTERM) therefore loses nothing, whatever the policy.
// Safe to call more than once.
func (a *AOF) Close() error {
	if !a.started {
		return nil
	}
	a.closeOnce.Do(func() { a.closeErr = a.close() })
	return a.closeErr
}

func (a *AOF) close() error {
	a.engine.SetLog(nil) // no new effects; everything appended so far gets written below
	close(a.quit)
	a.done.Wait()
	for a.rewriting.Load() { // let a running rewrite finish its file juggling
		time.Sleep(10 * time.Millisecond)
	}
	a.fileMu.Lock()
	defer a.fileMu.Unlock()
	return errors.Join(a.Err(), a.f.Close())
}

// Info is what INFO persistence reports.
type Info struct {
	Fsync            Fsync
	CurrentSize      int64 // base + incr
	BaseSize         int64
	PendingBytes     int64 // appended but not yet written
	LastFsyncAgo     time.Duration
	RewriteRunning   bool
	Rewrites         int64
	LastRewrite      time.Duration
	LastRewriteError string
	WriteError       string
	Fsyncs           int64
	Loaded           LoadStats
}

func (a *AOF) Info() Info {
	a.mu.Lock()
	appended := a.appended
	a.mu.Unlock()
	a.stateMu.Lock()
	written, err := a.written, a.err
	a.stateMu.Unlock()
	in := Info{
		Fsync:            a.Fsync(),
		BaseSize:         a.baseBytes.Load(),
		CurrentSize:      a.baseBytes.Load() + a.incrBytes.Load(),
		PendingBytes:     appended - written,
		RewriteRunning:   a.rewriting.Load(),
		Rewrites:         a.rewrites.Load(),
		LastRewrite:      time.Duration(a.lastRewriteDur.Load()),
		LastRewriteError: a.lastRewriteErr.Load().(string),
		Loaded:           a.loaded,
		Fsyncs:           a.fsyncs.Load(),
	}
	if ns := a.lastSyncNs.Load(); ns > 0 {
		in.LastFsyncAgo = time.Since(time.Unix(0, ns))
	}
	if err != nil {
		in.WriteError = err.Error()
	}
	return in
}
