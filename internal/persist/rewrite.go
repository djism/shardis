package persist

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/djism/shardis/internal/store"
)

// Rewrite starts a background AOF rewrite (BGREWRITEAOF): write a fresh base
// file holding just the current keys, so the log stops growing forever.
//
// Redis forks and lets the child dump a frozen copy of memory. Go can't fork
// safely, so we take a *fuzzy* snapshot instead (ADR 016):
//
//  1. Switch to a new incremental file. From here on, every change lands in it.
//  2. Dump the keyspace one slot group at a time, holding only that group's
//     lock, into a new base file. Writes to other groups carry on.
//  3. Atomically switch the manifest to [new base, new incr], then delete
//     the old files.
//
// The base isn't a picture of a single instant: group 7 may be dumped a few
// ms after group 3. That's fine, because every change after step 1 is also
// in the new incr file, and effects are absolute ("x is now 6"), so
// replaying one that's already reflected in the base lands on the same
// state. For each key the last record wins, and that's the true latest state.
func (a *AOF) Rewrite(e store.Engine) error {
	if !a.started {
		return errors.New("AOF not started")
	}
	if !a.rewriting.CompareAndSwap(false, true) {
		return ErrRewriteInProgress
	}
	go func() {
		defer a.rewriting.Store(false)
		start := time.Now()
		err := a.rewrite(e)
		a.lastRewriteDur.Store(int64(time.Since(start)))
		if err != nil {
			a.lastRewriteErr.Store(err.Error())
			a.log.Error("AOF rewrite failed; keeping the old files", "err", err)
			return
		}
		a.lastRewriteErr.Store("")
		a.rewrites.Add(1)
		a.log.Info("AOF rewrite done", "took", time.Since(start), "base_bytes", a.baseBytes.Load())
	}()
	return nil
}

func (a *AOF) rewrite(e store.Engine) error {
	// 1. New incremental file (the writer does the switch, between batches).
	resp := make(chan error, 1)
	select {
	case a.switchCh <- resp:
	case <-a.quit:
		return errors.New("shutting down")
	}
	if err := <-resp; err != nil {
		return err
	}
	a.fileMu.RLock()
	old := a.man
	cur := a.man.current()
	a.fileMu.RUnlock()
	if testHookAfterSwitch != nil {
		testHookAfterSwitch()
	}

	// 2. Fuzzy snapshot into a temp base file.
	seq := cur.seq // base and the incr that follows it share a number
	name := baseName(seq)
	tmp := filepath.Join(a.dir, name+".tmp")
	if err := a.dumpBase(e, tmp); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filepath.Join(a.dir, name)); err != nil {
		return err
	}

	// 3. Point the manifest at [new base, current incr], then clean up.
	a.fileMu.Lock()
	next := &manifest{base: &aofFile{name: name, seq: seq, base: true}, incrs: []aofFile{a.man.current()}}
	err := writeManifest(a.dir, next)
	if err == nil {
		a.man = next
	}
	a.fileMu.Unlock()
	if err != nil {
		return err
	}
	for _, f := range old.files() {
		if f.name != cur.name {
			os.Remove(filepath.Join(a.dir, f.name))
			os.Remove(filepath.Join(a.dir, f.name+".synced"))
		}
	}
	a.recordSizes()
	return syncDir(a.dir)
}

// testHookAfterSwitch lets a test freeze a rewrite right after step 1, to
// check that a crash at that moment loses nothing.
var testHookAfterSwitch func()

func (a *AOF) dumpBase(e store.Engine, path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var buf []byte
	for g := range store.NumGroups {
		e.DoGroup(g, func(tx *store.Tx, slots []uint16) {
			buf = tx.AppendSnapshot(buf[:0], slots) // copy out under the lock...
		})
		if _, err := w.Write(buf); err != nil { // ...write after releasing it
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// autoRewrite checks once a second whether the AOF has grown enough to be
// worth compacting (Redis's auto-aof-rewrite-percentage / -min-size).
func (a *AOF) autoRewrite() {
	defer a.done.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-a.quit:
			return
		case <-t.C:
		}
		if a.opts.RewritePercent <= 0 || a.rewriting.Load() || a.engine == nil {
			continue
		}
		base, total := a.baseBytes.Load(), a.baseBytes.Load()+a.incrBytes.Load()
		if total >= a.opts.RewriteMinSize && total > base+base*int64(a.opts.RewritePercent)/100 {
			a.log.Info("AOF grew enough; starting automatic rewrite", "bytes", total, "base", base)
			a.Rewrite(a.engine)
		}
	}
}
