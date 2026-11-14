package persist

import (
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djism/shardis/internal/store"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type clock struct{ ms atomic.Int64 }

func newClock() *clock                    { c := &clock{}; c.ms.Store(1_700_000_000_000); return c }
func (c *clock) Now() int64               { return c.ms.Load() }
func (c *clock) Advance(d time.Duration)  { c.ms.Add(d.Milliseconds()) }
func (c *clock) In(d time.Duration) int64 { return c.Now() + d.Milliseconds() }

// open creates an engine and an AOF in dir, loads it, and starts it.
func open(t *testing.T, dir string, fsync Fsync, clk *clock) (store.Engine, *AOF) {
	t.Helper()
	e, err := store.New(store.EngineSharded, store.Options{Clock: clk.Now})
	if err != nil {
		t.Fatal(err)
	}
	a, err := Open(Options{Dir: dir, Fsync: fsync, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Load(e); err != nil {
		t.Fatal(err)
	}
	if err := a.Start(e); err != nil {
		t.Fatal(err)
	}
	return e, a
}

type rec struct {
	val string
	at  int64
}

// state returns every live key with its value and deadline.
func state(e store.Engine) map[string]rec {
	out := map[string]rec{}
	e.DoAll(func(tx *store.Tx) {
		var keys []string
		tx.ForEach(func(k string) { keys = append(keys, k) })
		for _, k := range keys {
			v, _ := tx.Get([]byte(k))
			at, _ := tx.ExpireAt([]byte(k))
			out[k] = rec{string(v), at}
		}
	})
	return out
}

func set(e store.Engine, k, v string, at int64) {
	e.Do([][]byte{[]byte(k)}, func(tx *store.Tx) { tx.SetWithExpire([]byte(k), []byte(v), at) })
}

func TestRestartRestoresState(t *testing.T) {
	for _, f := range []Fsync{FsyncAlways, FsyncEverySec, FsyncNo} {
		t.Run(f.String(), func(t *testing.T) {
			dir, clk := t.TempDir(), newClock()
			e, a := open(t, dir, f, clk)
			set(e, "a", "1", 0)
			set(e, "b", "2", clk.In(time.Hour))
			set(e, "a", "3", 0) // overwrite
			set(e, "gone", "x", 0)
			e.Do([][]byte{[]byte("gone")}, func(tx *store.Tx) { tx.Delete([]byte("gone")) })
			e.Do([][]byte{[]byte("a")}, func(tx *store.Tx) { tx.SetExpire([]byte("a"), clk.In(time.Minute)) })
			e.Do([][]byte{[]byte("b")}, func(tx *store.Tx) { tx.SetExpire([]byte("b"), 0) }) // persist
			want := state(e)
			if err := a.WaitDurable(); err != nil {
				t.Fatal(err)
			}
			a.Close()
			e.Close()

			e2, a2 := open(t, dir, f, clk)
			defer func() { a2.Close(); e2.Close() }()
			if got := state(e2); !maps.Equal(got, want) {
				t.Fatalf("after restart:\n got %v\nwant %v", got, want)
			}
		})
	}
}

// Deadlines are stored as absolute times, so a key that expired while the
// server was down doesn't come back.
func TestKeysExpiredWhileDownStayGone(t *testing.T) {
	dir, clk := t.TempDir(), newClock()
	e, a := open(t, dir, FsyncEverySec, clk)
	set(e, "short", "x", clk.In(time.Second))
	set(e, "long", "x", clk.In(time.Hour))
	a.Close()
	e.Close()
	clk.Advance(time.Minute) // "downtime"
	e2, a2 := open(t, dir, FsyncEverySec, clk)
	defer func() { a2.Close(); e2.Close() }()
	got := state(e2)
	if _, ok := got["short"]; ok || len(got) != 1 {
		t.Fatalf("got %v, want only 'long'", got)
	}
}

func TestFlushAllIsReplayed(t *testing.T) {
	dir, clk := t.TempDir(), newClock()
	e, a := open(t, dir, FsyncNo, clk)
	set(e, "before", "x", 0)
	e.DoAll(func(tx *store.Tx) { tx.Flush() })
	set(e, "after", "y", 0)
	a.Close()
	e.Close()
	e2, a2 := open(t, dir, FsyncNo, clk)
	defer func() { a2.Close(); e2.Close() }()
	if got := state(e2); len(got) != 1 || got["after"].val != "y" {
		t.Fatalf("got %v", got)
	}
}

// A crash can tear the last record in half. It was never acknowledged, so
// loading drops it (and truncates the file) instead of refusing to start.
func TestTornLastRecordIsTruncated(t *testing.T) {
	dir, clk := t.TempDir(), newClock()
	e, a := open(t, dir, FsyncAlways, clk)
	set(e, "a", "1", 0)
	a.Close()
	e.Close()
	path := filepath.Join(dir, subdir, incrName(1))
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString("*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$5\r\nhal") // torn mid-value
	f.Close()

	e2, a2 := open(t, dir, FsyncAlways, clk)
	defer func() { a2.Close(); e2.Close() }()
	if got := state(e2); len(got) != 1 || got["a"].val != "1" {
		t.Fatalf("got %v", got)
	}
	if a2.Loaded().Truncated == 0 {
		t.Fatal("expected a truncation to be reported")
	}
	// Appending after the truncation must produce a valid file again.
	set(e2, "c", "3", 0)
	a2.WaitDurable()
	a2.Close()
	e2.Close()
	e3, a3 := open(t, dir, FsyncAlways, clk)
	defer func() { a3.Close(); e3.Close() }()
	if got := state(e3); len(got) != 2 {
		t.Fatalf("after truncate + more writes: %v", got)
	}
}

// Garbage in the middle of the log is real corruption: refuse to start.
func TestCorruptionInTheMiddleRefusesToLoad(t *testing.T) {
	dir, clk := t.TempDir(), newClock()
	e, a := open(t, dir, FsyncAlways, clk)
	set(e, "a", "1", 0)
	set(e, "b", "2", 0)
	a.Close()
	e.Close()
	path := filepath.Join(dir, subdir, incrName(1))
	data, _ := os.ReadFile(path)
	data[2] = 'X' // break the first record's header
	os.WriteFile(path, data, 0o644)

	e2, _ := store.New(store.EngineSharded, store.Options{Clock: clk.Now})
	a2, err := Open(Options{Dir: dir, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a2.Load(e2); err == nil {
		t.Fatal("expected Load to refuse a corrupt file")
	}
}

// With "always", WaitDurable returns only after an fsync covers the write.
// Many concurrent writers should share fsyncs (group commit).
func TestGroupCommit(t *testing.T) {
	dir, clk := t.TempDir(), newClock()
	e, a := open(t, dir, FsyncAlways, clk)
	defer func() { a.Close(); e.Close() }()
	const writers, each = 32, 50
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range each {
				set(e, fmt.Sprintf("w%d:%d", w, i), "v", 0)
				if err := a.WaitDurable(); err != nil {
					t.Error(err)
					return
				}
				a.stateMu.Lock()
				ok := a.synced >= a.written || a.written == 0
				a.stateMu.Unlock()
				_ = ok
			}
		})
	}
	wg.Wait()
	a.stateMu.Lock()
	synced, written := a.synced, a.written
	a.stateMu.Unlock()
	if synced < written {
		t.Fatalf("always: synced %d < written %d after all waits", synced, written)
	}
	n := a.Info().Fsyncs
	t.Logf("%d acknowledged writes, %d fsyncs (%.1f writes per fsync)", writers*each, n, float64(writers*each)/float64(n))
	if n >= writers*each {
		t.Fatalf("no group commit: %d fsyncs for %d writes", n, writers*each)
	}
}

func TestRewriteCompactsAndPreservesState(t *testing.T) {
	dir, clk := t.TempDir(), newClock()
	e, a := open(t, dir, FsyncEverySec, clk)
	for round := range 50 { // 50 overwrites of the same 200 keys
		for k := range 200 {
			set(e, fmt.Sprint("k", k), fmt.Sprint(round), 0)
		}
	}
	a.WaitDurable()
	before := a.Info().CurrentSize
	if err := a.Rewrite(e); err != nil {
		t.Fatal(err)
	}
	waitRewrite(t, a)
	after := a.Info().CurrentSize
	t.Logf("AOF %d bytes before rewrite, %d after", before, after)
	if after*10 > before {
		t.Fatalf("rewrite didn't compact: %d → %d bytes", before, after)
	}
	set(e, "post", "rewrite", 0) // goes to the new incr file
	want := state(e)
	a.Close()
	e.Close()
	entries, _ := os.ReadDir(filepath.Join(dir, subdir))
	if len(entries) != 3 { // manifest, base, incr
		t.Errorf("old files not cleaned up: %v", entries)
	}
	e2, a2 := open(t, dir, FsyncEverySec, clk)
	defer func() { a2.Close(); e2.Close() }()
	if got := state(e2); !maps.Equal(got, want) {
		t.Fatal("state differs after rewrite + restart")
	}
}

func waitRewrite(t *testing.T, a *AOF) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for a.Info().RewriteRunning {
		if time.Now().After(deadline) {
			t.Fatal("rewrite never finished")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if msg := a.Info().LastRewriteError; msg != "" {
		t.Fatal(msg)
	}
}

// The test behind ADR 016: writers keep changing keys (overwrites, TTLs,
// deletes) *while* the fuzzy snapshot is being taken. After a restart,
// base + incr must reproduce the exact final state.
func TestFuzzyRewriteUnderConcurrentWrites(t *testing.T) {
	for round := range 5 {
		t.Run(fmt.Sprint("round", round), func(t *testing.T) {
			dir, clk := t.TempDir(), newClock()
			e, a := open(t, dir, FsyncNo, clk)
			for k := range 2000 {
				set(e, fmt.Sprint("k", k), "init", 0)
			}
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for w := range 6 {
				wg.Go(func() {
					r := rand.New(rand.NewPCG(uint64(round), uint64(w)))
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						k := []byte(fmt.Sprint("k", r.IntN(2500)))
						e.Do([][]byte{k}, func(tx *store.Tx) {
							switch r.IntN(5) {
							case 0, 1:
								tx.Set(k, []byte(strconv.Itoa(i)))
							case 2:
								tx.SetWithExpire(k, []byte("ttl"), clk.In(time.Hour))
							case 3:
								tx.Delete(k)
							case 4:
								tx.SetExpire(k, 0)
							}
						})
					}
				})
			}
			time.Sleep(20 * time.Millisecond)
			if err := a.Rewrite(e); err != nil {
				t.Fatal(err)
			}
			waitRewrite(t, a)
			time.Sleep(20 * time.Millisecond)
			close(stop)
			wg.Wait()
			want := state(e)
			a.Close()
			e.Close()

			e2, a2 := open(t, dir, FsyncNo, clk)
			defer func() { a2.Close(); e2.Close() }()
			got := state(e2)
			if !maps.Equal(got, want) {
				diff := 0
				for k, v := range want {
					if got[k] != v {
						diff++
					}
				}
				t.Fatalf("%d of %d keys differ after fuzzy rewrite + restart", diff, len(want))
			}
		})
	}
}

// A crash in the middle of a rewrite (new incr file opened, base not yet
// written) must lose nothing: the manifest then lists the old base, the old
// incr and the new incr, and replaying all three gives the full state.
func TestCrashMidRewriteLosesNothing(t *testing.T) {
	dir, clk := t.TempDir(), newClock()
	e, a := open(t, dir, FsyncAlways, clk)
	set(e, "old", "1", 0)
	frozen, release := make(chan struct{}), make(chan struct{})
	testHookAfterSwitch = func() { close(frozen); <-release }
	defer func() { testHookAfterSwitch = nil }()
	a.Rewrite(e)
	<-frozen
	set(e, "new", "2", 0) // lands in the new incr file
	a.WaitDurable()

	// "Crash" now: copy the directory exactly as it is mid-rewrite.
	crashDir := t.TempDir()
	copyDir(t, filepath.Join(dir, subdir), filepath.Join(crashDir, subdir))
	close(release)
	waitRewrite(t, a)
	a.Close()
	e.Close()

	e2, a2 := open(t, crashDir, FsyncAlways, clk)
	defer func() { a2.Close(); e2.Close() }()
	if got := state(e2); len(got) != 2 || got["old"].val != "1" || got["new"].val != "2" {
		t.Fatalf("state after crash mid-rewrite: %v", got)
	}
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	os.MkdirAll(dst, 0o755)
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, en := range entries {
		data, err := os.ReadFile(filepath.Join(src, en.Name()))
		if err != nil {
			t.Fatal(err)
		}
		os.WriteFile(filepath.Join(dst, en.Name()), data, 0o644)
	}
}

func TestManifestRoundTrip(t *testing.T) {
	m := &manifest{base: &aofFile{name: baseName(3), seq: 3, base: true}, incrs: []aofFile{{incrName(3), 3, false}, {incrName(4), 4, false}}}
	got, err := parseManifest(m.encode())
	if err != nil || got.maxSeq() != 4 || got.current().name != incrName(4) || got.base.name != baseName(3) {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	for _, bad := range []string{"", "file x seq 1 type i\nfile y seq 2 type b\n", "file a/b seq 1 type i\n", "junk\n"} {
		if _, err := parseManifest([]byte(bad)); err == nil {
			t.Errorf("accepted bad manifest %q", bad)
		}
	}
}
