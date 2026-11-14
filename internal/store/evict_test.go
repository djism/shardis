package store

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/djism/shardis/internal/keyslot"
)

// recount computes the memory estimate from scratch. After any sequence of
// operations it must equal the running total exactly; a drift means some
// code path forgot to account for a change.
func recount(tx *Tx) int64 {
	var n int64
	for s := range keyslot.Count {
		n += mapBytes(tx.ks.peak[s], entrySlotBytes) + mapBytes(tx.ks.vpeak[s], volatileSlotBytes)
		for k, e := range tx.ks.slots[s] {
			n += tx.ks.entrySize(k, e)
		}
	}
	return n
}

func TestMemoryAccountingNeverDrifts(t *testing.T) {
	for _, exact := range []bool{false, true} {
		t.Run(fmt.Sprintf("exact=%v", exact), func(t *testing.T) {
			clk := newFakeClock()
			e := newSharded(Options{Clock: clk.Now, ExactLRU: exact})
			x := NewActiveExpirer(e, 10)
			r := rand.New(rand.NewPCG(7, 8))
			for i := range 30000 {
				k := keyN(r.IntN(2000))
				e.Do([][]byte{k}, func(tx *Tx) {
					switch r.IntN(7) {
					case 0, 1:
						tx.Set(k, make([]byte, r.IntN(300)))
					case 2:
						tx.SetWithExpire(k, make([]byte, r.IntN(50)), clk.In(time.Duration(1+r.IntN(2000))*time.Millisecond))
					case 3:
						tx.SetExpire(k, clk.In(time.Duration(r.IntN(3000)-500)*time.Millisecond))
					case 4:
						tx.SetExpire(k, 0)
					case 5:
						tx.Delete(k)
					case 6:
						tx.Get(k)
					}
				})
				if i%500 == 0 {
					clk.Advance(300 * time.Millisecond)
					x.Cycle(time.Second) // expiry + compaction both change the total
				}
			}
			e.DoAll(func(tx *Tx) {
				if got, want := tx.ks.mem.used.Load(), recount(tx); got != want {
					t.Fatalf("running total %d, recount %d (drift %d)", got, want, got-want)
				}
				tx.Flush()
				if tx.ks.mem.used.Load() != 0 {
					t.Fatal("used memory not zero after FLUSHALL")
				}
			})
		})
	}
}

func newEvictTest(t *testing.T, p Policy, limit int64) (Engine, *Evictor, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	e, err := New(EngineSharded, Options{Clock: clk.Now, ExactLRU: p == AllKeysLRUExact})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	x, err := NewEvictor(e, limit, p, 5)
	if err != nil {
		t.Fatal(err)
	}
	return e, x, clk
}

// write does what the server does: Reserve, then (if allowed) the write.
func write(e Engine, x *Evictor, k []byte, v []byte, at int64) error {
	if err := x.Reserve(); err != nil {
		return err
	}
	e.Do([][]byte{k}, func(tx *Tx) { tx.SetWithExpire(k, v, at) })
	return nil
}

func TestNoEvictionRefusesWritesWhenFull(t *testing.T) {
	e, x, _ := newEvictTest(t, NoEviction, 200_000)
	var err error
	i := 0
	for ; err == nil && i < 100000; i++ {
		err = write(e, x, keyN(i), make([]byte, 100), 0)
	}
	if !errors.Is(err, ErrOOM) {
		t.Fatalf("expected ErrOOM, got %v after %d writes", err, i)
	}
	if e.Stats().EvictedKeys != 0 {
		t.Fatal("noeviction evicted keys")
	}
	// Reads still work, and deleting makes room again.
	k := keyN(0)
	e.Do([][]byte{k}, func(tx *Tx) {
		if _, ok := tx.Get(k); !ok {
			t.Error("existing key unreadable when full")
		}
	})
	for j := range i / 2 {
		k := keyN(j)
		e.Do([][]byte{k}, func(tx *Tx) { tx.Delete(k) })
	}
	if err := write(e, x, keyN(999999), make([]byte, 100), 0); err != nil {
		t.Fatalf("write after deleting half the keys: %v", err)
	}
}

// Limits in these tests sit well above the memory floor: 16,384 slot maps
// cost ~9 MB on their own once keys are spread over every slot (ADR 011).
const testLimit = 16 << 20

// overshoot is how far one write can push memory past the limit: the write
// that crosses it lands before the next Reserve, and if it makes a slot map
// double, that map growth (up to a few KB) lands with it.
const overshoot = 64 << 10

// Every evicting policy must keep memory at (about) the limit no matter how
// much is written.
func TestEvictingPoliciesStayUnderTheLimit(t *testing.T) {
	for _, p := range []Policy{AllKeysLRU, AllKeysRandom, VolatileTTL, AllKeysLRUExact} {
		t.Run(p.String(), func(t *testing.T) {
			e, x, clk := newEvictTest(t, p, testLimit)
			for i := range 30000 {
				at := clk.In(time.Duration(1+i) * time.Second) // all have TTLs, for volatile-ttl
				if err := write(e, x, keyN(i), make([]byte, 1000), at); err != nil {
					t.Fatalf("write %d: %v", i, err)
				}
				if used := e.Stats().UsedMemory; used > testLimit+overshoot {
					t.Fatalf("write %d: used %d > limit %d", i, used, testLimit)
				}
			}
			if e.Stats().EvictedKeys == 0 {
				t.Fatal("nothing was evicted")
			}
		})
	}
}

// volatile-ttl only evicts keys with a TTL, never persistent ones.
func TestVolatileTTLNeverEvictsPersistentKeys(t *testing.T) {
	e, x, clk := newEvictTest(t, VolatileTTL, testLimit)
	for i := range 500 {
		if err := write(e, x, keyN(i), make([]byte, 1000), 0); err != nil { // persistent
			t.Fatal(err)
		}
	}
	for i := 500; i < 30000; i++ {
		if err := write(e, x, keyN(i), make([]byte, 1000), clk.In(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if e.Stats().EvictedKeys == 0 {
		t.Fatal("expected evictions")
	}
	for i := range 500 {
		k := keyN(i)
		e.Do([][]byte{k}, func(tx *Tx) {
			if !tx.Exists(k) {
				t.Fatalf("persistent key %s was evicted", k)
			}
		})
	}
}

// With only persistent keys left, volatile-ttl has nothing it may evict and
// refuses writes, like Redis.
func TestVolatileTTLWithoutTTLKeysActsLikeNoEviction(t *testing.T) {
	e, x, _ := newEvictTest(t, VolatileTTL, testLimit)
	var err error
	for i := 0; err == nil && i < 100000; i++ {
		err = write(e, x, keyN(i), make([]byte, 1000), 0)
	}
	if !errors.Is(err, ErrOOM) || e.Stats().EvictedKeys != 0 {
		t.Fatalf("err=%v evicted=%d, want OOM and no evictions", err, e.Stats().EvictedKeys)
	}
}

// volatile-ttl picks the soonest-expiring keys. Load keys with increasing
// deadlines and check the survivors are mostly the latest ones.
func TestVolatileTTLPrefersSoonestDeadline(t *testing.T) {
	e, x, clk := newEvictTest(t, VolatileTTL, testLimit)
	const n = 30000
	for i := range n {
		write(e, x, keyN(i), make([]byte, 1000), clk.In(time.Duration(i+1)*time.Minute))
	}
	late, survivors := 0, 0
	e.DoAll(func(tx *Tx) {
		tx.ForEach(func(k string) {
			survivors++
			var i int
			fmt.Sscanf(k, "key:%d", &i)
			if i >= n/2 {
				late++
			}
		})
	})
	if frac := float64(late) / float64(survivors); frac < 0.8 {
		t.Fatalf("only %.0f%% of survivors are from the later half", 100*frac)
	}
}

// LRU must keep recently used keys. Touch a small "hot" set constantly while
// writing lots of cold keys; nearly all hot keys must survive.
func TestLRUKeepsHotKeys(t *testing.T) {
	for _, p := range []Policy{AllKeysLRU, AllKeysLRUExact, AllKeysRandom} {
		t.Run(p.String(), func(t *testing.T) {
			e, x, clk := newEvictTest(t, p, testLimit)
			hot := 200
			for i := range hot {
				write(e, x, keyN(i), make([]byte, 1000), 0)
			}
			for i := hot; i < 40000; i++ {
				clk.Advance(100 * time.Millisecond) // LRU clock has 1 s resolution
				for range 5 {                       // keep the hot set hot
					k := keyN(rand.IntN(hot))
					e.Do([][]byte{k}, func(tx *Tx) { tx.Get(k) })
				}
				write(e, x, keyN(i), make([]byte, 1000), 0)
			}
			alive := 0
			for i := range hot {
				k := keyN(i)
				e.Do([][]byte{k}, func(tx *Tx) {
					if tx.Exists(k) {
						alive++
					}
				})
			}
			t.Logf("%s: %d/%d hot keys survived", p, alive, hot)
			if p != AllKeysRandom && alive < hot*9/10 {
				t.Fatalf("LRU lost too many hot keys: %d/%d survived", alive, hot)
			}
		})
	}
}

func TestPolicyRules(t *testing.T) {
	e, x, _ := newEvictTest(t, AllKeysLRU, 0)
	if err := x.SetPolicy(AllKeysLRUExact); err == nil {
		t.Fatal("switching to exact LRU at runtime should fail")
	}
	if err := x.SetPolicy(AllKeysRandom); err != nil {
		t.Fatal(err)
	}
	if err := x.SetSamples(0); err == nil {
		t.Fatal("samples 0 accepted")
	}
	if _, err := ParsePolicy("ALLKEYS-LRU"); err != nil {
		t.Fatal(err)
	}
	if _, err := ParsePolicy("allkeys-lfu"); err == nil {
		t.Fatal("unsupported policy accepted")
	}
	if err := x.Reserve(); err != nil { // no limit
		t.Fatal(err)
	}
	_ = e
}

// Writers on many goroutines, all over the limit at once, evicting
// concurrently with reads, writes and the expirer. Under -race.
func TestConcurrentEviction(t *testing.T) {
	for _, p := range []Policy{AllKeysLRU, AllKeysLRUExact, AllKeysRandom} {
		t.Run(p.String(), func(t *testing.T) {
			e, x, clk := newEvictTest(t, p, testLimit)
			var wg sync.WaitGroup
			for w := range 8 {
				wg.Go(func() {
					r := rand.New(rand.NewPCG(uint64(w), 1))
					for range 4000 {
						k := keyN(r.IntN(40000))
						if r.IntN(3) == 0 {
							e.Do([][]byte{k}, func(tx *Tx) { tx.Get(k) })
						} else if err := write(e, x, k, make([]byte, 1000), 0); err != nil {
							t.Error(err)
							return
						}
					}
				})
			}
			wg.Go(func() {
				ex := NewActiveExpirer(e, 10)
				for range 50 {
					clk.Advance(100 * time.Millisecond)
					ex.Cycle(time.Millisecond)
				}
			})
			wg.Wait()
			// Concurrent writers can each land one write after their Reserve.
			if used := e.Stats().UsedMemory; used > testLimit+8*overshoot {
				t.Fatalf("used %d well over the limit", used)
			}
		})
	}
}

// BenchmarkGetParallel shows what exact LRU costs on the read path: every
// GET moves a node in one shared list behind one mutex, while approximate
// LRU writes a per-key timestamp at most once a second.
func BenchmarkGetParallel(b *testing.B) {
	keys := make([][]byte, 100_000)
	for i := range keys {
		keys[i] = keyN(i)
	}
	for _, exact := range []bool{false, true} {
		name := "approx-lru"
		if exact {
			name = "exact-lru"
		}
		b.Run(name, func(b *testing.B) {
			e := newSharded(Options{ExactLRU: exact})
			for _, k := range keys {
				e.Do([][]byte{k}, func(tx *Tx) { tx.Set(k, []byte("v")) })
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(rand.Uint64(), 0))
				for pb.Next() {
					k := keys[r.IntN(len(keys))]
					e.Do([][]byte{k}, func(tx *Tx) { tx.Get(k) })
				}
			})
		})
	}
}
