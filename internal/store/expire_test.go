package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djism/shardis/internal/events"
)

// fakeClock lets tests move time forward instantly and deterministically.
type fakeClock struct{ ms atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ms.Store(1_700_000_000_000) // a realistic Unix-ms timestamp
	return c
}
func (c *fakeClock) Now() int64               { return c.ms.Load() }
func (c *fakeClock) Advance(d time.Duration)  { c.ms.Add(d.Milliseconds()) }
func (c *fakeClock) In(d time.Duration) int64 { return c.Now() + d.Milliseconds() }

func forEachEngineWithClock(t *testing.T, test func(t *testing.T, e Engine, clk *fakeClock)) {
	for _, kind := range Engines {
		t.Run(kind, func(t *testing.T) {
			clk := newFakeClock()
			e, err := New(kind, Options{Clock: clk.Now})
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			test(t, e, clk)
		})
	}
}

func keyN(i int) []byte { return fmt.Appendf(nil, "key:%d", i) }

func dbsize(e Engine) (n int) {
	e.DoAll(func(tx *Tx) { n = tx.Len() })
	return n
}

func TestLazyExpiry(t *testing.T) {
	forEachEngineWithClock(t, func(t *testing.T, e Engine, clk *fakeClock) {
		k := b("k")
		e.Do([][]byte{k}, func(tx *Tx) { tx.SetWithExpire(k, b("v"), clk.In(time.Second)) })

		clk.Advance(999 * time.Millisecond)
		e.Do([][]byte{k}, func(tx *Tx) {
			if _, ok := tx.Get(k); !ok {
				t.Error("key gone 1ms before its deadline")
			}
		})
		clk.Advance(time.Millisecond) // exactly at the deadline: expired
		if dbsize(e) != 1 {
			t.Fatal("key should still be stored until something touches it")
		}
		e.Do([][]byte{k}, func(tx *Tx) {
			if _, ok := tx.Get(k); ok {
				t.Error("key alive at its deadline")
			}
		})
		if dbsize(e) != 0 {
			t.Fatal("lazy expiry didn't delete the key")
		}
	})
}

func TestTTLRules(t *testing.T) {
	forEachEngineWithClock(t, func(t *testing.T, e Engine, clk *fakeClock) {
		k, missing := b("k"), b("missing")
		at := clk.In(time.Minute)
		e.Do([][]byte{k, missing}, func(tx *Tx) {
			tx.SetWithExpire(k, b("1"), at)
			tx.SetKeepTTL(k, b("2")) // INCR-style update keeps the TTL
			if got, _ := tx.ExpireAt(k); got != at {
				t.Errorf("SetKeepTTL lost the TTL: %d", got)
			}
			tx.Set(k, b("3")) // plain SET removes it
			if got, _ := tx.ExpireAt(k); got != 0 {
				t.Errorf("Set kept the TTL: %d", got)
			}
			if !tx.SetExpire(k, at) {
				t.Error("SetExpire on existing key returned false")
			}
			if !tx.SetExpire(k, 0) { // PERSIST
				t.Error("persist failed")
			}
			if tx.SetExpire(missing, at) {
				t.Error("SetExpire on missing key returned true")
			}
			if !tx.SetExpire(k, clk.Now()) || tx.Exists(k) {
				t.Error("deadline in the past should delete the key")
			}
		})
	})
}

// The TTL index must always hold exactly the keys that have a TTL. If it
// drifts, active expiry either misses keys (memory leak) or wastes time.
func TestVolatileIndexStaysConsistent(t *testing.T) {
	forEachEngineWithClock(t, func(t *testing.T, e Engine, clk *fakeClock) {
		r := rand.New(rand.NewPCG(1, 2))
		for range 20000 {
			k := keyN(r.IntN(500))
			e.Do([][]byte{k}, func(tx *Tx) {
				switch r.IntN(6) {
				case 0:
					tx.Set(k, b("v"))
				case 1:
					tx.SetWithExpire(k, b("v"), clk.In(time.Duration(1+r.IntN(5000))*time.Millisecond))
				case 2:
					tx.SetKeepTTL(k, b("v"))
				case 3:
					tx.SetExpire(k, clk.In(time.Duration(r.IntN(3000)-500)*time.Millisecond))
				case 4:
					tx.Delete(k)
				case 5:
					tx.Get(k)
				}
			})
			if r.IntN(50) == 0 {
				clk.Advance(100 * time.Millisecond)
			}
		}
		e.DoAll(func(tx *Tx) {
			for s := range tx.ks.slots {
				for k, en := range tx.ks.slots[s] {
					_, indexed := tx.ks.volatile[s][k]
					if indexed != (en.expireAt != 0) {
						t.Fatalf("key %s: expireAt=%d but indexed=%v", k, en.expireAt, indexed)
					}
				}
				for k := range tx.ks.volatile[s] {
					if _, ok := tx.ks.slots[s][k]; !ok {
						t.Fatalf("index has %s, which isn't stored", k)
					}
				}
				if tx.ks.volatile[s] != nil && len(tx.ks.volatile[s]) == 0 {
					t.Fatalf("slot %d has an empty (not released) index map", s)
				}
			}
		})
	})
}

func load(e Engine, from, n int, at int64) {
	for i := from; i < from+n; i++ {
		k := keyN(i)
		e.Do([][]byte{k}, func(tx *Tx) { tx.SetWithExpire(k, b("v"), at) })
	}
}

// The heart of active expiry: keys nobody reads still get deleted, and keys
// without a TTL are never touched.
func TestActiveExpiryCleansUnreadKeys(t *testing.T) {
	forEachEngineWithClock(t, func(t *testing.T, e Engine, clk *fakeClock) {
		load(e, 0, 50000, clk.In(time.Second)) // will expire
		load(e, 50000, 50000, 0)               // never expire
		clk.Advance(2 * time.Second)

		x := NewActiveExpirer(e, 10)
		cycles := 0
		for dbsize(e) > 50000 {
			if cycles++; cycles > 100 {
				t.Fatalf("still %d keys after 100 cycles", dbsize(e))
			}
			x.Cycle(time.Second)
		}
		if n := dbsize(e); n != 50000 {
			t.Fatalf("dbsize = %d, want 50000 (persistent keys must survive)", n)
		}
		t.Logf("50k expired keys cleaned in %d cycle(s)", cycles)
	})
}

// When nothing is expiring, a cycle should look at each slot's TTL keys once
// (one sample of up to 20) and move on: the 25% rule says "clean enough".
func TestActiveExpiryIsCheapWhenNothingExpires(t *testing.T) {
	forEachEngineWithClock(t, func(t *testing.T, e Engine, clk *fakeClock) {
		load(e, 0, 50000, clk.In(time.Hour))
		want := 0
		e.DoAll(func(tx *Tx) {
			for _, vm := range tx.ks.volatile {
				want += min(len(vm), expireSampleSize)
			}
		})
		st := NewActiveExpirer(e, 10).Cycle(time.Second)
		if st.Expired != 0 || st.Sampled != want || st.Groups != NumGroups {
			t.Fatalf("stats %+v, want sampled=%d groups=%d expired=0", st, want, NumGroups)
		}
	})
}

// A mass expiry must not turn into one long pause: each cycle stops at its budget.
func TestActiveExpiryRespectsBudget(t *testing.T) {
	forEachEngineWithClock(t, func(t *testing.T, e Engine, clk *fakeClock) {
		load(e, 0, 200000, clk.In(time.Second))
		clk.Advance(2 * time.Second)
		st := NewActiveExpirer(e, 10).Cycle(2 * time.Millisecond)
		if !st.TimedOut || st.Expired == 0 || st.Expired >= 200000 {
			t.Fatalf("stats %+v: want a partial cycle that timed out", st)
		}
		if st.Took > 50*time.Millisecond { // generous: -race slows everything down
			t.Fatalf("cycle took %v with a 2ms budget", st.Took)
		}
	})
}

// "Done when": memory actually drops after a mass expiry with no reads.
func TestMassExpiryFreesMemory(t *testing.T) {
	skipMemoryTest(t)
	clk := newFakeClock()
	e := newSharded(Options{Clock: clk.Now})
	base := heapAfterGC()
	val := make([]byte, 100)
	for i := range 300000 {
		k := keyN(i)
		e.Do([][]byte{k}, func(tx *Tx) { tx.SetWithExpire(k, append([]byte(nil), val...), clk.In(time.Second)) })
	}
	loaded := heapAfterGC()

	clk.Advance(2 * time.Second)
	x := NewActiveExpirer(e, 10)
	for dbsize(e) > 0 {
		x.Cycle(time.Second)
	}
	after := heapAfterGC()
	runtime.KeepAlive(e)

	used, left := float64(loaded-base), float64(after)-float64(base)
	t.Logf("heap: base %.1f MB, loaded %.1f MB, after expiry %.1f MB",
		float64(base)/(1<<20), float64(loaded)/(1<<20), float64(after)/(1<<20))
	if left > 0.1*used {
		t.Fatalf("only freed %.0f%% of the memory the keys used", 100*(1-left/used))
	}
}

func TestExpiryEvents(t *testing.T) {
	clk := newFakeClock()
	bus := events.NewBus("n1")
	sub := bus.Subscribe(10)
	defer sub.Close()
	e := newSharded(Options{Bus: bus, Clock: clk.Now})
	a, c := b("a"), b("c")
	e.Do([][]byte{a, c}, func(tx *Tx) {
		tx.SetWithExpire(a, b("v"), clk.In(time.Second))
		tx.SetWithExpire(c, b("v"), clk.In(time.Second))
	})
	<-sub.C() // the two set events
	<-sub.C()
	clk.Advance(2 * time.Second)
	e.Do([][]byte{a}, func(tx *Tx) { tx.Get(a) }) // lazy
	NewActiveExpirer(e, 10).Cycle(time.Second)    // active gets c
	for _, want := range []struct{ key, how string }{{"a", "lazy"}, {"c", "active"}} {
		ev := <-sub.C()
		if ev.Type != "expire" || ev.Data["key"] != want.key || ev.Data["how"] != want.how {
			t.Fatalf("event %+v, want expire %s %s", ev, want.key, want.how)
		}
	}
}

// The expirer runs on its own goroutine while clients read and write the
// same keys. The race detector checks the two never touch data unsafely.
func TestActiveExpiryRunsAlongsideClients(t *testing.T) {
	forEachEngineWithClock(t, func(t *testing.T, e Engine, clk *fakeClock) {
		ctx, cancel := context.WithCancel(context.Background())
		x := NewActiveExpirer(e, 500)
		done := make(chan struct{})
		go func() { x.Run(ctx); close(done) }()

		var wg sync.WaitGroup
		for w := range 8 {
			wg.Go(func() {
				r := rand.New(rand.NewPCG(uint64(w), 0))
				for range 3000 {
					k := keyN(r.IntN(1000))
					e.Do([][]byte{k}, func(tx *Tx) {
						if r.IntN(2) == 0 {
							tx.SetWithExpire(k, b("v"), clk.In(time.Duration(r.IntN(50))*time.Millisecond))
						} else {
							tx.Get(k)
						}
					})
					clk.Advance(time.Millisecond)
				}
			})
		}
		wg.Wait()
		cancel()
		<-done
	})
}

// Go maps never shrink. If expired keys share slots with surviving keys, the
// slot maps stay sized for their peak and memory isn't given back. This test
// mixes 30k surviving keys into 300k expiring ones and checks the heap ends
// up close to what 30k keys alone would cost.
func TestMemoryShrinksWhenMostKeysExpire(t *testing.T) {
	skipMemoryTest(t)
	val := make([]byte, 16)
	persistentOnly := func() uint64 {
		e := newSharded(Options{})
		base := heapAfterGC()
		for i := range 30000 {
			k := keyN(i)
			e.Do([][]byte{k}, func(tx *Tx) { tx.Set(k, append([]byte(nil), val...)) })
		}
		used := heapAfterGC() - base
		runtime.KeepAlive(e)
		return used
	}()

	clk := newFakeClock()
	e := newSharded(Options{Clock: clk.Now})
	base := heapAfterGC()
	for i := range 330000 {
		k := keyN(i)
		at := clk.In(time.Second)
		if i < 30000 {
			at = 0 // these survive
		}
		e.Do([][]byte{k}, func(tx *Tx) { tx.SetWithExpire(k, append([]byte(nil), val...), at) })
	}
	clk.Advance(2 * time.Second)
	x := NewActiveExpirer(e, 10)
	for dbsize(e) > 30000 {
		x.Cycle(time.Second)
	}
	after := heapAfterGC() - base
	runtime.KeepAlive(e)

	t.Logf("30k keys alone: %.1f MB; 30k survivors after 300k expired: %.1f MB",
		float64(persistentOnly)/(1<<20), float64(after)/(1<<20))
	if after > 3*persistentOnly {
		t.Fatalf("heap after expiry is %.1fx what the surviving keys need", float64(after)/float64(persistentOnly))
	}
}

// Same problem without TTLs: a mass DEL leaves slot maps sized for their
// peak. The background pass visits every slot, so it compacts these too.
func TestMemoryShrinksAfterMassDelete(t *testing.T) {
	skipMemoryTest(t)
	e := newSharded(Options{})
	base := heapAfterGC()
	for i := range 330000 {
		k := keyN(i)
		e.Do([][]byte{k}, func(tx *Tx) { tx.Set(k, make([]byte, 16)) })
	}
	for i := 30000; i < 330000; i++ {
		k := keyN(i)
		e.Do([][]byte{k}, func(tx *Tx) { tx.Delete(k) })
	}
	before := heapAfterGC() - base
	NewActiveExpirer(e, 10).Cycle(time.Second)
	after := heapAfterGC() - base
	runtime.KeepAlive(e)
	t.Logf("30k survivors of a mass DEL: %.1f MB before compaction, %.1f MB after",
		float64(before)/(1<<20), float64(after)/(1<<20))
	if after*2 > before {
		t.Fatalf("compaction freed less than half (%.1f → %.1f MB)", float64(before)/(1<<20), float64(after)/(1<<20))
	}
}
