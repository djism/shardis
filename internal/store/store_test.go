package store

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/djism/shardis/internal/events"
	"github.com/djism/shardis/internal/keyslot"
)

// forEachEngine runs a test against both engines: they must behave identically.
func forEachEngine(t *testing.T, test func(t *testing.T, e Engine)) {
	for _, kind := range Engines {
		t.Run(kind, func(t *testing.T) {
			e, err := New(kind, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			test(t, e)
		})
	}
}

func b(s string) []byte { return []byte(s) }

func TestBasicOps(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e Engine) {
		k := b("k")
		e.Do([][]byte{k}, func(tx *Tx) {
			if _, ok := tx.Get(k); ok {
				t.Error("found missing key")
			}
			tx.Set(k, b("v"))
			if v, ok := tx.Get(k); !ok || string(v) != "v" {
				t.Errorf("Get = %q, %v", v, ok)
			}
			if !tx.Exists(k) || !tx.Delete(k) || tx.Delete(k) || tx.Exists(k) {
				t.Error("exists/delete sequence wrong")
			}
		})
		e.DoAll(func(tx *Tx) {
			for i := range 100 {
				tx.Set(b(fmt.Sprint("key", i)), b("x"))
			}
			if tx.Len() != 100 {
				t.Errorf("Len = %d", tx.Len())
			}
			tx.Flush()
			if tx.Len() != 0 {
				t.Errorf("Len after Flush = %d", tx.Len())
			}
		})
	})
}

// The atomicity test that matters most: a read-modify-write (like INCR)
// from 100 goroutines must not lose a single update.
func TestReadModifyWriteIsAtomic(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e Engine) {
		const workers, perWorker = 100, 1000
		k := b("counter")
		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				for range perWorker {
					e.Do([][]byte{k}, func(tx *Tx) {
						v, _ := tx.Get(k)
						n, _ := strconv.Atoi(string(v))
						tx.Set(k, strconv.AppendInt(nil, int64(n+1), 10))
					})
				}
			})
		}
		wg.Wait()
		e.Do([][]byte{k}, func(tx *Tx) {
			if v, _ := tx.Get(k); string(v) != strconv.Itoa(workers*perWorker) {
				t.Errorf("counter = %s, want %d", v, workers*perWorker)
			}
		})
	})
}

// Two keys written together must never be seen half-updated, even when they
// live in different shards (so the sharded engine takes two locks).
func TestMultiKeyIsAtomic(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e Engine) {
		a, c := b("a"), b("{other}c")
		if shardOf(keyslot.Of(a)) == shardOf(keyslot.Of(c)) {
			t.Fatal("test keys should be in different shards")
		}
		keys := [][]byte{a, c}
		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			defer close(done)
			for i := range 20000 {
				v := strconv.AppendInt(nil, int64(i), 10)
				e.Do(keys, func(tx *Tx) { tx.Set(a, v); tx.Set(c, v) })
			}
		})
		for range 4 {
			wg.Go(func() {
				for {
					select {
					case <-done:
						return
					default:
					}
					e.Do(keys, func(tx *Tx) {
						va, _ := tx.Get(a)
						vc, _ := tx.Get(c)
						if string(va) != string(vc) {
							t.Errorf("torn read: a=%s c=%s", va, vc)
						}
					})
				}
			})
		}
		wg.Wait()
	})
}

// Many goroutines locking random overlapping key sets, declared in random
// order. If lock ordering were wrong, this would deadlock and time out.
func TestNoDeadlockOnOverlappingKeys(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e Engine) {
		pool := make([][]byte, 64)
		for i := range pool {
			pool[i] = b(fmt.Sprint("k", i))
		}
		finished := make(chan struct{})
		go func() {
			var wg sync.WaitGroup
			for range 32 {
				wg.Go(func() {
					for range 2000 {
						keys := make([][]byte, 1+rand.IntN(5))
						for i := range keys {
							keys[i] = pool[rand.IntN(len(pool))]
						}
						e.Do(keys, func(tx *Tx) {
							for _, k := range keys {
								tx.Set(k, b("v"))
							}
						})
					}
				})
			}
			wg.Go(func() { // whole-keyspace ops mixed in
				for range 200 {
					e.DoAll(func(tx *Tx) { tx.Len() })
				}
			})
			wg.Wait()
			close(finished)
		}()
		select {
		case <-finished:
		case <-time.After(30 * time.Second):
			t.Fatal("deadlock: workers did not finish")
		}
	})
}

// The sharded engine must refuse to touch a key that wasn't locked.
func TestShardedPanicsOnUndeclaredKey(t *testing.T) {
	e := newSharded(nil)
	declared, other := b("a"), b("{other}c")
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for undeclared key")
		}
	}()
	e.Do([][]byte{declared}, func(tx *Tx) { tx.Get(other) })
}

func TestShardedPanicsOnWholeKeyspaceInsideDo(t *testing.T) {
	e := newSharded(nil)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for Len inside Do")
		}
	}()
	e.Do([][]byte{b("a")}, func(tx *Tx) { tx.Len() })
}

// SCAN's guarantee: every key present for the whole scan is returned
// exactly once, even with other keys being added and removed meanwhile.
func TestScanVisitsStableKeysOnce(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e Engine) {
		stable := map[string]bool{}
		e.DoAll(func(tx *Tx) {
			for i := range 5000 {
				k := fmt.Sprint("stable:", i)
				stable[k] = true
				tx.Set(b(k), b("x"))
			}
		})
		seen := map[string]int{}
		var cursor uint16
		churn := 0
		for {
			e.DoAll(func(tx *Tx) {
				cursor = tx.Scan(cursor, 100, func(k string) { seen[k]++ })
			})
			// Churn between SCAN calls: add and delete other keys.
			churn++
			e.DoAll(func(tx *Tx) {
				tx.Set(b(fmt.Sprint("churn:", churn)), b("x"))
				tx.Delete(b(fmt.Sprint("churn:", churn-1)))
			})
			if cursor == 0 {
				break
			}
		}
		for k := range stable {
			if seen[k] != 1 {
				t.Fatalf("key %s seen %d times", k, seen[k])
			}
		}
	})
}

func TestDeleteReleasesEmptySlotMaps(t *testing.T) {
	e := newExecutor(nil)
	defer e.Close()
	k := b("k")
	e.Do(nil, func(tx *Tx) {
		tx.Set(k, b("v"))
		tx.Delete(k)
		if tx.ks.slots[keyslot.Of(k)] != nil {
			t.Error("empty slot map not released")
		}
	})
}

func TestWritesEmitEvents(t *testing.T) {
	bus := events.NewBus("n1")
	sub := bus.Subscribe(10)
	defer sub.Close()
	e := newExecutor(bus)
	defer e.Close()
	e.Do(nil, func(tx *Tx) { tx.Set(b("k"), b("v")); tx.Delete(b("k")) })
	for _, want := range []string{"set", "del"} {
		ev := <-sub.C()
		if ev.Type != "key_write" || ev.Data["op"] != want || ev.Data["key"] != "k" {
			t.Fatalf("event %+v, want op %s", ev, want)
		}
	}
}

func TestNewRejectsUnknownEngine(t *testing.T) {
	if _, err := New("nope", nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestGlobMatch(t *testing.T) {
	tests := []struct {
		pattern string
		match   []string
		noMatch []string
	}{
		{"*", []string{"", "anything"}, nil},
		{"h?llo", []string{"hello", "hallo"}, []string{"hllo", "heello"}},
		{"h*llo", []string{"hllo", "heeeello"}, []string{"hlo"}},
		{"h[ae]llo", []string{"hello", "hallo"}, []string{"hillo"}},
		{"h[^e]llo", []string{"hallo", "hbllo"}, []string{"hello"}},
		{"h[a-b]llo", []string{"hallo", "hbllo"}, []string{"hcllo"}},
		{`h\*llo`, []string{"h*llo"}, []string{"hello"}},
		{"user:*:cart", []string{"user:1:cart", "user::cart"}, []string{"user:1:carts"}},
		{"a*b*c", []string{"abc", "aXXbYYc"}, []string{"acb"}},
		{"[abc", []string{"[abc"}, []string{"a"}}, // unterminated class is literal
	}
	for _, tc := range tests {
		for _, s := range tc.match {
			if !Match(tc.pattern, s) {
				t.Errorf("Match(%q, %q) = false", tc.pattern, s)
			}
		}
		for _, s := range tc.noMatch {
			if Match(tc.pattern, s) {
				t.Errorf("Match(%q, %q) = true", tc.pattern, s)
			}
		}
	}
}

// A naive recursive matcher takes forever on this. Ours must be instant.
func TestGlobPathologicalPattern(t *testing.T) {
	s := string(slices.Repeat([]byte("a"), 5000))
	start := time.Now()
	if Match("*a*a*a*a*a*a*a*a*a*a*b", s) {
		t.Fatal("should not match")
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("took %v", d)
	}
}

// BenchmarkDoSpread: each op SETs a random key out of 100k, like the network
// benchmark. Shows how each engine scales with cores when keys don't collide.
func BenchmarkDoSpread(b *testing.B) {
	keys := make([][]byte, 100_000)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key:%d", i)
	}
	val := []byte("v")
	for _, kind := range Engines {
		b.Run(kind, func(b *testing.B) {
			e, _ := New(kind, nil)
			defer e.Close()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(rand.Uint64(), 0))
				for pb.Next() {
					k := keys[r.IntN(len(keys))]
					e.Do([][]byte{k}, func(tx *Tx) { tx.Set(k, val) })
				}
			})
		})
	}
}

// BenchmarkDoHotKey: every op hits the same key. Worst case for sharded
// (one lock, all cores fighting over it).
func BenchmarkDoHotKey(b *testing.B) {
	for _, kind := range Engines {
		b.Run(kind, func(b *testing.B) {
			e, _ := New(kind, nil)
			defer e.Close()
			key, val := []byte("key:1"), []byte("v")
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					e.Do([][]byte{key}, func(tx *Tx) { tx.Set(key, val) })
				}
			})
		})
	}
}
