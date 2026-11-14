package store

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// skipMemoryTest skips heap-measuring tests under -race (`make test`); they
// run in `make test-memory`, which CI runs too.
func skipMemoryTest(t *testing.T) {
	t.Helper()
	if raceEnabled || testing.Short() {
		t.Skip("measures heap sizes; run with `make test-memory`")
	}
}

// The memory estimate drives eviction, so it must track reality. This loads
// keys of several shapes and compares the estimate with how much the Go heap
// actually grew. The overhead constants in memory.go come from this test.
func TestMemoryEstimateMatchesHeap(t *testing.T) {
	skipMemoryTest(t)
	cases := []struct {
		name           string
		keyLen, valLen int
		ttl, exact     bool
	}{
		{"small", 16, 16, false, false},
		{"100B values", 20, 100, false, false},
		{"1KB values", 30, 1000, false, false},
		{"with TTL", 16, 16, true, false},
		{"exact LRU", 16, 16, false, true},
	}
	const n = 200_000
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := newFakeClock()
			e := newSharded(Options{Clock: clk.Now, ExactLRU: tc.exact})
			base := heapAfterGC()
			for i := range n {
				k := fmt.Appendf(nil, "%0*d", tc.keyLen, i)
				var at int64
				if tc.ttl {
					at = clk.In(time.Hour)
				}
				e.Do([][]byte{k}, func(tx *Tx) { tx.SetWithExpire(k, make([]byte, tc.valLen), at) })
			}
			real := float64(heapAfterGC() - base)
			est := float64(e.Stats().UsedMemory)
			runtime.KeepAlive(e)
			ratio := est / real
			t.Logf("%-12s real %6.1f B/key, estimate %6.1f B/key, ratio %.2f", tc.name, real/n, est/n, ratio)
			if ratio < 0.9 || ratio > 1.1 {
				t.Errorf("estimate off by more than 10%% (ratio %.2f)", ratio)
			}
		})
	}
}

func TestAllocSize(t *testing.T) {
	for n, want := range map[int]int64{0: 0, 5: 5, 16: 16, 17: 24, 100: 112, 1000: 1024, 40000: 40960} {
		if got := allocSize(n); got != want {
			t.Errorf("allocSize(%d) = %d, want %d", n, got, want)
		}
	}
}

// The constants were fitted at 200k keys. Map fill level changes with size
// (maps double when they grow), so check the estimate holds at other counts.
func TestMemoryEstimateAtOtherSizes(t *testing.T) {
	skipMemoryTest(t)
	for _, n := range []int{20_000, 60_000, 500_000, 1_000_000} {
		e := newSharded(Options{})
		base := heapAfterGC()
		for i := range n {
			k := fmt.Appendf(nil, "%016d", i)
			e.Do([][]byte{k}, func(tx *Tx) { tx.Set(k, make([]byte, 16)) })
		}
		real := float64(heapAfterGC() - base)
		est := float64(e.Stats().UsedMemory)
		runtime.KeepAlive(e)
		t.Logf("%8d keys: real %6.1f B/key, estimate %6.1f B/key, ratio %.2f", n, real/float64(n), est/float64(n), est/real)
		if r := est / real; r < 0.9 || r > 1.1 {
			t.Errorf("%d keys: estimate off by more than 10%% (ratio %.2f)", n, r)
		}
	}
}
