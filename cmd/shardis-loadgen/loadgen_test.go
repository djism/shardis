package main

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

// The Zipf generator should produce the textbook shape: rank r's share of
// draws is (1/(r+1)^theta) / zeta(n, theta). Gray et al.'s method is exact
// for the top two ranks and a close approximation after that (it inverts a
// continuous curve), which is the tradeoff that makes each draw O(1). YCSB
// accepts it, and so do we: check ranks 0 and 1 tightly, and ranges of
// ranks within 10%.
func TestZipfMatchesTheory(t *testing.T) {
	const n, draws, theta = 1000, 2_000_000, 0.99
	z := newZipf(n, theta, rand.New(rand.NewPCG(1, 2)))
	hits := make([]int, n)
	for range draws {
		hits[z.next()]++
	}
	zn := zeta(n, theta)
	share := func(lo, hi int) (got, want float64) {
		for r := lo; r <= hi; r++ {
			got += float64(hits[r]) / draws
			want += (1 / math.Pow(float64(r+1), theta)) / zn
		}
		return got, want
	}
	for _, tc := range []struct {
		lo, hi int
		tol    float64
	}{{0, 0, 0.02}, {1, 1, 0.02}, {2, 9, 0.1}, {10, 99, 0.1}, {100, 999, 0.1}} {
		got, want := share(tc.lo, tc.hi)
		if math.Abs(got-want)/want > tc.tol {
			t.Errorf("ranks %d-%d: share %.4f, want %.4f", tc.lo, tc.hi, got, want)
		}
	}
	// Skew sanity: the top 10% of keys get most of the traffic.
	top := 0
	for r := range n / 10 {
		top += hits[r]
	}
	t.Logf("top 10%% of keys get %.0f%% of requests", 100*float64(top)/draws)
	if float64(top)/draws < 0.6 {
		t.Error("distribution not skewed enough")
	}
}

func TestZipfStaysInRange(t *testing.T) {
	z := newZipf(10, 0.5, rand.New(rand.NewPCG(3, 4)))
	for range 100000 {
		if r := z.next(); r >= 10 {
			t.Fatalf("rank %d out of range", r)
		}
	}
}

func TestHistogramQuantiles(t *testing.T) {
	var h histogram
	for i := 1; i <= 1000; i++ {
		h.record(time.Duration(i) * 10 * time.Microsecond) // 10 µs .. 10 ms, uniform
	}
	for _, tc := range []struct {
		q    float64
		want time.Duration
	}{{0.5, 5 * time.Millisecond}, {0.99, 9900 * time.Microsecond}} {
		got := h.quantile(tc.q)
		if math.Abs(float64(got-tc.want))/float64(tc.want) > 0.07 { // ~6% bucket resolution
			t.Errorf("q%.2f = %v, want ~%v", tc.q, got, tc.want)
		}
	}
}

func TestKeyNamesAreDistinct(t *testing.T) {
	seen := map[string]bool{}
	for r := range uint64(100000) {
		k := string(keyName(r))
		if seen[k] {
			t.Fatalf("duplicate key for rank %d", r)
		}
		seen[k] = true
	}
}
