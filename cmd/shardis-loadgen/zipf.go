package main

import (
	"math"
	"math/rand/v2"
)

// zipf draws ranks 0..n-1 where rank 0 is the most popular and popularity
// falls off as 1/(rank+1)^theta. theta=0.99 is the YCSB default and a common
// model of real cache traffic: a few keys get most of the requests.
//
// Go's rand.Zipf needs s > 1, so this implements the algorithm from Gray et
// al., "Quickly Generating Billion-Record Synthetic Databases" (SIGMOD 1994),
// the same one YCSB uses. Setup is O(n) (computing zeta), each draw is O(1).
type zipf struct {
	n                        float64
	theta, alpha, zetan, eta float64
	half                     float64 // 0.5^theta
	rng                      *rand.Rand
}

func newZipf(n uint64, theta float64, rng *rand.Rand) *zipf {
	z := &zipf{n: float64(n), theta: theta, rng: rng}
	z.zetan = zeta(n, theta)
	zeta2 := zeta(2, theta)
	z.alpha = 1 / (1 - theta)
	z.eta = (1 - math.Pow(2/z.n, 1-theta)) / (1 - zeta2/z.zetan)
	z.half = math.Pow(0.5, theta)
	return z
}

// zeta(n, theta) = sum over i=1..n of 1/i^theta.
func zeta(n uint64, theta float64) float64 {
	var sum float64
	for i := uint64(1); i <= n; i++ {
		sum += 1 / math.Pow(float64(i), theta)
	}
	return sum
}

// next returns a rank in [0, n).
func (z *zipf) next() uint64 {
	u := z.rng.Float64()
	uz := u * z.zetan
	if uz < 1 {
		return 0
	}
	if uz < 1+z.half {
		return 1
	}
	r := uint64(z.n * math.Pow(z.eta*u-z.eta+1, z.alpha))
	return min(r, uint64(z.n)-1)
}

// copy returns a generator sharing the (expensive) setup but with its own
// random source, so each worker goroutine can draw without locking.
func (z *zipf) copy(rng *rand.Rand) *zipf {
	c := *z
	c.rng = rng
	return &c
}
