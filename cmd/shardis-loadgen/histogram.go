package main

import (
	"math/bits"
	"time"
)

// histogram records latencies in log-linear buckets: 16 buckets per power
// of two, i.e. about 6% resolution at any scale, from 1 µs to ~2 hours, in a
// fixed 1 KB-ish array. Cheap to record into and to merge across workers.
// (The same idea as HdrHistogram, much simplified.)
type histogram struct {
	buckets [64 * 16]int64
	count   int64
}

const subBuckets = 16

func bucketOf(d time.Duration) int {
	us := uint64(d / time.Microsecond)
	if us < subBuckets {
		return int(us)
	}
	exp := bits.Len64(us) - 5 // us has (exp+5) bits; keep the top 5 (1 implicit + 4 for the sub-bucket)
	sub := (us >> exp) & (subBuckets - 1)
	return (exp+1)*subBuckets + int(sub)
}

// bucketValue is the upper edge of a bucket, in time.
func bucketValue(i int) time.Duration {
	if i < subBuckets {
		return time.Duration(i+1) * time.Microsecond
	}
	exp := i/subBuckets - 1
	sub := uint64(i % subBuckets)
	us := (subBuckets + sub + 1) << exp
	return time.Duration(us) * time.Microsecond
}

func (h *histogram) record(d time.Duration) {
	i := min(bucketOf(d), len(h.buckets)-1)
	h.buckets[i]++
	h.count++
}

func (h *histogram) merge(o *histogram) {
	for i, n := range o.buckets {
		h.buckets[i] += n
	}
	h.count += o.count
}

// quantile returns the latency below which a fraction q of samples fall.
func (h *histogram) quantile(q float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	target := int64(q * float64(h.count))
	var seen int64
	for i, n := range h.buckets {
		seen += n
		if seen > target {
			return bucketValue(i)
		}
	}
	return bucketValue(len(h.buckets) - 1)
}
