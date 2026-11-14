package store

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

// TestMemoryPerKey measures what one key costs in Go heap bytes, without any
// network or GC-headroom noise (RSS includes free space the GC keeps around).
// Slow, so it only runs when asked:
//
//	SHARDIS_MEMTEST=1 go test ./internal/store -run TestMemoryPerKey -v
func TestMemoryPerKey(t *testing.T) {
	if os.Getenv("SHARDIS_MEMTEST") == "" {
		t.Skip("set SHARDIS_MEMTEST=1 to run")
	}
	const n = 1_000_000
	keyLen, valLen := len(fmt.Sprintf("key:%012d", 0)), 16

	before := heapAfterGC()
	e := newExecutor(nil)
	defer e.Close()
	e.Do(nil, func(tx *Tx) {
		for i := range n {
			tx.Set(fmt.Appendf(nil, "key:%012d", i), make([]byte, valLen))
		}
	})
	after := heapAfterGC()

	// A forced full GC with 1M keys live shows how long marking takes when
	// the heap is full of pointers (every key string and value slice is one).
	start := time.Now()
	runtime.GC()
	fullGC := time.Since(start)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	lastPause := time.Duration(ms.PauseNs[(ms.NumGC+255)%256])

	perKey := float64(after-before) / n
	t.Logf("keys=%d keyLen=%d valLen=%d heap=%.1f MB bytes/key=%.0f (payload %d, overhead %.0f)",
		n, keyLen, valLen, float64(after-before)/(1<<20), perKey, keyLen+valLen, perKey-float64(keyLen+valLen))
	t.Logf("forced full GC: %v wall, last stop-the-world pause %v", fullGC, lastPause)
}

func heapAfterGC() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}
