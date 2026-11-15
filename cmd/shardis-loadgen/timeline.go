package main

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/djism/shardis/pkg/client"
)

// timeline records throughput, errors and latency per interval (e.g. every
// 100 ms) for the whole run, warmup included, so a resharding or failover
// shows up as a shape over time rather than vanishing into an average.
type timeline struct {
	start time.Time
	every time.Duration
	slots []*tlSlot
}

type tlSlot struct {
	mu                  sync.Mutex
	ops, errors         int64
	lat                 histogram
	max                 time.Duration
	moved, ask, tryagin int64 // client redirect counters (cluster mode), sampled by sampleRedirects
}

func newTimeline(start time.Time, every, total time.Duration) *timeline {
	t := &timeline{start: start, every: every}
	for range int(total/every) + 2 {
		t.slots = append(t.slots, &tlSlot{})
	}
	return t
}

func (t *timeline) slot(at time.Time) *tlSlot {
	i := int(at.Sub(t.start) / t.every)
	return t.slots[min(max(i, 0), len(t.slots)-1)]
}

// record one operation that finished at `at`.
func (t *timeline) record(at time.Time, lat time.Duration, failed bool) {
	if t == nil {
		return
	}
	s := t.slot(at)
	s.mu.Lock()
	s.ops++
	if failed {
		s.errors++
	}
	s.lat.record(lat)
	s.max = max(s.max, lat)
	s.mu.Unlock()
}

// sampleRedirects copies the cluster client's counters into each interval
// (as deltas) until stop is closed.
func (t *timeline) sampleRedirects(st *client.Stats, stop chan struct{}) {
	var lastM, lastA, lastT int64
	tick := time.NewTicker(t.every)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-tick.C:
			m, a, tr := st.Moved.Load(), st.Ask.Load(), st.TryAgain.Load()
			s := t.slot(now.Add(-t.every / 2))
			s.mu.Lock()
			s.moved, s.ask, s.tryagin = m-lastM, a-lastA, tr-lastT
			s.mu.Unlock()
			lastM, lastA, lastT = m, a, tr
		}
	}
}

const timelineHeader = "t_s,ops_per_s,errors,moved,ask,tryagain,p50_ms,p99_ms,max_ms"

func (t *timeline) write(w io.Writer, until time.Time) {
	fmt.Fprintln(w, timelineHeader)
	for i, s := range t.slots {
		at := t.start.Add(time.Duration(i) * t.every)
		if !at.Before(until) {
			break
		}
		s.mu.Lock()
		fmt.Fprintf(w, "%.1f,%.0f,%d,%d,%d,%d,%.3f,%.3f,%.3f\n", float64(i)*t.every.Seconds(),
			float64(s.ops)/t.every.Seconds(), s.errors, s.moved, s.ask, s.tryagin,
			ms(s.lat.quantile(0.5)), ms(s.lat.quantile(0.99)), ms(s.max))
		s.mu.Unlock()
	}
}
