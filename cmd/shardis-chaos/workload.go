package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/djism/shardis/internal/resp"
	"github.com/djism/shardis/pkg/client"
)

// The workload: writers each own a set of keys (registers) and write
// increasing sequence numbers to them, one at a time. Readers read random
// keys. Every operation's start, end and result is recorded, and the
// checker compares that history with the final state. Writing increasing
// numbers is what makes "lost" and "stale" checkable: a key's value can
// only legitimately go up (ADR 040).
const (
	writers       = 8
	keysPerWriter = 64
	readers       = 8
	opInterval    = time.Millisecond // each worker paces itself: ~16k ops/s total
)

type status uint8

const (
	ok status = iota
	failed
	unknown // ErrUnknownOutcome (can't happen for SET under RetrySafe, but recorded)
)

type write struct {
	seq        int64
	start, end time.Duration
	st         status
}

type read struct {
	key        int
	val        int64 // 0 = nil
	start, end time.Duration
	st         status
}

type history struct {
	t0     time.Time
	writes [writers * keysPerWriter][]write // per key, in order
	reads  [readers][]read
	errs   []string // first few error messages
	errMu  sync.Mutex
}

// keyPrefix is unique per run, so a run never reads values a previous run
// left in a long-lived cluster (external mode). Without it, the checker
// reported "phantom" reads on Kubernetes: leftovers from the last run.
var keyPrefix = fmt.Sprintf("chaos:%d:", time.Now().UnixNano()%1e9)

func keyName(k int) []byte { return []byte(fmt.Sprint(keyPrefix, k)) }

func (h *history) noteErr(err error, v resp.Value) {
	h.errMu.Lock()
	defer h.errMu.Unlock()
	if len(h.errs) < 5 {
		h.errs = append(h.errs, fmt.Sprint(err, " ", string(v.Str)))
	}
}

func classify(v resp.Value, err error) status {
	switch {
	case errors.Is(err, client.ErrUnknownOutcome):
		return unknown
	case err != nil || v.Kind == resp.Error:
		return failed
	}
	return ok
}

// runWorkload runs until stop is closed.
func runWorkload(cl *client.Cluster, h *history, stop chan struct{}) {
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			seq := make([]int64, keysPerWriter)
			tick := time.NewTicker(opInterval)
			defer tick.Stop()
			for j := 0; ; j = (j + 1) % keysPerWriter {
				select {
				case <-stop:
					return
				case <-tick.C:
				}
				k := w*keysPerWriter + j
				seq[j]++
				s := time.Since(h.t0)
				v, err := cl.Do(context.Background(), []byte("SET"), keyName(k), strconv.AppendInt(nil, seq[j], 10))
				st := classify(v, err)
				if st != ok {
					h.noteErr(err, v)
				}
				h.writes[k] = append(h.writes[k], write{seq[j], s, time.Since(h.t0), st})
			}
		})
	}
	for r := range readers {
		wg.Go(func() {
			rnd := rand.New(rand.NewPCG(uint64(r), 7))
			tick := time.NewTicker(opInterval)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
				}
				k := rnd.IntN(writers * keysPerWriter)
				s := time.Since(h.t0)
				v, err := cl.Do(context.Background(), []byte("GET"), keyName(k))
				st := classify(v, err)
				if st != ok {
					h.noteErr(err, v)
				}
				n, _ := strconv.ParseInt(string(v.Str), 10, 64)
				h.reads[r] = append(h.reads[r], read{k, n, s, time.Since(h.t0), st})
			}
		})
	}
	wg.Wait()
}

// report is what the checker found.
type report struct {
	Scenario      string   `json:"scenario"`
	Ops           int      `json:"ops"`
	Errors        int      `json:"errors"`
	ErrorRate     float64  `json:"error_rate"`
	Unknown       int      `json:"unknown"`
	AckedWrites   int      `json:"acked_writes"`
	LostWrites    int      `json:"lost_writes"`        // acked, and the final value is lower
	RolledBack    int      `json:"rolled_back_writes"` // acked, and a later read saw a lower value
	StaleReads    int      `json:"stale_reads"`
	Reads         int      `json:"reads"`
	Phantoms      int      `json:"phantom_reads"`
	MaxStallMs    float64  `json:"max_stall_ms"`
	MaxStallAtS   float64  `json:"max_stall_at_s"` // when the longest stall ended (s since start)
	P99Ms         float64  `json:"p99_ms"`
	MaxLatencyMs  float64  `json:"max_latency_ms"`
	Events        int      `json:"events"`
	EventsDropped int64    `json:"events_dropped"`
	FirstErrors   []string `json:"first_errors,omitempty"`
}

// check compares the history with the final values (read after the
// cluster has healed). Definitions (ADR 040):
//
//	lost write:  acknowledged, but the final value is lower than its seq
//	             (the register went backwards: the write's effect vanished)
//	rolled back: acknowledged, and some read that started after the ack
//	             returned a lower value. Catches losses that later writes
//	             to the same key paper over, which "lost" can't see.
//	stale read:  returned a value lower than one whose write had already been
//	             acknowledged before the read started
//	phantom:     returned a value no writer ever tried to write
//	max stall:   longest time any writer went without an acknowledged write
func check(h *history, final []int64, end time.Duration) report {
	var r report
	var lat []time.Duration
	maxAttempted := make([]int64, len(h.writes))
	for k, ws := range h.writes {
		for _, w := range ws {
			r.Ops++
			lat = append(lat, w.end-w.start)
			maxAttempted[k] = max(maxAttempted[k], w.seq)
			switch w.st {
			case failed:
				r.Errors++
			case unknown:
				r.Unknown++
			case ok:
				r.AckedWrites++
				if final[k] < w.seq {
					r.LostWrites++
				}
			}
		}
		if final[k] > maxAttempted[k] {
			r.Phantoms++
		}
	}
	// Per writer: the longest gap between acknowledged writes.
	for w := range writers {
		var acks []time.Duration
		for j := range keysPerWriter {
			for _, wr := range h.writes[w*keysPerWriter+j] {
				if wr.st == ok {
					acks = append(acks, wr.end)
				}
			}
		}
		attempted := 0
		for j := range keysPerWriter {
			attempted += len(h.writes[w*keysPerWriter+j])
		}
		if attempted == 0 {
			continue // never wrote (only in hand-made test histories)
		}
		slices.Sort(acks)
		prev := time.Duration(0)
		for _, a := range append(acks, end) {
			if gap := float64(a-prev) / 1e6; gap > r.MaxStallMs {
				r.MaxStallMs, r.MaxStallAtS = gap, a.Seconds()
			}
			prev = a
		}
	}
	rolled := make([]map[int]bool, len(h.writes)) // key → indices of rolled-back writes
	for _, rs := range h.reads {
		for _, rd := range rs {
			r.Ops++
			lat = append(lat, rd.end-rd.start)
			if rd.st != ok {
				r.Errors++
				continue
			}
			r.Reads++
			if rd.val > maxAttempted[rd.key] {
				r.Phantoms++
				continue
			}
			// The highest seq acknowledged before this read began. Writes per
			// key are sequential, so acks are in seq order.
			var floor int64
			for _, w := range h.writes[rd.key] {
				if w.st == ok && w.end < rd.start {
					floor = w.seq
				}
				if w.start > rd.start {
					break
				}
			}
			if rd.val < floor {
				r.StaleReads++
				// Every acked write between the value read and the floor was
				// already acknowledged, yet this read didn't see it.
				for i, w := range h.writes[rd.key] {
					if w.st == ok && w.seq > rd.val && w.end < rd.start {
						if rolled[rd.key] == nil {
							rolled[rd.key] = map[int]bool{}
						}
						rolled[rd.key][i] = true
					}
				}
			}
		}
	}
	for k, ws := range h.writes { // final-state losses count as rolled back too
		for i, w := range ws {
			if w.st == ok && final[k] < w.seq {
				if rolled[k] == nil {
					rolled[k] = map[int]bool{}
				}
				rolled[k][i] = true
			}
		}
	}
	for _, m := range rolled {
		r.RolledBack += len(m)
	}
	if r.Ops > 0 {
		r.ErrorRate = float64(r.Errors+r.Unknown) / float64(r.Ops)
	}
	slices.Sort(lat)
	if len(lat) > 0 {
		r.P99Ms = float64(lat[len(lat)*99/100]) / 1e6
		r.MaxLatencyMs = float64(lat[len(lat)-1]) / 1e6
	}
	r.FirstErrors = h.errs
	return r
}

// timeline buckets completed operations per 100 ms for the trace.
type bucket struct {
	T    float64 `json:"t"`
	OK   int     `json:"ok"`
	Fail int     `json:"fail"`
}

func timeline(h *history, end time.Duration) []bucket {
	n := int(end/(100*time.Millisecond)) + 1
	out := make([]bucket, n)
	for i := range out {
		out[i].T = float64(i) / 10
	}
	add := func(at time.Duration, st status) {
		i := min(int(at/(100*time.Millisecond)), n-1)
		if st == ok {
			out[i].OK++
		} else {
			out[i].Fail++
		}
	}
	for _, ws := range h.writes {
		for _, w := range ws {
			add(w.end, w.st)
		}
	}
	for _, rs := range h.reads {
		for _, rd := range rs {
			add(rd.end, rd.st)
		}
	}
	return out
}
