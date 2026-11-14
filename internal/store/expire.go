package store

import (
	"context"
	"time"
)

// Active expiry: lazy expiry (in Tx.lookup) only deletes keys someone reads.
// Keys that expire and are never touched again would sit in memory forever,
// so a background loop goes looking for them. It never scans everything.
// It samples (ADR 008), Redis-style:
//
//	for each slot with TTL keys:
//	    sample 20 keys that have a TTL, delete the expired ones
//	    if more than 25% were expired, there are probably many more: repeat
//	    otherwise this slot is "clean enough", move on
//	stop when the cycle's time budget is spent
//
// The 25% rule makes the work adapt: almost nothing when few keys are
// expiring, flat out (up to the budget) during a mass expiry.
//
// The same pass also compacts slot maps that have become mostly empty
// (deletes, expiries), since Go maps never shrink by themselves.
const (
	expireSampleSize = 20
	// Keep sampling a slot while more than 1 in 4 of a sample was expired.
	expireRepeatRatio = 4
	// Each cycle may use this share of its interval (25% of one core at most).
	expireCPUPercent = 25
	// Max time we hold one group's lock before moving on, so a mass expiry
	// never blocks the clients on a shard for more than about a millisecond.
	expireGroupBudget = time.Millisecond
)

// ActiveExpirer runs expiry cycles in the background. Not safe for
// concurrent use: one goroutine calls Run (or tests call Cycle).
type ActiveExpirer struct {
	engine Engine
	hz     int
	next   int // the group the next cycle starts at (round-robin across cycles)
}

// CycleStats describes one cycle, for tests and benchmarks.
type CycleStats struct {
	Groups   int // groups visited
	Sampled  int // TTL keys looked at
	Expired  int // keys deleted
	TimedOut bool
	Took     time.Duration
}

// NewActiveExpirer creates an expirer that runs hz cycles per second.
func NewActiveExpirer(e Engine, hz int) *ActiveExpirer {
	return &ActiveExpirer{engine: e, hz: max(hz, 1)}
}

// Run does hz cycles a second until ctx is cancelled.
func (x *ActiveExpirer) Run(ctx context.Context) {
	interval := time.Second / time.Duration(x.hz)
	budget := interval * expireCPUPercent / 100
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			x.Cycle(budget)
		}
	}
}

// Cycle visits slot groups round-robin, continuing where the last cycle
// stopped, until every group has been visited once or budget is spent.
func (x *ActiveExpirer) Cycle(budget time.Duration) CycleStats {
	start := time.Now()
	deadline := start.Add(budget)
	var st CycleStats
	for range NumGroups {
		g := x.next
		x.next = (x.next + 1) % NumGroups
		groupDeadline := time.Now().Add(expireGroupBudget)
		if deadline.Before(groupDeadline) {
			groupDeadline = deadline
		}
		x.engine.DoGroup(g, func(tx *Tx, slots []uint16) {
			x.expireGroup(tx, slots, groupDeadline, &st)
		})
		st.Groups++
		if time.Now().After(deadline) {
			st.TimedOut = true
			break
		}
	}
	st.Took = time.Since(start)
	return st
}

func (x *ActiveExpirer) expireGroup(tx *Tx, slots []uint16, deadline time.Time, st *CycleStats) {
	for _, s := range slots {
		for tx.hasVolatile(s) {
			sampled, expired := tx.sampleExpired(s, expireSampleSize)
			st.Sampled += sampled
			st.Expired += expired
			if expired*expireRepeatRatio <= sampled {
				break // at most 25% stale: good enough
			}
			if time.Now().After(deadline) {
				return // let clients in; a later cycle comes back here
			}
		}
		// The slot has settled for this cycle: if its maps are now mostly
		// empty space, rebuild them once. Also catches slots drained by DEL.
		tx.compact(s)
	}
}
