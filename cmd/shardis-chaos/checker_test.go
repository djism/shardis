package main

import (
	"testing"
	"time"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

// A hand-made history with one of each anomaly. The checker must find
// exactly these, and nothing else.
func TestCheckerFindsAnomalies(t *testing.T) {
	h := &history{}
	// key 0: writes 1, 2, 3 acknowledged; final value 2 → write 3 was lost.
	h.writes[0] = []write{{1, ms(0), ms(1), ok}, {2, ms(10), ms(11), ok}, {3, ms(20), ms(21), ok}}
	// key 1: write 1 acked, write 2 failed but actually applied (final 2): not lost, not phantom.
	h.writes[1] = []write{{1, ms(0), ms(1), ok}, {2, ms(10), ms(5000), failed}}
	final := make([]int64, writers*keysPerWriter)
	final[0], final[1] = 2, 2
	h.reads[0] = []read{
		{key: 0, val: 1, start: ms(12), end: ms(13), st: ok}, // stale: 2 was acked at 11 ms, before the read began
		{key: 0, val: 1, start: ms(10), end: ms(13), st: ok}, // fine: write 2 was still in flight
		{key: 0, val: 3, start: ms(30), end: ms(31), st: ok}, // fine: read the (later lost) value 3
		{key: 1, val: 9, start: ms(30), end: ms(31), st: ok}, // phantom: nobody wrote 9
		{key: 0, val: 0, start: ms(40), end: ms(41), st: failed},
	}
	r := check(h, final, ms(100))
	if r.LostWrites != 1 || r.StaleReads != 1 || r.Phantoms != 1 || r.Errors != 2 || r.AckedWrites != 4 {
		t.Fatalf("lost %d (want 1), stale %d (1), phantoms %d (1), errors %d (2), acked %d (4)",
			r.LostWrites, r.StaleReads, r.Phantoms, r.Errors, r.AckedWrites)
	}
	// Rolled back: write 3 on key 0 (final 2) and write 2 on key 0 (the
	// stale read at 12 ms returned 1 after 2 was acked).
	if r.RolledBack != 2 {
		t.Fatalf("rolled back %d, want 2", r.RolledBack)
	}
	// Writer 0 owns keys 0..63: acks at 1, 1, 11, 21 ms, then nothing until
	// the end at 100 ms → longest stall 79 ms.
	if r.MaxStallMs != 79 {
		t.Fatalf("max stall %.0f ms, want 79", r.MaxStallMs)
	}
}
