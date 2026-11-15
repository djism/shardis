package client

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// backoff is how long to wait before retry number attempt (0-based):
// exponential (base, 2×base, 4×base, ...) capped at max, with "full
// jitter": a random wait between 0 and that value. Without jitter, 50
// clients that failed at the same instant (a primary died) would all retry
// at the same instant too, again and again (ADR 037).
func backoff(attempt int, base, maxWait time.Duration, rnd func(n int64) int64) time.Duration {
	ceil := maxWait
	if attempt < 30 {
		ceil = min(base<<attempt, maxWait)
	}
	if ceil <= 0 {
		return 0
	}
	return time.Duration(rnd(int64(ceil)) + 1)
}

func jitter(n int64) int64 { return rand.Int64N(n) }

// sleep waits d, or until ctx is done (returns false then).
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// ErrUnknownOutcome: the connection failed after the command was sent, so
// it may or may not have run, and it isn't safe to send again (running it
// twice would differ from running it once, e.g. INCR). The application
// decides: check the value, retry anyway, or report it (ADR 036).
var ErrUnknownOutcome = errors.New("client: command may or may not have run (connection lost after sending); not retried because it isn't idempotent")

// ErrRetryBudget: the cluster kept saying "not now" (node down, failover in
// progress, CLUSTERDOWN) for longer than MaxRetryTime.
var ErrRetryBudget = errors.New("client: retry budget exhausted")
