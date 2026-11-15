package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djism/shardis/internal/resp"
	shardisclient "github.com/djism/shardis/pkg/client"
)

// A primary asked to stop hands its slots to its replica first (ADR 043).
// Writers INCR counters throughout: every acknowledged INCR must survive,
// none may be applied twice, and the pause must be short.
func TestGracefulHandoff(t *testing.T) {
	tc := startCluster(t)
	c, err := shardisclient.NewCluster([]string{tc.nodes["b"].addr}, shardisclient.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	const n = 300
	var acked, unknown [n]atomic.Int64
	var errs atomic.Int64
	var maxLat atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 6 {
		wg.Go(func() {
			for i := w; ; i = (i + 6) % n {
				select {
				case <-stop:
					return
				default:
				}
				t0 := time.Now()
				v, err := c.Do(context.Background(), []byte("INCR"), []byte(fmt.Sprint("h:", i)))
				if l := time.Since(t0).Milliseconds(); l > maxLat.Load() {
					maxLat.Store(l)
				}
				switch {
				case errors.Is(err, shardisclient.ErrUnknownOutcome):
					unknown[i].Add(1)
				case err != nil || v.Kind == resp.Error:
					errs.Add(1)
				default:
					acked[i].Add(1)
				}
			}
		})
	}
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	if err := tc.nodes["a"].srv.Handoff(5 * time.Second); err != nil {
		t.Fatalf("handoff: %v", err)
	}
	took := time.Since(start)
	tc.nodes["a"].stop()
	if role := newClient(t, tc.nodes["d"].addr).do("ROLE"); !strings.HasPrefix(role, `["master"`) {
		t.Fatalf("d after the handoff: %s", role)
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	var lost, doubled, total int64
	for i := range n {
		v, err := c.Do(context.Background(), []byte("GET"), []byte(fmt.Sprint("h:", i)))
		if err != nil {
			t.Fatal(err)
		}
		got, _ := strconv.ParseInt(string(v.Str), 10, 64)
		a, u := acked[i].Load(), unknown[i].Load()
		total += a
		lost += max(0, a-got)
		doubled += max(0, got-a-u)
	}
	t.Logf("handoff took %v; %d INCRs acked, %d lost, %d doubled, %d unknown, %d errors; slowest request %d ms",
		took.Round(time.Millisecond), total, lost, doubled, unknownSum(&unknown), errs.Load(), maxLat.Load())
	if lost > 0 || doubled > 0 || errs.Load() > 0 {
		t.Fatalf("lost %d, doubled %d, errors %d", lost, doubled, errs.Load())
	}
}

func unknownSum(u *[300]atomic.Int64) (s int64) {
	for i := range u {
		s += u[i].Load()
	}
	return
}
