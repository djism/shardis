package server

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/djism/shardis/internal/resp"
)

// Graceful handoff (Iteration 14, ADR 043). When a primary is asked to stop
// (SIGTERM from Kubernetes during a rolling restart, or Ctrl-C), it doesn't
// just vanish and leave its replica to notice after node_timeout. That would
// be seconds of unavailability, plus losing writes still in flight to the
// replica. Instead it:
//
//  1. pauses writes atomically: takes every slot gate exclusively, so
//     in-flight writes finish and new ones get TRYAGAIN (clients retry);
//  2. waits until its replica has acknowledged everything (WAIT 1);
//  3. tells the replica CLUSTER FAILOVER TAKEOVER: it claims the slots with a
//     higher epoch at once, no election needed;
//  4. waits until its own view shows the new owner, so clients still
//     connected get MOVED instead of TRYAGAIN, then shuts down.
//
// Every write it acknowledged is on the new primary. If the replica isn't
// reachable or in sync within the timeout, it gives up on the handoff and
// shuts down anyway (the ordinary failover then takes over).

const errHandingOff = "TRYAGAIN this primary is handing its slots to a replica before shutting down; retry"

// pauseWrites makes write commands fail with TRYAGAIN, after every write
// already running has finished.
func (s *Server) pauseWrites() {
	for i := range s.gates {
		s.gates[i].Lock()
	}
	s.writesPaused.Store(true)
	for i := range s.gates {
		s.gates[i].Unlock()
	}
}

// Handoff runs the steps above. It returns nil if there was nothing to hand
// off (not a cluster primary, no slots, no replica) or if it worked.
func (s *Server) Handoff(timeout time.Duration) error {
	if s.cluster == nil || s.repl == nil {
		return nil
	}
	t := s.cluster.View()
	me := t.Me
	if !me.IsPrimary() || len(me.Slots) == 0 || len(me.Replicas) == 0 {
		return nil
	}
	start := time.Now()
	s.pauseWrites()
	s.log.Info("graceful handoff: writes paused, waiting for the replica to catch up")
	if s.repl.Wait(1, timeout/2) < 1 {
		return errors.New("no replica caught up in time; shutting down without a handoff")
	}
	var lastErr error
	for _, r := range me.Replicas { // the first one that agrees takes over
		if lastErr = takeover(r.Internal); lastErr == nil {
			break
		}
	}
	if lastErr != nil {
		return fmt.Errorf("no replica took over: %w", lastErr)
	}
	slot := me.Slots[0].Start
	for time.Since(start) < timeout {
		if s.cluster.View().Owner(slot) != s.cluster.View().Me {
			s.log.Info("graceful handoff done", "took", time.Since(start).Round(time.Millisecond))
			if s.bus.Active() {
				s.bus.Emit("handoff", map[string]any{"took_ms": time.Since(start).Milliseconds()})
			}
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("the replica didn't announce its takeover in time")
}

func takeover(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	w, r := resp.NewWriter(c), resp.NewReader(c)
	w.WriteCommand([]byte("CLUSTER"), []byte("FAILOVER"), []byte("TAKEOVER"))
	if err := w.Flush(); err != nil {
		return err
	}
	v, err := r.ReadValue()
	if err != nil {
		return err
	}
	if v.Kind == resp.Error {
		return errors.New(string(v.Str))
	}
	return nil
}
