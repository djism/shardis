package main

import (
	"fmt"
	"syscall"
	"time"

	"github.com/djism/shardis/internal/admin"
)

// A scenario injects faults while the workload runs, then waits until the
// cluster has recovered. Timings assume node timeout 2 s.
type scenario struct {
	name, desc string
	run        func(h *harness) error
}

var scenarios = []scenario{
	{"kill-primary", "Primary a is killed (kill -9). Its replica d is elected; a restarts and rejoins as d's replica.", killPrimary},
	{"kill-replica", "Replica d is killed for 4 s, then restarts and resyncs. Nothing should notice.", killReplica},
	{"partition-minority-primary", "Primary a is cut off from the other five (its clients can still reach it). The majority fails over to d; after healing, a rejoins as d's replica and drops what it accepted alone.", partitionMinority},
	{"flapping-network", "a's network flaps: cut off for 1.5 s, back for 1.5 s, six times (each cut shorter than the 2 s node timeout).", flapping},
	{"slow-node", "Primary b freezes for 1 s, runs for 1 s, six times (SIGSTOP/SIGCONT): a GC-pause-like stall, too short for a failover.", slowNode},
	{"reshard-during-failover", "A rebalance onto new node g is running when source primary b is killed. e takes over, then `fix` finishes the half-moved slot and the rebalance completes.", reshardDuringFailover},
	{"restart-all", "Every node is killed at once (kill -9) and restarted. Data comes back from each node's AOF (fsync every second).", restartAll},
}

func killPrimary(h *harness) error {
	h.kill("a")
	if err := h.waitInfo("d", "role:master", 20*time.Second); err != nil {
		return err
	}
	time.Sleep(time.Second)
	if err := h.start("a"); err != nil {
		return err
	}
	return h.waitInfo("a", "master_link_status:up", 20*time.Second)
}

func killReplica(h *harness) error {
	h.kill("d")
	time.Sleep(4 * time.Second)
	if err := h.start("d"); err != nil {
		return err
	}
	return h.waitInfo("d", "master_link_status:up", 20*time.Second)
}

func partitionMinority(h *harness) error {
	h.rec.note("partition", "a", "side", []string{"a"})
	if err := h.all("FAULT", "PARTITION", "a"); err != nil {
		return err
	}
	if err := h.waitInfo("d", "role:master", 20*time.Second); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	h.rec.note("heal", "")
	if err := h.all("FAULT", "HEAL"); err != nil {
		return err
	}
	return h.waitInfo("a", "master_link_status:up", 20*time.Second)
}

func flapping(h *harness) error {
	for range 6 {
		h.rec.note("partition", "a", "side", []string{"a"})
		if err := h.all("FAULT", "PARTITION", "a"); err != nil {
			return err
		}
		time.Sleep(1500 * time.Millisecond)
		h.rec.note("heal", "")
		if err := h.all("FAULT", "HEAL"); err != nil {
			return err
		}
		time.Sleep(1500 * time.Millisecond)
	}
	return h.waitOK("b", 20*time.Second)
}

func slowNode(h *harness) error {
	for range 6 {
		h.signal("b", syscall.SIGSTOP, "pause")
		time.Sleep(time.Second)
		h.signal("b", syscall.SIGCONT, "resume")
		time.Sleep(time.Second)
	}
	return h.waitOK("c", 20*time.Second)
}

func reshardDuringFailover(h *harness) error {
	if _, err := admin.AddNode(h.addr("a"), h.addr("g"), ""); err != nil {
		return fmt.Errorf("add-node: %w", err)
	}
	h.join("g")
	if err := h.waitOK("g", 20*time.Second); err != nil {
		return err
	}
	h.rec.note("rebalance_start", "g")
	done := make(chan error, 1)
	go func() { done <- admin.Rebalance(h.addr("a"), admin.MoveOptions{}) }()
	time.Sleep(1500 * time.Millisecond)
	h.kill("b")
	if err := h.waitInfo("e", "role:master", 20*time.Second); err != nil {
		return err
	}
	err := <-done // the rebalance hit the dead node and stopped
	h.rec.note("rebalance_stopped", "g", "error", fmt.Sprint(err))
	if err := h.waitOK("a", 20*time.Second); err != nil {
		return err
	}
	h.rec.note("fix", "")
	if err := admin.Fix(h.addr("a"), admin.MoveOptions{}); err != nil {
		return fmt.Errorf("fix: %w", err)
	}
	h.rec.note("rebalance_start", "g")
	if err := admin.Rebalance(h.addr("a"), admin.MoveOptions{}); err != nil {
		return fmt.Errorf("second rebalance: %w", err)
	}
	h.rec.note("rebalance_done", "g")
	if err := h.start("b"); err != nil {
		return err
	}
	return h.waitInfo("b", "master_link_status:up", 20*time.Second)
}

func restartAll(h *harness) error {
	for _, n := range allNames {
		h.kill(n)
	}
	time.Sleep(time.Second)
	for _, n := range allNames { // primaries first
		if err := h.start(n); err != nil {
			return err
		}
	}
	for _, n := range []string{"d", "e", "f"} {
		if err := h.waitInfo(n, "master_link_status:up", 30*time.Second); err != nil {
			return err
		}
	}
	return h.waitOK("b", 30*time.Second)
}
