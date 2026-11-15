package admin

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Create forms a new cluster from empty nodes, like redis-cli --cluster
// create. The first len/(replicas+1) nodes become primaries with equal slot
// ranges; the rest become replicas, round-robin. Used by the Kubernetes
// bootstrap Job, so it must be safe to run again: if the nodes already form
// a complete cluster it does nothing (ADR 044).
//
// Steps (the order matters):
//  1. wait until every node answers (pods may still be starting, DNS too);
//  2. give each node a distinct config epoch (only allowed while it knows
//     nobody else), so slot claims never tie;
//  3. assign each primary its slot range;
//  4. MEET everyone through the first node; gossip does the rest;
//  5. once everyone knows everyone, point each replica at its primary;
//  6. wait for cluster_state:ok.
func Create(addrs []string, replicas int, out func(format string, a ...any)) error {
	if out == nil {
		out = func(string, ...any) {}
	}
	nPrim := len(addrs) / (replicas + 1)
	if nPrim < 3 || len(addrs)%(replicas+1) != 0 {
		return fmt.Errorf("%d nodes can't make ≥3 primaries with %d replica(s) each", len(addrs), replicas)
	}
	deadline := time.Now().Add(3 * time.Minute)
	ids := make([]string, len(addrs))
	for i, a := range addrs {
		for {
			c, err := dial(a)
			if err == nil {
				v, err2 := c.do("CLUSTER", "MYID")
				c.close()
				if err2 == nil {
					ids[i] = string(v.Str)
					break
				}
				err = err2
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("%s never answered: %w", a, err)
			}
			out("waiting for %s (%v)\n", a, err)
			time.Sleep(2 * time.Second)
		}
	}
	formed, err := alreadyFormed(addrs[0], len(addrs))
	if err != nil {
		return err
	}
	if formed {
		out("cluster already formed: nothing to do\n")
		return nil
	}
	conns := make([]*conn, len(addrs))
	for i, a := range addrs {
		c, err := dial(a)
		if err != nil {
			return err
		}
		defer c.close()
		conns[i] = c
	}
	for i, c := range conns {
		if _, err := c.do("CLUSTER", "SET-CONFIG-EPOCH", strconv.Itoa(i+1)); err != nil && !strings.Contains(err.Error(), "non-zero") {
			return fmt.Errorf("%s: %w", addrs[i], err)
		}
	}
	for i := range nPrim {
		lo := i * 16384 / nPrim
		hi := (i+1)*16384/nPrim - 1
		if _, err := conns[i].do("CLUSTER", "ADDSLOTSRANGE", strconv.Itoa(lo), strconv.Itoa(hi)); err != nil && !strings.Contains(err.Error(), "busy") {
			return fmt.Errorf("%s: %w", addrs[i], err)
		}
		out("%s: primary, slots %d-%d\n", addrs[i], lo, hi)
	}
	host, port, err := net.SplitHostPort(addrs[0])
	if err != nil {
		return err
	}
	for i, c := range conns[1:] {
		if _, err := c.do("CLUSTER", "MEET", host, port); err != nil {
			return fmt.Errorf("%s: %w", addrs[i+1], err)
		}
	}
	for !everyoneKnowsAll(addrs, ids) {
		if time.Now().After(deadline) {
			return fmt.Errorf("the nodes never all met")
		}
		time.Sleep(200 * time.Millisecond)
	}
	for i := nPrim; i < len(addrs); i++ {
		p := (i - nPrim) % nPrim
		if _, err := conns[i].do("CLUSTER", "REPLICATE", ids[p]); err != nil {
			return fmt.Errorf("%s: %w", addrs[i], err)
		}
		out("%s: replica of %s\n", addrs[i], addrs[p])
	}
	// Wait until every node sees every replica as a replica (gossip spreads
	// the role change), so the next tool to look gets a settled picture.
	for !replicasSettled(addrs, ids[nPrim:]) {
		if time.Now().After(deadline) {
			return fmt.Errorf("replica roles never settled")
		}
		time.Sleep(200 * time.Millisecond)
	}
	for {
		if s, err := conns[0].do("CLUSTER", "INFO"); err == nil && strings.Contains(string(s.Str), "cluster_state:ok") {
			out("cluster formed: %d primaries, %d replica(s) each\n", nPrim, replicas)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cluster_state never became ok")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// alreadyFormed: all slots covered and at least n nodes known.
func alreadyFormed(addr string, n int) (bool, error) {
	c, err := dial(addr)
	if err != nil {
		return false, err
	}
	defer c.close()
	v, err := c.do("CLUSTER", "INFO")
	if err != nil {
		return false, err
	}
	info := string(v.Str)
	if strings.Contains(info, "cluster_slots_assigned:16384") && !strings.Contains(info, "cluster_known_nodes:1\r") {
		return true, nil
	}
	if !strings.Contains(info, "cluster_slots_assigned:0") {
		return false, fmt.Errorf("%s has some slots assigned but the cluster isn't complete: fix it by hand", addr)
	}
	return false, nil
}

func everyoneKnowsAll(addrs, ids []string) bool {
	for _, a := range addrs {
		nodes, err := clusterNodes(a)
		if err != nil {
			return false
		}
		known := map[string]bool{}
		for _, n := range nodes {
			if !n.has("handshake") {
				known[n.id] = true
			}
		}
		for _, id := range ids {
			if !known[id] {
				return false
			}
		}
	}
	return true
}

func replicasSettled(addrs, replicaIDs []string) bool {
	for _, a := range addrs {
		nodes, err := clusterNodes(a)
		if err != nil {
			return false
		}
		role := map[string]bool{}
		for _, n := range nodes {
			role[n.id] = n.has("slave")
		}
		for _, id := range replicaIDs {
			if !role[id] {
				return false
			}
		}
	}
	return true
}
