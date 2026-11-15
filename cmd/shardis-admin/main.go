// Command shardis-admin changes a running cluster: add or remove nodes, and
// move hash slots between primaries while clients keep working (live
// resharding, ADR 032). The logic lives in internal/admin, which the crash
// tool and tests use too.
//
//	shardis-admin add-node    -addr 127.0.0.1:7000 -new 127.0.0.1:7006 [-replica-of <primary-id>]
//	shardis-admin reshard     -addr 127.0.0.1:7000 -from <id> -to <id> -slots 1000
//	shardis-admin rebalance   -addr 127.0.0.1:7000      (even out slots, including empty primaries)
//	shardis-admin remove-node -addr 127.0.0.1:7000 -id <id>   (the node must own no slots)
//	shardis-admin fix         -addr 127.0.0.1:7000      (finish slots a crashed run left half-moved)
//	shardis-admin nodes       -addr 127.0.0.1:7000      (who owns how many slots)
//
// It works on real Redis Cluster too: it only uses standard commands.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/djism/shardis/internal/admin"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7000", "any node of the cluster")
	o := admin.MoveOptions{Out: os.Stderr}
	fs.IntVar(&o.Batch, "batch", 100, "keys per MIGRATE")
	fs.IntVar(&o.TimeoutMs, "migrate-timeout", 10000, "MIGRATE timeout (ms)")
	fs.BoolVar(&o.Verbose, "v", false, "print every slot moved")
	newNode := fs.String("new", "", "add-node: the new node's address")
	replicaOf := fs.String("replica-of", "", "add-node: make the new node a replica of this primary ID")
	from := fs.String("from", "", "reshard: source primary ID")
	to := fs.String("to", "", "reshard: target primary ID")
	slots := fs.Int("slots", 0, "reshard: how many slots to move")
	id := fs.String("id", "", "remove-node: the node to remove")
	fs.Parse(args)

	start := time.Now()
	var err error
	switch cmd {
	case "add-node":
		var newID string
		if newID, err = admin.AddNode(*addr, *newNode, *replicaOf); err == nil {
			fmt.Printf("added %s (%s)\n", *newNode, newID)
		}
	case "reshard":
		err = admin.Reshard(*addr, *from, *to, *slots, o)
	case "rebalance":
		err = admin.Rebalance(*addr, o)
	case "remove-node":
		if err = admin.RemoveNode(*addr, *id); err == nil {
			fmt.Println("removed; stop its process now")
		}
	case "fix":
		err = admin.Fix(*addr, o)
	case "nodes":
		err = admin.PrintNodes(*addr, os.Stdout)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "shardis-admin:", err)
		os.Exit(1)
	}
	if cmd != "nodes" {
		fmt.Fprintf(os.Stderr, "done in %v\n", time.Since(start).Round(time.Millisecond))
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: shardis-admin add-node|reshard|rebalance|remove-node|fix|nodes [flags]  (-h for flags)")
	os.Exit(2)
}
