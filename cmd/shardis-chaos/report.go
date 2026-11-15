package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
)

func printTable(w io.Writer, rs []report) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "scenario\tops\terrors\terror rate\tacked writes\tlost (final)\trolled back\tstale reads\tphantoms\tmax stall\tp99\tevents\t")
	for _, r := range rs {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.3f%%\t%d\t%d\t%d\t%d / %d\t%d\t%.0f ms\t%.1f ms\t%d\t\n",
			r.Scenario, r.Ops, r.Errors+r.Unknown, 100*r.ErrorRate, r.AckedWrites, r.LostWrites, r.RolledBack,
			r.StaleReads, r.Reads, r.Phantoms, r.MaxStallMs, r.P99Ms, r.Events)
	}
	tw.Flush()
	for _, r := range rs {
		if len(r.FirstErrors) > 0 {
			fmt.Fprintf(w, "%s first errors: %s\n", r.Scenario, strings.Join(r.FirstErrors, " | "))
		}
		if r.EventsDropped > 0 {
			fmt.Fprintf(w, "%s: %d events dropped by slow subscribers (trace has gaps)\n", r.Scenario, r.EventsDropped)
		}
	}
}

func writeCSV(path string, rs []report) error {
	os.MkdirAll(filepath.Dir(path), 0o755)
	var b strings.Builder
	b.WriteString("scenario,ops,errors,unknown,error_rate,acked_writes,lost_writes,rolled_back_writes,reads,stale_reads,phantoms,max_stall_ms,p99_ms,max_latency_ms,events,events_dropped\n")
	for _, r := range rs {
		fmt.Fprintf(&b, "%s,%d,%d,%d,%.5f,%d,%d,%d,%d,%d,%d,%.0f,%.2f,%.0f,%d,%d\n", r.Scenario, r.Ops, r.Errors, r.Unknown,
			r.ErrorRate, r.AckedWrites, r.LostWrites, r.RolledBack, r.Reads, r.StaleReads, r.Phantoms, r.MaxStallMs, r.P99Ms, r.MaxLatencyMs, r.Events, r.EventsDropped)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
