"""Summarize the resharding timelines: the rebalance window vs the same
window of the no-rebalance control run. python3 summarize.py > summary.txt"""
import csv, statistics as st

phases = {r["system"]: (float(r["rebalance_start_s"]), float(r["rebalance_end_s"])) for r in csv.DictReader(open("phases.csv"))}

def rows(name):
    return [{k: float(v) for k, v in r.items()} for r in csv.DictReader(open(f"{name}-timeline.csv"))]

print("system,run,window_s,median_ops_per_s,min_ops_per_s,intervals_below_20k,median_p99_ms,worst_max_ms,errors,moved,ask")
for system in ("shardis", "redis"):
    lo, hi = phases[system]
    for run in (system, system + "-control"):
        w = [r for r in rows(run) if lo <= r["t_s"] < hi]
        ops = [r["ops_per_s"] for r in w]
        print(f"{system},{'rebalance' if run == system else 'control'},{lo:.1f}-{hi:.1f},{st.median(ops):.0f},{min(ops):.0f},"
              f"{sum(o < 20000 for o in ops)},{st.median(r['p99_ms'] for r in w):.2f},{max(r['max_ms'] for r in w):.0f},"
              f"{sum(r['errors'] for r in rows(run)):.0f},{sum(r['moved'] for r in rows(run)):.0f},{sum(r['ask'] for r in rows(run)):.0f}")
