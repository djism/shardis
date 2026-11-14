#!/usr/bin/env python3
"""Draws hit-rate.svg from raw.csv: a dot plot of hit rate per eviction setup.

Usage (from repo root): python3 docs/benchmarks/iter-04-eviction/chart.py

A dot plot rather than bars: all values sit between 73% and 78%, and bars
must start at zero (which would hide the differences). Dots encode by
position, so a 70-80% axis is honest as long as it's labeled as such.
Colors follow the system (Shardis / Redis), with a legend and direct labels;
light and dark mode each have their own steps. Hover a dot for details.
"""
import csv
import os
from html import escape

HERE = os.path.dirname(os.path.abspath(__file__))
rows = [r for r in csv.DictReader(l for l in open(os.path.join(HERE, "raw.csv")) if not l.startswith("#"))
        if "calibration" not in r["label"]]

# Order: Shardis first, then Redis; within each, worst to best policy reading top-down.
order = ["allkeys-random", "allkeys-lru (5 samples)", "allkeys-lru (10 samples)",
         "exact LRU (same maxmemory)", "exact LRU (same keys)"]
def key(r):
    system, policy = r["label"].split(" ", 1)
    return (system != "shardis", order.index(policy))
rows.sort(key=key)

W, LEFT, RIGHT, TOP, ROW = 760, 300, 150, 70, 34
H = TOP + ROW * len(rows) + 50
LO, HI = 70.0, 80.0
plot_w = W - LEFT - RIGHT
def x(v): return LEFT + (v - LO) / (HI - LO) * plot_w

out = []
out.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
           'role="img" aria-labelledby="t d" font-family="-apple-system, Segoe UI, Helvetica, Arial, sans-serif">')
out.append('<title id="t">Cache hit rate by eviction policy</title>')
out.append('<desc id="d">Dot plot. 1M keys, Zipf 0.99, about 10% of keys fit in memory. '
           + "; ".join(f'{escape(r["label"])}: {100*float(r["hit_rate"]):.1f}%' for r in rows) + '</desc>')
out.append('''<style>
  svg { --surface:#fcfcfb; --text:#0b0b0b; --text2:#52514e; --grid:#e6e5e0; --s1:#2a78d6; --s2:#eb6834; }
  @media (prefers-color-scheme: dark) {
    svg { --surface:#1a1a19; --text:#f0efec; --text2:#c3c2b7; --grid:#353431; --s1:#3987e5; --s2:#d95926; }
  }
  .bg { fill: var(--surface); }
  .title { fill: var(--text); font-size: 16px; font-weight: 600; }
  .sub, .tick, .keys { fill: var(--text2); font-size: 12px; }
  .lab { fill: var(--text); font-size: 13px; }
  .val { fill: var(--text); font-size: 12px; font-weight: 600; }
  .grid { stroke: var(--grid); stroke-width: 1; }
  .shardis { fill: var(--s1); } .redis { fill: var(--s2); }
  .dot { stroke: var(--surface); stroke-width: 2; }
  .dot:hover { stroke: var(--text); }
</style>''')
out.append(f'<rect class="bg" width="{W}" height="{H}"/>')
out.append('<text class="title" x="16" y="26">Cache hit rate by eviction policy</text>')
out.append('<text class="sub" x="16" y="46">1M keys, Zipf θ=0.99, 100 B values, ~10% of keys fit. Axis starts at 70%.</text>')

# Legend (two series → always shown, plus direct value labels).
lx = W - RIGHT - 150
for i, (name, cls) in enumerate([("Shardis", "shardis"), ("Redis 8.10", "redis")]):
    cx = lx + i * 85
    out.append(f'<circle class="{cls}" cx="{cx}" cy="22" r="5"/><text class="sub" x="{cx+10}" y="26">{name}</text>')

# Gridlines + ticks.
base = TOP + ROW * len(rows)
for v in range(70, 81, 2):
    out.append(f'<line class="grid" x1="{x(v):.1f}" x2="{x(v):.1f}" y1="{TOP-8}" y2="{base}"/>')
    out.append(f'<text class="tick" x="{x(v):.1f}" y="{base+18}" text-anchor="middle">{v}%</text>')
out.append(f'<text class="tick" x="{LEFT + plot_w/2:.1f}" y="{base+38}" text-anchor="middle">hit rate (higher is better)</text>')
out.append(f'<text class="tick" x="{W-RIGHT+16}" y="{TOP-14}">keys in memory</text>')

for i, r in enumerate(rows):
    cy = TOP + ROW * i + ROW / 2
    system, policy = r["label"].split(" ", 1)
    cls = "shardis" if system == "shardis" else "redis"
    hr = 100 * float(r["hit_rate"])
    keys = int(r["resident_keys"])
    mem = int(r["used_memory"]) / 2**20
    name = ("Shardis " if cls == "shardis" else "Redis ") + policy
    tip = (f"{name}: {hr:.2f}% hit rate, {keys:,} keys resident, {mem:.1f} MB used, "
           f"{int(r['evicted_keys']):,} evicted, GET p99 {r['p99_ms']} ms")
    out.append(f'<text class="lab" x="{LEFT-14}" y="{cy+4:.1f}" text-anchor="end">{escape(name)}</text>')
    out.append(f'<circle class="dot {cls}" cx="{x(hr):.1f}" cy="{cy:.1f}" r="6"><title>{escape(tip)}</title></circle>')
    out.append(f'<text class="val" x="{x(hr)+12:.1f}" y="{cy+4:.1f}">{hr:.1f}%</text>')
    out.append(f'<text class="keys" x="{W-RIGHT+16}" y="{cy+4:.1f}">{keys/1000:.0f}k</text>')
    if i == 4:  # divider between Shardis and Redis rows
        out.append(f'<line class="grid" x1="16" x2="{W-16}" y1="{cy+ROW/2:.1f}" y2="{cy+ROW/2:.1f}"/>')

out.append('</svg>')
with open(os.path.join(HERE, "hit-rate.svg"), "w") as f:
    f.write("\n".join(out) + "\n")
print("wrote hit-rate.svg")
