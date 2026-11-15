#!/usr/bin/env python3
"""Draws gossip.svg from raw.csv: two small charts side by side (not one chart
with two y-axes): convergence time vs cluster size, and bus messages per node
per second vs cluster size. Each is a single series, so the title names it
and no legend is needed. Hover a point for its numbers.

Usage (from repo root): python3 docs/benchmarks/iter-08-gossip/chart.py
"""
import csv
import os
from html import escape

HERE = os.path.dirname(os.path.abspath(__file__))
rows = list(csv.DictReader(l for l in open(os.path.join(HERE, "raw.csv")) if not l.startswith("#")))
sizes = sorted({int(r["nodes"]) for r in rows})
conv = {n: [int(r["converge_ms"]) / 1000 for r in rows if int(r["nodes"]) == n] for n in sizes}
msgs = {n: next(float(r["msgs_per_node_per_s"]) for r in rows if int(r["nodes"]) == n and r["run"] == "1") for n in sizes}
kbs = {n: next(float(r["kbytes_per_node_per_s"]) for r in rows if int(r["nodes"]) == n and r["run"] == "1") for n in sizes}

W, H, PW, PH, TOP, LEFT = 760, 370, 300, 200, 110, 60

def panel(x0, title, ymax, yticks, ylabel, points, ranges=None):
    out = [f'<text class="ptitle" x="{x0}" y="{TOP-28}">{escape(title)}</text>']
    xs = lambda n: x0 + LEFT + (n - 0) / (sizes[-1] + 5) * (PW - LEFT)
    ys = lambda v: TOP + PH - v / ymax * PH
    for v in yticks:
        out.append(f'<line class="grid" x1="{x0+LEFT}" x2="{x0+PW}" y1="{ys(v):.1f}" y2="{ys(v):.1f}"/>')
        out.append(f'<text class="tick" x="{x0+LEFT-8}" y="{ys(v)+4:.1f}" text-anchor="end">{v:g}</text>')
    for n in sizes:
        out.append(f'<text class="tick" x="{xs(n):.1f}" y="{TOP+PH+18}" text-anchor="middle">{n}</text>')
    out.append(f'<text class="tick" x="{x0+LEFT+(PW-LEFT)/2:.1f}" y="{TOP+PH+38}" text-anchor="middle">nodes in the cluster</text>')
    out.append(f'<text class="tick" x="{x0+12}" y="{TOP+PH/2:.1f}" transform="rotate(-90 {x0+12} {TOP+PH/2:.1f})" text-anchor="middle">{escape(ylabel)}</text>')
    path = " ".join(f'{"M" if i == 0 else "L"}{xs(n):.1f},{ys(v):.1f}' for i, (n, v, _) in enumerate(points))
    out.append(f'<path class="line" d="{path}"/>')
    for n, v, tip in points:
        if ranges:  # min-max whisker for runs
            lo, hi = ranges[n]
            out.append(f'<line class="whisker" x1="{xs(n):.1f}" x2="{xs(n):.1f}" y1="{ys(lo):.1f}" y2="{ys(hi):.1f}"/>')
        out.append(f'<circle class="dot" cx="{xs(n):.1f}" cy="{ys(v):.1f}" r="5"><title>{escape(tip)}</title></circle>')
        out.append(f'<text class="val" x="{xs(n)+9:.1f}" y="{ys(v)-8:.1f}">{v:.1f}</text>')
    return out

med = lambda xs: sorted(xs)[len(xs) // 2]
out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-labelledby="t d" font-family="-apple-system, Segoe UI, Helvetica, Arial, sans-serif">',
       '<title id="t">Gossip convergence and bus overhead vs cluster size</title>',
       '<desc id="d">' + "; ".join(f"{n} nodes: converged in {med(conv[n]):.1f}s (median of 3), {msgs[n]:.1f} messages per node per second" for n in sizes) + '</desc>',
       '''<style>
  svg { --surface:#fcfcfb; --text:#0b0b0b; --text2:#52514e; --grid:#e6e5e0; --s1:#2a78d6; }
  @media (prefers-color-scheme: dark) { svg { --surface:#1a1a19; --text:#f0efec; --text2:#c3c2b7; --grid:#353431; --s1:#3987e5; } }
  .bg { fill: var(--surface); }
  .title { fill: var(--text); font-size: 16px; font-weight: 600; }
  .sub, .tick { fill: var(--text2); font-size: 12px; }
  .ptitle { fill: var(--text); font-size: 13px; font-weight: 600; }
  .val { fill: var(--text); font-size: 12px; font-weight: 600; }
  .grid { stroke: var(--grid); stroke-width: 1; }
  .line { stroke: var(--s1); stroke-width: 2; fill: none; }
  .whisker { stroke: var(--s1); stroke-width: 2; opacity: .45; }
  .dot { fill: var(--s1); stroke: var(--surface); stroke-width: 2; }
  .dot:hover { stroke: var(--text); }
</style>''',
       f'<rect class="bg" width="{W}" height="{H}"/>',
       '<text class="title" x="16" y="26">Gossip: how fast it spreads, what it costs</text>',
       '<text class="sub" x="16" y="44">Nodes MEET\'d in a chain (worst case), node timeout 15 s. Whiskers: min–max of 3 runs.</text>']
out += panel(20, "Time until every node knows every node", 18, [0, 5, 10, 15], "seconds",
             [(n, med(conv[n]), f"{n} nodes: {med(conv[n]):.1f}s median, {min(conv[n]):.1f}-{max(conv[n]):.1f}s over 3 runs") for n in sizes],
             {n: (min(conv[n]), max(conv[n])) for n in sizes})
out += panel(400, "Bus messages sent per node per second", 16, [0, 5, 10, 15], "messages / s",
             [(n, msgs[n], f"{n} nodes: {msgs[n]:.1f} msgs/s, {kbs[n]:.1f} KB/s per node") for n in sizes])
out.append('</svg>')
open(os.path.join(HERE, "gossip.svg"), "w").write("\n".join(out) + "\n")
print("wrote gossip.svg")
