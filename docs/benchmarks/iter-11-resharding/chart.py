"""Draw chart.svg: throughput over time while rebalancing 3 -> 4 primaries,
for Shardis and Redis, each against its no-rebalance control run.
Standard library only (no matplotlib). python3 chart.py"""
import csv

W, PANEL_H, TOP, LEFT, RIGHT, GAP = 760, 190, 56, 64, 128, 58
X_MAX, Y_MAX = 45.0, 200_000
BLUE, GRAY, INK, INK2, GRID, BAND, BG = "#2a78d6", "#a3a29c", "#0b0b0b", "#52514e", "#e8e7e3", "#eef4fc", "#fcfcfb"

phases = {r["system"]: (float(r["rebalance_start_s"]), float(r["rebalance_end_s"])) for r in csv.DictReader(open("phases.csv"))}

def series(name, smooth=5):
    rows = [(float(r["t_s"]), float(r["ops_per_s"])) for r in csv.DictReader(open(f"{name}-timeline.csv"))]
    # 0.5 s moving average: the raw 100 ms points are noisy enough to hide the shape.
    out = []
    for i in range(len(rows)):
        win = rows[max(0, i - smooth // 2): i + smooth // 2 + 1]
        out.append((rows[i][0], sum(v for _, v in win) / len(win)))
    return out

plot_w = W - LEFT - RIGHT
x = lambda t: LEFT + t / X_MAX * plot_w
svg = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {TOP + 2 * PANEL_H + GAP + 48}" font-family="system-ui, -apple-system, sans-serif">',
       f'<rect width="100%" height="100%" fill="{BG}"/>',
       f'<text x="{LEFT}" y="24" font-size="16" font-weight="600" fill="{INK}">Throughput while adding a 4th primary (4096 slots, ~250k keys move)</text>',
       f'<text x="{LEFT}" y="43" font-size="12" fill="{INK2}">50 clients, 1M keys, ops/s (0.5 s moving average). Shaded: rebalance running. 0 client errors in all four runs.</text>']

for p, (system, label) in enumerate([("shardis", "Shardis (MIGRATE + ASK, ours)"), ("redis", "Redis 8.10 (atomic slot migration)")]):
    top = TOP + p * (PANEL_H + GAP) + 18
    y = lambda v: top + PANEL_H - v / Y_MAX * PANEL_H
    lo, hi = phases[system]
    svg.append(f'<text x="{LEFT}" y="{top - 6}" font-size="13" font-weight="600" fill="{INK}">{label}</text>')
    svg.append(f'<rect x="{x(lo):.1f}" y="{top}" width="{x(hi) - x(lo):.1f}" height="{PANEL_H}" fill="{BAND}"/>')
    svg.append(f'<text x="{(x(lo) + x(hi)) / 2:.1f}" y="{top + 14}" font-size="11" fill="{INK2}" text-anchor="middle">rebalance {hi - lo:.1f} s</text>')
    for v in range(0, Y_MAX + 1, 50_000):
        svg.append(f'<line x1="{LEFT}" x2="{LEFT + plot_w}" y1="{y(v):.1f}" y2="{y(v):.1f}" stroke="{GRID}" stroke-width="1"/>')
        svg.append(f'<text x="{LEFT - 8}" y="{y(v) + 4:.1f}" font-size="11" fill="{INK2}" text-anchor="end">{v // 1000}k</text>')
    for run, color, name in [(system + "-control", GRAY, "no rebalance"), (system, BLUE, "with rebalance")]:
        pts = series(run)
        d = " ".join(f"{x(t):.1f},{y(v):.1f}" for t, v in pts if t <= X_MAX)
        svg.append(f'<polyline points="{d}" fill="none" stroke="{color}" stroke-width="2" stroke-linejoin="round"/>')
        tail = [v for t, v in pts if t > X_MAX - 5]
        ly = y(sum(tail) / len(tail))
        if name == "no rebalance":
            ly -= 8
        else:
            ly += 14
        svg.append(f'<text x="{LEFT + plot_w + 8}" y="{ly:.1f}" font-size="12" fill="{INK}">{name}</text>')
    for t in range(0, int(X_MAX) + 1, 5):
        svg.append(f'<text x="{x(t):.1f}" y="{top + PANEL_H + 16}" font-size="11" fill="{INK2}" text-anchor="middle">{t}s</text>')
svg.append("</svg>")
open("chart.svg", "w").write("\n".join(svg))
print("wrote chart.svg")
