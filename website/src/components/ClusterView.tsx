import { AnimatePresence, motion } from 'framer-motion';
import { useMemo } from 'react';
import { slotCounts, type ClusterState, type NodeState, type TraceEvent } from '../lib/model';

// The cluster, drawn so that change is easy to see: each primary/replica
// PAIR has a fixed column (a above d, b above e, ...), and the role is the
// fill. A failover is the fill moving down the column, not nodes jumping.

const W = 640, H = 330, TOP = 112, BOTTOM = 238, R = 30;

export const SHARD_VARS = ['var(--shard-1)', 'var(--shard-2)', 'var(--shard-3)', 'var(--shard-4)'];

export interface Layout {
  pos: Map<string, { x: number; y: number; column: number }>;
  columns: number;
}

/** Columns from the starting topology: each primary with its replicas, then extra nodes. */
export function layoutFor(nodes: { name: string; role: string; primary?: string }[]): Layout {
  const prims = nodes.filter((n) => n.role === 'primary').map((n) => n.name);
  const extras = nodes.filter((n) => n.role !== 'primary' && n.role !== 'replica').map((n) => n.name);
  const cols = [...prims, ...extras];
  const pos = new Map<string, { x: number; y: number; column: number }>();
  const x = (i: number) => (W / (cols.length + 1)) * (i + 1);
  cols.forEach((name, i) => pos.set(name, { x: x(i), y: TOP, column: i }));
  nodes.filter((n) => n.role === 'replica').forEach((n) => {
    const p = pos.get(n.primary ?? '');
    if (p) pos.set(n.name, { x: p.x, y: BOTTOM, column: p.column });
  });
  return { pos, columns: cols.length };
}

function status(n: NodeState): { label: string; tone: 'critical' | 'warning' | null } {
  if (n.down) return { label: 'DOWN', tone: 'critical' };
  if (n.fail) return { label: 'FAIL', tone: 'critical' };
  if (n.minority) return { label: 'MINORITY', tone: 'warning' };
  if (n.partitioned) return { label: 'CUT OFF', tone: 'warning' };
  if (n.pfail) return { label: 'SUSPECTED', tone: 'warning' };
  return { label: '', tone: null };
}

/** For a node without slots: replica, empty, or a primary that lost its slots (dead, replaced). */
function label(n: NodeState): string {
  return n.role === 'replica' ? 'replica' : n.role === 'empty' ? 'empty' : 'no slots';
}

interface Props {
  state: ClusterState;
  layout: Layout;
  gossip: TraceEvent[]; // gossip_send events in the last ~0.3 s
  traffic: { ok: number; fail: number }; // client ops/s right now
  now: number; // seconds, for animation keys
  motionOK: boolean;
  liveOps: boolean; // Live mode: ops per node are measured, not inferred
}

export function ClusterView({ state, layout, gossip, traffic, now, motionOK, liveOps }: Props) {
  const counts = useMemo(() => slotCounts(state.owners, state.nodes.length), [state]);
  const serving = state.nodes.map((n, i) => ({ n, i, slots: counts[i] })).filter((x) => x.slots > 0 && !x.n.down && !x.n.fail);
  const totalSlots = serving.reduce((a, x) => a + x.slots, 0) || 1;

  // Traffic dots. Replay: the recorded ops/s, split across serving primaries by
  // the share of slots they own (keys are uniform; ADR 049). Live: each node's
  // measured ops/s.
  const dots: { key: string; x: number; y: number }[] = [];
  if (motionOK) {
    const slotNow = Math.floor(now * 8); // a new wave 8 times a second
    serving.forEach(({ n, slots }) => {
      const p = layout.pos.get(n.name);
      if (!p) return;
      const rate = liveOps ? (n.ops ?? 0) : (traffic.ok * slots) / totalSlots;
      const perWave = Math.min(4, Math.round(rate / 4000)); // 1 dot ≈ 4k ops/s, capped
      for (let k = 0; k < perWave; k++) dots.push({ key: `${n.name}-${slotNow}-${k}`, x: p.x + (k - perWave / 2) * 9, y: p.y });
    });
  }

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-auto" role="img"
      aria-label={`Cluster: ${state.nodes.map((n) => `${n.name} ${n.role}${status(n).label ? ' ' + status(n).label : ''}`).join(', ')}`}>
      {/* clients */}
      <rect x={40} y={14} width={W - 80} height={28} rx={6} fill="var(--sunken)" stroke="var(--line)" />
      <text x={W / 2} y={33} textAnchor="middle" fontSize={12} fill="var(--ink-2)">
        clients{traffic.fail > 0 ? ` — ${Math.round(traffic.fail).toLocaleString()} errors/s` : ''}
      </text>
      {/* replication links */}
      {state.nodes.map((n) => {
        if (n.role !== 'replica' || !n.primary) return null;
        const a = layout.pos.get(n.name), b = layout.pos.get(n.primary);
        if (!a || !b) return null;
        return <line key={`r-${n.name}`} x1={b.x} y1={b.y + R} x2={a.x} y2={a.y - R} stroke="var(--line-2)" strokeWidth={2} strokeDasharray="4 4" />;
      })}
      {/* traffic: falls from the client bar into primaries */}
      <AnimatePresence>
        {dots.map((d) => (
          <motion.circle key={d.key} r={3} fill="var(--ink-3)" initial={{ cx: d.x, cy: 44, opacity: 0.9 }}
            animate={{ cx: d.x, cy: d.y - R - 2, opacity: 0.9 }} exit={{ opacity: 0 }} transition={{ duration: 0.35, ease: 'linear' }} />
        ))}
      </AnimatePresence>
      {/* gossip: a dot from sender to receiver */}
      {motionOK && gossip.map((e, i) => {
        const a = layout.pos.get(e.node), b = layout.pos.get(String(e.data?.to ?? ''));
        if (!a || !b) return null;
        const p = Math.min(Math.max((now - e.t) / 0.3, 0), 1);
        return <circle key={`g-${i}-${e.t}`} r={2.5} fill="var(--gossip)" cx={a.x + (b.x - a.x) * p} cy={a.y + (b.y - a.y) * p} opacity={0.8} />;
      })}
      {/* nodes */}
      {state.nodes.map((n, i) => {
        const p = layout.pos.get(n.name);
        if (!p) return null;
        const color = SHARD_VARS[p.column % SHARD_VARS.length];
        const st = status(n);
        const primary = counts[i] > 0;
        const dead = n.down;
        return (
          <g key={n.name} opacity={dead ? 0.45 : 1}>
            {/* plain fill + CSS transition: Framer Motion can't interpolate CSS variables */}
            <circle cx={p.x} cy={p.y} r={R}
              style={{ fill: primary ? color : 'var(--panel)', transition: motionOK ? 'fill 400ms ease, stroke 300ms ease' : undefined }}
              stroke={st.tone === 'critical' ? 'var(--critical)' : st.tone === 'warning' ? 'var(--warning)' : color}
              strokeWidth={st.tone ? 3 : 2} strokeDasharray={dead ? '5 4' : undefined} />
            <text x={p.x} y={p.y + 6} textAnchor="middle" fontWeight={600} fill={primary ? '#fff' : 'var(--ink)'} className="cv-name">{n.name}</text>
            {/* long label on wide screens, short and larger on phones (index.css) */}
            <text x={p.x} y={p.y + R + 16} textAnchor="middle" fill="var(--ink-2)" className="tabular cv-long">
              {primary ? `primary · ${counts[i].toLocaleString()} slots` : label(n)}
            </text>
            <text x={p.x} y={p.y + R + 24} textAnchor="middle" fill="var(--ink-2)" className="tabular cv-short">
              {primary ? counts[i].toLocaleString() : label(n)}
            </text>
            {st.label && (
              <g>
                <rect x={p.x - 38} y={p.y - R - 24} width={76} height={18} rx={9}
                  fill={st.tone === 'critical' ? 'var(--critical)' : 'var(--warning)'} />
                <text x={p.x} y={p.y - R - 11} textAnchor="middle" fontSize={10} fontWeight={700} fill={st.tone === 'critical' ? '#fff' : '#0b0b0b'}>{st.label}</text>
              </g>
            )}
            {liveOps && n.ops !== undefined && (
              <text x={p.x} y={p.y + R + 30} textAnchor="middle" fontSize={10} fill="var(--ink-3)" className="tabular">{Math.round(n.ops).toLocaleString()} ops/s</text>
            )}
          </g>
        );
      })}
    </svg>
  );
}
