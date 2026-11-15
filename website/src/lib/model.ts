// The cluster as the page draws it, the same shape in Replay mode (rebuilt
// from a recorded trace) and Live mode (from shardis-observer).

export type Role = 'primary' | 'replica' | 'empty';

export interface NodeState {
  name: string;
  role: Role;
  primary?: string; // for replicas
  pfail: boolean; // some node currently suspects it
  fail: boolean; // the majority marked it FAIL
  down: boolean; // the process is dead (kill) or frozen (pause)
  minority: boolean; // it can't reach a majority (ADR 030)
  partitioned: boolean; // cut off by a partition
  ops?: number; // commands/s (Live mode only: real per-node rate)
}

export interface ClusterState {
  nodes: NodeState[];
  owners: Uint8Array; // slot → index into nodes (255 = unowned)
  epoch: number;
  // Who suspects whom (PFAIL): suspect → suspecters. A node is shown as
  // suspected only if a healthy node suspects it: a node cut off from the
  // cluster suspects everyone, and that's its view, not a fact.
  suspects?: Map<string, Set<string>>;
}

export interface TraceEvent {
  t: number;
  node: string;
  type: string;
  data?: Record<string, unknown>;
}

export interface Trace {
  scenario: string;
  description: string;
  recorded: string;
  duration_s: number;
  nodes: { name: string; role: string; primary?: string; slots?: string }[];
  events: TraceEvent[];
  timeline: { t: number; ok: number; fail: number }[];
  report: Record<string, unknown>;
}

export const UNOWNED = 255;

/** The state at the start of a trace: the seed topology. */
export function initialState(trace: Trace): ClusterState {
  const nodes: NodeState[] = trace.nodes.map((n) => ({
    name: n.name,
    role: (n.role === 'primary' || n.role === 'replica' ? n.role : 'empty') as Role,
    primary: n.primary,
    pfail: false,
    fail: false,
    down: false,
    minority: false,
    partitioned: false,
  }));
  const owners = new Uint8Array(16384).fill(UNOWNED);
  trace.nodes.forEach((n, i) => {
    for (const r of (n.slots ?? '').split(/\s+/).filter(Boolean)) {
      const [a, b] = r.split('-').map(Number);
      owners.fill(i, a, (b ?? a) + 1);
    }
  });
  return { nodes, owners, epoch: 0, suspects: new Map() };
}

function clone(s: ClusterState): ClusterState {
  const suspects = new Map<string, Set<string>>();
  for (const [k, v] of s.suspects ?? []) suspects.set(k, new Set(v));
  return { nodes: s.nodes.map((n) => ({ ...n })), owners: s.owners.slice(), epoch: s.epoch, suspects };
}

/** Recomputes each node's pfail flag from the suspicions of healthy nodes. */
export function settleSuspicion(state: ClusterState): void {
  const healthy = new Set(state.nodes.filter((n) => !n.down && !n.fail && !n.minority && !n.partitioned).map((n) => n.name));
  for (const n of state.nodes) {
    const by = state.suspects?.get(n.name);
    n.pfail = !!by && [...by].some((x) => healthy.has(x));
  }
}

/**
 * Nodes are named in the seed (a..f). A node added at runtime (g, in the
 * resharding scenario) has no name, so the cluster labels it with the first
 * 8 hex chars of its ID. We map such labels to the one unnamed node.
 */
function resolver(state: ClusterState): (label: unknown) => number {
  const byName = new Map(state.nodes.map((n, i) => [n.name, i]));
  const extra = state.nodes.findIndex((n) => n.role === 'empty');
  return (label) => {
    if (typeof label !== 'string') return -1;
    const i = byName.get(label);
    if (i !== undefined) return i;
    return /^[0-9a-f]{8}$/.test(label) ? extra : -1;
  };
}

/** Applies one event to a state (mutates it). */
export function apply(state: ClusterState, e: TraceEvent, idx: (label: unknown) => number): void {
  const d = e.data ?? {};
  const at = (label: unknown) => state.nodes[idx(label)];
  switch (e.type) {
    case 'chaos': {
      const n = at(e.node);
      switch (d.action) {
        case 'kill':
          if (n) n.down = true;
          break;
        case 'pause':
          if (n) n.down = true;
          break;
        case 'start':
        case 'resume':
          if (n) n.down = false;
          break;
        case 'partition':
          for (const s of (d.side as string[]) ?? []) if (at(s)) at(s)!.partitioned = true;
          break;
        case 'heal':
          state.nodes.forEach((x) => (x.partitioned = false));
          break;
      }
      break;
    }
    case 'pfail': {
      const n = at(d.node), by = at(d.by ?? e.node);
      if (n && by) {
        if (!state.suspects) state.suspects = new Map();
        if (!state.suspects.has(n.name)) state.suspects.set(n.name, new Set());
        state.suspects.get(n.name)!.add(by.name);
      }
      break;
    }
    case 'fail':
      if (at(d.node)) at(d.node)!.fail = true;
      break;
    case 'fail_cleared':
      if (at(d.node)) {
        Object.assign(at(d.node)!, { fail: false, pfail: false });
        state.suspects?.delete(at(d.node)!.name);
      }
      break;
    case 'minority':
      if (at(d.node ?? e.node)) at(d.node ?? e.node)!.minority = true;
      break;
    case 'rejoined':
      if (at(d.node ?? e.node)) at(d.node ?? e.node)!.minority = false;
      break;
    case 'failover_won':
    case 'failover_takeover': {
      const w = idx(d.node), old = idx(d.old_primary);
      if (w < 0) break;
      state.nodes[w].role = 'primary';
      state.nodes[w].primary = undefined;
      if (old >= 0) for (let s = 0; s < state.owners.length; s++) if (state.owners[s] === old) state.owners[s] = w;
      if (typeof d.epoch === 'number') state.epoch = Math.max(state.epoch, d.epoch);
      break;
    }
    case 'demoted': {
      const n = at(d.node);
      if (n) {
        Object.assign(n, { role: 'replica', primary: state.nodes[idx(d.new_primary)]?.name, fail: false, pfail: false, minority: false });
        state.suspects?.delete(n.name);
      }
      break;
    }
    case 'slot_owner': {
      const o = idx(d.owner);
      if (o >= 0 && typeof d.slot === 'number') {
        state.owners[d.slot] = o;
        if (state.nodes[o].role === 'empty') state.nodes[o].role = 'primary';
      }
      break;
    }
    case 'repl_role':
      // A node that became a replica again (e.g. after a restart) is no longer suspected.
      break;
  }
  // A node seen speaking again is back: gossip from it clears our "down" guess.
  if (e.type === 'gossip_send' || e.type === 'gossip_recv') {
    const n = at(e.node);
    if (n && n.down && !n.fail) n.down = false;
  }
  // The server clears PFAIL when it hears from the suspect again (no event for
  // that), so we do the same: receiving gossip from X ends our suspicion of X.
  if (e.type === 'gossip_recv') {
    const from = at(d.from), me = at(e.node);
    if (from && me) state.suspects?.get(from.name)?.delete(me.name);
  }
}

/** Replays a trace to any time t, incrementally (forward) or from scratch (backward). */
export class Replayer {
  private trace: Trace;
  private base: ClusterState;
  private state: ClusterState;
  private cursor = 0;
  private idx: (label: unknown) => number;
  constructor(trace: Trace) {
    this.trace = trace;
    this.base = initialState(trace);
    this.state = clone(this.base);
    this.idx = resolver(this.base);
  }
  at(t: number): ClusterState {
    const ev = this.trace.events;
    if (this.cursor > 0 && ev[this.cursor - 1].t > t) {
      this.state = clone(this.base);
      this.cursor = 0;
    }
    while (this.cursor < ev.length && ev[this.cursor].t <= t) apply(this.state, ev[this.cursor++], this.idx);
    settleSuspicion(this.state);
    return this.state;
  }
}

/** Events worth stopping at with "step": what the chaos tool did and what the cluster decided. */
export const KEY_EVENTS = new Set(['chaos', 'fail', 'election_start', 'failover_won', 'failover_takeover', 'demoted', 'minority', 'rejoined', 'fail_cleared']);

export function markers(trace: Trace): TraceEvent[] {
  const seen = new Set<string>();
  return trace.events.filter((e) => {
    if (!KEY_EVENTS.has(e.type)) return false;
    // one marker per (type, subject) burst: FAIL is broadcast by every node
    const key = `${e.type}:${String(e.data?.node ?? e.data?.action ?? e.node)}:${Math.floor(e.t)}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/** Client operations per second at time t (from the recorded 100 ms timeline). */
export function opsAt(trace: Trace, t: number): { ok: number; fail: number } {
  const b = trace.timeline[Math.min(Math.max(Math.floor(t * 10), 0), trace.timeline.length - 1)];
  return b ? { ok: b.ok * 10, fail: b.fail * 10 } : { ok: 0, fail: 0 };
}

/** Slots owned per node index. */
export function slotCounts(owners: Uint8Array, n: number): number[] {
  const c = new Array<number>(n).fill(0);
  for (const o of owners) if (o !== UNOWNED) c[o]++;
  return c;
}

/** Events with from < t <= to (events are sorted by t): binary search, then scan. */
export function eventsBetween(events: TraceEvent[], from: number, to: number): TraceEvent[] {
  let lo = 0, hi = events.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (events[mid].t <= from) lo = mid + 1;
    else hi = mid;
  }
  const out: TraceEvent[] = [];
  for (let i = lo; i < events.length && events[i].t <= to; i++) out.push(events[i]);
  return out;
}

/** One line of plain English for a key event (the event log). */
export function describe(e: TraceEvent): string | null {
  const d = e.data ?? {};
  const s = (v: unknown) => String(v ?? '?');
  switch (e.type) {
    case 'chaos':
      switch (d.action) {
        case 'kill': return `${e.node} was killed (kill -9)`;
        case 'start': return `${e.node} restarted`;
        case 'pause': return `${e.node} froze (SIGSTOP)`;
        case 'resume': return `${e.node} resumed`;
        case 'partition': return `network partition: ${((d.side as string[]) ?? []).join(', ')} cut off`;
        case 'heal': return 'partition healed';
        case 'fix': return 'shardis-admin fix: finishing half-moved slots';
        case 'rebalance_start': return 'rebalance started';
        case 'rebalance_stopped': return 'rebalance stopped (a source died)';
        case 'rebalance_done': return 'rebalance finished';
        case 'scenario_start': return `fault injection begins`;
        case 'recovered': return 'cluster recovered';
        default: return null;
      }
    case 'pfail': return `${s(d.by)} suspects ${s(d.node)} (PFAIL: no reply within the node timeout)`;
    case 'fail': return `${s(d.by)}: ${s(d.node)} is FAIL (a majority of primaries agree)`;
    case 'fail_cleared': return `${s(d.node)} is reachable again (FAIL cleared)`;
    case 'election_start': return `${s(d.node)} asks for votes to replace ${s(d.primary)} (epoch ${s(d.epoch)})`;
    case 'failover_won': return `${s(d.node)} won the election: now primary of ${s(d.slots)} slots (epoch ${s(d.epoch)}, ${s(d.votes)} votes)`;
    case 'failover_takeover': return `${s(d.node)} took over ${s(d.old_primary)}'s slots (graceful handoff)`;
    case 'demoted': return `${s(d.node)} found it was replaced: now a replica of ${s(d.new_primary)}`;
    case 'minority': return `${s(d.node ?? e.node)} can't reach a majority: refusing writes`;
    case 'rejoined': return `${s(d.node ?? e.node)} reached the majority again`;
    default: return null;
  }
}
