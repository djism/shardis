import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { Replayer, markers, slotCounts, type Trace } from './model';

const load = (name: string): Trace => JSON.parse(readFileSync(`public/traces/${name}.json`, 'utf8'));

describe('replaying real traces', () => {
  it('kill-primary: a dies, d takes its slots, a comes back as d’s replica', () => {
    const tr = load('kill-primary');
    const r = new Replayer(tr);
    const start = r.at(0);
    expect(slotCounts(start.owners, start.nodes.length).slice(0, 3)).toEqual([5461, 5462, 5461]);
    const won = tr.events.find((e) => e.type === 'failover_won')!;
    const after = r.at(won.t + 0.01);
    const a = after.nodes.find((n) => n.name === 'a')!, d = after.nodes.find((n) => n.name === 'd')!;
    expect(d.role).toBe('primary');
    expect(a.fail).toBe(true);
    expect(slotCounts(after.owners, after.nodes.length)[3]).toBe(5461); // d now owns a's slots
    // scrubbing backwards rebuilds the earlier state
    const before = r.at(1);
    expect(before.nodes.find((n) => n.name === 'd')!.role).toBe('replica');
  });

  it('reshard-during-failover: g ends up with a quarter of the slots', () => {
    const tr = load('reshard-during-failover');
    const end = new Replayer(tr).at(tr.duration_s);
    const g = end.nodes.findIndex((n) => n.name === 'g');
    expect(slotCounts(end.owners, end.nodes.length)[g]).toBeGreaterThan(4000);
  });

  it('partition: only the minority is flagged; its own suspicions of everyone else don’t count', () => {
    const tr = load('partition-minority-primary');
    const minority = tr.events.find((e) => e.type === 'minority')!;
    const st = new Replayer(tr).at(minority.t + 0.05);
    const a = st.nodes.find((n) => n.name === 'a')!;
    expect(a.minority).toBe(true);
    for (const n of st.nodes.filter((n) => n.name !== 'a')) expect(n.pfail, n.name).toBe(false);
  });

  it('markers are deduplicated and include the kill and the failover', () => {
    const m = markers(load('kill-primary')).map((e) => e.type);
    expect(m).toContain('failover_won');
    expect(m).toContain('chaos');
    expect(m.filter((t) => t === 'fail').length).toBeLessThan(3); // FAIL is broadcast by many nodes
  });
});
