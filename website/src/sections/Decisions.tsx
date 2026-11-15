import { useMemo, useState } from 'react';
import decisions from '../data/decisions.json';

// Every design decision record, generated from docs/decisions at build time
// (ADR 051). Search, then open a card for the reasoning.

interface Rec { n: number; title: string; context?: string; decision?: string; why?: string; tradeoffs?: string; redis?: string; scale?: string }

const THEMES: { name: string; color: string; range: [number, number] }[] = [
  { name: 'Protocol & store', color: 'var(--a)', range: [1, 13] },
  { name: 'Persistence & replication', color: 'var(--c)', range: [14, 20] },
  { name: 'Cluster & failover', color: 'var(--b)', range: [21, 31] },
  { name: 'Resharding & client', color: 'var(--sun)', range: [32, 39] },
  { name: 'Proving & operating', color: 'var(--sky)', range: [40, 99] },
];
const themeOf = (n: number) => THEMES.find((t) => n >= t.range[0] && n <= t.range[1]) ?? THEMES[0];

export function Decisions() {
  const [q, setQ] = useState('');
  const [open, setOpen] = useState<number | null>(33);
  const recs = decisions as Rec[];
  const shown = useMemo(() => {
    const s = q.trim().toLowerCase();
    return s ? recs.filter((r) => `${r.title} ${r.decision} ${r.why}`.toLowerCase().includes(s)) : recs;
  }, [q, recs]);
  const cur = recs.find((r) => r.n === open);
  return (
    <div className="mt-10 grid gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.1fr)]">
      <div className="min-w-0 flex flex-col gap-3">
        <label className="text-sm text-ink-3" htmlFor="adrSearch">Search {recs.length} decisions</label>
        <input id="adrSearch" className="field" placeholder="failover, LRU, retries, kubernetes…" value={q} onChange={(e) => setQ(e.target.value)} />
        <div className="flex flex-wrap gap-2">
          {THEMES.map((t) => <span key={t.name} className="chip"><span className="size-2 rounded-full" style={{ background: t.color }} />{t.name}</span>)}
        </div>
        <ul className="m-0 p-0 list-none flex flex-col gap-1.5 max-h-[520px] overflow-auto pr-1">
          {shown.map((r) => (
            <li key={r.n}>
              <button type="button" onClick={() => setOpen(r.n)}
                className="w-full text-left rounded-2xl px-4 py-3 flex gap-3 items-baseline transition-colors"
                style={{ background: open === r.n ? 'var(--bg3)' : 'transparent', color: 'var(--fg)' }}>
                <span className="mono text-xs shrink-0" style={{ color: themeOf(r.n).color }}>{String(r.n).padStart(3, '0')}</span>
                <span className="text-[15px] leading-snug">{r.title}</span>
              </button>
            </li>
          ))}
          {shown.length === 0 && <li className="text-ink-3 text-sm px-4">No decision mentions “{q}”.</li>}
        </ul>
      </div>
      {cur && (
        <article className="panel flex flex-col gap-4" style={{ boxShadow: `inset 0 0 0 1.5px ${themeOf(cur.n).color}` }}>
          <p className="mono text-xs" style={{ color: themeOf(cur.n).color }}>ADR {String(cur.n).padStart(3, '0')} · {themeOf(cur.n).name}</p>
          <h3 className="text-[28px] leading-tight">{cur.title}</h3>
          {[['The problem', cur.context], ['Decision', cur.decision], ['Why', cur.why], ['Tradeoffs we accept', cur.tradeoffs], ['What real Redis does', cur.redis], ['At bigger scale', cur.scale]]
            .filter(([, v]) => v)
            .map(([k, v]) => (
              <div key={k}>
                <p className="text-xs font-semibold uppercase tracking-wider text-ink-3">{k}</p>
                <p className="mt-1 text-[15px] text-ink-2 leading-relaxed">{v}</p>
              </div>
            ))}
          <p className="note mono">docs/decisions/{String(cur.n).padStart(3, '0')}-*.md</p>
        </article>
      )}
    </div>
  );
}
