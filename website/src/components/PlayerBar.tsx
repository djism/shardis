import type { Player } from '../lib/hooks';

const SPEEDS = [0.25, 0.5, 1, 2, 4];

function Btn(props: React.ButtonHTMLAttributes<HTMLButtonElement>) {
  return <button {...props} className="h-8 min-w-8 px-2 rounded-md border border-line bg-panel text-sm text-ink hover:bg-sunken disabled:opacity-40" />;
}

/** play / pause / step between key events / speed / scrub. Keyboard: space, ←, →. */
export function PlayerBar({ p }: { p: Player }) {
  return (
    <div className="flex flex-wrap items-center gap-2"
      onKeyDown={(e) => {
        if (e.key === ' ') { e.preventDefault(); p.playing ? p.pause() : p.play(); }
        if (e.key === 'ArrowRight') p.step(1);
        if (e.key === 'ArrowLeft') p.step(-1);
      }}>
      <Btn onClick={() => p.step(-1)} aria-label="Previous key event" title="Previous key event (←)">⏮</Btn>
      <Btn onClick={() => (p.playing ? p.pause() : p.play())} aria-label={p.playing ? 'Pause' : 'Play'} title="Play / pause (space)">
        {p.playing ? '❚❚' : '▶'}
      </Btn>
      <Btn onClick={() => p.step(1)} aria-label="Next key event" title="Next key event (→)">⏭</Btn>
      <div className="relative min-w-0 flex-1 basis-40">
        <input type="range" min={0} max={p.duration || 1} step={0.05} value={p.t} onChange={(e) => p.seek(+e.target.value)}
          className="w-full accent-[var(--shard-1)]" aria-label="Time" />
        <div className="pointer-events-none absolute inset-x-0 -bottom-1 h-1.5">
          {p.marks.map((m, i) => (
            <span key={i} className="absolute top-0 h-1.5 w-px bg-ink-3" style={{ left: `${(m.t / (p.duration || 1)) * 100}%` }} />
          ))}
        </div>
      </div>
      <span className="font-mono text-xs text-ink-2 tabular w-20 text-right">{p.t.toFixed(1)}s / {p.duration.toFixed(0)}s</span>
      <select value={p.speed} onChange={(e) => p.setSpeed(+e.target.value)} aria-label="Speed"
        className="h-8 rounded-md border border-line bg-panel px-1 text-sm text-ink">
        {SPEEDS.map((s) => <option key={s} value={s}>{s}×</option>)}
      </select>
    </div>
  );
}
