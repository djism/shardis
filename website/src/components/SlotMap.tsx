import { useEffect, useRef } from 'react';
import { UNOWNED, type ClusterState } from '../lib/model';
import { SHARD_VARS, type Layout } from './ClusterView';

// All 16,384 slots as a 128 × 128 grid, coloured by the owner's column. A
// failover keeps the colour (the slots stay with the same shard, now on the
// replica); resharding repaints slots with the new node's colour.

const N = 128;

export function SlotMap({ state, layout, highlight }: { state: ClusterState; layout: Layout; highlight?: number | null }) {
  const ref = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    const c = ref.current;
    if (!c) return;
    const ctx = c.getContext('2d')!;
    const css = getComputedStyle(document.documentElement);
    const color = (v: string) => css.getPropertyValue(v.slice(4, -1)).trim();
    const colors = state.nodes.map((n) => color(SHARD_VARS[(layout.pos.get(n.name)?.column ?? 0) % SHARD_VARS.length]));
    const dim = state.nodes.map((n) => n.down || n.fail);
    const img = ctx.createImageData(N, N);
    const rgb = colors.map((h) => [parseInt(h.slice(1, 3), 16), parseInt(h.slice(3, 5), 16), parseInt(h.slice(5, 7), 16)]);
    const empty = color('var(--line)');
    const er = [parseInt(empty.slice(1, 3), 16), parseInt(empty.slice(3, 5), 16), parseInt(empty.slice(5, 7), 16)];
    for (let s = 0; s < N * N; s++) {
      const o = state.owners[s];
      const [r, g, b] = o === UNOWNED ? er : rgb[o];
      const a = o !== UNOWNED && dim[o] ? 90 : 255; // unavailable slots fade
      img.data.set([r, g, b, a], s * 4);
    }
    ctx.putImageData(img, 0, 0);
  }, [state, layout]);
  return (
    <div className="relative">
      <canvas ref={ref} width={N} height={N} className="w-full aspect-square rounded-md [image-rendering:pixelated] border border-line"
        role="img" aria-label="Hash slot map: 16,384 slots coloured by owning node" />
      {highlight != null && (
        <div className="absolute size-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-ink bg-panel shadow"
          style={{ left: `${((highlight % N) + 0.5) / N * 100}%`, top: `${(Math.floor(highlight / N) + 0.5) / N * 100}%` }} />
      )}
    </div>
  );
}
