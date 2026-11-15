// Benchmarks against real Redis 8.10, from docs/benchmarks/*/SUMMARY.md.
// Horizontal bars, one scale per chart, labels on every bar.

interface Bar { label: string; value: number; display: string; color: string; note?: string }
interface Chart { title: string; unit: string; bars: Bar[]; takeaway: string; source: string; log?: boolean }

const S = 'var(--a)', R = 'var(--fg3)', BAD = 'var(--bad)', OK = 'var(--ok)', SUN = 'var(--sun)';

const CHARTS: Chart[] = [
  { title: 'Throughput, one node, 50 clients pipelined (-P 16)', unit: 'requests / second',
    bars: [
      { label: 'Shardis SET', value: 1.46e6, display: '1.46M', color: S }, { label: 'Redis SET', value: 1.01e6, display: '1.01M', color: R },
      { label: 'Shardis GET', value: 1.56e6, display: '1.56M', color: S }, { label: 'Redis GET', value: 1.36e6, display: '1.36M', color: R }],
    takeaway: 'Faster when pipelined: 256 lock groups let many cores work at once, where Redis runs commands on one thread.', source: 'iter-02-concurrency' },
  { title: 'Throughput, one node, 50 clients, no pipelining', unit: 'requests / second',
    bars: [
      { label: 'Shardis SET', value: 128.8e3, display: '129k', color: S }, { label: 'Redis SET', value: 141.1e3, display: '141k', color: R },
      { label: 'Shardis GET', value: 126.9e3, display: '127k', color: S }, { label: 'Redis GET', value: 141.4e3, display: '141k', color: R }],
    takeaway: 'About 9% slower one request at a time: each request pays for a goroutine wake-up and a syscall, which Redis’s event loop avoids.', source: 'iter-02-concurrency' },
  { title: 'Failover time by node timeout (median of 20 runs)', unit: 'seconds until the replica serves',
    bars: [
      { label: '1 s timeout', value: 2.26, display: '2.26 s', color: SUN, note: 'max 2.47 s' },
      { label: '2 s timeout', value: 3.07, display: '3.07 s', color: SUN, note: 'max 3.37 s' },
      { label: '5 s timeout', value: 7.17, display: '7.17 s', color: SUN, note: 'max 8.46 s' }],
    takeaway: 'Failover is about the node timeout plus 1–2 s for agreement and the vote. A shorter timeout heals faster but raises false alarms.', source: 'iter-09-failover' },
  { title: 'Rebalancing 4,096 slots onto a new node, under load', unit: 'seconds',
    bars: [{ label: 'Shardis (MIGRATE + ASK)', value: 18.9, display: '18.9 s', color: BAD }, { label: 'Redis 8.10 (atomic slot migration)', value: 3.1, display: '3.1 s', color: R }],
    takeaway: 'Six times slower. We move keys in batches with redirects (the classic protocol); Redis 8 now streams whole slot ranges. Both had 0 client errors.', source: 'iter-11-resharding' },
  { title: 'Cache hit rate at 32 MB, Zipf traffic', unit: 'hit rate (%)',
    bars: [
      { label: 'Shardis approximate LRU', value: 77.2, display: '77.2%', color: S }, { label: 'Redis approximate LRU', value: 77.0, display: '77.0%', color: R },
      { label: 'Shardis random eviction', value: 73.7, display: '73.7%', color: S }, { label: 'Redis random eviction', value: 73.8, display: '73.8%', color: R }],
    takeaway: 'Same hit rates as Redis. But Shardis needs 32 MB where Redis uses 18 MB for similar key counts: Go maps and pointers cost more per key.', source: 'iter-04-eviction' },
  { title: 'Durability cost: SET throughput by fsync policy', unit: 'requests / second, 50 clients',
    bars: [
      { label: 'no AOF', value: 129e3, display: '129k', color: S, note: 'crash loses everything' },
      { label: 'appendfsync everysec', value: 115e3, display: '115k', color: S, note: 'power cut: ≤ 1 s lost' },
      { label: 'appendfsync always', value: 5.9e3, display: '5.9k', color: S, note: 'nothing lost' },
      { label: 'Redis everysec', value: 141e3, display: '141k', color: R }, { label: 'Redis always', value: 5.9e3, display: '5.9k', color: R }],
    takeaway: 'Group commit makes every-second fsync nearly free; fsync on every write costs 20×, in Redis too. A process crash with everysec lost 0 of 2.1M writes.', source: 'iter-05-persistence' },
  { title: 'Acknowledged writes lost when a primary dies', unit: 'writes lost (log scale)', log: true,
    bars: [
      { label: 'async replication', value: 4, display: '4 of 2.4M', color: BAD },
      { label: 'with WAIT 1', value: 0, display: '0 of 1.2M', color: OK },
      { label: 'slow replica link', value: 1515354, display: '1.5M of 2.3M', color: BAD },
      { label: 'slow link + WAIT', value: 0, display: '0', color: OK }],
    takeaway: 'Asynchronous replication loses what’s in flight; a lagging replica makes that huge. WAIT closes the gap for the writes that matter, at the cost of latency.', source: 'iter-06-replication' },
];

function ChartView({ c }: { c: Chart }) {
  const max = Math.max(...c.bars.map((b) => b.value));
  const scale = (v: number) => (c.log ? (v <= 0 ? 0 : Math.log10(v + 1) / Math.log10(max + 1)) : v / max);
  return (
    <figure className="panel m-0 flex flex-col gap-4">
      <figcaption>
        <h3 className="text-xl leading-tight">{c.title}</h3>
        <p className="note mt-1">{c.unit}</p>
      </figcaption>
      <div className="flex flex-col gap-2.5">
        {c.bars.map((b) => (
          <div key={b.label} className="grid grid-cols-[minmax(0,150px)_minmax(0,1fr)] items-center gap-3 text-sm">
            <span className="text-ink-2 leading-tight">{b.label}</span>
            <div className="flex items-center gap-2 min-w-0">
              <div className="h-5 rounded-md shrink-0" style={{ width: `${Math.max(scale(b.value) * 72, b.value > 0 ? 1.5 : 0.6)}%`, background: b.color, opacity: b.value > 0 ? 1 : 0.5 }} />
              <span className="mono text-xs text-ink whitespace-nowrap">{b.display}</span>
              {b.note && <span className="text-xs text-ink-3 truncate">{b.note}</span>}
            </div>
          </div>
        ))}
      </div>
      <p className="text-[15px] text-ink-2 leading-snug">{c.takeaway}</p>
      <p className="note mono">docs/benchmarks/{c.source}</p>
    </figure>
  );
}

export function Benchmarks() {
  return (
    <div className="grid gap-5 md:grid-cols-2 mt-10">
      {CHARTS.map((c) => <ChartView key={c.title} c={c} />)}
    </div>
  );
}
