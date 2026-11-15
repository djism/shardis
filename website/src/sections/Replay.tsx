import { useMemo, useRef, useState } from 'react';
import { ClusterView, layoutFor, type Layout } from '../components/ClusterView';
import { PlayerBar } from '../components/PlayerBar';
import { SlotMap } from '../components/SlotMap';
import { usePlayer, useTrace } from '../lib/hooks';
import { useLive } from '../lib/live';
import { describe, eventsBetween, opsAt, type TraceEvent } from '../lib/model';

// The recorded runs, replayed event by event (ADR 047): what every node
// reported and what the chaos tool did, plus a Live tab for your own cluster.

const SCENARIOS = [
  { id: 'kill-primary', label: 'Kill a primary' },
  { id: 'partition-minority-primary', label: 'Partition a primary' },
  { id: 'reshard-during-failover', label: 'Reshard during a failover' },
  { id: 'flapping-network', label: 'Flapping network' },
  { id: 'slow-node', label: 'Slow node' },
  { id: 'kill-replica', label: 'Kill a replica' },
  { id: 'restart-all', label: 'Restart everything' },
];

const params = typeof location !== 'undefined' ? new URLSearchParams(location.search) : new URLSearchParams();
const linkScenario = SCENARIOS.some((s) => s.id === params.get('scenario')) ? params.get('scenario')! : 'kill-primary';
const linkT = Math.max(0, Number(params.get('t')) || 0);

export function Replay({ reduced }: { reduced: boolean }) {
  const [mode, setMode] = useState<'replay' | 'live'>(params.get('mode') === 'live' ? 'live' : 'replay');
  const [scenario, setScenario] = useState(linkScenario);
  const [startAt, setStartAt] = useState(linkT);
  const [liveUrl, setLiveUrl] = useState('ws://127.0.0.1:8080/ws');
  const { trace, error } = useTrace(scenario);
  const player = usePlayer(trace, false, startAt);
  const live = useLive(liveUrl, mode === 'live');

  const replayLayout = useMemo(() => (trace ? layoutFor(trace.nodes) : null), [trace]);
  const liveLayout = useRef<{ key: string; layout: Layout } | null>(null);
  if (mode === 'live' && live.state) {
    const key = live.state.nodes.map((n) => n.name).join(',');
    if (liveLayout.current?.key !== key) liveLayout.current = { key, layout: layoutFor(live.state.nodes) };
  }
  const state = mode === 'replay' ? player.state : live.state;
  const layout = mode === 'replay' ? replayLayout : liveLayout.current?.layout ?? null;
  const now = mode === 'replay' ? player.t : live.now;
  const gossip: TraceEvent[] = mode === 'replay'
    ? (trace ? eventsBetween(trace.events, player.t - 0.3, player.t).filter((e) => e.type === 'gossip_send') : [])
    : live.recent.filter((e) => e.type === 'gossip_send' && e.t > live.now - 0.3);
  const traffic = mode === 'replay' ? (trace ? opsAt(trace, player.t) : { ok: 0, fail: 0 }) : { ok: (live.state?.nodes ?? []).reduce((a, n) => a + (n.ops ?? 0), 0), fail: 0 };
  const log = (mode === 'replay' ? player.marks.filter((m) => m.t <= player.t) : live.recent)
    .map((e) => ({ e, text: describe(e) })).filter((x) => x.text).slice(-6).reverse();
  const r = trace?.report ?? {};

  return (
    <div className="stage p-4 sm:p-6">
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex rounded-full p-1" style={{ boxShadow: 'inset 0 0 0 1px var(--bg3)' }} role="tablist" aria-label="Source">
          {(['replay', 'live'] as const).map((m) => (
            <button key={m} type="button" role="tab" aria-selected={mode === m} onClick={() => setMode(m)}
              className="rounded-full px-4 py-2 text-sm font-semibold" style={mode === m ? { background: 'var(--fg)', color: 'var(--bg)' } : { color: 'var(--fg2)' }}>
              {m === 'replay' ? 'Recorded runs' : 'Live: my cluster'}
            </button>
          ))}
        </div>
        {mode === 'replay' ? (
          <select value={scenario} onChange={(e) => { setStartAt(0); setScenario(e.target.value); }} aria-label="Recorded run" className="field py-2">
            {SCENARIOS.map((s) => <option key={s.id} value={s.id}>{s.label}</option>)}
          </select>
        ) : (
          <label className="flex items-center gap-2 text-sm text-ink-3">observer
            <input className="field py-2" value={liveUrl} onChange={(e) => setLiveUrl(e.target.value)} aria-label="Observer WebSocket URL" />
          </label>
        )}
        <span className="chip ml-auto"><span className="size-2 rounded-full" style={{ background: mode === 'live' && live.status === 'live' ? 'var(--ok)' : 'var(--sun)' }} />
          {mode === 'replay' ? `recorded ${trace ? new Date(trace.recorded).toLocaleDateString() : ''} · ${trace?.events.length.toLocaleString() ?? '…'} events` : live.status === 'live' ? 'live' : 'run make cluster-up-local and bin/shardis-observer'}
        </span>
      </div>

      {error && <p className="mt-4 text-critical">Couldn’t load the trace: {error}</p>}
      {state && layout ? (
        <div className="mt-4 grid gap-5 lg:grid-cols-[minmax(0,1.6fr)_minmax(0,1fr)]">
          <div className="min-w-0">
            <ClusterView state={state} layout={layout} gossip={gossip} traffic={traffic} now={now} motionOK={!reduced} liveOps={mode === 'live'} />
            {mode === 'replay' && <PlayerBar p={player} />}
          </div>
          <div className="flex flex-col gap-4 min-w-0">
            {mode === 'replay' && trace && (
              <dl className="grid grid-cols-3 gap-2 text-sm">
                <div className="panel" style={{ padding: 14 }}><dt className="note">operations</dt><dd className="mono text-lg m-0">{Number(r.ops ?? 0).toLocaleString()}</dd></div>
                <div className="panel" style={{ padding: 14 }}><dt className="note">client errors</dt><dd className="mono text-lg m-0">{Number(r.errors ?? 0).toLocaleString()}</dd></div>
                <div className="panel" style={{ padding: 14 }}><dt className="note">rolled back</dt><dd className="mono text-lg m-0" style={{ color: Number(r.rolled_back_writes ?? 0) > 0 ? 'var(--bad)' : undefined }}>{Number(r.rolled_back_writes ?? 0).toLocaleString()}</dd></div>
              </dl>
            )}
            <div className="grid grid-cols-[96px_minmax(0,1fr)] gap-3">
              <div><SlotMap state={state} layout={layout} /><p className="note mt-1">16,384 slots</p></div>
              <ol className="m-0 p-0 list-none flex flex-col gap-1.5 text-sm" aria-live="polite">
                {log.length === 0 && <li className="text-ink-3">{mode === 'replay' ? 'Press ▶ or step with ⏭ through the run.' : 'Waiting for events…'}</li>}
                {log.map(({ e, text }, i) => (
                  <li key={`${e.t}-${i}`} className="flex gap-2">
                    <span className="mono text-xs leading-5 text-ink-3 w-10 shrink-0">{mode === 'replay' ? `${e.t.toFixed(1)}s` : ''}</span>
                    <span className={/FAIL|killed|refusing|cut off|froze/.test(text!) ? 'text-ink' : 'text-ink-2'}>{text}</span>
                  </li>
                ))}
              </ol>
            </div>
            {mode === 'replay' && trace && <p className="note">{trace.description} Nodes, gossip, failures and roles are replayed from the recorded events; traffic dots split the recorded request rate across primaries by the slots they own.</p>}
          </div>
        </div>
      ) : !error && <div className="grid h-64 place-items-center text-ink-3">{mode === 'live' ? 'Waiting for the observer…' : 'Loading the recorded run…'}</div>}
    </div>
  );
}
