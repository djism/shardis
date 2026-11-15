import { useEffect, useRef, useState } from 'react';

// Kubernetes: a live pod grid (rolling restart with the SIGTERM handoff),
// a deck of the real manifests, and a terminal that replays output captured
// from our minikube runs (Iteration 14).

const SHARD = ['var(--a)', 'var(--b)', 'var(--c)'];

interface Pod { name: string; role: 'primary' | 'replica'; s: number; st: string; handoff?: boolean }
const initialPods = (): Pod[] => [0, 1, 2, 3, 4, 5].map((i) => ({ name: `shardis-${i}`, role: i < 3 ? 'primary' : 'replica', s: i % 3, st: 'Running' }));

type Line = [cls: '' | 'hl' | 'c', html: string];
interface File { fn: string; color: string; title: string; tag: string; yaml: Line[]; know: string[] }

const k = (s: string) => `<span class="k">${s}</span>`;
const v = (s: string) => `<span class="v">${s}</span>`;
const c = (s: string) => `<span class="c">${s}</span>`;

const FILES: File[] = [
  { fn: 'statefulset.yaml', color: 'var(--a)', title: 'Six nodes that keep their identity', tag: 'Name, disk and node ID survive any restart.',
    yaml: [['c', '# shardis-0 … shardis-5, created in parallel'], ['', `${k('kind:')} ${v('StatefulSet')}`], ['', `${k('replicas:')} ${v('6')}`],
      ['hl', `${k('terminationGracePeriodSeconds:')} ${v('30')}   ${c('# time to hand off slots')}`],
      ['hl', `${k('SHARDIS_CLUSTER_ANNOUNCE_ADDR:')} ${v('"$(POD_NAME).shardis.$(POD_NAMESPACE).svc:7000"')}`],
      ['', `${k('readinessProbe:')} { tcpSocket: { port: client } }`], ['hl', `${k('volumeClaimTemplates:')} [ data ]   ${c('# AOF + nodes.conf')}`]],
    know: ['Nodes announce a DNS name, not an IP. A rescheduled pod gets a new IP but rejoins as the same node.',
      'Each pod has its own volume, so its data and node ID come back with it.',
      'Ready means “accepting connections”, not “cluster healthy”: otherwise one failed shard would pull every pod out of DNS.'] },
  { fn: 'service.yaml', color: 'var(--b)', title: 'A stable address for every pod', tag: 'Headless: DNS per pod, no load balancer.',
    yaml: [['', `${k('kind:')} ${v('Service')}`], ['hl', `${k('clusterIP:')} ${v('None')}   ${c('# headless')}`], ['hl', `${k('publishNotReadyAddresses:')} ${v('true')}`], ['', `${k('ports:')} [ client: 7000, bus: 17000 ]`]],
    know: ['No virtual IP: a client must reach the node that owns the slot, so each pod gets its own DNS record.',
      'Not-ready pods are still published, so nodes can find each other while starting, or while one is in a minority.',
      'Port 17000 is the cluster bus: gossip, failure reports and votes travel there.'] },
  { fn: 'configmap.yaml', color: 'var(--c)', title: 'One config for all six', tag: 'Every SHARDIS_* setting in one place.',
    yaml: [['hl', `${k('SHARDIS_CLUSTER_NODE_TIMEOUT:')} ${v('"2000"')}   ${c('# ms until a node is suspected')}`], ['', `${k('SHARDIS_APPENDONLY:')} ${v('"yes"')}`],
      ['', `${k('SHARDIS_APPENDFSYNC:')} ${v('everysec')}`], ['hl', `${k('SHARDIS_MAXMEMORY:')} ${v('200mb')}`], ['', `${k('SHARDIS_MAXMEMORY_POLICY:')} ${v('allkeys-lru')}`], ['', `${k('SHARDIS_METRICS_PORT:')} ${v('"9121"')}`]],
    know: ['Node timeout is the big dial: shorter means faster failover but more false alarms. 2 s is a demo value; production uses 5–15 s.',
      'fsync every second: a power cut can lose up to a second of writes; a process crash loses nothing.',
      'Prometheus scrapes port 9121; a Grafana dashboard ships in the repo.'] },
  { fn: 'bootstrap-job.yaml', color: 'var(--sun)', title: 'Forms the cluster, once', tag: 'Safe to run again: a formed cluster is left alone.',
    yaml: [['', `${k('kind:')} ${v('Job')}`], ['hl', `${k('command:')} [${v('"/shardis-admin"')}, ${v('"create"')}, ${v('"-replicas"')}, ${v('"1"')},`], ['', `  ${v('"-nodes"')}, ${v('"shardis-0.shardis…:7000,…"')}]`], ['', `${k('restartPolicy:')} ${v('OnFailure')}`]],
    know: ['Gives each primary a distinct config epoch, assigns slots, introduces the nodes, then pairs replicas with primaries.',
      'Waits until every node agrees on who is a replica before it reports success.', 'Idempotent, so re-applying the manifests never re-forms a running cluster.'] },
  { fn: 'pdb.yaml', color: 'var(--sky)', title: 'Never two down on purpose', tag: 'Voluntary disruptions, one pod at a time.',
    yaml: [['', `${k('kind:')} ${v('PodDisruptionBudget')}`], ['hl', `${k('maxUnavailable:')} ${v('1')}`]],
    know: ['Node drains and upgrades can only take one Shardis pod at a time.', 'So a primary and its replica are never both gone by choice: one of them always serves the shard.'] },
  { fn: 'networkpolicy.yaml', color: 'var(--bad)', title: 'How we cut a node off', tag: 'The partition demo, with a real NetworkPolicy.',
    yaml: [['', `${k('kind:')} ${v('NetworkPolicy')}`], ['', `${k('podSelector:')} { pod-name: ${v('shardis-5')} }`], ['hl', `${k('ingress:')} [ from: { role: ${v('client')} } ]`], ['hl', `${k('egress:')}  [ to: { role: ${v('client')} }, dns ]`]],
    know: ['Calico enforces policy on new connections only, so open gossip links survived the first try. The demo also clears the pod’s connection tracking.',
      'The isolated primary stopped writes after 2 s, its replica was elected, and 2,029 writes it accepted alone were rolled back on heal.'] },
];

// Real output from deploy/k8s/demos on minikube: log timestamps trimmed,
// the checker's report tables condensed to one line each.
const TERMINAL: { text: string; cls?: 'cmd' | 'warn' | 'ok' }[] = [
  { text: '$ deploy/k8s/demos/rolling-restart.sh', cls: 'cmd' },
  { text: 'checker running for 150s (writes increasing values, reads them back, records every op)' },
  { text: '$ kubectl -n shardis logs -f shardis-5', cls: 'cmd' },
  { text: 'INFO graceful handoff: writes paused, waiting for the replica to catch up' },
  { text: 'INFO graceful handoff done took=15ms', cls: 'ok' },
  { text: 'INFO shardis stopped' },
  { text: 'partitioned roll out complete: 6 new pods have been updated...' },
  { text: 'external …  ops 1,122,348  errors 0  lost 0  rolled back 0  stale reads 0', cls: 'ok' },
  { text: '$ deploy/k8s/demos/partition.sh', cls: 'cmd' },
  { text: 'isolating shardis-5 from the other Shardis pods (NetworkPolicy)' },
  { text: 'shardis-5: WARN minority partition: can’t reach a majority of primaries; refusing commands', cls: 'warn' },
  { text: 'shardis-0: WARN node FAIL (told by peer) node=30a6d979' },
  { text: 'shardis-3: INFO voted for failover replica=b79a4b50 epoch=33' },
  { text: 'healing (deleting the NetworkPolicy)' },
  { text: 'external …  ops 943,699  errors 0  rolled back 2,029  stale reads 130', cls: 'warn' },
];

const sleep = (ms: number, reduced: boolean) => new Promise((r) => setTimeout(r, reduced ? 0 : ms));

export function Kubernetes({ reduced }: { reduced: boolean }) {
  const [pods, setPods] = useState<Pod[]>(initialPods);
  const [rolling, setRolling] = useState(false);
  const [note, setNote] = useState('On SIGTERM a primary pauses writes, waits for its replica to catch up, hands over its slots, then exits. Measured: 13 ms per handoff, 0 writes lost across a full rollout.');
  const [open, setOpen] = useState<number | null>(null);
  const [shown, setShown] = useState(false);
  const closeRef = useRef<HTMLButtonElement>(null);

  async function roll() {
    setRolling(true);
    let ps = initialPods().map((p, i) => ({ ...p, role: pods[i].role })) as Pod[];
    const put = (next: Pod[]) => { ps = next; setPods(next); };
    for (let i = 5; i >= 0; i--) {
      const p = ps[i];
      if (p.role === 'primary') {
        put(ps.map((q, j) => (j === i ? { ...q, handoff: true, st: 'SIGTERM · pausing writes' } : q)));
        await sleep(700, reduced);
        put(ps.map((q, j) => (j === i ? { ...q, role: 'replica', handoff: false } : q.s === p.s ? { ...q, role: 'primary', st: 'Running · took over slots' } : q)));
        await sleep(500, reduced);
      }
      for (const st of ['Terminating', 'ContainerCreating', 'Running']) { put(ps.map((q, j) => (j === i ? { ...q, st } : q))); await sleep(st === 'Running' ? 350 : 600, reduced); }
    }
    put(ps.map((q) => ({ ...q, st: 'Running' })));
    setNote('Rolled all six pods. Primaries and replicas swapped places; every write survived (measured: 0 lost across 1.1M operations).');
    setRolling(false);
  }

  useEffect(() => {
    if (open === null) return;
    const id = setTimeout(() => { setShown(true); closeRef.current?.focus(); }, reduced ? 0 : 260);
    return () => clearTimeout(id);
  }, [open, reduced]);
  const close = () => { setShown(false); setTimeout(() => setOpen(null), reduced ? 0 : 420); };

  const n = FILES.length;
  const f = open !== null ? FILES[open] : null;

  return (
    <div className="grid gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.15fr)] mt-10">
      <div className="flex flex-col gap-5 min-w-0">
        <div className="panel">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <h3 className="text-[22px]">StatefulSet shardis</h3>
            <button type="button" className="btn sky" onClick={roll} disabled={rolling}>kubectl rollout restart</button>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 mt-4">
            {pods.map((p) => (
              <div key={p.name} className={`pod${p.st === 'Terminating' ? ' term' : ''}${p.handoff ? ' handoff' : ''}`}>
                <div className="mono text-[13px] text-ink-2">{p.name}</div>
                <span className="mono inline-block mt-2.5 rounded-full px-2 py-1 text-[11px] font-semibold text-[var(--bg)]" style={{ background: SHARD[p.s] }}>{p.role} · {'ABC'[p.s]}</span>
                <div className="mt-2 text-xs text-ink-3 min-h-[1.4em]">{p.st}{p.handoff ? ' · handoff 13 ms' : ''}</div>
              </div>
            ))}
          </div>
          <p className="note mt-4" aria-live="polite">{note}</p>
        </div>
        <Terminal reduced={reduced} />
      </div>

      <div className="panel" style={{ padding: 0, minHeight: 470 }}>
        <div className={`deck${shown ? ' dim' : ''}`} aria-label="Configuration files">
          {FILES.map((file, i) => {
            const kk = i - (n - 1) / 2;
            const lifted = open === i;
            const transform = lifted ? 'translate(-50%, -30px) scale(1.08)' : `translate(calc(-50% + ${kk * 44}px), ${Math.abs(kk) * 16 + i * 26}px) rotate(${kk * 5}deg)`;
            return (
              <button key={file.fn} type="button" className="card" style={{ transform, zIndex: lifted ? 50 : i }} onClick={() => setOpen(i)}>
                <p className="mono text-[13px] font-semibold" style={{ color: file.color }}>{file.fn}</p>
                <h3 className="text-[22px] mt-2.5 leading-tight">{file.title}</h3>
                <p className="mt-3 text-sm text-ink-2">{file.tag}</p>
              </button>
            );
          })}
        </div>
        {f && (
          <div className={`detail${shown ? '' : ' closed'}`} role="dialog" aria-label={f.fn}>
            <div className="flex items-center justify-between gap-3">
              <div><p className="mono text-[13px]" style={{ color: f.color }}>{f.fn}</p><h3 className="text-[26px] mt-1.5">{f.title}</h3></div>
              <button type="button" className="btn ghost" ref={closeRef} onClick={close}>Back to files</button>
            </div>
            <pre className="yaml" dangerouslySetInnerHTML={{ __html: f.yaml.map(([cls, line]) => (cls === 'hl' ? `<span class="hl">${line}</span>` : cls === 'c' ? `<span class="c">${line}</span>` : line)).join('\n') }} />
            <ul className="know">{f.know.map((x) => <li key={x}>{x}</li>)}</ul>
          </div>
        )}
      </div>
    </div>
  );
}

function Terminal({ reduced }: { reduced: boolean }) {
  const [count, setCount] = useState(reduced ? TERMINAL.length : 0);
  const ref = useRef<HTMLDivElement>(null);
  const body = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (reduced) return;
    let timer = 0, started = false;
    const io = new IntersectionObserver(([e]) => {
      if (!e.isIntersecting || started) return;
      started = true;
      const tick = (i: number) => { setCount(i); if (i < TERMINAL.length) timer = window.setTimeout(() => tick(i + 1), TERMINAL[i]?.cls === 'cmd' ? 900 : 380); else timer = window.setTimeout(() => tick(0), 5000); };
      tick(1);
    }, { threshold: 0.3 });
    if (ref.current) io.observe(ref.current);
    return () => { io.disconnect(); clearTimeout(timer); };
  }, [reduced]);
  useEffect(() => { body.current?.scrollTo({ top: body.current.scrollHeight }); }, [count]);
  return (
    <div className="term" ref={ref}>
      <div className="bar"><i /><i /><i /><span className="ml-2">minikube · namespace shardis · real output from deploy/k8s/demos (condensed)</span></div>
      <div className="body" ref={body} aria-live="off">
        {TERMINAL.slice(0, count).map((l, i) => (
          <div key={i} style={{ color: l.cls === 'cmd' ? 'var(--sky)' : l.cls === 'ok' ? 'var(--ok)' : l.cls === 'warn' ? 'var(--sun)' : 'var(--fg2)' }}>{l.text}</div>
        ))}
        {count < TERMINAL.length && <span className="inline-block w-2 h-4 align-middle bg-[var(--fg2)] animate-pulse" />}
      </div>
    </div>
  );
}
