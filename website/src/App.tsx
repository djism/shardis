import { useEffect } from 'react';
import { useReducedMotion } from './lib/hooks';
import { mountBreak, mountHero, mountMall, mountRedirects, mountStorm, mountTTL } from './scenes/scenes.js';
import { Benchmarks } from './sections/Benchmarks';
import { Decisions } from './sections/Decisions';
import { Kubernetes } from './sections/Kubernetes';
import { Replay } from './sections/Replay';

function Head({ n, color, kicker, title, children }: { n: number; color: string; kicker: string; title: string; children: React.ReactNode }) {
  return (
    <div className="grid gap-10 md:grid-cols-2 md:items-end">
      <div>
        <span className="kicker" style={{ color }}><span className="n" style={{ background: color }}>{n}</span>{kicker}</span>
        <h2>{title}</h2>
      </div>
      <p className="say">{children}</p>
    </div>
  );
}

const NAV = [['mall', 'One request'], ['storm', 'Millions'], ['ttl', 'TTL'], ['redirects', 'Redirects'], ['break', 'Break it'], ['real', 'Real runs'], ['k8s', 'Kubernetes'], ['bench', 'Benchmarks'], ['decisions', 'Decisions'], ['score', 'Scorecard']];

export default function App() {
  const reduced = useReducedMotion();
  useEffect(() => {
    const stops = [mountHero(), mountMall(), mountStorm(), mountTTL(), mountRedirects(), mountBreak()];
    return () => stops.forEach((s) => s());
  }, []);

  return (
    <>
      <header className="top">
        <div className="wrap flex items-center gap-6 py-3.5">
          <a href="#top" className="flex items-center gap-2.5 text-[20px] font-extrabold no-underline" style={{ fontFamily: 'var(--display)', color: 'var(--fg)' }} aria-label="Shardis home">
            <svg width="26" height="26" viewBox="0 0 26 26" aria-hidden="true"><circle cx="7" cy="8" r="5" fill="#9b7bff" /><circle cx="19" cy="8" r="5" fill="#ff7a6b" /><circle cx="13" cy="19" r="5" fill="#2fe0c0" /></svg>
            Shardis
          </a>
          <nav className="ml-auto hidden lg:flex gap-4 text-sm" aria-label="Sections">
            {NAV.map(([id, label]) => <a key={id} href={`#${id}`} className="no-underline hover:text-[var(--fg)]" style={{ color: 'var(--fg2)' }}>{label}</a>)}
          </nav>
          <a href="https://github.com/djism/shardis" className="ml-auto lg:ml-0 btn ghost no-underline" style={{ padding: '9px 14px', fontSize: 14 }}>GitHub</a>
        </div>
      </header>

      <section id="top" className="relative overflow-hidden">
        <canvas id="heroCanvas" className="absolute inset-0 w-full h-full" aria-hidden="true" />
        <div className="wrap relative pointer-events-none" style={{ paddingBlock: '88px 72px' }}>
          <div className="pointer-events-auto">
            <p className="eyebrow">Distributed systems · built from scratch in Go</p>
            <h1 className="mt-5 max-w-[980px] font-extrabold" style={{ fontSize: 'clamp(46px, 8.2vw, 120px)', lineHeight: 0.92 }}>
              Shardis is a <span className="shimmer">distributed</span> mini‑Redis.
            </h1>
            <p className="mt-6 max-w-[640px]" style={{ fontSize: 'clamp(18px, 1.6vw, 22px)', color: 'var(--fg2)' }}>
              Six processes that behave like one cache. They split your keys between them, copy every write to a backup, gossip about each other's health, and replace a dead machine on their own. Real <span className="mono">redis-cli</span> and Redis client libraries talk to it unchanged.
            </p>
            <div className="flex flex-wrap gap-3 mt-8">
              <a className="btn no-underline" href="#mall">Follow one request</a>
              <a className="btn ghost no-underline" href="#break">Break the cluster</a>
            </div>
            <div className="flex flex-wrap gap-2.5 mt-11" aria-label="Legend">
              {[['var(--a)', 'shard A'], ['var(--b)', 'shard B'], ['var(--c)', 'shard C'], ['var(--sky)', 'gossip']].map(([c, l]) => (
                <span key={l} className="chip"><span className="size-2.5 rounded-full" style={{ background: c }} />{l}</span>
              ))}
              <span className="chip">writes copied to replicas <b id="heroRep">0</b></span>
            </div>
          </div>
        </div>
      </section>

      <section className="wrap pt-10" aria-label="What Shardis is">
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {[
            { bg: 'var(--a)', big: 'RESP', h: 'Speaks Redis', p: 'Same wire protocol, same MOVED and ASK redirects. redis-cli, redis-benchmark and go-redis work against it.' },
            { bg: 'var(--b)', big: '16,384', h: 'hash slots', p: 'split across three shards. Every key belongs to exactly one, and every client can work out which.' },
            { bg: 'var(--c)', big: '2×', h: 'every write', p: 'lives on a primary and its replica, in memory and in an append-only log on disk.' },
            { bg: 'var(--sun)', big: '~3 s', h: 'to heal', p: 'Nodes notice a dead peer by gossip, vote, and promote its replica, with no human involved.' },
          ].map((f) => (
            <article key={f.h} className="rounded-3xl p-6 min-h-[210px] flex flex-col justify-between gap-5" style={{ background: f.bg, color: 'var(--bg)' }}>
              <span className="font-extrabold" style={{ fontFamily: 'var(--display)', fontSize: 54, lineHeight: 1, letterSpacing: '-0.03em' }}>{f.big}</span>
              <p className="text-[15px] leading-snug" style={{ color: 'color-mix(in srgb, var(--bg) 82%, transparent)' }}><strong>{f.h}</strong> · {f.p}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="scene" id="mall">
        <div className="wrap">
          <Head n={1} color="var(--a)" kicker="One request" title="A shopper adds sneakers to her cart.">
            One write, start to finish. The key decides its own home: hash it, take the remainder, and every machine agrees where it lives. No lookup service, no single point of failure.
          </Head>
          <div className="stage grid lg:grid-cols-[minmax(0,1.6fr)_minmax(0,1fr)]">
            <canvas id="mallCanvas" height="440" aria-label="Animated path of one write through the cluster" />
            <div className="p-6 flex flex-col gap-4 border-t lg:border-t-0 lg:border-l" style={{ borderColor: 'var(--bg3)' }}>
              <div className="mono text-[15px] font-semibold rounded-xl px-4 py-3 overflow-x-auto whitespace-nowrap" style={{ background: 'var(--bg)', boxShadow: 'inset 0 0 0 1px var(--bg3)' }} id="mallCmd">SET cart:alice sneakers EX 1800</div>
              <ol className="steps" id="mallSteps" />
              <form className="flex flex-wrap gap-2" id="keyForm">
                <label htmlFor="keyInput" className="w-full text-[13px]" style={{ color: 'var(--fg3)' }}>Try your own key: the slot is computed in your browser with the real CRC16, the same function the server runs.</label>
                <input id="keyInput" className="field flex-1 min-w-0" defaultValue="cart:alice" autoComplete="off" spellCheck={false} />
                <button type="submit" className="btn warn">Send it</button>
              </form>
            </div>
          </div>
        </div>
      </section>

      <section className="scene" id="storm">
        <div className="wrap">
          <Head n={2} color="var(--b)" kicker="Millions" title="Then it's Black Friday.">
            Turn the dial. Each shard takes only the keys in its slots, so load spreads by itself. Inside a node, commands run in parallel and lock only one of 256 groups. Every write streams on to a replica.
          </Head>
          <div className="stage">
            <canvas id="stormCanvas" height="460" aria-label="Particles of traffic spreading across three shards" />
            <div className="flex flex-wrap gap-7 items-center px-6 py-5 border-t" style={{ borderColor: 'var(--bg3)' }}>
              <div><p className="note">load this animation represents</p><p id="stormCount" className="font-extrabold tabular" style={{ fontFamily: 'var(--display)', fontSize: 'clamp(34px,4.6vw,60px)', lineHeight: 1 }}>1,000</p><p className="note">requests per second</p></div>
              <div className="flex flex-wrap items-center gap-3.5 flex-1 basis-80">
                <label htmlFor="stormDial" className="note w-full">Traffic</label>
                <span className="note">1</span><input type="range" id="stormDial" min="0" max="6" step="0.01" defaultValue="3" className="flex-1 basis-60" /><span className="note">1M+</span>
              </div>
            </div>
          </div>
          <p className="note mt-3">Measured: one node serves about 1.46 million SETs a second, pipelined. The dots are scaled down; the counter is what they stand for.</p>
        </div>
      </section>

      <section className="scene" id="ttl">
        <div className="wrap">
          <Head n={3} color="var(--c)" kicker="TTL & memory" title="Keys expire. Memory runs out.">
            Each bubble is a key; its ring is the time it has left. When it runs out, the key pops. When memory hits the limit, Shardis samples five keys and evicts the one used longest ago: approximate LRU, within 0.4 points of exact LRU's hit rate.
          </Head>
          <div className="stage">
            <canvas id="ttlCanvas" height="440" aria-label="Keys expiring and being evicted" />
            <div className="flex flex-wrap gap-3 items-center px-6 py-4 border-t" style={{ borderColor: 'var(--bg3)' }}>
              <button type="button" className="btn warn" id="ttlBurst">Write 40 keys</button>
              <span className="chip">expired <b id="ttlExp">0</b></span>
              <span className="chip">evicted (LRU) <b id="ttlEv">0</b></span>
              <span className="chip">hit rate <b id="ttlHit">–</b></span>
            </div>
          </div>
        </div>
      </section>

      <section className="scene" id="redirects">
        <div className="wrap">
          <Head n={4} color="var(--sun)" kicker="Redirects" title="Every node knows the whole map.">
            Ask any node for any key. If it isn't the owner, it says where to go: MOVED means “it lives there now, update your map”; ASK means “mid-move, try there just this once.” During a live rebalance our client followed 800 MOVED and 35 ASK redirects, with 0 errors.
          </Head>
          <div className="stage">
            <canvas id="redirCanvas" height="420" aria-label="A client following MOVED and ASK redirects" />
            <div className="flex flex-wrap gap-3 items-center px-6 py-4 border-t" style={{ borderColor: 'var(--bg3)' }}>
              <p id="redirCap" className="flex-1 basis-80 text-[16px]" style={{ color: 'var(--fg2)' }} aria-live="polite" />
              <span className="chip">MOVED <b id="redirMoved">0</b></span>
              <span className="chip">ASK <b id="redirAsk">0</b></span>
            </div>
          </div>
        </div>
      </section>

      <section className="scene" id="break">
        <div className="wrap">
          <Head n={5} color="var(--bad)" kicker="Break it" title="Now kill a machine.">
            This is what makes it a distributed system. No node is in charge. Each pings a few others ten times a second. Silence turns into suspicion, suspicion spreads by gossip, a majority agrees, and the replica asks the other primaries for votes.
          </Head>
          <div className="stage">
            <div className="absolute left-4 top-4 flex gap-2 flex-wrap pointer-events-none z-10"><span className="chip" id="breakState">healthy · 16,384 slots served</span></div>
            <canvas id="breakCanvas" height="470" aria-label="Interactive cluster you can break" />
            <div className="flex flex-wrap gap-2.5 items-center px-6 py-4 border-t" style={{ borderColor: 'var(--bg3)' }}>
              <button type="button" className="btn danger" id="bKill">Kill primary A</button>
              <button type="button" className="btn warn" id="bPart">Cut A off the network</button>
              <button type="button" className="btn sky" id="bHeal" disabled>Heal the network</button>
              <button type="button" className="btn ghost" id="bReset">Reset</button>
            </div>
            <ol className="timeline" id="breakTimeline">
              <li data-at="0"><b>0.0 s</b>A stops answering</li>
              <li data-at="2"><b>+2.0 s</b>peers mark it suspect (PFAIL)</li>
              <li data-at="2.5"><b>+2.5 s</b>a majority agrees: FAIL</li>
              <li data-at="2.9"><b>+2.9 s</b>replica A′ asks for votes</li>
              <li data-at="3.2"><b>+3.2 s</b>A′ wins, owns shard A</li>
            </ol>
          </div>
          <p className="note mt-3">An interactive simulation using the timings of our recorded kill-primary run (2 s node timeout). The real recording is next.</p>
        </div>
      </section>

      <section className="scene" id="real">
        <div className="wrap">
          <Head n={6} color="var(--sky)" kicker="The real runs" title="Here's what actually happened.">
            Seven chaos scenarios ran against seven real processes while a checker recorded every operation. These are those recordings, event by event: every node's own reports and what we did to it. Step through the moment a replica wins its election.
          </Head>
          <div className="mt-10"><Replay reduced={reduced} /></div>
        </div>
      </section>

      <section className="scene" id="k8s">
        <div className="wrap">
          <Head n={7} color="var(--a)" kicker="Kubernetes" title="Six pods that remember who they are.">
            Pods are disposable; cache nodes aren't. A StatefulSet gives each node a permanent name and its own disk, so a restarted pod rejoins as itself. Roll the set, read the real terminal, and open a config file to see what matters in it.
          </Head>
          <Kubernetes reduced={reduced} />
        </div>
      </section>

      <section className="scene" id="bench">
        <div className="wrap">
          <Head n={8} color="var(--c)" kicker="Benchmarks" title="Against real Redis, including where we lose.">
            Same machine, same tools (redis-benchmark, our own load and crash testers), Redis 8.10 alongside. Where Shardis is slower, the reason is written under the chart.
          </Head>
          <Benchmarks />
        </div>
      </section>

      <section className="scene" id="decisions">
        <div className="wrap">
          <Head n={9} color="var(--b)" kicker="Design decisions" title="Every choice, written down.">
            Each real decision got a record: the options, the one we picked, why, the cost we accept, what Redis does, and what changes at a much bigger scale. These cards are built from those files.
          </Head>
          <Decisions />
        </div>
      </section>

      <section className="scene" id="scale">
        <div className="wrap">
          <Head n={10} color="var(--sun)" kicker="At Google scale" title="What I'd change with a thousand nodes.">
            Shardis makes Redis Cluster's tradeoffs on purpose. At a much larger scale, or for data you can't afford to lose, these are the changes I'd make first.
          </Head>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3 mt-10">
            {[
              ['Consensus for cluster metadata', 'Slot ownership through a Raft log instead of gossip and epoch bumps: no moment with two owners, no split-brain window.', 'ADR 035'],
              ['Idempotent writes end to end', 'Request IDs that servers remember and deduplicate, so clients can retry everything safely.', 'ADR 036'],
              ['Stream slots, don’t walk keys', 'Snapshot plus change stream per slot range, then a short handoff, like Redis 8 and CockroachDB: 6× faster rebalances.', 'ADR 032'],
              ['An operator, not scripts', 'A controller that scales shards, rebalances, finishes half-moved slots and rolls pods only when replicas are in sync.', 'ADR 044'],
              ['Gossip that scales', 'Full slot bitmaps in every heartbeat cost bandwidth that grows with cluster size; send deltas and partial views (SWIM-style).', 'ADR 025'],
              ['Zone-aware replicas', 'Replicas in another zone, reads from the nearest one, with bounded staleness instead of “maybe stale”.', 'ADR 039'],
            ].map(([h, p, adr]) => (
              <article key={h} className="panel flex flex-col gap-2.5">
                <h3 className="text-[22px] leading-tight">{h}</h3>
                <p className="text-[15px] leading-relaxed" style={{ color: 'var(--fg2)' }}>{p}</p>
                <p className="note mono mt-auto">{adr}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="scene" id="score">
        <div className="wrap">
          <Head n={11} color="var(--ok)" kicker="Scorecard" title="What it's good at, and where it isn't.">
            Redis Cluster trades consistency for speed and availability. Shardis does too, on purpose. Every number below was measured, including the bad ones.
          </Head>
          <div className="grid gap-4 lg:grid-cols-3 mt-10">
            {[
              { h: 'Does well', c: 'var(--ok)', items: [
                ['Heals without a human', 'Gossip, a FAIL quorum and an election replace a dead primary.', '~3 s with a 2 s timeout'],
                ['Clients barely notice', 'Seven chaos scenarios, from crashes to partitions to resharding mid-failover.', '0 errors in 1.2M operations'],
                ['Restarts lose nothing', 'Primaries hand off their slots before a pod stops.', '13 ms handoff · 0 lost'],
                ['Grows while serving', 'Slots move to a new node one batch at a time; clients follow ASK and MOVED.', '4,096 slots, 0 client errors'],
                ['Fast per node', '256 lock groups instead of one thread.', '1.46M SETs/s pipelined']] },
              { h: 'Where it hurts', c: 'var(--bad)', items: [
                ['Partitions roll back writes', 'A cut-off primary keeps saying OK until it notices it is in the minority.', '2,029 acknowledged writes lost on Kubernetes'],
                ['Crashes lose the in-flight tail', 'Replication is asynchronous: the primary replies before the replica has it.', 'up to 146 writes per failover'],
                ['Blind retries double-count', 'Retrying an INCR after a timeout can apply it twice; our client refuses to.', '32 of 32 doubled with retry-everything'],
                ['More memory than Redis', 'Go maps and pointers: similar key counts in 32 MB vs Redis’s 18 MB.', '~1.7× memory per key']] },
              { h: 'Doesn’t have yet', c: 'var(--sun)', items: [
                ['Data types beyond strings', 'No lists, hashes, sets or sorted sets.', ''],
                ['Transactions and pub/sub', 'No MULTI/EXEC, no PUBLISH/SUBSCRIBE.', ''],
                ['Auth and TLS', 'It assumes a trusted network, like default Redis.', ''],
                ['Strong consistency', 'By design. For that you’d want Raft and quorum writes, at the cost of latency.', '']] },
            ].map((col) => (
              <div key={col.h} className="panel">
                <h3 className="text-[28px] flex items-center gap-3"><span className="size-3.5 rounded-[5px]" style={{ background: col.c }} />{col.h}</h3>
                <ul className="list-none m-0 mt-4 p-0 grid gap-3.5">
                  {col.items.map(([t, d, m]) => (
                    <li key={t} className="grid gap-0.5">
                      <strong className="text-base">{t}</strong>
                      <span className="text-[15px] leading-snug" style={{ color: 'var(--fg2)' }}>{d}</span>
                      {m && <em className="not-italic mono text-[13px]" style={{ color: col.c }}>{m}</em>}
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
        </div>
      </section>

      <footer className="wrap" style={{ paddingBlock: '80px 60px' }}>
        <div className="flex flex-wrap gap-6 border-t pt-7 text-sm" style={{ borderColor: 'var(--bg3)', color: 'var(--fg3)' }}>
          <span>Shardis · a Redis Cluster–compatible cache written from the public specs. Scenes 1–5 are simulations using measured numbers; scene 6 is recordings.</span>
          <a className="ml-auto" href="https://github.com/djism/shardis">Source on GitHub</a>
        </div>
      </footer>
    </>
  );
}
