import { useEffect, useRef, useState } from 'react';
import { UNOWNED, type ClusterState, type NodeState, type TraceEvent } from './model';

// Live mode: the same ClusterState, built from shardis-observer's WebSocket
// (ADR 046) instead of a recorded trace.

interface ObsNode {
  name: string;
  role: string;
  primary?: string;
  slots?: [number, number][];
  flags?: string[];
  epoch: number;
  up: boolean;
  ops?: number;
}

export type LiveStatus = 'off' | 'connecting' | 'live' | 'error';

export interface Live {
  status: LiveStatus;
  state: ClusterState | null;
  recent: TraceEvent[]; // the last few seconds of events (t in seconds, page clock)
  now: number;
}

function toState(nodes: ObsNode[]): ClusterState {
  const sorted = [...nodes].sort((a, b) => a.name.localeCompare(b.name));
  const owners = new Uint8Array(16384).fill(UNOWNED);
  let epoch = 0;
  const out: NodeState[] = sorted.map((n, i) => {
    for (const [a, b] of n.slots ?? []) owners.fill(i, a, b + 1);
    epoch = Math.max(epoch, n.epoch);
    const flags = n.flags ?? [];
    return {
      name: n.name,
      role: n.role === 'replica' ? 'replica' : (n.slots?.length ?? 0) > 0 ? 'primary' : 'empty',
      primary: n.primary,
      pfail: flags.includes('fail?'),
      fail: flags.includes('fail'),
      down: !n.up,
      minority: false,
      partitioned: false,
      ops: n.ops,
    };
  });
  return { nodes: out, owners, epoch };
}

export function useLive(url: string, enabled: boolean): Live {
  const [status, setStatus] = useState<LiveStatus>('off');
  const [state, setState] = useState<ClusterState | null>(null);
  const [recent, setRecent] = useState<TraceEvent[]>([]);
  const [now, setNow] = useState(0);
  const buf = useRef<TraceEvent[]>([]);
  useEffect(() => {
    if (!enabled) {
      setStatus('off');
      return;
    }
    let ws: WebSocket | null = null;
    let closed = false;
    let retry = 0;
    const t0 = performance.now();
    const connect = () => {
      setStatus('connecting');
      try {
        ws = new WebSocket(url);
      } catch {
        setStatus('error');
        return;
      }
      ws.onopen = () => setStatus('live');
      ws.onerror = () => setStatus('error');
      ws.onclose = () => {
        if (!closed) {
          setStatus('error');
          retry = window.setTimeout(connect, 2000);
        }
      };
      ws.onmessage = (m) => {
        const msg = JSON.parse(m.data as string);
        const t = (performance.now() - t0) / 1000;
        if (msg.kind === 'topology') setState(toState(msg.nodes));
        else if (msg.kind === 'event' || msg.kind === 'action')
          buf.current.push({ t, node: msg.node ?? '', type: msg.kind === 'action' ? 'chaos' : msg.type, data: msg.kind === 'action' ? { action: msg.action, ...msg } : msg.data });
      };
    };
    connect();
    const tick = window.setInterval(() => {
      const t = (performance.now() - t0) / 1000;
      buf.current = buf.current.filter((e) => e.t > t - 15);
      setRecent([...buf.current]);
      setNow(t);
    }, 100);
    return () => {
      closed = true;
      window.clearTimeout(retry);
      window.clearInterval(tick);
      ws?.close();
    };
  }, [url, enabled]);
  return { status, state, recent, now };
}
