import { useEffect, useMemo, useRef, useState } from 'react';
import { Replayer, markers, type ClusterState, type Trace, type TraceEvent } from './model';

/** prefers-reduced-motion: no flying dots, no autoplay; everything stays steppable. */
export function useReducedMotion(): boolean {
  const q = '(prefers-reduced-motion: reduce)';
  const [reduced, set] = useState(() => typeof matchMedia !== 'undefined' && matchMedia(q).matches);
  useEffect(() => {
    const m = matchMedia(q);
    const on = () => set(m.matches);
    m.addEventListener('change', on);
    return () => m.removeEventListener('change', on);
  }, []);
  return reduced;
}

export function useTrace(name: string): { trace: Trace | null; error: string | null } {
  const [trace, setTrace] = useState<Trace | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    setTrace(null);
    setError(null);
    fetch(`./traces/${name}.json`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`${r.status} ${r.statusText}`))))
      .then((t: Trace) => !cancelled && setTrace(t))
      .catch((e: Error) => !cancelled && setError(e.message));
    return () => {
      cancelled = true;
    };
  }, [name]);
  return { trace, error };
}

export interface Player {
  t: number;
  duration: number;
  playing: boolean;
  speed: number;
  state: ClusterState | null;
  marks: TraceEvent[];
  play(): void;
  pause(): void;
  seek(t: number): void;
  step(dir: 1 | -1): void;
  setSpeed(s: number): void;
}

/** A clock over a trace: play / pause / step between key events / speed / seek. */
export function usePlayer(trace: Trace | null, autoplay: boolean, startAt = 0): Player {
  const replayer = useMemo(() => (trace ? new Replayer(trace) : null), [trace]);
  const marks = useMemo(() => (trace ? markers(trace) : []), [trace]);
  const duration = trace?.duration_s ?? 0;
  const [t, setT] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(1);
  const tRef = useRef(0);
  tRef.current = t;

  useEffect(() => {
    // A deep link (?t=6.3) opens paused at that moment; otherwise start at 0.
    setT(trace ? Math.min(startAt, trace.duration_s) : 0);
    setPlaying(autoplay && trace !== null && startAt === 0);
  }, [trace, autoplay, startAt]);

  useEffect(() => {
    if (!playing) return;
    let raf = 0;
    let last = performance.now();
    const tick = (now: number) => {
      const next = tRef.current + ((now - last) / 1000) * speed;
      last = now;
      if (next >= duration) {
        setT(duration);
        setPlaying(false);
        return;
      }
      setT(next);
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [playing, speed, duration]);

  const state = useMemo(() => (replayer ? { ...replayer.at(t) } : null), [replayer, t]);
  return {
    t, duration, playing, speed, state, marks,
    play: () => {
      if (tRef.current >= duration) setT(0);
      setPlaying(true);
    },
    pause: () => setPlaying(false),
    seek: (x) => setT(Math.min(Math.max(x, 0), duration)),
    step: (dir) => {
      setPlaying(false);
      const cur = tRef.current;
      const next = dir > 0 ? marks.find((m) => m.t > cur + 0.01) : [...marks].reverse().find((m) => m.t < cur - 0.01);
      setT(next ? next.t : dir > 0 ? duration : 0);
    },
    setSpeed,
  };
}
