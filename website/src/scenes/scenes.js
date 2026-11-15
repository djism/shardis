// Live scenes: canvas simulations whose numbers and timings come from our
// measured runs (ADR 050). Each mount function finds its elements by id,
// animates only while visible, and returns a cleanup function.

const reduced = () => matchMedia('(prefers-reduced-motion: reduce)').matches;
const tok = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
let COL = null;
function colors() {
  if (!COL) COL = { a: tok('--a'), b: tok('--b'), c: tok('--c'), sun: tok('--sun'), sky: tok('--sky'), ok: tok('--ok'), bad: tok('--bad'),
    bg: tok('--bg'), bg2: tok('--bg2'), bg3: tok('--bg3'), fg: tok('--fg'), fg2: tok('--fg2'), fg3: tok('--fg3') };
  return COL;
}
const SH = () => [colors().a, colors().b, colors().c];
const rand = (a, b) => a + Math.random() * (b - a);
const lerp = (a, b, t) => a + (b - a) * t;
const ease = (t) => (t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2);
export const fmt = (n) => Math.round(n).toLocaleString('en-US');

// CRC16-XMODEM + hash tags: the Redis Cluster key → slot function, same as the server.
const TABLE = (() => { const t = new Uint16Array(256); for (let b = 0; b < 256; b++) { let c = b << 8; for (let i = 0; i < 8; i++) c = c & 0x8000 ? ((c << 1) ^ 0x1021) & 0xffff : (c << 1) & 0xffff; t[b] = c; } return t; })();
export function slotOf(key) {
  let bytes = new TextEncoder().encode(key);
  const o = bytes.indexOf(0x7b);
  if (o >= 0) { const e = bytes.indexOf(0x7d, o + 1); if (e > o + 1) bytes = bytes.subarray(o + 1, e); }
  let crc = 0;
  for (const b of bytes) crc = ((crc << 8) ^ TABLE[((crc >> 8) ^ b) & 0xff]) & 0xffff;
  return crc % 16384;
}
export const shardOf = (slot) => (slot <= 5460 ? 0 : slot <= 10922 ? 1 : 2);

/** A canvas that animates only while visible. */
function scene(canvas, draw, opts = {}) {
  const ctx = canvas.getContext('2d');
  let w = 0, h = 0, running = false, last = 0, t = 0, raf = 0;
  const baseH = +canvas.getAttribute('height') || 400;
  function size() {
    const dpr = Math.min(devicePixelRatio || 1, 2);
    const r = canvas.getBoundingClientRect();
    w = r.width;
    h = opts.fill ? r.height : Math.min(baseH, Math.max(300, w * 0.75));
    if (!opts.fill) canvas.style.height = h + 'px';
    canvas.width = Math.round(w * dpr); canvas.height = Math.round(h * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    opts.resize && opts.resize(w, h);
    frame(0);
  }
  function frame(dt) { ctx.clearRect(0, 0, w, h); draw(ctx, w, h, dt, t); }
  function loop(now) {
    if (!running) return;
    const dt = Math.min((now - last) / 1000, 0.05); last = now; t += dt;
    frame(dt); raf = requestAnimationFrame(loop);
  }
  const ro = new ResizeObserver(size); ro.observe(canvas);
  let io = null;
  if (!reduced()) {
    io = new IntersectionObserver(([e]) => {
      if (e.isIntersecting && !running) { running = true; last = performance.now(); raf = requestAnimationFrame(loop); }
      else if (!e.isIntersecting) running = false;
    }, { rootMargin: '100px' });
    io.observe(canvas);
  }
  return () => { running = false; cancelAnimationFrame(raf); ro.disconnect(); io && io.disconnect(); };
}

function glowNode(ctx, x, y, r, color, { primary = true, label = '', sub = '', dead = false, ring = null, alpha = 1 } = {}) {
  const K = colors();
  ctx.save(); ctx.globalAlpha = alpha;
  if (primary && !dead) { const g = ctx.createRadialGradient(x, y, r * 0.2, x, y, r * 2.2); g.addColorStop(0, color + '55'); g.addColorStop(1, color + '00'); ctx.fillStyle = g; ctx.beginPath(); ctx.arc(x, y, r * 2.2, 0, Math.PI * 2); ctx.fill(); }
  ctx.beginPath(); ctx.arc(x, y, r, 0, Math.PI * 2);
  ctx.fillStyle = dead ? K.bg3 : primary ? color : K.bg; ctx.fill();
  ctx.lineWidth = 2.5; ctx.strokeStyle = dead ? K.fg3 : color; ctx.stroke();
  if (ring) { ctx.beginPath(); ctx.arc(x, y, r + 7, 0, Math.PI * 2); ctx.strokeStyle = ring; ctx.lineWidth = 3; ctx.setLineDash([5, 5]); ctx.stroke(); ctx.setLineDash([]); }
  if (dead) { ctx.strokeStyle = K.bad; ctx.lineWidth = 3; ctx.beginPath(); ctx.moveTo(x - r * 0.45, y - r * 0.45); ctx.lineTo(x + r * 0.45, y + r * 0.45); ctx.moveTo(x + r * 0.45, y - r * 0.45); ctx.lineTo(x - r * 0.45, y + r * 0.45); ctx.stroke(); }
  if (label) { ctx.fillStyle = primary && !dead ? K.bg : K.fg; ctx.font = `700 ${Math.round(r * 0.62)}px Bricolage Grotesque, system-ui`; ctx.textAlign = 'center'; ctx.textBaseline = 'middle'; ctx.fillText(label, x, y + 1); }
  if (sub) { ctx.fillStyle = K.fg2; ctx.font = '500 12px DM Sans, system-ui'; ctx.textAlign = 'center'; ctx.textBaseline = 'top'; ctx.fillText(sub, x, y + r + 10); }
  ctx.restore();
}
function dotAt(ctx, x, y, r, color) { ctx.beginPath(); ctx.arc(x, y, r, 0, Math.PI * 2); ctx.fillStyle = color; ctx.fill(); }
function curve(p0, p1, bend, t) { const mx = (p0.x + p1.x) / 2 - (p1.y - p0.y) * bend, my = (p0.y + p1.y) / 2 + (p1.x - p0.x) * bend; const u = 1 - t; return { x: u * u * p0.x + 2 * u * t * mx + t * t * p1.x, y: u * u * p0.y + 2 * u * t * my + t * t * p1.y }; }
function pill(ctx, x, y, text, bg, fg) { ctx.font = '700 11px JetBrains Mono, monospace'; const w = ctx.measureText(text).width + 16; ctx.fillStyle = bg; ctx.beginPath(); ctx.roundRect(x - w / 2, y - 10, w, 20, 10); ctx.fill(); ctx.fillStyle = fg; ctx.textAlign = 'center'; ctx.textBaseline = 'middle'; ctx.fillText(text, x, y + 1); }
const $ = (id) => document.getElementById(id);

// ================= HERO: the cluster breathing =================
export function mountHero() {
  const K = colors(), S = SH();
  let nodes = [], parts = [], gossip = [], copies = 0;
  const repEl = $('heroRep');
  const layout = (w, h) => {
    const wide = w > 900, cx = wide ? w * 0.7 : w * 0.5, cy = wide ? h * 0.52 : h * 0.8, R = Math.min(wide ? w * 0.18 : w * 0.3, 250);
    nodes = [];
    for (let s = 0; s < 3; s++) {
      const ang = -Math.PI / 2 + (s * Math.PI * 2) / 3;
      nodes.push({ x: cx + Math.cos(ang) * R, y: cy + Math.sin(ang) * R, s, primary: true });
      nodes.push({ x: cx + Math.cos(ang) * R * 1.55, y: cy + Math.sin(ang) * R * 1.55, s, primary: false });
    }
  };
  return scene($('heroCanvas'), (ctx, w, h, dt) => {
    if (!nodes.length) layout(w, h);
    ctx.lineWidth = 1;
    for (let i = 0; i < nodes.length; i++) for (let j = i + 1; j < nodes.length; j++) { ctx.strokeStyle = 'rgba(87,199,255,.07)'; ctx.beginPath(); ctx.moveTo(nodes[i].x, nodes[i].y); ctx.lineTo(nodes[j].x, nodes[j].y); ctx.stroke(); }
    for (let s = 0; s < 3; s++) { const p = nodes[s * 2], r = nodes[s * 2 + 1]; ctx.strokeStyle = S[s] + '55'; ctx.setLineDash([4, 6]); ctx.beginPath(); ctx.moveTo(p.x, p.y); ctx.lineTo(r.x, r.y); ctx.stroke(); ctx.setLineDash([]); }
    if (Math.random() < dt * 28) { const s = Math.floor(Math.random() * 3); parts.push({ from: { x: -10, y: rand(h * 0.1, h * 0.95) }, to: nodes[s * 2], s, t: 0, sp: rand(0.5, 0.9), kind: 'req' }); }
    if (Math.random() < dt * 6) { const a = nodes[Math.floor(Math.random() * 6)], b = nodes[Math.floor(Math.random() * 6)]; if (a !== b) gossip.push({ a, b, t: 0 }); }
    parts = parts.filter((p) => {
      p.t += dt * p.sp;
      if (p.t >= 1) { if (p.kind === 'req') parts.push({ from: p.to, to: nodes[p.s * 2 + 1], s: p.s, t: 0, sp: 1.6, kind: 'rep' }); else copies++; return false; }
      const q = p.kind === 'req' ? curve(p.from, p.to, 0.15, ease(p.t)) : { x: lerp(p.from.x, p.to.x, p.t), y: lerp(p.from.y, p.to.y, p.t) };
      dotAt(ctx, q.x, q.y, p.kind === 'req' ? 3 : 2.2, S[p.s]);
      return true;
    });
    gossip = gossip.filter((g) => { g.t += dt * 1.4; if (g.t >= 1) return false; const q = curve(g.a, g.b, 0.25, g.t); dotAt(ctx, q.x, q.y, 2, K.sky); return true; });
    const r = Math.max(18, Math.min(34, w / 40));
    nodes.forEach((n) => glowNode(ctx, n.x, n.y, n.primary ? r : r * 0.78, S[n.s], { primary: n.primary, label: 'ABC'[n.s] + (n.primary ? '' : '′'), alpha: 0.95 }));
    if (repEl) repEl.textContent = fmt(copies);
  }, { fill: true, resize: layout });
}

// ================= 1. ONE REQUEST =================
export function mountMall() {
  const K = colors(), S = SH();
  const stepsEl = $('mallSteps'), cmdEl = $('mallCmd'), form = $('keyForm'), input = $('keyInput');
  const STEPS = [
    ['0.0 ms', 'Alice taps “Add to cart”'],
    ['0.0 ms', 'her app hashes the key: CRC16 mod 16384'],
    ['', ''],
    ['0.1 ms', 'the primary writes it, holding one of 256 locks'],
    ['0.2 ms', 'and answers OK'],
    ['+1 ms', 'then copies the write to its replica'],
    ['≤ 1 s', 'and appends it to the log on disk (fsync every second)'],
    ['30 min', 'EX 1800: the key expires on its own'],
  ];
  const DUR = [1.1, 1.4, 1.5, 1.3, 1.0, 1.2, 1.0, 1.6];
  let key = 'cart:alice', slot = slotOf(key), shard = shardOf(slot), step = 0, stepT = 0, playing = true, offset = 48213, timer = 0;
  function render() {
    STEPS[2] = ['0.0 ms', `slot ${fmt(slot)} belongs to shard ${'ABC'[shard]}`];
    stepsEl.replaceChildren(...STEPS.map((s, i) => {
      const li = document.createElement('li'); li.className = i === step ? 'on' : i < step ? 'done' : '';
      const a = document.createElement('span'); a.className = 't'; a.textContent = s[0];
      const b = document.createElement('span'); b.textContent = s[1];
      li.append(a, b); return li;
    }));
  }
  function restart(k) { key = k; slot = slotOf(k); shard = shardOf(slot); step = 0; stepT = 0; playing = true; offset = 48213; cmdEl.textContent = `SET ${k} sneakers EX 1800`; render(); }
  const onSubmit = (e) => { e.preventDefault(); restart(input.value.trim().slice(0, 64) || 'cart:alice'); };
  form.addEventListener('submit', onSubmit);
  render();
  const stop = scene($('mallCanvas'), (ctx, w, h, dt) => {
    if (reduced()) step = STEPS.length - 1;
    else if (playing) { stepT += dt; if (stepT > DUR[step]) { stepT = 0; step++; if (step >= STEPS.length) { step = STEPS.length - 1; playing = false; timer = setTimeout(() => restart(key), 2400); } render(); } }
    const k = Math.min(stepT / DUR[step], 1);
    const phone = { x: w * 0.1, y: h * 0.5 }, hash = { x: w * 0.3, y: h * 0.5 };
    const cols = [w * 0.55, w * 0.72, w * 0.89].map((x) => ({ p: { x, y: h * 0.32 }, r: { x, y: h * 0.7 } }));
    const disk = { x: cols[shard].p.x, y: h * 0.1 };
    const bx = w * 0.2, bw = w * 0.76, by = h * 0.93;
    [[0, 5460], [5461, 10922], [10923, 16383]].forEach(([a, b], i) => { ctx.fillStyle = S[i] + (step >= 2 && i === shard ? 'ff' : '44'); ctx.beginPath(); ctx.roundRect(bx + (bw * a) / 16384, by, (bw * (b - a + 1)) / 16384 - 3, 8, 4); ctx.fill(); });
    if (step >= 2) { const sx = bx + (bw * slot) / 16384; ctx.fillStyle = K.fg; ctx.beginPath(); ctx.moveTo(sx, by - 2); ctx.lineTo(sx - 6, by - 12); ctx.lineTo(sx + 6, by - 12); ctx.fill(); }
    ctx.fillStyle = K.fg3; ctx.font = '500 11px JetBrains Mono, monospace'; ctx.textAlign = 'left'; ctx.textBaseline = 'alphabetic'; ctx.fillText('16,384 slots', bx, by - 8);
    ctx.fillStyle = K.bg3; ctx.beginPath(); ctx.roundRect(phone.x - 26, phone.y - 46, 52, 92, 12); ctx.fill();
    ctx.fillStyle = step === 0 ? K.sun : step >= 4 ? K.ok : K.fg3; ctx.beginPath(); ctx.roundRect(phone.x - 18, phone.y - 34, 36, 54, 6); ctx.fill();
    ctx.fillStyle = K.bg; ctx.font = '700 10px DM Sans'; ctx.textAlign = 'center'; ctx.fillText(step >= 4 ? 'OK' : 'cart', phone.x, phone.y - 6);
    ctx.fillStyle = K.fg2; ctx.font = '500 12px DM Sans'; ctx.fillText('Alice', phone.x, phone.y + 62);
    ctx.fillStyle = step === 1 ? K.sun : K.bg3; ctx.beginPath(); ctx.roundRect(hash.x - 58, hash.y - 30, 116, 60, 14); ctx.fill();
    ctx.fillStyle = step === 1 ? K.bg : K.fg; ctx.font = '700 13px JetBrains Mono'; ctx.fillText('CRC16', hash.x, hash.y - 8);
    const shown = step === 1 ? Math.floor(Math.random() * 16384) : step > 1 ? slot : 0;
    ctx.font = '700 15px JetBrains Mono'; ctx.fillText(step >= 1 ? fmt(shown) : '?', hash.x, hash.y + 12);
    cols.forEach((c, i) => {
      ctx.strokeStyle = S[i] + '66'; ctx.setLineDash([4, 6]); ctx.beginPath(); ctx.moveTo(c.p.x, c.p.y); ctx.lineTo(c.r.x, c.r.y); ctx.stroke(); ctx.setLineDash([]);
      const on = i === shard && step >= 2;
      glowNode(ctx, c.p.x, c.p.y, 26, S[i], { label: 'ABC'[i], sub: i === shard && step >= 3 ? 'primary · writing' : 'primary', alpha: on || step < 2 ? 1 : 0.35 });
      glowNode(ctx, c.r.x, c.r.y, 20, S[i], { primary: false, label: 'ABC'[i] + '′', sub: i === shard && step >= 5 ? `offset ${fmt(offset)}` : 'replica', alpha: on || step < 2 ? 1 : 0.35 });
    });
    if (step >= 6) { ctx.fillStyle = step === 6 ? K.sun : K.bg3; ctx.beginPath(); ctx.roundRect(disk.x - 30, disk.y - 14, 60, 28, 8); ctx.fill(); ctx.fillStyle = step === 6 ? K.bg : K.fg; ctx.font = '700 11px JetBrains Mono'; ctx.textBaseline = 'middle'; ctx.fillText('AOF', disk.x, disk.y + 1); ctx.textBaseline = 'alphabetic'; }
    const P = cols[shard].p, R = cols[shard].r;
    let pos = null, color = S[shard];
    if (step === 0) pos = { x: phone.x, y: phone.y - 60 - 6 * Math.sin(k * Math.PI) };
    if (step === 1) pos = curve(phone, hash, 0.1, ease(k));
    if (step === 2) pos = curve(hash, P, -0.12, ease(k));
    if (step === 3) { pos = { x: P.x, y: P.y }; ctx.beginPath(); ctx.arc(P.x, P.y, 26 + 18 * k, 0, Math.PI * 2); ctx.strokeStyle = color + Math.round((1 - k) * 255).toString(16).padStart(2, '0'); ctx.lineWidth = 3; ctx.stroke(); }
    if (step === 4) { pos = curve(P, phone, 0.18, ease(k)); color = K.ok; }
    if (step === 5) { pos = { x: lerp(P.x, R.x, ease(k)), y: lerp(P.y, R.y, ease(k)) }; if (k > 0.95) offset = 48213 + 34; }
    if (step === 6) pos = { x: lerp(P.x, disk.x, ease(k)), y: lerp(P.y, disk.y, ease(k)) };
    if (step === 7) { ctx.beginPath(); ctx.arc(R.x, R.y, 32, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * (1 - k)); ctx.strokeStyle = K.sun; ctx.lineWidth = 3; ctx.stroke(); pill(ctx, R.x, R.y + 58, 'TTL 30:00', K.sun, K.bg); }
    if (pos) { dotAt(ctx, pos.x, pos.y, 9, color); ctx.beginPath(); ctx.arc(pos.x, pos.y, 15, 0, Math.PI * 2); ctx.strokeStyle = color + '55'; ctx.lineWidth = 6; ctx.stroke(); }
  });
  return () => { stop(); clearTimeout(timer); form.removeEventListener('submit', onSubmit); };
}

// ================= 2. MILLIONS =================
export function mountStorm() {
  const S = SH(), K = colors();
  const dial = $('stormDial'), countEl = $('stormCount');
  let parts = [], reps = [], locks = [new Float32Array(64), new Float32Array(64), new Float32Array(64)], shown = 1000;
  return scene($('stormCanvas'), (ctx, w, h, dt) => {
    const L = Math.pow(10, +dial.value);
    shown = lerp(shown, L, Math.min(dt * 3, 1) || 1); countEl.textContent = fmt(shown);
    const rate = Math.min(20 + Math.log10(L + 1) * 140, 900);
    const prims = [w * 0.2, w * 0.5, w * 0.8].map((x) => ({ x, y: h * 0.36 }));
    const repls = prims.map((p) => ({ x: p.x, y: h * 0.82 }));
    const n = rate * dt;
    for (let i = 0; i < n; i++) { const s = Math.floor(Math.random() * 3); parts.push({ x: rand(-20, w + 20), y: -10, s, tx: prims[s].x + rand(-14, 14), ty: prims[s].y, t: 0, sp: rand(0.7, 1.2) }); }
    parts = parts.filter((p) => { p.t += dt * p.sp; if (p.t >= 1) { locks[p.s][Math.floor(Math.random() * 64)] = 1; if (Math.random() < 0.5) reps.push({ s: p.s, t: 0 }); return false; } const q = curve({ x: p.x, y: p.y }, { x: p.tx, y: p.ty }, 0.08, p.t); dotAt(ctx, q.x, q.y, 2.2, S[p.s]); return true; });
    reps = reps.filter((r) => { r.t += dt * 1.8; if (r.t >= 1) return false; const p = prims[r.s], q = repls[r.s]; dotAt(ctx, p.x + 30, lerp(p.y + 40, q.y - 30, r.t), 1.8, S[r.s]); return true; });
    prims.forEach((p, i) => {
      glowNode(ctx, p.x, p.y, 32, S[i], { label: 'ABC'[i] });
      const g = 8, cs = Math.max(5, Math.min(7, w / 120)), gx = p.x - (g * (cs + 2)) / 2, gy = p.y + 50, l = locks[i];
      for (let k = 0; k < 64; k++) { l[k] = Math.max(0, l[k] - dt * 2.4); ctx.fillStyle = S[i]; ctx.globalAlpha = 0.12 + l[k] * 0.88; ctx.fillRect(gx + (k % g) * (cs + 2), gy + Math.floor(k / g) * (cs + 2), cs, cs); }
      ctx.globalAlpha = 1; ctx.fillStyle = K.fg3; ctx.font = '500 11px JetBrains Mono'; ctx.textAlign = 'center'; ctx.textBaseline = 'alphabetic'; ctx.fillText('lock groups', p.x, gy + g * (cs + 2) + 16);
      glowNode(ctx, repls[i].x, repls[i].y, 24, S[i], { primary: false, label: 'ABC'[i] + '′', sub: 'replica' });
      ctx.fillStyle = K.fg; ctx.font = '800 15px Bricolage Grotesque'; ctx.textAlign = 'center'; ctx.fillText(fmt(shown / 3) + '/s', p.x, p.y - 58);
    });
  });
}

// ================= 3. TTL & LRU =================
export function mountTTL() {
  const S = SH(), K = colors();
  const expEl = $('ttlExp'), evEl = $('ttlEv'), hitEl = $('ttlHit'), burst = $('ttlBurst');
  let keys = [], pops = [], expired = 0, evicted = 0, hits = 0, gets = 0, clock = 0, sample = null, W = 800, H = 440, evictTimer = 0;
  const MAXMEM = 2600;
  const names = ['cart', 'session', 'user', 'feed', 'price', 'token', 'page', 'stock'];
  function addKey() {
    const r = rand(10, 22), ttl = Math.random() < 0.7 ? rand(6, 22) : 0;
    keys.push({ x: rand(90 + r, W - r - 20), y: rand(r + 20, H - r - 20), r, vx: rand(-12, 12), vy: rand(-12, 12), ttl, left: ttl, used: clock, name: names[Math.floor(Math.random() * names.length)] + ':' + Math.floor(rand(1, 999)), s: Math.floor(Math.random() * 3), flash: 0 });
  }
  for (let i = 0; i < 26; i++) addKey();
  const onBurst = () => { for (let i = 0; i < 40; i++) addKey(); };
  burst.addEventListener('click', onBurst);
  const mem = () => keys.reduce((a, k) => a + (k.r * k.r) / 4, 0);
  const stop = scene($('ttlCanvas'), (ctx, w, h, dt) => {
    W = w; H = h; clock += dt;
    if (Math.random() < dt * 8 && keys.length) { const k = keys[Math.floor(Math.random() * keys.length)]; k.used = clock; k.flash = 1; hits++; gets++; }
    if (Math.random() < dt * 1.5) gets++;
    if (keys.length < 18 && Math.random() < dt * 2) addKey();
    keys = keys.filter((k) => {
      k.x += k.vx * dt; k.y += k.vy * dt;
      if (k.x < 90 + k.r || k.x > w - k.r - 10) k.vx *= -1;
      if (k.y < k.r + 10 || k.y > h - k.r - 10) k.vy *= -1;
      if (k.ttl) { k.left -= dt; if (k.left <= 0) { expired++; pops.push({ x: k.x, y: k.y, r: k.r, c: K.sun, t: 0 }); return false; } }
      return true;
    });
    evictTimer -= dt;
    if (mem() > MAXMEM && evictTimer <= 0 && keys.length > 5) {
      const pick = []; while (pick.length < 5) { const k = keys[Math.floor(Math.random() * keys.length)]; if (!pick.includes(k)) pick.push(k); }
      sample = { pick, victim: pick.reduce((a, b) => (a.used < b.used ? a : b)), t: 0 }; evictTimer = 0.9;
    }
    if (sample) { sample.t += dt; if (sample.t > 0.7 && keys.includes(sample.victim)) { keys = keys.filter((k) => k !== sample.victim); evicted++; pops.push({ x: sample.victim.x, y: sample.victim.y, r: sample.victim.r, c: K.bad, t: 0 }); } if (sample.t > 1) sample = null; }
    const m = mem(), gx = 26, gy = 30, gh = h - 60;
    ctx.fillStyle = K.bg3; ctx.beginPath(); ctx.roundRect(gx, gy, 22, gh, 11); ctx.fill();
    const fillH = Math.min(m / (MAXMEM * 1.15), 1) * gh;
    ctx.fillStyle = m > MAXMEM ? K.bad : K.c; ctx.beginPath(); ctx.roundRect(gx, gy + gh - fillH, 22, fillH, 11); ctx.fill();
    const lim = gy + gh - gh / 1.15; ctx.strokeStyle = K.sun; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(gx - 6, lim); ctx.lineTo(gx + 28, lim); ctx.stroke();
    ctx.fillStyle = K.sun; ctx.font = '600 11px JetBrains Mono'; ctx.textAlign = 'left'; ctx.textBaseline = 'alphabetic'; ctx.fillText('maxmemory', gx + 34, lim + 4);
    keys.forEach((k) => {
      k.flash = Math.max(0, k.flash - dt * 2.5);
      const age = Math.min((clock - k.used) / 12, 1);
      ctx.globalAlpha = 1 - age * 0.55;
      ctx.beginPath(); ctx.arc(k.x, k.y, k.r, 0, Math.PI * 2); ctx.fillStyle = S[k.s] + '33'; ctx.fill(); ctx.strokeStyle = S[k.s]; ctx.lineWidth = 1.5; ctx.stroke();
      if (k.ttl) { ctx.beginPath(); ctx.arc(k.x, k.y, k.r + 4, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * (k.left / k.ttl)); ctx.strokeStyle = K.sun; ctx.lineWidth = 2.5; ctx.stroke(); }
      if (k.flash) { ctx.beginPath(); ctx.arc(k.x, k.y, k.r + 9 * k.flash, 0, Math.PI * 2); ctx.strokeStyle = K.fg; ctx.globalAlpha = k.flash; ctx.lineWidth = 1.5; ctx.stroke(); }
      ctx.globalAlpha = 1;
      if (k.r > 15) { ctx.fillStyle = K.fg2; ctx.font = '500 9.5px JetBrains Mono'; ctx.textAlign = 'center'; ctx.fillText(k.name, k.x, k.y + 3); }
    });
    if (sample) sample.pick.forEach((k) => { ctx.beginPath(); ctx.arc(k.x, k.y, k.r + 10, 0, Math.PI * 2); ctx.strokeStyle = k === sample.victim ? K.bad : K.sun; ctx.setLineDash([3, 4]); ctx.lineWidth = 2; ctx.stroke(); ctx.setLineDash([]); if (k === sample.victim) pill(ctx, k.x, k.y - k.r - 22, 'least recently used', K.bad, '#fff'); });
    pops = pops.filter((p) => { p.t += dt * 2; if (p.t >= 1) return false; ctx.beginPath(); ctx.arc(p.x, p.y, p.r + p.t * 26, 0, Math.PI * 2); ctx.strokeStyle = p.c; ctx.globalAlpha = 1 - p.t; ctx.lineWidth = 3; ctx.stroke(); ctx.globalAlpha = 1; return true; });
    expEl.textContent = fmt(expired); evEl.textContent = fmt(evicted); hitEl.textContent = gets ? Math.round((100 * hits) / gets) + '%' : '–';
  });
  return () => { stop(); burst.removeEventListener('click', onBurst); };
}

// ================= REDIRECTS: MOVED vs ASK =================
export function mountRedirects() {
  const S = SH(), K = colors();
  const capEl = $('redirCap'), movedEl = $('redirMoved'), askEl = $('redirAsk');
  // A scripted conversation between one client and the cluster. Each step:
  // [from, to, label, color key, caption]
  const STEPS = [
    ['client', 'A', 'GET user:42', 'fg', 'The client’s slot map is stale: it thinks slot 15,880 lives on A.'],
    ['A', 'client', 'MOVED 15880 C', 'sun', 'A answers MOVED: “that slot lives on C now, update your map.”'],
    ['client', 'C', 'GET user:42', 'fg', 'The client fixes its map and asks C. From now on it goes straight there.'],
    ['C', 'client', '"hello"', 'ok', 'C owns the slot and answers.'],
    ['client', 'C', 'GET user:42', 'fg', 'Later, slot 15,880 is being moved from C to the new node G…'],
    ['C', 'client', 'ASK 15880 G', 'b', '…and this key has already moved. C answers ASK: “try G, just this once.”'],
    ['client', 'G', 'ASKING + GET user:42', 'fg', 'The client sends ASKING to G, so G serves a slot it doesn’t own yet. The map stays as it is.'],
    ['G', 'client', '"hello"', 'ok', 'G answers. When the whole slot has moved, C starts saying MOVED, and the map changes for good.'],
  ];
  let step = 0, t = 0, moved = 0, ask = 0;
  const DUR = 1.9;
  return scene($('redirCanvas'), (ctx, w, h, dt) => {
    if (!reduced()) { t += dt; if (t > DUR) { t = 0; step = (step + 1) % STEPS.length; if (STEPS[step][2].startsWith('MOVED')) moved++; if (STEPS[step][2].startsWith('ASK ')) ask++; } }
    const pos = { client: { x: w * 0.12, y: h * 0.5 }, A: { x: w * 0.55, y: h * 0.2 }, B: { x: w * 0.78, y: h * 0.2 }, C: { x: w * 0.55, y: h * 0.75 }, G: { x: w * 0.82, y: h * 0.72 } };
    const migrating = step >= 4;
    // migration arrow C → G
    if (migrating) { ctx.strokeStyle = K.b; ctx.setLineDash([6, 6]); ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(pos.C.x + 34, pos.C.y); ctx.lineTo(pos.G.x - 34, pos.G.y); ctx.stroke(); ctx.setLineDash([]); pill(ctx, (pos.C.x + pos.G.x) / 2, pos.C.y - 18, 'slot 15880 migrating', K.b, K.bg); }
    glowNode(ctx, pos.A.x, pos.A.y, 30, S[0], { label: 'A', sub: 'slots 0–5460' });
    glowNode(ctx, pos.B.x, pos.B.y, 30, S[1], { label: 'B', sub: 'slots 5461–10922' });
    glowNode(ctx, pos.C.x, pos.C.y, 30, S[2], { label: 'C', sub: migrating ? 'MIGRATING 15880 → G' : 'slots 10923–16383' });
    glowNode(ctx, pos.G.x, pos.G.y, 30, K.sun, { primary: migrating, label: 'G', sub: migrating ? 'IMPORTING 15880' : 'new, empty' });
    // client
    ctx.fillStyle = K.bg3; ctx.beginPath(); ctx.roundRect(pos.client.x - 46, pos.client.y - 34, 92, 68, 16); ctx.fill();
    ctx.fillStyle = K.fg; ctx.font = '700 14px DM Sans'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle'; ctx.fillText('client', pos.client.x, pos.client.y - 8);
    ctx.fillStyle = K.fg3; ctx.font = '500 11px JetBrains Mono'; ctx.fillText(step >= 2 ? 'map: 15880→C' : 'map: 15880→A', pos.client.x, pos.client.y + 12);
    const [from, to, label, ck] = STEPS[step];
    const k = Math.min(t / (DUR * 0.7), 1);
    // Leave from / arrive at the client box's right edge so the message never covers its label.
    const edge = (n) => (n === 'client' ? { x: pos.client.x + 58, y: pos.client.y } : pos[n]);
    const p = curve(edge(from), edge(to), from === 'client' ? -0.08 : 0.08, ease(k));
    const col = K[ck] || K.fg;
    dotAt(ctx, p.x, p.y, 7, col);
    pill(ctx, p.x, p.y - 22, label, col, K.bg);
    capEl.textContent = STEPS[step][4];
    movedEl.textContent = fmt(moved); askEl.textContent = fmt(ask);
  });
}

// ================= 4. BREAK IT (interactive simulation) =================
export function mountBreak() {
  const S = SH(), K = colors();
  const stateEl = $('breakState');
  const tl = [...document.querySelectorAll('#breakTimeline li')];
  const btn = { kill: $('bKill'), part: $('bPart'), heal: $('bHeal'), reset: $('bReset') };
  let N = [], st, reqs = [], pings = [], votes = [];
  function reset() {
    st = { mode: 'ok', t: 0, aDead: false, pfail: false, fail: false, votesSent: false, promoted: false, minority: false, alone: 0, healed: false, healT: 0 };
    reqs = []; pings = []; votes = [];
    tl.forEach((li) => li.classList.remove('on'));
    btn.kill.disabled = false; btn.part.disabled = false; btn.heal.disabled = true;
  }
  reset();
  const onKill = () => { reset(); st.mode = 'kill'; st.aDead = true; btn.kill.disabled = btn.part.disabled = true; };
  const onPart = () => { reset(); st.mode = 'part'; btn.kill.disabled = btn.part.disabled = true; };
  const onHeal = () => { st.healed = true; st.healT = st.t; btn.heal.disabled = true; };
  btn.kill.addEventListener('click', onKill); btn.part.addEventListener('click', onPart); btn.heal.addEventListener('click', onHeal); btn.reset.addEventListener('click', reset);
  const layout = (w, h) => { const xs = [w * 0.25, w * 0.5, w * 0.75]; N = xs.map((x, i) => ({ x, y: h * 0.36, s: i, primary: true })).concat(xs.map((x, i) => ({ x, y: h * 0.78, s: i, primary: false }))); };
  const stop = scene($('breakCanvas'), (ctx, w, h, dt) => {
    if (!N.length) layout(w, h);
    if (st.mode !== 'ok') st.t += dt;
    const t = st.t;
    if (st.mode !== 'ok') {
      tl.forEach((li) => li.classList.toggle('on', t >= +li.dataset.at));
      if (t >= 2) st.pfail = true;
      if (t >= 2.5) st.fail = true;
      if (t >= 2.9 && !st.votesSent) { st.votesSent = true; [1, 2].forEach((i) => votes.push({ from: N[3], to: N[i], t: 0, back: false })); }
      if (t >= 3.2) st.promoted = true;
      if (st.mode === 'part' && t >= 2) st.minority = true;
      if (st.mode === 'part' && t >= 3.2 && !st.healed) btn.heal.disabled = false;
    }
    const A = N[0], A2 = N[3], cut = st.mode === 'part' && !st.healed;
    if (cut) { ctx.strokeStyle = K.sun; ctx.lineWidth = 2; ctx.setLineDash([10, 8]); ctx.beginPath(); ctx.moveTo(w * 0.375, 10); ctx.bezierCurveTo(w * 0.36, h * 0.3, w * 0.4, h * 0.6, w * 0.375, h - 10); ctx.stroke(); ctx.setLineDash([]); pill(ctx, w * 0.375, 22, 'network cut', K.sun, K.bg); }
    if (Math.random() < dt * 9) { const a = N[Math.floor(Math.random() * 6)], b = N[Math.floor(Math.random() * 6)]; if (a !== b) pings.push({ a, b, t: 0 }); }
    pings = pings.filter((p) => {
      p.t += dt * 1.6; if (p.t >= 1) return false;
      const crosses = cut && (p.a === A) !== (p.b === A), toDead = st.aDead && (p.b === A || p.a === A);
      if ((crosses || toDead) && p.t > 0.5) return false;
      const q = curve(p.a, p.b, 0.18, p.t); dotAt(ctx, q.x, q.y, 2.4, K.sky); return true;
    });
    if (Math.random() < dt * 22) reqs.push({ s: Math.floor(Math.random() * 3), y: rand(h * 0.15, h * 0.6), t: 0 });
    reqs = reqs.filter((r) => {
      r.t += dt * 1.2; let target = N[r.s];
      if (r.s === 0 && st.promoted && !(cut && r.y < h * 0.35)) target = A2;
      const blocked = r.s === 0 && ((st.aDead && !st.promoted) || (st.mode === 'part' && st.minority && target === A));
      if (r.t >= 1) { if (r.s === 0 && st.mode === 'part' && target === A && !st.minority) st.alone++; return false; }
      const q = curve({ x: -10, y: r.y }, target, 0.1, r.t);
      if (blocked && r.t > 0.7) { dotAt(ctx, q.x, q.y, 3, K.fg3); return r.t < 0.85; }
      dotAt(ctx, q.x, q.y, 2.6, S[r.s]); return true;
    });
    votes = votes.filter((v) => { v.t += dt * 2.2; if (v.t >= 1) { if (!v.back) votes.push({ from: v.to, to: v.from, t: 0, back: true }); return false; } const q = curve(v.from, v.to, -0.2, v.t); dotAt(ctx, q.x, q.y, 5, v.back ? K.ok : K.sun); return true; });
    for (let s = 0; s < 3; s++) { ctx.strokeStyle = S[s] + '55'; ctx.setLineDash([4, 6]); ctx.beginPath(); ctx.moveTo(N[s].x, N[s].y); ctx.lineTo(N[s + 3].x, N[s + 3].y); ctx.stroke(); ctx.setLineDash([]); }
    const healedA = st.healed && st.t - st.healT > 0.8;
    N.forEach((n, i) => {
      let primary = n.primary, sub = n.primary ? 'primary' : 'replica', dead = false, ring = null;
      if (i === 0) { dead = st.aDead; if (st.promoted) { primary = false; sub = st.aDead ? 'dead' : healedA ? 'replica of A′' : 'minority · refusing writes'; } else if (st.minority) sub = 'minority · refusing writes'; if (st.fail) ring = K.bad; else if (st.pfail) ring = K.sun; }
      if (i === 3 && st.promoted) { primary = true; sub = 'new primary'; }
      glowNode(ctx, n.x, n.y, n.primary || (i === 3 && st.promoted) ? 32 : 26, S[n.s], { primary, label: 'ABC'[n.s] + (n.primary ? '' : '′'), sub, dead, ring });
    });
    if (st.pfail && !st.fail) [1, 2, 4, 5].forEach((i) => pill(ctx, N[i].x, N[i].y - 50, 'A?', K.sun, K.bg));
    if (st.fail && !st.promoted) pill(ctx, A.x, A.y - 54, 'FAIL', K.bad, '#fff');
    stateEl.textContent = st.mode === 'ok' ? 'healthy · 16,384 slots served'
      : st.mode === 'kill' ? (st.promoted ? 'healed: A′ serves shard A · clients saw 0 errors' : st.fail ? 'A marked FAIL · electing a replacement' : st.pfail ? 'peers suspect A' : 'A is dead · shard A writes pause')
      : st.healed ? (healedA ? `healed · A rejoined as a replica · ${fmt(st.alone)} writes it accepted alone are rolled back (measured on Kubernetes: 2,029)` : 'healing…')
      : st.minority ? `A is cut off: refusing writes after accepting ${fmt(st.alone)} alone` : `A is cut off but doesn’t know yet · accepted alone: ${fmt(st.alone)}`;
  }, { resize: layout });
  return () => { stop(); btn.kill.removeEventListener('click', onKill); btn.part.removeEventListener('click', onPart); btn.heal.removeEventListener('click', onHeal); btn.reset.removeEventListener('click', reset); };
}
