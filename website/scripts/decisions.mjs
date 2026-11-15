// Builds src/data/decisions.json from docs/decisions/*.md, so the site's
// "Design decisions" cards are always the real records (ADR 051).
// Runs before `npm run dev` and `npm run build`.
import { readFileSync, readdirSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const clean = (s) => s
  .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1') // links → text
  .replace(/\*\*|__|`/g, '')
  .replace(/(^|[\s(])\*([^*\s][^*]*?)\*(?=[\s.,;:)!?]|$)/g, '$1$2') // *emphasis*
  .replace(/\s+/g, ' ')
  .trim();

const FIELDS = { 'Context': 'context', 'Decision': 'decision', 'Why': 'why', 'Tradeoffs we accept': 'tradeoffs', 'What real Redis does': 'redis', 'What real Redis clients do': 'redis', "At bigger scale I'd": 'scale' };

/** Parses one decision record into { n, title, context, decision, why, tradeoffs, redis, scale }. */
export function parse(md) {
  const lines = md.split('\n');
  const head = /^#\s+(\d+):\s+(.*)$/.exec(lines[0] ?? '');
  if (!head) return null;
  const out = { n: Number(head[1]), title: clean(head[2]) };
  const raw = {};
  let cur = null;
  for (const line of lines.slice(1)) {
    const m = /^- ([^:]+):\s*(.*)$/.exec(line);
    if (m) { cur = FIELDS[m[1]] ?? (/^What real Redis/.test(m[1]) ? 'redis' : m[1] === 'Options considered' ? 'options' : null); if (cur) raw[cur] = [m[2]]; continue; }
    if (cur && line.trim()) raw[cur].push(line);
  }
  // Numbered options, so "Decision: Option 2." can say what option 2 was.
  const options = {};
  let on = null;
  for (const l of raw.options ?? []) {
    const o = /^\s*(\d+)\.\s+(.*)$/.exec(l);
    if (o) { on = o[1]; options[on] = o[2]; } else if (on) options[on] += ' ' + l.trim();
  }
  for (const k of Object.values(FIELDS)) {
    if (!raw[k]) continue;
    // Drop list markers line by line so nested bullets read as sentences.
    out[k] = clean(raw[k].map((l) => l.replace(/^\s*(?:-|\d+\.)\s+/, '')).join(' '));
  }
  if (out.decision) {
    out.decision = out.decision.replace(/^Option (\d+)\b/, (all, d) => {
      const t = options[d] && /\*\*(.+?)\*\*/.exec(options[d]);
      return t ? clean(t[1]).replace(/[.:,]$/, '') : all;
    });
  }
  return out;
}

export function firstSentences(s, n = 2) {
  const parts = (s ?? '').match(/[^.!?]+[.!?]+(\s|$)/g) ?? [s ?? ''];
  return parts.slice(0, n).join('').trim();
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const here = dirname(fileURLToPath(import.meta.url));
  const dir = join(here, '..', '..', 'docs', 'decisions');
  const recs = readdirSync(dir).filter((f) => /^\d{3}-.*\.md$/.test(f) && !f.startsWith('000'))
    .sort().map((f) => parse(readFileSync(join(dir, f), 'utf8'))).filter(Boolean)
    .map((r) => ({ ...r, why: firstSentences(r.why, 2), redis: firstSentences(r.redis, 2), scale: firstSentences(r.scale, 2), context: firstSentences(r.context, 2), tradeoffs: firstSentences(r.tradeoffs, 2) }));
  mkdirSync(join(here, '..', 'src', 'data'), { recursive: true });
  writeFileSync(join(here, '..', 'src', 'data', 'decisions.json'), JSON.stringify(recs, null, 1));
  console.log(`decisions.json: ${recs.length} records`);
}
