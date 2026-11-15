import { readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { firstSentences, parse } from './decisions.mjs';

const ADR = `# 099: Pick the *gate* over a big lock
- Context: Two things can happen:
  - The key is here.
  - The key moved, see [ADR 033](033-gate.md).
- Options considered:
  1. **A big lock.** Simple.
  2. **A per-slot gate:** finer.
- Decision: Option 2, on by default.
- Why: Measured. It was \`3x\` faster. Third sentence.
- Tradeoffs we accept: More memory.
- What real Redis does: One thread.
- At bigger scale I'd: Keep it.
`;

describe('decision record parser', () => {
  it('reads every field and cleans markdown', () => {
    const r = parse(ADR);
    expect(r.n).toBe(99);
    expect(r.title).toBe('Pick the gate over a big lock');
    expect(r.context).toBe('Two things can happen: The key is here. The key moved, see ADR 033.');
    expect(r.why).toBe('Measured. It was 3x faster. Third sentence.');
    expect(r.scale).toBe('Keep it.');
  });

  it('names the chosen option instead of "Option N"', () => {
    expect(parse(ADR).decision).toBe('A per-slot gate, on by default.');
  });

  it('rejects files without an ADR heading', () => {
    expect(parse('hello')).toBeNull();
  });

  it('trims to the first sentences', () => {
    expect(firstSentences('One. Two! Three?', 2)).toBe('One. Two!');
  });

  it('parses every real record with a decision and a Redis comparison', () => {
    const dir = join(import.meta.dirname, '..', '..', 'docs', 'decisions');
    const files = readdirSync(dir).filter((f) => /^\d{3}-.*\.md$/.test(f) && !f.startsWith('000'));
    expect(files.length).toBeGreaterThan(40);
    for (const f of files) {
      const r = parse(readFileSync(join(dir, f), 'utf8'));
      expect(r, f).not.toBeNull();
      expect(r.decision, f).toBeTruthy();
      expect(r.redis, f).toBeTruthy();
    }
  });
});
