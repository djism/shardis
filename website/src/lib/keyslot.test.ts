import { describe, expect, it } from 'vitest';
import vectors from './keyslot.vectors.json';
import { crc16, hashPart, keySlot, utf8 } from './keyslot';

describe('keyslot (must match the Go server)', () => {
  it('agrees with every shared vector', () => {
    expect(vectors.length).toBeGreaterThan(50);
    for (const v of vectors) {
      expect(keySlot(v.key), v.key).toBe(v.slot);
      expect(crc16(hashPart(utf8(v.key)).bytes), v.key).toBe(v.crc16);
    }
  });
  it('has the standard CRC16-XMODEM check value', () => {
    expect(crc16(utf8('123456789'))).toBe(0x31c3);
  });
  it('applies hash tags like the spec', () => {
    expect(keySlot('{user:42}:cart')).toBe(keySlot('{user:42}:profile'));
    expect(hashPart(utf8('foo{}{bar}')).tag).toBeNull();
    expect(new TextDecoder().decode(hashPart(utf8('foo{{bar}}zap')).bytes)).toBe('{bar');
  });
});
