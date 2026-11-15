// Redis Cluster key → hash slot, in the browser. Same algorithm as the Go
// server (internal/keyslot): CRC16-XMODEM over the key's UTF-8 bytes (or
// just its hash tag), mod 16384. keyslot.vectors.json, generated from the Go
// code, keeps the two in lockstep (ADR 048).

export const SLOTS = 16384;

const TABLE: Uint16Array = (() => {
  const t = new Uint16Array(256);
  for (let b = 0; b < 256; b++) {
    let crc = b << 8;
    for (let i = 0; i < 8; i++) crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
    t[b] = crc;
  }
  return t;
})();

export interface CrcStep {
  index: number; // byte position
  byte: number;
  before: number; // CRC register before this byte
  after: number; // and after
}

/** CRC16-XMODEM with every intermediate value, for the animation. */
export function crc16Steps(data: Uint8Array): CrcStep[] {
  const steps: CrcStep[] = [];
  let crc = 0;
  data.forEach((byte, index) => {
    const before = crc;
    crc = ((crc << 8) ^ TABLE[((crc >> 8) ^ byte) & 0xff]) & 0xffff;
    steps.push({ index, byte, before, after: crc });
  });
  return steps;
}

export function crc16(data: Uint8Array): number {
  const s = crc16Steps(data);
  return s.length ? s[s.length - 1].after : 0;
}

export interface HashPart {
  bytes: Uint8Array; // what gets hashed
  tag: [number, number] | null; // [start, end) byte range of the tag inside the key, if one applies
}

/** The spec's hash-tag rule: hash only what's inside the first {...}, if non-empty. */
export function hashPart(key: Uint8Array): HashPart {
  const open = key.indexOf(0x7b); // {
  if (open >= 0) {
    const close = key.indexOf(0x7d, open + 1); // }
    if (close > open + 1) return { bytes: key.subarray(open + 1, close), tag: [open + 1, close] };
  }
  return { bytes: key, tag: null };
}

export function utf8(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

export function keySlot(key: string): number {
  return crc16(hashPart(utf8(key)).bytes) % SLOTS;
}
