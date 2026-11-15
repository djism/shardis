# 034: DUMP/RESTORE use our own small payload format, with a version and a CRC-64
- Context: MIGRATE moves a key by sending `RESTORE-ASKING key ttl <payload>` to the target. The
  payload must carry the value safely: it must be binary-safe, catch corruption, and let a newer
  node refuse a payload it doesn't understand instead of misreading it.
- Options considered:
  1. **Our own format:** `version byte | type byte | uvarint length | value | CRC-64 (ECMA)`.
  2. **Redis's RDB encoding** (type byte + RDB-encoded value + 2-byte RDB version + CRC-64 Jones),
     so DUMP output could be RESTOREd into real Redis and back.
  3. **Raw value, no checksum:** smallest, but a flipped bit becomes silently wrong data.
- Decision: Option 1. The TTL travels as a RESTORE argument (relative ms, like Redis), not in
  the payload.
- Why:
  - **Both ends of a MIGRATE are Shardis.** Moving data into or out of real Redis isn't a goal,
    and the RDB format isn't specified on redis.io. We implement from public specs only, so
    we'd be reverse-engineering it.
  - **Self-checking:** `TestDumpRejectsDamage` flips every bit and truncates at every length, and
    all are rejected. `FuzzParseDump` checks that anything accepted re-encodes byte-for-byte, so
    one value never has two encodings.
  - **Versioned:** when lists or hashes arrive, they get new type bytes, and old nodes refuse
    them with an error instead of corrupting data.
- Tradeoffs we accept:
  - A `DUMP` from Shardis can't be `RESTORE`d into real Redis (or the reverse). Our
    `redis-cli --cluster` compatibility still works, because those tools treat the payload as
    opaque and only move it between nodes of the same cluster.
  - 11 bytes of overhead per key (version, type, length, CRC). That's noise next to a TCP round
    trip.
- What real Redis does: The same structure: value, then version, then CRC-64. Its value is
  RDB-encoded, and it uses a different CRC-64 polynomial (Jones). We match the idea, not the
  bytes.
- At bigger scale I'd: Compress large values (Redis LZF-compresses big strings in RDB), and
  stream a whole slot's keys in one framed snapshot rather than one RESTORE per key (see
  ADR 032's atomic-migration alternative).
