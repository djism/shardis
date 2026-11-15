# 048: The browser computes key slots itself, kept identical to the server by shared vectors
- Context: "Where does my key live?" types a key and shows CRC16 → slot → node. The browser
  has to compute exactly what the server computes, or the demo lies.
- Options considered:
  1. **Reimplement CRC16-XMODEM + hash tags in TypeScript,** and check both implementations
     against the same vector file generated from the Go code.
  2. **Ask a server** (`CLUSTER KEYSLOT`) for every keystroke: always right, but the public
     site has no server, and we couldn't show the per-byte steps.
  3. **Compile the Go code to WebAssembly:** one implementation, but megabytes of runtime for
     a 20-line function, and still no step-by-step view.
- Decision: Option 1.
- Why:
  - **The vectors make drift impossible to miss.** `internal/keyslot/vectors_test.go` writes
    `website/src/lib/keyslot.vectors.json` (62 keys: the CRC16 check string, hash-tag edge
    cases like `foo{{bar}}zap`, empty tags, UTF-8, NUL bytes) with
    `SHARDIS_WRITE_VECTORS=1`, and verifies it on every `make test`. The TypeScript test
    checks the same file. A change to either implementation fails CI.
  - **The step-by-step view is the point:** `crc16Steps` returns the register before and
    after every byte, which the page shows as a table.
- Tradeoffs we accept:
  - **Two implementations of the same function.** That's acceptable for 20 lines guarded by
    shared vectors.
  - Keys are encoded as UTF-8 in the browser. The server hashes raw bytes, so a client that
    sent another encoding would see different slots. That's the client's business, as in Redis.
- What real Redis does: Every cluster client library reimplements CRC16 the same way (it's
  in the Redis Cluster spec). We're one more client.
- At bigger scale I'd: Publish the vector file as a conformance suite for any client library.
