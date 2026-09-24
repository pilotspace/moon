# WS4-protocol-wire — PLAN (wave 1)
personas: `.add/personas/performance-engineer.md` (lead) · `.add/personas/acl-security-gatekeeper.md` (advisor: the parser reads untrusted input — malformed input must never crash or hang)

Review measurement: one `RPUSH` of 1M one-byte elements (7 MB, sent in 64 KiB chunks) 15.3 s vs redis 0.10 s; 300K: 1.32 s vs 33 ms (9× for 3× input — quadratic).

## Issues
1. **moon#1164** incomplete multibulk re-scanned twice per read → O(n²).
   - Must: tri-state flat scan (`Complete | Incomplete | Decline`); `Incomplete` returns `Ok(None)` WITHOUT running `validate_frame`; no span reservation beyond the inline 16 until complete.
   - Must: resumable progress for the frame at the buffer front (cursor reset by any consuming op on the buffer — no pointer-keyed cache), so total work for an n-element frame is O(n).
   - Should: size reads from the parse hint when the front frame is incomplete (remaining bulk bytes / geometric growth, capped by the query-buffer limit; pre-auth clients stay capped).
   - Tests: equivalence with the two-pass parser on every prefix of randomized frames (property test); a chunked-feed test whose op/wall budget is linear; keep the `resp_parse_fused` fuzz target building (`cargo check --manifest-path fuzz/Cargo.toml --all-targets` — if the fuzz workspace cannot build here, say so in SUMMARY); the replica apply loop (`replication/apply.rs`) must use the same fixed parser.
   - Evidence: re-run the 100K/300K/1M RPUSH sweep against the baseline binary and redis.
2. **moon#1179** wire-path allocation/copy overhead (one commit per numbered item, all `refs moon#1179`, last one `fixes`):
   1. `FrameVec` → `Vec<Frame>` newtype with the same API (no call-site churn beyond what the compiler forces).
   2. `Frame::VerbatimString.encoding: [u8; 3]` → `size_of::<Frame>() == 40` (update the size test; fix all construct/match sites; RESP3 serialization byte-identical).
   3. `frames`/`responses` scratch allocated lazily and released in `downshift_idle_buffers`; per-shard scratch lazily allocated on first cross-shard use.
   4. monoio read path: read directly into `read_buf` spare capacity where the arm always returns the buffer (plain read, idle_park_read); keep a small rent buffer only for `select!` arms; replace the per-batch >16 KiB shrink with hysteresis.
   5. Pipeline deferral: carry parsed `frames[from..]` to the next iteration instead of re-serializing + re-parsing the tail (bytes only when migrating/task-parking). Both handlers.
   6. Inline GET `split_to` → `advance`; flush `split().freeze()` → pass `write_buf` by ownership; fused digit loop for bulk/count headers (decline to the old path on anything unusual); inline (telnet) protocol freezes the line once and slices.
   7. Remove dead `parse_single_frame` if truly unused (whole-repo grep incl. tests/benches/fuzz).
   - Evidence: `cargo bench --bench resp_parsing` is too heavy for this box — prefer allocation-count unit tests (counting allocator or the existing test helpers) + one release-fast A/B of `redis-benchmark -P 16 -r 1000000 SET key:__rand_int__ xxx EX 100` and `-d 65536 -t set` vs the baseline binary.

## Owned files
`src/protocol/**`, `src/server/codec*` / response-writing helpers, `src/server/conn/handler_monoio/mod.rs` — ONLY the read loop, rent/read buffer handling, batch-end shrink, deferral-carry block and `frames`/`responses` scratch; `src/server/conn/handler_monoio/idle_park.rs`; `src/server/conn/handler_sharded/mod.rs` — ONLY the equivalent regions + per-batch allocations; `src/server/conn/blocking.rs` — ONLY the inline GET `split_to` sites; `src/server/conn/util.rs`; `src/protocol/inline.rs`; `src/replication/apply.rs` parse call; `benches/resp_parsing.rs`; `fuzz/**` (only if adding/adjusting parser targets + both matrices in `.github/workflows/fuzz.yml`); tests `tests/perf_ws4_*.rs`.

## Not yours (wave 2 will edit these regions — keep your diff out of them)
ACL gates, CLIENT PAUSE, tracking gates, `can_inline_*` conjunctions, remote-dispatch/`remote_groups` blocks (4180–4400 in handler_monoio/mod.rs), AOF/replication calls, metrics calls; everything under `src/shard/`, `src/storage/`, `src/command/`.
