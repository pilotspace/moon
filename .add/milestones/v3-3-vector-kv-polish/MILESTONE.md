# MILESTONE: Vector & KV Latent-Correctness + Hot-Path Polish

goal: Moon's vector segments decode at the correct code length (SQ8 included) and FT.INFO reports true cross-segment doc counts, and the KV/vector command hot paths honor the no-alloc and parking_lot lock-discipline rules.
rationale: new-major (split 3/3) — the v3 "secondary-engine correctness & parity" theme's latent-correctness + rule-compliance slice from the 2026-06-16 deep review. Bundles the two latent vector-correctness traps (none block the current call paths, hence they survived to review) with the small hot-path-alloc / lock-discipline list. FTS (v3-1) and Graph (v3-2) are sibling slices.
stage: production · status: planned · created: 2026-06-16

> SDD living doc for this milestone. Keep it THIN: breadth, shared decisions, and
> exit criteria only — per-task detail lives in each `.add/tasks/<slug>/TASK.md`,
> written just-in-time. Update this doc whenever a task reveals a milestone gap.

## Scope
In:
- SQ8 segment `code_len = bytes_per_code - 4` is wrong for SQ8's 8-byte trailer — corrected decode
  across the segment lifecycle (search / merge / persistence). `src/vector/segment/compaction.rs:554`
  + `immutable.rs` / `mutable.rs`. (Latent P0; same family as the v0.3.0-deferred code_len note.)
- FT.INFO `num_docs` sums mutable + immutable segments, not just the mutable one.
  `src/command/vector_search/ft_info.rs:42`.
- FT.SEARCH avoids the ~3.2 MB `key_hash_to_key` clone per query (borrow / `Arc` the map).
- KV `INCR`/`DECR` write the integer via `itoa` to a buffer — no per-op `String` alloc on the hot
  path. `src/command/string/string_write.rs:312,317`.
- The command-dispatch path uses `parking_lot::RwLock`, not `std::sync::RwLock`, for the ACL table.
  `src/shard/event_loop.rs:65`, `src/command/connection.rs:374,460`.

Out:
- Re-quantization / new vector codecs — only the existing SQ8 decode length is in scope, not new
  formats.
- Vector-search QPS/recall competitiveness vs RediSearch (bench §10.5) — a larger HNSW/quant effort,
  deferred.
- Broader allocation/lock audit beyond these named sites.

## Shared decisions & glossary deltas   (living — every task must honor these)
- On-disk segment compatibility: the `code_len` fix MUST either decode existing SQ8 segments
  correctly or bump the segment format version with a documented migration — never silently mis-read
  persisted data (CLAUDE.md persistence/reload care; cf. the CWD-reload trap).
- No new hot-path allocations in command / protocol / event_loop / io (CLAUDE.md Allocations):
  `itoa` / `SmallVec` / borrow only.
- `parking_lot` locks only; never hold a lock across `.await` (CLAUDE.md Lock Handling).
- TDD red/green per fix; no new `unsafe` without approved SAFETY.

## Shared / risky contracts (freeze these first)
- SQ8 on-disk `code_len` / segment-trailer layout — the byte format read at decode. Wrong or
  un-versioned here corrupts persisted indexes. Freeze the decode contract (and migration stance)
  before touching the read path.                              -> owning task `vector-sq8-code-len`

## Tasks (breadth-first decomposition; detail lives in each TASK.md)
- [ ] vector-sq8-code-len           depends-on: none   — fix `code_len` for SQ8's 8-byte trailer;
      correct decode across search / merge / persistence (latent P0).
- [ ] vector-ftinfo-num-docs        depends-on: none   — FT.INFO `num_docs` sums all segments
      (mutable + immutable), not just mutable.
- [ ] vector-search-keyhash-noclone depends-on: none   — drop the ~3.2 MB `key_hash_to_key` clone per
      FT.SEARCH (borrow / `Arc`).
- [ ] kv-incr-itoa                  depends-on: none   — `INCR`/`DECR` via `itoa`-to-buffer; no
      `String` alloc on the hot path.
- [ ] kv-dispatch-lock-discipline   depends-on: none   — ACL / dispatch `std::sync::RwLock` ->
      `parking_lot::RwLock`.

## Exit criteria (observable; map each to the task that delivers it)
- [ ] An SQ8 immutable segment decodes vectors at the correct length — recall parity with the
      pre-compaction mutable segment (within quant tolerance) across search / merge / reload; test.  (← vector-sq8-code-len)
- [ ] FT.INFO `num_docs` == total docs across mutable + immutable after a compaction; test.          (← vector-ftinfo-num-docs)
- [ ] FT.SEARCH performs no per-query 3 MB `key_hash` clone — allocation probe / throughput test
      shows the clone gone.                                                                          (← vector-search-keyhash-noclone)
- [ ] `INCR`/`DECR` allocate no `String` on the hot path — `itoa` path asserted (test / no-alloc check). (← kv-incr-itoa)
- [ ] No `std::sync::RwLock` remains on the command-dispatch path — ACL table is `parking_lot`;
      audit/grep + test.                                                                             (← kv-dispatch-lock-discipline)
