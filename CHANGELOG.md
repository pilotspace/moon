# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed — `--disk-offload` without a durability backstop broke the default LRU cache recipe

- **GCP benchmark finding (2026-07-10)**: with `--disk-offload enable` (the
  default) but `--appendonly no` and no `--save`, the durable-spill eviction
  path added to fix the crash window above (`evict_batch_durable_no_aof`)
  never runs, because it needs a `ShardManifest` that is only threaded
  through the tick-driven memory-pressure cascade
  (`shard::persistence_tick::handle_memory_pressure`), itself gated on
  `persistence_dir`, which `main.rs` only constructs when
  `appendonly == "yes" || save.is_some()`. The inline write-path eviction
  gate has no manifest access, so durable spill is impossible from that call
  site — but the pre-fix `manifest is None` branch OOM'd **unconditionally**
  regardless of `--maxmemory-policy`, silently breaking the documented
  "Pure cache (no durability)" recipe
  (`--appendonly no --maxmemory <bytes> --maxmemory-policy allkeys-lru`): a
  classic LRU cache rejected writes at the cap instead of evicting.
- **Behavior fix** (`src/storage/eviction.rs`,
  `try_evict_if_needed_async_spill_with_total_budget`'s `manifest is None`
  branch): made the fail-close policy-aware, matching Redis semantics. When
  `total_memory <= budget` nothing happens; when `policy == NoEviction` it
  still OOMs (correct — noeviction always rejects); otherwise (any evicting
  policy — `allkeys-*`/`volatile-*`) it now reclaims the budget by
  **dropping** victims via `evict_one_with_spill(.., spill: None)` (the same
  plain-drop helper the disk-offload-disabled write path already uses — no
  new victim-selection logic), looping until the budget is satisfied or no
  eligible victim remains (then OOM). No durable spill is attempted here —
  that still only happens via the tick's `evict_batch_durable_no_aof` when a
  manifest is reachable. Three new tests in `src/storage/eviction.rs`:
  `async_spill_no_aof_backstop_no_manifest_allkeys_lru_evicts_pure_cache`
  (the regression test — fails with OOM on pre-fix code, passes after),
  `async_spill_no_aof_backstop_no_manifest_noeviction_still_rejects`
  (unchanged noeviction OOM behavior), and
  `async_spill_no_aof_backstop_no_manifest_volatile_lru_scoped_to_ttl_keys`
  (`volatile-*` only evicts keys with a TTL, never a persistent key). The
  `--appendonly yes` async-spill path and the manifest-reachable durable
  batch-spill path are untouched.
- **Startup warning**: `ServerConfig::disk_offload_spill_inert` (new
  predicate, `src/config.rs`) plus
  `ServerConfig::warn_disk_offload_without_durability` (mirrors
  `warn_deprecated_cold_tier_flags`) emit a single `tracing::warn!` at
  startup when `--disk-offload enable` + no durability backstop is
  detected, called from `main.rs` right after the existing cold-tier-flags
  warning: disk-offload's cold-spill tiering is still inert in this
  combination (evicting policies now drop instead of tiering; only
  `noeviction` still OOMs). Documented in `docs/guides/tuning.md`'s "Tiered
  memory offload" section.

### Fixed — test flakiness: oom_bypass_closure readiness on loaded CI runners

- `tests/oom_bypass_closure.rs`: the readiness path used a fixed 30s connect
  deadline with no CI allowance and no dead-child detection —
  `test_case_e_cross_db_copy_oom` lost that race on a loaded macOS CI runner
  (2026-07-10). Same remedy as the sigterm/bgsave harnesses (#264):
  `wait_ready` now fails fast via `try_wait()` when the server process has
  exited (surfacing `moon.stderr.log`/`moon.stdout.log` tails instead of
  burning the deadline), retries in bounded 1s connect windows, and extends
  the deadline 30s → 120s under `CI`.

### Fixed — CI: fts_query_parse missing from fuzz matrix

- `.github/workflows/fuzz.yml`: the `fts_query_parse` fuzz target was
  registered in `fuzz/Cargo.toml` but absent from both the PR and nightly
  CI matrices, so it never ran. Added to both. Also corrected CLAUDE.md's
  stale fuzz-target count (7 → 12) and target list.
### Fixed — RDB stream consumer-group allocation-DoS gap (`src/persistence/rdb.rs`)

- **Four more untrusted-length counts in the native RDB `TYPE_STREAM` decoder
  were unvalidated**, the same vulnerability class fixed for
  `snapshot.rs`/`redis_rdb.rs` above: `read_entry` and its zero-copy twin
  `read_entry_zero_copy` (`src/persistence/rdb.rs`) read a stream's
  `group_count`, `pel_count`, `consumer_count`, and `pending_count` straight
  off the wire without bounding them against the bytes actually remaining in
  the cursor, unlike every sibling count in the same functions
  (`entry_count`, `field_count`, hash/list/set/zset counts), which already
  went through `rdb::validate_count`. None of the four drives an eager
  `with_capacity`/zero-fill today (they grow a `BTreeMap`/`HashMap`
  incrementally), so this wasn't a single-allocation DoS, but it left a
  structural inconsistency that a future refactor could easily turn into
  one and skipped the fail-fast rejection every other count gets. Fixed by
  adding `validate_count` calls with the true structural minimum bytes per
  item (28 for a group: 4-byte empty name + 16-byte last-delivered-id +
  4-byte pel_count + 4-byte consumer_count; 36 for a PEL entry: 16-byte
  StreamId + 4-byte empty consumer name + 16-byte delivery time/count;
  16 for a consumer: 4-byte empty name + 8-byte seen_time + 4-byte
  pending_count; 16 for a pending id: StreamId) — chosen so no legitimate
  file is ever rejected. Red/green TDD: 4 crafted-blob tests (one per
  count) confirmed failing before the fix and passing after, plus a control
  test asserting a fully-populated 1-group/1-pel/1-consumer/1-pending
  stream is still accepted by both `read_entry` and `read_entry_zero_copy`.
### Fixed — KV disk-offload: make eviction spill durable before dropping the hot value

- **Crash window** (`src/storage/eviction.rs`): under memory pressure with
  `--disk-offload enable`, the async-spill eviction path (`evict_one_async_spill`,
  driven from the shard event loop's memory-pressure tick) queued the
  `SpillRequest` to the background `SpillThread` and immediately dropped the
  hot value from RAM — before the pwrite, fsync, or manifest commit had
  happened. Under `--appendonly yes` this is safe (a crash in that window is
  covered by AOF replay of the original write), but under `--appendonly no`
  there is no AOF to fall back on: the value existed nowhere (not in RAM, not
  yet on disk, no WAL/AOF record) for the entire `SpillThread` batching
  window (up to 256 entries or its 100ms tick, longer under backpressure). A
  kill -9 in that window was unrecoverable, silent data loss.
- **Fix**: `try_evict_if_needed_async_spill_with_total_budget` now branches on
  `--appendonly`. With an AOF backstop the fast fire-and-forget path
  (`evict_one_async_spill`, queue-then-drop over the `SpillThread` channel) is
  unchanged (documented, intentional trade-off). Without one, it drives a new
  `evict_batch_durable_no_aof`: collects up to 256 victims (mirroring
  `SpillThread`'s own `FLUSH_ENTRY_CAP`) *without* removing them from RAM,
  writes the whole batch as one durable, synchronous call to
  `spill_thread::flush_buffer` (pwrite + fsync + manifest commit — the same
  inline/oversized page routing the background thread itself uses, now made
  `pub(crate)` for reuse), and only *then* removes from RAM the keys whose
  file both pwrite-succeeded and manifest-committed, immediately populating
  `ColdIndex` so each stays read-through-able (previously only a later
  `SpillCompletion` — which never arrives on this path — did that). Batching
  is required, not optional: an earlier draft did one fsync per evicted key
  and stalled the single-threaded shard event loop under sustained pressure.
  `try_evict_if_needed_async_spill_with_total_budget` gained an
  `Option<&mut ShardManifest>` parameter to carry the manifest down; the
  memory-pressure cascade (`handle_memory_pressure` in
  `src/shard/persistence_tick.rs`) is the one call site that has a manifest
  and passes it through. The four other callers (inline per-connection
  write-path gate, sharded/monoio handlers, `spsc_handler`, the Lua scripting
  bridge) have no manifest reachable; under `--appendonly no` they now bail
  (retain the hot value, surface OOM) instead of risking the same crash
  window — the next 100ms memory-pressure tick reclaims the memory via the
  durable path instead. **Trade-off**: those inline call sites no longer
  evict opportunistically under `--appendonly no` (that unconditional
  fast-path eviction, regardless of durability, was the bug being fixed), so
  a write burst that crosses `maxmemory` now converges to budget only as fast
  as the 100ms tick reclaims it, evicting the minimum needed each pass rather
  than eagerly on every write — verified live (per-shard `estimated_memory()`
  accounting, not RSS) to converge correctly and stay converged.
- **Second bug, same file** (`evict_one_with_spill`, the legacy fully-sync
  spill path used when no `SpillThread` exists): on a spill I/O error it
  logged a warning and *still* evicted the key from RAM — unconditional data
  loss on every spill failure, independent of any crash. Now returns without
  evicting on failure, matching the async path's fail-closed contract.
- **Regression coverage**: `sync_spill_failure_retains_hot_value_no_silent_drop`,
  `async_spill_no_aof_backstop_durably_spills_before_drop` (locks the durable
  pwrite+fsync+manifest-commit-before-drop ordering and immediate
  `ColdIndex` availability), `async_spill_no_aof_backstop_no_manifest_retains_hot_value`,
  `async_spill_no_aof_backstop_spill_failure_retains_hot_value` — plus a
  `db.remove()` (DEL) check after the new durable-spill path to guard against
  reintroducing the DEL/cold-tombstone resurrection class (#257): an earlier
  draft of this fix inserted into `ColdIndex` *before* `db.remove()`, and
  `Database::remove`'s own cold-tier cleanup silently wiped the insert back
  out — caught by this test suite, not by inspection.

### Fixed — test flakiness: sigterm-shutdown readiness + BGSAVE file polling

- `tests/sigterm_shutdown.rs`: `wait_for_ready` now distinguishes a crashed
  child (fails fast via `try_wait()` instead of burning the full deadline)
  from a genuinely slow-to-start one, and every readiness panic surfaces the
  captured `moon.stdout.log` / `moon.stderr.log` so a real failure stays
  diagnosable. The readiness deadline also doubles under `CI` (60s → 120s) —
  the observed flake was macOS CI runner startup latency under load, not a
  hang in the SIGTERM path under test.
- `tests/integration.rs`: `test_bgsave_creates_rdb_file` and its two siblings
  that shut down and restart a server right after `bgsave_with_retry`
  (`test_rdb_restore_on_startup`, `test_aof_priority_over_rdb`) asserted
  `dump.rdb` existence off a fixed 200ms guess-sleep — BGSAVE queues the
  write on a `spawn_blocking` thread and replies before it lands on disk.
  Added a `wait_for_file` bounded-poll helper (50ms interval, 10s deadline)
  and used it at all three call sites in place of the point-in-time
  assumption. The MOON-magic-bytes assertion is unchanged.
### Fixed — CI: nightly fuzz shard (`graph_props_record`) + stale `deny.toml` comment

- **Missing fuzz harness**: `fuzz/Cargo.toml` and `.github/workflows/fuzz.yml`
  both declared a `graph_props_record` fuzz target, but
  `fuzz/fuzz_targets/graph_props_record.rs` didn't exist — that matrix shard
  failed every nightly run (audit finding, `docs/PRODUCTION-CONTRACT.md`
  FUZZ-01). The target function is real: `src/graph/csr/props.rs`'s module
  doc even names it (`fuzz target: graph_props_record`) — the v5 node/edge
  property-record codec (`decode_node_props`, `decode_node_prop`,
  `decode_node_embedding`, `node_record_len`, `decode_edge_weight`,
  `decode_edge_props`, `decode_edge_prop`, `edge_record_len`) decodes bytes
  read straight off a `.csr` segment file and is documented to return
  `None`/empty on malformed input rather than panic. Added the missing
  harness so the shard actually fuzzes it, following the exact pattern of
  the sibling targets (`wal_v3_record.rs`, `gossip_deser.rs`).
- **Stale comment**: `deny.toml`'s header claimed a `ci.yml` "safety-audit"
  job runs `cargo audit`/`cargo deny`; no such job exists. The actual "Lint"
  job runs `scripts/audit-unsafe.sh` + `scripts/audit-unwrap.sh` (SAFETY-
  comment coverage / unwrap ratchet) — a different, unrelated check.
  Corrected the comment to describe reality and left a `TODO(ci):` marking
  the gap (wiring `cargo deny check` into CI is a separate decision, out of
  scope here).
### Changed — Tiering: drop unused meta/undo placeholder files

- `transition_to_warm` (HOT->WARM segment seal) no longer writes `meta.mpf`
  (VecMeta) or `undo.mpf` (VecUndo) — both were always-empty placeholders with
  zero readers anywhere in the codebase (the WARM load path
  `WarmSearchSegment::from_files` only ever opens `codes.mpf`/`graph.mpf`/
  `mvcc.mpf`/optional `vectors.mpf` by name). Removes 2 extra
  `File::create` + fsync round-trips per WARM segment seal. `write_meta_mpf`
  and `write_undo_mpf` (`src/vector/persistence/warm_segment.rs`) are removed
  as they had no other callers.
- `src/vector/persistence/segment_io.rs`: removed the dead
  `sq_vectors.bin`/`f32_vectors.bin` comment-only branch (never emitted since
  SQ8/f32 storage was dropped from `ImmutableSegment` in favor of TQ-ADC) and
  corrected the module doc header, which still listed both files as if
  written.
- Back-compat: old WARM segments written by a prior build that still contain
  `meta.mpf`/`undo.mpf` continue to load — the loader opens files by name and
  never scans the segment directory, so the extra files are silently ignored
  (verified by a new regression test with stray legacy files on disk).
### Fixed — RDB DoS hardening

- **Two untrusted-input allocation-DoS vectors in RDB/snapshot loading are
  now bounds-checked.** `shard_snapshot_load`'s `SEGMENT_BLOCK_MARKER`
  branch (`src/persistence/snapshot.rs`) read an untrusted `entry_count:
  u32` and passed it straight to `Vec::with_capacity` with no gate; the
  Redis-RDB reader (`src/persistence/redis_rdb.rs`) did the same for
  `read_redis_string`'s length-prefixed byte buffer and for the
  list/set/hash/zset collection decoders, where `read_length`'s 8-byte
  (`0x81`) form can return up to `u64::MAX`. Both are on the server-startup
  / replica-full-sync path, so a single crafted or corrupt file could drive
  a multi-gigabyte allocation before a byte of the claimed data was read —
  aborting the process (release builds use `panic = "abort"`) or OOM-killing
  it. Fixed by promoting `rdb::validate_count` to `pub(crate)` and calling it
  before the snapshot segment's `Vec::with_capacity`, and by adding a
  matching `check_alloc_bound` helper in `redis_rdb.rs` that bounds every
  untrusted length/count against the bytes actually remaining in the input
  before allocating. No wire-format or valid-file behavior changes — a
  legitimate length/count always fits within the remaining input.
### Removed — Experimental DiskANN COLD vector tier

- **Deleted the experimental "COLD-ann" vector tier** (`src/storage/tiered/cold_tier.rs`,
  the whole `src/vector/diskann/` module — Vamana graph, product quantizer,
  co-located page format, io_uring beam search). It was gated off by default
  (`MOON_VEC_COLD_TIER=1`), incomplete by its own docs (no cold-segment
  deletion, ADC-only recall, no restart-time PQ-codebook reload), and had no
  restart recovery story. The M3-exit review decided delete-over-finish. The
  default COLD valve remains `SegmentList.unloaded` (`UnloadedSegment`,
  exact, near-zero RAM, reload-on-touch) plus WARM byte-budget LRU eviction
  (`--vec-warm-mmap-budget`) — neither is affected by this removal.
- Removed the dead call chain: `VectorIndex::try_cold_transitions[_all]`,
  `VectorStore::register_cold_segments`, the COLD 60s event-loop timer arm
  (tokio + monoio), `cold_tier_experimental_enabled()`, cold-segment manifest
  discovery in `src/persistence/recovery.rs` (`RecoveryResult::cold_segments`
  / `cold_segments_loaded`), and the `snap.cold` fan-out in `FT.SEARCH` /
  `FT.INFO` / `SegmentHolder::resident_bytes`/`total_vectors`.
- `--segment-cold-after`, `--segment-cold-min-qps`, `--vec-diskann-beam-width`,
  `--vec-diskann-cache-levels` are kept as **parseable, inert no-ops** (rather
  than a hard CLI break) so existing `moon.conf` files/launch scripts do not
  fail to start; a startup `tracing::warn!` fires once if any is set away
  from its historical default (`ServerConfig::warn_deprecated_cold_tier_flags`,
  called from `main.rs`).
- `StorageTier::Cold`/`Archive` (the shared on-disk tier-byte enum in
  `src/persistence/manifest.rs`, format §4.3) are left in place — they are
  part of the stable `FileEntry.tier` wire format and shared with the
  unrelated KV disk-offload subsystem's `ColdIndex`/`sweep_orphans` (which
  this change does not touch).

### Fixed — Vector: WARM segments now survive a restart, and stop leaking superseded segment directories

- **Restart-recovery dead wiring fixed.** `VectorStore::register_warm_segments`
  had zero non-test callers: `Shard::restore_from_persistence`'s v3 recovery
  pass discovered WARM segments from the manifest (`RecoveryResult.warm_segments`)
  into a throwaway `Shard`-owned `vector_store` that `event_loop.rs` discards
  wholesale in favor of the live `ShardSlice.vector_store` — so the discovery
  result was thrown away with it, and WARM's RSS win evaporated on every
  restart. Fixed by staging the discovered segments on a new
  `Shard::recovered_warm_segments` field and draining them into the live
  store via `register_warm_segments` right after B3 recovery
  (`RecoveryState::finish`) completes in `event_loop.rs`.
- **Superseded segment-directory leak fixed.** `VectorIndex::try_warm_transitions_idle`
  never told Stack B's durability manifest (`vector/persistence/manifest.rs`)
  that a segment had left the immutable tier, so the old
  `idx-<hex>/segment-<old_id>/` directory it left behind was never garbage
  collected — permanent disk growth on every HOT→WARM/COLD transition, and
  (independently) the reason a restart used to silently reload the segment
  as a fully-materialized HOT copy of stale data instead of respecting the
  WARM tier it had just been moved to. Fixed by calling the existing
  `persist_hook_after_install` durability hook after every transition; its
  manifest diff (`run_snapshot_job`) now GCs the superseded directory the
  same way a normal compact/merge already does.
- `VectorIndex::persist_hook_after_install` changed from `&mut self` to
  `&self` so `try_warm_transitions_idle` (which only holds `&self`) can call
  it; `next_snapshot_seq` changed from `u64` to `AtomicU64` to keep
  `VectorIndex`/`VectorStore` `Sync` (`fetch_add`, `Relaxed` — every actual
  writer is still the single owning shard thread, so this only needs to be a
  valid value, not a cross-thread synchronization point).
- COLD (`SegmentList.unloaded`, the reload-on-touch stub tier) is
  unaffected in the case that already worked (WARM→COLD idle transitions —
  those segments were never in Stack B's `segment_ids` to begin with) and
  loses only an accidental, undocumented side effect in the other case
  (HOT→COLD-direct transitions used to reload as stale HOT on restart due to
  the same leak this fixes) — it still has no dedicated restart-recovery
  path of its own; a restart now correctly falls through to the existing
  dedup-rescan/AOF-replay self-heal instead (same fallback as any index with
  no durable manifest state), never silent data loss.
- New TDD regression test (`vector::persistence::recover_v2::tests::
  warm_transition_leak_fix_and_restart_recovery`) proves, on real
  force-compacted on-disk state: (a) the superseded segment directory is
  GC'd within a bounded wait, (b) a simulated restart does NOT reload it as
  a phantom HOT segment, (c) `register_warm_segments` reattaches it as WARM,
  and (d) post-restart search returns the identical top-1 `global_id` as the
  pre-transition baseline — no recall regression. Verified red (fails with
  the leak-fix call removed) before green.

### Fixed — Vector: hand-review found `register_warm_segments` could permanently duplicate documents or attach a segment to the wrong index

- **CRITICAL — crash-before-GC restart could leave the same vectors live
  twice, forever.** `try_warm_transitions_idle` commits Stack A's shard
  manifest durably (synchronous, fsync+commit) *before*
  `persist_hook_after_install` merely schedules the async Stack B snapshot
  job. A `kill -9` landing in that window left the old segment still
  tracked by Stack B (reloaded as an ordinary HOT/immutable segment by
  `recover_v2`) *and* the warm copy discovered by Stack A (reattached WARM
  by the restart-recovery wiring above) — the same key_hashes live in two
  segments. `search_mvcc`'s merge (`src/vector/segment/holder.rs`,
  `all.sort_unstable(); all.truncate(k);`) has no key_hash dedup, so this
  never self-healed: duplicate keys in `FT.SEARCH` results, inflated
  `num_docs`, and the next snapshot re-adopted the reloaded HOT copy into
  `segment_ids` forever. Fixed: `register_warm_segments` now checks a warm
  segment's own key_hash set (`warm_search::peek_key_hashes`, a cheap
  `mvcc.mpf`-only read — no codes/graph mmap, no `CollectionMetadata`
  dependency) against its owning index's *current in-memory*
  `key_hash_to_key`. If those keys are already covered by a live HOT
  segment, Stack B wins: the warm copy is left unattached and its on-disk
  directory is deleted (`std::fs::remove_dir_all`) instead of being
  registered, so Stack B stays the single source of truth.
- **HIGH — a warm segment could attach to the wrong index.** The
  restart-recovery wiring above made a previously-dead code path live:
  `register_warm_segments` attached each segment to the *first* index for
  which `WarmSearchSegment::from_files` happened to succeed —
  `from_files` accepts any caller-supplied `CollectionMetadata` and never
  validates it against the on-disk codes/graph, so two indexes of the same
  dimension/quantization made the outcome a coin flip (wrong collection,
  wrong search results, no error). Fixed: ownership is now decided from
  key/keymap evidence, never from `from_files` success. Each candidate
  index's *persisted* Stack-B keymap (read straight off disk via a new
  `read_manifest_and_keymap_consistent` helper — bounded retry with
  backoff against the benign TOCTOU where a concurrent `run_snapshot_job`
  advances the epoch and GCs the old keymap file between the manifest read
  and the keymap read) is checked for key_hash overlap with the segment;
  the index with the most matches wins. No match, or a tie between two
  indexes, leaves the segment unregistered (files intact, logged via
  `tracing::warn!`) rather than guessing.
- Two new TDD regression tests in `vector::persistence::recover_v2::tests`:
  `warm_reattach_dedups_against_crash_before_gc_race` stages the exact
  crash-before-GC state (rolls the on-disk manifest/segment/keymap back to
  pre-transition *after* waiting for the real background GC to land, so the
  rollback is deterministically the last write) and asserts zero duplicate
  `global_id`s in search results, `warm.len() == 0`, and the superseded
  warm directory is deleted; `warm_reattach_picks_correct_index_among_same_dim_indexes`
  builds two same-dimension indexes with disjoint keysets — where the old
  first-match behavior would have funneled both segments into one index
  regardless of `HashMap` iteration order — and proves each segment lands
  on its true owner via an end-to-end recall check (a cross-attached
  segment would still pass a bare `warm.len() == 1` count but return the
  wrong top-1 result). Both verified red (fail at the exact assertion the
  fix satisfies) against the prior "first successful `from_files`"
  implementation before green.

### Fixed — Vector: WARM restart recovery was still defeated on every NORMAL restart (not just a crash) by call-site ordering

- **CRITICAL — round 2 of hand-review found the previous fix's own wiring re-triggered its own duplication check.** The full production B3 sequence is `create_index` -> keyspace dedup rescan (`reconcile_key`, once per matching HASH key) -> `finish` -> `register_warm_segments`. `load_segments_and_keymap` never populates `key_hash_to_key`/`key_hash_to_global_id` for WARM keys (see its `segment_resident` gate — nothing has attached the WARM segment to `idx.segments` yet at that point in `create_index`), so with `register_warm_segments` running LAST, the rescan saw every WARM key as unknown and re-encoded it into the mutable segment (full HNSW/TQ rebuild, not just metadata) on every boot. The duplication check added in the previous fix then saw those just-re-indexed keys as "already covered by a live HOT copy" and retired (permanently deleted) the WARM segment — the exact failure this feature exists to prevent, triggered by the fix's own ordering, on every normal restart. Fixed: `VectorStore::register_warm_segments` now runs immediately after the sidecar `create_index` loop, BEFORE the keyspace rescan (`event_loop.rs`) — ownership decisions only need every sidecar index to already exist, which they do by then.
- **On a clean attach, `register_warm_segments` now populates `key_hash_to_key`/`key_hash_to_global_id`/`key_hash_to_vec_checksum`** for the segment's keys, sourced from the same owner-evidence persisted-keymap entries already read for the ownership decision (no extra disk read). This is what lets the rescan (which runs right after) see the keys as known/checksum-unchanged instead of re-encoding them, and lets `FT.SEARCH` resolve a WARM doc's real key bytes via `key_hash_to_key` (the exact map `resolve_hybrid_doc_key` consults) instead of falling through to a synthetic `vec:<id>`. A segment key_hash absent from the owner's persisted keymap (an earlier async-snapshot job never committed for it — the same class of race as the crash-before-GC finding, at per-key rather than per-segment granularity) is left out of the maps, so the rescan self-heals it into mutable, and is tombstoned in the warm copy for that one key_hash (`WarmSearchSegment::seed_tombstones`) so the stale warm copy and the freshly re-indexed mutable copy never coexist as live duplicates.
- **`RecoveryState`'s deletion-probe baseline moved to a new explicit phase, `snapshot_recovered_baseline`.** Previously `create_index` snapshotted the baseline immediately (before any WARM segment existed in-memory), which — now that `register_warm_segments` populates WARM keys into `key_hash_to_key` — would have permanently excluded every WARM key from `finish()`'s deletion probe: a WARM key whose underlying HASH was deleted while the server was down would never be recognized as "used to exist, now doesn't," and would silently survive as a stale search hit forever. `event_loop.rs` now calls `snapshot_recovered_baseline` once, after BOTH `create_index` (all indexes) AND `register_warm_segments` have settled, and before the keyspace rescan begins — the baseline is then complete for the deletion probe to catch a delete-while-down WARM key too (`VectorIndex::mark_deleted_for_key_in_index` already tombstones across every tier including WARM, so once the probe fires the fix is mechanical).
- New TDD integration-level regression test,
  `vector::persistence::recover_v2::tests::warm_tests::warm_recovery_full_production_sequence_matches_real_boot_order`,
  drives the FULL real sequence in order (`create_index` -> `register_warm_segments`
  -> keyspace rescan, replaying every key except one to simulate a delete-while-down
  -> `finish`) and asserts: (a) the WARM segment attaches and is not retired, (b) every
  still-live WARM key is counted `verified_unchanged` (0 `re_indexed`, nothing lands in
  the mutable segment), (c) search resolves the ORIGINAL key bytes for a WARM doc via
  `key_hash_to_key` (not `vec:<id>`), (d) no duplicate `key_hash` across results, (e) the
  delete-while-down key is tombstoned by the deletion probe and absent from search.
  Verified red by hand (temporarily moving `register_warm_segments` back to run after
  the rescan/`finish`, matching the prior ordering): (a) fails first — the WARM segment
  is retired as a false-positive duplicate (`warm.len() == 0`) instead of attaching,
  which also makes (b)/(c) unreachable. The three existing WARM tests from the previous
  fix (`warm_transition_leak_fix_and_restart_recovery`,
  `warm_reattach_dedups_against_crash_before_gc_race`,
  `warm_reattach_picks_correct_index_among_same_dim_indexes`) and one pre-existing
  deletion-probe test (`recover_finish_tombstones_keys_missing_from_rescan`) were
  updated to call `register_warm_segments`/`snapshot_recovered_baseline` in the new
  production order so they keep testing real behavior instead of a now-stale sequence;
  two of them needed a full keyspace-rescan replay added (previously implicit/absent)
  to avoid the deletion probe wrongly tombstoning their now-baseline-visible WARM keys.
  `crash_recovery_vector_durability.rs`'s six real-server-spawning scenarios (`s1`-`s6`)
  were re-run against a release-fast build as an additional non-WARM regression check
  on the reordered `event_loop.rs` sequence — all pass unchanged.
- `vector::persistence::recover_v2`'s test module, having grown past the 1500-line file
  size guideline with these additions, was split: `recover_v2_tests.rs` keeps the
  non-WARM B3 recovery tests + shared helpers, and a new nested
  `recover_v2_warm_tests.rs` (`#[path]`-included as `mod warm_tests` from within
  `recover_v2_tests.rs`) holds all WARM-specific tests.

### Docs — Roadmap: native `moon://` / `moons://` connection URI scheme

- Define Moon's native connection URI scheme in `docs/roadmap/ROADMAP.md` as a
  superset of Redis's `redis://` / `rediss://` (`moons://` = TLS 1.3 variant,
  parity with `rediss://`). Adds the full cross-cutting spec (§8.5), the v0.6.1
  spec-doc task (H-7), the v0.7.0 implementation workstream (R6), and a Protocol
  node in the features mindmap. `redis://` / `rediss://` remain fully supported;
  the native scheme adds a `?workspace=<tenant>` selector and a fail-fast,
  no-opportunistic-downgrade TLS contract.
### Docs — `moon://` / `moons://` connection URI scheme spec (H-7, PR #TBD)

- New `docs/protocol/moon-uri.md`: the authoritative, doc-only spec for Moon's native
  `moon://` / `moons://` connection URI scheme — ABNF grammar, a field-by-field
  `redis(s)://` parity table, a query-parameter reference (parity + Moon-native
  `?workspace=`), the design-for-failure section (no opportunistic downgrade, no
  auto-upgrade, fail-fast diagnostic text, unknown-scheme parse errors, bounded
  connect), server-participation semantics (`--announce-url`, `INFO replication`,
  cluster redirects, `REPLICAOF`), worked examples, and the conformance checklist the
  v0.7.0 Workstream R6 implementation + `tests/uri_scheme.rs` must satisfy. No Rust
  code changes — implementation is tracked separately as v0.7.0 R6.
- Verified every cited flag/module against the current tree (`--tls-port` et al. in
  `src/config.rs`, TLS 1.3/rustls/aws-lc-rs + SIGHUP hot-reload in `src/tls.rs`, `WS
  AUTH` UUID-only argument in `src/command/workspace.rs`) rather than restating the
  roadmap's prose uncritically; flags the `WS AUTH` UUID-vs-name gap for the R6
  implementer as an open decision instead of assuming it away.
- `mkdocs.yml`: adds a "Protocol" nav section for the new page so `mkdocs build
  --strict` doesn't fail on an orphan doc.
### Fixed — Cold-tier proactive TTL reclaim (H-2, R1: tmp/OFFLOAD-COMPRESSION-REVIEW.md)

- **TTL-expired disk-offload cold entries that are never re-read no longer
  leak forever.** The on-read reclaim path (`cold_read.rs`) only reclaimed an
  expired `ColdIndex` entry when a caller issued a `GET` against it; a key
  that expired and was never touched again (the flagship offload use case —
  TTL'd sessions, caches) permanently leaked its index entry (RAM, full key
  bytes) and pinned its backing DataFile's refcount (disk), because nothing
  else in the system ever inspected a cold entry's TTL.
- `ColdLocation` now carries `ttl_ms: Option<u64>`, a cached copy of the
  on-disk `KvEntry::ttl_ms` populated at spill time and re-derived fresh by
  `rebuild_from_manifest` on every restart — so the sweep can judge expiry
  from the in-RAM index alone, without a pread of the cold file. No on-disk
  format changed and `ColdIndex` has no serialized form of its own, so there
  is no index format to version and no old file that could fail to load.
- New `ColdIndex::sweep_expired` runs in the same tick as the existing
  cold-tier orphan sweep (`--cold-orphan-sweep-interval-secs`, default 5
  minutes), bounded to `MAX_EXPIRED_SWEEP_BATCH` (4096) entries per call so
  an expiry storm against a large cold index cannot stall the shard event
  loop; any remainder is picked up by the next tick. Batch-file colocation is
  preserved exactly like the orphan sweep (an expired key never drags down a
  co-located live key's file).
- New `INFO` fields `reclamation_cold_expired_reclaimed_total` /
  `reclamation_cold_expired_bytes_reclaimed_total` (distinct from the
  existing orphan counters, which count any zero-ref file unlink regardless
  of cause).
### Docs — H-4 doc reconciliation (ROADMAP §8.1)

- **`docs/redis-compat.md`**: `FT.AGGREGATE` was listed as "Not implemented"; it is
  actually implemented and dispatched (`src/command/vector_search/ft_aggregate.rs` /
  `src/text/aggregate.rs`, wired through `src/command/mod.rs` and the
  single/sharded/monoio `FT.*` handlers on both read and write paths). Corrected to
  "Implemented", noting the `APPLY`-stage v1 stub, the FILTER-clause no-op, and that
  it has no `phf` `COMMAND_META` entry by design (so `COMMAND INFO`/`COMMAND DOCS`
  return nothing for it even though it works).
- **`docs/comparison-valkey.md`**: "Hash field expiration — `HEXPIRE`/`HTTL` family
  Moon does not implement" was stale — Moon has shipped the full family (`HEXPIRE`,
  `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HEXPIRETIME`,
  `HPEXPIRETIME`, `HPERSIST`, `HGETDEL`, `HGETEX`) with full read+write dispatch and
  WAL replay since before this document's last refresh. Moved from "Valkey wins
  decisively" to "Effective parity" (Moon trails Valkey by 4-10% on these ops per
  `docs/perf/2026-05-27-hash-ttl-3way-bench.md` — a tracked perf gap, not a missing
  feature); also dropped it from the cluster-features risk bullet in §4, since it is
  unrelated to cluster mode.
- **`SECURITY.md`**: supported-versions table said "0.1.x" (stale since well before
  v0.6.0). Updated to the current v0.6.x line, with an explicit pre-1.0 support
  policy (latest-minor-only; no LTS/backport promise until the v1.0.0 GA tag, per
  `docs/roadmap/ROADMAP.md`'s stated 18-month LTS policy).
### Docs — Production contract refresh + CI gate (ROADMAP H-5)

- **`docs/PRODUCTION-CONTRACT.md` refreshed from its stale v0.1.3-era draft to
  v0.6.0 reality** and converted into a checked, evidence-linked ledger (56
  GA-blocking rows across toolchain/CI, correctness hardening, durability,
  replication & HA, cluster mode, security, observability, compatibility, and
  release engineering). Every ✅ row cites a real file/CI job/script that was
  read and confirmed during this pass — no aspirational ticks. 33/56
  GA-blocking rows are shipped today; 22 are honestly unticked (multi-shard
  replication, `WAIT`/keyspace-notifications/`MONITOR`, monoio cluster
  wiring, encryption at rest, and others already tracked in
  `docs/roadmap/ROADMAP.md` §8). The verification pass also surfaced two
  previously undocumented defects, left unticked rather than assumed fixed:
  `deny.toml`'s header comment claims a `ci.yml` "safety-audit" job that does
  not exist (`cargo audit`/`cargo deny` are not CI-blocking), and
  `fuzz/Cargo.toml` + `.github/workflows/fuzz.yml` both reference a 12th fuzz
  target (`graph_props_record`) whose source file is missing from the tree,
  so that nightly fuzz matrix shard fails every run.
- **New `scripts/check-production-contract.sh`** (grep-based, mirrors
  `scripts/audit-unsafe.sh`'s style): reports unticked GA-blocking ledger rows
  on every tag, and hard-fails only on a `v1.0*` tag if any remain unticked.
  Wired into `.github/workflows/release.yml`'s `setup` job alongside the
  existing RELEASES.md release-ledger gate, using the same env-var
  interpolation pattern (`VERSION: ${{ steps.ver.outputs.version }}`, never
  inline `${{ }}` in a `run:` body).

### Changed — Memory: vector tiering accounting spine (M1, tiering-v2 D3/D9)

- **`--maxmemory` now counts vector segment memory.** The background eviction
  check compared only KV bytes against the per-shard budget, so a pure-vector
  workload could drive RSS to OOM while eviction reported "under budget".
  `timers::run_eviction` now gates on the shard AGGREGATE — Σ all dbs' KV +
  the shard's published vector bytes, computed once per 100ms tick — and
  evicts across dbs only until the aggregate is back under budget (adversarial
  review caught the initial per-db formulation, which both under-detected with
  KV spread across dbs and over-evicted sibling dbs). When the un-evictable
  vector term alone exceeds the budget, KV drains then errors OOM
  (shared-budget semantics; per-db quotas remain the tenant-isolation
  mechanism); the pressure cascade (which shrinks vectors via offload) fires
  earlier at `--disk-offload-threshold`, so with disk-offload enabled vectors
  shed first. **Known limitation:** the on-write eviction gate still checks
  KV-only, so under `noeviction` a vector-heavy shard over the vector-aware
  budget does not yet reject client writes — RSS is bounded by the pressure
  cascade and the RSS watchdog instead (write-gate consistency is a tracked
  M1 follow-up).
- **Elastic budget classification is vector-aware.** A vector-heavy/KV-light
  shard was misclassified as an idle donor, lending headroom to siblings while
  its true footprint was over base — and the pressure cascade compared a
  vector-inclusive used-term against a budget inflated by that donation. The
  donor/hot snapshot now sums KV + vector per shard; a vector-heavy shard is
  classified hot and borrows instead of donating.
- **`--vec-warm-mmap-budget` is now an instance-total cap divided across
  shards** (matching `--maxmemory` semantics). Each shard previously applied
  the full value — an N-shard instance silently allowed N× the configured WARM
  memory. **Behavior change:** multi-shard deployments relying on the old
  per-shard meaning should multiply their flag value by the shard count. A
  nonzero total floors at 1 byte/shard ("0" still disables enforcement).
- **IVF and DiskANN-cold segments report real `resident_bytes()`.** Both tiers
  contributed a hardcoded 0 to the roll-up (untracked RAM: IVF centroids +
  posting lists; DiskANN PQ codes + codebook). They now feed the pressure
  trigger, MEMORY DOCTOR, and Prometheus like every other tier.
- **`INFO reclamation_mmap_warm_bytes` reports the live WARM counter.** The
  field read a never-incremented local static (permanent 0) while the real
  `MmapBudget` counter was write-only. It now reads the live counter, and a
  new `reclamation_mmap_budget_evictions_total` field exposes cumulative
  byte-cap evictions.
- **D9 (DiskANN retention) quarantine:** config docs now disambiguate
  COLD-stub (`unloaded`, exact reload-on-touch default valve) vs COLD-ann
  (`cold`, DiskANN serve-from-disk, inert behind `MOON_VEC_COLD_TIER`); the
  dead knobs are marked `[reserved: M3/M5]`. Keep-vs-delete is decided at the
  M3 exit-review on real per-index query-frequency telemetry.
### Security — ACL now covers the early-intercepted command families (H-3) (PR #258)

- **CDC.READ bypassed ACL entirely on the monoio runtime (the default build).**
  It was dispatched ~117 lines before the ACL gate, so any authenticated user
  — even a `+get`-only one — could run `CDC.READ <any-wal-dir> <lsn>` and read
  arbitrary WAL directories off the server disk. Moved the CDC.READ intercept
  to after the ACL gate (matching the tokio/sharded handler, which already
  ordered it correctly).
- **Category carve-outs silently didn't cover TXN/WS/MQ/TEMPORAL/CDC.READ (all
  runtimes).** These connection-handler intercepts appeared in no
  `get_category_commands()` arm, so the common deny-list idiom
  (`+@all -@dangerous`, `+@all -@transaction`, `+@all -@write`) left them
  allowed. Added them to the relevant categories: `ws`/`cdc.read` →
  `@admin` + `@dangerous`; `txn`/`temporal` → `@transaction`; `mq`/`txn` →
  `@write`; all five → `@all`.
- **`-@pubsub` didn't block PUBLISH/SUBSCRIBE at the command level.** The
  pub/sub intercepts consulted only the `&pattern` channel rule, so a
  `-@pubsub` carve-out was ineffective for a user with `&*`. Added a
  command-level ACL check to the PUBLISH and SUBSCRIBE/PSUBSCRIBE paths in the
  monoio and tokio-single handlers (the sharded handler already gated them
  post-ACL-gate).
- **`CLIENT TRACKING` ran before the ACL gate on monoio.** Moved it to a
  post-ACL `try_handle_client_tracking` (it registers server-side invalidation
  state); `CLIENT ID`/`SETNAME`/`GETNAME` stay pre-ACL as connection-local
  metadata.
- Tests: new unit test pins the category expansion for all five families;
  new integration tests prove end-to-end NOPERM for `CDC.READ` under
  `-@dangerous` and `PUBLISH` under `-@pubsub`.

### Fixed — Vector: memory-aware WARM offload with a real, reloadable ceiling (PR #252)

- **Reloadable byte-cap eviction (A).** `MmapBudget::enforce_budget` (the
  per-shard `--vec-warm-mmap-budget` cap, default 2gb) evicted a WARM segment by
  dropping its `Arc` outright with no COLD stub — so a byte-cap eviction silently
  removed the segment from search until process restart (recall loss), despite
  the doc claiming reload-on-touch. It now demotes each evicted segment to a
  reloadable `UnloadedSegment` stub in `unloaded` (every WARM segment is durably
  disk-backed via `transition_to_warm`, so the stub reloads byte-identically).
  This makes `--vec-warm-mmap-budget` a real, reloadable memory ceiling. The
  eviction tick now also takes the holder `reload_lock` so it serializes with
  the reload/install path instead of relying solely on shard-thread affinity.
- **WARM memory accounting fix.** `SegmentHolder::resident_bytes()` hardcoded the
  WARM/COLD tiers to 0, so a shard whose HOT segments had aged into WARM reported
  ~0 vector memory — blinding both INFO/Prometheus and the memory-pressure
  trigger below. It now sums the WARM tier (the dominant term for a long-lived
  shard) plus COLD stubs. (IVF/DiskANN-cold still lack a resident accessor.)
- **Memory-triggered early offload (C).** Vector segment memory was invisible to
  every pressure mechanism, so a vector-heavy shard (the primary disk-offload
  case) only ever offloaded on the wall-clock idle timer
  (`--engine-offload-idle-secs`, default 3600s), never on RAM pressure; and the
  pressure cascade's step 2 demoted HOT→WARM (no RSS win). Now the shard's vector
  resident bytes (HOT immutable + WARM) count toward the pressure trigger, and
  under pressure step 2 offloads idle vector segments straight to COLD (real RAM
  reclaim, reloadable) at an aggressive 60s idle floor. Known follow-up:
  HOT-immutable memory still has no standalone byte budget (only idle/pressure
  demotion bounds it).

### Performance — Vector: COLD-segment reload moved off the shard event loop (PR #251)

- Under `--disk-offload`, an idle vector segment is demoted to the COLD tier
  (`UnloadedSegment` stub, everything in-memory dropped). The first FT.SEARCH to
  touch it must reload it — `WarmSearchSegment::from_files`: mmap + page-in (+
  optional mlock) of a whole segment. That reload ran **inline on the
  single-threaded shard event loop** (`promote_unloaded` at the top of the
  yielding search capture), so it stalled EVERY other connection on the shard for
  the full reload (hundreds of ms–seconds on real NVMe/EBS with a cold page
  cache) — a multi-tenancy violation (audit finding 18).
- A new process-global `SegmentReloadPool` (`src/vector/reload_pool.rs`, auto-on;
  `MOON_VEC_RELOAD_WORKERS=0` disables) now performs the reload I/O on dedicated
  worker threads. The yielding capture path calls `submit_unloaded_reloads`
  (non-blocking): it submits each COLD stub off-loop and stashes the `flume`
  receivers in the `SearchSnapshot`; the handler `await`s them
  (`await_pending_reloads`) before scanning, which parks only the triggering
  query's **task** (not the OS thread) and then splices the reloaded segments
  into that query's own snapshot. Result: the dense-KNN query that triggered the
  reload still sees **full recall**, while sibling connections on the shard keep
  running. Reloads are **single-flight** (concurrent first-touches of one segment
  share a job) and cached until the next capture installs them into WARM
  (reclaiming the stub's memory).
- Design-for-failure: a reload that errors or whose worker panics/dies replies
  `Err`/disconnect to every waiter — the query answers with degraded recall
  (segment stays COLD, retried next touch) rather than hanging or crashing. When
  the pool is disabled, `submit_unloaded_reloads` falls back to the blocking
  `promote_unloaded`, preserving exact pre-#18 behavior. Non-yielding sync search
  paths are unchanged (still block); HYBRID/SPARSE/RANGE/non-default-field
  queries do not take the off-loop path (accepted follow-up).
### Ops — v0.6.0 release-ledger closure + hygiene (PR #256)

- `RELEASES.md` gains the missing v0.6.0 entry (shipped 2026-07-08, ledger
  lagged); `release.yml` now fails a version-tag push whose entry is absent
  from `RELEASES.md` — the ledger can never lag a tag again.
- `replication.state` (root artifact from replication test runs) gitignored.

### Docs — engineering roadmap suite under `docs/roadmap/` (PR #254)

- New planning docs (excluded from the published docs site via `mkdocs.yml`
  `exclude_docs`): product roadmap v0.7→v1.0 with features mindmap and
  task-level execution plans (`ROADMAP.md`), scale/HA architecture incl.
  multi-shard PSYNC wire-format draft and failover timeline
  (`scale-ha-architecture.md`), and the standalone horizontal-scale deep
  dive (`standalone-horizontal-scale.md`). Commercial/EE planning is
  maintained privately.
- Docs only — no runtime code changes.

### Changed — CI: give the macOS `Test (tokio)` step 30m (was 15m)

- The macOS `Test (tokio)` step budget includes compilation. On a cold or
  rebased cache, ~11m compile + ~8m test suite exceeds 15m and trips a
  wall-clock timeout with zero actual test failures. Bumped to 30m to match
  the Windows Test step's headroom; removes recurring false-negative reds on
  rebased branches.
### Fixed — Disk-offload: crash recovery no longer resurrects deleted cold keys, nor drops the AOF after a spill (PR #TBD)

- **Deleted/flushed cold keys stayed deleted only until the next crash.** A
  DEL/UNLINK/FLUSHALL/FLUSHDB of a spilled key tombstones the in-memory
  ColdIndex, but the manifest entry stays `Active` until the orphan sweep. A
  kill-9 inside that window lost the tombstone, and boot-time replay could not
  re-apply it: the replayed DEL ran against databases whose `cold_index` was
  `None` — a silent cold-plane no-op — so the key resurrected via cold
  read-through (measured 85–97/200 probes under `--appendonly yes` +
  `--disk-offload`). Two detach points fixed: (a) `recover_shard_v3_pitr` now
  attaches the Phase-3-rebuilt ColdIndex to the databases BEFORE Phase 4 WAL
  replay (was: stashed on `RecoveryResult`, merged only after recovery
  returned); (b) the per-shard/multi-part AOF replay now keeps cold wiring
  live during incr replay — main.rs re-attaches it after the pre-replay hot
  wipe, and `replay_per_shard`/`replay_multi_part` bridge it across
  `rdb::load`'s wholesale database swap (which silently dropped it). New
  crash suite `tests/crash_recovery_cold_del_resurrection.rs` (DEL + FLUSHALL
  scenarios, kill-9 inside the pre-sweep window).
- **Any disk-offload spill silently discarded the AOF on the next restart.**
  The "WAL replayed 0 commands → fall back to the AOF" gate counted vector +
  file-lifecycle records: after a spill wrote `FileCreate` records into the
  shard WAL, the gate saw a "non-empty" WAL, skipped `appendonly.aof` (the
  only complete KV history — `--wal-kv-log` is auto-off when the AOF is the
  authority), and every KV write since the last snapshot was lost. The gate
  now keys on KV `Command` records only.

### Fixed — Conn-plane: monoio central listener joins the SO_REUSEPORT group (PR #250)

- Under the monoio runtime, each shard binds `bind:port` via `SO_REUSEPORT`, but
  the central listener bound the same port with a plain
  `monoio::net::TcpListener::bind` — unlike the tokio sibling, which uses
  `create_reuseport_socket`. Linux requires every socket sharing a REUSEPORT port
  to set the option, so depending on startup scheduling the central bind either
  lost the race (EADDRINUSE propagated out of `run_sharded`, silently killing the
  TLS listener + `conn_rx` fallback) or won it (forcing every shard's REUSEPORT
  bind to fail and collapsing all traffic onto the single central accept loop) —
  both silent and nondeterministic run-to-run. The monoio central listener now
  binds via `create_reuseport_socket` + `from_std` like the tokio path (plain-bind
  fallback on non-unix / unparseable addr). (audit finding 16)

### Fixed — Durability: checkpoint Finalize backs off on repeated failure instead of flooding the WAL (PR #250)

- When a checkpoint's Finalize step failed (`wal.wait_durable`, `manifest.commit`,
  `graph_save`, or the control-file write), the state machine stayed `Finalizing`
  and the next 1ms persistence tick re-ran finalize from step 1 — re-appending a
  WAL Checkpoint record every single millisecond with no backoff. Under a
  sustained failure (slow/degraded disk) this floods the WAL, and since
  `last_checkpoint_lsn` never advances, `recycle_segments_before` never fires —
  WAL disk usage grows fastest exactly during the disk-pressure incident it
  should be backing off from. `CheckpointManager` now arms an exponential backoff
  (50ms→5s cap) on each failed finalize; the tick checks `finalize_ready(now)`
  before doing any finalize I/O, so retries are bounded instead of per-tick, and
  a successful `complete()` clears it. (audit finding 13)

### Fixed — Durability: legacy AOF replay stops at mid-stream corruption instead of resyncing (PR #250)

- The default startup path `replay_aof` (single-file `appendonly.aof`, unframed
  RESP with no per-record length/CRC) treated ANY mid-stream parse error by
  discarding one byte, scanning forward to the next `*`, and resuming dispatch —
  but a `*` appears freely inside binary value blobs, so the resync could land
  mid-value and execute misaligned garbage as live SET/DEL/etc. against the
  recovering keyspace (silent data corruption, WARN-only). The per-shard
  `shard_replay` sibling already rejects exactly this. `replay_aof` now STOPS at
  the first genuine mid-stream corruption (clean tail-truncation is still handled
  gracefully), keeping only the valid prefix already applied and never
  dispatching past the corruption point; it logs a loud error with the byte
  offset and points at `redis-check-aof`. Operators who want the old best-effort
  behavior can opt in with `MOON_AOF_BEST_EFFORT_RESYNC=1`. (audit finding 12)

### Fixed — Xshard: bound cross-shard reply wait so a wedged shard can't hang a client (PR #250)

- Every cross-shard leg (MGET/MSET/DEL/UNLINK/EXISTS/BITOP/COPY/MSETNX remote
  legs, MULTI-on-owner, and the flush/scatter-gather loops) pushed a message via
  the backoff-retried `spsc_send` and then did `reply_rx.recv().await` with **no
  timeout**. `spsc_send`'s retry budget only bounds getting the message INTO the
  target ring buffer; if the push succeeds but the target shard then stalls while
  executing (wedged-disk fsync during snapshot/AOF, uninterruptible D-state I/O,
  or a dead shard), the awaiting connection parked forever with no response and
  no way to cancel. Added a `recv_reply_bounded` helper (races the receiver
  against a runtime-agnostic 30s `XSHARD_REPLY_TIMEOUT` via `race2`) and applied
  it to all 11 reply-await sites in the coordinator — a genuinely wedged shard
  now surfaces the existing cross-shard-reply error instead of hanging. (audit finding 11)

### Fixed — Hardening: bound TLS handshake + cluster-bus body reads against slow-loris (PR #250)

- **TLS handshake has no timeout (#17):** after `try_accept_connection` consumed
  a `maxclients` slot, `acceptor.accept(tcp_stream).await` ran with no deadline
  on both the monoio and tokio TLS paths. A peer that completed the TCP handshake
  on the TLS port and then sent no (or a partial) ClientHello pinned the task,
  its fd, and its `maxclients` slot indefinitely at zero CPU cost — a classic
  slow-loris that could exhaust the connection limit. Wrapped both accepts in a
  10s `TLS_HANDSHAKE_TIMEOUT`, releasing the slot via `record_connection_closed()`
  on expiry.
- **Cluster-bus gossip body read has no timeout (#10):** `handle_cluster_peer`
  guarded only the 4-byte length-prefix read with a shutdown-cancel select; once
  a valid length was read, the body `read_exact` had no timeout and no
  shutdown arm. A client could send a valid length header and never send the
  body, blocking the spawned task forever (immune to graceful shutdown, socket
  never reclaimed). Both runtimes now read the body under a
  `GOSSIP_BODY_READ_TIMEOUT` (10s) + shutdown-cancel select.

### Fixed — Txn: MULTI locality analysis honors SORT/GEORADIUS STORE destination (PR #250)

- In a multi-shard deployment, `MULTI; SORT src STORE dst; EXEC` (or
  `GEORADIUS ... STORE/STOREDIST dst`) where `src` and `dst` hash to different
  shards was misclassified as single-shard and routed entirely to `src`'s
  shard — writing `dst` into the WRONG shard's dataset, invisible to a later
  normally-routed `GET dst`. The STORE/STOREDIST destination is positional
  (the arg after the token) and so was not covered by the fixed command-metadata
  key specs `analyze_txn_locality` consulted. It now detects the STORE clause
  and forces a CrossShard classification, which the caller rejects with
  CROSSSLOT instead of silently misrouting. (audit finding 15)

### Fixed — Vector: DEL tombstones secondary fields + FT.INFO num_docs accuracy (PR #250)

- **Multi-vector-field deletion resurrection (#20):** `DEL`/`UNLINK`/`HDEL` on a
  document only tombstoned the *default* vector field. Secondary VECTOR fields
  live in a separate `field_segments` map that `tombstone_key_in_index` never
  iterated, so a subsequent `FT.SEARCH idx '@field2:[...]'` still returned the
  deleted document (under a synthetic `vec:<id>` key, since the shared
  key-hash→key map was cleared). Now every field's segments are tombstoned.
- **FT.INFO `num_docs` over/under-count (#28):** the mutable segment's `len()`
  counts tombstoned entries in place until compaction, inflating `num_docs` by
  up to `compact_threshold` under DEL/HSET churn — switched to a new `live_len()`
  that excludes tombstones. Separately, DiskANN cold-tier segments (experimental,
  gated by `MOON_VEC_COLD_TIER`) were never summed at all even though cold docs
  ARE returned by FT.SEARCH; added a `snap.cold` counting loop. (audit findings 20, 28)

### Fixed — Vector: GraphUnion merge recall gate no longer lets a total collapse through (PR #250)

- The background/manual GraphUnion merge recall gate read
  `recall < tolerance && recall > 0.0`, so a merge whose verified recall was
  **exactly 0.0** (total collapse) slipped past the guard and committed. Since
  `verify_merge_recall` returns 1.0 (not 0.0) for the too-few-vectors cases,
  0.0 only ever means a real measured collapse — the `&& recall > 0.0` clause
  was removed so such a merge now aborts. (audit finding 21)

### Fixed — Hardening: APPEND 512MB cap + reject FT.* inside MULTI on prod handlers (PR #250)

- **APPEND** now enforces the 512MB max-string-size limit (matching
  SETRANGE/SETBIT); repeated APPENDs previously grew a value past the documented
  limit unbounded. (audit finding 22)
- **FT.\*** commands are now explicitly rejected inside MULTI/EXEC on the
  production sharded + monoio handlers (they aren't wired through the txn
  execution path), matching the existing handler_single guard — a clear
  "not supported inside MULTI/EXEC" error instead of an incidental one.
  (audit finding 25)

### Changed — Vector storage: fence experimental DiskANN cold tier + drop dead vector WAL (PR #250)

- The DiskANN cold tier (WARM→COLD segment demotion + on-disk Vamana beam
  search) was running by default (`--disk-offload` defaults on, `--segment-cold-after`
  defaults 86400) despite being incomplete — **no cold-segment deletion**
  (DEL/HDEL never removes cold docs), blocking per-hop uring I/O holding the
  segment mutex, `FT.INFO num_docs` under-counting cold segments, unfinished
  restart reload of PQ codebooks, and a library-code `panic!` in
  `DiskAnnSegment::new`. It is now **fenced behind the experimental
  `MOON_VEC_COLD_TIER=1` env gate** (default off); the WARM→COLD transition is
  a no-op otherwise. Warm-tier mmap + LRU eviction (`--vec-warm-mmap-budget`)
  continues to handle out-of-RAM indexes. (audit findings 19, 24, 28)
- `DiskAnnSegment::new` now returns `io::Result` instead of panicking on a
  file-open error, so a cold transition can never abort the shard.
- Removed the dead vector WAL module (`vector/persistence/wal_record.rs`): it
  was never on the live recovery path (superseded by manifest + segment +
  keymap + dedup rescan) and carried a documented double-apply footgun.

### Fixed — Cluster: gossip PING task leak + non-blocking accept send (PR #250)

- The gossip PING ticker (both tokio + monoio) spawned an unbounded
  connect+write+read task every 100ms with **no timeout**; against a dead or
  partitioned peer the tasks piled up forever (task/FD/memory leak), and the
  "random peer" it documented was always the first non-self node. Now a single
  in-flight guard bounds outstanding probes to one, a probe timeout
  (`node_timeout/2`, ≥100ms) cancels a hung connect/read, and targets rotate
  across peers. (audit finding 7)
- The monoio per-shard accept task used the **blocking** `flume::Sender::send()`
  on a bounded channel; because the accept task shares the shard's single
  monoio thread with the event-loop consumer, a full channel deadlocked the
  loop that drains it under connection-storm churn. Fixed by `send_async().await`
  (cooperative yield, keeps backpressure). (audit finding 8, Batch B)

### Fixed — Security: bound client-controlled allocation counts (DoS class, PR #250)

- Six wire-reachable command paths fed a client-supplied count straight into
  `Vec::with_capacity` / `Vec::resize` with no upper bound. On a real host the
  oversized request fails allocation and Rust's `handle_alloc_error` calls
  `abort()` — uncatchable, crashing every shard and connection. A single
  unauthenticated request was a full-process DoS. Fixed by clamping each count
  before allocating: **FT.SEARCH HIGHLIGHT/SUMMARIZE FIELDS** (bound by
  remaining tokens), **HSCAN COUNT** (`count.min(total)`),
  **SRANDMEMBER / HRANDFIELD / ZRANDMEMBER** negative COUNT (absolute 2²⁰ cap
  with a loud `ERR COUNT is out of range` beyond it — the first-cut relative
  `len()*10` cap silently truncated legitimate requests on small collections,
  breaking Redis's exact-|COUNT|-with-duplicates contract; caught in review and
  fixed at all six sites including the pre-existing HRANDFIELD/ZRANDMEMBER
  ones), **BITFIELD SET/INCRBY** offset (reject past the 512MB limit, matching
  SETBIT; GET exempt), and **ZMPOP COUNT** (`pop_count.min(card)` — this last
  site was missed by the audit finders and caught by the allocation sweep).
  Each fix has a red/green test. From the production-hardening audit (Batch A).

### Fixed — Review follow-ups on the hardening sweep (PR #250)

- **Gossip PONG fragmentation (monoio):** the failure-detector's PING probe read
  the 4-byte length and the PONG body with single `read()` calls, so a valid
  frame arriving fragmented was silently dropped — leaving `pong_recv_ms` stale
  and pushing a healthy peer toward PFAIL. Both reads now use the bus listener's
  exact-read loop, and both runtimes' probes cap the peer-supplied PONG length
  at the bus's 64KB frame bound before sizing the buffer (the tokio probe
  previously allocated an unchecked `vec![0u8; pong_len]`).
- **AOF corruption offsets are file-absolute:** replay's truncation/corruption
  logs reported offsets relative to the RESP section; with an RDB preamble the
  operator-facing repair guidance (redis-check-aof, truncation point) pointed at
  the wrong file location. Offsets now include the preamble length.
- **Cross-shard MSET no longer OKs an unconfirmed leg:** a timed-out, closed, or
  errored remote `MultiExecute` ack was discarded and the client still got `OK`.
  All legs are drained (each was already dispatched, and the local leg still
  persists), but any failed leg now surfaces as an error instead of a false ack.
- **`recv_reply_bounded` extended to the handler plane:** the coordinator's 30s
  bounded reply-await now also covers the 11 remaining direct cross-shard acks
  (WS.DROP cleanup, routed MQ commands, routed graph commands, TXN.COMMIT MQ
  materialization acks, and the txn-abort remote rollback ack, on both
  handler_monoio and handler_sharded) — a wedged owner shard can no longer park
  those connections forever. MQ/graph/TXN owner execution is synchronous on the
  shard thread (no blocking-wait commands route through these paths), so the
  bound cannot cut off a legitimate long wait.
- **Checkpoint finalize backoff arms from the failure time:** the backoff was
  armed with a timestamp captured before the finalize I/O; when
  `wait_durable`/`manifest.commit`/`graph_save`/control-write took longer than
  the backoff window, the retry deadline was already in the past and the next
  1ms tick retried immediately — exactly the flood the backoff exists to stop.

### Added — CLI/moon.conf defaults for vector + graph tuning knobs (PR #TBD)

- **`--vector-ef-runtime` / `--vector-rerank-mult` / `--vector-exact-beam`**
  set the server-wide starting values every NEW vector index is created with
  (FT.CREATE), so recall-sensitive fleets configure once instead of issuing
  FT.CONFIG per index. Per-index `FT.CONFIG SET` always overrides. Same
  ranges as FT.CONFIG (ef 10-4096 or 0=auto, mult 1-64), validated loudly at
  startup. All three work as `moon.conf` keys (`vector-exact-beam yes|no`).
- **`--graph-result-cache-entries` / `--graph-result-cache-bytes`** size the
  per-graph Cypher result cache (previously hardcoded 256 entries / 4 MiB).
- Startup wiring is first-write-wins process state, installed by both the
  binary entry and `run_embedded`; unit tests and library embedders that
  never install defaults keep the exact pre-flag behavior.

### Added — Recall knobs: FT.CONFIG RERANK_MULT + EXACT_BEAM (PR #TBD)

- **`FT.CONFIG SET <idx> RERANK_MULT <n>`** (1-64, default 4) deepens the HQ-1
  exact-rerank stage: the top `n·k` beam candidates are re-scored with true f16
  sidecar distances before top-k truncation, recovering true neighbors the
  quantized ADC ranking dropped below the default 4·k cut. Cost ~`n·k·dim` f16
  decodes per segment.
- **`FT.CONFIG SET <idx> EXACT_BEAM ON|OFF`** (default OFF) navigates the HNSW
  beam itself with exact f16 distances instead of quantized ADC estimates —
  beam candidate *selection* becomes exact, so recall is graph-limited
  (Qdrant-parity at equal ef) rather than quantization-limited. QPS cost grows
  with dimension. Segments without an exact-rerank sidecar (pre-HQ-1 disk
  reloads) silently keep the quantized beam.
- Both knobs are per-index, apply to the next FT.SEARCH (all search paths:
  sync, yielding, worker-pool fan-out, FT.RECOMMEND, hybrid), broadcast to all
  shards, and persist across restarts via a new v4 index-meta sidecar format
  (v1-v3 sidecars load with defaults).

### Added — Runtime-tunable EF_RUNTIME via FT.CONFIG (PR #TBD)

- **`FT.CONFIG SET <idx> EF_RUNTIME <n>`** adjusts the HNSW search beam width at
  runtime without rebuilding the index (RediSearch parity). Range matches
  FT.CREATE (10-4096); `0` restores the auto heuristic. Applies to the next
  FT.SEARCH immediately and persists via the index meta sidecar. `FT.CONFIG GET
  EF_RUNTIME` added alongside.
- **Fixed — FT.CONFIG SET was local-shard only under monoio**: at shards>1 the
  setting silently applied to 1/N index partitions (AUTOCOMPACT,
  COMPACTION_WEIGHT, MERGE_RECALL_TOLERANCE were equally affected). SET now
  broadcasts to all shards like FT.CREATE; GET stays local.

### Fixed — TQ ADC estimator is now metric-faithful on raw L2 (PR #TBD)

- **TQ quantization ranked L2 queries by `sphere_dist·‖a‖²`** — the unit-sphere
  distance between normalized directions scaled by the document norm. On
  unnormalized L2 data this makes every small-norm vector look near regardless
  of direction (gist-960: recall 0.002). All TQ scoring paths (HNSW beam +
  budgeted variant, mutable brute-force + FastScan pre-filter bound, flat
  scan, multibit brute-force, A2 decoded-L2) now reconstruct the true metric:
  `‖a−q‖² = (‖a‖−‖q‖)² + ‖a‖·‖q‖·d̂²_sphere`. COSINE/IP scoring is unchanged.
  Unit tests pin recall on varying-norm data at both the mutable-scan and
  HNSW-beam levels (7+/10 and 6+/10 vs exact f32 ground truth; 4/10 and worse
  before the fix). GCE re-verification on gist-960 with explicit TQ4:
  recall@10 0.784/0.942/0.986 at ef 16/64/256 vs 0.002-0.003 pre-fix (~350x),
  within ~0.03 of SQ8.

### Changed — FT.CREATE L2 indexes default to SQ8 quantization (PR #TBD)

- **`FT.CREATE ... DISTANCE_METRIC L2` without an explicit `QUANTIZATION` now
  defaults to SQ8** instead of TQ4. TQ's norm-scaled ADC estimator assumes
  unit-sphere metrics (COSINE/IP) and collapses on unnormalized L2 data —
  recall 0.002 measured on gist-960-euclidean (1M × 960d). SQ8 is
  metric-faithful on raw L2. COSINE/IP defaults are unchanged (TQ4). An
  explicit `QUANTIZATION TQ*` + L2 is still honored but logs a warning.
- Benchmarks: `BENCHMARK.md` §10.10 — ANN-benchmarks 4-way campaign (Moon vs
  RediSearch vs Qdrant vs turbovec on glove-200-angular 1.18M / gist-960 1M /
  glove-100K). Tuning guide gains bulk-load (`--max-unflushed-immutable-segments
  0`), settle (`VACUUM VECTOR`), and runtime-`EF_RUNTIME` recipes.

### Added — FastScan SIMD vector scan: NEON TBL kernel + live-path integration (PR #TBD)

- **NEON TBL FastScan kernel** (`src/vector/distance/fastscan.rs`): register-resident
  4-bit LUT accumulation via `vqtbl1q_u8` for aarch64 — the primary platform previously
  fell back to the scalar kernel. 32 candidate distances per block at ~2 ns/candidate
  (128d), 17-21× faster than the per-candidate scalar ADC path.
- **AVX2 kernel overflow fix**: nibble-pair distances were added as u8 before widening,
  silently wrapping for LUT pairs summing past 255 and corrupting rankings. All kernels
  now widen to u16 before adding and accumulate with saturating adds (scalar/NEON/AVX2
  bit-identical across the full u8 LUT range; parity tests no longer mask LUTs to 0x7F).
- **Adaptive quantized LUT builder** (`build_quantized_lut`): FAISS-style per-coordinate
  bias + global scale chosen so entries fit u8 AND the worst-case accumulated sum fits
  u16 (no kernel saturation for in-range data), with f32 reconstruction (`acc/scale +
  bias`). Replaces the legacy `precompute_lut`, which hardcoded the v1 1/sqrt(768)
  `CENTROIDS` table (encode/search codebook asymmetry — the same recall bug class fixed
  by codebook v2) and a fixed `LUT_SCALE` that could overflow both u8 entries and the
  u16 accumulator.
- **Mutable-segment FastScan pre-filter** (`src/vector/segment/mutable.rs`): the MVCC
  brute-force scan now maintains a FAISS-interleaved shadow of the TQ4 codes (32-vector
  blocks, +padded_dim/2 B/vector) and screens candidates with the SIMD kernel; only
  candidates whose sound lower bound (`approx − 0.5·padded_dim/scale`) beats the current
  heap worst get the exact f32 ADC rescore. Results are bit-identical to the plain scan
  (the bound is a true lower bound; saturation only under-estimates). Measured: top-10
  over 20K×128d 1.25 ms → 96 µs (~13×), 5K×768d 1.66 ms → 423 µs (3.9×) on Apple
  Silicon. Engages above 64 entries (per-query LUT build costs ~2-8 µs); SQ8/A2
  collections and TQ-prod scoring keep the existing paths.
- New criterion bench `benches/fastscan_bench.rs` (block kernels, LUT build, end-to-end
  mutable scan A/B); shadow-layout + FastScan-vs-plain equality tests (full scan,
  chunked scan, MVCC visibility + bitmap-filter paths).

## [0.6.0] — 2026-07-08

**Release highlights** (full detail in the sections below; "PR #TBD" entries
below all shipped together in this release's PR):

- **Multi-db isolation is now a first-class, enforced boundary**: FT.*/graph/
  full-text indexes are scoped to the db that created them (`SELECT 1`'s
  indexes are invisible to db 0, verified across shards, restarts, and the
  recovery path); per-db memory quotas (`--db-maxmemory <db>:<bytes>`,
  `CONFIG SET db-maxmemory`) enforce noeviction/evicting policies per db slot
  with Redis-style deny-OOM semantics (shrink commands always pass, so a
  tenant can never wedge itself); named workspaces harden the prefix layer.
- **Memory accounting is now truthful under container growth**: HSET/LPUSH/
  SADD/ZADD growth into existing keys is charged to `used_memory` in O(1)
  (previously invisible to `--maxmemory` — an unbounded single-key hash could
  never trigger eviction).
- **Engines offload when idle**: vector segments demote HOT→WARM (mmap)→COLD
  (unloaded stub, reload-on-search) on configurable idle/age thresholds
  (`--engine-offload-idle-secs`, `--segment-warm-after`), with DEL/HDEL
  tombstone correctness across all tiers — measured −26% process RSS on a
  40K×768d corpus with search results identical after reload.
- **Single-node tuning preset**: `--profile standalone` fills in the measured
  best flags for a shard-1 deployment (the p=1 busy-poll configuration that
  beats Redis on both GCE arches).
- **Command parity widened**: SORT_RO / BITFIELD_RO / GEORADIUS_RO /
  GEORADIUSBYMEMBER_RO plus subcommand gaps; the compat matrix is regenerated.
  PFDEBUG/PFSELFTEST/FAILOVER/MODULE/SENTINEL are documented non-goals.

> **Operator callout — historical `WS DROP` key leak.** Before this release,
> `WS DROP`'s cleanup sweep was hardcoded to logical db 0: any workspace whose
> connection ever `SELECT`ed a non-zero db before writing leaked those keys
> permanently on drop. v0.6.0 fixes the sweep (all dbs on the owning shard),
> but keys leaked by PAST drops are still resident. To detect/clean on an
> upgraded instance: `SELECT <n>` each non-zero db and `SCAN 0 MATCH <ws-uuid>:*`
> for workspace prefixes that no longer appear in `WS LIST`, then `DEL` the
> matches (or `FLUSHDB` if the db held nothing else).

### Fixed — WS3 COLD/WARM tier adversarial-review fixes (round 2 follow-up, PR #TBD)

An adversarial review of the round-2 COLD tier (below) found one CRITICAL
correctness bug and several hardening gaps. All fixed on the same branch:

1. **CRITICAL: DEL/HDEL resurrection through WARM/COLD.**
   `tombstone_key_in_index` (`src/vector/store.rs`) only ever walked
   `mutable` + `immutable` — a HDEL against a key living in a WARM or COLD
   (unloaded) segment was silently dropped. Sequence: index idles overnight
   -> HDEL an old doc -> next-day query resurrects it as a phantom hit, and
   `num_docs` over-counts. Fixed by giving both tiers a live tombstone set:
   - `WarmSearchSegment` gained the same interior-mutable
     `tombstoned_keys`/`has_tombstones` pattern `ImmutableSegment` already
     has (`mark_deleted_by_key_hash`, `seed_tombstones`,
     `tombstoned_key_hashes`, `live_count`) — `search_filtered` now filters
     tombstoned entries out before rerank/truncate, same ordering as the HOT
     path. Takes effect immediately, no reload required.
   - `UnloadedSegment` gained a `pending_tombstones` set — a HDEL against a
     COLD key is queued in the stub (no reload forced just to record a
     delete) and replayed onto the freshly-reloaded `WarmSearchSegment` in
     `reload()` via `seed_tombstones`.
   - `tombstone_key_in_index` now also calls `mark_deleted_by_key_hash` on
     every `warm` and `unloaded` entry.
   - Bonus fix found while wiring this up: `mvcc_raw_bytes()` (the HOT->WARM
     transition's serialization of MVCC headers) only captures INSTALL-time
     deletes (`mvcc[i].delete_lsn`), never the STEADY-STATE
     `tombstoned_keys` interior set — so a HDEL landing shortly before a
     transition was ALSO silently lost, independent of the round-2 COLD
     work (this bug predates it, in the pre-existing age-based WARM path).
     `try_warm_transitions_idle` now seeds the new `WarmSearchSegment` with
     `imm.tombstoned_key_hashes()` right after opening it, closing this gap
     for both WARM and COLD destinations for free.
   - `FT.INFO`'s `num_docs` now sums `warm.live_count()` /
     `stub.live_count()` (total minus live tombstones) instead of the raw
     `total_count()`, at all 4 call sites (top-level + per-field, default +
     non-default field).
   - New tests: `tests/vector_idle_unload.rs`'s
     `hdel_during_cold_does_not_resurrect` and
     `hdel_during_warm_does_not_resurrect` — HDEL a doc, wait for the
     COLD/WARM transition (or vice versa), assert `num_docs` drops
     immediately and the doc never resurfaces in a post-transition
     `FT.SEARCH`, in both tier orderings.
2. **Reload thread placement — verified empirically, documented honestly.**
   Traced all 3 `promote_unloaded` call sites
   (`SegmentHolder::search_filtered`, `SegmentHolder::search_mvcc`, and the
   FT.SEARCH yielding-path snapshot capture in
   `command/vector_search/ft_search/dispatch.rs`): all three run inside
   `crate::shard::slice::with_shard(...)`, i.e. on the shard's own OS
   thread, before any `.await` boundary — PR #179's off-loop worker pool
   only carries the HNSW beam search itself off-loop, not the capture phase
   a COLD reload runs in. This means a first-touch reload is a SHARD-WIDE
   stall (every connection sharing that shard, not just the querying one),
   for the reload's duration. A real off-loop fix needs `SegmentHolder`
   reachable from the async continuation without an open `VectorIndex`
   borrow (e.g. `Arc`-wrapping it, a ~100-call-site change) — judged
   out-of-scope / too risky for this pass and tracked as a follow-up rather
   than attempted half-finished. `docs/guides/tuning.md` now documents this
   precisely instead of the earlier (incorrect) "one-query latency hit"
   framing, plus a same-instance measurement of the stall's actual duration:
   the first-touch `FT.SEARCH` reload took 79.59 ms round-trip, during which
   a concurrent `PING` on a second connection to the same shard peaked at
   76.70 ms (vs sub-millisecond baseline) — confirming the block is
   shard-wide and roughly the full reload duration, not a per-connection cost.
3. **WARM segments could never reach COLD.** `try_warm_transitions_idle`
   only ever scanned `snapshot.immutable` for idle/age eligibility — a
   segment that had already demoted to WARM (age-based) had no further path
   to COLD even if it then went idle, defeating the whole point of the COLD
   tier for that segment permanently. Fixed: the same function now also
   scans `snapshot.warm` (`WarmSearchSegment::idle_secs`, a new method
   mirroring `ImmutableSegment::idle_secs`) and demotes idle WARM segments
   straight to COLD stubs via the same `UnloadedSegment::from_warm` used for
   the immutable path — mechanical, since a WARM segment's `.mpf` files
   already exist on disk.
4. **Repo-rule compliance: no per-query `SystemTime::now()` syscalls.**
   `WarmSearchSegment::touch_last_access`/`from_files` and
   `ImmutableSegment`'s module-level `now_micros()` called
   `SystemTime::now()` directly on a path that runs once per query per
   segment. Both now read `crate::storage::entry::current_time_ms()` — the
   existing shard-tick-populated thread-local cached clock (`TL_NOW_MS`,
   updated once per ~1ms shard tick) that already backs the KV hot path,
   with its documented automatic syscall fallback on threads that never
   ticked a shard clock (tests, or an off-loop FT.SEARCH worker thread).
   Millisecond resolution (×1000 for unit parity) is sufficient since every
   caller only compares `age_secs`/`idle_secs` at second granularity.
5. **Tightened the RSS regression test.** `cold_unload_reduces_process_rss`
   asserted a plain `rss_after < rss_before`, which would still pass for a
   regression that only frees (say) the HNSW graph while leaving TQ codes +
   sidecar resident. Now asserts `rss_after < rss_before * 0.85` (a 15%
   floor — the real 40K×768d measurement showed −26.2%, so this has margin
   for the smaller CI-scale fixture and jemalloc arena noise without being
   toothless).
6. **`SegmentList::with_unloaded` / `with_warm_and_unloaded` clone-and-patch
   helpers.** `SegmentList` now derives `Clone` (all fields are
   `Arc`/`Vec<Arc<_>>` — refcount bumps, never a data copy) so reconstruction
   sites can use Rust's functional-update syntax
   (`SegmentList { changed_field, ..old.clone() }`) instead of enumerating
   all 6 fields by hand. Applied at `try_warm_transitions_idle`'s final
   `swap` (the site touched by this review's item 3 fix). This does NOT
   convert all ~17 `SegmentList { ... }` construction sites in the codebase
   — only the ones this pass's changes already touch, verified to compile.
   The remaining sites are left as full literals rather than converted
   speculatively without individual verification; they will fail to compile
   the moment a new field (e.g. WS5a's `db_index`) is added, which is itself
   the forcing function that gets them individually attended to. The
   pattern (derive Clone + functional update, plus the two named helpers)
   is now established for that future pass to reuse.
7. **Freshly compacted segments were unloaded to COLD before they were ever
   searchable.** Two stacked bugs, found via `cold_unload_reduces_process_rss`
   timing out 600s waiting for `graph_segments >= 1`:
   - `ImmutableSegment` seeds `last_access_micros` at *construction* (build
     start), but a multi-second HNSW build consumes the whole
     `--engine-offload-idle-secs` budget before the segment is published —
     the idle sweep then unloaded the brand-new segment in its very next
     tick (server log: "Cold (unload) transition: segment 1 (15000 vectors,
     idle 3s)" 4.4s after server start). Fixed with
     `ImmutableSegment::mark_installed()`, called at every point a built
     segment is swapped into the live `SegmentList` (inline + background
     compaction installs, both merge installs): idle time is now measured
     from *visibility*, not from the start of the build.
   - `mark_installed` deliberately bypasses the cached clock (item 4 above)
     with a real `SystemTime::now()` syscall: the inline FT.COMPACT path
     builds ON the shard thread, so `TL_NOW_MS` hasn't advanced for the
     entire build and stamping the cached value would be build-duration
     stale — the first iteration of this fix did exactly that and still
     produced "idle 9s" on a just-installed segment. Install is a cold path;
     one syscall is fine (does not violate the per-query no-syscall rule).
### Added — true COLD tier: idle segments now actually free memory (WS3 round 2, PR #TBD)

Round 1 (below) shipped an idle *trigger* but routed it into the pre-existing
WARM tier, which copies `.mpf` payloads into owned buffers of the same size
as the HOT segment it replaces -- measured flat-to-+1.1% RSS, not a
reduction. Round 2 fixes the actual memory problem:

- **New tier, `SegmentList.unloaded: Vec<Arc<UnloadedSegment>>`**
  (`src/vector/persistence/unloaded_segment.rs`, new file): a COLD segment
  stub holding only a handful of scalars (segment id, doc count, whether it
  had an exact-rerank sidecar, an `mlock_codes` flag) plus a cloned
  `SegmentHandle` keeping the on-disk directory alive. Nothing else --
  no TQ/SQ8 codes, no HNSW graph, no f16 sidecar buffer.
- **Idle now routes straight to COLD, not WARM**
  (`VectorIndex::try_warm_transitions_idle`, `src/vector/store.rs`): the two
  triggers now have different destinations. `age_eligible`
  (`--segment-warm-after`) still demotes to WARM exactly as before (an
  old-but-still-hot segment structurally simplifies to disk-backed storage,
  no memory-reduction promise). `idle_eligible`
  (`--engine-offload-idle-secs`, genuinely cold) now demotes straight to
  COLD -- this is the one that actually frees memory. If a segment
  satisfies both simultaneously, COLD wins. Both destinations write the
  identical `.mpf` files via the existing `warm_tier::transition_to_warm`
  (sidecar included); COLD additionally drops the materialized
  `WarmSearchSegment` immediately after capturing the stub, instead of
  keeping it resident.
- **Transparent, synchronous, single-flight reload on touch**
  (`SegmentHolder::promote_unloaded`, `src/vector/segment/holder/promote.rs`
  -- split out as a child module to keep `holder.rs` under the 1500-line
  cap): every search path (`search_filtered`, `search_mvcc`, and the
  yielding worker-pool capture in
  `command/vector_search/ft_search/dispatch.rs`) calls this before scanning
  -- a KNN query must consider every segment for correctness, so a COLD
  segment cannot stay unloaded and still participate. Fast path (nothing
  unloaded) is a single `is_empty()` check, no lock, no allocation. When
  there IS something to reload, a blocking (never `try_lock`, never held
  across `.await`) `reload_lock` mutex makes it single-flight: N concurrent
  callers touching the same COLD segment all wait for the one reload
  (double-checked re-load after acquiring the lock) rather than racing
  ahead with a stale, missing-segment snapshot. Reload reuses
  `WarmSearchSegment::from_files` -- the same function the WARM tier and
  server-boot recovery already use -- so recall/exactness (sidecar
  included) is unaffected; a segment that fails to reload (corrupt/missing
  files) stays in `unloaded` and is retried on the next touch without
  poisoning the rest of the batch.
- **`FT.INFO`**: new additive counters `unloaded_segments` /
  `unloaded_segments_with_exact_rerank` (the latter answered from the stub,
  no reload needed), and `num_docs` (both top-level and per-field) now also
  sums `unloaded.iter().map(UnloadedSegment::total_count)` -- the same
  regression class fixed for WARM in round 1, this time for COLD.
- **Lifecycle**: `VectorStore::drop_index` / `clear_all_contents` (FLUSHALL/
  FLUSHDB) now also tombstone every `unloaded` stub's `SegmentHandle`, or
  its on-disk directory would leak once the stub itself is dropped (WARM
  segments already got this treatment; COLD needed the same). GraphUnion
  merge scheduling (`VectorIndex::needs_merge` / `begin_background_merge`)
  only ever reads/filters `snapshot.immutable` (HOT segments) -- it never
  touches `warm`, `cold` (DiskAnn), or `unloaded`, so COLD segments are
  skipped by construction, not by added special-casing. Server restart:
  COLD segments have no restart-time restoration path, same as WARM already
  didn't -- the segment directories they point at are simply reloaded as
  fresh HOT/immutable segments by the existing boot-recovery scan
  (`persistence/recover_v2.rs`); "should be free" per the original ask.
- **RSS re-measurement** (same methodology as round 1: real server,
  `--shards 1`, 40,000 x 768-dim vectors, SQ8, `ps -o rss=`) -- this time a
  real drop, not the round-1 flat-to-increase result:

  | Phase | RSS (KB) | Delta |
  |---|---|---|
  | Before unload (HOT) | 400,544 | -- |
  | After unload (COLD) | 295,760 | -104,784 KB (-26.2%) |
  | After reload (touched, back to WARM) | 395,376 | +99,616 KB vs COLD |

  The reload number lands just under the original HOT figure because the
  segment reloads into the WARM (owned-buffer) representation, which is
  itself ~1.3% smaller than the original HOT `ImmutableSegment` layout in
  this run -- consistent with round 1's WARM-vs-HOT measurement, not a new
  effect.
- Red/green: `tests/vector_idle_unload.rs`'s
  `hot_segment_idle_unloads_to_cold_and_preserves_recall` (supersedes round
  1's `hot_segment_idle_unloads_and_preserves_recall`, which polled
  `warm_segments` -- now stays 0 for a purely-idle transition) walks HOT ->
  COLD -> reload -> WARM, asserting `FT.INFO` counters at every stage
  (`graph_segments`, `warm_segments`, `unloaded_segments`,
  `*_with_exact_rerank`, `num_docs`) plus recall/exactness before and after.
  `cold_unload_reduces_process_rss` is the process-level RSS proxy (40,000 →
  scaled down to a CI-reasonable 3,000 × 256-dim fixture -- see the full
  40K×768d numbers below for the production-scale claim) asserting a real
  `ps` RSS drop, not just a counter flip. `unloaded_segment.rs`'s own unit
  tests cover stub-capture + reload round-trip and tombstone-triggers-
  directory-removal.
- **Correctness edges considered and their disposition**: concurrent search
  during unload -- covered by single-flight `reload_lock` above; writes/
  tombstones arriving for a COLD segment's key -- `WarmSearchSegment` (and
  therefore also the COLD stub, which reloads into one) has **no per-key
  tombstone mechanism at all**, a pre-existing gap discovered during this
  work (`tombstone_key_in_index` only ever touches `mutable` and
  `immutable`) -- COLD inherits exactly the same behavior as WARM already
  had, so this is a documented pre-existing limitation, not a regression;
  see "Known limitation" below. FLUSHALL/FLUSHDB/DROPINDEX -- handled (see
  Lifecycle above). GraphUnion merge -- skipped by construction (see
  Lifecycle above). Single-flight reload -- handled (see above).

- **⚠ Known limitation (pre-existing, not introduced by WS3): no per-key
  tombstoning for WARM or COLD segments.** `tombstone_key_in_index`
  (`src/vector/store.rs`) only marks deletions in the `mutable` and
  `immutable` (HOT) segments; `WarmSearchSegment` has no delete-bitmap or
  MVCC-visibility mechanism of its own. A key deleted (DEL/HDEL/UNLINK)
  after its vector's HOT segment has demoted to WARM or COLD stays
  returned by that segment's own local search until the segment is later
  merged away or the whole index is dropped/flushed. This predates WS3
  (round 1's WARM tier already had it) and reloading a COLD stub does not
  make it worse or better -- it inherits WARM's exact behavior. Flagged
  here because implementing the new COLD tier required reading this code
  path closely enough to notice it; fixing it is out of scope for this
  workstream (adding per-segment tombstone tracking to `WarmSearchSegment`
  is its own, separately-scoped change) and is recorded as a follow-up.

### Added — vector idle-unload with sidecar-preserving WARM transition (WS3, PR #TBD)

- **`src/vector/segment/immutable.rs`**: `ImmutableSegment` gains a
  `last_access_micros` atomic (mirrors `WarmSearchSegment`'s existing field),
  touched on every `search`/`search_filtered`, exposed via `idle_secs()`.
- **`src/config.rs`**: new `--engine-offload-idle-secs <secs>` (default
  `3600`, `0` disables). A HOT segment now demotes to the mmap-backed WARM
  tier when EITHER `--segment-warm-after` (age) OR
  `--engine-offload-idle-secs` (idleness since last search) is reached —
  whichever fires first — so a segment that is old but still busy stays HOT
  instead of unloading mid-traffic. `src/vector/store.rs` adds
  `try_warm_transitions_idle` / `try_warm_transitions_all_idle` (the
  pre-existing age-only methods now delegate to these with idle disabled, so
  every existing caller is unaffected). `src/shard/{persistence_tick,
  event_loop}.rs` thread the new threshold through; the warm-check poll
  interval also adapts to the lower of the two thresholds for fast local
  testing.
- **Recall fix (was silently dropped, not new WS3 behavior)**:
  `VectorIndex::try_warm_transitions` always passed `None` for the HOT
  segment's f16 exact-rerank sidecar when writing `vectors.mpf`, and
  `WarmSearchSegment` had no sidecar field or rerank logic at all — every
  HOT->WARM transition silently downgraded to quantized ADC-only distances,
  contradicting the sidecar's whole purpose (HQ-1, PR #232). Fixed:
  `src/vector/persistence/warm_search.rs`'s `WarmSearchSegment` now loads
  `vectors.mpf` (when present) into a `RawF16Store`, exposes `raw_f16()`, and
  runs the same `rerank_exact` HQ-1 logic `ImmutableSegment` does before
  truncating to `k`. `VectorIndex::try_warm_transitions` now passes
  `imm.raw_f16()`'s bytes through instead of `None`.
- **Correctness fix (also pre-existing, caught by the new integration
  test)**: `warm_search.rs`'s `parse_global_ids` assumed a 24-byte MVCC entry
  with no `key_hash` field; the actual writer
  (`ImmutableSegment::mvcc_raw_bytes`) emits 32-byte entries
  (`internal_id + global_id + key_hash + insert_lsn + delete_lsn`). Every
  entry past the first was misaligned, and `key_hash` was dropped entirely —
  every WARM-tier FT.SEARCH hit resolved to a synthetic `vec:<id>` key
  instead of the real Redis key. Renamed to `parse_mvcc_ids`, fixed to the
  correct 32-byte stride, and `key_hash` now propagates onto
  `SearchResult.key_hash` via `remap_to_global_ids`.
- **`FT.INFO`**: new additive (scatter-gathered across shards) counters
  `warm_segments` / `warm_segments_with_exact_rerank`, mirroring the existing
  `graph_segments` / `segments_with_exact_rerank` for the HOT tier.
- **Correctness fix (regression this WS3 work would otherwise have shipped,
  caught by RSS verification below)**: `command/vector_search/ft_info.rs`'s
  `num_docs` computation (both the top-level and per-`vector_fields` entry)
  only summed `mutable.len()` + `imm.live_count()` over HOT segments — it
  never counted WARM segments. Before WS3 this was latent (age-based
  HOT->WARM demotion existed but nothing exercised it in a live num_docs
  check); with idle-unload now demoting segments routinely, a fully-idle
  single-segment index would report `num_docs 0` even though every document
  was still present and searchable. Fixed by adding
  `warm.iter().map(WarmSearchSegment::total_count)` to both sums.
- **⚠ Known limitation — RSS does NOT drop after HOT->WARM idle-unload.**
  Verified with a real server (`--shards 1`, SQ8, 40,000 × 768-dim vectors,
  `ps -o rss=` sampled every 2s for 16s after the transition): RSS went from
  395,168 KB to a flat 399,504–399,520 KB post-unload — a **1.1% increase**,
  not a decrease, with zero decay over the observation window. Root cause is
  pre-existing (predates this WS3 change, confirmed by the standing doc
  comment on `WarmSearchSegment::from_files` in
  `src/vector/persistence/warm_search.rs`): the "mmap-backed" WARM tier opens
  each `.mpf` file's mmap only for the duration of `from_files()` and
  immediately copies every payload out into owned `Vec<u8>` /
  parsed-`HnswGraph` buffers of essentially the same size as the HOT
  segment's — the mmap is then dropped. So the two structures that dominate
  memory at scale (TQ codes + HNSW graph) are fully duplicated in heap memory
  on the WARM side, not lazily paged in from disk. The idle-based *trigger*
  added by this change is correct and verified end-to-end (`FT.INFO` counters
  flip, recall/exactness preserved, `num_docs` now stays correct — see
  `tests/vector_idle_unload.rs`), but it does not, by itself, achieve the
  memory-reduction goal this workstream set out to prove. A genuine RSS win
  requires reworking `WarmSearchSegment` to search directly over borrowed
  mmap'd bytes (HNSW traversal + TQ-ADC distance kernels operating on
  `&[u8]` instead of owned `Vec`), which is a substantially larger, higher-risk
  change (touches SIMD distance kernels, needs careful lifetime/alignment
  handling, and was explicitly out of scope for this pass per the "no new
  `unsafe` without approval" constraint). Tracked as an open follow-up; see
  `docs/guides/tuning.md`.
- **`src/config.rs`**: corrected the `--disk-offload-threshold` doc comment,
  which claimed "parsed but not acted upon" — the memory-pressure cascade
  (`persistence_tick::{should_run_pressure_cascade,handle_memory_pressure}`)
  has actually acted on it since an earlier phase; the comment was stale.
- **`src/storage/tiered/cold_read.rs`**: new `_cached` variants
  (`cold_read_through_outcome_cached`, `read_cold_entry_at_cached`) read the
  cold KV leaf page through `PageCache` when given one, instead of always
  `pread`ing — the existing (uncached) functions are now thin wrappers
  calling these with `page_cache: None`, so their behavior and every existing
  call site are unchanged. **Not yet wired into `Database::get()`'s live
  cold-read path** — that needs a `PageCache` handle threaded from the
  per-shard event loop into `Database`, a broader plumbing change deferred as
  a follow-up; the cache-capable read path itself is implemented and tested.
- Docs: `docs/guides/tuning.md` gains "Tiered memory offload" and
  "Vector/FTS/graph idle-unload" sections documenting the new flags,
  `FT.INFO` counters, and the FTS/graph limitation (no equivalent idle-unload
  yet — FTS has no aggregate memory-accounting API, and the graph engine's
  mmap'd `MmapCsrSegment` has no LRU-eviction-driven unload comparable to
  vector's `MmapBudget`).
- Red/green: `tests/vector_idle_unload.rs` (real server process, `MOON_BIN` /
  `CARGO_BIN_EXE_moon`) — idle-out a HOT segment with `--segment-warm-after`
  set far above the test's timeout so only `--engine-offload-idle-secs` can
  trigger the transition; asserts `FT.INFO` counters flip HOT->WARM and a
  post-transition `FT.SEARCH` returns the same key with the same near-zero
  exact-rerank distance (this is what caught both fixes above — it failed on
  the pre-fix code, first on distance, then on key identity).
  `src/storage/tiered/cold_read.rs` unit test proves a second cold read is
  served from `PageCache` even after the backing file is renamed away.
  `src/vector/persistence/warm_search.rs` unit tests cover the sidecar
  round-trip and the corrected MVCC entry parsing.

### Fixed — WS6: self-inflicted write lockout on an over-quota key (HIGH, adversarial review 2026-07-08)

- **HIGH, release-blocking**: making container growth visible to
  `used_memory` (see the accounting-gap fix below) made a second,
  previously-unreachable bug reachable: `run_write_eviction_gate`
  (`src/server/conn/handler_monoio/mod.rs`) and
  `check_db_maxmemory_for_command` (`src/storage/db_quota.rs`) applied
  the `noeviction` reject to **every** write command uniformly — so once
  a key's growth tripped the global `--maxmemory` gate or a db's
  `--db-maxmemory` quota, a pure-shrink command on that SAME key —
  `HDEL`, `SREM`, `LPOP`, `ZREM`, ... — was *also* rejected. A tenant
  that grew a key past the boundary had **no self-recovery path** short
  of `FLUSHALL`/restart, undermining the WS5b per-db-quota guarantee
  this same release ships.
- Fixed with a static, provably shrink-only command classification,
  `db_quota::is_shrink_only_command` (`src/storage/db_quota.rs`),
  mirroring Redis's `CMD_DENYOOM` semantics: `DEL`/`UNLINK`,
  `HDEL`/`HGETDEL`, `SREM`/`SPOP`, `LPOP`/`RPOP`/`LREM`/`LTRIM`/`LMPOP`/
  `BLMPOP`, `ZREM`/`ZPOPMIN`/`ZPOPMAX`/`ZMPOP`/`BZPOPMIN`/`BZPOPMAX`/
  `BZMPOP`/`ZREMRANGEBYSCORE`/`ZREMRANGEBYRANK`/`ZREMRANGEBYLEX`,
  `GETDEL`, `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT`/`PERSIST`,
  `FLUSHDB`/`FLUSHALL` now bypass the *reject* from both the global
  maxmemory gate and the per-db quota gate — eviction is still attempted
  first (an evicting policy may as well reclaim while the write lock is
  already held); only the reject is skipped. Deliberately conservative
  — commands that can grow a destination key (`LMOVE`/`SMOVE`/`COPY`/
  `RESTORE`/any `*STORE` variant) or aren't statically classifiable
  (`SET`, even with a shorter value) are excluded ("when in doubt,
  exclude").
- Applied at all three connection-handler write-path chokepoints:
  `handler_monoio::run_write_eviction_gate`, the inline eviction block in
  `handler_sharded`'s sharded write path, and both inline blocks in
  `handler_single` (the pipelined-dispatch loop and the single-command
  write path).
- RED/GREEN: `test_hdel_self_recovery_past_maxmemory_boundary` and
  `test_hdel_self_recovery_past_db_maxmemory_boundary`
  (`tests/container_growth_memory_accounting.rs`) — confirmed RED against
  the pre-fix handler/db_quota code (`git stash`): both failed with the
  exact OOM-on-HDEL symptom described above. Each test now: grows a hash
  past its cap (asserting the growing HSET IS rejected with the correct
  OOM flavor), asserts `HDEL` on that same over-cap key succeeds, drains
  the rest, then asserts a follow-up `HSET` succeeds once back under
  budget.

### Fixed — WS6: container-growth memory accounting gap (HSET/LPUSH/SADD/ZADD... growth was invisible to `--maxmemory`)

- **Critical**: `used_memory` was charged once at key-creation time for an
  EMPTY Hash/List/Set/ZSet container (`entry_overhead()` in
  `src/storage/db.rs`, called from `get_or_create_hash`/`_list`/`_set`/
  `_sorted_set`), then a raw `&mut` reference into the map/deque/set/tree
  was handed to the command layer. Every subsequent mutation through that
  reference — `HSET` adding fields, `LPUSH` growing the deque, `SADD`/
  `ZADD` growing the set/sorted-set, and their listpack/intset compact-
  encoding equivalents — never updated `used_memory` again, so growing a
  SINGLE key's contents was invisible to both the global `--maxmemory`
  gate and the per-db quotas (WS5b). Confirmed by adversarial review
  2026-07-08 (flagged as a known limitation in `docs/guides/isolation.md`
  at the time).
- Fixed via O(1) per-mutation delta accounting rather than a full
  recompute: `RedisValue::estimate_memory()` sums over every element for
  the expanded Hash/List/Set/SortedSetBPTree encodings, so recomputing it
  on every single-field HSET on a large hash would turn an O(1) command
  into O(n). New `Database::charge_memory`/`credit_memory`/`adjust_memory`
  (O(1), saturating) plus per-container byte-cost helpers next to
  `entry_overhead` (`hash_field_cost`, `list_elem_cost`, `set_member_cost`,
  `zset_member_cost`) that mirror `RedisValue::estimate_memory`'s
  per-variant formulas exactly. The listpack/intset COMPACT encodings are
  the exception — their `estimate_memory()` is already O(1)
  (capacity-based), so those call sites snapshot before/after instead.
- Every hash/list/set/zset mutation site instrumented: `HSET`/`HMSET`/
  `HINCRBY`/`HINCRBYFLOAT`/`HSETNX`/`HDEL`/`HGETDEL`/`HEXPIRE`-family
  past-expiry delete (`src/command/hash/hash_write.rs`,
  `src/storage/db.rs`); `LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`LSET`/`LINSERT`/
  `LREM`/`LTRIM`/`LMOVE`/`LPUSHX`/`RPUSHX`/`LMPOP`
  (`src/command/list/list_write.rs`, `src/storage/db.rs`'s
  `list_push_front`/`_back`/`list_pop_front`/`_back`); `SADD` (intset +
  HashSet paths)/`SREM`/`SPOP`/`SMOVE` (`*STORE` variants were already
  correct — they replace the whole entry via `Database::set`, which
  already accounted correctly); `ZADD`/`ZREM`/`ZINCRBY`/`ZPOPMIN`/
  `ZPOPMAX`/`ZMPOP`/`ZUNIONSTORE`/`ZINTERSTORE`/`ZRANGESTORE`
  (`src/command/sorted_set/sorted_set_write.rs`). Whole-key removal paths
  (`Database::remove`/`Database::set`) needed no changes — they already
  recompute `entry_overhead` on the final (already-mutated) entry, an O(n)
  cost paid once per key lifetime, not per mutation, so they correctly
  credit a grown container's true size when it's fully removed or
  replaced.
- The eviction loop's before/after `estimated_memory()` delta arithmetic
  (`src/storage/eviction.rs`) required no code changes — it already reads
  the (now-correct) live `used_memory` accumulator.
- RED/GREEN: `tests/container_growth_memory_accounting.rs` (growing one
  HSET/LPUSH key past `--maxmemory` under `noeviction` now rejects further
  writes; draining a grown hash via `HDEL` correctly credits the freed
  bytes back). Unit tests per container family (hash/list/set/zset
  `mod.rs`) assert `estimated_memory()` rises/falls with insert/remove/
  overwrite, including a same-length overwrite netting a zero delta.
- Making this growth visible surfaced a second, previously-unreachable
  bug — a self-inflicted write lockout where even shrink commands like
  `HDEL` were rejected on an over-quota key. Now fixed; see the dedicated
  entry above.

### Added — sharded MULTI/EXEC routes a single-owner-shard body to its owner (Phase B, PR #TBD)

- **`src/shard/{dispatch,spsc_handler,coordinator}.rs`,
  `src/server/conn/{handler_sharded,handler_monoio}/write.rs`**: with keys routed
  per-key to their owning shard but MULTI/EXEC executing on the connection's
  (random, SO_REUSEPORT) accept shard, a hash-tagged `{tag}` transaction only
  worked from the ~1/N connections that happened to land on the owning shard —
  Phase A rejected the rest with `CROSSSLOT`. Phase B adds a boxed
  `ShardMessage::TxnExecute` (kept within the 64-byte cache-line cap like
  `MqCommand`) and a `coordinator::execute_txn_on_owner` hop: when
  `analyze_txn_locality` proves every key is owned by ONE shard that isn't the
  accept shard, the whole body is routed there, executed atomically on the
  owner's slice, and each write persisted to the OWNER's AOF/WAL via the same
  `wal_append_and_fanout` path as normal cross-shard writes. PUBLISH fan-out is
  deferred back to the originating connection (returned in the reply) so it keeps
  the normal scatter path; under `appendfsync=always` the originator issues one
  `fsync_barrier` to the owner before acking (H1-BARRIER parity), and owner-side
  append backpressure surfaces `AOF_APPEND_LOST_ERR`. Genuinely multi-shard
  bodies (`CrossShard`) stay rejected — a shared-nothing engine can't commit
  across shards atomically. Red/green: `tests/sharded_multi_exec_routing.rs`
  (hash-tagged txn succeeds from every connection + writes visible; routed writes
  survive kill-9 + restart via the owner's AOF; multi-shard span still CROSSSLOT).
  WATCH in sharded mode remains an unimplemented follow-up.

### Fixed — sharded MULTI/EXEC writes are now persisted (were lost on restart) (PR #TBD)

- **`src/server/conn/{shared,handler_sharded/write,handler_monoio/write}.rs`**:
  `execute_transaction_sharded` — the MULTI/EXEC executor used by the monoio
  handler at **every** shard count (including `--shards 1`) and by the tokio
  sharded handler at `--shards ≥ 2` — appended **nothing** to the AOF. Every
  transactional write was silently lost on restart under `appendonly=yes`, while
  an identical write issued outside MULTI survived. The executor now returns the
  serialized AOF bytes for each successful write (mirroring the single-shard
  tokio `execute_transaction`), and a shared `persist_txn_aof` helper appends
  them to the owning shard's writer via the normal group-commit path, issuing one
  `fsync_barrier` under `appendfsync=always` before EXEC is acked. On a barrier
  failure EXEC returns `AOF_FSYNC_ERR` and suppresses any queued PUBLISH fan-out,
  rather than acking a durability it can't guarantee. `try_handle_multi_exec` is
  now `async` in both sharded handlers. Red/green:
  `tests/sharded_multi_exec_durability.rs` pins *EXEC-committed write survives
  kill-9 + restart, exactly like a non-MULTI write* (fails on the pre-fix path:
  the txn key returns nil after restart while the plain control key survives).

### Security — sharded MULTI/EXEC no longer silently misplaces cross-shard writes (Phase A, PR #TBD)

- **`src/server/conn/{shared,handler_sharded/write,handler_monoio/write}.rs`**:
  `execute_transaction_sharded` runs the whole queued body on the connection's
  OWN shard with no per-key routing, so at `--shards ≥ 2` a key owned by another
  shard was silently written to / read from the wrong shard's table — EXEC
  reported success while the data diverged (silent lost updates; other
  connections routing to the true owner saw nothing). Hash tags did **not** help:
  they co-locate keys with each other but not with the (random, SO_REUSEPORT)
  execution shard, and migration is disabled during MULTI. A new
  `analyze_txn_locality` classifies the queued body via the command-metadata key
  specs + `key_to_shard`; EXEC now **rejects** with `CROSSSLOT` any transaction
  whose keys aren't all owned by the executing shard, instead of corrupting.
  Invariant restored: *EXEC-success ⇒ every write is visible to other
  connections; CROSSSLOT ⇒ nothing was written.* No effect at `--shards 1`.
  Red/green: `tests/sharded_multi_exec_locality.rs` (silent-divergence invariant,
  multi-shard span rejection, single-shard unaffected) + `analyze_txn_locality`
  unit tests. Phase B (above) now routes single-owner-shard bodies to their owner
  instead of rejecting them, so only genuinely cross-shard bodies still get
  CROSSSLOT. Note found in passing (unrelated, out of scope): a *solo* GET sent
  mid-MULTI on the monoio single-shard handler executes immediately instead of
  queueing.

### Added — `--profile standalone` tuning preset (v0.6.0 WS4)

- New `--profile <name>` flag (`src/config.rs`). `standalone` fills the proven
  single-instance p=1 recipe — `--shards 1`, `--io-busy-poll-us 40` (implying
  `--io-driver epoll`) — for any of those flags the operator left unset.
  Fill-only precedence: an explicitly-passed flag (CLI or `moon.conf`) always
  wins over the preset. Startup logs exactly which flags the profile set;
  an unknown profile name is a startup error (exit 2), never a silent no-op.
- `ServerConfig::parse_from_with_matches` + `ServerConfig::apply_profile` use
  `clap::ArgMatches::value_source` to distinguish explicit flags from
  defaults; `FromArgMatches::from_arg_matches` (non-consuming, clones
  internally) is used instead of `from_arg_matches_mut`, which removes
  consumed entries from `ArgMatches` and would erase this provenance.
- **Safety:** `--io-busy-poll-us` busy-polls the shard thread and REGRESSES
  throughput on shared/unpinned cores (OrbStack default, laptops,
  noisy-neighbor cloud VMs) — `standalone` prints a prominent startup warning
  requiring pinned/dedicated cores, and `docs/guides/tuning.md#profiles` +
  `docs/configuration.md` document the same caveat. No raw flag default
  changed.
- Tests: profile expansion, explicit-flag-wins precedence, no-profile no-op,
  and unknown-profile error (`src/config.rs` `tests::test_profile_*`).
### Added — WS1 command parity: `*_RO` variants + ACL LOG/CLIENT LIST/OBJECT HELP audit (PR #TBD)

- **`BITFIELD_RO`, `SORT_RO`, `GEORADIUS_RO`, `GEORADIUSBYMEMBER_RO`**: new
  read-only twins registered in the `phf` command registry with all three
  dispatch paths wired (mutable `dispatch()`, immutable `dispatch_read()`,
  and the `is_dispatch_read_supported()` fast-reject bucket list — a command
  missing that last one is unreachable over the wire despite compiling and
  passing unit tests). Each rejects its write-capable subcommand/option
  (`SET`/`INCRBY`/`OVERFLOW` for BITFIELD_RO; `STORE` for SORT_RO;
  `STORE`/`STOREDIST` for the GEORADIUS twins) via positional parsing (not a
  blind token scan, so a `GET`/`BY` pattern value that happens to read
  "STORE" is never misclassified).
- **CLIENT LIST/INFO**: added the missing Redis fields (`laddr`, `multi-mem`,
  `tot-net-in`/`tot-net-out`, `rbs`/`rbp`/`obl`/`oll`/`omem`, `events`,
  `cmd`, `redir`, `resp`, `lib-name`, `lib-ver`) so key=value parsers no
  longer choke on absent keys; `redir` and the tracking flag stay at their
  "off" defaults; wiring them to the CLIENT TRACKING state shipped in PR #234
  is a known follow-up.
- **Audit findings**: `ACL LOG` (real entries + RESET) and `OBJECT HELP`
  were already fully implemented — added regression tests to lock in that
  coverage rather than re-implementing.
- `docs/redis-compat.md` regenerated against `src/command/metadata.rs`
  (258 commands): fixed stale claims that `WAIT` and `FUNCTION *` are
  unimplemented (both are live), documented the four new `_RO` commands,
  and added an explicit non-goals section for `PFDEBUG`, `PFSELFTEST`,
  `FAILOVER`, `MODULE *`, `SENTINEL *` with one-line rationale each.
- `scripts/test-consistency.sh` (+section 9b) and `scripts/test-commands.sh`
  (key-commands category) gained coverage for all four new commands.

### Fixed — WS5b fix-first adversarial review: db-quota bypass on non-inline writes

- **Critical**: `--maxmemory 0` combined with `--disk-offload disable` (no
  disk-offload spill sender) silently bypassed the per-db quota gate for
  every non-inline write command (HSET/LPUSH/SADD/ZADD/INCR/APPEND/MSET/
  SET-with-options/RESTORE/...) — plain `SET`/`GET` were unaffected because
  they take a separate, always-correctly-gated inline fast path. Root cause:
  `handler_monoio`'s `batch_eviction_active` and its cross-shard-leg twin
  `spsc_handler`'s `evict_active` only checked
  `spill_sender.is_some() || maxmemory != 0`, entirely skipping the
  write-eviction-gate call (and the db-quota check nested inside it) when
  neither was true. Fixed by adding `db_quota::db_maxmemory_any_set()` to
  both conditions, mirroring the Lua bridge's gate (which already had this
  term). RED/GREEN TDD via `git apply -R`; new integration test
  `test_quota_rejects_non_inline_writes_without_spill_sender` in
  `tests/db_maxmemory_quota.rs` (HSET + SET-with-EX). Manually re-verified
  `RESTORE` also rides the fixed gate (rejected before payload
  deserialization).
- Along the way, found a second, independent, pre-existing bug: `used_memory`
  accounting for Hash (and, by the same code pattern, List/Set/ZSet) is only
  charged once at key-creation time for the empty container's overhead —
  subsequent field/element inserts into an EXISTING key never update
  `used_memory`. This defeats the *global* `--maxmemory` gate identically
  (verified: growing one hash key's fields never trips a small
  `--maxmemory` cap regardless of value size), so it predates and is
  independent of db-quota. At the time this was out of scope to fix here
  (touches every container-type mutation site, a much larger body of work)
  and was documented as a known limitation; **now fixed** — see "WS6:
  container-growth memory accounting gap" above.
- `--db-maxmemory` CLI parsing now fails fast at startup (`REFUSING TO
  START:` + nonzero exit, matching this file's existing trusted-config
  validation convention) instead of silently warning and dropping a
  malformed/out-of-range entry — this is trusted operator config supplied
  at launch, not untrusted wire input, so a typo should refuse to start
  rather than silently leave a db unprotected. `CONFIG SET db-maxmemory`
  was already this strict; the CLI form now matches. New
  `config::validate_db_maxmemory_cli()`; new tests
  `test_malformed_db_maxmemory_cli_refuses_to_start` /
  `test_out_of_range_db_maxmemory_cli_refuses_to_start`.
- Documented (not changed): `WS DROP`'s all-dbs cascade-delete sweep is a
  synchronous, in-place O(total keys × `--databases`) scan on the owning
  shard's event-loop thread — it blocks every other connection pinned to
  that shard for the duration. Accepted trade-off for an admin-rare
  operation at the default `--databases 16`; flagged in
  `docs/guides/isolation.md` and inline code comments for large-keyspace,
  large-`--databases` deployments.

### Added — per-db resource quotas + workspace hardening sweep (WS5b, PR #TBD)

- New per-db memory quota: `--db-maxmemory <db>:<bytes>` (repeatable CLI
  flag) and `CONFIG SET/GET db-maxmemory <db> <bytes>` (0 = unlimited,
  default). Enforcement mirrors global `--maxmemory`: `noeviction`
  rejects writes at quota with a `MOONERR db maxmemory exceeded` error;
  eviction policies shed keys from the offending db only. Zero-cost
  when unconfigured via a single relaxed-atomic pre-gate
  (`DB_MAXMEMORY_ANY_SET`), same pattern as the existing global-maxmemory
  gate. New `src/storage/db_quota.rs`.
- Fixed a real bug found via this work: `SELECT`/`SWAPDB` are flagged
  `is_write` in moon's command-metadata table (for unrelated ACL/dispatch
  reasons), which meant a connection that filled a `noeviction`-quota'd db
  could not even `SELECT` away from it afterward — the eviction/quota gate
  ran against the pre-switch db index before `SELECT` updated connection
  state. Fixed for the new db-quota gate via
  `check_db_maxmemory_for_command()` (SELECT/SWAPDB exempt); the
  pre-existing identical quirk in the global `--maxmemory` gate is
  documented but deliberately left unfixed (out of scope — shared,
  widely-used eviction code).
- Fixed a real, pre-existing bug: `WS DROP`'s best-effort key cleanup
  only swept logical db 0 (hardcoded), silently leaking a workspace's
  keys forever if its connection ever `SELECT`ed to a non-zero db before
  writing (`WS AUTH` and `SELECT` are orthogonal connection state). Now
  sweeps every db on the owning shard. Proven via RED/GREEN TDD
  (`test_workspace_drop_cleans_keys_across_all_dbs`).
- New `docs/guides/isolation.md` — honest "isolation semantics" reference
  covering logical dbs, workspaces, and per-db quotas: what each
  guarantees, and every discovered limit (FLUSHDB is whole-db not
  workspace-scoped; MOVE/SWAPDB reconciled lazily by a background sweep,
  not synchronously; no disk-offload spill integration for db-quota
  eviction; FT.* indexes remain keyspace-global, not workspace-scoped —
  handoff note for the concurrent WS5a db-scoped-FT-index work).
- New tests: 9 unit tests in `src/config.rs` (CLI/CONFIG parsing), 7 in
  `src/storage/db_quota.rs` (accounting + enforcement), 6 in
  `src/command/config.rs` (CONFIG GET/SET), 3 new workspace hardening
  tests in `tests/workspace_integration.rs` (WS DROP all-dbs regression,
  cross-workspace KEYS non-leakage, FLUSHDB-is-whole-db pin), and a new
  `tests/db_maxmemory_quota.rs` real-server integration test.

### Fixed — WS5a round 4: write-path db-isolation leak in auto-index/auto-delete hooks (adversarial review)

A follow-up adversarial delta-review of round 3 (below) confirmed all of
round 3's fixes, then found one more **CRITICAL, data-integrity-severity**
gap: the auto-index/auto-unindex WRITE-path hooks were never db-scoped,
despite the round-2 CHANGELOG entry (further below) explicitly claiming
they were. Unlike the read-path leak fixed in round 3 (a foreign db could
merely *see* another db's search results), this gap let a foreign db
*corrupt or erase* another db's index contents — and it triggers at
`--shards 1` (no multi-shard fan-out required):

- **`auto_index_hset`** (`src/shard/spsc_handler.rs`, private; reached via
  `auto_index_hset_public`/`_public_txn` from `handler_single.rs`,
  `handler_monoio/mod.rs`, `handler_sharded/mod.rs`, `server/conn/shared.rs`,
  and the sharded MULTI/EXEC replay path) called UNSCOPED
  `vector_store.find_matching_index_names(key)` /
  `text_store.find_matching_index_names(key)`. An `HSET` issued from ANY db
  whose key happened to match another db's index PREFIX silently fed that
  foreign index. Fixed via `find_matching_index_names_for_db`, with
  `db_index: u8` threaded through both public wrappers and every call site
  (`conn.selected_db as u8` / `sel_db as u8` / `db_idx as u8`, matching the
  idiom already established for FLUSHDB scoping at each of those sites).
- **`auto_delete_vectors`** (same file) called UNSCOPED
  `vector_store.mark_deleted_for_key`, so a `DEL`/`UNLINK` issued from ANY
  db could tombstone another db's vector entry for a same-named key — the
  already-existing, already-unit-tested `mark_deleted_for_key_for_db` had
  **zero production callers** before this fix. Now threaded the same way.
- **`auto_hdel_vectors`** (bonus fix, same bug class, not explicitly named
  by the review but caught while auditing the surrounding code): also
  called unscoped `find_matching_index_names`; now uses
  `find_matching_index_names_for_db`.
- **The inline `DEL`/`UNLINK` unindex path** inside `ShardMessage::Execute`
  (`spsc_handler.rs` ~line 747) called unscoped `mark_deleted_for_key`
  directly (not through `auto_delete_vectors`) — fixed to
  `mark_deleted_for_key_for_db(key, db_idx)`.
- **Restart-time reconciliation** (`RecoveryState::reconcile_key` in
  `src/vector/persistence/recover_v2.rs`, called from `event_loop.rs`'s
  per-db rescan loop) had the SAME unscoped `find_matching_index_names`
  call and delegated to the unscoped `auto_index_hset_public`. Left
  unfixed, this would have silently RE-INTRODUCED the exact write-path leak
  on every server restart even after the live-write fix above — closed by
  threading `db_index: u8` through `reconcile_key`, sourced from
  `event_loop.rs`'s existing `for db_idx in 0..db_count` loop (the caller
  already had the right value; it just wasn't being passed in).
- **Checked and cleared**: active expiry (`expire_cycle`/`expire_cycle_direct`
  in `src/server/expiration.rs`, ticked per-db by `run_active_expiry` in
  `src/shard/timers.rs`) and hash-field lazy/active expiry do not call any
  vector/text unindex hook at all — `Database::remove` is a pure KV-layer
  operation with no vector_store/text_store hook. There is therefore no
  unscoped call to fix on the expiry path; this is a separate, pre-existing
  "TTL'd keys never get unindexed at all" gap (vectors/text entries for an
  expired key linger until an explicit DEL or HDEL), out of scope for a
  db-isolation fix and not new in this pass.
- New integration tests in `tests/vector_db_isolation.rs`:
  `write_path_auto_index_does_not_leak_across_db` (FT.CREATE in db 0, HSET
  of a prefix-matching key from db 5, assert db 0's `FT.INFO num_docs`
  unchanged and FT.SEARCH does not return the foreign doc) and
  `write_path_auto_delete_does_not_leak_across_db` (HSET in db 0, DEL of
  the same key name from db 5, assert the db-0 document remains
  searchable). Both verified RED against the unfixed hooks (real
  cross-db corruption reproduced with the exact documented symptoms),
  GREEN after the fix. Note: `FT.INFO num_docs` is `mutable_segment.len()`
  (append-only; a tombstone doesn't decrement it), so the DEL-side test
  asserts on `FT.SEARCH` hit count instead — an early draft of that
  assertion only checked "not Nil", which a foreign-db tombstone's
  empty-but-OK `[0]` result also satisfies, and silently passed against
  the unfixed code; corrected to check for a real (`> 0`) hit count.

### Fixed — WS5a round 3: multi-shard text-search db-isolation leak + hardening (adversarial review)

A fix-first adversarial review of round 2 (below) found the headline
"index created in db N is invisible from every other db" promise still
had a **critical multi-shard gap**: `scatter_text_search` (the plain-text/
BM25 FT.SEARCH scatter path), plus the remote legs of `scatter_hybrid_search`
and `scatter_text_aggregate`, took no `db_index` at all — on `--shards >= 2`
a connection on any db got real results from another db's TEXT index. Round
2's own suite only spawned single-shard servers, so the bug was invisible
to CI (single-shard scatter degenerates to the already-scoped local path).

- **`scatter_text_search`** (`src/shard/coordinator.rs`) now threads
  `db_index` through its `num_shards == 1` fast path AND its true
  multi-shard DFS fan-out (both the local DocFreq/TextSearch legs and the
  remote `DocFreq`/`TextSearch` SPSC legs), via new `db_index: u8` fields
  on `TextSearchPayload`, `InvertedSearchPayload`, and a newly-boxed
  `DocFreqPayload` (boxing was required to keep `ShardMessage` within its
  64-byte cache-line cap after adding the field to the previously-inline
  `DocFreq` variant). The dead `scatter_text_search_filter` was scoped the
  same way rather than left as a latent unscoped copy.
- **`scatter_hybrid_search`'s remote leg** and **`scatter_text_aggregate`'s
  remote leg** — previously documented below as an open residual gap — are
  now also `db_index`-scoped (`FtHybridPayload`/`TextAggregatePayload` gain
  the field; `execute_hybrid_search_local_raw_streams` and
  `execute_local_partial` take it as a parameter). **The "known gap" note
  in the round-2 entry below is superseded: both legs are closed.**
- **`VACUUM VECTOR`** (`src/command/server_admin.rs::vacuum_vector`) was an
  unscoped existence oracle — any db could merge/compact/probe another
  db's vector index by name. Now takes `db_index` and resolves ownership
  via `get_index_for_db`/`get_index_mut_for_db` before any mutation or
  existence check; the subsequent unscoped `needs_merge`/
  `immutable_segment_count`/`force_merge_index` calls are safe unchanged
  (index names are globally unique per shard, so an ownership check by
  name is sufficient once performed).
- **`--databases` is now bounded to 256** (`ServerConfig::MAX_DATABASES`,
  `validate_databases_bound()`, checked unconditionally at both normal
  boot and `--check-config`). Vector/text index db-scoping uses a `u8`
  tag; an unbounded `--databases` (e.g. 300) silently aliased `SELECT 256`
  onto db 0's indexes. Deliberately NOT widened to `u16` — 256 dbs is
  already far beyond Redis's default of 16.
- **New multi-shard coverage** in `tests/vector_db_isolation.rs`
  (`--shards 4`): plain-text FT.SEARCH cross-db invisibility (the exact
  finding-1 scenario), KNN cross-db invisibility, and FT._LIST scoping,
  all under real multi-shard fan-out. Verified RED (real cross-db hits
  leaked) against the unfixed `scatter_text_search`, GREEN after the fix.
- Stale comments in `handler_monoio/ft.rs` / `handler_sharded/ft.rs` /
  `coordinator.rs`'s test module claiming `execute_text_search_local` is
  the multi-shard path were corrected — that function is dead code outside
  its own unit tests; the real multi-shard path is `scatter_text_search`
  → `run_text_query_on_index`.

### Fixed — WS5a: db-scoped vector/text index isolation (headline bug closed)

Round 1 shipped only the data model (see below) — FT.CREATE still tagged
every index `db_index: 0`, so no read-path scoping had any observable
effect outside db 0. Round 2 closes the headline promise: **an index
created in db N is invisible from every other db, across all three
dispatch paths (`handler_single`, `handler_sharded`/tokio,
`handler_monoio`), including the multi-shard scatter/broadcast legs.**

- **`IndexMeta`/`TextIndex`** gain a `db_index: u8` tag (vector sidecar
  format bumped to v4, text sidecar to v2; legacy sidecars default to db 0,
  fully backward compatible).
- **FT.CREATE now tags the connection's actual SELECTed db** instead of a
  hardcoded 0. Naming decision: index **names stay globally unique per
  shard** (not a composite `(db, name)` key) — each index is bound to
  exactly one db; `FT.CREATE` of a name that already exists in ANY db
  (including the same db) errors `"Index already exists"`, unchanged from
  pre-WS5a behavior. This avoids migrating the `HashMap<Bytes, _>` key
  type across ~150+ call sites for a benefit (same name reusable per db)
  nobody asked for; an operator wanting per-db name reuse renames (e.g.
  `idx_db0`, `idx_db1`).
- **Full read-path scoping**: FT.SEARCH, FT.INFO, FT._LIST, FT.DROPINDEX,
  FT.COMPACT, FT.CONFIG (GET/SET), FT.AGGREGATE, FT.CACHESEARCH,
  FT.RECOMMEND, FT.NAVIGATE, FT.INVALIDATE_RANGE, and hybrid search all
  resolve indexes scoped to the caller's current db across every dispatch
  path — ~45 call sites migrated from `get_index`/`get_index_mut` to
  `get_index_for_db`/`get_index_mut_for_db` (or the equivalent scoped
  helper), no new hot-path allocations (same `O(n)` filter shape as the
  unscoped originals).
- **Write-path scoping (claim corrected by round 4 below): this entry
  originally claimed `auto_index_hset` and the DEL/HDEL auto-unindex hooks
  only touched indexes bound to the write's db via
  `mark_deleted_for_key_for_db`. That claim was FALSE — the round-4
  adversarial review found `auto_index_hset`, `auto_delete_vectors`, and
  `auto_hdel_vectors` all called their UNSCOPED counterparts
  (`find_matching_index_names`/`mark_deleted_for_key`), and
  `mark_deleted_for_key_for_db` had zero production callers at the time
  this was written. See the "WS5a round 4" entry above for the actual fix.**
- **Cross-shard scatter/broadcast fully threaded**: `ShardMessage::VectorCommand`
  and `VectorSearchPayload` now carry `db_index` across the SPSC boundary;
  `broadcast_vector_command`, `scatter_ft_info`, `scatter_invalidate_range`,
  `scatter_vector_search`/`_remote` all honor it. `scatter_hybrid_search`
  and `scatter_text_aggregate`'s multi-shard DFS legs honor it on the
  `num_shards == 1` fast path; the true multi-shard remote leg of hybrid
  search and FT.AGGREGATE's `execute_local_partial`, **and the
  plain-text/BM25 `scatter_text_search` scatter path itself, were left
  fully unscoped** — a critical gap an adversarial review caught (single-
  shard-only test coverage hid it). **Closed in the WS5a round 3 entry
  above** — do not treat this note as still-open.
- **Bonus fix found in the same code path**: `scatter_text_aggregate`'s
  single-shard fast path always hardcoded `s.databases.first()` (db 0)
  for `@field` value materialization regardless of the caller's SELECTed
  db — fixed alongside the db_index threading (was a pre-existing bug,
  not a WS5a regression).
- **Known gaps, explicitly NOT implemented this pass** (see
  `.planning/v0.6.0-release/WS5A-NOTES.md` for the full design rationale):
  - **SWAPDB** does not retag index ownership. `SWAPDB a b` swaps the KV
    keyspace but leaves every index's `db_index` tag untouched — an index
    created in db `a` stays visible only from db `a` even after its data
    moves to db `b`. Target design (swap `db_index` on every index owned
    by either swapped db) is documented but not coded.
  - **MOVE/COPY** of an indexed hash does not re-index the key into the
    target db's matching index — operator must re-HSET in the target db.
  - **Graph engine** (`src/graph/store.rs`) is a single per-shard
    `GraphStore`, structurally global across all 16 logical dbs — same
    shape vector/text stores had before this fix, but NOT scoped in this
    pass (see "Known Limitations" below). Timeboxed analysis confirmed no
    per-db concept exists anywhere in the graph engine (no `db_index`, no
    per-db FLUSHDB differentiation); scoping it would require the same
    class of work done here for vector/text, sized as its own follow-up.
- Legacy (pre-v0.6.0) persisted sidecars continue to load with
  `db_index` defaulting to 0 (unit-tested, unchanged from round 1).
- New integration suite `tests/vector_db_isolation.rs` (real spawned
  server, real TCP client): cross-db invisibility both directions,
  FT._LIST db scoping, FT.CREATE cross-db name-collision error, and a
  true process-restart round-trip proving a db-1-scoped index reloads
  still bound to db 1.

### Fixed — WS5a: FLUSHDB no longer clears vector/text index contents across dbs (foundation)

- **`IndexMeta`/`TextIndex`** gain a `db_index: u8` tag (vector sidecar
  format bumped to v4, text sidecar to v2; legacy sidecars default to db 0,
  fully backward compatible).
- **FLUSHDB now scopes** to the connection's selected db —
  `VectorStore::clear_all_contents_for_db` /
  `TextStore::clear_all_contents_for_db`, wired through all 3 dispatch
  paths (`handler_single`, `handler_sharded`, `handler_monoio`) plus the
  cross-shard MULTI/EXEC replay path. FLUSHALL is unchanged (still clears
  every db). Previously FLUSHDB in ANY db cleared ALL index contents
  keyspace-wide — direct fix, covered by a new integration test
  (`tests/vector_flush_hdel_tombstone.rs`).
- Db-scoped lookup/listing/delete primitives (`get_index_for_db`,
  `index_names_for_db`, `find_matching_index_names_for_db`,
  `mark_deleted_for_key_for_db`, `drop_index_for_db`) added alongside the
  existing unscoped methods on both `VectorStore` and `TextStore`, unit
  tested.

### Docs — tuning guide: vector bulk load & compaction (PR #TBD)

- `docs/guides/tuning.md`: new "Vector bulk load and compaction" section — documents
  immediate-search-then-background-HNSW-build behavior, the `COMPACT_THRESHOLD`
  time-to-serve vs recall lever, the `MOON_VEC_COMPACT_WORKERS` pool knob, the
  ~10K-vector parallel-build threshold, per-shard bulk-load parallelism, and the
  ingest-rate/recall trade-offs. Operator guidance for the parallel-build +
  insert-path-compaction work shipped in #237.
- Removes a stray `<<<<<<< HEAD` conflict marker accidentally left in the
  `[Unreleased]` heading by #237's squash merge.

### Added — parallel HNSW build + insert-path compaction trigger: time-to-index-green 11× (PR #237)

- **`src/vector/hnsw/parallel_build.rs`** (new): concurrent HNSW
  construction into one shared graph — per-node `parking_lot::Mutex`
  adjacency (copy-under-lock, distance math unlocked; single lock held at a
  time, deadlock-free by construction), entry point under a `RwLock`,
  levels pre-generated from the same seeded LCG as the sequential builder,
  sequential 1K warmup, then dynamic fan-out over an atomic cursor.
  Finalize adds a connectivity repair pass (concurrent back-link pruning
  orphaned ~0.2% of nodes = permanent recall loss; BFS + force-link makes
  every node reachable, unit-asserted) and the exact same BFS-reorder →
  `HnswGraph` path as the sequential builder — drop-in for search,
  persistence, and GraphUnion merge. `compact()` routes builds ≥ 10K
  vectors here; smaller segments keep the bitwise-deterministic
  single-threaded builder. Measured (50K × 384d, 6-core Linux VM):
  `FT.COMPACT` wall 30.2s → **2.71s**; a 24K segment build 14.1s →
  **2.65s** (~88% scaling efficiency).
- **Affinity-mask trap fixed (3 sites)**: shard threads are core-pinned and
  every thread spawned from one inherits the SINGLE-core mask on Linux, so
  `std::thread::available_parallelism()` returned 1 — the "parallel" build
  ran at exactly sequential speed and the background-compactor pool sized
  itself to one worker. New `shard::numa::system_parallelism()` (sysfs
  online-CPU count, affinity-independent, cached) now sizes both; parallel
  build workers and pool workers explicitly re-pin round-robin across the
  machine (`pin_worker_to_core`), pool workers from the last core downward
  so small builds stop time-slicing shard 0's core.
- **`src/shard/spsc_handler.rs`**: HSET auto-index hook now calls
  `try_compact()` after appending a vector — a pure bulk load (no
  FT.SEARCH traffic) previously left everything in the brute-force mutable
  tier until the autovacuum backstop's 30s tick, so the whole HNSW build
  landed on the first explicit `FT.COMPACT`. Builds now start and install
  DURING ingest (in-flight guard unchanged; respects
  `FT.CONFIG AUTOCOMPACT OFF`). Red/green: new
  `test_insert_path_triggers_background_compact_without_search`.
- Net effect on the §10.8 losing metric (bulk load 50K × 384d → HNSW-tier
  serving, VM): ~30s of post-ingest compaction becomes ~0 (already
  compacted by measure time) — Moon's time-to-green now beats Qdrant's
  optimizer on the same box (GCE re-validation pending). Multi-segment
  recall@10 0.9986 vs 0.9992 single-segment (−0.0006, within the ≥0.99
  gate); multi-shard verified (`--shards 4`: per-shard triggers, 9
  segments, FT.COMPACT 1.57s).

### Docs — tuning guide: durability + pub/sub enhancements (PR #TBD)

- `docs/guides/tuning.md`: rewrote the "Persistence: what durability costs"
  section to match the write-path campaign — `everysec` is now a win at
  pipeline depth (~1.32× Redis), `always` is disk-fsync-bound (parity floor)
  and safe to pipeline via group commit (P16 0.12×→0.91×), with a
  "which policy?" decision line. All framed as automatic behavior (no knobs).
- New "Pub/sub fan-out" section: coalesced delivery (5.09M msg/s, zero drops)
  and the intentional slow-subscriber drop policy (256-msg queue cap).
- Quick-recipe table: refreshed the durable-store row and added a pub/sub row.
  Cross-links to `BENCHMARK.md` §7.3.

### Docs — durability write-path benchmark results (PR #TBD)

- `BENCHMARK.md` §7.3: new section recording the 2026-07-08 durability
  write-path campaign (PRs #238–#242) — the vs-Redis matrix (`always` P16
  0.12×→0.91×, `everysec` P16 1.32× win, `everysec` P1 0.80×→0.99× parity,
  pub/sub fan-out 438→5.09M msg/s), per-PR root-cause table (strace
  diagnostics), durability-invariant note, and A/B reproduction steps.
  Executive summary + header updated.
- `docs/production-guide.md`, `docs/benchmarks.md`, `docs/configuration.md`:
  AOF `appendfsync` tuning guidance updated with the measured ratios and a
  plain-language explanation of group commit / coalesced writes / park-free
  writer poll (all automatic; durability unchanged).

### Fixed — Windows build: `accept_backoff` used unix-only `libc` errnos (PR #TBD)

- `is_resource_exhaustion` (accept-loop backoff, PR #230) referenced
  `libc::EMFILE`/`ENFILE`/`ENOBUFS`/`ENOMEM` unguarded; `libc` is not linked
  on Windows, breaking the main-push Windows check (PRs never caught it —
  Windows CI is skipped on PRs). Now cfg-split: unix keeps the errno match,
  Windows matches WSAEMFILE (10024) / WSAENOBUFS (10055) /
  `ErrorKind::OutOfMemory`. Follow-up: the module's unit test
  (`resource_exhaustion_classification`) also referenced `libc` errnos —
  now cfg-split the same way (unix errnos vs WSA codes).

### Changed — AOF writer coalesces each group-commit batch into one write (PR #TBD)

- The two monoio AOF writer paths (TopLevel via `commit_group_commit_batch`,
  PerShard framed loop) wrote each record with its own `write(2)` on the raw
  unbuffered `File` (the PerShard path even used a header+body pair). strace
  during always-P16 SET: **127,812 write calls / 1.25 s** of an 8 s window vs
  2,144 fdatasyncs / 0.2 s — the writer thread was write-syscall-bound, not
  fsync-bound, while Redis batches ~120 records per write via `aof_buf`. Each
  batch's records now coalesce into one contiguous buffer (reusable in the
  PerShard loop, capped at 1 MB high-water) and are written with ONE
  `write_all` before the single per-batch fsync. Single-message batches keep
  the zero-copy direct write. Failure semantics unchanged: a failed batch
  write acks every waiter `WriteFailed` and engages the torn-stream latch.
- tokio writer loops already amortize via `BufWriter` — untouched.
- `wal_group_commit` sink tests updated to the coalesced contract (one write,
  channel-order bytes, fsync after it).

### Changed — AOF writer polls park-free under everysec/no (PR #TBD)

- The two std-thread AOF writer loops (monoio TopLevel + PerShard) no longer
  park in `recv_timeout` under `appendfsync everysec`/`no`. A parked receiver
  makes every producer `try_send` pay a futex WAKE **on the shard thread** —
  measured at ~150k futex calls (63% of shard-thread syscall time) during an
  8s non-pipelined SET run, the mechanism behind Moon's everysec P1 SET
  deficit vs Redis (whose AOF append is a plain memcpy). The writer now polls
  `try_recv` with adaptive sleeps (wait/16, clamped 500µs–50ms) so producer
  sends stay pure userspace atomics; post-fix the same run shows 96 futex
  calls. `appendfsync always` keeps the parked recv (its callers await the
  per-batch fsync ack, so receive latency is client-visible RTT).
- EverySec durability bound unchanged: at the 50ms fast floor the deadline is
  re-checked within ~3ms slack; idle cost is ≤20 writer wakes/s at the 1s
  escalated wait (vs 1/s parked — still far below the pre-wave-5 fixed 50ms
  cadence).

### Changed — Pub/sub subscriber delivery coalesces message bursts (PR #TBD)

- All three connection handlers' subscriber delivery arms (`handler_monoio`,
  `handler_sharded`'s `run_subscriber_step`, `handler_single`) now drain the
  subscriber queue (`try_recv`, capped at 64 KB) and deliver the burst with
  ONE `write_all` instead of one write syscall per message — the same batched
  write pattern the command path already uses. Raises the fan-out delivery
  ceiling and shortens the window in which a slow subscriber's bounded queue
  (256) overflows and drops messages. The single-message case keeps the
  zero-copy path (`is_empty` fast path, no allocation; `handler_sharded`
  reuses the connection's `write_buf`).
- New wire-level integration test `tests/pubsub_burst_delivery.rs`: a
  pipelined publish burst must arrive complete, frame-boundary-intact, and in
  order at 1 and 4 shards (both runtimes).

### Changed — `appendfsync always`: local writes now group-commit per pipeline batch (PR #TBD)

- **All three connection handlers** (`handler_monoio`, `handler_sharded`,
  `handler_single`): plain local writes (plus MOVE/COPY and single-shard
  GRAPH.* WAL records) no longer await one fsync ack **per command** under
  `appendfsync always`. Appends are enqueued fire-and-forget
  (`send_append_group`) and the whole pipelined batch is confirmed by ONE
  `fsync_barrier` before response serialization — the same contract
  cross-shard writes and coordinator local legs already used (PR #213).
  The writer processes its channel in order, so an acked barrier proves
  every prior append durable; the fsync-before-ack H1 guarantee is
  unchanged (a failed barrier converts every joined response to an error,
  never a silent `+OK`).
- **Why**: a 16-deep pipeline paid 16 serialized fsync round-trips per
  connection while Redis fsyncs once per event-loop iteration — measured
  8× SET-throughput deficit at P16 (Moon 5.2k vs Redis 44k ops/s, GCE
  c3-standard-8, tmp/MOON-VS-REDIS-DURABILITY.md). Writer-side group
  commit existed but could only batch *across* connections; the per-command
  await defeated it *within* a connection.
- everysec/no policies are unchanged (`send_append_group` degrades to the
  same bounded-backpressure enqueue).
- `flush_with_aof_ack`'s discriminating H1 ordering test updated to the
  batch protocol (one `Append` + one `AppendSync` barrier; ack still gates
  the first response).

### Changed — WAL v3 fsync moved off the shard event loop (PR #TBD)

- **`src/persistence/wal_v3/sync_agent.rs` (new)**: per-shard `WalSyncAgent`
  thread receives fd-dup'd sync requests over a bounded flume channel and
  publishes a monotonic durable-LSN watermark after each `fdatasync`.
  `WalWriterV3` gains `request_sync()` (non-blocking initiation; queue-full
  falls back to inline fsync — a durability request is never dropped) and
  `wait_durable(lsn, timeout)` (bounded blocking wait, used only by the two
  checkpoint ordering invariants and shutdown). An fsync error poisons the
  agent permanently and fails subsequent syncs loudly; the checkpoint then
  refuses to advance `redo_lsn`.
- **Why**: `flush_sync()` ran `fdatasync` on the shard event-loop thread.
  Measured on GCE pd (tmp/WALV3-OFFLOOP-FSYNC.md): the everysec 1s timer
  froze every connection on the shard for **10–16 ms once per second** when
  the WAL held real bytes, and `appendfsync always` paid −20% RPS / ~2× tail
  on top of the AOF cost.
- **Call sites**: everysec timer (`timers::sync_wal_v3`) and the always-mode
  per-drain-batch sync (both runtimes) now use `request_sync()`; the
  checkpoint log-before-data page gate and WAL-before-manifest finalize use
  `wait_durable` (5s bound); shutdown paths keep the inline `flush_sync()`.
  everysec semantics are now "sync initiated every 1s, durable typically ms
  later" — the same window the AOF everysec writers provide.
- Loom model for the watermark/poison state machine in
  `tests/loom_wal_sync_agent.rs`.

### Changed — consolidated dependency bumps wave 2 (PR #TBD, supersedes dependabot #223–227)

- Patch-level bumps rolled into one `Cargo.lock` update (no `Cargo.toml`
  edits — all within existing ranges): `bumpalo` 3.20.2→3.20.3,
  `uuid` 1.23.1→1.23.4, `smallvec` 1.15.1→1.15.2,
  `unicode-segmentation` 1.13.2→1.13.3, and the crypto-tls group
  `rustls` 0.23.40→0.23.41 + `aws-lc-rs` 1.17.0→1.17.1
  (pulls `aws-lc-sys` 0.41.0→0.42.0).
- **Perf side-effect verified** by same-instance p=1 KV A/B (deps vs main)
  on GCE `c3-standard-4` (x86) + `c4a-standard-4` (ARM), shard=1, spin={0,40},
  clients={1,8}, best-of-5, steal=0%. All single-op cells land 0.974–1.008
  (`new_vs_base`) on both arches — no regression from `smallvec` on the
  dispatch/protocol hot path, nor from the `aws-lc-sys` 0.42 codegen. See
  `tmp/DEPS-WAVE2-AB.md`.
- **CI-only GitHub Actions bumps** folded in (no runtime effect):
  `actions/setup-node` 4→6 (release.yml), `actions/setup-python` 5→6
  (docs.yml), `taiki-e/install-action` 2.81.10→2.82.9 (fuzz.yml) —
  supersedes dependabot #220/#221/#222.

### Changed — BREAKING: WAL v3 is now the only WAL; XactCommit unified to the db-aware record (pre-1.0 format freeze)

Moon is pre-release: this drops on-disk format compatibility that no shipped
release depended on. There is no migration path — operators upgrading a node
that still has WAL v2 files (`shard-N.wal`) or pre-freeze XactCommit(0x51)
records on disk must let those age out via the AOF-authoritative recovery
path (below), or replay them on a pre-freeze build first and re-persist.

- **`src/persistence/wal_v3/record.rs`**: deleted the pre-1.0 `XactCommit`
  (v1, tag `0x51`) record type, its encoder, and its replay arm. Renamed
  `XactCommitV2` -> `XactCommit`, **keeping discriminant `0x53`** (`0x51` is
  retired, not reused — a stray `0x51` record in a v3 stream now decodes as
  an unrecognized tag rather than being silently reinterpreted). All
  producers (the TXN.COMMIT WAL record in `handler_sharded`/`handler_monoio`)
  already wrote only the v2 (now unified) record, so this is a pure
  simplification of the read side.
- **`src/persistence/wal.rs` deleted** — WAL v2 (`WalWriter`, `replay_wal`,
  `wal_path`) is removed in its entirety. WAL v3 (`WalWriterV3`,
  `src/persistence/wal_v3/`) is the only WAL, and is now also created in the
  old v2 niche (`--appendonly yes` with `--disk-offload disable`), rooted at
  `<persistence_dir>/shard-N/wal-v3/` instead of the old flat
  `<persistence_dir>/shard-N.wal` file — matching the disk-offload layout.
  This closes a latent gap: `--appendfsync always` previously only fsynced
  WAL v3 (a no-op in non-offload mode, since only v2 existed there); it now
  fsyncs the WAL unconditionally.
- **`replay_wal_auto`** rejects a v2-format WAL file (`RRDWAL` magic +
  version byte `2`) with a loud `WalError::UnsupportedVersion` instead of
  delegating to the deleted v2 replay path.
- **Recovery**: the `shard-N.wal` (v2) fallback rung is removed from both
  `persistence::recovery::recover_shard_v3_pitr` (disk-offload path) and
  `Shard::restore_from_persistence_v2` (legacy path) — `appendonly.aof`
  remains the recovery authority and is unaffected. `shared_databases::
  replay_graph_wal` (graph-feature boot-time WAL scan) is ported from the v2
  flat file to the v3 segment directory (`shard-N/wal-v3/*.wal`), matching
  `replay_workspace_wal`/`replay_temporal_wal`. Additionally, both legacy-dir
  recovery paths gain a **last-resort WAL v3 fallback**: when NO
  `appendonly.aof` exists, the legacy-mode `shard-N/wal-v3/` directory is
  replayed (with a loud partial-coverage warning — WAL v3's KV coverage is
  intentionally partial post-#211, so it never shadows a present AOF), and a
  leftover legacy `shard-N.wal` (v2) file on disk now triggers a
  `tracing::error!` naming the file instead of being silently ignored.
- **`wal_append_and_fanout` / `wal_fanout_has_work`** (`src/shard/
  spsc_handler.rs`) drop the v2 `wal_writer` parameter and the "v3
  supersedes v2" branch — a single `Option<WalWriterV3>` parameter, renamed
  `wal_writer`. `finalize_snapshot_success` no longer takes a WAL writer:
  v2's per-snapshot `truncate_after_snapshot(epoch)` had no v3 analogue — v3
  retention is LSN-driven (`WalWriterV3::recycle_aggressive` /
  `recycle_segments_before`, run from autovacuum Pass C and the checkpoint
  protocol) and already ran independently of legacy snapshot epochs, so
  nothing was lost by not porting it over.
- Deferred (unrelated to this change, found in passing): `shared_databases::
  replay_temporal_wal` scans `shard-N/` directly for `*.wal` files instead of
  `shard-N/wal-v3/` like its siblings — looks like a pre-existing directory
  mismatch, left as-is pending its own investigation.
### Fixed — CLIENT TRACKING invalidation actually works on sharded servers (PR #TBD)

- **`src/tracking/`, all three handler stacks**: RESP3 client-side-caching
  invalidation was non-functional outside the single-shard handler via five
  stacked defects: (1) sharded handlers registered the per-connection
  invalidation channel but never drained it — pushes went into a channel
  nobody read; (2) invalidation was gated on the WRITER's own tracking state
  (backwards — a non-tracking writer never invalidated anyone); (3) only the
  first key of a multi-key write was invalidated (`DEL k1 k2` left `k2`
  stale); (4) the tracking table was per-shard, so a reader and writer
  accepted on different shards never saw each other; (5) FLUSHALL/FLUSHDB
  never invalidated. The table is now process-global (`OnceLock`, gated by a
  relaxed `ACTIVE_TRACKERS` atomic so the hot path pays one load when
  tracking is unused), keys are extracted via the command-metadata key
  specs, and all write paths (local, cross-shard remote leg, multi-key
  coordinator, monoio inline fast-path via a tracking-aware gate, FLUSH) hook
  invalidation. Red/green: `tests/client_tracking_invalidation.rs` (4 tests,
  raw RESP3 client) fails 0/4 on the previous binary.

### Changed — pub/sub publish fan-out moved outside the registry lock (P1, PR #TBD)

- **`src/pubsub/mod.rs`** (`publish_shared`): PUBLISH previously held the
  per-shard registry **write** lock for the whole O(N) subscriber fan-out —
  and the SPSC drain held it across the entire drain cycle. High fan-out
  (10K cache clients on one invalidation channel) stalled every concurrent
  (UN)SUBSCRIBE and the shard's cross-shard message drain. The new 3-phase
  path snapshots matching subscribers under a brief read lock, serializes +
  `try_send`s lock-free, and takes the write lock only to remove slow
  subscribers actually hit. Handler PUBLISH sites (both runtimes), the SPSC
  `PubSubPublish`/`PubSubPublishBatch` arms, and the MQ trigger-notification
  loop all switched. Documented at-most-once trade (matches Redis).

### Fixed — PUBLISH inside MULTI executed immediately instead of queueing (C2, PR #TBD)

- **All three handler stacks**: the PUBLISH arm ran before the `in_multi`
  queue check, so `MULTI; SET k v; PUBLISH ch m; EXEC` fanned the message out
  at queue time — subscribers received it before the transaction's writes
  applied (even on DISCARD). PUBLISH now queues like any other command; both
  transaction executors intercept it and the handler fans it out after the
  transaction body, patching the reply placeholder with the real receiver
  count (remote shards awaited via targeted `PubSubPublish`). Discovered but
  NOT fixed (pre-existing, tracked as follow-up): sharded MULTI/EXEC executes
  the queued body on the connection's local shard regardless of key
  ownership — at shards=4 most txn-written keys are silently invisible to
  other connections; the new test documents this and runs on `--shards 1`.

### Added — pub/sub ↔ KV ordering guarantee documented + locked in (C1/C2, PR #TBD)

- **`src/pubsub/mod.rs`** module docs now state the contract: same-connection
  write→publish ordering IS guaranteed (a received message implies every
  preceding write on the publisher's connection is visible), cross-connection
  ordering is NOT, delivery is at-most-once. `tests/pubsub_kv_ordering.rs`
  pins both the pipelined `SET;PUBLISH` case and `MULTI{SET,PUBLISH}EXEC`
  across a 4-shard server. (P2 of the review — event-loop/scheduler coupling
  benchmark — deferred to a Linux box; macOS numbers are not decision-grade.)

### Security — channel ACL enforced on the transactional PUBLISH path (C2 follow-up, PR #TBD)

- **`src/server/conn/{shared,handler_single,handler_sharded/mod,handler_monoio/mod}.rs`**:
  the C2 ordering fix queues `PUBLISH` inside `MULTI` and fans it out after the
  transaction body, but the fan-out skipped the per-channel ACL check that the
  immediate path enforces — so a client denied a channel could wrap `PUBLISH`
  in `MULTI/EXEC` to reach it. A shared `publish_channel_acl_deny` helper now
  gates every fan-out site (all three handlers); a denied channel is patched
  into the EXEC reply slot as `NOPERM` and never delivered. The check runs at
  fan-out time because Moon has no queue-time EXECABORT machinery. Also closes a
  pre-existing gap where the single-handler *immediate* PUBLISH never checked
  channel ACLs at all (the sharded/monoio immediate paths already did).
  Red/green: `tests/pubsub_multi_channel_acl.rs` (2 tests, 1- and 4-shard) —
  a restricted user's `MULTI; PUBLISH denied m; EXEC` returned `:0` on the old
  binary, `NOPERM` on the fixed one.

### Fixed — CLIENT TRACKING invalidation + cleanup edge cases (review follow-up, PR #TBD)

- **EXEC now invalidates tracked keys**: `SET`/`DEL`/`MSET` applied inside
  `MULTI/EXEC` bypassed the post-write invalidation hook, leaving RESP3
  client-side caches stale until the next non-txn write. All three handlers now
  run `invalidate_after_write` for each successful queued write (self-gated on
  `tracking_active()`, so no cost when tracking is unused).
- **monoio disconnect releases tracking registration**: a client that
  disconnected without `CLIENT TRACKING OFF` left `ACTIVE_TRACKERS` nonzero and
  kept `tracking_active()` hot for the rest of the process (the single/sharded
  handlers already cleaned up on close). The monoio disconnect path now calls
  `untrack_all`, gated on `tracking_active()` to keep the common no-tracking
  close lock-free.

### Fixed — TopLevel-monoio AOF writer: EverySec fsync deferred indefinitely when idle (PR #TBD)

- **`src/persistence/aof/writer_task.rs`**: the TopLevel monoio AOF writer
  blocked on an **untimed** `rx.recv()`, with its EverySec deadline check
  living only inside the batch-commit path. A batch written under
  `appendfsync everysec` gets no per-batch fsync, so if the client stopped
  writing right after a burst, the buffered bytes only became durable when
  the NEXT message happened to arrive — the 1s fsync bound was deferred
  indefinitely while idle. Exposure is host-crash-only (the per-batch
  `flush()` already reaches the kernel page cache, so a plain process kill
  loses nothing), which is exactly the window EverySec exists to bound.
  The loop now mirrors the audited PerShard writers: bounded
  `recv_timeout` on the wave-5 `IdleWait` ladder (50ms → 250ms → 1s,
  pinned at the floor while a batch awaits its fsync) plus an end-of-loop
  proactive fsync that runs on message AND timeout iterations. This
  supersedes wave 5's "TopLevel monoio needs none of this" note — it was
  the one writer loop left without the ≤ ~1s idle durability bound.
- **`tests/crash_matrix_per_shard_aof.rs` harness hardening** (found while
  validating the above): (1) `redis_set` asserted only redis-cli's exit
  status, which is 0 even for server ERROR replies — a tripped diskfull
  guard (host <5% free) turned every SET into a silent no-op and surfaced
  as a bogus "200 keys missing after recovery"; the helper now pins the
  reply to `+OK`. (2) The three server-spawning tests split-brain when run
  in parallel: `unique_port()` hands out OS-sequential ephemeral ports, the
  other tests offset +1/+2, and SO_REUSEPORT lets two tests bind the SAME
  port without an error — one test's redis-cli traffic lands on another
  test's server (rotating total-loss false alarms). A shared mutex now
  serializes them.

### Fixed — RSS/CPU remediation wave 5 (PR #TBD)

- **Item A — mmap the exact-rerank f16 sidecar on segment reload**
  (`src/vector/segment/raw_f16_store.rs`, new `RawF16Store` enum): a segment
  reloaded from disk used to `fs::read` the entire `raw_f16.bin` sidecar into
  a second heap `Vec<u16>`, doubling resident vector memory for
  reload-heavy deployments (warm starts, segment promotion). Reload now
  memory-maps the file (`memmap2`, already a workspace dependency) and hands
  out a zero-copy `&[u16]` view backed by the kernel page cache — RSS only
  grows for pages the rerank path actually touches. Freshly-built segments
  (compaction/merge) are unaffected — they keep their owned buffer. Rerank
  parity (Owned vs Mapped, byte-identical sidecar + identical `search()`
  output) is pinned by
  `test_reload_raw_f16_sidecar_uses_mmap_and_matches_owned_rerank`.
- **Item A follow-up — text posting-list capacity reclaim**
  (`src/text/posting.rs`): `PostingList::term_freqs`/`positions` grow to the
  peak document count ever seen for a term and, per the existing
  `remove_doc` contract, the `postings` HashMap entry is kept forever even
  once a term has zero live documents. The buffers now `shrink_to_fit()`
  once the last document leaves a posting, releasing peak capacity for
  terms that go idle without changing the "entry survives" contract.
- **Item B — AOF writer idle wake made adaptive** (`src/persistence/aof/writer_task.rs`,
  new `IdleWait` state machine): the 3 steady-state writer loops that need a
  bounded channel poll to service the EverySec proactive-fsync deadline
  (TopLevel tokio, PerShard tokio, PerShard monoio — TopLevel monoio blocks
  on an untimed `rx.recv()` and needed no change) used to poll at a FIXED
  cadence forever (50ms monoio / 200ms tokio), waking an idle server's AOF
  writer thread 5-20 times a second doing nothing. The wait now escalates
  50ms → 250ms → 1s once a poll times out with nothing queued, and resets to
  the floor the instant any message arrives — a real write always wakes the
  loop immediately regardless of the current timeout, since the poll races
  a message against the deadline. Escalation is refused (pinned at the
  floor) whenever a write is buffered under `FsyncPolicy::EverySec` without
  an immediate fsync, or `last_fsync` was manually back-dated (the F6
  post-fold drain trick) — the ~1.2s EverySec bound is provably unchanged.
  `FsyncPolicy::Always`/`No` have no such deadline and escalate freely once
  idle.
- **Item C1 — WAL v3 write buffer shrinks after an oversized flush**
  (`src/persistence/wal_v3/segment.rs`): a single large record (e.g. a
  FullPageImage) grew the 8KB write buffer to fit it, and `clear()` alone
  never released that capacity — the peak allocation was pinned for the
  writer's lifetime. `flush_write`/`rotate_segment` now `shrink_to` the
  8KB default once capacity exceeds 4x that, a no-op for the common
  small-record case.
- **Item C2 — SearchScratch visited-set: already bitset-based (SKIP)**
  (`src/vector/hnsw/search.rs`): the per-query search hot path already uses
  a word-based `BitVec` (u64 words, `test_and_set`/`clear_all` memset),
  thread-cached and reused across queries — no change needed. The other
  `Vec<bool>` visited sets found in the vector module are all build-time/
  compaction/merge-oracle code, not the per-query path; `search_sq.rs` in
  particular carries an explicit comment warning that a prior BitVec
  conversion there caused correctness issues, so it was left untouched.
- **Item C3 — SmallVec the per-tick elastic-budget shard snapshot**
  (`src/shard/shared_databases.rs`): `recompute_elastic_budget` (called
  from every shard's 100ms eviction tick) `collect()`ed a fresh
  `Vec<usize>` snapshot of all shards' published memory on every call.
  Switched to `SmallVec<[usize; 16]>` — stack-only for the common <=16
  shard case, unchanged single heap allocation beyond that.
- **Item C4 — Lua script-cache byte estimate exposed via INFO/MEMORY
  DOCTOR** (`src/scripting/cache.rs`, `ScriptCache::resident_bytes()`): the
  per-shard Lua cache was invisible to observability — its growth folded
  silently into "allocator overhead." Added a byte-estimate accounting
  method, published per-shard via the existing C5/M4 `ShardStoreMemory`
  tick pattern (new `lua` atomic), and surfaced in both the Prometheus
  `moon_memory_bytes{kind="lua_scripts"}` gauge and `MEMORY DOCTOR`'s text
  report. The cache itself remains intentionally unbounded (Redis parity —
  `SCRIPT FLUSH` is the only eviction path); this is observability only.
- **Item C5 — removed dead `parse_single_frame_zc` RESP parser**
  (`src/protocol/parse.rs`): a full ~150-line RESP2/RESP3 parser
  superseded by the current `validate_frame` + `parse_frame_zerocopy`
  pipeline, with zero external callers (only self-recursion) — silently
  masked by the file's `#![allow(dead_code)]`. Removed along with its
  exclusively-private helper `read_decimal_zc`.
- **Item C6 — jemalloc decay policy audited, docs added (SKIP code
  change)** (`CLAUDE.md`): the baked-in `_rjem_malloc_conf` static and the
  `--memory-arenas-cap` re-spawn override already carry byte-identical
  `dirty_decay_ms:1000,muzzy_decay_ms:5000,background_thread:true` tuning
  — no drift to reconcile. Added the missing operator-facing
  `_RJEM_MALLOC_CONF` documentation (docs-only, no code changed).
- **Item C7 — tokio 1ms shard tick idle cost audited (SKIP)**
  (`src/shard/event_loop.rs`, `src/shard/spsc_handler.rs`): the 1ms
  `periodic_interval` tick's SPSC drain is already a non-blocking,
  zero-allocation `try_pop()` loop, and every downstream side effect is
  already gated behind a cheap conditional. The one unconditional cost
  (`cached_clock.update()`, a single `clock_gettime`) is the documented
  "Timestamp caching" design. Unlike item B's AOF writer poll, this 1ms
  cadence IS the low-latency WAL-flush contract (CLAUDE.md), not
  incidental idle waste — escalating it would widen that bound. No code
  change; a real fix would be event-driven WAL triggering, an
  architectural change out of scope here.
- Item C8 (sigterm readiness deadline) landed early via the Windows-CI PR
  (#229) — see the CI section below.

### CI — fix Windows main-push test failures (PR #TBD)

- `test_poll_real_process_smoke` is now gated to Linux/macOS: `get_rss_bytes()`
  returns the documented `0` fallback on every other platform, so asserting
  `rss_bytes() > 0` can never pass on Windows.
- `test_scatter_text_aggregate_single_shard_skips_spsc` now runs on a fresh
  OS thread and calls `init_shard` itself (via a new shared
  `slice::test_support::make_init` fixture). As a plain `#[tokio::test]` it
  only passed when libtest scheduled it onto a harness thread where an
  earlier test had left a `ShardSlice` behind — a latent order-dependent
  flake on all platforms that failed deterministically on Windows CI.
- All 8 `find_moon_binary` test helpers now fall back to
  `env!("CARGO_BIN_EXE_moon")` (after the `MOON_BIN` override) instead of
  probing `target/{release,debug}/moon`: the old probing found nothing on
  Windows (missing `.exe`), ignored `CARGO_TARGET_DIR`, and preferred a
  possibly-stale release binary over the one cargo just built for the run.
- New `MOON_DISK_FREE_MIN_PCT` env override for `--disk-free-min-pct`
  (clap `env` feature; CLI flag still wins). Windows CI exports it as `0`:
  windows-latest runners sit below the 5% free-disk default, so every
  server-spawning suite failed with `MOONERR diskfull` instead of `OK`.
- `shardslice_shape.rs::split_off_test_module` computed line offsets with a
  1-byte `\n` assumption; on CRLF checkouts (Windows runners set
  `core.autocrlf=true`) the split point drifted one byte per line and
  panicked slicing inside a multibyte comment char. Now uses
  `split_inclusive('\n')` for byte-exact offsets on both line endings.
- mem_watchdog integration cases A/B (guard-engages assertions) are gated
  to Linux/macOS for the same `get_rss_bytes()` 0-fallback reason as the
  lib smoke test: with RSS reported as 0 the memfull guard is structurally
  inert on Windows. Cases C/D (guard-disabled directions) still run there.
- `sigterm_shutdown.rs` readiness deadline widened 15s → 60s behind a
  shared `READY_TIMEOUT` constant (poll-based loop notices readiness within
  100ms either way; the fixed 15s was a documented host-load flake,
  observed on the macOS CI runner).
### Fixed — connection-plane durability & lifecycle wave (PR #230)

- **D-2 — FLUSHDB/FLUSHALL now clear every shard, not just the local one**
  (`src/shard/coordinator.rs` `coordinate_flush_broadcast`, both sharded
  handlers): FLUSHDB/FLUSHALL are keyless, so key-based routing executed them
  only on the issuing connection's shard — on `--shards 4` a FLUSHALL left
  ~3/4 of the keyspace intact (red/green: 49/64 keys survived, now 0). The
  originating shard now broadcasts the flush to every peer shard as a
  `MultiExecute` leg, which reuses the existing remote dispatch + per-shard
  AOF/WAL persistence + index-flush path. Any failed leg returns a loud
  `MOONERR FLUSH partial` error (local shard already flushed; client should
  retry). Known limits: the flush is not atomic across shards (same relaxed
  semantics as SWAPDB), and a FLUSH issued inside MULTI/EXEC still executes
  local-only (follow-up); FLUSHALL still clears only the selected db
  (documented v0.1.5 single-active-DB behavior — all-dbs semantics is a
  separate follow-up needing replay parity).
- **D-1 — cross-store TXN crash replay restores into the correct db**
  (`src/transaction/mod.rs`, `wal_v3/record.rs`, `wal_v3/replay.rs`, txn
  handlers): `CrossStoreTxn` had no db field and WAL replay hardcoded
  `databases[0]`, so a `TXN.BEGIN…COMMIT` issued under `SELECT n≠0` replayed
  its KV ops into db 0 after a crash — silent recovery corruption. Commits now
  write a new `XactCommitV2` record (`0x53`) whose header carries the
  BEGIN-time db index; replay targets that db (out-of-range falls back to
  db 0 with a loud warning). v1 records still replay into db 0 (faithful to
  the builds that wrote them).
- **R-6 — connection migration fails open** (`conn_accept.rs`,
  `handler_sharded/mod.rs`): the source shard dropped its socket before the
  SPSC hand-off to the target was confirmed, so a full ring lost the client —
  precisely under the overload that triggers migration. Both runtimes now
  keep the original stream alive until the push succeeds (bounded retry,
  8 × 100µs) and on give-up resume serving the connection on the source shard
  with migration disabled, using the state recovered from the undelivered
  message. Also fixes the tokio path's `connected_clients` leak on the old
  loss path.
- **Observability** (`admin/metrics_setup.rs`): new
  `moon_xshard_backpressure_drops_total{target_shard}` counter + warn on every
  R-1 give-up, and `moon_shard_connected_clients{shard}` gauge (maintained at
  registry register/deregister) to surface SO_REUSEPORT imbalance and
  affinity funnels without parsing `CLIENT LIST`.
- **P-1 — lazy per-connection `FunctionRegistry`** (both handlers,
  `conn/core.rs`): the Functions API registry + `LuaEvictionCtx` (6 Arc/Rc
  clones) were built eagerly for every connection; now built on first
  FUNCTION/FCALL/FCALL_RO, cutting per-connection setup cost at high
  connection counts.
- **S-5 — `--tcp-backlog`** (`config.rs`, `conn_accept.rs`, `main.rs`): the
  per-socket listen backlog was hardcoded 1024; now configurable (default
  unchanged) for connection-storm tuning alongside `ulimit -n`.

### Fixed — connection-plane robustness hardening (Track B) (PR #230)

- **R-3 — `CLIENT KILL` force-closes idle connections** (`src/client_registry.rs`,
  handlers, `conn_accept.rs`): `CLIENT KILL` only set a cooperative `kill_flag`
  checked once per batch, so a connection parked in `read().await` (idle, the
  default `timeout 0`) was never torn down until it next sent bytes. Now the
  registry stores each connection's socket fd and `kill_clients` also
  `shutdown(2)`s it, so the parked read returns `Ok(0)`/`Err` immediately (the
  existing disconnect path). The raw-fd close is race-free: `kill_clients` holds
  the registry read lock, and a connection's `RegistryGuard` deregisters (needs
  the write lock) strictly before its stream drops — so a visible entry always
  has a live fd. No-op on non-unix (`CLIENT KILL` stays cooperative there).
  Self-kill (the filter matching the executing connection) stays cooperative —
  flag only, no fd shutdown — so the `+count` reply is still delivered before
  the handler closes, matching Redis's reply-first self-kill semantics.
- **R-2 — inline-command length cap** (`src/protocol/inline.rs`,
  `frame.rs`): the RESP-less inline path had no maximum line length, so a
  client that never sends `\r\n` (raw non-RESP bytes) grew the connection read
  buffer without bound — a per-connection memory-exhaustion vector. Added
  `ParseConfig::max_inline_size` (default 64 KB, mirroring Redis's
  `PROTO_INLINE_MAX_SIZE`); `parse_inline` now rejects an over-cap line
  (complete or incomplete) with `Protocol error: too big inline request`
  instead of buffering forever.
- **R-1 — bounded cross-shard `spsc_send`** (`src/shard/coordinator.rs`): the
  single dispatch primitive behind every scatter-gather command (MGET/MSET/
  DEL/EXISTS/SCAN/KEYS/DBSIZE, vector scatter, FT.INFO, graph traverse) retried
  a full SPSC ring in an **unbounded** `loop { try_push; yield/sleep }` — on
  tokio `yield_now()` reschedules with no backoff, busy-spinning a full core,
  and a wedged target could block graceful shutdown forever. Replaced with a
  bounded retry (`CROSS_SHARD_PUSH_MAX_RETRIES` × `CROSS_SHARD_PUSH_BACKOFF`,
  the same budget as `push_with_backpressure`) returning `PushOutcome`. On
  give-up the message is dropped; reply-carrying callers observe the closed
  reply channel and synthesize a per-shard error via the path they already
  handle. `PushOutcome` is `#[must_use]` so no call site can drop a message
  silently by accident, and the two side-effect-bearing senders fail loud:
  a dropped `MqTxnMaterialize` leg turns `TXN.COMMIT`'s reply into an explicit
  `MOONERR TXN.COMMIT partial` error (the commit is already WAL-durable; only
  foreign MQ materialization was lost) and a dropped `GraphRollback` logs at
  `error!`. A brief bounded spin (≤64 iterations) before the first timed
  backoff preserves the old ~10µs-class latency when the target ring is only
  transiently full (the 100µs timer sleep may round up to ~1ms).
- **R-4 — accept-loop backoff on fd exhaustion** (`src/server/accept_backoff.rs`,
  `listener.rs`, `shard/event_loop.rs`): all six socket-accept loops (tokio
  and monoio; sharded, non-sharded, and TLS) retried `accept()` errors with
  zero backoff, so `EMFILE`/`ENFILE` under a connection storm pinned a shard
  core at 100% and flooded the log. Added `AcceptBackoff`: capped exponential
  backoff (1 ms → 100 ms) on resource-exhaustion errors only, plus rate-limited
  logging; benign per-connection errors (e.g. `ECONNABORTED`) log without
  sleeping. The cap is 100 ms (not 1 s) because the sleep runs inside `select!`
  arms where the shutdown branch cannot preempt it — this bounds shutdown
  latency during a storm. The io_uring multishot-accept resubmit (opt-in `MOON_URING=1`
  bridge) is documented as a scoped follow-up (synchronous CQE handler, no
  async context to sleep in).

### Changed — graph engine deep-review fixes + optimization waves (PR #TBD)

- **Freeze-boundary correctness (P0):** CSR v5 segments now persist node
  properties, embeddings, and edge weights/properties across freeze;
  Cypher reads see the frozen tier via `MergedNodeView` (NodeScan/eval/
  IndexScan); cross-tier delta edges; GRAPH.NEIGHBORS existence +
  direction fixes; GRAPH.HYBRID/VSEARCH operate across both tiers. New
  red/green `graph_freeze_boundary` suite (11 tests) pins the lifecycle.
- **Point-query indexes (P1):** lazy per-segment property indexes (built
  once at first use from freeze-time data) + `PhysicalOp::IndexScan` in
  the planner/executor — `MATCH (n:L {p: v})` narrows instead of
  label-scanning.
- **Query-path cost (P2):** plan-cache auto-parameterization (literal
  variants share one cached plan; cache hits skip parse+compile
  entirely) with LRU eviction; slot-indexed executor rows (no per-row
  HashMap allocation, no per-insert String clone); write-path/WAL
  allocation cleanups (ADDNODE double WAL-encode deleted, itoa/ryu
  statement temporaries, FxHash for slotmap-keyed sets, Dijkstra
  three-maps-to-one merge, allocation-free neighbor iteration in Cypher
  Expand).
- **Traversal engine (P3):** CSR row-space BFS fast path on fully-frozen
  graphs — dense-bitmap visited set, TRUE parallel frontier expansion
  (thread::scope over Send+Sync CsrStorage), direction-optimizing
  Beamer push/pull over the incoming index; `TraversalGuard` wall-clock
  budget (30s default) now enforced per hop in Cypher variable-length
  Expand and ShortestPath (`ExecErrorKind::Timeout`); GRAPH.HYBRID's
  `HnswPreFilter` strategy is real — a lazy per-segment HNSW bridge
  (`src/graph/hnsw_bridge.rs`, raw-f32 cosine over v5 embeddings, >= 4096
  vectors) replaces the silent brute-force stub, with exact-scoring
  fallback whenever the approximate beam under-fills.
- **Pre-merge review fixes:** (1) restart NodeKey aliasing (P0) — a fresh
  post-recovery `MemGraph`'s deterministic SlotMap could mint keys
  bit-identical to loaded CSR segments' `external_id`s, silently
  shadowing frozen nodes; recovery now seeds the mutable tier via
  `MemGraph::with_id_offset` (watermark past the largest persisted id)
  and WAL replay dedup-skips AddNodes already resident in a loaded
  segment (red/green `graph_restart_id_aliasing` suite). (2) Plan-cache
  raw-hash fast path — exact-repeat queries hit the cache without the
  `parameterize()` lexer pass, and the `SlotTable` is cached alongside
  the plan instead of being rebuilt per execution; cache backing moved
  to `FxHashMap`. (3) `CsrStorage::resident_bytes` now counts the v5
  property blobs, lazily-built per-segment property indexes, and the
  HNSW bridge (heap-owned; mmap-backed sections count 0 per the
  `RawF16Store` precedent), keeping the elastic memory budget honest.

### Changed — graph engine wave 2: durability, Cypher coverage, hardening (PR #TBD)

- **Durability (P0, found by GCP production-hardening soak):** graph data now
  survives kill -9 under the DEFAULT disk-offload (WAL v3) configuration.
  Two stacked pre-existing bugs erased graphs on crash: (A) v3 recovery
  collected graph WAL commands into a throwaway replay engine — graph replay
  was a complete no-op in v3 mode (a dedicated `replay_graph_wal_v3` boot
  pass now applies them); (B) the checkpoint advanced the WAL replay floor
  and recycled segments without ever snapshotting the graph store —
  `save_graph_store` ran only on graceful shutdown and never persisted the
  mutable tier (checkpoint finalize now freezes each graph's write buffer,
  persists segments + a `snapshot_lsn` replay floor, and ABORTS the
  checkpoint if the graph snapshot fails, keeping the old floor). The prior
  crash suite ran exclusively with `--disk-offload disable`; new G4/G5
  scenarios cover the default config with and without a checkpoint crossing.
- **Durability:** stable external node/edge ids across WAL replay (handles
  handed to clients before a crash resolve to the same rows after recovery);
  Cypher `SET` property/label writes now emit WAL records
  (`GRAPH.SETPROP`/`GRAPH.SETLABEL` — previously a documented durability gap:
  SET mutations silently vanished on kill -9); new kill-9 crash-recovery
  suite (`tests/crash_recovery_graph_durability.rs`: mutable tier, frozen
  64K-edge tier rebuilt through replay-time freeze, double-crash idempotence).
- **Cypher coverage:** aggregations `count`/`sum`/`avg`/`min`/`max`/`collect`
  with implicit grouping, `DISTINCT`, and count-over-zero-rows semantics;
  `OPTIONAL MATCH` null-pads unmatched expansions (previously compiled
  silently to inner MATCH; unsupported shapes now reject loudly);
  `WITH` rebinds the pipeline mid-query (aggregate + `WHERE`-as-HAVING,
  `ORDER BY`/`SKIP`/`LIMIT` between WITH and RETURN; previously every clause
  after WITH ran on an empty row stream). `WITH *` rejects loudly.
- **Copy-up writes:** `SET`/`DELETE`/`MERGE` on frozen rows copy the row up
  into the write buffer instead of silently missing the frozen tier.
- **Query performance:** IndexScan range predicates (`WHERE n.p > x` prunes
  via per-segment B-tree-ish numeric index, superset semantics + residual
  filter); write-side plan cache (repeated write shapes skip parse+compile);
  row-BFS fast path engages on multi-segment fully-frozen graphs (~4× vs
  reader fallback, criterion-pinned); `Value::String` holds `Bytes` for a
  zero-copy reply path; HYB-02/HYB-04 use the HNSW bridge with off-thread
  bridge builds.
- **Query performance — mutable-tier property index (task #31):** `MATCH
  (a:N {id: X})` on the write buffer used to degrade to a full
  `O(live_node_count)` linear scan even though `PhysicalOp::IndexScan` was
  already the plan (profiling on the 5K-node/15K-edge Cypher point-query
  bench put this scan at 82.5% of shard CPU — the mutable tier never
  freezes below `edge_threshold`, default 64,000). A new incrementally
  maintained `MutablePropertyIndex` (`src/graph/index.rs`, mirrors the
  frozen tier's `SegmentPropertyIndexes` numeric-BTree/string-hash split,
  keyed by `NodeKey`) turns the mutable-tail probe into an O(log N +
  |result|) index seed; `index_scan_keys`'s residual label/MVCC/property
  checks are unchanged (SUPERSET contract preserved, no planner changes).
  `MemGraph::set_node_property`/`remove_node_property`/`undelete_node` are
  now the single source of truth for node-property mutation, replacing 5
  previously hand-rolled call sites (Cypher `SET`, MERGE ON CREATE/MATCH
  SET, TXN.ABORT undo, WAL replay) that could silently let the index drift
  from live state. Closes a TXN.ABORT `UndeleteNode` gap found during
  design review: undoing a `DELETE` inside a transaction now re-indexes the
  restored node instead of leaving it live but permanently unreachable by
  index probes.
- **Query performance — Cypher result cache (task #32):** read-only
  `GRAPH.QUERY`/`GRAPH.RO_QUERY` now cache the fully-encoded RESP reply
  bytes for repeated identical queries (same raw Cypher text + args),
  keyed by `(query_hash, args_hash)` and served via the existing
  `Frame::PreSerialized` passthrough — no new protocol variant needed, and
  both RESP2 and RESP3 encodings are cached in separate slots so a
  protocol-version switch is a clean miss rather than a stale re-encode.
  Invalidation is a per-graph monotonic `write_gen: u64` bumped by
  `NamedGraph::touch()` at every real mutation site (plain `GRAPH.ADDNODE`/
  `GRAPH.ADDEDGE`, the Cypher write-plan mutation loop, `TS.*` temporal
  invalidation, and TXN.ABORT rollback of graph undo-ops) — deliberately
  NOT at `freeze_and_compact`, which reshapes storage without changing
  query-visible content. The write_gen is captured before executing a
  read and the encoded reply is only cached if it is still unchanged
  after — a miss is always safe (SUPERSET semantics), so a benign race
  just skips caching rather than serving stale data. Decayed queries and
  errored/timed-out results are never cached. `ResultCache` is a bounded
  per-graph LRU (256 entries / 4MiB default, `src/graph/cypher/
  result_cache.rs`) whose resident bytes are folded into
  `GraphStore::resident_bytes()`; dropping a graph (`GRAPH.DELETE`) drops
  its cache with it. The cache is only consulted on the connection-local
  dispatch path where the negotiated protocol version is reliably known
  (`dispatch_graph_read`, threaded as `Option<u8>`); the cross-shard
  `GraphCommand` hop and `graph_query_or_write`'s internal read-execute
  calls pass `None` and bypass the cache entirely rather than risk an
  unverified protocol version.
- **Query performance — FTS reuse for Cypher text predicates (task #33):**
  first-class `CONTAINS`/`STARTS WITH`/`ENDS WITH` Cypher operators (pure
  syntax sugar over the existing `=~` byte-level checks — `src/graph/
  cypher/lexer.rs`, `ast.rs`, `parser/expr.rs`, `executor/eval.rs`), plus a
  new per-frozen-segment `SegmentTextIndex` (`src/graph/text_index.rs`,
  lazy `OnceLock` + `resident_bytes` accounting, same pattern as
  `SegmentPropertyIndexes`/`hnsw_bridge`) that accelerates `CONTAINS`/
  `STARTS WITH`/`ENDS WITH`/`=~` conjuncts in `WHERE` (`planner.rs`'s new
  `extract_text_conjuncts`, mirroring W2-3's `extract_range_conjuncts`).
  Reuses `crate::text::posting::PostingStore`/`crate::text::bm25::
  {FieldStats, bm25_score}`/`crate::text::term_dict::TermDictionary`
  verbatim (already ID-space-agnostic, zero-fork). **Correctness note:**
  the index prunes on PROPERTY PRESENCE only, not token identity — a
  tokenized/stemmed/case-folded posting lookup cannot safely decide
  substring/prefix/suffix containment (e.g. `CONTAINS 'rust'` must also
  match `"trusted"`, which tokenizes to a different term entirely), so
  `candidate_rows` returns every row whose target property is a
  String/Bytes at all, and the existing residual `Filter` remains the sole
  decider (SUPERSET contract, same as `prop_eq`/`prop_range`). The
  MUTABLE tier gets no acceleration (falls back to an exact scan of the
  write buffer, pre-approved scope decision — mirrors the pre-task-#31
  numeric story); a `GraphUnion`-merged segment does not remap posting row
  ids and simply rebuilds its text index lazily on first use post-merge.
  `SegmentTextIndex::bm25_score_for` is a tested-but-unwired internal BM25
  relevance-scoring hook (no Cypher `ORDER BY` grammar was specified for
  it) — proves the `bm25_score` reuse end-to-end; surface syntax is a
  documented follow-up.
- **Operability:** traversal timeout is configurable — `--graph-timeout-ms`
  server default plus per-query `GRAPH.QUERY ... TIMEOUT <ms>` (RedisGraph
  parity, 0 = unlimited).
- **Dead code:** cross-shard traverse scaffolding deleted (532 lines, no
  sender existed); the single-shard-per-graph sharding model is now
  documented in `src/graph/mod.rs`.

- Cargo: `ringbuf` 0.4.8 → 0.5.0 and `metrics-exporter-prometheus` 0.16.2 →
  0.18.3 (semver-major; both compile and test green with no code changes —
  verified by dependabot's per-bump CI runs and locally), `bytes` 1.11.1 →
  1.12.0, `rand` 0.10.1 → 0.10.2, `lz4_flex` 0.13.0 → 0.13.1, `cmov` 0.5.3 →
  0.5.4, `cudarc` 0.19.4 → 0.19.8 (gpu feature, not covered by CI — patch
  bump), and `openssl` 0.10.78 → 0.10.81 in `sdk/rust` (security bump).
- GitHub Actions: `actions/checkout` v4 → v7 (including one straggler in
  `docker-publish.yml` dependabot missed), `actions/deploy-pages` v4 → v5.
- Console: `vite` 7.3.2 → 7.3.5 (dev dependency).
- Dependabot: `dtolnay/rust-toolchain` is now ignored — its pin IS the MSRV
  (1.94) gate, so auto-bumping it to latest stable (rejected PR #195)
  silently defeats the MSRV check.
- Supersedes dependabot PRs #191, #194, #196–#201, #208–#210 (their Lint
  failures were the CHANGELOG-entry gate, which this consolidated PR
  satisfies once).

### Fixed — proactive RSS watchdog pauses writes before kernel OOM (PR #TBD)

- **New `mem_monitor` guard** (`src/shard/mem_monitor.rs`), the memory
  analogue of the existing diskfull guard (MA12): pauses writes once process
  RSS crosses `--mem-full-pct` (default 95, 0 = disabled) of the DETECTED
  system/cgroup memory limit, not the configured `--maxmemory` (which can be
  an unconfigured `0`/unlimited). Mirrors `disk_monitor`'s hysteresis state
  machine, inverted for direction (high RSS is bad): pauses at
  `rss% >= mem_full_pct`, resumes only at `rss% <= mem_full_pct - 5`.
  `MOONERR memfull: writes paused until memory pressure recovers` joins the
  existing `MOONERR diskfull` / `MOONERR busy` message-selection chain in
  both dispatch paths (order: diskfull, memfull, segment-backlog busy); the
  hot-path cost is one additional `AtomicBool::load(Relaxed)` inside the
  already write-gated branch. Polled on shard 0's existing 5s disk-monitor
  timer tick (both event-loop poll sites), plus one immediate poll at
  startup (`init_global`) so the AOF-replay/segment-load recovery peak is
  visible before the first tick. New INFO fields
  `reclamation_mem_rss_bytes` / `reclamation_mem_watchdog_active`
  (OR'd into the existing `reclamation_write_stall_active`). Same accepted
  trade-off as the diskfull guard: `DEL`/`UNLINK`/`EXPIRE`/`FLUSHALL` are
  write-flagged and blocked too while paused — no allowlist.

### Fixed — vector keymap CoW amplification + bounded snapshot queue (PR #TBD)

- `VectorIndex`'s three `key_hash -> V` maps (`key_hash_to_key`, `key_hash_to_global_id`,
  `key_hash_to_vec_checksum`) were each a single `Arc<HashMap<u64, V>>`. Under
  `ft-search-off-eventloop`, a live FT.SEARCH snapshot holds an extra `Arc` clone of the whole
  map while the event loop keeps processing writes; the next write's `Arc::make_mut` then sees
  refcount > 1 and full-clones the entire map synchronously on the shard event loop (~47MB
  @1M vectors). Replaced with `BucketedKeyMap<V>` (`src/vector/keymap.rs`): 256 fixed buckets,
  each independently `Arc<HashMap<u64, V>>`, bucket selected by `(key_hash >> 56)`. A concurrent
  snapshot now pins at most 1/256th of the map per write instead of the whole thing. Same
  treatment applied to `SnapshotJob`'s mirrored fields and `SearchSnapshot.key_hash_to_key`.
  On-disk `keymap.bin` format is unchanged (read-compatible).
- `SnapshotPool` (`src/vector/persistence/manifest.rs`) used an unbounded `flume` queue with a
  single worker; if compact/merge cadence outpaced the worker, queued jobs pinned every
  submitter's `Arc`s indefinitely. Replaced with a coalescing slot keyed by index directory: a
  new submit for an index with a job still pending (not yet dequeued) replaces it in place,
  dropping the stale job's `Arc`s immediately; an in-flight (already-dequeued) job is unaffected.
  Submit never blocks the caller.

### Fixed — CI: de-flake OOM case E + Windows test-compile gate (PR #TBD)

- **`test_case_e_cross_db_copy_oom` (was red on main's post-merge CI for PR #217/#218/#219,
  0/300 OOM on both platforms while passing locally):** the single-shot statistical assert sat
  inside the slack GAP-1's elastic budget + its 100ms-stale usage snapshots can grant. Rewritten
  as bounded escalation: up to 8 rounds of COPYs into fresh destination keys until the
  destination db's per-shard usage (~2.4MB) provably exceeds the ABSOLUTE budget ceiling
  (`compute_elastic_budget ≤ maxmemory` = 2MB) — timing-independent on any runner speed, and the
  RED floor stays exact (gate stashed = 0 OOM through all rounds; re-verified red/green).
- **`tests/quickwins_red_api.rs` broke the Windows CI leg at COMPILE time** (also red on main):
  `qw1_accepted_socket_has_nodelay` calls `apply_client_socket_opts`, which is `#[cfg(unix)]`
  (takes `AsFd`). The test (and its imports) are now unix-gated to match.
- **`write_stall_segment_backlog`: two tests raced on the process-global
  `RECL_SEGMENT_STALL_ACTIVE` atomic** (test harness runs same-binary tests on parallel
  threads; observed on CI macOS as `store(1)` reading back `0`). All mutation of the global now
  lives in one merged test.

### Fixed — FT.COMPACT left mutable residue behind when draining a background build (PR #TBD)

- **Pre-existing** (`main`, not introduced by this branch): when an explicit
  `FT.COMPACT`/`force_compact` found a background auto-compaction already in flight, it drained
  that build, installed it, persisted, and returned — WITHOUT compacting the documents inserted
  while the background build was running (the `clone_suffix(frozen_len)` mutable tail). Those
  docs stayed mutable-only (no durable segment) until some future compact fired, breaking
  force_compact's documented full-drain contract (frozen == mutable on reply) and — combined
  with the B3 phantom-keymap hole below — silently losing them on a kill -9 (observed stuck
  durable state: segments live=1961, keymap=2000, converging never). Load-dependent: only
  reproduces when insert cadence keeps a background build in flight at FT.COMPACT time (this is
  what made the crash suite's S1/S2 flake by machine load, masquerading as a branch regression).
  `force_compact` now falls through to the inline drain loop after installing the in-flight
  build (a no-op when the tail is empty).

### Fixed — B3 vector recovery: phantom keymap entries silently dropped docs (PR #TBD)

- **Pre-existing silent data loss on kill -9** (found while validating this branch against the
  `crash_recovery_vector_durability` suite; reproduces on `main`): the durable
  `keymap-<epoch>.bin` covers EVERY indexed key (mutable + immutable) at snapshot-submit time,
  while the paired `manifest.json` lists only installed immutable segments. A crash landing after
  one snapshot commits but before the next (covering a just-frozen segment) leaves the on-disk
  keymap a strict superset of the on-disk segments. The B3 dedup rescan then "verified" those
  uncovered keys as unchanged (keymap checksum matches the AOF blob — exactly the crash-window
  signature) and never re-indexed them: their documents vanished from search with
  `num_docs` under-reporting (observed: 1950/2000) while recovery logged
  `verified unchanged` for all keys. Fix: `recover_v2::load_segments_and_keymap` now drops any
  keymap entry whose `key_hash` is not LIVE in a loaded segment
  (`ImmutableSegment::live_key_hashes`), so the rescan re-indexes those keys from the AOF; a
  loud log line reports the dropped-phantom count. New deterministic unit regression
  (`recover_reindexes_keymap_entries_not_backed_by_any_segment`, red/green verified) plus
  integration scenario S6 (kill inside the async-snapshot window; asserts only the crash
  contract: `num_docs == N`, every key answers its own exact-match query). S1/S2's
  `re_indexed == 0` fast-path determinism is restored by a new full-durability pre-kill gate
  (`wait_for_durable_docs`: manifest'd segment live-counts AND keymap entries both equal N).

### Fixed — zombie/CPU hardening wave 1 (PR #TBD)

- **Busy-poll spin idle-disengages** (`--io-busy-poll-us` / vendored monoio
  legacy driver): an idle server no longer burns the spin budget on every
  park forever (measured ~2.4s CPU per 3s wall on an idle 4-shard server at
  200µs). The driver tracks the last real event per thread and skips the
  spin window after `MOON_SPIN_IDLE_DISENGAGE_US` (default 10ms; 0 = old
  always-spin) of quiet; the next event re-arms it via the normal wake path,
  so steady traffic — the GCE p=1 win path — never disengages.
- **Shard-thread panics abort the whole process** instead of leaving an
  N-1-shard server silently answering with a dead shard's keys gone; the
  shutdown join loop logs instead of swallowing panics. New `DEBUG PANIC`
  subcommand (Redis parity) as the crash-handling test aid.
- **SIGTERM shutdown regression matrix**: shards × held-connections ×
  busy-poll × 16-writer AOF write-storm (7 cases, both runtimes, 3
  platforms). The documented SIGTERM+SO_REUSEPORT bench hang did NOT
  reproduce at HEAD in any composition; the matrix stays as the guard and
  the detached monoio accept-task change is deferred until a reproducer
  exists.
- **`wait_ready` test harness hardening**: readiness probes retry with a
  fresh connection on mid-startup connection resets (CI flake: per-shard
  SO_REUSEPORT listeners accept-then-reset during init).

### Fixed — OOM eviction bypass closure (PR #TBD)

- **Cross-shard SPSC write legs now enforce `--maxmemory`.** `Execute`,
  `MultiExecute`, `PipelineBatch` and their `*Slotted` variants
  (`src/shard/spsc_handler.rs`) executed writes against the TARGET shard's
  `Database` directly, bypassing the connection handlers' eviction gate
  entirely — a scatter-gather write (e.g. a pipeline of individually-routed
  `SET`s hashing to a remote shard) could grow that shard's memory past
  `maxmemory` without limit. A new `spsc_eviction_gate` helper mirrors
  `run_write_eviction_gate` (`handler_monoio/mod.rs`) exactly — same elastic
  per-shard budget (GAP-1), same spill-vs-plain branching — threaded through
  `drain_spsc_shared`/`handle_shard_message_shared` and gated by a
  once-per-drain-cycle `evict_active` snapshot (perf parity with
  `batch_eviction_active`: zero extra lock acquires when `maxmemory` is
  unset). **Perf follow-up (same PR):** that snapshot, and the Lua bridge's
  gate below, each used to answer "is `maxmemory` nonzero?" with either a
  `runtime_config.read()` per drain cycle or a generation/`Cell` snapshot.
  Both now read a single process-global `AtomicU64`
  (`storage::eviction::{publish_maxmemory, maxmemory_is_set}`), published at
  every production write site of `RuntimeConfig.maxmemory` (`CONFIG SET
  maxmemory`, server startup) — one Relaxed load, no lock, no per-script
  Cell bookkeeping.
- **Lua `redis.call`/`redis.pcall` writes now enforce `--maxmemory`.**
  EVAL/EVALSHA carry no WRITE command flag, so the dispatch-level OOM check
  never saw them, and the bridge (`src/scripting/bridge.rs`) never ran the
  check before executing a write inside a script — a tight `redis.call('SET',
  ...)` loop could write arbitrarily far past the cap. A new
  `LuaEvictionCtx`, captured by value into the `redis.call`/`redis.pcall`
  closures at VM-setup time (`setup_lua_vm`, once per shard — zero new
  `unsafe`, zero per-call allocation), runs the same gate before a WRITE
  `redis.call` executes; `redis.call` raises the OOM error as a Lua runtime
  error (script aborts, matching Redis semantics), `redis.pcall` returns it
  as an `{err = ...}` table. Read-only scripts are unaffected.
- **FCALL-internal `redis.call`/`redis.pcall` writes now enforce
  `--maxmemory` too.** `FUNCTION LOAD` (`src/scripting/functions.rs`)
  creates its own per-library sandboxed Lua VM, separate from the shard's
  shared EVAL/EVALSHA VM — that per-library VM registered `redis.call`/
  `redis.pcall` with `LuaEvictionCtx::disabled()` unconditionally, so a
  `FUNCTION` whose body wrote in a loop could grow memory past the cap
  independent of the EVAL/EVALSHA fix above. `FunctionRegistry::new` now
  takes a `LuaEvictionCtx`, stored on the registry and cloned into every
  library's VM at `create_library` time; both production construction sites
  (`handler_monoio/mod.rs`, `handler_sharded/mod.rs`) build a real ctx from
  the same shard handles `setup_lua_vm` already uses for the shared VM
  (`ConnectionContext::{shard_databases, runtime_config, shard_id,
  spill_sender, spill_file_id, disk_offload_dir}`); only test-only callers
  pass `LuaEvictionCtx::disabled()`.
- **Cross-shard `MOVE`/`COPY ... DB n` two-database intercept now runs on
  EVERY `ShardMessage` SPSC arm, not just the plain `Execute` arm.** The
  intercept (special-cased ahead of the generic single-db write path because
  both commands need two `&mut Database` borrows at once) previously existed
  only in `Execute`. Every other arm — including `PipelineBatchSlotted`, the
  one real client traffic (`handler_monoio`/`handler_sharded`'s pipelined
  remote-write path) actually uses — fell through to the generic single-db
  `key_extra::copy`, which parses and silently ignores the `DB` clause: a
  remote `COPY src dst DB n` performed a same-db copy instead (data
  corruption, confirmed pre-fix via manual probe: 4/20 remote `COPY`s landed
  in the wrong db), and a remote `MOVE` returned a loud-but-wrong "MOVE
  requires handler-level dispatch (cross-db operation)" error instead of
  moving the key. The intercept is now extracted into a shared helper
  (`src/shard/spsc_two_db::try_two_db_intercept`) that `Execute`,
  `MultiExecute`, `PipelineBatch`, `ExecuteSlotted`, `MultiExecuteSlotted`
  and `PipelineBatchSlotted` all call verbatim; cross-db `COPY` still runs
  `spsc_eviction_gate` against the destination db before `copy_core` (`COPY`
  duplicates the value, so it needs the gate; same-shard `MOVE` is net-zero
  and stays ungated, same rationale as `DEL`).
- **Found, NOT fixed (out of scope): cross-shard `COPY`'s destination key is
  only reliably retrievable when it hash-tag-colocates with the source key.**
  Moon shards purely by key hash, with no db-awareness. A `COPY src dst DB n`
  executes on whichever shard owns `src` (that's the routing key for the
  whole command) and writes `dst` into that SAME shard's db space — it does
  not (and cannot, without a real cross-shard two-phase protocol) relocate
  the write to whichever shard `dst`'s OWN hash would naturally pick. A later
  plain `GET dst` routes by `hash(dst)`, so it only reliably finds the value
  when `src` and `dst` share a hash tag (or `--shards 1`). `MOVE` is exempt
  (the key name is unchanged, so `hash(key)` is identical before and after).
  This is a pre-existing, inherent property of the per-key-hash sharding
  model applied to `COPY` with a renamed destination — the plain `Execute`
  arm this fix mirrors has the identical property — not a regression from
  extending the intercept to every arm. Fixing it is a materially larger
  change (routing the destination write to `dst`'s own natural shard) than
  "apply the existing intercept to more arms" and is out of scope here.
- **Found, NOT fixed (out of scope): Moon's eviction gate is per-DATABASE,
  not per-shard-aggregate-across-databases.** `try_evict_if_needed_budget`
  (and every SPSC write arm, including the new cross-db `COPY` gate above)
  measures `db.estimated_memory()` for the ONE database being written to,
  not the sum across all 16 logical dbs a shard holds — consistent
  throughout Moon's write path, but a deviation from real Redis's
  instance-wide `maxmemory`. A cross-db `COPY` into a mostly-empty
  destination db can undercount total shard pressure. Uniform, pre-existing
  behavior; fixing it is a cross-cutting change to the eviction model, not a
  `COPY`/`MOVE`-specific one — out of scope here.
- New wire-level regression suite `tests/spsc_two_db.rs` (3 cases, both
  runtimes) exercises the one arm real client traffic uses
  (`PipelineBatchSlotted`): pipelined cross-shard `COPY ... DB n` (RED
  before the fix — `MOVE`'s loud wrong error and `COPY`'s silent same-db
  copy — both confirmed via a git-stash re-run against the pre-fix source),
  pipelined cross-shard `MOVE`, and a same-db `COPY` control (routes through
  the coordinator's multi-key scatter path, unaffected by this fix, kept as
  a negative control). The `COPY` case uses `{i}`-hash-tagged src/dst key
  pairs so the destination key is independently `GET`-able post-fix — see
  the "destination hash-tag" caveat above; an untagged version of the same
  test only succeeds for the ~1-in-4 keys where `hash(dst)` collides with
  `hash(src)` by chance.
- New wire-level regression suite `tests/oom_bypass_closure.rs` (8 cases,
  both runtimes): direct-SET control (A), cross-shard pipeline (B), Lua EVAL
  loop (C), read-only EVAL under pressure not blocked (D), cross-shard COPY
  under pressure (E), `CONFIG SET maxmemory` atomic publish/un-publish (F),
  FCALL-internal write loop (G), FCALL control with no maxmemory (H).
  B, C, E and G RED before their respective fixes. D locks the write-only
  scope of the Lua gate. Case F (added with the atomic-snapshot perf
  follow-up) starts the server with `--maxmemory 0 --disk-offload disable`
  (both required so the atomic starts genuinely unset — omitting
  `--maxmemory` trips the auto-guardrail's nonzero default, and
  disk-offload's spill-sender presence ORs into the same gate) and drives a
  cross-shard pipeline before and after `CONFIG SET maxmemory`/`CONFIG SET
  maxmemory 0`; RED before the CONFIG SET call site published the atomic
  (775/3000 OOM — the local-only share) GREEN after (3000/3000). Case G
  (FCALL) RED before the FunctionRegistry fix — a 2000-iteration
  `redis.call('SET', ...)` loop inside an FCALL'd function completed with
  `'DONE'` instead of an OOM error against a 2MB-capped `noeviction` server;
  GREEN after. Case H locks the write-only/OOM-only scope of the FCALL gate,
  mirroring case D for EVAL. Case E was revised for the cross-shard SPSC
  intercept fix above: it now pre-loads db 1 (the `COPY` destination) with
  the same ballast as db 0 before the OOM phase, since Moon's per-database
  eviction gate (see the "found, not fixed" note above) needs the
  destination db under its OWN pressure to trip — the original version
  passed only as a side effect of the pre-fix same-db-copy bug (0/300 OOM
  with the destination-db gate disabled and routing intact, confirmed via a
  targeted stash of just that gate block; 76/300 fully fixed).

### Added — int8 symmetric ADC for SQ8 vector search (task #13)

- New per-candidate integer dot-product path for SQ8 asymmetric distance
  computation (ADC), replacing the f32-widening `sq8_stats` kernel on the
  hot per-candidate pass with exact-integer int8 dot products: NEON
  widen-multiply (`vmull_s8` + the `c^0x80` offset trick, since baseline
  ARMv8-A has no mixed u8×i8 widening multiply), AVX2 (`cvtepu8/cvtepi8_epi16`
  → `mullo_epi16` → `madd_epi16`, saturation-free), and AVX512 VNNI
  (`_mm512_dpbusd_epi32`, feature-gated behind `simd-avx512`). Query is
  quantized to symmetric int8 once per search
  (`turbo_quant::sq8::sq8_quantize_query_scalar`); `sum_c`/`sumsq_c` are
  exact integers straight from the u8 codes. Combines through the unchanged
  `sq8_l2_from_stats` — the on-disk SQ8 format and the exact-rerank sidecar
  (HQ-1) are untouched. Wired into both `hnsw/search.rs` beam-search closures
  and all three SQ8 call sites in `segment/mutable.rs` brute-force search.
  Local (dev-Mac, NEON) directional speedup vs the existing f32 SIMD path:
  ~1.5x at dim 128, ~2.0x at dim 384, ~2.15x at dim 768 (`benches/sq8_adc_bench.rs`,
  `int8_dispatch` variant) — absolute/cross-arch numbers are GCE-validation
  scope (task #14). Recall A/B-gated (`turbo_quant::sq8` test module):
  R@10 delta ≤ 0.01 vs the f32 ADC path and ≥ 0.98 top-10 overlap, both L2
  and Cosine, at dim 128/384. `MOON_SQ8_INT8_ADC=0` forces the f32 kernels
  (bench/diagnostic escape hatch, not yet added to CLAUDE.md's env list —
  flagged for a follow-up doc pass).

### Fixed — merge recall gate self-exclusion bias (manual merges could never pass)

- `verify_merge_recall` compared HNSW results **including the query point**
  (every sampled query is a database point, distance 0 → always rank 1)
  against a ground truth that **excluded** it, structurally capping measurable
  recall at (k−1)/k = 0.90 — exactly the manual `FT.COMPACT`/`VACUUM VECTOR`
  merge gate, so any manual merge of an index with ≥ 50 vectors was rejected
  with `merge recall 0.9000 < tolerance 0.9000` regardless of actual graph
  quality (the 0.70 background gate masked this). The merged-graph search now
  requests k+1 and drops the self-point before comparison.

### Added — vector-index durability: kill-9 crash-recovery test suite (B4)

- New `tests/crash_recovery_vector_durability.rs`: five end-to-end scenarios
  against real server processes (SIGKILL + restart): unchanged-key dedup
  fast path, update/delete reconcile, orphan sweep on boot, collection_id
  pin surviving a post-recovery compact + GraphUnion merge, and a
  no-persistence regression guard. All waits are bounded condition polls;
  servers run with `--disk-free-min-pct 0` so the suite is immune to the
  dev volume hovering at the 5% diskfull guard.

### Added — vector-index durability: startup recovery from disk (B1-B3)

- **Vector indexes now persist their segments across restarts** instead of
  paying a full re-index (TQ encode + HNSW build) of every matching hash key
  on every restart. Each index gets an `idx-<hex(name)>/` directory holding
  an atomically-written `manifest.json` (collection_id / segment ids /
  id-allocator floors), a checksummed `keymap-<epoch>.bin` (key_hash →
  global_id + vector checksum + original key), and its immutable HNSW
  segments (staged write: `staging-<id>` → fsync → atomic rename), written
  in the background after each compact/merge install (B1/B2).
- **Startup now loads that state instead of discarding it**: segments are
  read back and reattached (pinning the recreated index's `collection_id` to
  the persisted value — required for the HNSW QJL rotation seed to match),
  and the keyspace rescan is now a **dedup rescan**: a key whose vector
  bytes checksum-match the last snapshot is rebuilt as metadata-only (no
  HNSW/TQ re-encode); only genuinely changed or unknown keys are fully
  re-indexed; keys removed from the keyspace are tombstoned (B3).
- **Crash-safety contract**: any corrupt/missing artifact (segment,
  checksum mismatch, unreadable headers) degrades to a rescan-rebuild of
  exactly the affected keys, never to wrong search results; a manifest may
  understate what's durable (costs a rescan) but never overstates. Startup
  also sweeps orphaned segment/staging/keymap files and stale `idx-*`
  directories left by an interrupted drop.
- Multi-field indexes: only the default vector field's segments/checksum
  are persisted; additional named vector fields always re-encode on
  restart (documented gap, safe/conservative).
- Removed the vestigial WAL-replay vector recovery path
  (`src/vector/persistence/recovery.rs`, `VectorStore::attach_recovered`/
  `pending_segments`): it only ever populated a `VectorStore` that was
  discarded before the shard event loop started (recovery authority is now
  exclusively the manifest/segment/keymap layout above).

### Fixed — FLUSHALL/FLUSHDB ghost vectors + HDEL stale-vector gap

- **FLUSHALL/FLUSHDB never touched the FT indexes** (persistence-review R3):
  flushed hashes stayed searchable as ghost vectors/documents until restart.
  Both commands now clear every vector + text index's CONTENTS (segments,
  key-hash maps, postings, TAG/NUMERIC indexes) while KEEPING the FT.CREATE
  definitions — matching restart semantics, where definitions come from the
  sidecar and contents are re-derived from the (now empty) keyspace.
  Recovered-but-unclaimed `pending_segments` are discarded too, so a
  post-flush FT.CREATE cannot resurrect pre-flush contents.
- **`HDEL key <vector-field>` left the vector searchable** (R4): only
  whole-key DEL/UNLINK tombstoned. A successful HDEL that removes an
  index's vector field now tombstones that key in exactly the affected
  indexes (per-index `mark_deleted_for_key_in_index`; sibling indexes keyed
  on other fields keep their entries). Known follow-ups documented in
  `auto_hdel_vectors`: multi-vector-field indexes tombstone the whole doc,
  and TEXT/TAG/NUMERIC field removal is not yet re-indexed.
- Both hooks wired with wire-parity on ALL dispatch paths (single-shard,
  monoio conn-local, tokio sharded, SPSC execute, shared batch, MULTI/EXEC).
  New integration suite `tests/vector_flush_hdel_tombstone.rs` (red→green).

### Observability — exact-rerank sidecar coverage is visible and loud (R5)

- A GraphUnion merge with even one sidecar-less source segment silently
  dropped the exact-rerank f16 sidecar for the ENTIRE merged segment
  (all-or-nothing propagation — a partial sidecar would mix exact and ADC
  distances within one segment). The drop now logs a `tracing::warn` with
  source counts, and FT.INFO exposes additive `graph_segments` /
  `segments_with_exact_rerank` counters (merged across shards) so ADC-only
  segments are observable in the steady state.

### Performance — AE-1: saturation-gated per-segment adaptive ef

- With G graph segments, every segment searched at the FULL resolved ef —
  ~G× the CPU of one ef-wide beam (the root cause of the pooled regression
  on SMT-constrained boxes in PR #214's GCE run). Compact/GraphUnion builds
  now run a one-time self-probe against the exact f16-sidecar ground truth
  (leave-self-out R@10 across an ef ladder, `estimate_suggested_ef`): a
  segment whose curve is FULLY SATURATED from the minimum rung (flat at
  ≈1.0) is certified trivially easy and searches at min-ef (24); every
  other segment keeps the full resolved ef. Applied identically to the
  sync, serial-yielding, and worker-pool paths (pooled == serial identity
  holds); never active when the user pins `EF_RUNTIME`; in-memory only
  (reloaded segments fall back to the full beam).
- Two weaker designs were tried and REJECTED by recall A/B (20k×384d SQ8,
  5 segments): a blanket `ef/√G` split (EF-SPLIT) silently cost gaussian
  R@10 0.9915 → 0.9295 (its original "recall preserved" validation had
  exercised a stale binary), and letting the self-probe pick a mid-ladder
  knee cost 0.9915 → 0.939 — self-sampled queries are optimistically
  biased on unstructured data, so the probe's ONLY trustworthy verdict is
  "saturated at min-ef".
- Recall/latency gate (same seeds/harness end-to-end): clustered 5-seg
  R@10 1.0000 at p50 0.79 → 0.26 ms (~3×), clustered 1-seg 1.0000 → 0.9960
  (within the probe's ε=0.005 contract) at 1.78 → 1.46 ms; gaussian 5-seg
  0.9915 at ~0.79 ms and 1-seg 0.7710 — byte-identical to the pre-split
  baseline (probe self-rejects, full beam).

### Performance — FT.SEARCH per-query fixed-cost cleanup (QP-2/QP-3)

- **Thread-cached `SearchScratch`** (QP-3): the wire path built a fresh
  scratch per query — heaps, visited bitmap, and the 32–65KB ADC LUT
  reallocated every FT.SEARCH. Captures now take a thread-local recycled
  scratch (exact padded-dim match, mirroring the worker pool's cache) and
  the yielding search returns it on completion.
- **Amortized committed-treemap capture** (QP-2): every search cloned the
  MVCC committed `RoaringTreemap`; `TransactionManager::committed_snapshot()`
  now hands out a cached `Arc` refreshed only on the first capture after a
  commit/prune — read-heavy captures are one refcount bump, write-heavy is
  never worse than before. All 7 capture sites switched.
- **ryu score formatting**: RESP `__vec_score` replies formatted f32 via
  `Display` (grisu, 1.4% of a matched-recall query); now ryu.
- VM A/B (same box/config as the f16 kernel entry): ef64 8,468 → 8,810 QPS
  (+4.0%; +13% cumulative with the f16 kernels).

### Performance — SIMD f16 exact-rerank kernels (NEON + F16C)

- The HQ-1 exact-rerank pass decoded its f16 sidecar with scalar software
  `f16_to_f32` — 17% of a matched-recall (ef 64) query. New fused
  decode+distance kernels in the distance dispatch table (`f16_l2`,
  `f16_dot_normsq`): aarch64 uses a baseline-NEON integer-rescale decode
  (no ARMv8.2 FP16 intrinsics needed; exact for all finite f16, Inf/NaN
  bit-selected), x86_64 uses F16C `vcvtph2ps` + FMA (runtime-detected with
  AVX2, scalar fallback). Subnormal/Inf/NaN semantics match the scalar
  reference exactly (pinned by tests). VM A/B (20k×384d clustered SQ8,
  single conn, ef 64): 7,795 → 8,468 QPS (+8.6%).
- **Negative result, kept for the record**: skipping the rerank pass
  entirely for SQ8 was A/B'd and REFUTED — it costs real recall
  (R@10 −0.010 clustered, −0.017 gaussian 5-segment). The pass stays
  unconditional; the SIMD kernels make it cheap instead.

### Performance — FT.SEARCH intra-query worker pool + bounded bulk compaction

- **`--ft-search-workers N` worker pool** (`src/vector/search_pool.rs`): the
  per-segment HNSW searches of ONE KNN query fan out across a pool of searcher
  threads while the shard task scans the mutable segment; replies are awaited
  with `flume::recv_async`, so the shard event loop is never blocked. Default
  **0 (off, opt-in)** — each segment pays the full resolved ef, so a pooled
  N-segment query does ~N× the CPU work for its latency win: a 4.9× QPS win on
  physical-core-rich boxes but a measured regression on SMT-constrained ones
  (same posture as `--io-busy-poll-us`). Good opt-in size: physical cores −
  shards, cap 8. Results are identical pooled or serial (enforced by
  pooled-vs-serial identity + 8-thread concurrency stress tests); worker
  panics are contained per-job (empty segment result + warn, never a hang).
  Runtime-agnostic (monoio + tokio).
- **Bounded bulk compaction**: with a pool active and `COMPACT_THRESHOLD > 0`,
  one compaction build freezes at most `max(threshold, len/8)` entries
  (`MutableSegment::freeze_prefix`), so FT.COMPACT after a bulk load yields
  several independently searchable segments instead of one giant graph —
  bounded build memory and pool-parallel search. Pool-less deployments keep
  single-segment builds (multi-segment serial search would be strictly
  slower). Red/green: `test_force_compact_bulk_bounded_segments`,
  `test_bg_compact_bulk_bounded_segments`.
- Measured (20k×384d clustered SQ8, single connection, post-FT.COMPACT,
  R@10 = 1.0 in all rows): macOS 10-core p50 2.06 ms → **0.46 ms**, 473 →
  **2,321 QPS (4.9×)**; GCE c3-standard-8 (4 physical cores) regresses at
  default ef (1,732 → 1,165 QPS) — hence the opt-in default. The GCE probe
  matrix also showed the per-index `EF_RUNTIME` knob alone closes most of the
  vs-RediSearch clustered gap (ef 64: 4,493 QPS serial at R@10 = 1.0). Full
  GCE vs-RediSearch tables in PR #214.

### Fixed — update-churn no longer mass-deletes vectors at compact/merge install (soak-diagnostic find)

- **Compact install: dead window entries no longer kill their own update.**
  `snap_and_reconcile` treated ANY tombstoned entry in the frozen window as
  "key deleted" and applied a key_hash-wide tombstone to the new immutable —
  but since the VEC-1 update path tombstones the old copy in place, a key
  updated before the freeze had a dead old copy AND a live new copy in the
  same window, and the install deleted the new copy out of the segment.
  Every key updated-then-compacted silently vanished from FT.SEARCH (32% of
  live keys lost / live-set recall 0.985 → 0.685 in a 1-minute churn soak;
  regression vs v0.5.1). A dead window entry now only proves deletion when
  the key has no live window sibling.
- **Merge install: source tombstones are origin-gated.** The merge replay
  applied each source segment's lifetime interior tombstone set key_hash-wide
  to the merged output, so a key whose old copy was tombstoned-by-update in
  one source killed its current copy merged in from a sibling segment. The
  replay now only tombstones entries whose `global_id` originated in the
  tombstone's own source; DEL/UNLINK tombstones land in every source's set
  and still apply everywhere.
- **`VectorStore::insert_vector` allocates monotonic LSNs** (same allocator
  as the wire path) instead of `mutable.len()+1`, which restarted after every
  compaction and made merge dedup keep a stale copy over the current one.
- Red/green: `test_bg_compact_update_before_freeze_survives_install`,
  `test_bg_merge_update_across_segments_survives`; end-to-end churn repro
  (24k mixed ops): LOST 1054 → 0, live-set recall 0.685 → 0.98.

### Fixed — DEL/UNLINK now unindexes vectors on every dispatch path (soak-diagnostic find)

- **Deleted keys no longer resurface in FT.SEARCH** — the vector auto-delete
  hook (`mark_deleted_for_key`) existed only on the cross-shard SPSC `Execute`
  arm and the tokio sharded handler. The monoio conn-local path (the default
  runtime's only path at `--shards 1`), `handler_single`, the MULTI/EXEC batch
  paths, and the SPSC pipeline arms never tombstoned vectors on DEL/UNLINK, so
  deleted keys kept matching KNN searches forever (20% of results after one
  minute of mixed churn; live-set recall collapsed 0.985 → 0.735 in the
  Bundle-5 soak diagnostic). All paths now share one `auto_delete_vectors`
  parity helper; wire-level red/green coverage in `tests/vector_del_unindex.rs`.

### Added — long-run vector reliability harness

- `scripts/vector-validate.py` — on-target validation driver: recall/QPS
  comparison between two moon binaries (SQ8 + TQ4, ground-truth brute force),
  churn soak with live-set recall / RSS / resurrection sampling, and kill -9
  durability (settled-write survival + double-crash restart) under
  `appendonly yes`.
- `scripts/gcloud-vector-soak.sh` — GCE orchestration (c4a ARM + c3 x86):
  ships the working branch via `git bundle`, builds baseline + branch binaries,
  runs the validation driver, fetches JSON results, tears down on exit.
- `scripts/bench-vector-vs-redisearch.py` — Moon vs RediSearch head-to-head
  driver (same FT.* wire dialect for both engines): insert throughput, KNN-10
  QPS/p50/p99, and R@10 vs numpy ground truth on random-Gaussian and
  clustered-mixture 384d datasets, with an `EF_RUNTIME` sweep so QPS is
  compared at matched recall.

### Fixed — vector search correctness bundle (deep-review VEC-1/XC-SHARD-1/XC-3/VEC-4/VEC-7)

- **HSET update no longer duplicates a vector** — re-indexing an existing key
  tombstones the old copy first (O(1) fast path in the mutable segment via the
  key→global-id map, scan fallback across mutable + immutable segments), so KNN
  totals and results no longer count both the stale and the fresh vector.
- **FT.INFO is now cluster-wide at `--shards N`** — previously answered from the
  local shard only (~1/N of `num_docs`). Now scatter-gathers to every shard and
  merges additively (top-level counters + per-field stats), on both the sharded
  and monoio handlers.
- **Filtered FT.SEARCH on the cooperative-yield path honors `FilterStrategy`** —
  the yielding search always graph-filtered; the post-filter strategy (selective
  filters) now searches unfiltered with 3×k oversampling and bitmap post-filter,
  matching the non-yielding path's recall behavior.
- **`FT.CREATE ... MERGE_MODE KEEP_RAW` is rejected fail-loud** — it silently
  behaved as graph-union; now errors until the raw-vector sidecar is implemented
  (the separate `KEEP_RAW ON` flag is unchanged).
- **MEMORY DOCTOR / Prometheus KV memory no longer report 0 under unlimited
  `maxmemory`** — the per-shard KV memory publish on the 100ms eviction tick
  had been gated on `maxmemory > 0` since the GAP-1 elastic-budget work,
  permanently zeroing the `DashTable + entries` line for the default config.
  The publish (an O(1) accumulator read per DB) now runs unconditionally;
  elastic-budget recompute stays gated on a finite cap.

### Added

- **`FT.CONFIG SET/GET <index> MERGE_RECALL_TOLERANCE <0.0..=1.0>`** — per-index
  recall gate for unattended (background/vacuum) GraphUnion merges; default 0.70
  unchanged.

### Added — exact rerank stage (deep-review HQ-1)

- **FT.SEARCH distances on compacted segments are now (near-)exact.** Immutable
  segments carry an f16 sidecar of the original vectors (built at compaction,
  BFS-ordered); the top `4·k` beam candidates are re-scored with true metric
  distances (L2: squared L2; Cosine/InnerProduct: normalized-pair squared L2)
  before top-k truncation, replacing pure quantized ADC estimates — the
  recall lever vs engines that keep full-precision vectors. SQ8, previously
  ZERO-refinement, benefits most; TQ4 estimates improve from percent-level
  error to f16 tolerance (~1e-3).
- The sidecar persists (`raw_f16.bin` per segment dir, missing file = no
  sidecar, fully backward/forward compatible), survives GraphUnion merges
  (all-or-nothing propagation), and is MEMORY DOCTOR-accounted. Memory cost:
  +2·dim bytes per vector in both the mutable segment and compacted segments
  (384d ≈ +768 B/vector); an opt-out knob is a follow-up.

### Performance — SIMD SQ8 ADC kernels (deep-review HQ-2)

- **SQ8 asymmetric distance is now SIMD-dispatched** (NEON / AVX2+FMA /
  AVX-512F, scalar fallback) via an algebraic decomposition: per-query
  constants `(Σq, Σq²)` computed once, per-candidate work reduced to three
  fused widen-u8→f32 FMA sums `(Σq·c, Σc, Σc²)` combined in O(1). Wired into
  HNSW beam search and both mutable-segment brute-force paths. Criterion
  (aarch64 NEON): 2.7×/1.8×/1.6× faster per candidate at 128/384/768d.
  Note: the pre-AVX2 x86 scalar fallback is ~1.65× slower than the old naive
  loop (the decomposition only pays with SIMD); all supported targets
  (aarch64 NEON, x86-64 AVX2+) are wins.

### Performance — vector search hot-path quick wins

- Copy-on-write `Arc` key map (no full `HashMap` clone per snapshot), hoisted
  brute-force query prep, striped search metrics counters, stable aarch64
  `prfm` node prefetch, dead quantize removed from the insert path, SQ8
  `code_len` fix in compaction, and raw-buffer work skipped for SQ8 segments.
### Performance — coordinator local legs ride group commit under appendfsync=always (PR #TBD)

- The cross-shard coordinator's LOCAL-leg persist (co-located MSET/MSETNX and
  scattered-MSET local slices) awaited one fsync ack **per command**, each
  bounded by `--aof-fsync-timeout-ms` (default 2000ms) — a pipeline of
  coordinated writes stacked these serially into the 2000–3000ms `always`
  far-tail measured on Linux/GCE (c2d-standard-16, pd-ssd; see the v3-4
  bench notes — OrbStack/macOS fsync is near-free and does not reproduce it). Local legs now enqueue fire-and-forget (bounded
  backpressure, same contract as the remote SPSC legs) and the connection
  handler confirms them with **one** `fsync_barrier` on the local shard per
  pipeline batch, before responses are serialized — so `+OK` still implies
  confirmed durability, but a batch of N coordinated writes costs 1 awaited
  fsync instead of N. `everysec`/`no` behavior is unchanged. On barrier
  failure every affected response is replaced with `MOONERR AOF fsync`
  (never a false `+OK`).

### Fixed — BITOP/COPY/DEL/UNLINK coordinator local legs now persist (PR #TBD)

- The cross-shard coordinator's in-process legs for BITOP (dest write),
  COPY (dst write + TTL restore), and multi-key DEL/UNLINK (co-located
  fast path AND the scattered local slice) executed in memory but never
  reached the owning shard's AOF — deleted keys **resurrected** from
  their seed writes on restart, and BITOP/COPY results on the
  connection's own shard silently vanished (carried v3-4 follow-up;
  remote legs were always durable via MultiExecute). All four now
  persist through the same `persist_local_leg` group-commit path as
  MSET/MSETNX: synthesized over only locally-owned keys, skipped when
  nothing was written (DEL of missing keys), and confirmed by the
  batch-end fsync barrier under `appendfsync=always`.

### Fixed — cold-tier (disk offload) correctness & reliability (PR #TBD)

- **DEL/UNLINK/FLUSHALL now reach the cold tier** — deleting a key whose value
  had been offloaded to disk left its `ColdIndex` entry alive, so the next GET
  resurrected the deleted value from the `.mpf` heap file (`DEL` even returned
  0 for cold-only keys). `Database::remove`/`clear` now drop the cold-index
  entry (and queue file unlinks) alongside the hot entry, and `DEL`/`UNLINK`
  count cold-only keys correctly.
- **Expired cold reads reclaim their index entry** — a cold read that found an
  expired entry returned nothing but left the index entry + file refcount
  behind forever (nothing else ever reclaims them). The read path now
  distinguishes `Expired` from `Miss` and removes the index entry on expiry;
  transient I/O errors still leave the entry alone.
- **Spill files survive a crash at the directory level** — spill writes fsynced
  the file but never the directory, so after a power loss the (dir-fsynced)
  manifest could reference a heap file whose directory entry vanished. Both the
  batch (tmp+rename) and single-file spill paths now fsync `data/` after
  publishing the file.
- **Crash-orphaned heap files are swept at startup** — a crash between the
  spill write and the manifest commit left `heap-*.mpf`/`.tmp` files on disk
  that no manifest references (invisible to the cold index, leaked disk
  forever). Recovery now unlinks unregistered heap files once the manifest has
  opened successfully.

### Added

- **Spill-thread liveness metrics in `INFO persistence`** —
  `spill_batches_flushed`, `spill_completions_dropped`, and
  `spill_last_heartbeat_ms` (0 = never ran) expose a silently-dead spill
  thread, whose only prior symptom was an unbounded eviction backlog.

## [0.5.1] — 2026-07-04

### Fixed

- **AOF+WAL double-write eliminated for the common config (PR #211)** — new
  `--wal-kv-log auto|on|off` (default `auto`). At `--appendonly yes` every
  SPSC-executed KV write was logged to BOTH the per-shard AOF and the per-shard WAL
  (measured 2.7× file-byte / 4.1× device write amplification at `--shards 4`), yet
  startup recovery wipes WAL-replayed state and replays the AOF — the WAL copy was
  pure disk wear. `auto` skips WAL KV records while the AOF is the recovery
  authority and no CDC subscriber is attached (logging re-engages dynamically when
  one attaches); `on` restores the pre-0.6 always-log behavior (needed for PITR /
  full CDC history alongside AOF); `off` never logs KV records. FPI/checkpoint/
  feature records are unaffected.
- **everysec/no AOF appends are no longer silently dropped under backpressure
  (PR #211)** — a full writer channel used to `warn!` + drop the record while the
  client still received `+OK` (client-acked write loss + AOF/memory divergence on
  replay). Durable handler paths now await enqueue under `--aof-fsync-timeout-ms`
  and surface `ChannelFull`/`WriteFailed` as an error frame; the synchronous
  SPSC-drain and monoio inline-SET paths apply a bounded blocking send (ONE 5ms
  budget shared across a whole pipeline/MULTI batch) and, on loss, replace the
  success frame with `MOONERR AOF backpressure` instead of acking — every drop is
  `error!`-logged and counted. `--wal-kv-log` also rejects unknown values at parse
  time. Watch `aof_backpressure_dropped` in `INFO`.

- **Docker image build** — the multi-stage `Dockerfile` now copies the vendored
  `vendor/monoio` source into the cargo-chef planner and cook stages. The v0.5.0
  release introduced a `[patch.crates-io] monoio = { path = "vendor/monoio" }`
  path dependency, but the chef stages only copied `Cargo.toml`/`Cargo.lock`/`src`,
  so `cargo chef cook` panicked (`failed to load source for dependency monoio …
  vendor/monoio/Cargo.toml: No such file or directory`) and the `Docker Image`
  release job failed. Adds a standalone `docker-publish.yml` workflow to
  (re)publish only the container image for a tag whose other assets are already live.

## [0.5.0] — 2026-07-04

Two milestones since v0.4.1:

- **v3-4 KV Write Correctness & Data-Integrity Parity** — the primary KV engine now honors
  Redis's data-integrity contracts (no wrong-type data loss, no integer-overflow panics/wraps,
  past-expire deletes, atomic MSETNX).
- **p=1 & multi-shard throughput** — Moon now beats Redis on non-pipelined (p=1) GET/SET on both
  GCE arches (ARM 1.19–1.21×, x86 1.65–1.66×), and multi-shard wins from 8 concurrent connections
  up (s4 c8 1.57–2.0×, c64 2.50×), via a poll-mode park (`--io-busy-poll-us`, backed by a vendored
  monoio fork) plus a cross-shard reply-path convoy fix. All perf gains are bench-only knobs /
  opt-in flags — the default runtime behavior is unchanged unless you set `--io-busy-poll-us`.

### Added

- **MSETNX** — atomic multi-key string write (set all pairs iff none of the keys exist; returns 1/0).
  Single-shard atomic (two-phase check-then-set, no await between phases). The cross-shard coordinator
  rejects a key span with `CROSSSLOT` (by design — no two-phase commit) and runs co-located `{hash-tag}`
  keys atomically on the owning shard. New `--shards 4` regression suite `tests/msetnx_cross_shard_reject.rs`.
- **`--io-busy-poll-us <µs>`** — poll-mode park for the monoio legacy (epoll/kqueue) driver: the shard
  thread busy-polls readiness for the given budget before blocking, deleting the per-op scheduler
  sleep+wake. This is the lever that flips non-pipelined (p=1) GET/SET to a win vs Redis on both GCE
  arches. Defaults to `0` (off); implies `--io-driver epoll`. Costs up to budget-µs CPU per idle park —
  only a win on pinned, disjoint cores (a regression on shared-core hosts). Env equivalent
  `MOON_EPOLL_SPIN_US`.
- **`--io-driver <auto|epoll>`** for the monoio runtime — force the epoll/kqueue LegacyDriver instead of
  io_uring (some platforms, e.g. GCE ARM Axion, run KV faster on epoll).
- **Per-use-case tuning guide** at `docs/guides/tuning.md` (quick recipes, shard-count guidance,
  busy-poll trade-offs, persistence cost, platform notes), linked from the docs-site Operations nav.

### Performance

- **Multi-shard now wins from 8 concurrent connections up, even without pipelining.** Fixed the C2
  reply-spin *convoy*: the cross-shard reply busy-poll was synchronous on the shard thread and, at
  ≥2 connections per shard, a spinning connection starved its sibling and the shard's SPSC drain — the
  root cause of the `--shards 4` c8-P1 0.45× collapse. A solo-conn gate now spins only when a connection
  is alone on its shard. Result (s4, P1, `--io-busy-poll-us 40`, vs Redis): c8 GET/SET ARM 1.57/1.71×,
  x86 1.9/2.0×; c64 2.50× both arches. (c1-P1 remains the structural single-hop ceiling, 0.91–0.96×.)
- **monoio cross-shard replies unified on the zero-allocation `ResponseSlotPool`** (L3b), removing the
  per-op flume-oneshot allocation and chunked park from the hot reply path (+11–14% at c8).
- **Non-pipelined GET reply framed from a borrow** — dropped a per-GET `Vec` copy.
- Stripped four per-op lock/atomic costs from the p=1 dispatch hot path.
- Tuned the default monoio io_uring driver (`COOP_TASKRUN | SINGLE_ISSUER | DEFER_TASKRUN`) on Linux.

### Changed

- Corrected default-config documentation: `--shards` default is **1** (not "0/auto") and `--appendonly`
  default is **yes** (not "no") — both config pages were wrong.

### Fixed

- **Cross-shard reply use-after-free on panic-unwind (memory safety).** The cross-shard reply path
  sent a raw `*const ResponseSlot` into a pool living on the connection task's stack; a panic unwinding
  through the reply dispatch/drain would drop that pool while a target shard still held the pointer,
  making the target's later `slot.fill()` a use-after-free (heap corruption). `ResponseSlotPtr` now
  carries an `Arc<ResponseSlot>`, so the slot outlives whichever of {connection pool, in-flight message}
  drops last — structurally closing the window on both dispatch and drain. This also **retroactively
  fixes the same latent hazard on the tokio runtime** (shipped since v0.4.0) and removes four `unsafe`
  blocks (net unsafe reduction). Follow-up: a shutdown-aware bound on the reply await (now safe to add)
  to close a partial-shutdown liveness hang.
- **`MOON_NO_URING=1` now actually switches the monoio driver.** It was a silent no-op for the monoio
  FusionDriver (io_uring was selected regardless); it now forces the epoll/kqueue LegacyDriver, matching
  its documented contract and the new `--io-driver epoll`.

- **KV data-integrity (P0):** wrong-type `GETDEL` / `GETSET` / `SET … GET` no longer silently delete or
  overwrite a key holding a non-string — they return `WRONGTYPE` and preserve the value (check-before-mutate).
- **Integer overflow (P0):** `DECRBY key -9223372036854775808` no longer panics; every expiry-setting
  command (`SET EX/PX/EXAT`, `SETEX`, `PSETEX`, `EXPIRE`/`EXPIREAT`/`PEXPIRE`) now rejects a time whose
  absolute expiry falls outside the `i64` millisecond domain with an "invalid expire time" error —
  matching Redis (`when > LLONG_MAX/1000`). This closes a latent wrap where an accepted-but-huge TTL
  (e.g. `EXPIRE k 15000000000000000`) surfaced as a **negative** `PTTL`/`PEXPIRETIME` on a live key, and
  makes an extreme-negative `EXPIRE`/`EXPIREAT` (`< i64::MIN/1000`) error instead of deleting.
- **Expire-in-the-past semantics (P0):** `EXPIRE`/`PEXPIRE`/`EXPIREAT` with a non-positive or already-past
  time now deletes the key and returns 1 (Redis parity); verified to propagate through command-based WAL
  replay so replicas stay consistent.
- **Cross-shard coordinator local-leg durability (P0, pre-existing):** a co-located `MSET`/`MSETNX` whose
  keys hash to the **connection's own shard** now appends to that shard's AOF. The coordinator's local
  leg previously executed the write in memory but never persisted it (the remote `MultiExecute` leg
  always did), so with `--shards >1 --appendonly yes` an own-shard co-located `MSET`/`MSETNX` could be
  lost on crash. It now persists via the same append path every local single-key write uses — the whole
  command for a co-located owner, and a synthesized `MSET` over **only the local keys** for a scattered
  `MSET`'s local slice — returning an AOF error instead of a false `+OK` on append failure.
  Crash-recovery verified on both monoio and tokio (`tests/coordinator_local_leg_durability.rs`).
  Single-shard (the default) was never affected. The same local-leg gap remains for coordinator
  `BITOP` / `COPY` / `DEL` / `UNLINK` (tracked follow-up).

### Dependencies

- **Vendored `monoio` 0.2.4** (`vendor/monoio`, wired via `[patch.crates-io]`; upstream git sha
  `f7827ddd`, recorded in `vendor/monoio/.cargo_vcs_info.json`). A fork of upstream monoio 0.2.4 with a
  bounded "moon patch" (marked `// moon patch`, confined to 4 files — `lib.rs`, `driver/mod.rs`,
  `driver/uring/mod.rs`, `driver/legacy/mod.rs` — verified against pristine upstream) that adds the
  env/CLI-gated poll-mode park behind `--io-busy-poll-us` and a per-thread spin-hook handshake. Adds
  **zero new `unsafe`**; the dependency list is byte-identical to upstream (no added transitive deps);
  behavior is byte-identical to upstream when the `MOON_*` spin knobs are unset. Introduced to enable
  the p=1 poll-mode park.

## [0.4.1] — 2026-06-23

Measure-only validation release — **no server behavior change**. Bundles the closed
milestone **Cross-Shard Read Absolute Validation** (v2-2): bare-metal GCloud proof that
the v0.4.0 cross-shard read C2 fast-path performs as claimed, plus a reusable
absolute-latency benchmark harness and a data-backed decision to not pursue further
cross-shard-read optimization.

### Added

- `scripts/gcloud-xshard-absolute.sh` — reusable dual-vendor (Intel c3 + AMD c2d),
  dual-runtime (monoio + tokio) absolute cross-shard latency harness: best-of-5 RPS floor,
  fail-closed validity gates (load / steal / clean / s1-LOCAL-control), per-run
  provision + teardown, and an `--self-test` that exercises the gates with no cloud cost.

### Validated

- The shipped C2 cross-shard read fast-path, measured **absolute on bare-metal** across
  four instruments (Intel + AMD × monoio + tokio): C2 recovers **38–49%** of the
  SPSC-migration regression, leaving a vendor/runtime-converging **~10µs (±1µs) structural
  residual** — one irreducible cross-thread reply hop. monoio is the high-confidence set
  (tokio is rep-bimodal; best-of-5 floor clean). Disposition: **close-the-line** —
  coalescing cannot help the singleton path (the concurrent guard is already flat) and
  RCU's per-shard RSS cost is unjustified by a 10µs gap. Mechanism asserted byte-identical
  (`tests/xshard_mechanism_unchanged.rs`); `git diff --stat src/` empty.

### Risk-accepted (shipped, disclosed)

- The shardslice cross-shard-read fast-path removal waiver (PR #175, owner Tin Dang,
  expires 2026-08-01) rides into this release. **This release validates it**: the regression
  is quantified and the C2 recovery confirmed on bare-metal, so the waiver is now retirable
  and its follow-up task (`cross-shard-read-acceleration`) is closed as "no further work
  warranted."

## [0.4.0] — 2026-06-22

Bundles 5 closed milestones (first ADD-recorded cut; see `RELEASES.md` for
attribution): **Shared-Nothing Integrity & Cross-Shard Latency** (3 carried) ·
**Multi-Core Throughput Hardening** (12 carried) · **Throughput Polish**
(3 carried) · **FTS Hardening** (8 carried) · **Graph Correctness & Cypher
Filtering**. Headlines: working FTS query combinators (OR / TEXT+TAG / NUMERIC)
with true total-matched counts and the O(M²) high-DF cliff gone; correct Cypher
inline-property narrowing, incoming-edge traversal after compaction, and >32
graph labels; WAL group-commit, cross-shard read fast-path, and off-event-loop
FT.SEARCH throughput wins. Per-change detail below.

### Risk-accepted (shipped, disclosed) — cross-shard read fast-path removed (PR #175)

The shared-nothing migration (PR #175) deleted the direct-`RwLock` cross-shard
read fast-path; cross-shard reads now route through the owner shard's SPSC ring,
so cross-shard-read cells regress versus the pre-migration lock-read. This is a
**signed RISK-ACCEPTED waiver** (owner: Tin Dang) carried into this release, with
a follow-up task **cross-shard-read-acceleration** (lock-free cross-shard read
acceleration) tracked to land before **2026-08-01**. No correctness impact —
consistency is 197/197 across 1/4/12 shards on both runtimes; the cost is latency
on the cross-shard read path only.

### Docs — Verified cross-arch GCloud benchmark confirms v3-1 FTS + v3-2 graph fixes (PR #194)

Re-ran the 4-feature concurrent-vs-competitor benchmark (KV / Vector / Graph / FTS
vs Redis / RediSearch / FalkorDB) on GCloud x86 `c3-standard-8` (Sapphire Rapids) +
ARM `t2a-standard-8` (Neoverse-N1) against build `8238515`, with the graph harness
corrected to filter on the node `id` **property value** (not the internal
`GRAPH.ADDNODE` handle that produced a false `cypher_match_rows=0` in the prior
re-baseline). Proves **in-harness** that v3-2's inline filter narrows Cypher
point-queries (`cypher_match_rows` 14991 → **4**, identical to FalkorDB; cypher_1hop
~28–30× faster than its old full-scan self) and that v3-1's FTS hardening makes every
query combinator return RediSearch-exact total counts (OR 2072, TAG 5064, TEXT+TAG
253, NUMERIC 10119) while indexing ~40× faster (376 → 15052 docs/s) with the O(M²)
high-DF cliff gone (419 ms → 33 ms). KV and Vector unchanged (no regression). Folded
as layered BENCHMARK.md §2.9 / §10.6 / §11.5 / §12.3 over the 2026-06-16 record;
full report + corrected harness + raw logs in `docs/reviews/2026-06-17/`.

### Docs — Wider cross-arch benchmark: shard scaling (1/4/12) × structures × pipeline/datasize × production (PR #194)

Ran the repo-canonical `scripts/bench-compare.sh` (all-commands × pipeline 1–128 ×
datasize 8B–64KB × connections) at `--shards 1/4/12` plus `scripts/bench-production.sh`
(10 scenarios at shards 1 and 12) on GCloud x86 `c3-standard-8` + ARM `t2a-standard-8`,
Moon `8238515` vs Redis 7.0.15. Quantifies the core scaling claim honestly: Moon's
advantage is **pipeline depth, not shard count** (break-even p=16; GET p=128 ~2.8×, SET
p=64 ~1.85×), every core data structure tracks GET/SET at p=1 (~0.79×, TCP-RTT bound),
and **adding shards hurts uniform single-key non-pipelined throughput** (12 shards →
0.46–0.51× Redis vs ~0.79× at 1 shard). Folded as BENCHMARK.md §4.5 (structure coverage)
+ §6.4 (cross-arch shard scaling); full detail + logs in
`docs/reviews/2026-06-17/WIDER-BENCH.md`. The run also patched a latent unfairness in the
bench scripts (they start Redis AOF-off but Moon AOF-on, flooding ~1M WARN lines).

Follow-up root cause for the `shards=12, pipeline=1` worst case: a controlled single-VM
A/B (`docs/reviews/2026-06-22/`) isolates it as a three-factor interaction — cross-shard
dispatch (2 cross-thread wakes/op) × `p=1` (no batch amortization) × random keys (defeat
the ≥62.5% `AffinityTracker` migration, so the connection never goes local). The collapse
is confined to the `s12 × random` cell (GET 0.47× / SET 0.41×); `s1 random` is 0.95×/1.15×
and `s12 single` self-heals to 0.74×/0.73×. WIDER-BENCH.md §4a + `XSHARD-P1-ROOTCAUSE.md`.

### Fixed — bench-compare.sh / bench-production.sh start moon AOF-off to match Redis (fair, no flood) (PR #194)

Both benchmark scripts started Redis in-memory (`--save "" --appendonly no`) but moon with
persistence defaults (AOF on). Under SET load moon's AOF channel saturated, flooding
`AOF append dropped … channel full` WARN logs (~1M lines, which also pinned the port so
later shard configs failed to bind) and unfairly handicapping moon against a no-AOF Redis.
Both scripts now start moon with `--appendonly no --disk-offload disable` (and redirect its
stdout/stderr), so the comparison is AOF-off on both sides and the report stays clean.

### Fixed — Graph node labels with id ≥ 32 are stored, matched, and persisted (no silent truncation) (PR #193)

CSR node labels were packed into a 32-bit `NodeMeta::label_bitmap`, so any label
id ≥ 32 was silently dropped at `from_frozen` (`if label < 32`) — `MATCH (a:Label40)`
matched nothing and a graph using more than 32 distinct labels lost the rest. Labels
0–31 keep the u32 bitmap fast path; ids ≥ 32 now live in a sparse, per-row
`label_overflow` store, and `LabelIndex` is built from both sources, so every `u16`
label id is matchable. The on-disk CSR format is bumped to **version 4** with a new
version-gated trailing section (`[entry_count][ (row, label_count, label_id…) ]`,
ascending by row, covered by the existing CRC); the `GraphSegmentHeader` gains a
`label_overflow_offset` field reusing existing padding (on-disk header size
unchanged). Both the heap (`from_bytes`) and mmap (`from_mmap_file`) loaders parse
the section into an owned map via one shared bounds-checked parser (returns
`CsrError` on truncation, never panics; transitively fuzzed by `csr_from_bytes`),
and `compact_segments` re-keys overflow labels to each surviving node's new merged
row. Backward-compatible: version ≤ 3 segments load with an empty overflow store and
behave exactly as before; `NodeMeta`'s 48-byte layout is unchanged.

### Fixed — Graph Incoming / Both traversals return incoming edges after compaction (PR #193)

A compacted graph stores edges only in immutable CSR segments, which keep just
OUTGOING adjacency. `SegmentMergeReader` therefore `continue`d past the CSR for
`Direction::Incoming` (returning nothing) and silently dropped the incoming half
for `Direction::Both` — so `MATCH (a)<-[]-(b)` / undirected traversals over
compacted data missed every predecessor. The reader now serves predecessors from
a derived reverse index (`IncomingIndex`) built once per segment from the existing
forward arrays (`row_offsets` + `col_indices`) and cached in a non-persisted
`OnceLock` — no segment format/version change, so on-disk heap and mmap segments
answer incoming queries unchanged. Incoming edges reuse the same `edge_meta` /
`edge_created_ms` / validity / node-visibility by edge index, so the edge-type
filter, tombstones, decay stamp, deleted-node exclusion, snapshot rules, and
NodeKey dedup are identical to the outgoing path. `Direction::Outgoing` is
unchanged. MemGraph already handled all three directions; this fixes the
immutable-CSR path only.

### Fixed — Cypher MATCH narrows on inline node-property predicates instead of full-scanning the label (PR #193)

`compile_match` built each pattern node's `NodeScan`/`Expand` but silently
dropped the node's inline properties `(v {k:e, …})`, so
`MATCH (a:Person {id:1})-[]->(b)` ignored `{id:1}` and returned every edge of the
`Person` label (≈|E| rows) instead of node 1's out-edges. The planner now emits a
`Filter` — the equality conjunction `v.k = e AND …` built by the existing
`properties_to_filter` — immediately after the op that binds each
inline-propertied node: after its `NodeScan` for the first node, after the
`Expand` that produces any subsequent node. It reuses the existing `Filter`/`Expr`
evaluation, so parameters (`{id:$p}`), cross-type compares, and missing-property
⇒ excluded all follow the same semantics as `WHERE`; an empty `{}` pattern emits
no Filter, leaving plain label/all scans byte-identical. No new `PhysicalOp`, no
index, no executor change.

### Fixed — FT.SEARCH routing: prose "knn" searches as text, standalone SPARSE reaches the vector engine, no panic on the BM25 AND path (PR #192)

`is_text_query` uppercased the query and matched the bare substring `KNN `, so a
text search whose terms merely contained the word *knn* (e.g.
`FT.SEARCH idx "knn tutorial"`) was misclassified as a vector query and fell to
the KNN parser, returning `ERR invalid KNN query syntax` instead of text
results. It now keys on the canonical `[KNN` vector-query bracket — prose
containing "knn" searches as text while `*=>[KNN …]` still routes to the vector
engine. A standalone `SPARSE @field $param` clause paired with a text-looking
query string was silently dropped onto the text path (`is_text_query` only sees
`args[1]`); a `has_sparse_clause` guard now defers any SPARSE-carrying query to
the vector engine at every text-route gate. The three `.expect("posting exists")`
on `TextStore::search_field`'s BM25 AND + scoring path are replaced with
defensive control flow, so a vanished posting yields empty results instead of
panicking the server. Output is byte-identical for all existing queries.

### Fixed — FT.SEARCH integer reply is the true total-matched, not the page size (PR #192)

The first element of an `FT.SEARCH` reply (the match count) was capped at the
`LIMIT`/`top_k` page size: `FT.SEARCH idx "term" LIMIT 0 5` over 100 matches
reported `5`, not `100`, because the count was read from the already-truncated
result page. On multi-shard indexes the coordinator compounded it by counting the
merged-and-truncated returned docs rather than each shard's true matched count.
The reply now reports the true number of matched, key-resolvable documents
(RediSearch semantics): the evaluator surfaces the count before truncation, and
the multi-shard merge sums each shard's local matched count (keys partition to
exactly one shard, so the sum is exact; errored shards contribute 0). Verified
identical on 1- and 4-shard servers. Returned document pages (count, order,
scores, keys) are unchanged.

### Performance — FT.SEARCH upsert / bulk re-index no longer O(V) per document (PR #192)

Re-indexing a document (an HSET upsert, or a bulk re-index pass) called
`PostingStore::remove_doc`, which scanned EVERY term's posting list to clear one
document — O(total-vocabulary) per doc — so per-upsert cost grew with the corpus
(the indexing-rate cliff: ~376 vs ~18,052 docs/s as vocabulary V grew).
`PostingStore` now keeps a reverse `doc_id → term_ids` index, populated once per
(doc, term) edge as terms are added, so `remove_doc` visits only the terms the
document actually contributed — O(terms-in-doc), independent of V. Search output
is byte-identical (the rank-aligned posting contract is unchanged): matched doc
sets, BM25 scores, `doc_freq`, `num_docs`, and avgdl are unaffected. A missing or
already-cleared reverse entry is skipped defensively — never an unwrap/panic.

### Fixed — FT.SEARCH `OR` and multi-clause queries return correct result sets (PR #190)

`FT.SEARCH` query combinators were silently broken: `OR` (`alpha | beta`)
collapsed to an intersection (returning only docs matching *both* terms), and a
multi-clause query such as `@body:foo @tag:{bar}` returned **zero** results
instead of the intersection. A code trace showed the root cause was a missing
parser layer — four runtime handlers each re-parsed the query inline with a
flat, AND-only shape that could not represent an `OR`/grouped AST, and the
cross-shard scatter carried that same flat shape so `OR` was unfixable at the
leaf. This lands a recursive-descent query parser producing a frozen
`QueryNode` AST and one centralized evaluator (`eval_set` folds the AST to a
`RoaringBitmap` — `AND` = intersection, `OR` = union — over the shared doc-id
space; `eval_query` adds best-effort BM25 with deterministic score-desc /
doc-id-asc ordering). All four FT.SEARCH text branches plus the cross-shard
Phase-2 scatter now route raw query bytes through `parse_query → eval_query →
build_text_response`; the cross-shard payload carries the opaque query bytes and
each shard re-parses, so `OR`/grouping/`TEXT+TAG`/`TEXT+NUMERIC` work in every
shard configuration. Malformed queries return a coded `Frame::Error` (one of
five frozen codes) and never panic the server. Verified end-to-end over the wire
on 1- and 4-shard servers (`alpha | beta` → union, `@body:foo @tag:{bar}` →
intersection, 1-shard vs 4-shard result sets identical). `HYBRID`/vector-KNN/
`SPARSE`/`SESSION`/`RANGE` dispatch is untouched.

### Performance — high-DF FT.SEARCH term queries no longer O(M²) (PR #190)

The per-document term-frequency lookup on the BM25 path was an O(N)
`.position(|id| id == doc_id)` linear scan over a posting list, so a query on a
high-document-frequency term degraded to O(M²) (a ~5%-of-corpus term took
~419 ms on a 100K-doc index). `PostingList` now keeps `term_freqs`/`positions`
in sorted-doc-id (rank) order aligned with the doc-id `RoaringBitmap`, and
`tf()`/`positions_for()` resolve via `RoaringBitmap::rank` (sub-linear). The
rank alignment also fixed a latent BM25 corruption where re-indexing an updated
low-id document pushed its term frequency out of position, silently misaligning
scores.

### Performance — monoio FT.SEARCH yield is now cost-free; brute-force knee raised to 1024 (PR #189)

PR #179's monoio cooperative yield reaped the io_uring completion queue by
parking on `sleep(ZERO)` — correct, but ~1746 µs per yield, which is what forced
#179 to keep the brute-force chunk small and defer ~22% of transient throughput.
The yield now reaps the CQ by parking on a pre-armed `UnixStream::pair` read
(~0.317 µs — effectively free), so the brute-force chunk knee walks back up from
256 to **1024** elements per yield without reintroducing the co-located latency
#179 fixed. The knee was A/B confirmed per-architecture on GCloud: K=512 breached
the 5% latency budget on x86 (+6–8%) but held on aarch64, so 1024 was chosen as
the value safe on both. #179's co-located p99 relief is fully preserved — this
reclaims only the throughput #179 had to trade away. tokio is unaffected (it
already used `yield_now()`). Companion benchmark/review docs land alongside:
`BENCHMARK.md` §2.8 (GCloud cross-arch re-measurement) and a 4-feature deep
review + concurrent-vs-competitor benchmark under `docs/reviews/2026-06-16/`.

### Performance — FT.SEARCH no longer stalls the shard event loop (PR #179)

A heavy `FT.SEARCH` (large brute-force mutable segment or deep HNSW traversal)
used to run fully synchronously on its shard's event loop, blocking the 1ms
tick and every co-located command on that shard until the search returned.
The per-shard local search slice now captures an owned, point-in-time snapshot
of the index at entry and walks the same logical steps cooperatively, yielding
to the event loop between bounded chunks so the tick fires and co-located
`PING`/`GET` are serviced while the search is in flight. Results are
byte-identical to the synchronous path (the mutable segment is append-only, so
a chunked scan over the captured length matches an atomic scan), MVCC snapshot
isolation is preserved, and there is no new cross-thread lock and no
steady-state RSS growth. The cooperative yield is runtime-specific: tokio uses
`yield_now()`; monoio uses a zero-duration timer park, because a bare self-wake
never lets monoio's io_uring loop reap the completion queue (it would be a
silent no-op). Measured co-located `PING` p99 during a heavy search drops from
~48 ms (1 client) / ~300 ms (3 clients) to **6.6 ms / 27 ms** — roughly 7–11×
relief — on both runtimes. The yield costs throughput only on a large
*uncompacted* brute-force scan (operator-tunable via `MOON_FT_YIELD_CHUNK`);
light and HNSW searches whose work is under one chunk never yield and pay
nothing. The cross-shard scatter-gather, merge/rerank, and result semantics are
unchanged.

### Performance — WAL group commit under `appendfsync=always` (PR #178)

Concurrent writes pending at the same shard now coalesce into a single
`fsync` instead of one sync per write, collapsing a large share of the
~11× `appendfsync=always` throughput penalty that appears when multiple
clients write in parallel. The batching is opportunistic — a writer drains
every `AppendSync` already queued on its shard channel and issues one
barrier `fsync` for the whole group — and is wired into all four AOF writer
loops (`{TopLevel, PerShard}` × `{monoio sync, tokio async}`). Durability is
unchanged: control records route to a separate deferred queue so a batch can
never straddle a non-data message, and the exactly-once-under-crash
invariant (crash-matrix + SIGKILL integration tests) holds on both runtimes.
The absolute throughput gain is disk-dependent — structurally unmeasurable on
the near-free virtio `fsync` of the dev VM, so the coalescing mechanism (K
`AppendSync` → 1 `fsync`) is pinned by a deterministic batching seam test and
the wall-clock magnitude is deferred to real-disk hardware.

### Removed (BREAKING) — `--cross-shard-fast-path` flag and its dead telemetry

The orphaned `--cross-shard-fast-path` CLI flag (with its `CrossShardFastPath`
config enum and `cross_shard_fast_path_enabled()`) is deleted, along with the
dead `moon_cross_shard_lock_contention_total` metric, the
`moon_dispatch_cross_read_fastpath_latency_us` histogram, the
`record_dispatch_cross_read_fastpath_*` recorders, and the hardcoded-`0`
`cross_read_fast_dispatches` stat. These were Phase-0 scaffolding for the
pre-shared-nothing RwLock read path (gone since PR #175) and had zero
production callers. **Breaking:** `moon --cross-shard-fast-path …` now exits
non-zero with a clap unknown-argument error — remove it from any launch
script. The internal `scripts/bench-cross-shard-fastpath.sh` is also deleted.

### Performance — Lock-free cross-shard read recovery (idle-gated reply spin) (PR #177)

Cross-shard single-key reads recover a meaningful share of the latency the
shared-nothing migration (PR #175) gave up, **without** re-introducing a
cross-thread lock or growing per-key memory. When a shard is near-idle the
requesting connection briefly busy-polls its cross-shard reply instead of
parking — skipping one of the round-trip's two cross-thread wakes. The poll is
gated on a thread-local in-flight counter (`Cell<u32>`, no atomic/lock), so a
busy shard never spins and high-concurrency throughput is unaffected. Same-run
quiesced-VM measurement (monoio, 4 shards): single-client cross-shard GET
**+18.0%** (and cross-shard SET +21%, sharing the reply path); c100 GET +8.4%
with no starvation. Consistency 197/197 across 1/4/12 shards; dual-runtime
(monoio + tokio). Cross-connection read *coalescing* — the second half of the
original design — is deferred to a dedicated follow-up.

### Changed — Shared-nothing thread-local shard storage (PR #175)

Shard storage moved from `RwLock`/`Mutex`-wrapped shared databases to
thread-local **ShardSlices** owned exclusively by each shard's event
loop, on both runtimes (monoio and tokio). The lock wrappers are
deleted; all cross-shard access routes through the owner shard's SPSC
ring. Routed multi-shard workloads are parity or better (up to +12% on
pipelined GET at 4 shards); the cross-shard *read fast-path* (direct
RwLock read of another shard's data) is definitionally gone and those
cells regress — accepted with a follow-up task for lock-free cross-shard
read acceleration. BGREWRITEAOF on every layout now uses the C4
cooperative-fold protocol: the AOF writer requests an atomic snapshot
from the owning shard and drains pre-snapshot appends with an exact
bound, so concurrent writes are neither lost nor double-applied. This
also fixes BGREWRITEAOF being silently inoperative on the top-level AOF
layout (`--shards 1`), where the rewrite previously errored internally
while the client was told it had started.

### Added — HYBRID FT.SEARCH `FILTER` push-down on both branches (PR #174)

`FT.SEARCH ... HYBRID` now accepts an optional `FILTER` clause that is
applied to **both** the BM25 and the dense-KNN streams *before* RRF
fusion, closing a bypass where a filtered hybrid query leaked
foreign-field hits through the unfiltered dense branch. The filter is a
recursive, arity-counted grammar — `TAG @field value` (exact, or prefix
when `value` ends in `*`), `NUMERIC @field min max` (inclusive range),
and `AND`/`OR` combinators — bounded at depth 4 / 16 leaves and parsed
without panicking on malformed input. Filtering is per-shard on the
scatter/gather path, so multi-shard results stay correct. Absent a
`FILTER` clause the wire format and behavior are byte-identical to
before (fully backward compatible). Exposed through the Rust SDK as
`TextClient::hybrid_search(..., filter: Option<&HybridFilter>)`.

### Fixed — Cross-shard commands now route to the owning shard (PR #173)

On multi-shard servers, SO_REUSEPORT spreads client connections across
shard accept loops — and several command families operated on the
*connection's* shard instead of the shard that owns the data. Whether
these commands worked depended on which shard the kernel happened to
accept the connection on. All four groups are fixed; the consistency
suite now passes 197/197 at 1, 4, and 12 shards:

- **BITOP** routed by its sub-operation literal (`AND`/`OR`/…): sources
  were read and the destination written on an arbitrary shard. A new
  coordinator gathers sources per owning shard and writes the result on
  the destination's owner (Redis semantics: NOT arity, zero-padding,
  all-missing sources delete the destination and return 0).
- **COPY** wrote the destination into the *source's* shard, making it
  unreadable at its own. Cross-shard string copies preserve value + TTL
  and honor REPLACE/NX; cross-shard non-string COPY returns an explicit
  error instead of corrupting state.
- **GRAPH.\*, TEMPORAL.INVALIDATE, and TXN.ABORT graph rollback** ran on
  the connection's per-shard graph store — graphs randomly "did not
  exist" from ~3/4 of connections at 4 shards. Graph commands now hop to
  the graph-name-owning shard; transactional rollback partitions undo
  work by owner and awaits acknowledgements.
- **WS.\* and MQ.\***: the workspace registry was per-shard (WS AUTH
  from another connection answered "workspace not found"; WS LIST
  diverged per connection) and durable queues lived on the creating
  connection's shard (MQ PUSH/POP/ACK/DLQLEN from elsewhere answered
  "queue is not durable"). The workspace registry is now global with a
  single WAL stream, and every MQ operation targets the queue key's
  owning shard — including dead-letter routing, trigger registration,
  and MQ.PUBLISH materialization at TXN.COMMIT.

### Changed — Event-driven cross-shard wake (the ~1ms monoio floor is gone)

- **The monoio shard event loop is now event-driven.** Its single await point
  was the 1ms periodic tick, so every cross-shard hop queued up to 1ms on the
  target shard plus up to 1ms on the origin's reply sweep. The loop now races
  the tick against the shard's SPSC `Notify` (hand-rolled allocation-free
  `race2` future — `monoio::select!` remains banned), and connection tasks
  await cross-shard replies directly. Cadence work (WAL flush, cached clock,
  snapshot/auto-save sub-timers) stays pinned to the timer arm. Measured on
  a 4-shard Linux server: single-client cross-shard SET p99 4.07 ms → 0.071 ms
  (57×), pipelined SET +368%, non-pipelined SET +404%; single-shard
  throughput +63–80%. (PR #172)
- **Drain-cap self-re-notify:** a >256-message cross-shard burst no longer
  strands its tail until the next tick — a capped drain re-arms the wake
  immediately (both runtimes).
- **New INFO Stats counters:** `spsc_notify_wakes` and `spsc_drain_renotify`
  make the event-driven path observable from a black-box client.

### Changed — Hot-path lock quick wins

- Per-command global lock acquisitions removed from the command dispatch path
  (replication offset reads, shared-databases lookups, socket-option setup,
  accept-path state), with the replication backlog append switched to a
  bulk-copy. (PR #172)

### Fixed

- `uring_handler` in-flight send queue used `Vec` API on a `VecDeque`
  (`.push` → `.push_back`) — the Linux+tokio io_uring bridge path failed to
  compile on its own. (PR #172)

## [0.3.0] — 2026-06-11

Hot-shard elasticity + vector engine maturity: background HNSW compaction
(no more shard freezes), working SQ8 quantization, elastic per-shard memory
budgets, `HOTKEYS` detection, and a +33%/+25% pipelined SET/GET perf pass.

### Added — Docs: Design Advantages page with architecture diagrams

- New docs page (`design-advantages.md`, MkDocs nav → Concepts) telling the
  full architecture story with 6 whiteboard diagrams: platform hero, memory
  engine, persistence, vector engine, graph engine, converged GraphRAG
  workflow, plus an "AI agent era" section mapping agent memory types to
  Moon surfaces. Originally authored for the retired Mintlify site (PR
  #145), converted to MkDocs Material and refreshed: quantization guidance
  now says SQ8 (TQ8 never existed), and the segment-lifecycle and disk
  offload sections reflect background compaction (PR #165) and elastic
  memory budgets (PR #170). README "Why Moon" embeds the hero diagram.

### Changed — Background FT compaction + segment merge (no more shard freezes)

- **HNSW index builds no longer block the shard event loop.** `try_compact`
  (auto-compaction on search) now begins the build on a background worker
  pool and installs the finished immutable segment on a later poll; the
  triggering search continues against the still-present brute-force mutable
  segment. Inline builds previously froze the shard for the full build
  (0.42s @2k → 24.9s @50k vectors, measured); the event-loop stall is now
  ~4.4ms and PING latency stays flat during compaction.
- **Background immutable-segment merge**: many small immutable segments are
  merged into one on the same worker pool (graph-union, no lossy
  decode/re-encode), bounding per-search segment fan-out. Real-MiniLM
  recall preserved across the merge (0.9447 → 0.9440 R@10, 18 segments → 1).
- Deletes that race a background build are reconciled at install time
  (delete-window replay) — no resurrection of removed vectors.
- `FT.COMPACT` (explicit user intent) stays synchronous: it drains any
  in-flight background build, then compacts inline. Tradeoff: searches run
  brute-force on the mutable segment until the background build installs.

### Added — Elastic per-shard memory budgets (hot-shard headroom borrowing)

- **Hot shards now borrow idle siblings' unused `maxmemory` headroom**
  instead of being capped at a static `maxmemory / num_shards`. Each shard
  publishes its used memory on its existing 100ms eviction tick and
  recomputes an elastic budget from the published snapshot:
  `base + Σ(under-budget surplus) / hot_shard_count`, clamped to total
  `maxmemory`. No new locks or channels — two relaxed atomics per shard,
  read once per write-path eviction check.
- Snapshot invariant: hot shards' budgets plus under-budget shards' usage
  never exceed `N × base` at recompute time; transient overshoot is bounded
  by one tick of donor write growth and converges via write-path eviction.
- Live 4-shard validation (32MB cap, allkeys-lru, all writes to one shard
  via a `{tag}`): hot shard retained ~28k × 1KB keys (~30MB) vs the ~7.2k
  (8MB) static ceiling — 3.9× more of the configured memory actually
  usable under skewed load. Uniform load is unchanged (same global
  ceiling, budgets stay at base).
- Disk-offload pressure cascade and timer-based eviction enforce the same
  elastic budget; budget `0` (not yet published, single shard, or
  `maxmemory=0`) falls back to the static per-shard cap everywhere.

### Added — Hot-key detection: `HOTKEYS` command + per-shard SpaceSaving sketch

- **New `HOTKEYS [COUNT n]` command** (Moon extension, `@server` ACL,
  read-only): returns the top sampled keys as `[key, sampled_count]` pairs,
  count descending. Counts are 1-in-64 samples of keyed commands — multiply
  by 64 for an approximate command rate. In multi-shard mode the connection
  handler merges per-shard sketches (DBSIZE-style scatter-gather), so clients
  always see the global view.
- **Per-shard SpaceSaving top-K sketch** (K=128, ~5 KB per database,
  L1-resident, single-pass scan): fed by all three execution paths — write dispatch, the shared
  read path, and the inline GET/SET fast path. The tick counter is one relaxed
  `fetch_add` per command; the O(K) sketch update runs only on sampled ticks
  and `try_lock`s (a contended sample is dropped — the hot path never blocks).
  Kill switch: `MOON_NO_HOTKEYS=1`.

### Fixed — `OBJECT` was unreachable over the wire (monoio handler)

- **`OBJECT ENCODING/FREQ/IDLETIME/REFCOUNT` returned `ERR unknown command`
  on a live server** even though dispatch implemented them: the monoio
  handler routes every non-write command through the read dispatcher, which
  had no `OBJECT` arm. Added a read-only `OBJECT` handler (`get_if_alive`,
  never mutates expiry state) and enabled it on the cross-shard fast path.
- **`OBJECT <subcmd> <key>` routed by the subcommand name** in multi-shard
  mode (`extract_primary_key` hashes `args[0]`, which is `FREQ`/`ENCODING`,
  not the key) — the command landed on an arbitrary shard and answered
  `ERR no such key`. Key extraction now uses the real key (arg 2).

### Changed — Hot-path performance: deep-review optimization pass (PR #168)

- **+33% pipelined SET, +25% pipelined GET (single shard)** from a deep
  performance review covering protocol → dispatch → shard → storage →
  persistence → vector. 11 verified findings implemented:
  - `Database::get`: 3 → 2 DashTable probes on a hit, 2 → 1 on a miss
    (single-probe live/expired/absent classification).
  - WAL v2 flush: 3 → 1 `write(2)` syscalls per 1ms tick via a reusable
    staging buffer; on-disk format unchanged.
  - WAL v3 record append no longer heap-allocates per non-FPI record
    (`Cow::Borrowed`); checkpoint state machine no longer clones per tick.
  - SPSC drain (`drain_spsc_shared`) reuses thread-local batch buffers
    instead of allocating two `Vec`s per call.
  - `Frame::Double` serialization is zero-alloc (stack `f64` Display
    formatter, byte-identical wire output) in RESP2 and RESP3.
  - SQ8 vector search no longer heap-allocates per query (HNSW + both
    brute-force paths); IVF rotation/LUT buffers hoisted out of the
    per-segment loop in `search_filtered` (parity with `search_mvcc`).
  - io_uring `drain_completions` reuses persistent CQE/event buffers
    (allocation-free drain loop at steady state).
- Multi-shard (2/4/8/16/auto): no regressions; +9–32% pipelined SET at
  2 and 8 shards. The unpipelined cross-shard latency floor (~1.7ms p50,
  waker-relay + dispatch tick) is unchanged and tracked for follow-up.

### Fixed — SQ8 vector quantization now works across the full FT lifecycle

- **`FT.CREATE ... QUANTIZATION SQ8` was a declared-but-unimplemented
  quantizer** — vectors fell through to the TQ4 encoder against an empty
  codebook, producing degenerate codes and random search (recall 0.014,
  exact-match returned the wrong key) on real embeddings. Implemented a real
  per-vector affine scalar-8-bit quantizer (`dim` u8 codes + `(min, scale)`
  trailer) and wired it through the entire segment lifecycle: append,
  brute-force search, compact, immutable HNSW search, multi-segment search
  fan-out, segment **merge**, and persistence reload. Recall on real
  all-MiniLM-L6-v2 384d embeddings: **0.014 → 0.897**, exact-match correct.
- **`append_transactional()` corrupted SQ8 vectors** — it wrote the TQ slot
  layout (`padded/2 + 4`) into `dim + 8` SQ8 slots, corrupting the code stride
  for every transactionally-inserted or WAL-recovered SQ8 vector. Both append
  paths now share one `encode_sq8_slot()` helper.
- **SQ8 ignored the index metric** — an `InnerProduct` index ranked
  non-normalized inputs by magnitude. SQ8 now normalizes for both unit-sphere
  metrics (Cosine + InnerProduct), matching the rest of the engine; `L2` keeps
  raw vectors for true Euclidean ranking.
- **Docs:** corrected the ≤384d quantization guidance to recommend **SQ8**
  (the real 8-bit option) instead of the nonexistent "TQ8".

### Changed — Rust SDK released as moondb 0.2.0 on crates.io

- **`moondb` crate `0.1.1` → `0.2.0`** — publishes the v0.2-era client API
  that has lived in-tree since the `hybrid_search` sparse upgrade
  (`f4fcd5a`). Breaking: `text().hybrid_search()` now takes
  `sparse_field: Option<&str>` and `weights: [f64; 3]` (was two-way
  `[f64; 2]`) and speaks the PARAMS-based wire format with `@`-prefixed
  field refs. Added: `Client::connect_with_timeout` for bulk-write
  workloads. Released as 0.2.0 — not 0.1.2 — because cargo treats 0.1.x
  versions as compatible, and the signature change would break published
  0.1.x consumers (lunaris-retrieve / lunaris-storage-moon 0.2.1 pin
  `"0.1.1"` with the two-way call). Aligns the SDK version with the Moon
  v0.2.0 server release.

## [0.2.0] — 2026-06-06

The v0.2 enterprise beachhead. Built additively on per-shard WAL v3 + the
dual-root manifest; no changes to the KV hot path, MVCC, page format, or
transaction layer.

### Changed — default persistence directory is the platform user-data dir (was: current directory)

- **`--dir` now defaults to the platform user-data directory**, created
  on first run: Linux `$XDG_DATA_HOME/moon` (or `~/.local/share/moon`),
  macOS `~/Library/Application Support/moon`, Windows
  `%LOCALAPPDATA%\moon`. Installed binaries no longer litter whatever
  directory they happen to be started from.
- **Back-compat guard:** if the startup directory already contains moon
  persistence data (`appendonlydir`, `shard-0`, `dump.rdb`,
  `replication.state` — the pre-v0.2.0 default layout), moon keeps using
  it and logs a warning, so upgrades never silently boot with an empty
  keyspace away from their data.
- Explicit `--dir <path>` / conf `dir` (including `--dir .`) opt out of
  auto-resolution entirely; environments with no `HOME`/`LOCALAPPDATA`
  fall back to the current directory with a warning. Docker
  (`--dir /data`) and the systemd package (`dir /var/lib/moon`) already
  pass explicit paths and are unaffected.

### Changed — default shard count is now 1 (was: auto-detect)

- **`--shards` defaults to 1** instead of auto-detecting the CPU count.
  Single-shard gives the best throughput for non-pipelined workloads
  (cross-shard SPSC dispatch dominates local lookups) and a deterministic
  persistence layout across hosts. `--shards 0` remains the explicit
  auto-detect opt-in; nothing changes for deployments that pass `--shards`
  explicitly.
- **Upgrade note:** deployments that previously relied on the auto-detect
  default with `appendonly yes` will refuse to start after upgrading
  (`ERR shard count changed (manifest=N, config=1)`) — this is the
  intended data-loss guard. Start with `--shards <N>` matching the
  manifest, or see the new
  [shard-count-change runbook](docs/runbooks/shard-count-change.md)
  (also linked from the error message itself, now as a full GitHub URL so
  installed binaries point somewhere reachable).

### Fixed — release pipeline verification + prerelease safety

- `release.yml` sign job now publishes Fulcio certificates (`*.crt`)
  alongside signatures: keyless `cosign verify-blob` requires the
  certificate — the previous `.sig`-only output was unverifiable by
  users.
- Docker job no longer moves `ghcr.io/pilotspace/moon:latest` on
  prerelease tags (e.g. `v0.2.0-rc.1`); only stable releases repoint
  `:latest`. The versioned tag is always pushed.

### Changed — v0.2.0 release prep

- Crate version bumped 0.1.12 → 0.2.0; `INFO server` `moon_version` now
  reports 0.2.0 (previously released binaries would have self-reported
  the stale crate version regardless of the git tag).
- `release.yml`: the `homebrew-tap` bump job is gated behind the
  `HOMEBREW_TAP_ENABLED` repository variable. Homebrew distribution is
  deferred — v0.2.0 ships via curl `install.sh`/`install.ps1`, .deb/.rpm,
  and Docker. Without the gate the job would hard-fail on every stable
  tag (missing `HOMEBREW_TAP_TOKEN` secret). `packaging/bump-homebrew.sh`
  and the formula template stay in-tree for later enablement.

### Added — Temporal-decay traversal scoring (agent-memory recency)

User-facing recency bias for graph traversal: paths through recently
created edges win over stale ones. The decay engine (scorers, Dijkstra
composite cost) existed but was unreachable — `shortestPath()` hardcoded
`lambda = 0`. This wires it end to end.

- **`GRAPH.QUERY ... --decay <λ> [--time-weight <w>]`** — per-edge cost
  becomes `|weight| + λ·w·age_seconds` for `shortestPath()`; λ is 1/seconds,
  strictly validated. Decay off (no flag) keeps exact distance-only
  behavior — the age term contributes zero to every edge cost, so path
  choice is identical to pre-decay Moon. Applies to the read-only and
  `GRAPH.PROFILE` paths via `ExecutionContext` (same pattern as
  `VALID_AT`); write queries (CREATE/SET/DELETE/MERGE) reject the flag
  instead of silently ignoring it.
- **`FT.NAVIGATE ... DECAY <λ>`** — graph-expanded hits pay
  `λ × age_seconds` of their discovery edge on top of the hop penalty
  (a re-rank of the already-explored expansion, not a steer of the
  expansion itself); KNN direct hits unaffected.
- **Edges stamp `created_ms` at insert** from the shard-cached clock
  (zero syscall on the insert path). `0 = unknown` is decay-neutral —
  pre-upgrade edges never look maximally old. Distinct from the
  user-owned bi-temporal `valid_from`/`valid_to`.
- **CSR segment format v3** — per-edge `created_ms` array (parallel to
  `col_indices`) survives freeze → disk → mmap → compaction, so decay
  sees true edge age after segments rotate. v1/v2 files keep loading
  (empty stamps = neutral); both parsers (heap + mmap zero-copy) are
  version-gated, plus a new `csr_from_bytes` fuzz target.
- **Fixed (latent durability bug):** `compact_segments` stamped merged
  segments `version: 1` while the serializer always writes 48-byte v2+
  NodeMeta records — a vacuumed segment written to disk misparsed on
  reload (panic in debug, silent node_meta corruption in release).
  Merged segments are now v3 and carry per-edge stamps through dedup.
- Docs: `guides/temporal.mdx` decay section + `commands.mdx`; script
  coverage in `test-commands.sh` (6 DECAY cases) and `test-consistency.sh`
  (1/4/12-shard path-flip parity).
- Known gap: WAL-replayed not-yet-frozen edges re-stamp to replay time
  on restart (newest edges look new — bias direction preserved);
  CSR-resident edges keep exact age.

### Added — Ship moon as an installable application on macOS, Linux, and Windows

Five-PR milestone making moon installable on all three platforms
(consolidated entry; the sibling PRs carry the `skip-changelog` label).

- **Windows native port** — cross-platform positioned-I/O helper
  (`src/util/file_ext.rs`; pwrite/pread on unix, `seek_write`/`seek_read`
  loops on Windows), portable `RawSocketFd` alias with `#[cfg(unix)]`-gated
  connection-migration fd ops, `compile_error!` guard for `jemalloc` on
  Windows MSVC (mimalloc fallback), and Windows-only startup warnings
  (VirtualLock working-set, no memory guardrail). Build:
  `--no-default-features --features runtime-tokio,graph,text-index`.
- **Graceful shutdown on SIGTERM** — `ctrlc` `termination` feature with a
  single centralized handler in `main.rs`; `systemctl stop` / `launchctl
  stop` now flush AOF before exit (previously only SIGINT was caught).
- **Version strings** — `INFO server` reports `redis_version:7.4.0`
  (client feature-gating) plus a new `moon_version:` field from
  `CARGO_PKG_VERSION`; HELLO/LOLWUT report the real moon version
  (previously hardcoded `0.1.0`).
- **moon.conf config file** — redis-style `key value` config
  (`moon /etc/moon/moon.conf` or `--config`), CLI flags override the file;
  commented default shipped as `packaging/moon.conf.example` and installed
  to `/etc/moon/moon.conf` (deb/rpm, `config|noreplace`).
- **Install channels** — `install.sh` (Linux/macOS) and `install.ps1`
  (Windows) one-liner installers with SHA256 verification; Homebrew tap
  automation (`packaging/homebrew/moon.rb.tmpl` + `bump-homebrew.sh`
  publishing to `pilotspace/homebrew-moon` on release).
- **Release pipeline overhaul** — 8-target matrix (linux gnu/musl
  x86_64/aarch64, macOS arm64/Intel, Windows x86_64 MSVC), all release
  binaries now include `graph,text-index,console` (previously missing from
  releases), `prepare-console` pnpm build job, checksums + cosign over all
  artifacts including .deb/.rpm, `--prerelease` for `-rc` tags, and a
  `workflow_dispatch` dry-run mode.
- **Packaging fixes** — nfpm license corrected to Apache-2.0,
  `moon.service` ExecStart path fixed (`/usr/bin/moon`), CI gains
  main-push-only `check-windows` and `check-console` jobs plus a macOS
  Intel cross-build check.
- **Console build fix (PR #152)** — graph components aligned with the
  pinned `@cosmos.gl/graph` 2.6.4 API (`setConfigPartial` → `setConfig`,
  no async `ready` hook); `pnpm run build` type-checks again, unblocking
  the Console Integration workflow and the release `prepare-console` job.
- **RESP3 negotiation fix (PR #153)** — the monoio (default) handler now
  syncs the wire codec when `HELLO 3` negotiates RESP3; previously the
  codec stayed on RESP2 and silently flattened map/set/push frames,
  breaking RESP3 clients on connect (redis-py 8 defaults to RESP3).
- **Windows runtime fixes (PR #154)** — first fully green `check-windows`
  run: fsync helpers reworked for Windows semantics (no directory
  handles, writable handle for `FlushFileBuffers`); unsigned-`Instant`
  underflows fixed (autovacuum scheduling, rate-limiter cutoffs, mvcc
  test anchors); the tokio sharded handler no longer initiates connection
  migration on non-unix platforms (previously aborted clients mid-session
  in multishard workloads); `GraphManifest::save` now propagates
  parent-dir fsync errors.
- **Known limitations (v0.2.0)** — Windows binaries are not
  Authenticode-signed (SmartScreen warning); connection migration is
  unix-only (connections stay on the originating shard on Windows);
  Windows service wrapper + MSI deferred.

### Fixed — CodeRabbit PR #136 durability follow-ups + decomposition + test isolation (PR #144)

Closes the 8 CodeRabbit findings left open after PR #136, plus two PR #144-review
Majors, oversized-file decomposition, and a parallel-test flake. No production
hot-path behaviour change.

- **Disk-offload spill (data-loss fixes):** block instead of dropping spill
  completions; salvage inline-batch spill failures per-entry rather than
  wholesale; preserve spill context across tokio connection migration; recover
  the cold tier under `appendonly=no`.
- **Per-shard AOF rewrite robustness:** clear the rewrite flag when fan-out
  fails partway; roll per-shard rewrite writers back to the committed generation
  on abort (barrier-before-resume + panic-safe `ShardDoneGuard`); ack drained
  `AppendSync` only after the boundary fsync (issue #140 ordering).
- **Async-spill eviction:** corrected a stale doc comment, removed the dead
  remove-first eviction path, and added a regression test locking the fail-safe
  send-before-remove ordering (a full spill channel keeps the victim resident —
  no data loss).
- **Platform hygiene:** gate the migrated-connection spawn fns behind
  `cfg(all(..., unix))` to match their `RawFd` usage.
- **File decomposition (1500-line cap):** split `aof_manifest.rs` (3058 →
  mod/shard_replay/shard_rewrite) and `aof.rs` (4379 → mod/pool/writer_task/
  rewrite); pure code relocation, verified line-exact on both runtimes.
- **Test isolation:** fixed the `VECTOR_INDEXES` counter flake — the process-
  global metrics counter is now guarded by an `RwLock` (delta-reader tests take
  `write()`, mutator tests take `read()`), making the index-count delta
  assertions deterministic under the parallel test harness.

### Persistence — Per-shard AOF migration complete (PR #129)

Closes the P0 multi-shard AOF data-loss bug (~50% loss on SIGKILL with
`--shards >= 2 + --appendonly yes`) by shipping the full per-shard AOF
architecture (Option B of `tmp/rfc-per-shard-aof-v02.md`).

- **H2 closed** — `src/main.rs` no longer skips multi-shard AOF replay.
  Each shard owns its own writer task; recovery walks every shard's segment
  manifest independently. Shard replay is parallel (recovery time does
  not grow linearly with shard count).
- **H1 closed** — new `AppendSync { bytes, ack }` rendezvous variant
  ensures `+OK` is on the wire only after fsync ack under
  `appendfsync=always`. `try_send` (`everysec`/`no`) paths unchanged.
- **`--unsafe-multishard-aof` deprecated** — was the v0.1.13 escape hatch
  acknowledging the ~50% loss risk. The flag is now a no-op that prints
  `[DEPRECATED]` at startup; will be removed in v0.2.0-rc.
- **CRASH-01-LITE matrix** — 200/200 SIGKILL recoveries across
  `--shards 1/2/4/8` × `appendfsync always/everysec`. Gated in
  `.github/workflows/integration-tests.yml`; run locally with
  `cargo test --release crash_01_lite` on the moon-dev OrbStack VM.
- **TopLevel-manifest safety guard** — Moon refuses to start (exit 2) when
  it finds an existing v0.1.13-style TopLevel manifest with `--shards >= 2`,
  to prevent silent data loss from replaying a non-routed log. Migration
  via `docs/runbooks/multi-shard-aof-rewrite.md` Option A.
- **`INFO persistence`** — new field `aof_backpressure_dropped:<N>` exposes
  per-shard writer drop counts; non-zero indicates the AOF writer is
  falling behind write throughput.
- **Per-shard layout on disk:** `appendonlydir/shard-{N}/moon.aof.{seq}.base.rdb`
  + `moon.aof.{seq}.incr.aof` mirrors the per-shard WAL v3 design.

Architectural follow-ups parked for v0.2.0:

- Rule 3 (LSN ordering invariant) under per-shard topology — issue #131
- Always-fsync handler-layer integration audit — issue #132
- `OrderedAcrossShards` merge-replay correctness on large transcripts — issue #133

Per-shard `BGREWRITEAOF` (step 6 of the migration RFC) is **not yet in
this PR**; rewriting a per-shard AOF returns `ERR BGREWRITEAOF is not yet
supported under per-shard AOF layout`. Tracked for v0.2.0.

**Headline capabilities landed in alpha:**

- **Point-in-Time Recovery (PITR)** — `--recovery-target-lsn` /
  `--recovery-target-time` restore to any LSN or wall-clock boundary
  inside the WAL retention window.
- **Change Data Capture (CDC)** — `CDC.READ` polling command with
  Debezium-compatible JSON envelopes, resumable cursors, segment-rotation
  safety.
- **Hash-field TTL** — full Valkey 9.0 / 9.1 surface (`HEXPIRE` /
  `HPEXPIRE` / `HEXPIREAT` / `HPEXPIREAT` / `HEXPIRETIME` / `HPEXPIRETIME`
  / `HTTL` / `HPTTL` / `HPERSIST` / `HGETDEL` / `HGETEX`) with O(1) HGET +
  HLEN fast path. Three-way benchmark vs Redis 8.0.2 / Valkey 9.1.0
  ships in `docs/perf/2026-05-27-hash-ttl-3way-bench.md`.
- **Tier 2 Lane A** — `SWAPDB`, `MOVE`, `COPY ... DB n`,
  `CLUSTER REPLICAS` / `SLAVES`, `CLUSTER COUNT-FAILURE-REPORTS`. All
  WAL-durable with cross-shard atomic semantics.
- **Storage format v1 commitment** — RDB v2 + WAL v3 + multi-part AOF
  manifest grouped under a single `--storage-format v1` umbrella with
  ≥18-month LTS forward-read guarantees.
- **Embedded sharded server** — `server::embedded::run_embedded(config,
  cancel)` exposes the full sharded handler (with `TXN.*`) to in-process
  embedders.

**What is not yet in alpha:** PITR live-snapshot LSN wiring (P3c),
`CDC.SUBSCRIBE` push channel (C3b), and the multi-shard master PSYNC
deferred from v0.1.10. Tracked in `.planning/rfcs/v02-enterprise-architecture.md`.

### Fixed — Multishard idle-RAM blowup + tokio-Linux serving hang + graph parser DoS (PR #136)

Closes the "multishard RAM zombie": a fresh multishard instance with no
`maxmemory` could commit multiple GB of RSS while idle, and the tokio
runtime could hang its accept loop on Linux. Three independent root
causes, all fixed and verified in the OrbStack VM under a cgroup memory
cap (idle RSS **3791 MB → 29 MB** for a 4-shard no-`maxmemory` instance;
~45 MB under load):

- **PageCache eager pre-allocation.** Each shard committed
  `num_frames × PAGE` zeroed bytes at construction, sized to 25% of the
  **whole-instance** `maxmemory` (or a large default when unset). Now the
  page buffers allocate lazily and the frame budget is divided by shard
  count (`per_shard_pagecache_budget`); a startup `WARN` reports the
  resolved per-shard budget.
- **tokio listener bind-race.** The central accept listener bound the port
  *without* `SO_REUSEPORT` while per-shard listeners bound *with* it,
  producing a bind-order race that could leave the port served by a shard
  that never received connections. The central listener now also binds
  `SO_REUSEPORT`; `--per-shard-accept` defaults to `false` under tokio.
- **io_uring-under-tokio default-off.** The tokio→io_uring bridge floods
  errors under load. It is now **opt-in via `MOON_URING=1`**; tokio shards
  run plain epoll/kqueue by default. `MOON_NO_URING=1` still force-disables
  io_uring everywhere. The monoio runtime is unaffected (always io_uring
  unless `MOON_NO_URING`).

Two durability defects found during review were fixed in-PR with
red/green TDD:

- **Manifest compact-reopen failure no longer silently loses commits.**
  `manifest.rs compact()` reopened `self.file` *after* `rename(tmp, path)`;
  if that reopen failed, later commits silently wrote to an orphaned inode.
  A `needs_reopen` flag now reattaches to `self.path` before any subsequent
  commit (or fails loudly).
- **tokio per-shard AOF writer now latches after a torn write.** The tokio
  per-shard writer lacked the `write_error` latch present on the single-file
  and monoio writers; a torn write (header OK, data fails) corrupted the
  frame stream. It now latches on any write failure and acks `WriteFailed`.

- **Cypher parser stack-overflow DoS bounded.** The precedence-climbing
  expression grammar pushes ~9 stack frames per source nesting level but
  `check_depth()` counts only one level per recursion, so the previous
  limit of 64 overflowed a 2 MiB async-worker stack (SIGABRT) **before**
  the guard could fire. `DEFAULT_MAX_NESTING_DEPTH` is now `32`
  (~288 worst-case debug frames, ~50% margin); deep nesting returns
  `NestingDepthExceeded` gracefully.

Low-severity durability edge cases filed as follow-ups: #137
(`apply_spill_completions` failed-commit cold-key window), #138
(`do_rewrite_per_shard` panic wedges `--experimental-per-shard-rewrite`),
#139 (multi-DB `SELECT >0` cold recovery restores db0 only).

### Fixed — `maxmemory` is now a whole-instance cap across shards (G2)

**Behavior change for multishard deployments.** Previously each shard
enforced eviction against the *full* `maxmemory`, so an N-shard server
tolerated ~N× the configured cap before evicting (a 4-shard server at
`--maxmemory 100mb` retained ~307 MB / 768K keys vs a 1-shard server's
124 MB / 322K — the "RAM keeps growing in multishard mode" report).

`maxmemory` is now a true **whole-instance** cap. Each shard enforces
eviction against `maxmemory / num_shards`, so aggregate RSS converges on
the configured value regardless of shard count.

- **`CONFIG GET maxmemory` / INFO are unchanged** — they report the
  whole-instance value verbatim (Redis-compatible). Division happens only
  at enforcement.
- **Operators running explicit `--maxmemory N --shards M (M>1)`** now get
  an effective ceiling of `N` (not `N×M`). A startup log line states the
  resolved per-shard budget so the change is visible:
  `maxmemory <N> bytes is a whole-instance cap; each of <M> shards enforces
  eviction against a per-shard budget of <N/M> bytes`.
- Single-shard servers are byte-for-byte unaffected (`num_shards == 1` ⇒
  no division).

### Docs — Hash-field TTL three-way benchmark suite (PR #127)

- `scripts/bench-hash-ttl.sh` (2-way harness) + `scripts/bench-hash-ttl-3way.sh`
  (3-way harness with Valkey 9.1.0). Both use `redis-benchmark` against
  concurrent Moon / Redis / Valkey servers on distinct ports with trap-based
  cleanup, FLUSHALL + re-seed between scenarios, and median-of-3 RPS reporting.
- `docs/perf/2026-05-26-hash-ttl-bench.md` — pre-fix 2-way baseline that
  surfaced the two HashWithTtl perf issues fixed in PR #126.
- `docs/perf/2026-05-27-hash-ttl-3way-bench.md` — headline Moon vs Redis 8.0.2
  vs Valkey 9.1.0 comparison across 26 scenarios. Plain HGET p=16 ties both
  competitors (1.00–1.01×). HEXPIRE-family Moon vs Valkey: 0.90–0.99× across
  the surface; HGETEX hits parity at 0.99×. Redis 8.x has no HEXPIRE-family —
  Moon is the only Redis-compatible alternative aside from Valkey.

### Performance — HashWithTtl HGET + HLEN O(1) fast path (PR #126)

Resolves the two HashWithTtl perf issues surfaced by the 2026-05-26 bench:

- `HGET` on `HashWithTtl` was 39.9% slower than plain `Hash` at p=16; now
  **+4% faster** (within VM measurement noise — effective parity).
- `HLEN` on `HashWithTtl` was 80.5× slower than plain `Hash` at 1000 fields;
  now **1.00–1.03× parity** (the O(N) live-count scan is fully eliminated when
  no field has expired).

Two changes shipped together (variant layout forces both at once):

- **`HashWithTtl.ttls` BTreeMap → HashMap.** Per-field TTL probe becomes O(1)
  HashMap lookup instead of O(log N) BTreeMap descent. Active-expire iteration
  doesn't require ordered keys.
- **`HashWithTtl.min_expiry_ms: u64` cached minimum.** Tracks the smallest
  expiry across all per-field TTLs. Invariant: `min_expiry_ms = min(ttls.values())`.
  When `cached_now_ms < min_expiry_ms`, no field can be expired, so `HGET`
  skips the `ttls` probe and `HLEN` returns `fields.len()` directly. The hot
  path is a single `u64 < u64` compare. Invariant maintenance is amortized
  O(1) (one `min(min, ts_ms)` per `HEXPIRE`; conditional recompute on
  `HPERSIST` / overwrite / active reap when the removed TTL equalled the min).

No on-disk format change. `min_expiry_ms` is recomputed at load time from the
existing v2 RDB per-field TTL trailer. 6 new invariant tests cover HEXPIRE /
HPERSIST / HSET-overwrite / active-reap / persistence-decode paths.

### Docs — Hash-field TTL audit follow-up (PR #123)

- `docs/commands.mdx` — Hashes section bumped from "(14)" → "(25)" and now
  lists all 11 new Valkey 9.0/9.1 commands plus a paragraph on the per-field
  return convention and the active-expiry downgrade behaviour.
- `docs/STORAGE-FORMAT-V1.md` §3.2 — added the per-field hash TTL trailer
  format: every `TYPE_HASH` body is followed by
  `[ttl_count u32][field_len varint | field_bytes | ttl_ms u64]*` in v2 RDB
  files; v1 readers stop after the hash body.
- `src/command/metadata.rs` — `HEXPIRETIME` / `HPEXPIRETIME` / `HTTL` /
  `HPTTL` PHF flags flipped from `R` (READONLY) to `RF` (READONLY|FAST) —
  per-field TTL lookup is O(1) thanks to the PR #126 fast path. Cosmetic;
  no behavioural impact.

### Added — HGETDEL / HGETEX atomic compound hash commands (phase 199, issue #110)

Two new Valkey 9.1 atomic compound hash commands:

- `HGETDEL key FIELDS numfields field [field ...]` — returns the values of
  the specified fields and deletes them from the hash atomically. Returns a
  RESP Array with one `BulkString(value)` per found field and `Null` for
  missing fields. If the hash becomes empty after all deletes the key is
  removed entirely (auto-cleanup).

- `HGETEX key [EX s | PX ms | EXAT unix-s | PXAT unix-ms | PERSIST] FIELDS numfields field [field ...]`
  — returns the values of the specified fields and optionally updates (or
  removes) their per-field TTLs atomically. TTL modes:
  - `EX s`         — set relative expiry in **seconds** from now.
  - `PX ms`        — set relative expiry in **milliseconds** from now.
  - `EXAT unix-s`  — set absolute expiry as unix **seconds**.
  - `PXAT unix-ms` — set absolute expiry as unix **milliseconds**.
  - `PERSIST`      — remove any existing per-field TTL.
  - (no mode)      — pure read; no TTL change (fast path, zero DB mutation).
  TTL changes apply only to live (non-expired) fields. Missing / expired
  fields return `Null` and leave TTLs untouched.

Atomicity: per-shard single-threaded execution gives atomicity for free
across the entire field list — no client can observe a partial state between
reads and deletes/TTL-updates within a single call.

Implementation:
- Two new `Database` primitives in `src/storage/db.rs`:
  - `hash_get_and_delete_field` — atomically reads and removes a single
    field; handles Hash, HashListpack, and HashWithTtl (also removes TTL
    sidecar). Downgrades HashWithTtl → Hash when the last TTL is removed.
  - `cleanup_empty_hash` — removes the key when its hash has become empty;
    called once after a HGETDEL/HGETEX field loop.
- `HGETDEL` handler uses `parse_key_and_fields` (phase-198 shared parser)
  plus a single `cleanup_empty_hash` call after the field loop.
- `HGETEX` parser (`parse_hgetex_args`) scans the optional mode token(s)
  before `FIELDS` with mutual-exclusion enforcement; uses saturating i128
  arithmetic + u64 clamp for safe overflow handling (mirrors phase 196).
- Both commands dispatch as writes (WF flags, `&mut Database`); neither is
  added to `is_dispatch_read_supported`.
- 17 new unit tests: 8 for HGETDEL, 9 for HGETEX.

### Added — Hash-field TTL read + persist (phase 198, issue #109)

Five new Valkey 9.0 hash-field TTL commands:

- `HEXPIRETIME key FIELDS numfields field [field ...]` — absolute expiry
  per field as a unix timestamp in **seconds**.
- `HPEXPIRETIME key FIELDS numfields field [field ...]` — absolute expiry
  per field as a unix timestamp in **milliseconds**.
- `HTTL key FIELDS numfields field [field ...]` — remaining TTL per field
  in **seconds**; already-expired-but-not-reaped fields return `0`.
- `HPTTL key FIELDS numfields field [field ...]` — remaining TTL per field
  in **milliseconds**; same `0` edge-case for expired-but-not-reaped.
- `HPERSIST key FIELDS numfields field [field ...]` — removes the per-field
  TTL; downgrades `HashWithTtl` back to plain `Hash` when the last TTL is
  removed (handled by the phase-195 `hash_persist_field` primitive).

Per-field return codes (Valkey 9.0):
- `-2` — field does not exist (or key is missing — **not** a WRONGTYPE error)
- `-1` — field exists but has no TTL
- `≥0` — absolute unix time or remaining duration (HEXPIRETIME/HPEXPIRETIME)
- `1` — TTL successfully removed (HPERSIST only)

WRONGTYPE is returned immediately (before field iteration) when the key
holds a non-hash value.

Implementation:
- `FieldState` tri-state enum and `hash_field_state` helper (pre-landed in
  phase 197) provide the zero-allocation field-state read used by all five
  commands.
- `parse_key_and_fields` shared parser (`hash_write.rs`) extracts
  `key FIELDS numfields field [field ...]`; reuses `SmallVec<[&[u8]; 4]>`
  to avoid heap allocation for the common ≤4-field case.
- Four read handlers (`HEXPIRETIME`, `HPEXPIRETIME`, `HTTL`, `HPTTL`) take
  `&Database`; `HPERSIST` takes `&mut Database`.
- All five commands routed in both `dispatch()` (mutable path) and
  `dispatch_read_inner()` / `is_dispatch_read_supported()` (shared-read
  path) for the four read commands.
- 14 unit tests cover all return-code variants, the already-expired edge
  case, WRONGTYPE, missing key, numfields=0, and encoding downgrade.

### Added — Hash-field active expiration (phase 197, issue #108)

All 9 hash read commands (`HGET`, `HMGET`, `HGETALL`, `HEXISTS`, `HLEN`,
`HKEYS`, `HVALS`, `HSCAN`, `HRANDFIELD`) now respect per-field TTLs set by
the `HEXPIRE` family. Expired fields are invisible to callers without
requiring the active-expiry tick to have run first (lazy expiry).

**Lazy expiry** (read path, `&Database`): `HashRef` gains a third variant
`WithTtl { fields, ttls, now_ms }` that filters expired fields on every
field-level operation. The `get_hash_ref_if_alive` accessor now returns this
variant for `HashWithTtl` entries instead of falling through to `WRONGTYPE`.
All 9 mutable read commands now call `get_hash_ref_if_alive` instead of the
unfiltered `get_hash`, so expired fields are never returned.

**Active expiry** (tick path, `&mut Database`): the per-shard `expire_cycle`
gains a second sweep via `Database::hashes_with_field_expiry()` and
`Database::reap_expired_fields_one_hash()`. The reaper removes expired
fields from the `fields` and `ttls` maps, downgrades the hash back to plain
`Hash` when the last TTL sidecar entry is drained, and signals `KeyDeleted`
when all fields expire (the key is then removed by the caller).

The `maybe_has_expiring_keys` fast-path flag is now cleared only when
**both** the whole-key sweep and the hash-field sweep return empty, preventing
premature flag-clearing that would have silenced future field reaping.

**Complexity change**: `HLEN` is now O(N) for `HashWithTtl` hashes (counts
only live fields). Plain `Hash` and `HashListpack` remain O(1) / O(N)
respectively — no regression.

### Added — HEXPIRE-family write commands (phase 196, issue #107)

- `HEXPIRE key seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]`
- `HPEXPIRE key milliseconds [NX|XX|GT|LT] FIELDS numfields field [...]`
- `HEXPIREAT key unix-seconds [NX|XX|GT|LT] FIELDS numfields field [...]`
- `HPEXPIREAT key unix-ms [NX|XX|GT|LT] FIELDS numfields field [...]`

Per-field return codes match Valkey 9.0: `0` (no such field), `1` (TTL set or
updated), `2` (field expired during this call and was deleted), `-2` (NX / XX /
GT / LT condition not met). Wrong-type key returns `WRONGTYPE`; missing key
returns one `0` per requested field. PHF + dispatch routes the four commands
to `hash::{hexpire,hpexpire,hexpireat,hpexpireat}`; AOF replay already handled
by the phase-200 intercepts.

Side fix — HashWithTtl-aware hash-write path: `HSET`, `HMSET`, `HSETNX`,
`HDEL`, `HINCRBY`, and `HINCRBYFLOAT` previously returned `WRONGTYPE` once
`HEXPIRE` had promoted a hash to the `HashWithTtl` encoding. Extended
`Database::get_or_create_hash` / `get_or_create_hash_listpack` to handle
`HashWithTtl` (returning the inner `fields` map / `Ok(None)` respectively).
Added `Database::hash_clear_field_ttls` (called by HSET/HMSET on overwrite,
no-op on plain `Hash`) and `Database::hash_delete_field` (used by HDEL —
removes from both the fields map and the per-field TTL sidecar, downgrades
back to plain `Hash` when the last TTL is dropped). HSETNX correctly leaves
existing TTLs intact; HINCRBY / HINCRBYFLOAT preserve TTL on the incremented
field.

### Fixed — Test infra: txn_kv_wiring flake diagnosis

- `test_txn_commit_wal_crash_recovery` previously masked moon-server crashes
  as "Connection refused (os error 61) after 60s" because the spawned moon
  binary's stdout / stderr were piped to `Stdio::null()`. Hardened the
  child-process harness:
  - `ChildGuard::spawn` now redirects child stdout/stderr to
    `moon-phase-{1,2}.log` inside the per-test temp dir.
  - `ChildGuard::poll_exit` checks `try_wait()` inside the connection retry
    loop — if the child exited, the test fails immediately with the exit
    status instead of timing out for a useless 60 s.
  - `ChildGuard::dump_log` is called on every failure path
    (`wait_for_server` timeout, `connect_redis_with_retry` timeout,
    child crash), so the CI log records the actual server output.
  - `wait_for_server` deadline widened from 5 s → 15 s, matching the
    realistic CI-runner spawn + WAL boot envelope.
- No semantic change for the happy path. Future flakes become diagnosable
  instead of silently retrying for 60 s.

### Docs — Storage Format v1 commitment (phase 192, PR #115)

- `docs/STORAGE-FORMAT-V1.md` — public on-disk format contract for v0.2.x.
  Documents the WAL v3 / RDB v2 / AOF multi-part sub-formats as a single
  "storage format v1" umbrella with explicit forward-read, reverse-read,
  crash-recovery, and migration guarantees through ≥18 months of LTS.
  Adds cross-reference doc-comments to `src/persistence/aof_manifest.rs`,
  `snapshot.rs`, and `wal_v3/segment.rs` pointing readers at the canonical
  on-disk markers. Reserves a `--storage-format <v1>` CLI flag for the
  follow-up code PR closing issue #103's second checkbox.

### Added — Hash-Field TTL primitive (phase 195, PR #116)

- `RedisValue::HashWithTtl { fields, ttls }` storage variant + borrowed
  `RedisValueRef::HashWithTtl` view. Per-field TTL sidecar (`BTreeMap<Bytes, u64>`)
  carries absolute unix-ms expiry alongside an unchanged `HashMap<Bytes, Bytes>`
  field map. `OBJECT ENCODING` reports `hashtable`.
- `Database::hash_set_field_ttl(key, field, abs_ms, cond)` — Valkey 9.0 parity
  result codes (0 missing / 1 set / 2 deleted-on-set / -2 cond not met).
  NX / XX / GT / LT semantics matching Valkey (non-volatile is +∞ for GT/LT).
  Auto-promotes `Hash` / `HashListpack` → `HashWithTtl` on first per-field TTL;
  past-expiry short-circuits to in-place delete with code 2.
- `Database::hash_persist_field(key, field)` — clears one field's TTL;
  downgrades back to plain `Hash` when the last TTL is removed.
- `Database::hash_get_field_ttl_ms` + `Database::hash_field_state` — read
  helpers returning the tri-state `FieldState::{Missing, NoTtl, Ttl(u64)}`
  consumed by phase-198 HTTL / HEXPIRETIME / HPTTL / HPEXPIRETIME read commands.
- Additive `HashWithTtl` match arms in `command::key::should_async_drop`,
  `server_admin::estimate_serialized_length`, `eviction::evict_one_async_spill`,
  `tiered::kv_spill`, `tiered::kv_serde`, `persistence::rdb::write_entry`,
  `persistence::redis_rdb`, and `persistence::aof::generate_rewrite_commands`.
  Persistence-side TTL payload lands in PR #117 (phase 200); arms here are
  TTL-stripping placeholders so HEXPIRE handlers (phase 196) cannot be
  merged ahead of the persistence wiring.

### Added — Hash-Field TTL persistence wiring (phase 200, PR #117)

- **RDB v2** — bumped `RDB_VERSION 1 → 2`. New per-hash trailer
  `[ttl_count u32][field, ttl_ms u64]*` follows every `TYPE_HASH` body
  (count=0 for non-TTL hashes). Reader accepts both v1 and v2 files;
  `count_entries_per_db` / `skip_entry` / `read_entry_zero_copy` /
  `read_entry` all plumb a `has_hash_ttl_trailer` flag so the format
  is parsed correctly on both code paths (file load + in-memory bytes).
- **Shard RDB V3** — `SHARD_RDB_VERSION 2 → 3` with the same trailer
  plumbed through `rdb::read_entry`. V1 (legacy) / V2 (PITR LSN+ts) /
  V3 (TTL trailer) all load via per-version preamble + min-file-size
  branching.
- **Tiered KV serde** — per-hash trailer on disk-offload blobs. Plain
  Hash + HashListpack serializers now append `ttl_count = 0`;
  HashWithTtl serializer writes the real trailer. Deserializer treats
  a truncated trailer as zero TTLs for graceful migration from
  pre-trailer in-process spill blobs.
- **BGREWRITEAOF** — `RedisValueRef::HashWithTtl` arm emits
  `HSET key f1 v1 f2 v2 ...` followed by per-field
  `HPEXPIREAT key abs_ms FIELDS 1 field`, one per TTL'd field. Per-
  field framing keeps the replay shim simple (single-field parse).
- **Replay shim** — `CommandReplayEngine::replay_command` intercepts
  `HEXPIRE` / `HPEXPIRE` / `HEXPIREAT` / `HPEXPIREAT` / `HPERSIST`
  (case-insensitive) **before** `command::dispatch` and routes
  directly to `Database::hash_set_field_ttl` / `hash_persist_field`.
  This bypasses the phase-196 command handlers (which do not yet
  exist) so crash-restart restores per-field TTLs from any AOF stream
  emitted by either user-typed HEXPIRE or BGREWRITEAOF.
- **Redis-compat RDB** — emits `tracing::warn!` when dropping per-field
  TTLs on Redis-compat export. Redis 7.4 hash-field-TTL opcode
  emission deferred to a future cross-vendor compat phase.

### Added — Tier 2 Lane A (PR #100)

- **T2.1** `c381b31` — `SWAPDB` cross-shard atomic swap via `ShardMessage::SwapDb`;
  WAL-durable; BGREWRITEAOF concurrency guard; restart-replay test.
- **T2.2** `4958dc9` — `MOVE key db` with `with_two_dbs_locked` (lower-index-first
  lock ordering); WAL-durable; intercept in all four handler paths.
- **T2.3** `bbc6117` — `COPY ... DB n` cross-database; reuses `with_two_dbs_locked`;
  WAL-durable.
- **T2.4** `f538589` — `CLUSTER REPLICAS` / `CLUSTER SLAVES`; shared
  `format_node_line(node, self_node_id)` helper extracted from `CLUSTER NODES`.
- **T2.5** `ebd240a` — `CLUSTER COUNT-FAILURE-REPORTS`; counts non-stale
  `pfail_reports`; exposes `DEFAULT_NODE_TIMEOUT_MS` as `pub(crate)`.

### Fixed

- **PERF** `608e2d1` — collapse duplicate `is_write` PHF gate on MOVE/COPY hot
  path; restores s=1 SET p=1 throughput (−9.5 % → +0.9 % vs. merge base).
- **CR** _(this PR)_ — SWAPDB now runs **after** the ACL gate in `handler_monoio`,
  closing a runtime-specific authorization bypass.
- **CR** _(this PR)_ — `with_two_dbs_locked` and `ShardDatabases::swap_dbs` now
  hard-assert non-equal indices in release builds, preventing same-index
  self-deadlock.
- **CR** _(this PR)_ — `SWAPDB` strict arity (exactly two args) across all three
  handlers; rejects `SWAPDB 0 1 extra` with the canonical wrong-arity error.
- **CR** _(this PR)_ — `DashTable::Segment::insert_or_update_at` now sets
  `has_non_home_keys = true` whenever the chosen free slot is in a non-home
  group; fixes a latent miss where `find()` could not locate a fallback-placed
  key on subsequent lookups.
- **CR** _(this PR)_ — Local `MOVE` / `COPY` AOF append is gated on
  `Frame::Integer(1)` (success) rather than `!Error`, matching the
  `handler_single` behavior and suppressing no-op `:0` log entries.
- **CR** _(this PR)_ — `MOVE`, `COPY ... DB n`, and `SWAPDB` are rejected with
  `ERR_TXN_CROSS_SHARD` while an `active_cross_txn` is in flight; previously the
  intercepts bypassed undo/intents bookkeeping and escaped `TXN.ABORT` rollback.
- **CR** _(this PR)_ — `spsc_handler` `MOVE`/`COPY` arms call
  `refresh_now_from_cache` on both source and destination DBs before
  `move_core`/`copy_core`; fixes expired-key visibility skew on the local-write
  path.

### Refactor

- `e429b2b` — `src/storage/dashtable/segment.rs` (1587 LOC) split into
  `segment/{mod,find,insert,ops}.rs`; mechanical refactor, zero semantic change,
  brings all files under the 1500-LOC limit ahead of future hot-path additions.

### Added — Point-in-Time Recovery (PITR)

- **P0** `ac3aa92` — `WalWriterV3::new()` now scans existing `.wal` segments on
  open and resumes `next_lsn` from `max_observed_lsn + 1`. Fixes a latent
  durability bug where LSN reset to 1 on every restart and blocked both PITR
  and CDC.
- **P1** `e1e9bda` — `FileEntry` extended with `last_modified_lsn` (offset
  48..56, struct size 48 → 56). Manifest `format_version` bumped to v2;
  backward-compat reader synthesizes `last_modified_lsn = created_lsn` for v1
  entries. New CLI flags `--recovery-target-lsn` and `--recovery-target-time`.
- **P2** `25ece4b` — Snapshot header bumped to `SHARD_RDB_VERSION = 2`,
  embedding `last_lsn` and `created_at_unix_ms`. v1 snapshots load with
  `last_lsn = 0` and are conservatively skipped by PITR. Adds
  `read_snapshot_metadata` peek API.
- **P3a** `048a883` — `replay_wal_v3_dir_until(stop_at_lsn)` and
  `resolve_target_time_to_lsn()` (scans `TemporalUpsert` / `GraphTemporal`
  records for `system_from` anchors).
- **P3b** `a496413` — `recover_shard_v3_pitr()` honors `target_lsn`: skips
  snapshots whose `last_lsn` is unknown or past the target, then stops replay
  at the cutoff without advancing `wal_flush_lsn`.

See `docs/guides/pitr.md` for operator usage.

### Added — Change Data Capture (CDC)

- **C1** `b271e21` — `WalTailReader` with resumable `TailCursor { segment_seq,
  byte_offset, last_lsn }`. Re-stats segment metadata on each call for
  torn-write safety, auto-advances on segment rotation.
- **C2** `e97b80d` — New `src/cdc/` module with typed `CdcEvent` enum,
  `decode_wal_record()` translator, and hand-rolled
  Debezium-compatible JSON envelope serializer
  (`encode_debezium`). Non-UTF-8 keys fall back to `{"_b64":"..."}`.
- **C3 v1** `bf4230b` — `CDC.READ <wal_dir> <from_lsn> [LIMIT N]` polling
  command. Returns RESP array `[next_lsn, env1, env2, ...]`; default LIMIT
  256, hard ceiling 10 000. Idle response is `[from_lsn]` (length 1) — stable
  no-new-data signal.

See `docs/guides/cdc.md` for consumer integration.

### Added — Embedded sharded server

- **PR #95** — `server::embedded::run_embedded(config, cancel)` exposes the
  full sharded handler (with TXN.* cross-store transactions) to in-process
  embedders such as `helios moon-daemon`. The existing `run_with_shutdown`
  drives `handler_single`, which deliberately does not implement TXN.
  Embedded mode skips TLS, console, cluster bus, admin port, and multi-part
  AOF manifest replay; it does include per-shard RDB + WAL recovery,
  graph/temporal/workspace/MQ WAL replay, SO_REUSEPORT, NUMA pinning, and
  cancel-driven graceful shutdown.

### Fixed — PR #96 test deflake + tokio AOF replay

- `main.rs` AOF recovery: gated the multi-part AOF manifest replay block to
  `#[cfg(feature = "runtime-monoio")]`. Under tokio, the legacy single-file
  `appendonly.aof` is loaded via the v2 recovery chain; the multi-part loader
  no longer creates an empty manifest at first boot that wiped v2-loaded
  state on the next restart (every tokio SET was lost on restart).
- `tests/txn_kv_wiring.rs`: made `test_txn_commit_wal_crash_recovery`
  runtime-agnostic by polling for either the monoio multi-part `.base.rdb`
  artifact or the tokio single-file `appendonly.aof`. Added
  `connect_redis_with_retry` helper to bound and retry the post-bind RESP
  handshake (was racing the shard accept loop, surfacing as EAGAIN on Linux
  and ECONNRESET on macOS CI).

### Fixed — PR #95 review hardening

- `main.rs` `malloc_conf` symbol: replaced the union-based unsafe pun with
  a `#[repr(transparent)]` `Sync` wrapper around a `c"..."` literal.
- `command/server_admin.rs` `get_vsz_bytes`: replaced four `unsafe`
  libc::{open,read,close,sysconf} blocks with safe `/proc/self/status`
  parsing on the cold MEMORY DOCTOR path.
- `main.rs` arena scan now uses `env::args_os()` so non-UTF-8 argv no
  longer panics before clap reports the error.
- `server/embedded.rs` shutdown sequence: cancel → join shard threads →
  drop the outer `aof_tx` → join the AOF thread, so the writer never
  exits while shards are still queuing appends. Thread join panics are
  now propagated through the function result.
- `storage/db.rs` annotated two `.expect()` calls in `Database::set`'s
  `insert_or_update` closures for the hot-path unwrap ratchet.

### Deferred to v0.2 follow-ups

- **P3c** — wire `SnapshotState::set_last_lsn(wal_flush_lsn)` into the live
  persistence tick so freshly-written snapshots embed their LSN (currently
  PITR falls back to full WAL replay when only v1-shaped snapshots exist).
- **C3b** — push-based `CDC.SUBSCRIBE` over RESP3 Push frames, per-shard
  subscriber registry hooked into `wal_append_and_fanout`, slow-consumer
  disconnect policy. Envelope format unchanged.
- Integration suites (`tests/pitr_integration.rs`,
  `tests/cdc_integration.rs`), `scripts/test-pitr-cdc.sh` end-to-end smoke,
  and the benchmark gates (PITR restart ±10%, CDC ≥100K events/s/shard,
  write p99 ±5%).

## [0.1.12] — 2026-05-12

Performance & memory observability release. 50 commits since v0.1.11, no
public API breaks, no on-disk format change. Validated on OrbStack `moon-dev`
(2026-05-12) and locally green for both `runtime-monoio` and
`runtime-tokio,jemalloc`.

### Performance — DashTable hot-path (Phase 189, PERF-07 + PERF-09)

- **Pre-sized DashTable.** `DashTable::with_capacity()` plus the new
  `--initial-keyspace-hint <N>` flag size the segment array up front so
  steady-state operation hits zero `split_segment` calls. Pre-size
  invariant test confirms zero splits at 1 M keys. The 27 % CPU spent
  in `split_segment` during SET p=16 (PERF-07) is fully eliminated.
- **`Database::set` rewrite.** New `DashTable::insert_or_update` /
  `Segment::insert_or_update_at` single-probe helpers replace the
  previous `find + remove + insert` triple-probe pattern.
- **`Segment::find` fallback elimination + force-inlined SIMD.** The
  cold "key spilled to non-home group" fallback path is removed once
  `has_non_home_keys` is invariant-tracked on insert (the
  `insert_or_update_at` change above already maintains the flag);
  the SIMD probe helpers are `#[inline(always)]`. PERF-09
  attributed 12.65 % of `Segment::find` self-time to the fallback;
  remaining cost is the irreducible per-hit `memcmp` confirm
  (threshold amended to <3 %). 1 M-key correctness gate validates
  zero false positives/negatives.

### Performance — Memory observability (Phase 190)

- **`moon_memory_bytes{kind=…}` Prometheus gauge.** Seven subsystem
  labels — `dashtable`, `hnsw`, `csr`, `wal`, `sealed_replication_backlog`,
  `allocator_overhead`, and the rolled-up `total`. Updated every
  scrape via a single hook so the sum reconciles to `RSS` within the
  CI tolerance window.
- **`MEMORY DOCTOR` full schema.** Multi-line RESP response covering
  every subsystem, the rolled-up total, and a derived `allocator_overhead`
  pseudo-kind (RSS − Σ subsystems). Adds operator triage signal beyond
  the legacy single-line summary.
- **`resident_bytes()` trait** implemented across `Database`,
  `DashTable`, `VectorStore` (HNSW + IVF), `GraphStore` (CSR + SlotMap),
  `WalWriter`, `ReplicationBacklog` (sealed-segment side), and
  `AllocatorOverhead`. Zero-allocation, on-demand poll.
- **Memory steady-state CI job.** `scripts/bench-memory-steady-state.sh`
  + baseline fixture; gate widened to `±10 %` on RSS / Σ ratio after a
  Linux-CI tolerance pass.

### Changed — Allocator UX (Phase 191)

- **jemalloc `narenas:8` cap** with `--memory-arenas-cap <N>` CLI
  override. Caps the per-CPU arena explosion that inflates VSZ on
  high-core hosts; mostly a cosmetic fix on Linux containers but
  produces a meaningfully tighter `top`/`ps` reading for operators.
- **Tri-state allocator selection.** New `mimalloc-alt` cargo
  feature alongside the existing `jemalloc` / `mimalloc` (fallback)
  paths; mutually exclusive at compile time. A/B benchmark script
  `scripts/bench-allocator-ab.sh` ships with the release.
- **`docs/OPERATOR-GUIDE.md` — Memory Accounting section.** Documents
  the VSZ-vs-RSS distinction, MEMORY DOCTOR field-by-field, and the
  `--memory-arenas-cap` / `mimalloc-alt` tuning knobs.

### Added — Dispatch Observability (Phase 177)

- **`moon_dispatch_path_total{path=...}` Prometheus counter**: four-way classification of every command by shard-routing decision — `local_inline` (SIMD fast path), `local` (standard local branch), `cross_read_fast` (RwLock shared-read bypass of SPSC), `cross_spsc` (deferred cross-shard write via `PipelineBatchSlotted`). Ratio `cross_spsc / Σ` is the ground-truth signal for dispatch-layer optimization work. Zero-allocation hot-path overhead (`&'static str` labels, `#[inline]` with early-return on `!METRICS_INITIALIZED`). Verified on macOS + Linux: counter sums close exactly to driven traffic, no overcount.

### Changed

- **`text-index` is now a default feature.** BM25 full-text search (`FT.SEARCH` BM25 mode), `FT.AGGREGATE`, and three-way RRF hybrid fusion are included in all standard builds. No longer requires `--features text-index`. To exclude it (e.g. minimal embedded builds): `--no-default-features --features runtime-monoio,jemalloc,graph`.

### Added — SDK Validation

- **Python SDK `sdk/python/examples/validate.py`**: End-to-end live validator for all SDK sub-clients: ping, strings, counter, hash, list, set, zset, vector index lifecycle, graph engine, session search, semantic cache, text search (BM25 + aggregate + hybrid), and server info. Result against Moon with `text-index`: **114 PASS / 0 FAIL / 0 SKIP**. Gracefully skips text sections when server built without `text-index`.
- **Rust SDK `sdk/rust/examples/validate.rs`**: Re-validated against `text-index` build — **85 PASS / 0 FAIL**.

### Fixed — Python SDK

- **`moondb.graph._parse_neighbors`**: server returns alternating `[edge_map, node_map, ...]` as flat key-value arrays (`b'id'`, `int`, `b'src'`, `int`, `b'dst'`, `int`, …). Previous parser expected positional `[node_id, label, props]` — caused `int() on b'id'` crash. Now correctly identifies node entries by `labels` key and parses them from the flat kv format.

### Fixed — CI Hygiene

- **`tests/pipeline_auto_index.rs`**: tighten outer cfg from `runtime-tokio` to `all(runtime-tokio, text-index)` so the file compiles to zero tests when text-index is disabled. Previously the file compiled but the FT.SEARCH text fast path was `#[cfg]`-ed out, causing `@name:corpus` queries to fall through to the KNN-only parser and panic with "invalid KNN query syntax".
- **4 FT unwraps**: add inline `#[allow(clippy::unwrap_used)]` with invariant justifications in `vector_search/ft_text_search.rs` (3 sites inside `apply_post_processing` where `do_summarize` / `do_highlight` implies the Option is Some) and `handler_monoio/ft.rs:165` (`is_text` was derived from `query_bytes.as_ref().map_or(false, _)`). Restores the audit-unwrap baseline to 0.

### Compatibility

- **Wire protocol**: unchanged. Drop-in replacement for v0.1.11.
- **Persistence on-disk format**: unchanged.
- **Default feature set**: `text-index` is now on by default. Minimal
  embedded builds need an explicit `--no-default-features --features
  runtime-monoio,jemalloc,graph`.

## [0.1.11] — 2026-04-27

Hot-path perf release — eliminates two atomic-CAS hot paths in the write
dispatch loop discovered via ARM `perf annotate` on c4a-16 (GCloud Axion).
Empirically validated on the same hardware: **8-shard SET p=64 c=200
throughput 1.84M → 3.87M RPS (+110%)** when run with `--disk-offload disable`,
or **+15% under default flags**. No public API change.

Sprint 3.5a and 3.5b from `.planning/rfcs/v02-enterprise-architecture.md`.

### Performance — Sprint 3.5a: Lock-free `is_replica` mirror

`try_enforce_readonly` was taking `RwLock::try_read()` on
`Arc<RwLock<ReplicationState>>` for every command before dispatch — an
atomic CAS on the per-command hot path. ARM annotate showed `mov w8, #0xfffd;
cmp w11, w9` consuming **84% of self-time inside the function** (10% of
total CPU on 8-shard SET p=64).

Fix: replace the per-command lock probe with a single
`AtomicBool::load(Acquire)`.

- **`ReplicationState::is_replica_mirror: Arc<AtomicBool>`** — lock-free mirror
  of `role == Replica { .. }`, kept in sync via the new
  `ReplicationState::set_role(&mut self, role)` method (single owner of the
  invariant).
- **`ConnectionContext::is_replica_mirror: Option<Arc<AtomicBool>>`** —
  snapshotted from `ReplicationState` once at connection setup; per-command
  `try_enforce_readonly` is now just an atomic load with no lock
  acquisition.
- **All 6 production `rs_guard.role = ...` sites** in `handler_single`,
  `handler_monoio/dispatch`, and `handler_sharded/dispatch` migrated to
  `set_role()`. Test fixtures in `replication/handshake.rs` migrated too so
  the invariant holds in test code.
- Round-5 verification on commit `32f48c4` (c4a-16, 8-shard SET p=64 c=200):
  `try_enforce_readonly` is now **0%** of profile (down from 10%). Sprint
  3 acceptance criterion `<1%` met.

### Performance — Sprint 3.5b: WAL no-op bypass

`wal_append_and_fanout` was acquiring a `parking_lot::Mutex` (replication
backlog) and a `std::sync::RwLock` (replication state) on every write,
even when no replica was connected and no WAL writer existed. ARM annotate
showed `caslb`/`casab` ARM CAS-byte atomics dominating self-time (~21% of
total CPU on 8-shard SET p=64 with `--appendonly no` and zero replicas).

Fix: hoist a single early-return at the top of the function:

```rust
if wal_writer.is_none() && wal_v3_writer.is_none() && replica_txs.is_empty() {
    return;
}
```

The criterion is fully derivable from existing inputs — no new shared
state. Skips both the backlog `Mutex::lock` and the `repl_state`
`RwLock::read` on the cold path.

- **Round-5 verification with `--disk-offload disable`** (commit `32f48c4`):
  `wal_append_and_fanout` is now **0.05%** of profile (down from 21%).
  Sprint 3 acceptance criterion `<2%` met.
- **Operator note**: the bypass only fires when (a) `--appendonly no`,
  (b) `--disk-offload disable`, AND (c) no replicas are connected. Default
  builds with disk-offload on (the production default) keep
  `wal_v3_writer = Some(_)` and the function continues to do real WAL v3
  work — that path is unaffected.

### Throughput Impact

| Workload (8-shard SET p=64 c=200, c4a-16, frame pointers ON) | v0.1.10 | v0.1.11 | Δ |
|---|---|---|---|
| Default flags | 1.84M RPS | **2.11M RPS** | **+15%** |
| `--disk-offload disable` | ~2.95M RPS (projected) | **3.87M RPS sustained** | **+31%** |
| `try_enforce_readonly` self-time | 10.0% | **0%** | -10pp |
| `wal_append_and_fanout` self-time | 21.2% | **0.05%** (with disk-offload disable) | -21.1pp |

Production builds without frame pointers should clear 5.5–6.5M RPS at the
same flag set. Round-4 baseline data: `memory/benchmark_perf_round4_2026_04_27.md`.
Round-5 verification data: `memory/benchmark_perf_round5_2026_04_27.md`.

### Tests Added

- `replication::state::tests::test_set_role_updates_is_replica_mirror`
- `replication::state::tests::test_is_replica_mirror_default_false`
- `shard::spsc_handler::wal_append_tests::test_wal_append_bypass_when_no_writers_no_replicas`
- `shard::spsc_handler::wal_append_tests::test_wal_append_writes_backlog_when_replicas_present`

All 2450 lib tests passing locally (`cargo test --no-default-features
--features runtime-tokio,jemalloc --lib`); clippy clean (`cargo clippy
--no-default-features --features runtime-tokio,jemalloc -- -D warnings`).

### Drive-by Fixes

- **`src/shard/mod.rs`**: pre-existing test compile failure where two
  `drain_spsc_shared` call sites passed `&mut None` for `repl_backlog`
  instead of a `SharedBacklog` (`Arc<Mutex<Option<...>>>`). Fixed by
  constructing `Arc::new(parking_lot::Mutex::new(None))` in the test fixture.
  This was blocking `cargo test --lib` on `main` independent of this
  release.
- **`src/shard/dispatch.rs`**: pre-existing clippy `doc_lazy_continuation`
  error on the `TextSearchPayload` doc comment, blocking
  `cargo clippy -- -D warnings` on `main`.

### Compatibility

- **Wire protocol**: unchanged. Drop-in replacement for v0.1.10.
- **Public API**: only additive (`ReplicationState::set_role`,
  `ReplicationState::is_replica_mirror` field). No breaking changes.
- **Persistence on-disk format**: unchanged.
- **Replication wire format**: unchanged.

## [0.1.10] — 2026-04-23

Stable replication marker. **Single-shard PSYNC2 wired end-to-end and
production-ready** for `--shards 1` master with any `--shards N` replica
topology. Multi-shard master PSYNC is scheduled for v0.2 (see
`.planning/rfcs/multi-shard-replication-design.md`).

- **Replication** (`081c43b`): single-shard master PSYNC2 end-to-end wired,
  REPLCONF validated, `master_link_status` reports the actual handshake
  state instead of the legacy `up` stub.
- **Performance**: batch-level eviction gate; `try_handle_*` paths
  `#[inline]`-ed; DashTable carries through the v0.1.10 pre-size
  groundwork (capacity hint + headroom).
- **Docs**: BENCHMARK.md §2.7 updated with the 2026-04-22 GCloud
  re-measurement; v0.1.x replication scope documented under
  `docs/guides/clustering.mdx#replication`.

## [0.1.9] — 2026-04-19

**Lunaris Retriever Gap Closure.** Every v0.1.8 client-side fallback in
the Lunaris SDK is now closed so `HybridRRFRetriever` (dense path),
`GraphFirstRetriever`, and `PathReasoningRetriever` run Moon-native.

- **Phase 167 CYP-01/02**: Cypher `CREATE` / `MERGE` writes participate
  in `CrossStoreTxn` via `record_graph()`; `TXN.ABORT` rolls them back.
- **Phase 168 CYP-03/06**: `coalesce()` built-in + single-hop edge-var
  binding in variable-length `EXPAND`.
- **Phase 169 CYP-04/05**: `shortestPath()` parser + Dijkstra executor
  bridge with path-variable binding.
- **Phase 170 HYB-01/02/04**: `FT.SEARCH HYBRID` dense stream honours
  `as_of_lsn`.
- **Phase 171 SCAT-01/02/03**: `ShardMessage::VectorSearch` +
  `FtHybridPayload` carry `as_of_lsn` for multi-shard `AS_OF` correctness.
- **Phase 172 PIPE-01/02/03**: pipeline-aware HSET auto-indexing
  regression guard (3-test suite).

Audit status: **PASSED_WITH_DOCUMENTED_DEFERRALS**. 15 / 20 requirements
fully satisfied; HYB-03 BM25 MVCC deferred and closed in v0.1.10
follow-up (G-1); Phase 173 hygiene HYG-02 handler split RFC'd.

Stats: 6 phases shipped, 17 plans, 27 files changed, +2924 / −376 LOC.

## [0.1.8] — 2026-04-18

### Added — Cross-Store ACID Transactions (Phases 157, 161-163)

- **TXN.BEGIN**: Start a cross-store transaction — buffers KV, vector, and graph writes as intents.
- **TXN.COMMIT**: Commit all changes atomically with WAL record (`XactCommit` 0x34) for crash recovery.
- **TXN.ABORT**: Roll back all changes via undo-log replay with before-images.
- **KvWriteIntents**: Sparse MVCC side-table for uncommitted KV writes during transactions.
- **DeferredHnswInserts**: Vector index inserts deferred until commit, avoiding partial graph states.
- **UndoLog**: SmallVec-based KV rollback with before-image recording for all mutation types.
- **WAL transaction records**: `XactBegin` (0x33), `XactCommit` (0x34), `XactAbort` (0x37) with crash recovery replay.
- **Mutual exclusion**: `TXN` and `MULTI/EXEC` cannot be mixed (enforced at handler level).

### Added — Bi-Temporal MVCC (Phase 158)

- **TEMPORAL.SNAPSHOT_AT**: Record wall-clock → WAL LSN binding for point-in-time queries.
- **TEMPORAL.INVALIDATE**: Set `valid_to` on a graph entity (NODE or EDGE) for temporal visibility control.
- **FT.SEARCH AS_OF**: Query vector indexes at a historical timestamp via LSN resolution.
- **GRAPH.QUERY VALID_AT**: Execute Cypher queries against graph state valid at a specific timestamp.
- **TemporalRegistry**: BTreeMap-backed wall-clock → LSN mappings with O(log n) range lookups.
- **TemporalKvIndex**: Sparse versioned KV index with lazy initialization.
- **CSR segment format v2**: Bi-temporal `NodeMeta` with `valid_from`/`valid_to` fields.
- **WAL temporal records**: `TemporalUpsert` (0x35) and `GraphTemporal` (0x36) for crash recovery.

### Added — Workspace Partitioning (Phase 159)

- **WS CREATE**: Create a workspace with UUID v7 (time-ordered, 74-bit random). Name max 64 bytes.
- **WS DROP**: Delete a workspace and its registry entry.
- **WS AUTH**: Bind a connection to a workspace — all subsequent commands transparently prefixed.
- **WS INFO**: Return workspace metadata (name, creation timestamp).
- **WS LIST**: Enumerate all registered workspaces.
- **Transparent key rewriting**: `workspace_rewrite_args()` injects `{ws_hex}:` hash tag prefix on key arguments and strips it from responses.
- **WorkspaceRegistry**: Per-shard metadata with creation timestamps and WAL persistence.

### Added — Durable Message Queues (Phase 160)

- **MQ CREATE**: Create a durable queue with `MAXDELIVERY` (default 3) and `DEBOUNCE` options.
- **MQ PUSH**: Enqueue messages with field/value pairs (returns stream ID).
- **MQ POP**: Claim messages with optional `COUNT` (defaults to 1). Increments delivery counter.
- **MQ ACK**: Acknowledge messages by stream ID.
- **MQ DLQLEN**: Return dead-letter queue depth.
- **MQ TRIGGER**: Register debounced trigger callbacks with configurable debounce interval.
- **MQ PUBLISH**: Transactional enqueue within a `TXN` block — applied on `TXN COMMIT`.
- **Dead-letter queue**: Automatic DLQ at `{queue_key}::mq:dlq` after exceeding `MAXDELIVERY` attempts.
- **TriggerRegistry**: Debounced callback execution via pub/sub publish.
- **WAL recovery**: `replay_mq_wal()` with cursor rollback for durable queue state restoration.

### Added — Handler Parity

- All TXN/TEMPORAL/MQ/WS commands wired into `handler_monoio.rs`, `handler_sharded.rs`, `uring_handler.rs`, and `handler_single.rs`.
- 12 KV transaction integration tests, 14 MQ integration tests, workspace cross-shard dispatch tests.
- TEMPORAL/MQ/WS/TXN entries added to `test-commands.sh` and `test-consistency.sh`.

## [0.1.7] — 2026-04-17

### Added — BM25 Full-Text Search Engine (Phases 149-156)

- **BM25 inverted index**: Full-text search with multi-field boosting and per-field term frequency tracking.
- **TEXT field type**: Unicode tokenization with stemming and normalization in `FT.CREATE`.
- **TAG field type**: Categorical tag filtering with multi-value support (`@field:{val1|val2}`).
- **NUMERIC field type**: Range filtering (`@field:[min max]`) in `FT.CREATE` and `FT.SEARCH`.
- **FT.AGGREGATE**: Aggregation pipeline with `GROUPBY`/`REDUCE`, scatter-gather across shards, HLL `COUNT_DISTINCT`.
- **Three-way RRF hybrid fusion**: Combines BM25 + dense vector + sparse vector results via Reciprocal Rank Fusion.
- **Typo tolerance**: FST Levenshtein fuzzy matching (`%%term%%`) and prefix search (`term*`).
- **HIGHLIGHT/SUMMARIZE**: Post-processors for formatting search results with matched term highlighting.
- **Multi-shard DFS global IDF**: Distributed frequency statistics for accurate BM25 scoring regardless of shard count.
- **FT.DROPINDEX DD**: Atomic index + document deletion flag — deletes all hash keys matching index prefixes.
- **Python SDK text module**: `client.text.text_search()`, `client.text.aggregate()`, `client.text.hybrid_search()` with typed pipeline DSL.
- **LangChain/LlamaIndex hybrid adapters**: Framework integrations updated for three-way hybrid search.

### Statistics

- 8 phases (149-156), 27 plans, 26 requirements, 122 commits.

## [0.1.6] — 2026-04-15

### Added — AI-Native Data Primitives

- **Multi-field vector indexes** (`FT.CREATE`): multiple VECTOR fields per index, per-field segment storage, field-targeted `@field_name` syntax in `FT.SEARCH` KNN clause.
- **Sparse vector module** (`src/vector/sparse/`): inverted index with `SparseStore`, enabling BM25-style sparse retrieval alongside dense HNSW.
- **Hybrid dense+sparse search**: `SPARSE` clause in `FT.SEARCH` with Reciprocal Rank Fusion (RRF) for combining dense and sparse results.
- **Text index** (`src/vector/text_index.rs`): Unicode tokenization pipeline with stemming and normalization, feature-gated under `text-index`.
- **Boolean and geo filter expressions**: `BoolEq` and `GeoRadius` filter variants with evaluation logic in `FilterExpr`.
- **FT.RECOMMEND**: centroid-based recommendation over vector indexes — computes centroid of seed vectors and returns nearest neighbors.
- **FT.NAVIGATE**: multi-hop knowledge graph navigation from vector search results, bridging vector and graph queries.
- **FT.EXPAND**: GraphRAG expansion command — traverses graph edges from vector search results to discover related entities, with configurable depth.
- **FT.CACHESEARCH**: semantic cache-or-search command — returns cached results on similarity hit, falls back to full search on miss.
- **FT.CONFIG SET/GET**: runtime configuration for per-index knobs (e.g., `AUTOCOMPACT` toggle).
- **SESSION clause**: session-scoped filtering in `FT.SEARCH` for multi-tenant and agent memory isolation.
- **RANGE threshold post-filter**: distance threshold filtering in `FT.SEARCH` results.
- **LIMIT pagination**: `LIMIT offset count` support in `FT.SEARCH` with multi-segment merge.

### Added — Production Infrastructure

- **moondb Python SDK** (`python/moondb/`): high-level client with vector, graph, session, cache, and framework integrations (LangChain, LlamaIndex).
- **5 quickstart examples**: RAG, semantic cache, GraphRAG, AI agent tools, memory engine — all using real MiniLM embeddings.
- **Production deployment guide** (`docs/production-guide.md`): configuration reference, TLS setup, monitoring, ACL, tuning.
- **Dockerfile improvements**: OCI labels, admin port exposure, production defaults.
- **docker-compose.yml**: production configuration with resource limits, health checks, ulimits.
- **Prometheus metrics**: counters and histograms for v0.1.6 commands (FT.RECOMMEND, FT.NAVIGATE, FT.EXPAND, FT.CACHESEARCH, FT.CONFIG).
- **OpenTelemetry stubs**: `otel` feature flag with tracing infrastructure for future OTLP export.
- **Benchmark script** (`scripts/bench-v0.1.6.sh`): automated benchmarks for new vector search features.

### Added — Graph-Vector Integration

- **EXPAND GRAPH clause** in `FT.SEARCH`: inline graph expansion during vector search with configurable depth.
- **`graph_expand.rs` bridge module**: connects vector search results to graph traversal engine.
- **`key_to_node` mapping** on `NamedGraph`: enables graph expansion from vector search key hashes.

### Fixed — Critical Production Bugs

- **Deadlock in cross-shard HSET auto-index**: `parking_lot::Mutex` re-entry in `PipelineBatch` and `PipelineBatchSlotted` handlers — `shard_databases.vector_store(shard_id)` attempted to re-lock a non-reentrant mutex already held by the caller. Fixed by using the passed-in reference.
- **Auto-index HSET inside MULTI/EXEC**: vector auto-indexing now works correctly within transactions and pipeline batch paths.
- **FT.RECOMMEND filter bug**: filter expressions were not applied correctly to recommendation results.
- **FT.CONFIG/RECOMMEND/NAVIGATE/EXPAND routing**: commands now dispatched correctly in all connection handlers (monoio, tokio, single-threaded).
- **Session deduplication**: fixed duplicate session entries in search results.
- **Inline filter parsing**: corrected parsing of filter expressions in inline command mode.
- **FT.COMPACT, FT.CONFIG, FT.CACHESEARCH metadata**: registered in phf command metadata table for ACL and COMMAND DOCS.

### Fixed — CI/Release Pipeline

- **nfpm download URL**: pinned v2.46.1 with correct `Linux_x86_64` asset name (old `nfpm_linux_amd64.tar.gz` returns 404).
- **SBOM filenames**: `cargo-cyclonedx` v0.5+ writes `.json` not `.cdx.json`.
- **Cosign keyless signing**: added `id-token: write` permission for Sigstore OIDC (was falling back to interactive device flow).
- **Package job race condition**: deb/rpm upload now waits for GitHub release to be created first.
- **upload-artifact**: upgraded v4 → v7 (Node.js 20 deprecation).
- **Test compilation**: added tokio dev-dependency with `process` feature for `blocking_list_timeout.rs` under default (monoio) features.

### Changed

- **Graph enabled by default**: `graph` feature now included in default feature set.
- **Vector index persistence**: v2 format with backward-compatible v1 migration.
- **Memory engine example**: rewritten as 142-line script with real MiniLM embeddings (was 487-line complex agent loop).
- **Clippy 1.94 compliance**: all warnings resolved for Rust 1.94 MSRV.

### Validation

- 2,613+ unit tests pass (release mode, default features).
- 2,139 library tests pass under `runtime-tokio` feature set.
- 184 unsafe blocks, all with SAFETY comments (audit pass).
- 0 unannotated unwraps on hot paths (ratchet pass).
- Zero clippy warnings (default + `runtime-tokio,jemalloc` feature sets).
- `cargo fmt --check` clean.
- 8 fuzz targets in CI.
- Full release pipeline validated: 6 binary targets, Docker image, deb/rpm packages, SBOMs, cosign signatures.

## [0.1.5] — 2026-04-12

### Added — Moon Console (Interactive Data Client)

- **HTTP/WebSocket gateway** (`src/admin/`): REST endpoints (`/api/v1/info`, `/api/v1/command`, `/api/v1/keys`, `/api/v1/key/*`, `/api/v1/memory/treemap`, `/api/v1/hnsw/trace`), WebSocket-to-RESP3 bridge at `/ws/console`, SSE metrics stream at `/sse/metrics` (1 Hz), CORS allowlist, per-IP token-bucket rate limit, HMAC-SHA256 Bearer auth, HTTP/2 support, static file serving via `rust-embed`.
- **React 19 console** (`console/`): 7-view SPA (Dashboard, Browser, Console, Vector Explorer, Graph Explorer, Memory, Help) served at `/ui/`. 50.9 KB gzipped initial bundle (6× under 300 KB target) via Vite 8 + manual chunk splitting (Three.js/Monaco/Recharts lazy).
- **Real-time Dashboard**: 7 widgets (QPS, latency P50/P99, memory, clients, ops by type, keyspace) driven by SSE stream.
- **KV Data Browser**: namespace tree, virtual-scrolled key list (TanStack Virtual), type-specific editors for Strings/Hashes/Lists/Sets/Sorted Sets/Streams, TTL display + edit, bulk delete with toasts.
- **Query Console**: Monaco editor with RESP + Cypher Monarch syntax, 233-command auto-complete, multi-tab, history, Cmd+Enter (current line) / Cmd+Shift+Enter (whole buffer), line-by-line execution for paste safety.
- **Vector 3D Explorer**: UMAP projection in a web worker, HNSW layer overlay, KNN search with distance rings, lasso selection, Three.js r183 + React Three Fiber.
- **Graph 3D Explorer**: force-directed layout (d3-force-3d worker), Cypher editor, node/edge property inspector, hybrid query integration.
- **Memory view**: keyspace treemap (server-aggregated `/api/v1/memory/treemap`), slowlog table, command stats.
- **Built-in Help guide**: 427-line Getting Started tutorial with seed examples.
- **Core admin commands** (`src/command/server_admin.rs`): FLUSHALL, FLUSHDB, DBSIZE, DEBUG OBJECT/SLEEP/JMAP, MEMORY USAGE — closing pre-existing dispatch gaps.
- **Multi-shard SCAN fan-out** (`src/admin/scan_fanout.rs`): composite cursor `{shard_id}:{cursor}` so Browser sees unified keyspace.
- **Frontend test infrastructure**: 56 Vitest unit tests + 9 Playwright E2E specs. New `scripts/test-integration.sh` harness and `.github/workflows/console-integration.yml`.
- **Admin-port hardening** (`src/admin/{auth,cors,rate_limit,middleware}.rs`): Bearer auth, CORS allowlist, per-IP rate limit.

### Fixed

- **WebSocket request ID echo**: errors now echo back the client's `id`, preventing client-side promise timeouts on malformed input.
- **Console type badges**: `execCommand` response was returning the full `{result, type}` envelope; now unwraps `.result` correctly.
- **Multi-line paste in Console**: Cmd+Enter now executes the current line only (redis-cli paste behavior). Cmd+Shift+Enter executes the whole buffer line-by-line.

### Validation

- 101+ Rust unit/integration tests pass on both `runtime-tokio` and `runtime-monoio`.
- 56 Vitest tests + 9 Playwright specs pass.
- Zero clippy warnings (default + `runtime-tokio,jemalloc` feature sets).
- `cargo fmt --check` clean.
- 8 fuzz targets in CI.

## [0.1.4] — 2026-04-11

### Added — Graph Engine Integration (v0.1.4, 2026-04-11)

- **Property graph engine** (`src/graph/`, feature-gated under `graph`): segment-aligned CSR storage with SlotMap generational indices, ArcSwap lock-free reads, Roaring validity bitmaps, and Rabbit Order compaction for cache locality. 8,500+ LOC, 319 tests.
- **12 GRAPH.\* commands**: CREATE, ADDNODE, ADDEDGE, NEIGHBORS, QUERY, RO_QUERY, EXPLAIN, VSEARCH, HYBRID, INFO, LIST, DELETE — all with RESP3 Map responses and ACL annotations.
- **Cypher subset parser**: hand-rolled recursive descent with logos lexer, 12 clauses (MATCH/WHERE/RETURN/CREATE/DELETE/SET/MERGE/WITH/UNWIND/CALL/ORDER/LIMIT), parameterized queries ($param), nesting depth limit (64), plan caching.
- **Hybrid graph+vector queries**: graph-filtered vector search, vector-to-graph expansion, vector-guided walk with automatic strategy selection.
- **Traversal engine**: BFS/DFS/Dijkstra with bounded frontiers (100K cap), temporal decay + distance scoring, segment merge reader across mutable + immutable segments.
- **Graph indexes**: per-label/type Roaring bitmaps, boomphf minimal perfect hash (~3 bits/key), property B-tree for range queries.
- **Cross-shard traversal**: scatter-gather via SPSC mesh, graph hash tags for shard co-location, snapshot-LSN forwarding, configurable depth limit.
- **Graph MVCC**: extends existing TransactionManager with graph write intents, snapshot-isolated multi-hop traversal, bounded epoch hold (30s).
- **Graph WAL durability**: RESP-encoded graph commands in per-shard WAL, two-pass replay (nodes before edges), CRC32-validated CSR segment persistence.
- **Cost-based planner**: GraphStats with incremental degree tracking, graph-first vs vector-first strategy selection, P99 hub detection.
- **Criterion benchmarks**: CSR 1-hop 1.02ns, edge insert 64.8ns, 2-hop BFS 4.99µs, CSR freeze 5.12ms, SIMD cosine 384d 33.9ns.
- **Fair comparison benchmark** (`tests/graph_bench_compare.rs`): Moon 2.4x FalkorDB on Cypher MATCH, 19x on native 1-hop, 23x on population.
- **New dependencies**: `slotmap` 1.x (generational indices), `boomphf` 0.6 (MPH), `logos` 0.14 (Cypher lexer, optional).

### Added — High-Impact Redis Command Parity (2026-04-10)

- **COPY command** — atomic key duplication with DESTINATION, REPLACE options (Redis 6.2+).
- **Bit operations** — GETBIT, SETBIT, BITCOUNT (byte/bit range modes), BITOP (AND/OR/XOR/NOT), BITPOS (byte/bit range modes) with read-only dispatch variants.
- **SORT command** — full BY/GET/LIMIT/ALPHA/ASC/DESC/STORE support for lists, sets, and sorted sets.
- **Geospatial commands** — GEOADD (NX/XX/CH), GEOPOS, GEODIST (M/KM/FT/MI), GEOHASH (11-char base32), GEOSEARCH (FROMLONLAT/FROMMEMBER, BYRADIUS/BYBOX, WITHCOORD/WITHDIST/WITHHASH), GEOSEARCHSTORE.
- **CONFIG REWRITE** — atomic write of runtime config to `<dir>/moon.conf` (tmpfile + rename). CONFIG RESETSTAT stub.
- **CLIENT PAUSE/UNPAUSE** — delays command processing with WRITE-only mode support. CLIENT INFO, CLIENT LIST (stub), CLIENT NO-EVICT/NO-TOUCH accepted.
- **MEMORY USAGE/DOCTOR/HELP** — key memory estimation via `estimate_memory()`.
- **Lazyfree threshold** — configurable via `CONFIG SET lazyfree-threshold N` (default 64).
- **GETBIT/SETBIT metadata** — added to PHF command registry.
- **GEOADD/GEOSEARCHSTORE** — added to AOF write commands test list.
- **EXPIREAT/PEXPIREAT** — absolute Unix timestamp expiry (seconds/milliseconds).
- **EXPIRETIME/PEXPIRETIME** — read back absolute expiry timestamp.
- **FLUSHDB/FLUSHALL** — clear all keys in current database.
- **TIME** — server clock as `[seconds, microseconds]`.
- **RANDOMKEY** — return a random key from the database.
- **TOUCH** — refresh LRU/LFU access time without reading value.
- **SHUTDOWN** — dispatch entry (graceful stop via signal handler).
- **BITFIELD** — GET/SET/INCRBY with type specifiers (u8/i16/u32/...), OVERFLOW WRAP/SAT/FAIL.
- **LCS** — Longest Common Substring with LEN option.
- **XSETID** — set stream last-delivered ID without adding entries.
- **GEORADIUS/GEORADIUSBYMEMBER** — deprecated wrappers translating to GEOSEARCH.
- **OBJECT FREQ/IDLETIME/REFCOUNT** — LFU counter, idle seconds, reference count introspection.
- **LOLWUT** — Easter egg returning Moon version.

### Added — Client Connection Security Hardening (2026-04-10)

- **`--maxclients` (P0):** Connection limit with atomic CAS rejection (default 10000, 0=unlimited). Returns `-ERR max number of clients reached` when exceeded.
- **`--timeout` (P0):** Client idle timeout in seconds (default 0=disabled). Disconnects idle clients via `tokio::time::timeout` / `monoio::select!`.
- **`--tcp-keepalive` (P0):** TCP keepalive interval (default 300s, 0=disabled). Sets `SO_KEEPALIVE` + `TCP_KEEPIDLE` on accepted sockets via `socket2`.
- **AUTH rate limiting (P0):** Per-IP exponential backoff on AUTH failures (100ms base, 10s cap, 60s auto-reset). New module `src/auth_ratelimit.rs`.
- **CLIENT LIST / INFO / KILL (P1):** Global client registry with Drop-guard deregister. Redis-compatible output format. Kill by ID/ADDR/USER. New module `src/client_registry.rs`.
- **CLIENT PAUSE / UNPAUSE (P1):** Server-wide pause with ALL/WRITE modes and auto-expiry. New module `src/client_pause.rs`.
- **CLIENT NO-EVICT / NO-TOUCH (P1):** Accepted stubs for Redis compatibility.
- **ACL GENPASS (P1):** Cryptographically secure random password generation (1-4096 bits, hex output).
- **CONFIG GET/SET** support for `maxclients`, `timeout`, `tcp-keepalive` (runtime-mutable).
- **Monoio connection tracking:** Added missing `record_connection_opened` / `record_connection_closed` for accurate `connected_clients` metric.

### Fixed — Deep Review Findings (2026-04-11)

- **DoS protection**: `execute_profile` and `execute_mut` Cypher paths now enforce MAX_HOPS_LIMIT=20 and MAX_RESULT_ROWS=100K (were unbounded).
- **WAL correctness**: Cypher DELETE passes actual LSN to `remove_node`/`remove_edge` (was hardcoded to 0).
- **GRAPH.DROP metadata**: added missing phf dispatch table entry.
- **SAFETY comments**: added to all 7 unsafe SIMD/mmap functions.
- **BFS 30% faster**: scratch buffer reuse in SegmentMergeReader, zero-alloc CsrStorage callback, MergedNeighbor derives Copy.
- **ParallelBfs**: uses plain HashSet on sequential path (was DashSet with 64 shards overhead).
- **Recovery hardening**: CSR manifest path traversal validation, WAL embedding dimension cap (65536), LSN saturating_add.
- **CI optimized**: consolidated 26 jobs → 4 per PR, concurrency groups cancel superseded runs, fixed org runner group for public repos.

### Fixed — Wave 0-4 Gap Closure (2026-04-09)

- **ZREVRANGEBYSCORE/ZREVRANGEBYLEX correctness bug:** Fixed double-swap of min/max bounds in `zrange_by_score` and `zrange_by_lex` that caused empty results for finite score ranges (e.g., `ZREVRANGEBYSCORE key 3 1`). Added finite-range test to `test-commands.sh`.
- **INFO command enriched:** Clients section now reports `connected_clients`, Memory section reports `used_memory`/`used_memory_human`/`used_memory_rss` (from `/proc/self/status`), Replication section wired to actual `ReplicationState` (role, connected_slaves, master_replid, master_repl_offset).
- **Tracing spans:** Added `#[instrument]` to connection handlers (single, monoio), replication master (tokio, monoio), HNSW compaction, and AOF rewrite — 6 new spans.
- **Replication lag metric wired:** `moon_replication_lag_bytes` Prometheus gauge now updated from `get_replication_info()`.
- **CI supply chain security:** `cargo deny check` + `cargo audit` added to CI pipeline (deny.toml was previously unenforced).
- **Release pipeline:** aarch64-unknown-linux-gnu build added via `cross` for primary production target.
- **Crash matrix expanded:** BGSAVE and BGREWRITEAOF crash cells added (6/7 coverage).
- **Compatibility tests expanded:** Stream (XADD/XLEN/XRANGE/XTRIM), Lua scripting (EVAL/EVALSHA/SCRIPT), and ACL (WHOAMI/LIST) tests added to `redis_compat.rs`.

### Added — Production Contract (Phase 87, 2026-04-08)

- **`docs/PRODUCTION-CONTRACT.md`** — Moon's v1.0 promises: per-command-class SLOs (provisional until Phase 97), supported platform matrix (Linux aarch64 primary, Linux x86_64 secondary contingent on `PERF-04`, macOS dev-only via OrbStack), durability mode semantics per `appendfsync` × failure-class, availability & replication guarantees, security guarantees, explicit out-of-scope list, and a machine-checkable GA Exit Criteria checklist that every v0.1.3 phase ticks off. This is the contract every downstream hardening phase (88–100) tests against.
- **`docs/runbooks/`** — stub directory for operator runbooks authored in Phase 99 (`REL-05`).

### Changed — Toolchain Upgrade (Phase 88, 2026-04-08)

- **MSRV bumped from Rust 1.85 to 1.94.0.** `rust-toolchain.toml` committed so fresh clones auto-install the pinned version; CI workflows (`ci.yml`, `codeql.yml`, `release.yml`) and OrbStack `moon-dev` VM provisioning in `CLAUDE.md` updated. No language/runtime behavior change; downstream phases benefit from new clippy lints and std/compiler improvements. Contributors must run `rustup update` on next pull.

### Added — Production Readiness Phases 92-105 (2026-04-09)

- **Observability:** Prometheus `/metrics` on `--admin-port`, SLOWLOG GET/LEN/RESET/HELP, HEALTHZ + READYZ commands, `/healthz` + `/readyz` HTTP endpoints, INFO extended with Server/Clients/Memory/Stats/CPU sections, `--check-config` flag, per-command latency histograms + connection metrics wired into dispatch
- **Durability proof:** Crash-injection test matrix, torn-write WAL v3 tests (CRC32C validated), Jepsen-lite linearizability harness, backup/restore workflow test
- **Replication hardening:** PSYNC partial resync, full resync, network partition, kill-restart, replica promotion tests
- **Client compatibility:** CI matrix (redis-py, go-redis, jedis, ioredis, node-redis, redis-rs, hiredis), 24 Redis compat tests, vector client smoke script, `docs/redis-compat.md`
- **Performance gates:** Criterion regression CI with baseline caching, RSS-per-key memory gate script
- **Security hardening:** `deny.toml` (cargo-deny), `SECURITY.md`, `docs/THREAT-MODEL.md`, `docs/security/lua-sandbox.md`, TLS cipher suite freeze
- **Release engineering:** `docs/versioning.md`, 6 operator runbooks, CHANGELOG CI gate, user docs (getting-started, configuration, monitoring), release pipeline SHA256 checksums + SBOM + cosign

## [0.1.3] — 2026-04-10

Production-readiness foundation: dispatch hot-path recovery, vector-search
4× QPS + correctness fixes, and the tiered disk-offload landing with 100 %
crash recovery across 7 persistence configurations. Bundles three work
streams originally tracked as separate Unreleased blocks (Apr 7–8).

### Dispatch Hot-Path Recovery (2026-04-08)

**Pipelined SET +37%, pipelined GET +68% at p=16 after PR #43 regression recovery.**

Three targeted perf fixes landed after flamegraph-driven analysis of pipelined
SET on aarch64 (OrbStack moon-dev, 1 shard, default config, redis-benchmark
-c 50 -n 3M -P 16 -r 100000 -d 64):

| Metric                  | Broken baseline | After T0a+T0b+T0c | Δ      |
|-------------------------|----------------:|------------------:|-------:|
| SET p=1   (ratio Redis) | 0.99x           | **1.12x**         | +13pp  |
| SET p=16                | 1.42M/s         | **1.94M/s**       | +37%   |
| SET p=32                | 2.06M/s         | **2.26M/s**       | +10%   |
| GET p=16                | 2.40M/s         | **4.04M/s**       | +68%   |
| GET p=128 vs Redis      | 1.87x           | **1.91x**         | +4pp   |

### Perf fixes

- **T0a — Thread-local cached clock** (4041b0d). `Entry::new_*` constructors
  were calling `SystemTime::now()` / `clock_gettime` on every write, showing up
  at **10.14% of CPU** in the perf profile. Added a thread-local `Cell<u32>` /
  `Cell<u64>` refreshed once per shard tick (~1 ms) from `CachedClock::update()`.
  `current_secs` / `current_time_ms` now read the Cell and fall back to the
  syscall only on tests / cold init. `__kernel_clock_gettime` dropped from
  10.14% → **0%** of CPU.

- **T0b — Hot command dispatch bypasses phf SipHasher** (4b0eec3). The command
  metadata registry is a `phf::Map` keyed by `&'static str` using `SipHasher` —
  cryptographic overkill for a 173-entry ASCII table. Combined `phf::Map::get`
  + `SipHasher::write` + `hash_one` was **~6% of CPU**. Added a direct match
  path in `command::metadata::lookup`: pack the first ≤8 bytes of the command
  name as a `u64` with ASCII letters uppercased, match against 24 hand-picked
  hot commands (GET/SET/DEL/TTL/MGET/MSET/INCR/DECR/HSET/HGET/HDEL/HLEN/LPOP/
  RPOP/LLEN/PING/LPUSH/RPUSH/EXPIRE/EXISTS/INCRBY/DECRBY/SELECT/HGETALL).
  Hot-path resolves through a pre-resolved `LazyLock<[&'static CommandMeta; 24]>`
  — single array index, no hashing. Cold commands fall through to phf unchanged.
  Correctness asserted by `hot_path_matches_phf_map` test: every hot entry must
  return the same `&'static` pointer as a direct phf probe, in both upper and
  lowercase.

- **T0c — ACL unrestricted-user short-circuit** (4603511). Every command
  executed `check_command_permission` + `check_key_permission` even for the
  default `on nopass ~* &* +@all` user, burning **2.11% of CPU** on
  lowercasing, `extract_command_keys`, and glob matching. Added a cached
  `unrestricted: bool` field to `AclUser`, true iff the user is enabled, has
  `AllAllowed` commands, only `~*` read/write key patterns, and only `*`
  channel patterns. The three `check_*_permission` methods early-return `None`
  on `unrestricted` before any allocation or iteration. The cache is
  recomputed once at the end of `apply_rule` (the single mutation entry point
  used by ACL SETUSER / LOAD / reset). Correctness covered by three new tests
  (`default_user_is_unrestricted`, `restrictions_clear_unrestricted_flag`,
  `unrestricted_user_passes_all_checks`).

### Correctness fix (PR #43 review)

- **Inline monoio fast-path restricted to GET** (613c164). The previous inline
  dispatch in `try_inline_dispatch` handled both GET and SET directly against
  the DashTable, bypassing replica READONLY enforcement, ACL checks, maxmemory
  eviction, client-side tracking invalidation, keyspace notifications,
  replication propagation, and blocking-waiter wakeups. Under any of those
  configurations the inlined SET would silently diverge from the normal path —
  accepted writes on replicas, ACL-denied clients writing, maxmemory overshoot,
  stale client-side caches. Fix: inline only handles `*2\r\n$3\r\nGET` now;
  SET and everything else fall through to the full dispatcher where all
  side-effects run.

### Cold-tier lock hygiene (PR #43 review)

- **Release shard read guard before cold-tier disk read** (ff51135). The
  cold-tier fallback in `server::conn::blocking` previously called
  `get_cold_value()` — which does a synchronous `std::fs::read()` — while still
  holding the per-shard read guard, blocking all concurrent operations on that
  shard during disk I/O. Split the path: `Database::cold_lookup_location`
  returns the `(ColdLocation, PathBuf)` under the lock, the guard is dropped,
  and `cold_read::read_cold_entry_at` performs the disk read unlocked.

### Additional PR #43 fixes

- `read_overflow_chain` now bounded at 1000 iterations (cycle guard against
  corrupted `next_page` links)
- `recovery.rs` FPI replay replaces `.unwrap()` on `try_into()` with explicit
  byte-array construction (coding-guidelines compliance)
- `bench-production.sh`: fixed unsupported `-t zrangebyscore` (→ `zpopmin`),
  MSET rps parser for `"MSET (10 keys):"` output, heredoc `$(date)` expansion,
  and Redis RSS probe (`pgrep`/`/proc` instead of missing `lsof`)
- `bench-cold-tier.sh`: removed stray `&` backgrounding `FT.CREATE`
- `test-recovery-all-cases.sh`: `NoPersistence` case now PASSes at 0 keys
- `benches/resp_parsing.rs`, `benches/get_hotpath.rs`: wrap `Vec<Frame>` in
  `FrameVec` via `.into()` after frame.rs type change

All 1872 unit tests pass under `--no-default-features --features
runtime-tokio,jemalloc`. Follow-up work (T1 `dispatch_raw` zero-alloc entry
point, Tier 2 storage/DashTable optimization, residual ACL SipHash elimination)
captured as todo in `.planning/todos/pending/`.

---

### Vector Search 4× QPS + Correctness (2026-04-07)

**4x search QPS, 4.1x lower latency, 2.56x faster than Qdrant on real MiniLM data.**

#### Performance (perf-profiled on GCloud c3-standard-8, Intel Xeon 8481C)
- 8-wide ILP unrolled `dist_bfs_budgeted` subcent path (the real hot loop, 90% of
  search time per perf profile). Loads 4 code bytes + 1 sign byte per iteration,
  8 independent f32 accumulators. Confirmed via objdump: parallel `vaddss` into
  xmm3-xmm8 (vs serial single-xmm0 chain before).
- 4-way unrolled `dist_bfs` non-subcent path with `unsafe` pointer arithmetic
- Pre-allocated ADC LUT in `SearchScratch` (eliminates 32-65KB heap alloc per query)
- Hoisted IVF `q_rotated` and `lut_buf` allocation out of per-segment loop

#### Correctness fixes
- **`FT.COMPACT` silent no-op**: split `try_compact` (threshold-gated) from
  `force_compact` (unconditional). Previously `FT.COMPACT` returned OK without
  compacting when `compact_threshold >= mutable_len`, leaving all vectors in
  brute-force O(n) mutable segment.
- **`key_hash_to_key` mapping restored** (lost in earlier refactor). `FT.SEARCH`
  now returns original Redis keys (`doc:N`) instead of `vec:<internal_id>`.
  Carried through `SearchResult.key_hash` and populated by `remap_to_global_ids`.
- **`FT.INFO num_docs`** now sums mutable + immutable segments (was 0 after compact)
- **Vector index recovery** metadata loads without `--disk-offload` flag
  (was gated behind `server_config.disk_offload_enabled()`)

#### Real MiniLM benchmarks (10K vectors, 384d, x86 Xeon 8481C)

| Metric | Mar 31 (M4 Pro) | Apr 7 (Xeon 8481C) | Δ |
|--------|---:|---:|---:|
| Recall@10 | 0.9250 | **0.9670** | +4.5% |
| QPS | 1,126 | **1,296** | +15% |
| p50 | 0.878 ms | **0.783 ms** | -11% |

| | Moon | Qdrant 1.12 FP32 | Ratio |
|---|---:|---:|---:|
| QPS (10K MiniLM) | 1,296 | 507 | **2.56x** |
| p50 | 0.783 ms | 1.79 ms | **2.29x lower** |
| Recall@10 | 0.967 | ~0.95 | **+1.7%** |

#### Infrastructure (for future segment merge work)
- `ImmutableSegment::decode_vector` / `iter_live_decoded`
- `MutableSegment::iter_live`

#### Attempted and reverted
Segment merge on `FT.COMPACT` via TQ4 decode → re-encode. Dropped recall from
0.73 → 0.0005 due to accumulated quantization error across 14 segments. Proper
fix requires retaining f32/f16 vectors alongside TQ codes in immutable segments.

#### Known limitation
TQ4 quantization at 384d with random Gaussian inputs hits ~0.73 recall floor
(curse of dimensionality — all points nearly equidistant). Real semantic
embeddings (clustered) achieve 0.92-0.97 recall with the same code.

---

### Disk Offload & x86_64 Performance (2026-04-06)

Tiered storage, crash recovery, and 2× Redis on x86_64 (Intel Xeon, io_uring).

### Added

#### Disk Offload (Tiered Storage)
- `--disk-offload enable` — evicted keys under maxmemory are spilled to NVMe instead of being deleted
- Async SpillThread: background pwrite via dedicated `std::thread` per shard (no event loop blocking)
- Cold read-through: GET transparently reads spilled keys from NVMe DataFiles
- ColdIndex: in-memory key→file mapping, updated immediately on eviction for consistent reads
- SpillThread channel capacity: 4096 bounded flume channel for burst absorption
- `--disk-offload-dir`, `--disk-offload-threshold` configuration flags

#### Crash Recovery
- V3 recovery falls back to appendonly.aof when WAL v3 has 0 commands
- V2 recovery falls back to appendonly.aof when shard WAL has 0 commands
- Automatic `--dir` creation before AOF writer starts (fixes silent write failure)
- Cold index rebuilt from manifest during v3 recovery
- Verified: 100% recovery (5000/5000 keys) across 7 persistence configurations after SIGKILL

#### Inline GET Optimization
- `read_db` + `get_if_alive` replaces `write_db` + triple-lookup `get()` — single DashTable probe
- Removed unnecessary write lock for timestamp refresh before inline dispatch
- Multi-shard inline dispatch: local keys bypass Frame construction via `key_to_shard()` check
- Cold storage fallback in `get_readonly` and inline GET dispatch paths

### Changed

- Connection handler eviction uses `try_evict_if_needed_async_spill` when disk offload enabled
- `spawn_monoio_connection` passes spill sender, file ID counter, and offload dir to handlers
- Event loop syncs `next_file_id` between `Rc<Cell<u64>>` (handlers) and local variable (timer tick)
- Inline dispatch `try_inline_dispatch` takes `now_ms` and `num_shards` parameters

### Fixed

- **Data loss under maxmemory**: evicted keys were silently deleted instead of spilled to disk (6 bugs)
- **Crash recovery = 0 keys**: appendonly.aof never tried as fallback source
- **AOF writer silent failure**: `--dir` directory not created before AOF writer task started
- **Cold read miss**: `get_if_alive` (read path) didn't check cold storage; `get_readonly` returned NULL for spilled keys
- **ColdIndex never initialized**: `cold_index` and `cold_shard_dir` were None on all databases at startup

### Performance (GCP c3-standard-8, Intel Xeon 8481C, CPU-pinned)

| Metric | Before | After |
|--------|--------|-------|
| c=1 p=1 GET vs Redis | 0.35x (47K) | **1.0x (47K)** — parity |
| c=10 p=64 GET | 2.29M | **4.71M** (2.06x Redis) |
| c=50 p=64 GET | 2.36M | **4.81M** (2.04x Redis) |
| Disk offload GET overhead | N/A | **<1%** vs no-persist |
| Recovery (SIGKILL) | 0/5000 | **5000/5000** (100%) |

---

## [0.1.2] - 2026-03-29

Multi-shard scaling milestone. Eliminated negative scaling, achieving 5M GET/s and 2.5M SET/s at 4 shards — both exceeding Redis 8.6.1.

### Added

#### Shared-Read Direct Access (Phase 49)
- `Arc<ShardDatabases>` with `parking_lot::RwLock<Database>` replaces `Rc<RefCell<Vec<Database>>>`
- Cross-shard read commands (GET, HGET, SCARD, ZRANGE, etc.) bypass SPSC channels entirely via `read_db()` + `dispatch_read()` — reduces cross-shard read latency from ~88μs to ~56ns
- Local read path uses shared `read_db()` lock instead of exclusive `write_db()` — eliminates RwLock contention between shards

#### Connection Affinity (Phase 50)
- `AffinityTracker` samples first 16 commands per connection to detect dominant shard
- Lazy FD migration: if ≥60% of keys target a non-local shard, migrates the TCP connection's file descriptor to the target shard via `ShardMessage::MigrateConnection`
- `MigratedConnectionState` preserves selected_db, client_name, protocol_version across migration
- Graceful fallback: if migration fails, connection stays on current shard with shared-read

#### Pre-Allocated Response Slots (Phase 51)
- `ResponseSlotPool` with lock-free `AtomicU8` state machine for zero-allocation cross-shard write dispatch (Tokio path)
- Eliminates per-dispatch `channel::oneshot()` heap allocation (~80-120ns savings per cross-shard write)

#### SO_REUSEPORT Per-Shard Accept (Phase 52)
- Each shard opens its own TCP listener with `SO_REUSEPORT` on Linux via `socket2` crate
- Kernel distributes connections across shard listeners using consistent 4-tuple hashing
- macOS/non-Linux: falls back to single-listener + MPSC round-robin (no behavior change)

#### jemalloc Production Tuning (Phase 53)
- `malloc_conf` static: `percpu_arena:percpu`, `background_thread:true`, `metadata_thp:auto`, `dirty_decay_ms:5000`, `muzzy_decay_ms:30000`, `abort_conf:true`
- Closes ~50% of allocation speed gap with mimalloc while retaining jemalloc's superior fragmentation behavior

#### New Commands (Phase 55)
- GETRANGE — return substring of stored string value
- SETRANGE — overwrite part of stored string at offset with zero-fill
- SUBSTR — alias for GETRANGE (Redis 1.x compatibility)

### Changed

- Custom `AtomicU8` oneshot channel replaced with `flume::bounded(1)` for cross-thread safety on monoio's `!Send` executor
- `pending_wakers` relay pattern: event loop locally wakes connection tasks after SPSC processing, bridging monoio's cross-thread waker limitation
- `write_db()` uses `try_write()` spin loop instead of blocking `write()` — prevents OS thread freeze on monoio when cross-shard readers hold locks
- Benchmark scripts: `scripts/bench-scaling.sh` for multi-shard test matrix, `scripts/bench-production.sh` updated

### Fixed

- ResponseSlot `UnsafeCell<Option<Waker>>` data race on ARM64 — replaced with `AtomicWaker`
- Local read path took exclusive write lock (`write_db()`) even for GET — split into `read_db()` + `dispatch_read()`
- Monoio local write path silently dropped responses (`responses.push(response)` missing after read/write split) — all write commands (SET, INCR, LPUSH, etc.) hung on monoio
- Pipeline ordering guard: `!remote_groups.contains_key(&target)` prevents stale reads when batch has pending writes for same shard

### Performance

| Metric | Before (v0.1.0) | After (v0.1.2) | Change |
|--------|:---------------:|:--------------:|:------:|
| Multi-shard GET p=16 | 688K (0.38x Redis) | **1,923K (1.17x Redis)** | **2.8x** |
| Multi-shard GET p=64 | N/A | **5,002K (1.60x Redis)** | New |
| Multi-shard SET p=16 | N/A | **1,515K (1.32x Redis)** | New |
| Multi-shard SET p=64 | N/A | **2,500K (1.55x Redis)** | New |
| Monoio 1s p=128 GET | 5,407K | **5,005K (1.25x Redis)** | Maintained |
| Negative scaling | -25% at 12 shards | **Zero at 1-8 shards** | Eliminated |
| Command coverage p=1 | Parity | **Monoio beats Redis 8/10** | Improved |

## [0.1.1] - 2026-03-28

Structural stability milestone. Codebase refactoring for maintainability — no feature changes, no performance changes.

### Changed

#### Error and State Foundations (Phase 44)
- Unified `MoonError` type hierarchy with structured `#[source]` on I/O variants carrying `PathBuf` context
- `ConnectionContext` struct for connection state (selected_db, authenticated, client_name, protocol_version)
- Criterion benchmark baseline (GET dispatch 69.1ns) to guard against regressions

#### Command Metadata Registry (Phase 45)
- `phf` static perfect hash map for O(1) command lookup (112 commands)
- `CommandMeta` struct: name, arity, flags (read/write/fast/admin), key positions, ACL categories
- `is_write()` classification via const bitflags — replaces duplicated match arms across codebase

#### Persistence Hardening (Phase 46)
- Eliminated server-crashing `unwrap()` calls in WAL, AOF, and RDB persistence code
- Corruption recovery: WAL uses per-block CRC32 log+skip, AOF seeks to next RESP `*` marker, RDB breaks on mid-stream corruption
- `WalWriter` methods remain `std::io::Result` (must-panic on flush = data loss prevention)

#### AOF Replay Decoupling (Phase 47)
- `CommandReplayEngine` trait breaks circular dependency between persistence and command dispatch
- `StorageEngine` trait boundary for persistence replay and Lua scripting
- `execute_command()` at command level (not individual get/set methods)

#### God-File Decomposition (Phase 48)
- `connection.rs` (5,102 lines) → 6 sub-modules in `conn/`: `handler_sharded.rs`, `handler_monoio.rs`, `handler_single.rs`, `shared.rs`, `blocking.rs`, `conn_state.rs`
- `shard/mod.rs` (2,004 lines) → 6 sub-modules: `event_loop.rs`, `spsc_handler.rs`, `persistence_tick.rs`, `conn_accept.rs`, `timers.rs`, `uring_handler.rs`
- Module facade pattern with `pub(crate)` re-exports preserving all external import paths
- No single file exceeds 800 lines

### Added
- Docker: optimized multi-stage build (113MB → 41MB)
- Mintlify documentation site
- Claude Code GitHub workflow for PR reviews
- `scripts/bench-resources.sh` for memory/CPU efficiency benchmarking

## [0.1.0] - 2026-03-27

Initial release. A Redis-compatible in-memory data store written in Rust, achieving 1.84-1.99x Redis throughput at 8 shards and 27-35% less memory for 1KB+ values.

### Added

#### Core Data Types (Phases 1-5)
- RESP2 protocol parser and serializer with inline command support
- TCP server with concurrent connections, graceful shutdown, and `redis-cli` compatibility
- String commands: GET, SET, MGET, MSET, INCR/DECR, APPEND, GETEX, GETDEL (17 commands)
- Hash commands: HSET, HGET, HGETALL, HINCRBY, HSCAN (14 commands)
- List commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LPOS (12 commands)
- Set commands: SADD, SREM, SINTER, SUNION, SDIFF, SPOP (15 commands)
- Sorted Set commands: ZADD, ZRANGE, ZRANGEBYSCORE, ZINCRBY, ZPOPMIN (18 commands)
- Key management: DEL, EXISTS, EXPIRE, TTL, SCAN, KEYS, RENAME (13 commands)
- Lazy + active key expiration with probabilistic sampling
- RDB persistence with point-in-time snapshots
- AOF persistence with configurable fsync (always/everysec/no)
- Pub/Sub messaging: SUBSCRIBE, PUBLISH, PSUBSCRIBE (4 commands)
- Transactions: MULTI/EXEC/DISCARD with WATCH optimistic locking
- LRU/LFU/random eviction policies with configurable maxmemory

#### Performance Architecture (Phases 6-15)
- SIMD-accelerated RESP parsing via memchr CRLF scanning and atoi
- CompactValue 16-byte SSO struct with embedded TTL delta
- DashTable segmented hash table with Swiss Table SIMD probing
- Thread-per-core shared-nothing architecture with per-shard event loops
- io_uring networking layer with multishot accept/recv and registered buffers
- Per-shard memory management with jemalloc and bumpalo arenas
- Forkless compartmentalized persistence (no COW memory spike)
- B+ tree sorted sets replacing BTreeMap for cache-friendly access
- Per-connection arena allocation with bumpalo

#### Protocol & Data Types (Phases 16-18)
- RESP3 protocol: Map, Set, Double, Boolean, VerbatimString, Push frames
- HELLO command for protocol negotiation
- Client-side caching invalidation via Push frames
- Blocking commands: BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX
- Streams data type: XADD, XREAD, XRANGE, XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM

#### Clustering & Replication (Phases 19-20, 26)
- PSYNC2-compatible replication with per-shard WAL streaming
- Partial resync support with replication backlog
- Cluster mode with 16,384 hash slots and gossip protocol
- MOVED/ASK redirections and live slot migration
- Majority consensus failover election with automatic promotion

#### Scripting & Security (Phases 21-22, 43)
- Lua 5.4 scripting via mlua: EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH
- Sandboxed Lua VM with Redis API bindings (redis.call, redis.pcall)
- ACL system: per-user command/key/channel permissions
- ACL SETUSER, GETUSER, DELUSER, LIST, WHOAMI, LOG, SAVE, LOAD
- TLS 1.3 via rustls + aws-lc-rs with dual-port support
- mTLS client authentication
- Protected mode (reject non-loopback when no password set)

#### Optimization (Phases 24-42)
- WAL v2 format: checksums, header, block framing, corruption isolation
- CompactKey SSO: 23-byte inline keys, eliminating heap allocation
- Response buffer pooling and adaptive pipeline batching
- Dual runtime: Tokio (all platforms) + Monoio (Linux io_uring / macOS kqueue)
- Full Monoio migration: channels, TCP, codec, spawn, persistence, replication
- Direct GET serialization bypassing Frame allocation
- Zero-copy argument slicing from parse buffer
- Lock-free oneshot channels (12% CPU reduction vs tokio::oneshot)
- CachedClock timestamp caching (4% throughput gain)
- HeapString for values (eliminates Arc overhead)
- Inline dispatch for single-shard commands

### Performance

| Benchmark | Result |
|-----------|--------|
| Peak GET throughput | 3.79M ops/sec (4 shards, p=64) |
| Peak SET with AOF | 2.78M ops/sec (AOF everysec, p=64) |
| vs Redis (pipeline=64) | 3.17x SET, 2.50x GET |
| vs Redis (8 shards, p=16) | 1.84-1.99x |
| vs Redis with AOF | 2.75x (per-shard WAL vs global) |
| Memory (1KB+ values) | 27-35% less than Redis |
| Memory (empty server) | Identical 7.0 MB baseline |
| p50 latency (8 shards) | 0.031ms (Redis: 0.26ms) |
| Data consistency | 132/132 tests pass |

### Technical Details

- **Language:** Rust (stable, edition 2024)
- **Lines of code:** ~54,000 across 96 files
- **Dependencies:** tokio, monoio, jemalloc, rustls, mlua, bumpalo, bytes, clap
- **Supported platforms:** Linux (io_uring via Monoio), macOS (kqueue via Monoio or Tokio)
- **Build time:** ~50s release build

[0.1.0]: https://github.com/pilotspace/moon/releases/tag/v0.1.0
