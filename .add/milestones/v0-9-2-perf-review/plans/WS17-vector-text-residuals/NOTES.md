# WS17-vector-text-residuals — working notes (ADD discipline, not a shared artifact)

## Context loaded
TEAM-RULES (§0 part-3 addendum), PLAN, MILESTONE, personas (performance-engineer lead,
ci-test-integrity-engineer for the test-hygiene items), moon#1226 body + comment, moon#1228
("Vector (WS11) small items"), moon#1220 + comment (only item 3 remains), moon#1222, WS5a / WS11 /
WS5b / WS13 NOTES. Base `ae21476`; WS8's branch was read (git show, read-only) to see its edit to
`ft_search/dispatch.rs` (`capture_dense_knn_snapshot` made `pub(crate)` for `shard::vector_scatter`,
which captures with `filter = None`).

Ownership constraints that shape the designs below:
- `ft_search/dispatch.rs` (WS8) and `src/shard/**` are not touched. Every fix that would naturally
  land at a call site there is made in the callee (parse layer, `execute.rs`, `session.rs`,
  `payload_filter.rs`, `holder.rs`) or recorded as a one-line integration follow-up.
- Files already over 1500 lines on the base (`holder.rs` 2035, `mutable.rs` 2264, `search.rs` 1910,
  `immutable.rs` 1671, `text/store.rs` 3927, `graph/cypher/executor/read.rs` 1488 is under) are not
  grown where it can be avoided; new code goes into sibling modules.

## 1a. moon#1226 — `MOON_VECTOR_PAYLOAD_TEXT=off` + TextMatch (commit 6395586)
- Mechanism verified: `index_payload_field` (spsc_handler.rs:~4537) skips `insert_text_shared`
  when the flag is off; `PayloadIndex::evaluate_bitmap_with`'s TextMatch arm still searches the
  (empty) payload text index → empty bitmap → every filtered KNN returns `*1 :0`. Reproduced on the
  base binary: `"*1\r\n:0\r\n"`.
- Routes a TextMatch can take: (a) payload text index (default); (b) BM25 plane for a declared TEXT
  field under `MOON_VECTOR_PAYLOAD_SCHEMA=declared`. (b) works with the flag off and must not be
  refused. Without the `text-index` feature neither exists (same silent-empty class; the build
  refusal mirrors moon#728's `text_engine_absent_refusal`).
- Design: rule = "refuse iff the payload route is unavailable AND some TextMatch field is not
  BM25-routed". At PARSE time (no index known) the BM25 route is impossible unless declared mode is
  on → decide there (covers sync, yielding capture, FT.CACHESEARCH, cross-shard `parse_ft_search_args`).
  In declared mode, decide per index in `execute.rs` (both sync paths).
- Residual: the yielding capture (`dispatch.rs`, WS8) evaluates filters without the per-index
  check → only "declared mode + payload text off + TextMatch on a non-TEXT field" still yields an
  empty page there. One-line integration follow-up recorded in the commit.
- FT.INFO field placement: top level after `unloaded_segments_with_exact_rerank`; the cross-shard
  merge takes non-additive keys from the template shard (every shard of one process agrees).
- Risk: the tokio leg (no `text-index`) now answers a TextMatch filter with an ERR instead of an
  empty page; the two parser-shape unit tests were made leg-aware. No integration test used a
  TextMatch filter on the tokio leg (grepped).
- A multi-shard filter-parsing observation outside this issue was reported to the orchestrator
  privately (SECURITY.md).
- Self-score: Completeness 0.9 (residual named, needs WS8 file) · Clarity 0.95 · Practicality 0.95 ·
  Optimization 0.95 (parse-time check is O(filter nodes), no allocation) · Edge cases 0.9 (nested
  And/Or/Not, declared TEXT, no-text-index build, explicit + inline) · Self-evaluation 0.9.

## 1b. moon#1226 — prepared-state mismatch (commit 984925b)
- Mechanism verified: `PreparedTqQuery::matches` = ptr-eq or (checksum, id, dims, padded, quant,
  metric). `metadata_checksum` covers "all fields above" it in `CollectionMetadata`, and
  `sub_centroid_table` is declared after it → not covered. `lut32()` = `None` when the prepared
  collection lacks the table; `unwrap_or(&[])` → the length guard `adc_lut.len() < 2·code_len·epc`
  → `return SmallVec::new()` (debug_assert in debug). Red reproduced exactly that.
- Fix: prepared LUT accepted only when `len == padded·epc`, else local fill (same routines →
  bit-identical, test compares bits).
- Self-score: 0.95 across; edge cases: 16-level path unchanged (lut16 always sized), SQ8 never
  prepared.

## 2. moon#1222 — stream_effect flake (commit after the loop)
- Mechanism verified: `Stream::read_group_new` (stream.rs:414) reads `current_time_ms()` once per
  STREAM; the test's two streams → two syscalls outside a shard (TL cache 0). The test derives one
  `now` from `s` and replays `XCLAIM … TIME now` for `t` too → 1 ms off when the clock ticked.
- Fix: `ClockPin::set(..)` (RAII, resets on drop) at the top of the test — the in-shard invariant.
- Red evidence (temporary stress tests, never committed, same build): the unpinned body looped
  100,000× diverged 944 times (0.94 %); pinned (ms advanced per iteration) 0/100,000.
- Sibling audit: the only other tests reading real time are `a_claim_logs_what_it_took…`
  (single stream; both PEL entries stamped by ONE `read_group_new` call; the IDLE record is derived
  from the passed `now`) and `an_autoclaim_logs…` (compares id/consumer/count only). Neither can
  diverge on a tick. `a_multi_stream_read_serializes…` compares log framing only.
- Self-score: 0.95 across.

## 3a. moon#1228 — declared-schema TEXT resolver (commit 5c0a906)
- Mechanism verified: `bm25_text_match` → `search_field(fidx, terms, None, None, num_docs)` →
  `FieldScorer::exact` + `top_k_for_field` (BM25 per candidate, heap of all, key clone per hit).
- Fix: analysed terms → postings (absent term → empty) → `intersect_rarest_first` →
  `restrict_to_live` → hash each key in place. Identical membership by construction
  (`top_k_for_field` scores exactly `restrict_to_live(candidates)` and AllSum never drops one);
  test compares against the verbatim HEAD resolver incl. holes and a reused id.
- Self-score: 0.95; optimization: allocation is one bitmap + the analysis strings.

## 3b. moon#1228 — prefetch line count (commit d359dac)
- Mechanism: `bpc.div_ceil(64)` ignores the start offset; issue's example corrected in the test —
  516 B at offset 60 spans exactly 9 lines, >60 spans 10.
- `code.addr()` (strict-provenance `addr`, stable) keeps the pointer arithmetic safe; no new unsafe.
- DEFERRED: aarch64 A/B (no hardware).
- Self-score: Completeness 0.9 (A/B deferred by hardware) · others 0.95.

## 4c. moon#1226 — mutable-segment exact rerank (commits 2205f86, harness 379c6e3)
- Mechanism verified: every holder mutable leg (`search_filtered` 4 arms incl. HnswPostFilter,
  `search_mvcc`, `search_mvcc_yielding_with_pool` chunked) ranked by TQ-ADC only; `raw_f16` is
  appended for every quantizer in both build modes (`append` / `append_transactional`) and is what
  compaction hands the sidecar. Immutable `rerank_exact` re-scores `mult·k` beam candidates.
- Design: ADC top `rerank_mult·k` (the index's FT.CONFIG RERANK_MULT, same knob as HQ-1) →
  `MutableSegment::rerank_exact` (global id − base → row) → sort → truncate k. One shared kernel
  (`hnsw::prepared::exact_f16_distance`, the immutable loop now calls it: same ops, same bits).
  New code in `segment/mutable/rerank.rs` (child module: private fields visible, mutable.rs +3
  lines). All paths reranked identically → the sync/MVCC/yield G-IDENTITY holds (test asserts bits).
- Recall (unit test, 3,000 × 384d EXACT, mutable-only): far 0.703 → 0.990, in 0.945 → 0.997.
- COST (release-fast in-process A/B, 7 alternating reps): FT.SEARCH's MVCC scan x1.59–1.70 (the
  FastScan pre-filter prunes against the heap's worst entry, and a 4k-deep heap prunes less);
  sync scan x1.01–1.05. mult 2: x1.23–1.29 for R@10 0.935–0.968 (vs 0.979–0.995 at 4).
- Why not cheaper here: a sound cheaper selection would pick the `mult·k` by the FastScan estimate
  itself (no scalar ADC rescoring) and rerank those — a change to the shared chunk-scan kernel
  (mutable.rs, 2264 lines, also the SQ8 / A2 / <64-entry fallbacks) that needs its own A/B. Left as
  a follow-up; the knob (RERANK_MULT 1–64) already lets operators trade.
- Self-score: Completeness 0.95 · Clarity 0.95 · Practicality 0.9 · Optimization 0.8 (cost above,
  reason stated) · Edge cases 0.9 (rows missing → HEAD's ADC order; zero rows keep ADC; ids below
  base skipped) · Self-evaluation 0.9.

## 4a. moon#1226 — PreparedTqQuery per-query allocation (commit 2ff4284)
- `prepare_graph_query` built the prepared state at ≥ 1 graph segment. At exactly 1 the segment's
  own path fills the reused scratch (no allocation). Rule now ≥ 2 (pool also fans out at ≥ 2).
- Measured (release-fast): the prepared build is 11–14 µs/query vs a 5,000-vector segment search
  1.1–1.7 ms → ~1 % CPU on a single-segment query, plus a 64–128 KB allocation avoided. Small, honest.
- Thread-local LUT store for ≥ 2 segments rejected: the state is `Sync`, shared with pool workers
  and dropped on whichever thread last holds it; there it replaces ≥ 2 per-segment fills with one.

## 4b. moon#1226 — session `results.clone()` (commit 91b0a44, PARTIAL)
- All three callers are in `ft_search/dispatch.rs` (WS8, do-not-touch). The in-place API
  (`retain_unseen_in_db`) is added and the borrowed wrapper implemented over it; switching the
  three call sites (they own `sv` / `results`) is a 3-line integration follow-up.

## 4d. docs (commit dacc1b2) · merged-QJL stride: confirmed resolved (merge.rs has only a comment).

## 5a. moon#1220 item 3 — `.tpost` doc-id compaction (commits d00bda0, b9e67df)
- Holes came from `FT.INVALIDATE_RANGE` / boot deletion probe; reuse refills them only on new
  inserts. `.tpost` carried them; `install_recovered` sizes columns by the top id and the density
  guard REFUSES files with holes > 2·docs + 64 Ki → full keyspace rebuild on every boot.
- Monotone remap at encode (rank among live ids) keeps every order (incl. doc-id tie order), so a
  restart is reply-identical on one shard; cross-shard tie interleaving can change across a restart
  (it already depended on allocation history; a rebuild renumbers in scan order anyway).
- Golden `.tpost` of perf_ws13_text_positions changed (its corpus has 141 holes): verified with the
  renumbering disabled it still hashes to WS13's 0xe4dc7f45658079e0.
- `doc_terms` densification NOT done: HashMap<u32, SmallVec<[u32;8]>> entry = 48 B + ctrl, load
  0.44–0.875 → 55–110 B per (doc, field); a dense `Vec<SmallVec>` slot = 40 B for populated fields
  (saves ~16–70 B) but costs 40 B per EMPTY slot for sparse fields. Mixed; and the map is not
  billed in `estimated_bytes` at all — billing it is the more important follow-up.

## 5b. term-at-a-time scratch (commit ec9782f): blocked fold, 4096 ids; A/B 50 terms × 200K docs:
   whole-window 16.99 ms vs blocked 16.54 ms (x0.97) — no regression; scratch 480 KB → ≤ 32 KiB.
## 5c. small inefficiencies: LeafTable (1ed070d, 2,001,000 → ≤ 26,000 comparisons for 2,000 leaves),
   TopK (20f92e5, 200K pushes k ≥ n: heap 25.3 ms → 7.06 ms, x3.6), Chunks split capacity (2aa9597,
   2.01x → ≤ 1.1x), TEXT leaf borrow (d8d4963).
## 5d. oracle gaps (591de45): model-based TAG/NUMERIC truth; planted `numeric_range_bitmap` bug caught.
## 5e. timing asserts (eb3590d, b874426) · 5f BM25 order check (1de2740) · 6 splits (0f0025f, 70e4050)
   · 7 root-container skip (d98b52b, probes the premise instead of calling geteuid, which needs unsafe).

## Declared-TEXT resolver measurement (3a): 66,667 matches of 200K docs, 2-term AND:
   scoring resolver 30.4 ms → bitmap 15.9 ms (x1.9), 7 alternating reps, release-fast.

## Final gates (tree 9789b5a)
- `cargo fmt --check` 0 · `scripts/audit-unsafe.sh` PASSED (244/244 with SAFETY, no new unsafe in the diff) ·
  `scripts/audit-unwrap.sh` PASSED (0 hot-path; every added unwrap/expect is in test code).
- `cargo clippy --all-targets -- -D warnings` 0 · `cargo clippy --no-default-features --features
  runtime-tokio,jemalloc -- -D warnings` 0 · `cargo check --all-targets --no-default-features --features
  runtime-tokio,jemalloc` 0 after 9789b5a (it had warned once on an ungated test helper).
- Lib, monoio: `vector:: command::vector_search text:: graph:: replication::stream_effect protocol::parse
  storage::tiered::cold_index_rebuild` 1950 passed / 0 failed / 10 ignored. Tokio: 1078 / 0 / 9.
- Integration, monoio: perf_ws13_{graph_index_scan,graph_topk,text_positions,text_prefix}, perf_ws17_payload_text_off,
  perf_ws4_multibulk_linear (ignored in debug), perf_ws5b_{graph_limit,text_billing,text_search,text_upsert
  (ignored in debug)}, inverted_search_{,numeric_}shard_consistency, vector_exact_rerank, vector_edge_cases,
  ft_search_yield_red, ft_cachesearch_metric_748, ft_text_meta_tag_numeric_restart, fts_query_eval_e2e,
  vector_update_tombstones — green. Tokio: the applicable subset green (text/graph-gated files compile to 0 tests).
- Aliasing seen once: `vector_update_tombstones::rewrite_left_in_mutable_when_the_keymap_was_snapshotted_survives_kill9`
  failed (num_docs 0 after restart) in an unpinned batch run at load 11; the rerun recompiled `moon` (another tree had
  replaced the shared artifacts) and passed, and the whole suite passed 16/16 with `MOON_BIN` pinned to
  `/home/user/wt/bin/ws17-dbg-9789b5a` (proven mine by perf_ws17_payload_text_off's FT.INFO field in the same run).

## 4c follow-up — rows outside the f16 range (commit e4d31c4)
- Found by probing a `--shards 2` server with the suites' ASCII test blob: every `__vec_score` was `inf`. A component
  beyond ±65,504 is stored as ±inf in `raw_f16`; the exact distance is inf / NaN and every candidate tied → id order.
  HQ-1's immutable rerank had the same behaviour since it landed. `exact_f16_distance` now returns `None` for a
  non-finite result (caller keeps the ADC estimate), like the zero-row case. Red: `q=37: [inf, inf, inf, inf, inf]`.
- Re-gated at e4d31c4: fmt 0, audits PASSED, clippy --all-targets 0, clippy tokio 0, check --all-targets tokio 0
  (no warnings), `vector:: command::vector_search` lib 1075/1075 monoio, 914/914 tokio.
- Orchestrator disk note (root volume < 5 % free): every server my tests spawn passes `--disk-free-min-pct 0`
  (perf_ws17_payload_text_off, inverted_search_shard_consistency, vector_update_tombstones), so no `diskfull`
  artefact applies to the results above.
