# WS13-text-graph-followups — working notes (ADD discipline, not a shared artifact)

## Context loaded
TEAM-RULES, PLAN, WS5b SUMMARY + NOTES, personas (performance-engineer lead, ci-test-integrity-engineer
for moon#1219), CONVENTIONS FTS lessons (asymmetric fixtures, non-zero ran counts, stale `#[ignore]`d
tests guard nothing), text-postings-persistence.md, issues moon#1219 / #1220 / #884 (+ comments) /
#1218 / #1199.

## Frozen contracts kept
- fts-posting-rank-tf: rank-aligned tf/positions, `tf_absent = 0`, `positions_for` slices unchanged.
- `.tpost` v1 byte-identical (golden hash pinned on the base layout in `tests/perf_ws13_text_positions.rs`).
- fts-query-eval-dispatch: `eval_query(_counted)` signatures, order score DESC / doc_id ASC, DFS path.
- moon#1197: plan shape and PhysicalOp set unchanged; `execute_profile` is the oracle.

## Decisions
- moon#1219: servers spawned PER TEST (ServerGuard reaps on unwind) rather than shared statics —
  statics are never dropped, so a shared server would orphan (moon#713). Index name + prefix are
  still per test. Stale assertions updated to the current contract (tag `{a|b}` union,
  `numeric_filter_invalid`), not deleted. An in-test oracle is compared as well as 1-vs-4 shards,
  plus a key_to_shard spread check so a one-shard answer cannot pass a broken merge.
- #1220.1: term-at-a-time only for AnyMax (fuzzy/prefix) leaves; the choice is a cost model over the
  FINAL candidate set (Σ|posting| ≤ 4·|cand|·terms, window ≤ |cand|·terms, ≥ 4 terms). Bit-identity
  by construction (same per-doc fold order, NaN = absent sentinel); MaxScore/WAND rejected — `total`
  needs full membership anyway and pruning adds a second correctness surface for little gain once
  the scan is linear.
- #1220.2: `PosColumn` (ends + data) per run/flat posting; runs bounded by positions as well as
  entries (else a high-tf doc makes a run memmove unbounded). `add_term_positions` batches a
  field's tokens per term (moon#884's per-token Vec gone) while `add_term_occurrence` keeps its
  signature (graph text index, fst_dict tests). No `.tpost` change → no version gate needed.
- #1220.3 DEFERRED: PR #1221 (WS5b review fixes) adds doc-id reuse / free-id bitmap / density
  guard to exactly these columns and to the `.tpost` decode. Renumbering ids on rewrite must be
  designed on top of that allocator, not beside it; `doc_terms` densification likewise waits for
  the reuse guarantee (holes stay bounded once ids are reused).
- #1220.4: fused RETURN + ORDER BY top-k in `executor/topk.rs`; streamed through row-local ops,
  materialised variant for everything else; declines on non-total key order (detected on the fly).
- #1220.5: `index_scan_keys` → visitor `index_scan_try_for_each`; moved to `executor/index_scan.rs`
  (read.rs 1810 → 1468 lines, under the 1500 rule).

## Red → green (all against this worktree's own compile; "Compiling moon … WS13" in each run)
- #1219: sabotaged `merge_text_results` (debug build, env-selected modes, never committed) →
  4/5 NUMERIC + 5/6 TAG red (drop remote / drop remote totals), 2 paging tests red (half offset),
  per-query checks red with the seed guard bypassed.
- #1220.1 prefix 8.7x → 3.0x of one broad term (bound 6x); .2 107.6 → 48.4 B/entry VmRSS (bound 75);
  .4 1.46x → 2.74x (bound 2x); .5 3.3x → 286.6x (bound 15x; re-run after the container restart:
  3.3x/4.0x FAILED at the bound, green at 630908f).

## Measurements (release-fast; `.bench/ws13_ab.py`, raw log `.bench/ab-run1.log`, not committed)
- Binaries: baseline-935c555, ws13-base-f32546c (build 1), ws13-final-630908f (build 2).
- FT.SEARCH LIMIT 0 10, 200K Zipf docs, s1, 3 interleaved blocks × median of 9: `ka*` 132.8/136.6/129.2
  → 29.6/31.2/29.3 ms (4.5x); `kalo*` 6.0x; `ka* @t:{red}` 2.2x; `%kalomi%` and rank-0 term 1.0x
  (controls). Replies byte-identical ws13 == base (and == 935c555 on this corpus).
- Load CPU (server utime+stime, 100K docs, 4 alternations): base 5.19 s → ws13 4.42 s (0.85x). Wall
  time is useless on this box right now (load 6–9 from other agents: 9–38 s spread).
- Upsert doc0 / docN-1 at 200K (150 reps interleaved): base 499/286 µs (1.75), ws13 479/276 (1.73).
  NOT flattened. Position sweep (single binary, sequential): cost falls ~linearly with doc id on
  BOTH binaries (base 229→135 µs, ws13 170→92 µs doc0→docN-1) — a pre-existing O(N−pos) term the
  24-byte headers were not; needs `perf` on Linux (not installed here).
- RSS after 200K body docs: base +683 MB → ws13 +433 MB (−37%); used_memory unchanged (billing
  constants). 100K tagged docs (2 runs): base +132.1/+132.2 MB → ws13 +113.8/+113.8 MB (−14%).
- Cypher 200K nodes, s1, `--params` nonce bypasses the result cache, 3 blocks × median of 5:
  RETURN-then-ORDER BY 468/470/455 → 221/213/230 ms (2.1x); wide RETURN 3.0x; range IndexScan
  LIMIT 42.9/44.2/46.1 → 4.3/7.5/4.7 ms (9.4x); WITH ORDER BY control 1.0x. Rows identical.

## PR #1221 integration (dry-run `git merge-tree HEAD fix/pr1221-ws5b`)
- One textual conflict: read.rs `execute_with_slots` loop — keep WS13's `while` loop, call
  `apply_op(op, &mut st, &env, demand[i], 0)?`.
- Semantic: topk.rs streamed segment `apply_op(op, &mut part, env, usize::MAX)` needs `, 0`
  (topk streams single-hop Expand only, so the var-length cap does not apply).
- posting.rs (1702 lines) kept its test module inline: #1221 adds a test inside it; split the tests
  into posting_tests.rs as a pure move AFTER integration.

## Gates (final tree beb1a15)
fmt --check 0 · clippy --all-targets --keep-going (default) FAILS only on WS6's pre-existing
tests/perf_ws6_aof_record_alloc.rs (useless_vec; fixed on fix/pr1221-ws5b 68741e0) · clippy tokio+text-index 0 ·
check --all-targets --keep-going tokio FAILS only on WS5b's pre-existing benches/text_search.rs (no
required-features; fixed on fix/pr1221-ws5b 5882cdc) · clippy --lib tokio 0 · audit-unsafe PASSED, 0 new unsafe.
Suites: lib text::/command::vector_search/graph::/command::graph 1120; default-feature integration 21 suites
(incl. both consistency suites, MOON_BIN=ws13-final-630908f); tokio+graph+text-index 16 suites — all green.
