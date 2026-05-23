# W2-M2 FT.INVALIDATE_RANGE — Summary

**Date:** 2026-05-23  
**Branch:** `feat/ft-invalidate-range`  
**Author:** Tin Dang

---

## What was built

`FT.INVALIDATE_RANGE` — a new Moon text-index command that bulk-deletes
documents matching a `(node_id, hlc_wall_lo..hlc_wall_hi)` filter. This is
the upstream primitive that Lunaris's `invalidate_range(node_id, hlc_lo..hlc_hi)`
calls when `helios-git` detects a force-push and needs to bulk-invalidate
stale recall after a rebase (UC-G3 per INTEGRATION-PLAN.md §3.3).

---

## Wire shape

```
FT.INVALIDATE_RANGE <index> <node_id_field> <node_id_value> <hlc_wall_field> <hlc_wall_lo> <hlc_wall_hi>
```

Returns: `(integer)` count of deleted documents.

**Preconditions (locked schema discipline):**
- `node_id_field` MUST be declared `TAG` in `FT.CREATE` schema.
- `hlc_wall_field` MUST be declared `NUMERIC` in `FT.CREATE` schema.

---

## Commit sequence

| SHA | Message |
|-----|---------|
| `9042ab7` | `chore(moon): merge feat/version-token-readside for W2-M2 base` |
| `228e1f4` | `test(moon): W2-M2 RED — FT.INVALIDATE_RANGE failing tests` |
| `ecd2bac` | `feat(moon): W2-M2 GREEN — FT.INVALIDATE_RANGE delete-by-query` |
| `90670e8` | `feat(moon): W2-M2 MULTI-SHARD — FT.INVALIDATE_RANGE scatter-sum across shards` |

---

## Files changed

| File | Change |
|------|--------|
| `src/text/store.rs` | Added `TextIndex::remove_doc_by_doc_id` — hard-delete from all inverted indexes |
| `src/command/vector_search/ft_invalidate_range.rs` | New handler + unit tests |
| `src/command/vector_search/mod.rs` | Module declaration + re-export |
| `src/shard/spsc_handler.rs` | `dispatch_vector_command` registration |
| `src/shard/coordinator.rs` | Added `scatter_invalidate_range` — fan-out to all shards, sum `Frame::Integer` counts |
| `src/server/conn/handler_monoio/ft.rs` | Slice + non-slice single-shard paths; multi-shard scatter-sum path |
| `src/server/conn/handler_sharded/ft.rs` | Single-shard path; multi-shard scatter-sum path |

---

## Design decisions

### No FT.DEL primitive existed
Moon had no per-document delete on TextIndex. The only "delete docs" path was
`FT.DROPINDEX DD` (drops the whole index). Built `TextIndex::remove_doc_by_doc_id`
as the canonical per-doc hard-delete that cleans TEXT postings, TAG bitmaps,
NUMERIC BTreeMap entries, and all MVCC LSN records.

### Direct bitmap intersection (not FT.SEARCH re-parse)
The handler uses `TextIndex::search_tag` + `TextIndex::search_numeric_range`
directly for the two bitmap lookups, then intersects with a two-pointer merge.
This is O(1) tag lookup + O(log N + k) range scan — faster than building a
query string and routing through the full FT.SEARCH parser.

### Version token bumps on every successful invocation (including zero-match)
A force-push that touches no indexed documents still advances wall-clock state;
Lunaris consumers need to re-poll. Only SYNTAX / RANGE / WRONGTYPE errors skip
the bump. This matches the contract specified in INTEGRATION-PLAN.md §3.3.

### Feature flag gating
All dispatch sites are gated under `#[cfg(feature = "text-index")]`. Non-text-index
builds return `ERR FT.INVALIDATE_RANGE requires text-index feature` (consistent
with other text-only commands like FT.AGGREGATE).

---

## Test results

```
6 passed, 3206 filtered out (existing tests)
```

All 6 new tests pass:
- `invalidate_range_deletes_matching_docs` — happy path (TAG ∩ NUMERIC range)
- `invalidate_range_empty_returns_zero` — no-match returns 0
- `invalidate_range_bumps_version_token` — exactly +1 per call, including zero-match
- `invalidate_range_invalid_args_returns_syntax_error`
- `invalidate_range_lo_gt_hi_returns_range_error`
- `invalidate_range_nonexistent_index_returns_wrongtype`

**Pre-existing failure (unrelated):**
`graph::cypher::parser::tests::test_nesting_depth_exceeded` — stack overflow
in debug builds on a deep-nesting stress test. Pre-exists on `main` branch;
not caused by W2-M2.

---

## Surprises / follow-ups

1. **`.planning/` is a git submodule in Moon** — commit messages cannot be
   stored there. Used `.claude/worktrees/invalidate-range/docs/` instead.

2. **`tmp/` is gitignored in Moon** — same issue as above; commit messages
   written via git commit `-m` heredoc instead.

3. **No FT.DEL existed** — the prompt assumed "reuses existing FT.DEL primitives"
   but Moon has no such command. Built `remove_doc_by_doc_id` from scratch using
   the same internal APIs that the existing upsert path uses. Scope expanded ~50
   LOC but the implementation is solid and follows existing patterns.

4. **`test_nesting_depth_exceeded` SIGABRT** — pre-existing; unrelated to this
   task. The full suite (3211 tests) passes when this test is excluded.

---

## Success criteria checklist

- [x] Merge commit landed (W1-M1 base merged in)
- [x] RED + GREEN commits landed
- [x] `cargo clippy -- -D warnings` clean on touched crates
- [x] All 6 new tests pass
- [x] Command registered in dispatcher (`dispatch_vector_command`) and both
      handler ft.rs files (monoio slice + non-slice + multi-shard paths; sharded single + multi-shard paths)
- [x] Multi-shard scatter-sum (`scatter_invalidate_range` in coordinator.rs) — all N shards contacted, counts summed
- [x] `version_token` bumps verifiable (exactly +1 per successful invocation)
- [x] File LOC ≤700 on new files (`ft_invalidate_range.rs` = ~350 LOC)
- [x] No modifications outside this Moon worktree
- [x] No merge conflicts (clean fast-forward)
