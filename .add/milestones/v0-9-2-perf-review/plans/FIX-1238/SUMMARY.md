# FIX-1238 SUMMARY

- **Branch:** `fix/1238-inline-prefilter`, base `int/part3b` at `1a635f1`.
- **Design notes:** see NOTES.md in this directory.
- **Authorship:** the orchestrator committed this file. The harness refused the subagent's write (TEAM-RULES §6), so the content below is the agent's final report.

## Verdict
moon#1238 is **FIXED**. At `--shards > 1`, FT.SEARCH now parses an inline KNN prefilter (`@f:{v}=>[KNN …]`) with the `--shards 1` grammar and carries it to every shard leg.

If the prefilter cannot be parsed, or a leg cannot evaluate it, the query returns the same ERR as at `--shards 1`. It never falls back to an unfiltered search.

## Commits
| commit | contents |
|---|---|
| fb8f290 | the fix, plus integration and unit tests |
| ddb15a7 | consistency-suite rows (cross-ownership) |
| ede5b2e | NOTES |

## Evidence

### Integration test
`tests/perf_fix_1238_inline_prefilter.rs` runs at `--shards 1`, `2` and `4`. Each multi-shard reply must match the `--shards 1` reply byte for byte, and the `--shards 1` reply is checked against the expected keys.

**Red on `1a635f1`:**
- monoio: 4 of 5 tests fail;
- tokio: 3 of 4 fail;
- `ae21476` release build: 4 of 5 fail.

Example from `--shards 2`: `@lang:{fr}=>[KNN 5 @vec $q]` returned d:0, d:2 and d:3, which are not `fr`. `--shards 1` returned d:1, d:4, d:7 and d:10.

**Green on the fix:** monoio 5/5, tokio 4/4.

### Mutation check
Reverting the leg-error check in `merge_knn_legs` makes the declared-schema test fail. It returns an empty page instead of the ERR.

### Unit tests
- **New:** 3.
  - `multi_shard_args_resolve_the_prefilter_as_one_shard_does` (15 query shapes);
  - `an_inline_prefilter_is_parsed_and_an_unreadable_one_is_an_error`;
  - `a_refusing_leg_is_the_reply_of_a_filtered_query`.
- **Updated:** 2 existing parse tests.
- **Filtered lib runs:** monoio 346 passed, tokio 212 passed.

### Suite rows

| binary | pass | fail |
|---|---|---|
| fixed monoio | 11 | 0 |
| `1a635f1` monoio | 5 | 6 |

The 6 base failures are KNNFILT-01..05 at `--shards 4` plus the 1/4/12 parity row. KNNFILT-01..04 come from moon#648 and never passed at `--shards 4` before this fix.

### Gates
All clean:
- fmt;
- audit-unsafe and audit-unwrap;
- both clippy legs;
- tokio `check --all-targets`.

## Cross-ownership edits
All in fb8f290:
- `src/shard/dispatch.rs`: `VectorSearchPayload.filter`.
- `src/shard/spsc_handler.rs`: the `VectorSearch` arm now uses `plan_knn_leg`.
- `src/shard/coordinator.rs`: `scatter_vector_search_remote`.
- `src/shard/vector_scatter.rs`: `KnnLeg`, `plan_knn_leg` and `merge_knn_legs`.
- `src/server/conn/handler_{monoio,sharded}/ft.rs`: the multi-shard KNN branch.
- `src/command/vector_search/tests.rs`.

In ddb15a7: `scripts/test-commands.sh` (KNNFILT-05) and `scripts/test-consistency.sh` (one parity row).

## Risks
1. **API changes.**
   - `parse_ft_search_args` now returns an `FtSearchArgs` struct.
   - `scatter_vector_search_remote` takes a `filter` argument.
   - `VectorSearchPayload.filter` is a new field.
   - `capture_knn` is replaced by `plan_knn_leg`.

   Other branches fail to compile against these rather than misbehave.
2. **Behaviour change (the fix itself).** At `--shards > 1`, an inline prefilter now filters, and an unreadable one returns `ERR invalid FILTER expression`.
3. **Explicit `FILTER` at multi-shard is still refused.** Lifting that is now a one-line change per handler.
4. **Not changed here:**
   - At `--shards 1`, a query with both FILTER and an inline prefix uses FILTER and ignores the prefix (pre-existing).
   - Multi-shard KNN ignores LIMIT and a non-default `@field`.
   - An error from one shard on an unfiltered query still becomes an empty result.
5. **Performance.** One linear `=>` scan per query, plus one `Arc` per filtered query. No new hot-path allocation, so no release A/B was run.

## CHANGELOG bullet
**FT.SEARCH: an inline KNN prefilter is honoured at `--shards > 1` (moon#1238).**
- The multi-shard scatter used to drop the prefix of `@field:{v}=>[KNN …]` and return the nearest documents unfiltered. An unparseable prefilter returned rows instead of `ERR invalid FILTER expression`.
- The prefilter is now parsed with the single-shard grammar and carried to every shard. A shard that cannot evaluate it fails the query rather than being skipped.
- An explicit `FILTER` clause is still refused at `--shards > 1`.
- ⚠ Behaviour change: these queries now return what `--shards 1` returns.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.92 · Practicality 0.93 · Optimization 0.90 · Edge cases 0.91 · Self-evaluation 0.90.
