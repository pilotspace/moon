# WS13-text-graph-followups — PLAN (wave 2)
personas: `.add/personas/performance-engineer.md` (lead) · `.add/personas/ci-test-integrity-engineer.md` (for moon#1219: a green check is only evidence if it could have gone red)
Context: WS5b's wave-1 work is merged — read `plans/WS5b-text-graph/SUMMARY.md` first (oracle tests, A/B scripts, the new dense columns and PostingList API).

## Issues
1. **moon#1219** ignored cross-shard FT consistency suites broken at their seed: fix the seed, per-test index/prefix isolation, un-ignore; run at `--shards 1` and `--shards 4` against a pinned `MOON_BIN`; assert a non-zero ran count. If a real cross-shard defect surfaces, fix it if it is in `src/text/**` / FT text paths, else file it and report.
2. **moon#1220** text/graph follow-ups (one commit per item, `refs moon#1220`, last `fixes`):
   1. term-at-a-time (or MaxScore/WAND) scoring for wide prefix/fuzzy expansions — scores bit-identical to the current oracle (`text::query::eval::oracle_tests`), 50-term prefix over 200K matches measurably faster;
   2. contiguous per-run posting positions (coordinate with moon#884's allocation fix) — `.tpost` byte-identical or version-gated with a backward-compat test; oldest-vs-newest upsert ratio toward 1.0;
   3. doc-id hole compaction on `.tpost` rewrite + `PostingStore::doc_terms` to a dense column — only if persistence/recovery round-trips stay green;
   4. Cypher `RETURN … ORDER BY … LIMIT k`: evaluate sort keys, keep the top-k, project only those rows — rows identical to `execute_profile` (the oracle);
   5. range `IndexScan` streams in chunks like the label scan.

## Owned files
`src/text/**`, `src/graph/**`, `src/command/graph/**`, `src/command/vector_search/ft_text_search.rs` + BM25 paths of `ft_aggregate.rs`, `tests/inverted_search_*_consistency.rs`, `benches/text_search.rs`, tests `tests/perf_ws13_*.rs`.

## Not yours
`src/vector/**` (WS11), `src/shard/**` (WS8), everything else.
