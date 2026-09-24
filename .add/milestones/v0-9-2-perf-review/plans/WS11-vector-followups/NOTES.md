# WS11-vector-followups — working notes

## Hygiene
- `refs/stash` is shared by every worktree. My `git stash push` interleaved
  with FIX-ws5a's, and each `pop` took the other's entry. I recovered from the
  dangling commit (f975641), reported it, and never used stash again
  (`git diff > file` / `git apply`). TEAM-RULES now says so.
- A debug build of a DIFFERENT tree (the compose check below) leaves dep-info
  that makes my own sources look fresh, so I `touch` the changed files
  afterwards. Do not `xargs touch` a diff's name list: it recreates deleted
  files as empty files (exact_qjl.rs came back once; it was never compiled
  and I removed it).
- `vector::store::bg_compact_tests::test_bg_compact_pool_parallelism` is a
  wall-clock ratio (a K-worker pool must be < 0.6 × K × a single build). At
  load average 10 on the 4-vCPU box a single build took 11 s and the test
  failed. It passed 2/2 in isolation at normal load, and in the compose run.
  This is the environment, not the code: none of its paths changed.
- Server-level identity needs n < 10,000. `build_graph_auto` switches to the
  concurrent (nondeterministic) builder at PARALLEL_THRESHOLD = 10,000, so
  two runs of the SAME binary differ there.

## moon#1213 item 1 — immutable QJL data
- Readers, exhaustively: `rerank_with_prod` (gated on EMPTY sub-signs AND no
  sidecar), the merge pass-through, and `resident_bytes`. Writer: EXACT
  compaction. Persistence never wrote it (a reload gets it empty), and WARM
  `.mpf` never carried it.
- A fresh compaction always had a non-empty sign buffer (zero-filled for
  SQ8/A2 at the time), so the fallback never ran on a fresh segment.
- The one live reader: a GraphUnion merge that lost signs AND sidecar
  (pre-v2 + pre-HQ-1 source). There HEAD fed `score_l2_prod` the TQ vector
  norm as the residual norm, with a QJL stride derived from `total_count`
  (wrong after dead rows), while its reloaded twin used ADC. Dropping the data
  made in-memory equal to reloaded (red test).
- No format change, so no version gate. EXACT saves (8·⌈d/8⌉+4) B/vector:
  388 B @384d, 772 B @768d.

## moon#1213 item 2 — `qjl_matrices`
- After item 1 the only reader is `metadata_checksum`. HEAD held 8·d²·4 B
  per EXACT collection, per index per shard AND per reloaded EXACT segment
  (segment_io rebuilds a collection per directory). An Arc cache keyed by
  (dim, seed) would still hold one copy per seed, and since seed = collection
  id it would share across shards only when the ids line up. Holding nothing
  is strictly better. The checksum is streamed (`for_each_qjl_chunk` →
  `Xxh64`) and is byte-identical to the one-shot formula (5 golden values
  from f32546c).
- In-process VmRSS: 4 shards × 2 EXACT 768d indexes +145.5 → +1.4 MB;
  6 reloaded EXACT 768d segments +108.2 → +0.0 MB.

## moon#1213 item 3 — WARM signs
- The signs go in codes.mpf after the codes, with `has_sub_signs` (sub-header
  byte 25, reserved = 0 in every older file) set on every page. Older readers
  index codes from the start of the stream, so the tail is invisible to them.
- Reader: a flagged stream must be exactly n·(bpc + ⌈padded/8⌉) bytes, must
  not be all zero, and must not belong to SQ8. Otherwise the signs are
  dropped (with a warning) and the codes kept.
- Composition with PR #1221's "only real sub-centroid signs are built,
  persisted or loaded" (0d30a1e / 345da74): after that fix a HOT segment
  holds either REAL signs (TQ4 insert-time, or encoder-computed for EXACT
  TQ4/TQ4A2) or an EMPTY buffer, never a placeholder. The WARM transition
  carries a buffer only when it is non-SQ8, complete and not all-zero, and
  the reader applies the same three rules. These are the rules `segment_io`
  applies to `sub_signs.bin`, so HOT, HOT-reloaded and WARM always agree on
  "real signs, or the 16-level LUT". At integration the inline `all(== 0)`
  checks can call `sub_signs::is_placeholder` instead.
- Compose check: HEAD merged with fix/pr1221-ws5a (one textual conflict in
  merge.rs, resolved by keeping the fix's `all_have_signs` guards and
  dropping the QJL lines) gives `cargo test --lib -- vector::
  command::vector_search` = 1062 passed, 0 failed.

## moon#1213 item 4 — graph-build input
- LIGHT from the f16 sidecar, measured in-process (release-fast, 20K
  embedding-shaped docs, R@10 vs exact cosine at ef 24/64/128). 384d:
  compaction 7.18 → 5.47 s, far +0.030/+0.007/+0.033, in/near equal. 768d:
  compaction 12.94 → 10.78 s, in −0.005/−0.001/−0.001, far
  −0.035/−0.020/−0.005. That violates "recall must not drop", so it is NOT
  shipped. Next step: several seeds plus real MiniLM before deciding.
- Shipped: EXACT no longer decodes the centroid vectors it never reads
  (n·(4·padded+24) B transient, plus the decode pass).

## moon#1213 item 5 — prefetch
- Measured with an in-binary interleaved A/B (a cfg(test) switch picks
  HEAD's pattern; release-fast; 40K docs; 7 alternating reps; results
  asserted identical). Medians: 768d −4.0% / −4.5% (ef 64/200), 384d
  −8.8% / −7.4%. Kept. At server level (single python client) the effect is
  inside QPS noise.

## Server level (ws11-vector-followups-r1 vs ws5a-vector-engine-r1 vs baseline-935c555)
- `.bench/ws11_bench.py compact`: 10,000 docs, FT.COMPACT, then a 12 s
  jemalloc decay before reading RSS. Two reps, the three binaries
  interleaved.
  - EXACT 384d compaction: 16.6/16.8 (HEAD), 3.31/3.27 (WS5a), 1.60/1.47 s
    (WS11). Peak growth over loaded: 76/76, 66/68, 45/45 MB.
  - EXACT 768d compaction: 72.7/71.5, 12.5/12.2, 2.50/2.51 s. Peak growth
    151/151, 141/142, 97/87 MB. RSS per vector: 10,520/10,548,
    9,042/9,013, 6,195/6,183 B.
  - LIGHT 384d/768d: compaction and RSS unchanged within noise, as
    expected.
  - QPS: no difference beyond noise.
- `identity` (n = 5,000, deterministic build, 300 queries across
  in/near/far): top-10 identical 300/300 for WS11 vs WS5a vs HEAD in EXACT
  and LIGHT at both 384d and 768d.

## moon#1194 — schema-aware payload indexing
- `MOON_VECTOR_PAYLOAD_SCHEMA=declared` (opt-in):
  - TAG: indexed as before;
  - NUMERIC: numeric index only;
  - TEXT: BM25 plane only, with KNN TextMatch routed there;
  - undeclared: not indexed.
  Indexes with no declared payload fields keep HEAD's policy. index_persist
  v6 carries the schema. The sidecar writer also stopped writing v4, which
  had lost the FT.CONFIG knobs on every restart.
- 6,000 RAG docs: the payload index shrinks by 5,136 B/doc (21% of the
  load).
- Known semantic edges under the flag: filters on undeclared fields match
  nothing; a TEXT filter follows the BM25 analysis (stop words, NOSTEM); an
  AS_OF query's TEXT filter sees the current text plane. `serialize_index
  _metas_v5` still ships v5 definitions to replicas (shard RDB aux +
  replication/master.rs), so replicas index every field. That is safe; the
  one-line follow-up belongs to those files' owners.
- key_hash-map merge: DEFERRED. It touches ~150 references in 25 files, 14
  of them outside src/vector: src/shard/spsc_handler.rs ×7 in the insert
  path (WS8's this wave), both conn handlers, and 80+ in
  command/vector_search including tests.rs ×22. The saving is
  18 B / load factor, about 21–41 B per vector. Design: a
  `KeyRecord { key: Bytes, vec_checksum: u64, global_id: u32, flags: u8 }`
  in one `BucketedKeyMap`, a `KeyRegistry` with typed accessors, and a
  `ResolveKey` trait taken by the response/session/hybrid builders. Land it
  after WS8.

## moon#1192 watch item — EXACT mutable far-query recall
- Reproduced on an embedding-shaped fixture (low-rank power-law spectrum,
  shared mean direction, Zipf topics; `.bench/ws11_bench.py
  mutable_recall`). Setup: 5,000 docs, 100 queries per class, mutable-only,
  release binaries, R@10 vs exact cosine, deterministic across 3 reps.
  | | in | near | far |
  |---|---|---|---|
  | 384d HEAD 935c555 | 0.709 | 0.744 | 0.805 |
  | 384d WS5a | 0.898 | 0.906 | **0.675** |
  | 384d WS5a + f16 rerank of top 4k (emulated) | 1.000 | 1.000 | 0.981 |
  | 768d HEAD | 0.836 | 0.845 | 0.814 |
  | 768d WS5a | 0.934 | 0.930 | **0.669** |
  | 768d WS5a + f16 rerank of top 4k (emulated) | 1.000 | 1.000 | 0.978 |
- Cause: TQ-ADC's ‖ĉ‖² term. When the true neighbours are barely similar,
  quantization noise dominates the mutable scan's ADC ranking. Immutable
  segments never show this, because they rerank mult·k candidates from the
  f16 sidecar (HQ-1). The mutable segment keeps the same f16 rows
  (`raw_f16`, both build modes) but never reranks.
- PROPOSAL (not shipped): exact-rerank the mutable scan's top
  `rerank_mult·k` from `raw_f16`, using the kernel and distance convention of
  `ImmutableSegment::rerank_exact`. Emulated, it improves recall on all three
  classes against BOTH HEAD and WS5a, at 384d and 768d. Cost is about
  mult·k f16 dot products per query. It needs its own change: four mutable
  scan entry points (sync, filtered, MVCC, chunked-yielding) must all get it,
  or they diverge.
