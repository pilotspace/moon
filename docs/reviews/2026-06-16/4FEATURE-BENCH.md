# Moon 4-Feature Benchmark — KV · Vector · Graph · Full-Text Search (2026-06-16)

**Where:** GCloud on-demand, x86_64 `c3-standard-8` (Xeon 8481C Sapphire Rapids) + ARM64 `t2a-standard-8`
(Neoverse-N1), 8 vCPU, Ubuntu 24.04. **Moon:** `562ef00` (= db61973 K=1024 code + §2.8 doc), `target-cpu=native`,
fat-LTO. **Baselines (Redis-family, "where a baseline exists"):** Redis 7.0.15 (KV) · RediSearch via
`redis/redis-stack-server` Docker (Vector + FTS) · FalkorDB Docker (Graph). Both competitors came up on **both** arches.
**Method:** same-run ratios; KV via `redis-benchmark` best-of-3; Vector/FTS/Graph via an 8-thread concurrent
redis-py harness (QPS + p50/p99) + recall@10 vs numpy exact KNN. No CPU pinning. Harness: `gce-4feature-bench.sh`.

> **One-line takeaway:** Moon is a clear win on **KV throughput** and on **bulk insert** (vector 6–20×, graph 21–26×
> faster than the specialized competitor), and competitive on low-cardinality FTS terms + native graph 1-hop. It
> **trails** the mature competitors on **vector search QPS+recall**, **FTS indexing + high-DF queries + some query
> correctness**, and **Cypher point-queries**. Several gaps were predicted by the code review (FTS O(N) TF lookup).

---

## 1. KV — Moon-fair vs Redis  ✅ Moon wins pipelined

Moon `--appendonly no --disk-offload disable --initial-keyspace-hint 1000000`. Best-of-3.

| arch | method | GET p64 | SET p64 | GET p16 | SET p16 | GET p1 | SET p1 |
|------|--------|:-------:|:-------:|:-------:|:-------:|:------:|:------:|
| x86 | loose  | **1.90×** (4.55M) | **1.67×** (2.99M) | 0.97× | **1.42×** | 0.81× | 0.81× |
| x86 | strict | **1.24×** (1.29M) | **1.20×** (957K) | **1.27×** | **1.20×** | 0.81× | 0.75× |
| ARM | loose  | **1.79×** (2.90M) | **2.05×** (2.56M) | 0.96× | **1.13×** | 0.87× | 0.84× |
| ARM | strict | **1.25×** (976K) | **1.24×** (821K) | **1.16×** | **1.14×** | 0.84× | 0.83× |

Consistent with §2.8: Moon wins at pipeline depth (the per-shard batch path), loses p=1 (GCloud TCP-RTT-bound).
No regression. (Ratio = Moon/Redis; absolute Moon RPS in parens.)

---

## 2. Vector — Moon vs RediSearch  ⚖️ Moon insert ≫, RediSearch search ≫

50K × 384d clustered vectors (mixture-of-Gaussians → meaningful recall), COSINE, KNN-10, 8 concurrent threads.

| metric | x86 Moon | x86 RediSearch | ARM Moon | ARM RediSearch |
|--------|:--------:|:--------------:|:--------:|:--------------:|
| **insert/s** | **25,133** | 3,989 | **35,937** | 1,799 |
| search QPS (HNSW) | 552 | **9,088** | 505 | **6,842** |
| search p50 | 14.2 ms | **0.71 ms** | 15.5 ms | **1.17 ms** |
| **recall@10** (HNSW) | 0.858 | **0.961** | 0.858 | **0.961** |
| recall@10 (mutable "brute") | 0.759 | — | 0.759 | — |

- **Moon insert is 6.3× (x86) / 20× (ARM) faster** — zero-copy HSET + in-memory auto-index (matches §10.4's 7.7×).
- **RediSearch search is ~16× higher QPS and higher recall (0.96 vs 0.86)** at 384d under concurrency. Moon's
  default auto-quantization (SQ8/TQ) trades recall for memory; the mutable-segment "brute" recall of 0.76 reflects
  background compaction already quantizing at 50K (CLAUDE.md: 384d quantization loses recall).
- **Reframes §10's 12.7K QPS** — that was single-connection *pipelined* Moon-only. Under 8-way concurrency vs a live
  RediSearch, Moon's vector *search* is the weaker side; its *insert* is the strong side.
- RediSearch has no `FT.COMPACT` (auto-builds HNSW on insert) → its "brute" row **is** its HNSW.

---

## 3. Full-Text Search — Moon vs RediSearch  ⚠️ Moon early-stage (NEW benchmark)

100K docs, Zipf vocab (8000 terms), BM25; TEXT + TAG + NUMERIC schema; 8 concurrent threads.

| query | x86 Moon qps (p50) | x86 RediSearch qps (p50) | note |
|-------|:------------------:|:------------------------:|------|
| index docs/s | **376** | **18,052** | Moon ~48× slower to index |
| term_hi (high-DF ~5k docs) | 19 (419 ms) | **3,813** (2.1 ms) | **Moon O(M²) TF-lookup cliff** (review P1, confirmed) |
| term_mid (~1k docs) | 429 (18.6 ms) | **4,909** (1.0 ms) | Moon slower |
| term_rare (~95 docs) | **7,765** (0.81 ms) | 3,606 (1.9 ms) | **Moon faster** (small posting, lock-free) |
| and2 | 7,665 | 8,485 | parity; both total-hits=10 |
| or2 | 7,686 | 3,474 | **Moon OR broken**: total=10 vs RediSearch 2072 |
| tag `@cat:{}` | 7,791 | 3,319 | Moon total=10 vs RediSearch 5,064 (count semantics) |
| numeric `@price:[..]` | 107 (74.7 ms) | **351** (22.8 ms) | Moon ~3× slower |
| text+tag combo | 12,843 | 4,077 | **Moon returns 0 hits** vs RediSearch 253 — combo broken |
| aggregate groupby | 6 (698 ms) | 11 (361 ms) | Moon ~2× slower |

ARM mirrors x86 (Moon index 410/s, term_hi 15 qps/533 ms, term_rare 7,111 qps; RediSearch index 12,418/s).

**Findings (this is the headline of the run):**
1. **Indexing is ~40–48× slower than RediSearch** (376–410 vs 12K–18K docs/s) — consistent with the review's O(V)
   per-doc upsert scan (`posting.rs:139`) + synchronous analyzer pipeline.
2. **High-DF query cliff** — a term in ~5% of docs takes **419 ms** (19 qps) vs RediSearch's 2 ms. This is the
   review's **P1 O(N) TF-lookup → O(M²)** (`store.rs:504`), reproduced exactly at scale.
3. **Correctness gaps:** OR (`|`) doesn't union (total 10 vs 2072); TEXT+TAG combo returns 0; TAG/NUMERIC report the
   *returned* count not the *total-matched* (10 vs 5,064 / 10,119) — an `FT.SEARCH` total-count protocol deviation.
4. **Moon wins low-DF single-term** (rare term 7,765 vs 3,606 qps) — when posting lists are small the per-shard
   lock-free path is genuinely fast.

Net: Moon's full-text is **functional but early-stage** vs RediSearch's mature engine. The fixes are well-scoped
(RoaringBitmap::rank for TF, OR-union, combo-query, count semantics, async/batched indexing).

---

## 4. Graph — Moon vs FalkorDB  ✅ Moon native ≫ build & 1-hop; ⚠️ Moon Cypher can't point-filter

5K nodes, 15K edges. Moon native `GRAPH.CREATE`/`ADDNODE`/`ADDEDGE`; FalkorDB Cypher. 8 concurrent threads.

| metric | x86 Moon | x86 FalkorDB | ARM Moon | ARM FalkorDB |
|--------|:--------:|:------------:|:--------:|:------------:|
| **build ops/s** | **25,333** | 962 | **12,592** | 605 |
| 1-hop qps (Moon native / Falkor Cypher) | **5,671** | 4,739 | **4,474** | 3,794 |
| 1-hop p50 | **1.12 ms** | 1.53 ms | 1.54 ms | 1.94 ms |
| 2-hop qps | (Cypher 13†) | **4,459** | (Cypher 9†) | **3,639** |
| Moon Cypher 1-hop | 40 qps (183 ms)† | — | 28 qps (245 ms)† | — |

† **Moon's Cypher `MATCH (a {id:N})` does not filter on an inline node property** — it full-scans the label
(`cypher_match_rows=14991` ≈ all edges vs FalkorDB's correctly-filtered 4). So Moon Cypher point-queries are
whole-graph scans (40/13 qps); FalkorDB's filtered Cypher wins those decisively.

- **Moon builds 21–26× faster** (native API) and **native 1-hop neighbor lookup edges out FalkorDB Cypher** (5,671 vs
  4,739 qps) — matches §11.3's "native API" strength.
- **But for Cypher point/2-hop queries, FalkorDB wins** because Moon's Cypher subset doesn't narrow on node-property
  predicates. Use `GRAPH.NEIGHBORS` for Moon point lookups, not Cypher.

---

## 5. Code-review findings the benchmark confirmed

| Review finding (DEEP-REVIEW.md) | Benchmark evidence |
|---|---|
| FTS **P1 O(N) TF lookup** (`store.rs:504`) → O(M²) | term_hi: 419 ms / 19 qps vs RediSearch 2 ms / 3,813 qps |
| FTS **P1 O(V) upsert scan** (`posting.rs:139`) | indexing 376–410 docs/s vs RediSearch 12K–18K |
| Graph **Cypher subset** (no inline property filter) | `cypher_match_rows`=14,991 (full scan) vs FalkorDB 4 |
| Vector **default quantization** (SQ8/TQ at 384d) | recall 0.86 vs RediSearch FP32 0.96 |

## 6. Caveats
- No CPU pinning (same-run ratios are the signal). 8-thread concurrency for Vector/FTS/Graph (latency+QPS), not a
  single-stream pipeline (so not comparable to §10's single-conn 12.7K).
- Vector/FTS use synthetic data (clustered vectors / Zipf corpus) — directional, reproducible; absolute recall on real
  MiniLM embeddings is higher (§10: 0.96).
- Moon FTS `hits` = returned-count, RediSearch `hits` = total-matched — counts not directly comparable (a finding itself).
- RediSearch/FalkorDB are single-process; Moon ran `--shards 1` for a like-for-like single-keyspace comparison.
