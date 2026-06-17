# Moon 4-Feature Benchmark — VERIFIED cross-arch (2026-06-17)

**Purpose:** prove the v3-1-fts-hardening + v3-2-graph-correctness fixes *work* under a real
cross-arch GCloud benchmark, with the **graph-filter harness bug fixed** so graph numbers are
apples-to-apples with FalkorDB in-harness (not via a side diagnostic, as the 2026-06-17 v3-2
re-baseline had to do).

**Where:** GCloud on-demand — x86_64 `c3-standard-8` (Xeon 8481C Sapphire Rapids, 2.70 GHz,
`us-central1-a`) + ARM64 `t2a-standard-8` (Neoverse-N1, `us-central1-b`), 8 vCPU, Ubuntu 24.04, pd-ssd.
**Moon:** `8238515` (= `da87a75` v3-2 fixes + `adb0bca`/`2329834` v3-1 fixes, folded → foundation-v6),
`target-cpu=native`, release. **Baselines:** Redis 7.0.15 (KV) · RediSearch `redis/redis-stack-server`
(Vector + FTS) · FalkorDB (Graph) — all up on both arches. **Harness:** `docs/reviews/2026-06-17/gce-4feature-bench.sh`
(= the 2026-06-16 harness with the §GRAPH filter fixed to use the `id` **property value** `i`, not the
internal `GRAPH.ADDNODE` handle `nid[i]`; `BR=main`). Raw logs: `bench-{x86,arm}.log` (this dir).
Instances DELETED after the run (0 instances / 0 disks verified).

> **Bottom line:** All three fix-sets are **verified working** on both arches.
> **Graph (v3-2):** Moon's Cypher point-query now narrows to the matched node (`cypher_match_rows=4`,
> *identical to FalkorDB's 4*) and runs ~28–30× faster than its old full-scan self.
> **FTS (v3-1):** indexing is ~40× faster (now near-RediSearch), the O(M²) high-DF cliff is gone, and
> **every query combinator returns the exact total-matched count RediSearch returns** (OR/TAG/NUMERIC/combo
> were all broken at 2026-06-16). **KV & Vector:** no regression — KV still wins pipelined, Vector insert
> still wins big, Vector search QPS/recall gap unchanged (= v3-3-vector-kv-polish territory).

---

## 1. GRAPH — v3-2 inline-filter ✅ verified correct in-harness (was the whole point)

5K nodes / 15K edges. Moon native `GRAPH.CREATE`/`ADDNODE`/`ADDEDGE`; both engines queried via Cypher.
8 concurrent threads.

| metric | 2026-06-16 (pre-v3-2) | **2026-06-17 verified** | FalkorDB | verdict |
|--------|:---------------------:|:-----------------------:|:--------:|---------|
| Moon `cypher_match_rows` (x86 & ARM) | 14991 (full scan) | **4** | 4 | **identical to FalkorDB — fix proven** |
| Moon cypher_1hop qps (x86) | 40 (183 ms) | **1195 (6.68 ms)** | 4734 (1.54 ms) | **~30× vs old self**; ~4× behind Falkor |
| Moon cypher_1hop qps (ARM) | 28 (245 ms) | **787 (10.15 ms)** | 3869 (1.90 ms) | **~28× vs old self** |
| Moon cypher_2hop qps (x86) | 13 (full scan) | **1156 (6.92 ms)** | 4395 | **~89×** |
| Moon cypher_2hop qps (ARM) | 9 | **755 (10.33 ms)** | 3659 | **~84×** |
| Moon build ops/s (x86 / ARM) | 25333 / 12592 | **25094 / 13939** | 959 / 604 | **~26× / ~23×** (Moon native ≫) |
| Moon native_neighbors qps (x86 / ARM) | 5671 / 4474 | **5650 / 4442** | (Falkor Cypher 4734 / 3869) | native edges out Falkor Cypher |

**The fix:** v3-2 added the inline `Filter` op so `MATCH (a:N {id:N})` narrows on the node-`id` property
instead of scanning the whole label. The 2026-06-16 bench showed Moon returning ~all 15K edges
(`cypher_match_rows=14991`); now it returns **4** — *exactly* FalkorDB's filtered result on the same graph.
The 2026-06-17 v3-2 re-baseline could only prove this with a side diagnostic because its harness filtered
on `nid[i]` (the internal handle) → printed a false `0`; this run fixes the harness and proves it inline.

**Still behind FalkorDB (~4×)** because Moon does a **filtered label scan** while FalkorDB has a **property
index** on `id`. A property index for inline-equality is a legitimate future optimization — explicitly out
of v3-2 scope. Moon's native `GRAPH.NEIGHBORS` (5650/4442 qps) remains the fast path for point lookups.

---

## 2. FTS — v3-1 hardening ✅ counts exact, cliffs gone (x86 shown; ARM mirrors)

100K docs, Zipf vocabulary (8000 terms), BM25, TEXT + TAG + NUMERIC schema; 8 concurrent threads.
**Every Moon `hits` count below equals RediSearch's `hits` exactly** — the headline correctness result.

| query | 2026-06-16 Moon | **2026-06-17 Moon** | RediSearch | `hits` (Moon = RS) | verdict |
|-------|:---------------:|:-------------------:|:----------:|:------------------:|---------|
| index docs/s | 376 | **15052** | 18232 | — | **~40× faster**, ~0.83× parity |
| term_hi (high-DF ~5k) | 19 qps / 419 ms | **240 qps / 33 ms** | 3791 / 2.1 ms | 5068 = 5068 | **O(M²) cliff gone** |
| term_mid (~1k) | 429 | **1416** | 5293 | 1046 = 1046 | faster, correct |
| term_rare (~95) | 7765 | **7714** | 3594 | 95 = 95 | **Moon wins** (low-DF) |
| and2 | 7665 (hits=10) | **841** | 8277 | 10 = 10 | correct |
| or2 (`\|`) | 7686 (**hits=10 broken**) | **688** | 3778 | **2072 = 2072** | **union fixed** |
| tag `@cat:{}` | 7791 (**hits=10 broken**) | **2227** | 3367 | **5064 = 5064** | **count fixed** |
| numeric `@price:[..]` | 107 | **97** | 377 | 10119 = 10119 | correct |
| text+tag combo | 12843 (**hits=0 broken**) | **270** | 4314 | **253 = 253** | **combo fixed** |
| aggregate groupby | 6 | **5** | 9 | — | ~parity |

**Reading the QPS honestly:** the 2026-06-16 OR/TAG/combo numbers looked *fast* (7–12K qps) only because
they did almost no work (returned 10 or 0 hits — broken). Now that they compute the full correct result
(2072 / 5064 / 253 hits), the QPS reflects real work and is lower but **correct**. Moon still trails
RediSearch on raw multi-term QPS, indexes at near-RediSearch rates, and **wins low-DF single-term**.

ARM mirrors x86 (index 10768 docs/s, term_hi 146 qps/54.5 ms hits=5068, tag 1026 qps near RediSearch's 1143;
all counts identical to RediSearch). Full numbers: `bench-arm.log`.

All four correctness gaps the 2026-06-16 bench flagged — OR union, TAG/NUMERIC total-match counts, TEXT+TAG
combo, O(V) indexing + O(M²) high-DF — are **resolved on both arches**.

---

## 3. KV — no regression ✅ Moon wins pipelined (Moon-fair vs Redis, ratio = Moon/Redis)

| arch | mode | GET P64 | SET P64 | GET P16 | SET P16 | GET P1 | SET P1 |
|------|------|:-------:|:-------:|:-------:|:-------:|:------:|:------:|
| x86 | loose  | **1.87×** (4.44M) | **1.69×** (2.99M) | 0.96× | **1.43×** | 0.81× | 0.82× |
| x86 | strict | **1.15×** (1.10M) | **1.14×** (864K) | **1.11×** | **1.12×** | 0.83× | 0.77× |
| ARM | loose  | **1.85×** (2.99M) | **2.05×** (2.50M) | 0.95× | **1.34×** | 0.82× | 0.82× |
| ARM | strict | **1.27×** (1.02M) | **1.32×** (870K) | **1.12×** | **1.19×** | 0.83× | 0.80× |

Consistent with §2.8 / 2026-06-16 (x86 loose P64 1.87–1.90×). Moon wins at pipeline depth (per-shard batch
path), loses p=1 (TCP-RTT bound). v3-1/v3-2 touched only text/graph paths — KV unaffected, as expected.

---

## 4. Vector — no regression ⚖️ Moon insert ≫, RediSearch search ≫ (= v3-3 territory)

50K × 384d clustered vectors (mixture-of-Gaussians → meaningful recall), COSINE, KNN-10, 8 threads.

| metric | x86 Moon | x86 RediSearch | ARM Moon | ARM RediSearch |
|--------|:--------:|:--------------:|:--------:|:--------------:|
| insert/s | **28136** | 3750 | **28343** | 1906 |
| search QPS (HNSW) | 473 | **9143** | 540 | **6926** |
| search p50 | 17.05 ms | **0.71 ms** | 14.29 ms | **1.17 ms** |
| recall@10 (HNSW) | 0.858 | **0.961** | 0.858 | **0.961** |
| recall@10 (mutable brute) | 0.759 | — | 0.759 | — |

Moon insert **7.5× (x86) / 14.9× (ARM)** faster (zero-copy HSET + in-memory auto-index). RediSearch search
~16× higher QPS at higher recall (0.96 vs 0.86) — Moon's default SQ8/TQ quantization trades recall for
memory at 384d. Identical to 2026-06-16; Vector untouched by v3-1/v3-2. The QPS/recall gap is the explicit
target of the planned **v3-3-vector-kv-polish** milestone.

---

## 5. Caveats
- No CPU pinning; same-run ratios are the signal. 8-thread concurrency for Vector/FTS/Graph (QPS + p50/p99),
  not single-stream pipelining (so not comparable to BENCHMARK.md §10's single-conn 12.7K vector QPS).
- Vector/FTS use synthetic data (clustered vectors / Zipf corpus) — directional, reproducible.
- RediSearch has no `FT.COMPACT` (auto-builds HNSW on insert) → its "brute" row *is* its HNSW.
- Graph `cypher_match_rows=4` is the out-degree of node `id:0` in the harness's seeded graph; both engines
  bench the *same* graph in the *same* harness, so 4 = 4 is a true apples-to-apples agreement.
- Total run: ~11–14 min wall per instance (build ~3m50s x86 / 4m38s ARM + benches). ~$0.50 total.
