# Gate zero — where the 47x between the bench reconcile and production goes

**Status: PARTIAL ATTRIBUTION — 1.84x of 44.4x measured, 24.2x named and not
explained.** All five candidates in `CONTEXT.md` are answered. Four are
falsified, including the leading one, which the brief itself proposed and which
turned out to rest on two figures that were not measurements. The fifth
("something not on this list") is two things: **vocabulary**, worth 1.56x, and
**corpus scale**, worth 1.17x — the latter contradicting the published cost
model's "flat in corpus size at fixed density".

A fourth mechanism, memory pressure, was proposed mid-task, tested on Linux
under a cgroup, and **falsified**: it shifts the mean 1.24x and does not
reproduce the tail shape.

The measured terms multiply to **1.84x of a 44.4x gap**. They are the smaller
half of the problem by a wide margin. The residual is 6.6x structural and 3.65x
stall tail, and §6 names both rather than closing the table with a story.

Every timing below is from GCE `t2a-standard-8` (aarch64, 8 vCPU, 31 GB,
dedicated), moon `0bfc730d`, `release` + `RUSTFLAGS="-C target-cpu=native"`,
binary sha256 `51de528cd7f1a203` — **byte-identical to the binary behind
`startup-reconcile-cost-model.md`**. No macOS timing appears here.

Production facts come from its own log and from read-only copies of its AOF and
index sidecars. The live instance was never signalled, restarted, or written to.

---

## 0. Instrument checks, before any result

| check | result |
|---|---|
| bench anchor reproduces | `base` = 0.4726 / 0.4736 / 0.4764 ms/key vs published 0.4767 |
| **noise floor (3 interleaved `base` reps)** | **CV 0.42%; treat anything under ~1.5% as a tie** |
| binary provenance | sha `51de528cd7f1a203`, commit `0bfc730d`, dep-info freshness guard passed |
| first build attempt | **rejected** — `cargo` was not on the non-interactive `PATH`, so the "build" never ran and the box still held a `#885` binary from a later commit. Caught before any leg. |
| independent key count | RDB string scan found 321,884 indexed keys; the log's previous boot reported 322,049 (0.05%) |
| **contamination window audit** | another agent ran a build on this host **03:17:26-03:21:23 UTC**. The binary under test did not finish linking until **03:26:44** and the first leg started **03:26:55** — every measurement post-dates the window by >5 min, so none can have overlapped it. Base reps taken across the whole session read 0.4726 / 0.4736 / 0.4764 / 0.4782 (CV 0.49%), consistent with a quiet box throughout. |
| harness flakes | 2 of 30 legs failed and are reported as failures, not dropped: `prodlike` (`MOONERR busy: compaction backlog` during LOAD — a generator limit, fixed with a retry) and `hb350` (connection reset during `FT.CREATE`, no panic and no OOM in the log; re-run in round 2) |
| vocabulary generator, fit | per-index Heaps curve `V = 2.100 x tokens^0.613`, reset per index: **936** stems @70 docs / **15,725** @7,000 vs production's measured **937** / ~**15,765** |
| vocabulary generator, **independent** check | distinct terms per document 57-69% of 300, against production's measured 60.3% (126.3 of 209.3) — a statistic the fit was NOT tuned on |
| memory-cap guard proven able to fire | probe at `MemoryMax=200M`: `memory.current` pinned at the cap, `memory.swap.current` 1.06 GB, `memory.events: max 5533` |
| first vocabulary calibration | **discarded** — the generator kept one global vocabulary across all indexes, overshooting production by 6.7x at 70 docs/index. Production's indexes are per-repository and each carries its own dictionary; the per-index curve was re-measured from the AOF and the sweep re-run. |

---

## 1. The production corpus, measured

Sources: `appendonlydir/moon.aof.7.incr.aof` (119.6 MB, 31,487 HSETs, 13,674 in
the indexed key family), `shard-0/{vector,text}-indexes.meta` (2,873 + 4,139
index definitions, binary-decoded), `moon.aof.7.base.rdb` (3.9 GB, string scan),
and 165 boots of `moon-6381.log`.

| property | production | bench | ratio |
|---|---|---|---|
| keys reconciled | 293,439 | 35,000 | — |
| vector index definitions | 2,873 | 500 | 5.7x |
| text index definitions | 4,139 | (same 500) | — |
| **indexes actually holding a key** (RDB scan) | **596** | 500 | **1.19x** |
| indexes with recovered durable state (B3 lines) | 158 | — | — |
| indexes matched per key | 1 vector + 1 text, same name | 1 (both planes) | **1.00x** |
| docs per index, **key-weighted** mean (RDB scan) | **7,678** (median 138, max 28,518) | **70** | **110x** |
| **analyzer term passes per key** | **641.8** | **615** | **1.04x** |
| distinct stems in ONE index at 70 docs | **937** | **54** | **17x** |
| distinct stems in ONE index at 7,000 docs | **~15,765** | **54** | **292x** |
| vector dimension | 768 | 384 | 2.0x |
| `hnsw_m` / `ef_construction` | 16 / 200 | 16 / 200 | 1.00x |
| `.tpost` postings on disk | **0 files** | deleted (`notpost`) | 1.00x |
| allocator | jemalloc | jemalloc | 1.00x |
| `--maxmemory` | auto-capped **20.6 GB**, policy `allkeys-lru` | `0` (unlimited) | **unmeasured** |
| keys taking the full re-encode path | **85.5%** (159,591 of 186,710) | see §3.6 | — |

**The README's "~45x document size" guess is wrong by a factor of ~43.**
Production documents are 209.3 analyzer terms of `content` plus 219.3 of `meta`
(a JSON wrapper that re-embeds `content`) plus 4 more from `source`/`valid_time`.
`index_payload_field` runs `insert_text` over every UTF-8 field, so the payload
plane sees 432.5 terms/key and the BM25 plane — whose schema has exactly one
field, `content` — sees 209.3. That is **641.8 passes against the bench's 615**.
The `vec` field is a 3,072-byte non-UTF-8 blob (768 f32) the analyzer skips.

**One configuration difference is not measured.** Production runs under an
auto-applied 20.6 GB `maxmemory` cap with `allkeys-lru`; every bench leg runs
`--maxmemory 0`. The bench corpus is ~1 GB, so no cap would bind there and the
difference cannot be reproduced by simply setting the flag. It is worth flagging
because the anchor boot reconciled **293,439** keys where the boot 22 minutes
earlier had scanned **322,049** — a drop of 28,610 keys between two boots of the
same instance, which an LRU policy under pressure would produce and which
nothing in the log explains. Not a claim; a loose end.

Key naming settles the fan-out question by inspection: keys are
`lunaris_git_<16hex>_chunks_idx:<hex>` — the key *is* the index name plus a
suffix — so a key matches exactly one vector index prefix and its same-named
text index. There is no 2x or 4x fan-out.

## 2. Production per-key cost is a distribution, not a number

21.06 ms/key (the harness's own `scanned-db -> done` interval, §2b) is the mean
of an extremely heavy-tailed distribution. Key-weighted
quantiles over the anchor boot's 432 progress intervals
(`prod-reconcile-series.tsv`):

| p10 | p25 | p50 | p75 | p90 | p95 | p99 |
|---|---|---|---|---|---|---|
| 2.32 | 3.82 | **5.77** | 8.89 | 21.32 | 80.79 | 363.78 |

Threshold-free: **6.2% of keys consume 67.3% of the wall time.** The fastest 10%
of intervals run at 3.40 ms/key. The split is not an artifact of where the
threshold is drawn — see the sensitivity table in `prod-boot-tail.tsv`; the body
rate moves from 4.98 to 8.33 ms/key as the cut moves from 10 to 100 ms/key,
while the overall mean stays 21.09.

The previous boot (315,868 keys, 15.64 ms/key) has the **same body and a
different tail**: body 8.58 ms/key, tail only 5.6% of the time. And three boots
at an identical 46,906 keys with an identical 1,085 vector + 2,483 text indexes
(rows 155-157 of `prod-boots.tsv`, all on 2026-08-20 within 11 minutes) read
**2.90 / 5.58 / 5.67 ms/key** — a **1.96x spread with every input fixed**. The
fourth boot 15 minutes later, at 47,028 keys, read 5.58.

So the 44.4x is really two terms: a **reproducible structural term**
(median interval 5.77 ms/key = **12.2x** the bench's 0.4742) and a **variable
stall term** (21.06 / 5.77 = **3.65x** on this boot, 1.8x on the one before it).
12.2 x 3.65 = 44.5, which is the gap.

### 2b. The two anchors are not measured over the same interval

`reconcile_time.py` — the harness the 0.4767 came from — times
`recovery scanned db` → `auto-reindexed`. `CONTEXT.md`'s 22.35 ms/key times the
*vector sidecar restore* → `auto-reindexed`, which folds in 351 s of sidecar
restore and prefix scan that the bench number excludes.

| interval | seconds | ms/key |
|---|---|---|
| process start → done (client-visible outage) | 6,582.4 | 22.432 |
| vector-restore → done (`CONTEXT.md`) | 6,558.6 | **22.351** |
| text-restore → done | 6,529.6 | 22.252 |
| **scanned-db → done (harness definition)** | **6,178.4** | **21.055** |

Like for like, the gap is **21.055 / 0.4742 = 44.4x**, not 46.9x. **1.06x of the
headline number is definitional.** Everything below uses 21.055.

---

### 2c. Two figures in the task brief were not measurements

Both came from the orchestrator's `CONTEXT.md` / the bench-data README, and both
are falsified by the sweeps below. Recording them so the ranking that follows is
not read as agreeing with the brief:

- **"Index count 7,056 vs 500 = 14.1x, the single largest unmatched ratio ...
  therefore the leading hypothesis."** 7,056 counts index *definitions*.
  Production's *effective* index count — indexes that hold at least one key — is
  **596** (RDB scan), against the bench's 500 — **1.19x**, not 14.1x. The
  task's leading hypothesis was
  wrong, and 14x more indexes measures -1.6% on Linux (§3.1).
- **"The production corpus is ~45x the synthetic document size."** This was
  never measured; the bench-data README obtained it by inverting the cost model
  against the 22.35 ms/key figure — i.e. by assuming the answer and solving for
  document size. Measured against the real corpus, production documents are
  **1.04x** the bench's in analyzer term passes (§3.2).

---

## 3. Falsification, candidate by candidate

Sweep 1: 8 legs, all at **35,000 keys** and `notpost`, 2 interleaved
repetitions, round-robin (`linux-sweep1.tsv`). Noise floor 0.42%.

| leg | shape | ms/key (median of 2) | CV | vs base | verdict |
|---|---|---|---|---|---|
| `base` | 500 idx x 70 docs, dim 384, 300 w | 0.4745 | 0.57% | 1.000x | anchor |
| `idx7000` | **7000** idx x 5 docs | 0.4671 | 0.27% | **0.984x** | index count: **FALSIFIED** |
| `dpi350` | 100 idx x 350 docs | 0.4648 | 0.11% | 0.980x | |
| `dpi1750` | 20 idx x 1750 docs | 0.4841 | 0.03% | 1.020x | |
| `dpi7000` | 5 idx x **7000** docs | 0.4698 | 1.28% | **0.990x** | docs/index: **FALSIFIED** |
| `dim768` | dim **768** | 0.4786 | 0.33% | **1.009x** | vector dim: **FALSIFIED** |
| `w500` | 500 words/doc | 0.7248 | 0.25% | 1.528x | confirms the cost model |
| `uniq` | every term unique | 1.0554 | 0.10% | **2.224x** | **vocabulary: the only axis that moves** |

### 3.1 Index count — FALSIFIED (the leading hypothesis)

**14x more indexes at a fixed key count costs -1.6%.** This is not a tie in the
noise; it is *slightly faster*, and it is falsified twice over:

- **By construction.** `recover_indexes_task` matches prefixes through
  `PrefixMap::any_matching` (`src/util/prefix_map.rs:100`), which is
  `(0..=upto).any(|i| self.by_prefix.contains_key(&key[..i]))` — one hash lookup
  per *prefix length* of the key, capped at the longest registered prefix. That
  is **O(key length), independent of the number of indexes**: ~41 lookups per
  key for production's 48-character keys whether there are 596 indexes or 7,056.
  The `O(keys x indexes)` walk it replaced was removed in **#873**, which the
  production binary already contains (its log emits `recovery scanned db`).
- **By the corpus.** Production has 2,873 vector + 4,139 text index
  *definitions*, but a string scan of the 3.9 GB base RDB finds only **596 of
  them hold a single key** (321,886 keys across 596 index names). The
  `entities`/`facts` families — 1,439 vector definitions — are essentially
  empty: **208** `entities` keys and **zero** `facts` keys in the whole RDB.
  Production's *effective* index count is **596 against the bench's 500 —
  1.19x**, not 14.1x. And 14x measures -1.6%, so even that is moot.

The 7,056-vs-500 = 14.1x that made this the leading hypothesis is a count of
index *definitions*, and definitions are free.

### 3.2 Document size — FALSIFIED as a large term (1.04x, not 45x)

`w500` confirms the published cost model on Linux: 0.1051 + 1.227 us/word
predicts 0.7186 for 500 words, measured 0.7248 (+0.9%). The model is sound.

What was never checked is the input. Both planes analyse, so the unit is *term
passes per key*:

| | text/BM25 plane | payload plane (`index_payload_field`, every non-vector field) | total |
|---|---|---|---|
| bench | `content` 300 + `title` 5 = **305** | `content` 300 + `title` 5 + `path` 4 + `lineno` 1 = **310** | **615** |
| production | `content` **209.3** (its schema has exactly one field) | `content` 209.3 + `meta` 219.3 + `source` 3 + `valid_time` 1 = **432.6** | **641.9** |

**1.04x.** Applying the
model: 0.499 vs 0.483 ms/key. The `~45x` in the bench-data README came from
inverting the cost model against the 22.35 ms/key figure, i.e. it *assumed the
answer* and solved for document size. It is wrong by a factor of ~43.

### 3.3 Multiple indexes per key — FALSIFIED (1.00x)

Production keys are literally `<index name>:<hex>`, so each matches exactly one
vector index prefix and its identically-named text index — the same 1 index /
2 planes the bench builds. There is no 2x or 4x reconcile fan-out.

### 3.4 Vector dimension and HNSW parameters — FALSIFIED (1.009x)

All 2,873 production index metas decode to `dim=768, hnsw_m=16,
ef_construction=200`; the bench's `FT.CREATE` defaults are `m=16, ef_c=200` at
dim 384. Doubling the dimension on Linux costs **0.9%, inside the noise floor**.

### 3.5 Docs per index — FALSIFIED at fixed vocabulary (0.99x)

70 -> 350 -> 1,750 -> 7,000 docs per index at a fixed 35,000 keys reads
0.4745 / 0.4648 / 0.4841 / 0.4698 — a 4.1% spread over a **100x range**, with no
trend. `startup-reconcile-cost-model.md` §3 calls this axis superlinear, and
its own sentence contains the reason the two results agree: it measured
superlinearity *"with a high-cardinality vocabulary"*. **Docs per index is not
an axis. It was a proxy for vocabulary**, which grows with document count in
real text and does not grow at all in the bench's 54-word corpus.

One caveat this sweep did not control for, discovered later: index count also
decides which *recovery path* boot 2 takes (500 x 70 never seals a segment, so
`vunch=0`; 5 x 7,000 seals and reaches the verified-unchanged branch with
`vunch=33,000`). So these legs varied two things. It does not change the
conclusion — every leg landed within 4.1%, and §3.7 measures the two paths
against each other directly at 1.012x — but the sweep was measuring both at
once. See [`gotcha-bench-recovery-path.md`](gotcha-bench-recovery-path.md).

### 3.6 `.tpost` — 1.00x in the table, and the clearest actionable finding

Production has **zero `.tpost` and zero `.tfst` files on disk**, so it boots on
the no-postings path. Both anchors are on that path (every bench leg deletes
`.tpost`), so it contributes **1.00x** to the gap and is correctly absent from
the factor table.

**Why they are absent is now established, not guessed.** A string scan of the
deployed binary `~/.lunaris/bin/moon` finds **0** occurrences of `.tpost`; the
same scan of the bench binary at `0bfc730d` finds **9**. The production binary
**predates #879** — it cannot write those files. Nothing is deleting them, and
there is no durability bug to chase here.

That matters for the ranking even though it is not a factor in the table. On
the no-`.tpost` path a separate A/B measured #885 at 29.0% faster
(0.4781 -> 0.3396 ms/key, floor 0.23%), and #879 measured 49.5% faster with
`.tpost` on. Production is currently getting **neither**. Those are bench-shape
numbers, and this report's whole finding is that bench-shape numbers do not
transfer — so they are quoted as the reason to **measure a redeploy**, not as a
prediction of what one will buy.

### 3.7 The full re-encode path — FALSIFIED (0.99x), and it needed a real control

Production recovers almost none of its durable vector state: at the anchor boot
only **33 of 124** warm segments registered, **291** manifest entries were
retired as missing directories, **290,485** keymap entries were dropped as *"not
backed by any loaded segment (crash inside the async-snapshot window)"*, and
**13** segments loaded across the 158 recovered indexes. The result is that
**159,591 of 186,710** B3-covered keys (85.5%) took the full re-encode branch
rather than the verified-unchanged one. That looked like a strong candidate: a
durability defect forcing production onto a slow path the bench never sees.

It costs nothing.

| leg | shape | `vunch` | `reidx` | ms/key | vs its control |
|---|---|---|---|---|---|
| `hb7000` | 5 idx x 7000, dim 384 | 33,000 | 2,000 | 0.9288 | control (94% fast path) |
| `h7000nokm` | same, dim 768 + `nokeymap` | **0** | **35,000** | 0.9471 | **1.020x** |

Forcing 100% of keys through a full 768-dimension HNSW re-encode instead of 6%
costs **2.0%** here, against a 1.93% CV on that leg. The same comparison at
production's **real** vocabulary, over two reps — `pv7000nk` 0.7502 against
`pv7000` 0.7545 — gives **0.994x**. The two controls bracket 1.00x: the full
re-encode path is free. The reason is in the code: the
verified-unchanged path is `update_metadata_only`
(`src/shard/spsc_handler.rs:4207`), which still calls `remove_field` **and**
`index_payload_field` for every field of every key. It skips the vector
re-encode and nothing else — and the vector plane is 1.009x (§3.4).

**This result needed a control that was actually on the other path.** The first
attempt compared `nokm384` (0.4798) against `base3` (0.4795) and got 1.001x — but
both report `vunch=0`, because 500 indexes x 70 documents never fills a mutable
segment, so nothing is sealed, no manifest is written, and there was no durable
state for `nokeymap` to delete. That comparison proves nothing, and without the
`vunch`/`reidx` columns printed per leg it would have looked identical to the
real result. See [`gotcha-bench-recovery-path.md`](gotcha-bench-recovery-path.md).

### 3.8 Cold tier / page cache — FALSIFIED, both halves

`CONTEXT.md`'s fourth candidate has two halves and they separate.

**Per-key cold-tier fetches: ruled out.** The reconcile walks `db.data()` — the
in-memory DashTable, after the AOF/RDB replay that precedes the `recovery
scanned db` line and is therefore *outside* the measured interval. Among the 543
log lines inside the anchor boot's reconcile window, **zero** match
`evict|oom|maxmemory|spill|memory|pressure|swap|compress`. The vector-segment
COLD/WARM transitions that do appear are the vector tier, not the keyspace, and
their rate is the same inside fast and slow windows (5.54 vs 5.46 per 1,000 s).

**Host memory pressure: tested under a cgroup, and FALSIFIED.** Production is a
24 GB host carrying a 14 GB dataset with ~78 MB of free pages; the bench host is
31 GB carrying ~1 GB. §6b drove a 7.04 GB working set down to a 1.89 GB cap —
5.28 GB in swap, 26,000 throttle events — and the rate moved **1.24x** with the
distribution's shape unchanged. Both halves of `CONTEXT.md`'s fourth candidate
are therefore out.

The 8.72 s vs 119.70 s scan-time variance across two boots of the same data,
which `CONTEXT.md` reads as a page-cache tell, is **not** part of either anchor:
the harness times `scanned-db -> auto-reindexed`, so the scan is excluded from
both sides (§2b).

---

## 4. The axis that survives the sweeps: vocabulary — and it is smaller than it
first looks

`uniq` (every term unique) was the only leg in sweep 1 that moved: **2.224x**.
But it changes two things at once, and the confound is large.

`load.py`'s `WORDS` list is **54 distinct tokens of 5.41 characters**. Its
`UNIQ_WORDS` generator emits terms of **19.32** characters. So `uniq` varies
term *cardinality* and term *length* together. Sweep 3's `lword` leg holds
cardinality at 54 and raises length to **19.41** characters — matching `uniq`'s
19.32 — to separate them:

| leg | distinct types / index | token length | ms/key | vs base |
|---|---|---|---|---|
| `base3` | 54 | 5.41 | 0.4795 | 1.000x |
| **`lword`** | **54** | **19.41** | 0.7383 | **1.540x** |
| `uniq` | ~21,000 | 19.32 | 1.0554 | 2.224x |

**Term length alone is 1.540x of `uniq`'s 2.224x.** The cardinality residual is
2.224 / 1.540 = **1.44x**.

That matters because production's terms are *short*. Measured over 4,000,120
analyzer terms from the real corpus:

| | production | bench `base` | bench `HEAPS` | bench `lword` / `uniq` |
|---|---|---|---|---|
| token-weighted mean length | **6.35** | 5.41 | 14.30 | 19.41 / 19.32 |
| type-weighted mean length | **14.19** | 5.41 | 14.30 | 19.41 / 19.32 |

Production's *tokens* are 6.35 characters — 1.17x the bench's — while its
*types* are 14.19, because the rare tail is full of hashes and identifiers.
Every synthetic generator gets this wrong by emitting one length for both. So
the `HEAPS` legs below, which use a uniform 14.30 characters, **overstate**
production's vocabulary term, and `uniq` overstates it badly.

### The `HEAPS` curve (calibrated cardinality, wrong length)

Per-index dictionary sized to production's measured Heaps curve
(`V = 2.100 x tokens^0.613`, reset per index — 936 stems at 70 docs, 15,725 at
7,000, against production's measured 937 and ~15,765):

| leg | docs/index | types/index | ms/key | vs base |
|---|---|---|---|---|
| `hb70` | 70 | 936 | 0.7836 | 1.639x |
| `hb1750` | 1,750 | ~7,700 | 0.8757 | 1.831x |
| `hb7000` | 7,000 | 15,725 | 0.9288 | 1.937x |

The curve **saturates**: a 17x increase in dictionary size from `hb70` to
`hb7000` buys 1.18x more cost. Most of even this is the 14.30-character terms,
not the dictionary.

### The honest measurement: production's actual vocabulary

`PRODVOCAB` samples the **30,616 real (term, count) pairs** harvested from the
13,674 production documents, capped per index to production's measured
cardinality. It reproduces token length (6.75 / 6.27 against production's 6.35),
type length (13.85 against 14.19) and the real Zipf frequency shape. Sweep 6:

| leg | docs/index | types/index | term source | ms/key | vs `base6` |
|---|---|---|---|---|---|
| `base6` | 70 | 54 | synthetic, 5.41 ch | 0.4829 (CV 1.4%) | 1.000x |
| `pv70` | 70 | 937 | **real corpus** | 0.5640 (CV 0.7%) | **1.168x** |
| `pv7000` | 7,000 | 15,765 | **real corpus** | 0.7545 (CV 4.2%) | **1.562x** |

**At production's real vocabulary the term is 1.56x** — against **1.937x** from
the `HEAPS` generator at the same cardinality. The synthetic generator
**overstated the only surviving axis by 24%**, purely because it emitted 14.30-
character tokens where production has 6.35-character ones. `uniq`, the leg that
first flagged the axis, overstates it by 42%.

(`pv7000` is the noisiest leg in the study at CV 4.2% over two reps, 0.7319 and
0.7770. The term is 1.52-1.62x; nothing below turns on which end.)

That is the whole lesson of this report in miniature: a generator that gets a
distribution's *mean* right and its *shape* wrong will mis-size the term it was
built to measure, and the error is not small.

**Vocabulary is the only confirmed structural term, and it is 1.56x of 44.4x.**

---

## 5. The factor table

Every ratio below is measured on Linux against the baseline **in its own sweep
file**, so no ratio crosses a file boundary. Noise floor 0.42% within a sweep,
1.4% across sweeps (`base` legs read 0.4745 / 0.4782 / 0.4795 / 0.4783 in the
four sweeps). Anything under ~1.5% is a tie.

The gap decomposes in two stages. Production's per-key cost is a distribution,
so the honest split is *structural* (what a key costs when nothing is stalling)
times *stall* (what the tail adds):

```
44.4x total  =  12.2x structural  x  3.65x stall tail
```

<!--FACTOR-TABLE-->
| term | production vs bench | measured | source | verdict |
|---|---|---|---|---|
| effective index count | 596 vs 500 *(7,056 definitions)* | **0.984x** | `idx7000`/`base`, a 14x change | FALSIFIED |
| docs per index | 7,678 vs 70 *(110x)* | **0.990x** | `dpi7000`/`base`, a 100x change | FALSIFIED |
| indexes matched per key | 1 vs 1 | **1.000x** | key naming, by construction | FALSIFIED |
| document size | 641.9 vs 615 term passes | **1.033x** | cost model; `w500` verifies it to 0.9% | FALSIFIED |
| vector dim / HNSW params | 768 vs 384; m=16 ef=200 both | **1.009x** | `dim768`/`base` | FALSIFIED |
| `.tpost` fast path | absent vs absent | **1.000x** | both anchors on the no-postings path | not a term |
| allocator | jemalloc vs jemalloc | **1.000x** | binary string scan | not a term |
| full re-encode path | 85.5% vs 6% of keys | **0.994x** | `pv7000nk`/`pv7000` (2 reps) | FALSIFIED |
| **vocabulary** | **15,765 vs 54 types/index** | **1.562x** | `pv7000`/`base6`, production's REAL terms | **CONFIRMED** |
| **corpus scale** | **293,439 vs 35,000 keys** | **1.168x** | `s294k`/`s035k` interleaved (2 reps), log law on 3 sizes | **CONFIRMED** |
| | | | | |
| **product of measured terms** | | **1.84x** | | |
| **structural gap** | production p50 5.77 / bench 0.4745 | **12.2x** | | |
| **STRUCTURAL RESIDUAL** | | **6.6x** | | **unattributed — §6a** |
| **stall tail** | production mean 21.055 / p50 5.77 | **3.65x** | | **§6b** |
| **TOTAL** | 21.055 / 0.4745 | **44.4x** | 12.2 x 3.65 = 44.4 | |

### Reading the table

**Ten terms measured, eight of them ties.** Everything the brief proposed, and
everything the cost model's ranking implies is worth optimising, multiplies to
**1.84x of a 44.4x gap**. Only two terms are real: vocabulary at 1.56x — visible
at all only because the bench corpus draws from **54 words** — and corpus scale
at 1.17x, which the cost model says should not exist.

**The eight ties are the load-bearing result.** Index count moved 14x and cost
nothing. Docs-per-index moved 100x and cost nothing. Those are the two axes the
cost model and the task brief both rank first.

**The residual is the result.** It is not a rounding error to be argued away; it
is larger than everything measured put together, and it is named in §6.

---

## 6. Naming the residual

Two residuals, not one, and they are different in kind.

### 6a. The structural residual — 6.6x, unattributed

A key in production costs **5.77 ms** at the median. The same key on the bench
costs **0.474**. Ten measured terms explain **1.84x** of that 12.2x. What is
left is **6.6x** — larger than everything measured in this report put together.

Three candidates were available for it. One is now measured, one is now
falsified, and one is out of reach.

#### Candidate 1 — corpus scale: MEASURED, 1.168x

**Measured, and the model's "flat in corpus size" is wrong.** No leg in sweeps
1/2/3/6 varied the corpus at all — every one is 35,000 keys — so none of them
could test this, and the memcap gold vs `h7000nokm` is not a test of it either
(those differ in corpus size *and* in docs-per-index, and the model calls the
second term superlinear). A dedicated sweep pins docs-per-index at 3,500 and
moves only the index count, hence only the key count; dim, words, vocabulary
generator and both sabotages match the memcap gold exactly. 2 reps, interleaved:

| keys | ms/key (rep1 / rep2) | mean |
|---|---|---|
| 14,000 | 0.8556 / 0.8624 | 0.8590 |
| 35,000 | 0.9280 / 0.9140 | 0.9210 |
| 147,000 | 1.0209 / 1.0171 | 1.0190 |
| **294,000** | **1.0793 / 1.0785** | **1.0789** |

ms/key is **not flat in corpus size**: it is log-linear, rising 0.068 ms/key per
natural log of the key count. The two independent slopes agree to 0.9% (0.0677
over 14k->35k, 0.0683 over 35k->147k), which is inside the noise floor.

The published cost model's "flat in corpus size at fixed density" (35k / 70k /
140k = 0.220 / 0.219 / 0.225 ms/key) was measured on the **54-word** corpus,
where the per-index term map never leaves cache however many documents exist.
With a real vocabulary the resident structures grow with the corpus and the walk
touches them in DashTable hash order. **That claim in
`docs/internal/startup-reconcile-cost-model.md` should be scoped or retired.**

**The factor.** The log fit predicts 1.0660 ms/key at production's 293,439 keys.
The direct measurement at 294,000 keys — within 0.2% of production's count —
reads **1.0789**, 1.2% above the prediction and inside the floor. Against the
35k control run **in the same interleaved rounds** (0.9238):

```
corpus scale  35,000 -> 294,000 keys   =  1.0789 / 0.9238  =  1.168x
```

Two guards on this number. The interleaved 35k control read 0.9276 here against
0.9280 in the earlier sweep — **0.04% apart**, so the two sweeps did not drift
and the ratio is not absorbing one. And a log law fitted on six legs at three
sizes predicted the seventh to 1.25% before it was run.

Three guards on this number. The two reps of the 294,000-key leg agree to
**0.07%**. The interleaved 35k control read 0.9238 here against 0.9210 in the
earlier sweep — **0.3% apart**, so the two sweeps did not drift and the ratio is
not absorbing one. And a log law fitted on six legs at three smaller sizes
predicted the seventh to 1.2% before it was run.

**And it does not produce the tail either.** The 294,000-key legs run at
production's own key count with a real vocabulary, and their distribution is
flat: mean/p50 **0.99x**, and the costliest 6.2% of keys carry **6.7%** of the
time against production's **67.3%**. Corpus scale changes the *level*, not the
*shape* — the same verdict §6b reaches for memory pressure, reached
independently on a different axis.

**What it does not explain.** 1.168x, against a 12.2x structural gap.
2. **Memory pressure.** §6b.
3. **Platform.** Production is macOS on Apple Silicon; the bench is Linux on
   Ampere Altra. Same allocator (jemalloc), same source. This term is **not
   measured** — the task forbids a macOS timing, and no Linux experiment can
   produce one. Its sign is not known from data here, and it is not offered as
   an explanation; it is the part of the residual that this investigation cannot
   reach.

#### Candidate 2 — memory pressure: FALSIFIED

§6b in full. Driving 70% of the working set to swap, with the cgroup throttled
26,000 times, moved the mean by **1.24x** and left the distribution's shape
flat. It is not the stall tail, and 1.24x under an extreme that production never
approaches does not supply 6.6x of structural cost either.

#### Candidate 3 — platform: NOT MEASURED, and not offered as an explanation

Production is macOS on Apple Silicon; the bench is Linux on Ampere Altra. Same
allocator, same source — different kernel, different memory subsystem, different
core. The task forbids a macOS timing and no Linux experiment can produce one,
so this term has **no number and no known sign**. It is not being used as an
explanation. It is the part of the residual this investigation cannot reach, and
naming it is not the same as measuring it.

#### What that leaves

**6.6x of the structural gap is unattributed.** With §6b's 3.65x stall tail,
also unattributed, **24.2x of the 44.4x is not explained by anything measured
here** — against 1.84x that is. The measured terms are the smaller half of the
problem by a wide margin, and no combination of them closes it.

### 6b. The stall tail (3.65x) — measured on Linux, not assumed

**The hypothesis, stated so it could fail.** `recover_indexes_task` materialises
the whole `matching` set — every matching key's full HASH contents as
`Frame::BulkString` — before reconciling the first key. At production's shape
that is ~1.7 GB of fresh allocations held for the entire walk, on a 24 GB host
with ~78 MB free pages. *If the tail is allocation stall, constraining available
memory reproduces the tail shape on Linux; if the series stays flat and only
shifts, it is not.*

**Result: FALSIFIED.** It is a uniform shift. Memory pressure is not the tail.

**Setup.** One binary (`51de528cd7f1a203`), one corpus, built once and copied
per leg: 42 indexes x 3,500 docs = **147,000 keys**, dim 768, 300 words/doc,
calibrated Heaps vocabulary, both sabotages (no `.tpost`, no keymap). Each leg
boots inside its own `systemd-run --scope`, including the uncapped control, so
every leg differs in exactly one thing and `memory.peak` is always read from the
cgroup in bytes. Caps are fractions of the measured uncapped peak (7,559,462,912
B = 7.04 GiB). Interleaved, 2 reps. Cap-to-cap noise floor **1.2%** (worst
rep-pair spread), sweep floor **0.53%** (phase-A 1.0446 vs `cap_none` 1.0391).

**The guard fires — proven before the result was read.** A cap that never
throttled measures nothing, so every leg reports what the cgroup did:

| cap | of peak | ms/key (rep1 / rep2) | `memory.peak` | `memory.swap.peak` | throttle events (`max`) |
|---|---|---|---|---|---|
| none | — | 1.0391 / 1.0340 | 7.56 GB | **0** | **0** |
| 6,425 MB | 85% | 1.0632 / 1.0438 | pinned at cap | 0.78 GB | 686 / 685 |
| 4,536 MB | 60% | 1.1143 / 1.1007 | pinned at cap | 2.66 GB | 3,675 / 3,654 |
| 3,402 MB | 45% | 1.1749 / 1.1658 | pinned at cap | 3.78 GB | 7,432 / 7,378 |
| 2,646 MB | 35% | 1.2312 / 1.2191 | pinned at cap | 4.54 GB | 13,444 / 13,572 |
| 1,890 MB | 25% | 1.2882 / 1.2908 | pinned at cap | **5.28 GB** | **25,920 / 26,326** |

`memory.peak` sits exactly on the cap and swap rises monotonically to 5.28 GB of
a 7.04 GB working set. The instrument bound, hard, and the uncapped control
confirms it reads zero when nothing throttles.

**The shape did not move.** The claim is specifically about shape — a heavy tail
over a stable body — so the statistic has to survive a uniform slowdown.
`tailcmp.py`'s ">50 ms/key" threshold is production-scaled and would print 0.0%
at every cap here, measuring nothing; `shape.py` reports the Lorenz curve of
time over keys instead, which a uniform slowdown leaves unchanged.

| | mean/p50 | p99/p50 | costliest 1% of keys carry | 6.2% carry | 25% carry |
|---|---|---|---|---|---|
| **production** | **3.66x** | **63.1x** | **29.6%** of time | **67.3%** | **83.0%** |
| uncapped | 0.98x | 1.05x | 7.1% | 7.1% | 28.6% |
| cap 60% | 1.00x | 1.17x | 6.7% | 13.3% | 33.3% |
| cap 35% | 0.98x | 1.05x | 5.9% | 11.8% | 29.4% |
| cap 25% | 0.99x | 1.27x | 5.6% | 11.1% | 33.3% |

Under the tightest cap the costliest 6.2% of keys carry 11.1% of the time.
In production they carry 67.3%. The bench curve is flat — the costliest keys
carry roughly their own share of the time, which is what "no tail" means.

**Why this survives the obvious objection.** These legs tick every 10 s over a
~150 s run: 14–18 intervals of ~9,000 keys each, against production's 432
intervals of ~680 keys. That is genuinely coarser, and a tail concentrated in
sub-second bursts would be smoothed away — so the *shape* rows above are
suggestive, not conclusive, on their own.

The **mean** is not smoothed. Total time is conserved no matter how coarsely it
is bucketed, and the mean rose from 1.0391 to 1.2908 ms/key: **1.24x at the most
extreme cap**. The term this was recruited to explain is **3.65x**. Memory
pressure severe enough to push 70% of the working set to swap and throttle the
cgroup 26,000 times cannot supply 3.65x, at any tick resolution.

**And the real host should be gentler, not harsher.** This sweep swapped to
disk. Production is macOS, whose memory compressor is compressed-RAM — strictly
faster than a disk swapfile. If disk swap costs 1.24x, RAM compression costs
less. The falsification is conservative.

**Scope of the claim.** This rules out *host-level memory pressure* as the tail
mechanism. It does **not** rule out the `matching` materialisation as a
*structural* cost — that allocation happens on every leg here, uncapped
included, and is inside the measured baseline. What is falsified is that
scarcity of memory is what turns a 5.77 ms body into a 21 ms mean.

**The 3.65x stall tail therefore remains unattributed.** It is the second of two
named residuals, and the fourth candidate mechanism to be measured and refuted.


---

## 7. Revised fix ranking

The old ranking (`startup-reconcile-cost-model.md` §6) is calibrated on a corpus
where analysis is 96.5% of cost. Production's corpus is not that corpus.

### 1. Deploy a post-#879 binary and re-measure. Zero engineering.

The deployed binary contains **zero** `.tpost` strings; `0bfc730d` contains 9.
Production cannot write persisted postings, so it gets neither #879 (49.5% on
the bench) nor #885 (29.0% on the no-`.tpost` path). It is the only item here
whose mechanism is *known absent* in production rather than merely small, and
the only one that costs nothing to try. Measure the boot afterwards — do not
assume the bench percentages transfer, which is what this whole report is about.

### 2. Stop. Do not optimise any of these.

| proposal | measured |
|---|---|
| reduce index count / shard the index set | **0.984x** |
| reduce documents per index | **0.990x** |
| reduce vector dimension, retune HNSW `m` / `ef_construction` | **1.009x** |
| shrink documents | **1.033x** |
| avoid the full re-encode path | **0.994x** |
| reduce per-key index fan-out | **1.000x** (already 1) |
| add RAM / relieve memory pressure | **1.24x** only at 3.7x over-subscription |

Seven plausible proposals, all dead. The keymap durability loss behind the fifth
(33 of 124 warm segments registered, 291 manifest entries retired, 290,485
keymap entries dropped) is still a **correctness and disk-hygiene** bug worth
fixing on its own merits. It is not a startup-latency fix.

### 3. moon#884 — the per-token cost in `TextIndex::insert_terms`.

The only structural axis that moves: **1.5x**. §5.6 of the cost model recorded
it as *"remains untested"*. One correction to its framing: the sub-term that
dominates is the **per-token work proportional to term length** (hashing the
term to probe `field_idx.terms`, and comparing it inside `carried.sort_unstable`),
not the per-novel-term allocation — production has short tokens (6.35 chars) and
a long-tailed *type* distribution (14.19), and the `lword` control shows length
alone is worth 1.54x at 19.41 characters.

### 4. Recalibrate the cost model, or retire its absolute numbers.

`ms/key = 0.1051 + 1.227 us/word` is fitted on a **54-word vocabulary of
5.41-character tokens**. The word-count axis is real and reproduces on Linux to
within 1% (`w500`). The vocabulary those words are drawn from is not
production's, and the model has no term for it at all.

**One claim in it is not a calibration issue but wrong as stated:** *"flat in
corpus size at fixed density"*. At a real vocabulary, ms/key is log-linear in
the key count — 0.068 per ln key, two independent slopes agreeing to 0.9% over a
10.5x range, confirmed by a direct measurement at 294,000 keys (§6a). The
original was measured on the 54-word corpus, where the per-index term map never
leaves cache however many documents exist. Scope that sentence to that corpus or
drop it.

Its README's "the ratios transfer; the absolute ms/key does not" is the right
instinct. This report's finding is stronger: **most of the ratios do not
transfer either**, because they were measured on axes that are flat at
production's shape — and one of the model's own flatness claims is an artifact
of the same 54-word corpus.

### 5. Fix the harness before the next measurement.

Print `vunch`/`reidx` on every reconcile leg. Two legs here compared
configurations that were both on the same recovery path and would have read as a
clean 1.00x result. See [`gotcha-bench-recovery-path.md`](gotcha-bench-recovery-path.md).

---

## 8. What this study did not measure

Stated so the next person does not read a bound as a result.

- **Platform.** Production is macOS/Apple Silicon; every number here is
  Linux/Ampere Altra. Same source, same allocator, same HNSW parameters. The
  task forbids a macOS timing and no Linux experiment can produce one. This term
  is unmeasured and its **sign is unknown**.
- **`--maxmemory`.** Production runs under an auto-applied 20.6 GB cap with
  `allkeys-lru`; every bench leg runs `--maxmemory 0`. The bench corpus is ~1 GB
  so the flag cannot bind there. Related loose end: the anchor boot reconciled
  293,439 keys where the boot 22 minutes earlier scanned 322,049.
- **Tail resolution.** The bench legs tick every 10 s, giving 14-30 intervals
  of 9,000-10,000 keys each, against production's 432 intervals of ~680 keys. A
  tail concentrated in sub-second bursts would be smoothed away, so the *shape*
  rows in §6a/§6b are corroborating, not decisive, on their own. The decisive
  statistic is the **mean**, which no bucketing can smooth: it moved 1.24x under
  the most extreme memory cap, against the 3.65x it was recruited to explain.
- **Memory pressure was tested with a disk swapfile**, not a compressed-RAM
  swap. macOS's memory compressor should be *faster* than disk swap, so §6b's
  falsification is conservative in the right direction — but it is not the same
  mechanism, and no zram leg was run.
- **The production corpus beyond the incr AOF.** Document shape, vocabulary and
  term lengths come from 13,674 real documents — the ~4.7% of the keyspace
  present in `moon.aof.7.incr.aof`. The other 95% lives in the 3.9 GB base RDB
  and was characterised only by **inter-key byte spans**, not parsed. The two
  agree on homogeneity, and the AOF sample is the most recent day of writes, so
  it could be unrepresentative of older documents.
- **Text-only indexes.** 1,266 of the 4,139 text indexes have no vector twin.
  Their keys are inside the 293,439 but they were not modelled separately; every
  bench leg builds both planes on one index.
- **The production instance itself.** Never instrumented, signalled, restarted
  or written to. Everything about it here comes from its log and from read-only
  copies of its AOF and sidecars. A single `perf`/`sample` profile of a live
  reconcile would answer more than the whole of §6a — and taking one is a
  decision for its owner, not for this task.
