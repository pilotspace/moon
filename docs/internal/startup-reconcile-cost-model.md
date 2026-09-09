# The startup reconcile — a bench model, and why it does not describe production

Written after two days spent on why a production instance took **1 hour
50 minutes** to become available after a restart. Its purpose is largely
**negative knowledge**: thirteen explanations were proposed and eleven were
measured to be wrong, including every one this document's first version ranked
worth fixing. All of them are recorded here so nobody spends a third day
re-deriving them.

**The gap is now fully decomposed, and the largest term is the machine.**
Production reconciles a key **44.4x** slower than the synthetic bench. Ten
candidate *code and corpus* terms were measured on Linux; eight are ties. The
residual those left — 24.1x, unexplained for a day — was closed by booting
production's own data dir on Linux with the same commit: **macOS costs 5.80x on
this workload.** See §4b.

Every number below comes from a Linux host — GCE `t2a-standard-8` (aarch64,
8 vCPU, 31 GB RAM), dedicated, moon `0bfc730d` built with the shipping `release`
profile and `RUSTFLAGS="-C target-cpu=native"`. Raw data:
[`bench-data/2026-09-09-reconcile-linux/`](bench-data/2026-09-09-reconcile-linux/).

**No macOS number appears in this document, and none should be added.** Every
figure this investigation produced on a developer laptop was either wrong or
misleading, one of them by 2.5x in the direction that would have deprioritised
the fix it was pointing at.

---

## 1. What the reconcile is

After the keyspace is restored, `recover_indexes_task`
(`src/shard/event_loop.rs`) restores index definitions from the sidecars and
then walks every key matching an index prefix, re-deriving its postings and
vectors. Until it finishes the shard answers `-LOADING` to every data command,
so from a client's point of view the server is down for the whole walk.

## 2. The two anchors, and the definitional gap between them

| interval | production ms/key |
|---|---|
| process start -> done (client-visible outage) | 22.432 |
| vector-restore -> done | 22.351 |
| **scanned-db -> done (the harness's definition)** | **21.055** |

The bench harness times `recovery scanned db` -> `auto-reindexed`. Comparing
production's *whole boot* against that is comparing different intervals, and
**1.06x of the originally reported 46.9x was definitional.** Like for like the
gap is **21.055 / 0.4745 = 44.4x**.

## 3. Production is a distribution, not a number

Key-weighted quantiles over the anchor boot's 432 progress intervals:

| p10 | p25 | p50 | p75 | p90 | p95 | p99 |
|---|---|---|---|---|---|---|
| 2.32 | 3.82 | **5.77** | 8.89 | 21.32 | 80.79 | 363.78 |

**6.2% of keys consume 67.3% of the wall time.** So the gap is two terms:

```text
44.4x total  =  12.2x structural (p50 5.77 vs bench 0.4745)  x  3.65x stall tail
```

Four boots over a byte-identical corpus read 2.90 / 5.58 / 5.67 / 5.58 ms/key —
a **1.93x spread with every input fixed.** No cost model explains that.

## 4. The factor table

Noise floor 0.42% within a sweep, 1.4% across sweeps. Anything under ~1.5% is a
tie.

| term | production vs bench | measured | verdict |
|---|---|---|---|
| effective index count | 596 vs 500 *(7,056 definitions)* | **0.984x** | FALSIFIED |
| docs per index | 7,678 vs 70 *(110x)* | **0.990x** | FALSIFIED |
| indexes matched per key | 1 vs 1 | **1.000x** | FALSIFIED |
| document size | 641.9 vs 615 term passes | **1.033x** | FALSIFIED |
| vector dim / HNSW params | 768 vs 384; m=16 ef=200 both | **1.009x** | FALSIFIED |
| full re-encode path | 85.5% vs 6% of keys | **0.994x** | FALSIFIED |
| `.tpost` fast path | absent vs absent | 1.000x | not a term |
| allocator | jemalloc both | 1.000x | not a term |
| **vocabulary** | **15,765 vs 54 types/index** | **1.562x** | **CONFIRMED** |
| **corpus scale** | **293,439 vs 35,000 keys** | **1.168x** | **CONFIRMED** |
| **product of measured terms** | | **1.84x** | |
| **STRUCTURAL RESIDUAL** | | **6.6x** | resolved in §4b |
| **stall tail** | mean 21.055 / p50 5.77 | **3.65x** | resolved in §4b |
| **TOTAL** | 21.055 / 0.4745 | **44.4x** | |

**The eight ties are the load-bearing result.** Index count moved 14x and cost
nothing — `PrefixMap` (`src/util/prefix_map.rs`) makes prefix matching O(key
length), and the O(keys x indexes) walk was removed in #873. Docs-per-index
moved 100x and cost nothing. Those are the two axes this document's first
version ranked first.

**The residual is larger than everything measured put together.** It is not a
rounding error to argue away.

## 4b. The residual was the platform — measured, not inferred

The 24.1x left over after the ten sweeps was closed by the one experiment the
first pass called untestable: boot **production's own 15 GB data directory** on
a Linux host, with the **same commit** and the **same starting state**.

```text
macOS,  adf3a808, production data, no .tpost    4.585 ms/key   (303,778 keys)
Linux,  adf3a808, production data, no .tpost    0.790 ms/key   (303,778 keys)
                                                -----------
                                                     5.80x
```

Same binary, same corpus, same zero-`.tpost` start. **macOS is 5.80x slower**,
and that figure is *conservative*: the macOS run had a warm page cache (the
directory had just been cloned), the Linux run started cold.

The whole gap then multiplies out:

| term | factor |
|---|---|
| binary + page cache + boot-to-boot variance (old macOS binary -> `adf3a808`) | 4.59x |
| **platform / environment (macOS -> Linux, same commit and data)** | **5.80x** |
| production corpus vs the 54-word synthetic bench (both Linux) | 1.66x |
| **product** | **44.37x** |
| target (21.055 / 0.4745) | **44.40x** |

**This validates §4's factor table from an independent direction.** That table
predicted the corpus term as vocabulary 1.562x times scale 1.168x = **1.82x**,
derived entirely from synthetic sweeps. Measured against the real production
corpus on the same host: **1.66x**. Within 10%. The eight ties are real.

Two confounds stay attached and must not be dropped when this is quoted: the
Linux host has 31 GB for a 15 GB dataset where the production Mac has 24 GB for
the same data plus everything else it runs, and only the macOS side had a warm
cache. Both bias *toward* Linux, so 5.80x is an **upper bound** on a pure-OS
term and a fair estimate of the environment term as actually experienced.

### What `.tpost` is worth on a real corpus

The same run measured the fast path on production data, which the 54-word
fixture could not:

```text
Linux, no .tpost      0.790 ms/key
Linux, .tpost present 0.603 ms/key      ->  24% faster
```

**The bench claimed 49.5%.** It overstated the benefit by roughly 2x. This
document's own thesis — that bench percentages do not transfer — applies to the
one number in it that looked safest.

One operational detail found the same way: the clean-shutdown flush
(`persist_all_postings_and_wait`) wrote **618** `.tpost` files, while a
production instance sampled shortly after boot had **103** — the periodic
duty-cycled writer had not caught up. A hard kill therefore forfeits most of the
fast path on the next boot. Shut down cleanly.

## 5. What the bench model actually describes

Within the bench's own corpus the model is sound and reproducible:

```text
ms/key = 0.1051 + 1.227 us/word     (verified on Linux to 0.9% at 500 words)
```

Two corrections to its first published form:

- **It is NOT "flat in corpus size at fixed density."** It is log-linear in key
  count: 14,000 / 35,000 / 147,000 keys read 0.8590 / 0.9210 / 1.0190 ms/key,
  slopes 0.0677 and 0.0683 per ln key (0.9% apart), and the fit predicted the
  294,000-key leg to 1.2% before it ran.
- **Its "superlinear in docs-per-index" axis does not exist.** That was
  vocabulary in disguise — the original measurement's own sentence says it was
  taken *"with a high-cardinality vocabulary."* At fixed vocabulary, 70 ->
  7,000 docs per index is 0.99x.

## 6. The fixture was a monoculture, and that is why the model misleads

The synthetic corpus draws from **54 words** (~100 distinct stems). Production
carries **15,765 types per index**. Vocabulary is the only content axis that
moves the reconcile at all, and the bench held it at a value 290x too low.

A monoculture fixture does not merely hide a bug. It produces a confident,
internally consistent, **published** performance model of a system that does not
exist — one that aimed this investigation at tokenization, which turns out to
differ between bench and production by **1.03x**.

Even the corrected vocabulary term needed a second correction: an all-unique
generator first measured 2.22x, which fell to **1.56x** once the generator was
rebuilt from 30,616 real production `(term, count)` pairs, because synthetic
terms averaged 14.30 characters against production's 6.35.

**Before trusting any performance model, measure the real corpus's cardinality
and put it in the fixture. State the fixture's vocabulary size next to every
number derived from it.**

## 7. Disproved — do not re-derive these

**1. "The `cooperative_yield` between rescan slices is expensive."** Refuted by
`/usr/bin/sample`: 8,459 of 8,459 shard-thread samples were on-CPU inside
moon's own reconcile stack. The thread never parks.

**2. "moon#873's 10 ms slice budget regressed it."** Refuted by A/B. The
measurements behind the claim were taken while a 533%-CPU `rustc` — spawned by
the investigation itself — saturated the box.

**3. "The remaining time is 12 hours."** The server's own `ETA for this db` is
computed from the RECENT rate and read 121,067 s while the cumulative rate on
the same log line implied ~48 minutes. Tracked as **moon#882**.

**4. "It is macOS memory-compressor pressure."** Refuted by the full
199-interval series, which runs the OPPOSITE way: `corr(keys, decompressions) =
+0.440`. Decompression rate is an **output** of allocating hard, not an input.
Published as "unambiguous" on four hand-picked rows before the series existed.

**5. "The rate is bimodal — two regimes 150x apart."** One noisy ramp with ~10x
neighbour scatter. Choosing two window boundaries manufactures "two regimes"
where none exist.

**6. "`add_term_occurrence` hashes every term twice — 41.8% of self-time."**
The 41.8% is the whole function; the duplicate probe runs warm. +0.6% against a
2.0% floor. **moon#884**, retitled to the allocations question.

**7. "The 48.7% residual is a redundant second stemming pass."** It is the
vector plane's payload/filter rebuild. Redundant only when both planes run; on
the `.tpost` path the text plane is skipped and it is the ONLY thing rebuilding
filter state.

**8. "Index count is the gap."** 14x more indexes costs **-1.6%**. Production
has 7,056 index *definitions* but only ~158 hold a key; definitions are free.

**9. "Production documents are ~45x the synthetic size."** Derived by inverting
the cost model against the answer. Measured: **641.9 vs 615 term passes per
key — 1.03x.** Wrong by a factor of ~43.

**10. "Docs per index is superlinear."** 0.99x over a 100x range at fixed
vocabulary. It was a proxy for vocabulary.

**11. "Vector dimension / HNSW parameters."** dim 768 vs 384 costs 0.9%,
inside the floor. `m` and `ef_construction` are identical in both.

**12. "The full HNSW re-encode path explains it."** 85.5% of production keys
take it, against 6% in the bench: **0.994x**.

**13. "It is memory pressure from the 1.7 GB `matching` materialisation."**
Tested on Linux under cgroup v2 + zram, with the guard proven to bind
(`memory.peak` pinned on each cap, 5.28 GB swapped, 26,326 throttle events).
Result: a **uniform 1.24x shift with no tail** — production's tail
concentration is 3.66x, the capped bench's 0.99x. The falsification criterion
was stated before the run and it failed cleanly.

**The pattern across all thirteen:** every wrong explanation had data
*consistent* with it. None had data that *discriminated* against the
alternatives. Consistency is free; only discrimination is worth anything.

## 8. What to do about it

**1. Deploy a post-#879 binary and re-measure. Zero engineering.** The
production binary's mtime is `2026-09-08 22:39`; #879 was committed
`2026-09-08 23:12:35` — it predates `.tpost` by 33 minutes and contains none of
that code. It gets neither #879 (49.5% on the bench) nor #885 (29.0% on the
no-`.tpost` path, which is the path the first boot after a deploy takes).
**Per this document's own thesis, do not assume those bench percentages
transfer — measure the boot afterwards.**

**2. Stop. Do not optimise anything in the falsified list.** Eight of the ten
terms are ties, including every axis this document's first version recommended.

**3. `PayloadIndex` has no durable form.** `src/vector/store.rs:1905` and
`:2037` construct it fresh; nothing under `src/vector/persistence/` writes it,
and the keymap (`manifest.rs:205-217`) persists only
`key_hash / global_id / vec_checksum / key`. Filter state is rebuilt from the
live keyspace on **every** boot, forever. No text-index format eliminates the
keyspace walk while that holds.

**4. The residual was the platform, and it is now measured: 5.80x (§4b).**
It is the largest single term in the decomposition — bigger than every code
change in this document combined — and no optimisation in this repository
touches it. If restart time matters, the host matters more than the code.
On Linux the same 303,778-key corpus reconciles in **240 seconds**.

**5. Fix the harness before the next measurement.** Its corpus must carry
production's vocabulary cardinality and term length, or the next model will be
as confidently wrong as this one's first version.

## 9. How to reproduce

The harness is `run2.sh` + `load.py` + `reconcile_time.py`, described in the raw
data's [README](bench-data/2026-09-09-reconcile-linux/README.md). Time the
interval between the `recovery scanned db` and `auto-reindexed` log lines of the
second boot — **not** the reconcile progress lines, which tick every 10 s and
emit nothing at all for a run shorter than one tick.

Gate any run on binary freshness against `target/release/moon.d`, the build's
own declared inputs. A stale binary from an earlier session produces
real-looking numbers for the wrong commit, and a guard comparing the binary to
the build log instead rejects every correct build — cargo appends `Finished`
about 64 ms after linking.

Check host load before every leg. Two separate findings in this investigation
were contaminated by builds the investigation itself had started.
