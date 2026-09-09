# The startup reconcile cost model — what was measured, and what was disproved

Written after a day spent diagnosing why a production instance took **1 hour
50 minutes** to become available after a restart. Its purpose is largely
**negative knowledge**: six explanations were proposed and five were wrong, and
one of the wrong ones was published to the user as "unambiguous". All of them
are recorded here so nobody spends another day re-deriving them.

Every number below comes from a Linux host — GCE `t2a-standard-8` (aarch64,
8 vCPU, 31 GB RAM), dedicated, nothing else running, moon `0bfc730d` built with
the shipping `release` profile and `RUSTFLAGS="-C target-cpu=native"`. Raw data:
[`bench-data/2026-09-09-reconcile-linux/`](bench-data/2026-09-09-reconcile-linux/).

**No macOS number appears in this document, and none should be added.** Every
figure this investigation produced on a developer laptop was either wrong or
misleading, including one that was off by 2.5x in the direction that would have
deprioritised the real fix. See §5.

---

## 1. What the reconcile is

After the keyspace is restored, `recover_indexes_task`
(`src/shard/event_loop.rs`) restores index definitions from the sidecars and
then walks every key matching an index prefix, re-deriving its postings and
vectors. Until it finishes the shard answers `-LOADING` to every data command,
so from a client's point of view the server is down for the whole walk.

## 2. The finding: it is text analysis, run twice

Plane isolation, 500 indexes x 70 docs = 35,000 keys, 300 words/doc, `.tpost`
deleted between boots, 3 interleaved repetitions (CV 0.03%):

```text
both planes           0.4767  0.4766  0.4769   ms/key
--no-vector (text)    0.2342  0.2278  0.2280
--no-text  (vector)   0.0190  0.0165  0.0167
```

The legs do not sum, and the residual is the whole point:

| component | ms/key | share |
|---|---|---|
| text plane (`index_document` -> analyzer -> stemmer) | 0.2280 | 47.8% |
| vector plane, **including a full HNSW re-encode** | 0.0167 | **3.5%** |
| second stemming pass in `index_payload_field` | 0.2320 | **48.7%** |

The residual appears only when a vector index and text fields coexist:
`--no-vector` removes the payload index entirely, `--no-text` leaves it nothing
long to stem. That is `PayloadIndex::insert_text`
(`src/vector/filter/text_index.rs`) re-tokenising and re-stemming content the
text plane has already stemmed — and constructing a `Stemmer` per call. Tracked
as **moon#885**.

By profile (`/usr/bin/sample`, shard-0, 13,919 samples), ~55% of on-CPU self
time is normalize + segment + stem: `rust_stemmers` 34.1%,
`unicode_normalization` 10.2%, `unicode_segmentation` 4.7%.

## 3. The cost model

Cost is linear in **field bytes**, over a 16x size range, fitting within 1.5%:

```text
ms/key = 0.1051 + 1.227 us/word          (~0.614 us/word per analysis pass)

   300 w   measured 0.4733   predicted 0.4733
  1200 w   measured 1.6026   predicted 1.5780   (-1.5%)
  4800 w   measured 5.9966   predicted 5.9966
```

It is **flat in corpus size** at fixed density: 35k / 70k / 140k keys cost
0.220 / 0.219 / 0.225 ms/key. It **is** superlinear on docs-per-index with a
high-cardinality vocabulary: 3.5k / 7k / 14k docs per index cost
0.321 / 0.618 / 1.484 ms/key, roughly n^2 (measured on macOS; the axis is real,
the constants are not authoritative).

## 4. What `.tpost` buys (moon#879)

Persisted postings, on vs off, 3 interleaved repetitions:

```text
off  median 0.4709 ms/key (CV 0.52%)
on   median 0.2376 ms/key (CV 0.27%)
ratio 0.5046  ->  49.5% faster        noise floor 0.52%  ->  SIGNAL
```

`.tpost` and #885 are independent halvings and they compose.

## 5. Disproved — do not re-derive these

Six explanations were proposed for the slow boot. Five were wrong.

**1. "The `cooperative_yield` between rescan slices is expensive."** Refuted by
`/usr/bin/sample`: 8,459 of 8,459 shard-thread samples were on-CPU inside
moon's own reconcile stack. The thread never parks. A yield it does not wait on
cannot be the cost.

**2. "moon#873's 10 ms slice budget regressed it."** Refuted by A/B. The
measurements behind the claim were taken while a 533%-CPU `rustc` — spawned by
the investigation itself — saturated the box. The pre-#873 binary showed the
same rate.

**3. "The remaining time is 12 hours."** The server's own
`ETA for this db` field is computed from the RECENT rate and read 121,067 s
while the cumulative rate on the same log line implied ~48 minutes. On a noisy
series the recent-rate ETA is the misleading one. Tracked as **moon#882**.

**4. "It is macOS memory-compressor pressure."** Refuted by the full 199-interval
series, which runs the OPPOSITE way:

```text
corr(keys reconciled, free MB)         = -0.190
corr(keys reconciled, decompressions)  = +0.440
10 FASTEST intervals: median  60 MB free,  67,055 decompressions/s
10 SLOWEST intervals: median 728 MB free,   7,099 decompressions/s
```

Decompression rate is an **output** of the process allocating hard, not an input
that slows it. This was published as "unambiguous" on the strength of four
hand-picked rows before the full series was computed.

**5. "The rate is bimodal — two regimes 150x apart."** There is no regime
switch. A single run's per-tick series reads
`2899 1923 1235 438 901 598 467 736 393 1020 661 458 360 96 184 180 308 375 273 206`
keys/s — one noisy ramp with ~10x neighbour-to-neighbour scatter. Choosing two
window boundaries in such a series manufactures "two regimes" with none present;
the split that produced the claim compared a 611-second window against a
172-second one holding 144 keys.

**6. "`add_term_occurrence` hashes every term twice, and that is 41.8% of
self-time."** The 41.8% is the whole function; the duplicate `contains_key`
probes the bucket the following `entry` probes anyway, so it runs warm.
Measured as a tie: +0.6% against a 2.0% noise floor over 6 interleaved
repetitions. Tracked in **moon#884**, retitled to the three-allocations-per-term
question, which remains untested.

**The one that survived** came from a controlled reproduction with
plane-isolating flags and one axis varied at a time. Every wrong explanation had
data *consistent* with it; none had data that *discriminated* against the
alternatives.

## 6. What this means for anyone optimising startup

- **Measure the text plane first.** It and its duplicate are 96.5% of the cost.
- **Do not go near the vector engine for this.** 3.5%, with no persisted keymap,
  so every key took the full HNSW re-encode path.
- **Isolate planes with a load-generator flag** before profiling. A single
  end-to-end number cannot attribute anything.
- **Live-instance timings on a shared box discriminate nothing.** Interleave
  legs, report a noise floor, and treat a ratio inside it as a tie.

## 7. How to reproduce

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
