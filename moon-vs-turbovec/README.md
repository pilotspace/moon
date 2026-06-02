# Moon vs TurboVec — A Reproducible Retrieval Benchmark

> **A decision guide, not a leaderboard.** When should you reach for an embedded
> flat-scan quantizer (TurboVec), and when have you outgrown it and need a
> converged engine (Moon)? This repo runs both, on identical hardware and data,
> and lets the numbers draw the boundary.

[![reproducible](https://img.shields.io/badge/results-reproducible-brightgreen)](./METHODOLOGY.md)
[![fairness](https://img.shields.io/badge/fairness-iso--recall%20%2B%20iso--memory-blue)](#fairness-statement)

---

## The honest one-liner

**They are not the same category.** TurboVec is a best-in-class *embedded
flat-scan TurboQuant library* — 2/4-bit compression, inner-product, in-RAM,
Python-native, with a hand-tuned SIMD kernel that beats FAISS FastScan. Moon is
a *converged, networked, durable engine* (KV + vector + graph + search) where
the same TurboQuant family is one quantizer feeding **HNSW / DiskANN / IVF**
indexes.

So this benchmark does **not** ask "who is faster." It asks: **where is the
boundary** past which a flat-scan library stops being the right tool?

| Choose **TurboVec** when… | Choose **Moon** when… |
|---|---|
| Corpus fits comfortably in RAM | Corpus exceeds RAM (disk-resident serving) |
| Single-node, embedded in your Python process | You need a networked, multi-tenant service |
| Pure unfiltered top-k on a static set | You need filtered / hybrid / multi-modal retrieval |
| You manage persistence & rebuilds yourself | You need durability, replication, online ingest |
| d ≥ 1536, you want maximum compression | You also need KV / graph / search in one engine |
| You want the fastest single-node scan kernel | You've outgrown "one flat index in a process" |

The experiments below quantify every row of that table — including **the rows
where TurboVec wins.**

---

## Fairness statement

This benchmark is designed to be *attackable* — and to survive the attack.
Credibility is the entire point; a rigged comparison is worthless for the
decision it's meant to inform.

1. **Iso-recall + iso-memory.** Every latency/QPS number is reported *with* the
   recall **and** the resident memory at that exact operating point, or as a
   full Pareto frontier. A speed win at 5× the RAM or lower recall is reported
   as exactly that.
2. **Same metric, same ground truth.** Both systems run cosine / inner-product
   on L2-normalized vectors. Ground-truth neighbors are computed by **exact
   brute force on the original float32 vectors**, identical query set for both.
3. **Both tuned, or both default — disclosed.** We do not grid-search Moon's
   `ef`/`M` while handing TurboVec stock parameters. Every parameter for both
   systems is in [`config/experiments.yaml`](./config/experiments.yaml).
4. **Moon's padding is disclosed everywhere.** Moon's FWHT rotation pads to the
   next power of two (1536→2048, 3072→4096 ⇒ **+33% vectors** before index
   overhead). This is included in *every* Moon memory figure.
5. **Real embeddings at scale.** No synthetic Gaussian vectors (their recall is
   misleading). We use standard ANN datasets + real embedding sets at
   TurboVec's native dimensions.
6. **Pinned & cloud-run.** Numbers are produced on a pinned cloud instance (not
   a laptop/dev VM), with both repos pinned to commit hashes recorded in
   [`results/run_metadata.json`](./results/). See [METHODOLOGY.md](./METHODOLOGY.md).

> **We publish the experiments Moon loses.** A result table that is honest about
> where the flat-scan library is the better choice is the only kind worth
> trusting.

---

## Experiments

| ID | Question | Battleground | Favors |
|----|----------|--------------|--------|
| **E1** | Recall@10 vs QPS (Pareto) at N = 100K / 1M / 10M | Core retrieval | neutral |
| **E2** | **Operating envelope** — where does each tool stop being viable? (RAM ceiling, filter, durability, ingest) | Capability boundary | Moon (capability) |
| **E3** | Memory footprint vs N (compression) | Storage | **TurboVec** |
| **E4** | Filtered search: allowlist vs payload/expression filters, selectivity sweep | Capability | Moon |
| **E5** | Concurrent online ingest + query throughput | Mixed load | Moon |
| **E6** | Disk-scale (>RAM): DiskANN at 100M where flat-scan OOMs | Capability | Moon |
| **E7** | Convergence / ops capability matrix (qualitative) | Platform | Moon |

E2 is deliberately **not** "indexed beats brute-force" (a triviality). It holds
recall and hardware fixed and reports the corpus size / RAM / feature point at
which each tool stops being usable.

### Results (summary)

> ⚠️ **All values below are `<PENDING-RUN>` until produced by the pinned-hardware
> run.** Do not quote any number from this README in marketing until it is
> populated from `results/` by an actual run. Placeholders are intentionally
> non-numeric so they cannot leak as fake data.

**E1 — Recall@10 / QPS / memory at iso operating points**

| Dataset (dim) | N | System | Config | Recall@10 | QPS | RAM | Notes |
|---|---|---|---|---|---|---|---|
| GloVe (200) | 1M | TurboVec | 4-bit FastScan | `<PENDING-RUN>` | `<PENDING-RUN>` | `<PENDING-RUN>` | TurboVec's strong/likely-win region |
| GloVe (200) | 1M | Moon | TQ8 HNSW | `<PENDING-RUN>` | `<PENDING-RUN>` | `<PENDING-RUN>` | low-d: Moon uses TQ8/FP32 (TQ4 weak ≤384d) |
| OpenAI (1536) | 1M | TurboVec | 4-bit FastScan | `<PENDING-RUN>` | `<PENDING-RUN>` | `<PENDING-RUN>` | |
| OpenAI (1536) | 1M | Moon | TQ4 HNSW | `<PENDING-RUN>` | `<PENDING-RUN>` | `<PENDING-RUN>` | +33% padding (1536→2048) included in RAM |
| OpenAI (1536) | 10M | TurboVec | 4-bit FastScan | `<PENDING-RUN>` | `<PENDING-RUN>` | `<PENDING-RUN>` | full RAM scan |
| OpenAI (1536) | 10M | Moon | TQ4 HNSW / DiskANN | `<PENDING-RUN>` | `<PENDING-RUN>` | `<PENDING-RUN>` | |

**E3 — Memory vs N (TurboVec's home turf — co-reported, not buried)**

| System | 1M @1536 | 10M @1536 | bits | native dim? |
|---|---|---|---|---|
| TurboVec | `<PENDING-RUN>` | `<PENDING-RUN>` | 2/4 | yes |
| Moon | `<PENDING-RUN>` | `<PENDING-RUN>` | 1–4 | no (pads to 2048) |

(Full tables per experiment in [`results/`](./results/) after a run.)

---

## Where TurboVec wins (and you should use it)

*Populated from the actual run — this section is the credibility anchor and is
expected to contain real losses for Moon.*

- **Small N, in-RAM:** at N ≤ ~100K the flat-scan FastScan kernel is faster and
  far simpler than building/serving an index. `<PENDING-RUN>`
- **Low dimension (GloVe-200):** Moon's TQ4 loses recall ≤384d; Moon must fall
  back to TQ8/FP32, narrowing or erasing any edge. `<PENDING-RUN>`
- **Compression:** native-dimension 2-bit packing vs Moon's padded codes.
  `<PENDING-RUN>`
- **Single-node kernel throughput:** TurboVec's AVX-512BW/NEON FastScan beats
  FAISS; Moon's flat scan is currently scalar/AVX2. `<PENDING-RUN>`
- **Embedding ergonomics:** `pip install`, drop-in LangChain/LlamaIndex/
  Haystack/Agno stores, zero ops.

## Where Moon wins (the envelope)

- **Past the RAM ceiling** (E2/E6): DiskANN serves corpora that OOM a flat-scan
  library. `<PENDING-RUN>`
- **Filtered & hybrid retrieval** (E4): payload/expression/text filters + RRF
  fusion vs a flat allowlist. `<PENDING-RUN>`
- **Online ingest under query load** (E5): MVCC segments vs rebuild-on-add.
  `<PENDING-RUN>`
- **Convergence** (E7): KV + vector + graph + search + CDC behind one Redis
  wire — no Redis+Qdrant+Neo4j+ES glue.
- **Durability & replication:** crash-consistent persistence, master/replica,
  cluster failover — structurally absent from a library.

---

## Reproduce it yourself

```bash
make setup           # python deps + build Moon (release) + install turbovec (pinned)
make datasets        # download SIFT1M, GloVe-200, OpenAI-1536/3072, compute exact ground truth
make bench           # run all experiments on this host
make plot report     # render Pareto plots + regenerate result tables in README
```

Everything — datasets, parameters, hardware capture, raw CSVs, plot scripts — is
in this repo. If you can't reproduce a number, it's a bug; open an issue.

See **[METHODOLOGY.md](./METHODOLOGY.md)** for the full protocol and
**[ATTRIBUTION.md](./ATTRIBUTION.md)** for credits and licensing.

---

## Attribution

[TurboVec](https://github.com/RyanCodrai/turbovec) by Ryan Codrai (MIT) implements
Google Research's TurboQuant. This benchmark uses it unmodified at a pinned
commit. Moon's vector engine implements an independent TurboQuant variant plus
HNSW/DiskANN/IVF. This comparison is built to be fair to both; corrections via
PR/issue are welcome.
