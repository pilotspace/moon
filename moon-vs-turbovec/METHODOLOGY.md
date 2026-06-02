# Methodology

The result is only worth as much as the protocol. This document is the contract
every number in the repo must satisfy.

## 1. Hardware & environment (pinned)

- **Published numbers come from a pinned cloud instance**, never a laptop or a
  dev VM. (Per Moon's own rule: production benchmarks target Linux on
  GCloud/OrbStack-Linux; macOS/dev numbers are never published.)
- Default reference instance: `c4a-standard-8` (Axion, arm64) **and**
  `c3-standard-8` (x86_64) — TurboVec's kernel advantage is arch-specific
  (NEON vs AVX-512BW), so we report **both architectures**.
- `harness/hardware.py` captures CPU model, core count, RAM, kernel, compiler,
  BLAS backend, and both repos' commit hashes into `results/run_metadata.json`.
  No result is valid without this sidecar.

## 2. Versions (pinned commits)

| Component | Pin | Where recorded |
|---|---|---|
| Moon | git commit hash + build flags (`--release`, target-cpu) | `run_metadata.json` |
| TurboVec | git commit hash / PyPI version | `run_metadata.json` |
| Datasets | content hash | `datasets/manifest.json` |

## 3. Datasets — real embeddings only

| Dataset | Dim | Metric | Why |
|---|---|---|---|
| GloVe | 200 | cosine | TurboVec's strong/likely-win region; exposes Moon's ≤384d TQ4 weakness honestly |
| SIFT1M | 128 | L2 | classic ANN baseline (Moon uses FP32/TQ8 here) |
| OpenAI text-embedding-3 | 1536 | cosine | TurboVec's headline dim; real LLM embeddings |
| OpenAI text-embedding-3 | 3072 | cosine | high-d where TQ4 shines |
| Deep / BIGANN slices | 96–128 | L2 | scale tiers 1M → 10M → 100M for the envelope/disk experiments |

**No synthetic Gaussian vectors.** Random high-dimensional vectors exhibit
distance concentration that makes recall numbers misleading — especially at the
10M+ tier where it would be tempting to fabricate data. Scale tiers use real
ANN-benchmark sets.

## 4. Ground truth

- Exact top-100 neighbors per query via brute-force on the **original float32**
  vectors (`datasets/ground_truth.py`, GPU/BLAS accelerated, cached + hashed).
- Identical query set fed to both systems.
- `Recall@k = |returned_topk ∩ true_topk| / k`, averaged over the query set.

## 5. The iso-recall / iso-memory discipline (non-negotiable)

Every latency/QPS data point is emitted as a tuple:

```
(system, dataset, N, dim, params, recall@10, qps, p50_ms, p95_ms, p99_ms, rss_bytes)
```

- **Never** compare a high-recall config of one system against a low-memory
  config of the other and report only the favorable axis.
- Latency/QPS claims are made **at matched recall** (interpolated on each
  system's Pareto frontier) **or** the full frontier is shown.
- Memory is **resident set size** of the serving process at steady state, and
  for Moon **includes the power-of-2 FWHT padding** (e.g. 1536→2048 = +33%
  raw vectors before index/graph overhead). This is stated on every Moon row.

## 6. Parameter policy (both tuned or both default — disclosed)

- All tunables for both systems live in `config/experiments.yaml`.
- Two modes, both reported when they differ:
  - **`default`** — each project's documented defaults.
  - **`tuned`** — a *symmetric* sweep: if we sweep Moon's `ef_search`/`M`, we
    sweep TurboVec's `bit_width` (2/4) and any documented knob over a comparable
    grid. The grid is in the config; nothing is hand-picked post-hoc.

## 7. Measurement hygiene

- Warmup: discard first `warmup_queries` (default 1000) before timing.
- Repeats: median of `repeats` runs (default 5); report dispersion.
- Latency: per-query wall time → p50/p95/p99. QPS: single-thread and
  `n_threads`-thread (both reported; TurboVec MT vs Moon's per-shard model).
- Client overhead: Moon is measured over the loopback RESP wire (real serving
  path); TurboVec in-process. **This asymmetry favors TurboVec and is stated** —
  we do not subtract network time to flatter Moon.
- Isolation: one system per process; cgroup memory cap recorded.

## 8. What we deliberately do NOT do

- No subtracting Moon's network/serialization cost to fake parity with an
  in-process library.
- No reporting Moon-L2 vs TurboVec-IP (different objectives).
- No quoting a 10M number extrapolated from 1M — each tier is measured.
- No populating README tables by hand. `make report` regenerates them from
  `results/*.csv`; placeholders stay `<PENDING-RUN>` until a real run overwrites
  them.

## 9. Output artifacts

```
results/
  run_metadata.json     # HW, versions, commit hashes, dataset hashes
  raw/*.csv             # one row per (system, dataset, N, params) measurement
  plots/*.png           # Pareto recall-vs-QPS, memory-vs-N, envelope curves
```
