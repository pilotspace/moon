# scripts/

Reusable benchmark, test, and inspection tools. Throwaway debugging scripts
should NOT live here — keep one-off iteration scripts in `/tmp` or your
worktree's `.gitignore`.

All scripts assume Linux (run via `orb run -m moon-dev` from macOS hosts).

## Disk-offload / tiered storage (added in feat/disk-offload)

| Script | Purpose |
|---|---|
| `bench-cold-tier.sh` | DiskANN cold-tier benchmark — measures insert/query throughput when vectors live on disk via the io_uring path. Canonical 3-tier disk-offload bench. |
| `bench-warm-tier.py` | Warm-tier benchmark with real MiniLM-L6-v2 (384d) embeddings. Lifecycle-driven: insert → warm transition → query against mmap'd warm segments. |
| `test-cross-tier-pressure.py` | Cross-tier memory pressure test. Fills HOT, drives the disk-offload cascade, validates that KV + vector data flow correctly across HOT → WARM → COLD. |
| `test-recovery-all-cases.sh` | Comprehensive crash-recovery matrix across persistence configurations (snapshot only, AOF only, AOF + WAL v3, disk-offload). |

## MoonStore v2 benchmark suite (added in feat/disk-offload)

Orchestrator + 6 component scripts. Run `bench-moonstore-v2.sh` to drive the
full pipeline; the components can also be invoked individually.

| Script | Phase |
|---|---|
| `bench-moonstore-v2.sh` | Orchestrator — runs the full pipeline end-to-end |
| `bench-moonstore-v2-generate.py` | Synthetic dataset generation (KV + vectors) |
| `bench-moonstore-v2-kv.py` | KV throughput / latency phase |
| `bench-moonstore-v2-vector.py` | Vector ingest + search phase |
| `bench-moonstore-v2-warm.py` | Warm-tier transition + warm-search phase |
| `bench-moonstore-v2-recovery.py` | Crash + recovery phase |
| `bench-moonstore-v2-report.py` | Aggregates phase outputs into a single report |

## Vector search benchmarks (added in feat/disk-offload)

| Script | Purpose |
|---|---|
| `bench-vector-realworld.py` | Realistic mixed insert + search workload, Moon vs Qdrant. The general-purpose vector head-to-head. |
| `bench-minilm-recall.py` | MiniLM-384d Recall@10 vs throughput, Moon vs Qdrant. The only script that measures recall against brute-force ground truth — keep when evaluating any quantization or HNSW change. |

## Inspection / debugging (added in feat/disk-offload)

| Script | Purpose |
|---|---|
| `moonstore-inspect.py` | MoonStore v2 file decoder. Walks a tier directory and pretty-prints manifest, control file, KV heap files, vector segments, WAL v3. Use first when investigating any disk-offload issue. |

## Cloud benchmarking (added in feat/disk-offload)

| Script | Purpose |
|---|---|
| `gcloud-benchmark.sh` | GCloud `e2-highmem-4` benchmark runner — Moon vs Redis vs Qdrant on a controlled instance. |
| `run-gcloud-bench.sh` | Driver script that provisions, runs `gcloud-benchmark.sh`, collects results, tears down. |

## KV benchmarks (pre-existing, referenced by CI/docs)

| Script | Purpose |
|---|---|
| `bench-compare.sh` | Single-shard Moon vs Redis throughput comparison |
| `bench-production.sh` | Production-like benchmark with realistic pipeline depth |
| `bench-resources.sh` | CPU / memory profile during a long run |
| `bench-scaling.sh` | Multi-shard scaling curves |

## Vector benchmarks (pre-existing canonicals)

Two orthogonal entry points. `bench-server-mode.sh` is the canonical
head-to-head driver; it orchestrates `bench-vs-competitors.py` (the engine)
across Moon + Redis 8.x + Qdrant and emits `BENCHMARK-REPORT.md`.
`bench-vector-production.sh` is the Criterion-level micro-benchmark suite
(distance kernels, HNSW build/search, FWHT, recall, memory audit, e2e).

| Script | Purpose |
|---|---|
| `bench-server-mode.sh` | 3-way server-mode head-to-head (Moon vs Redis vs Qdrant); calls the engine below |
| `bench-vs-competitors.py` | Shared engine: `--generate-only / --bench-{moon,redis,qdrant} / --report` |
| `bench-vector-production.sh` | Criterion micro-benchmarks — subcommands: `distance hnsw fwht recall memory e2e` |
| `bench-mixed-workload.py` | Mixed insert + search simulation across 5 phases |

## Profiling (pre-existing)

| Script | Purpose |
|---|---|
| `profile.sh` | CPU & memory profiling suite — generates `PROFILING-REPORT.md` |
| `profile-vector.sh` | flamegraph / samply wrapper for HNSW search hot path |

## Test suites (referenced by CI)

| Script | Purpose |
|---|---|
| `test-commands.sh` | Command-coverage smoke test |
| `test-consistency.sh` | Redis-vs-Moon consistency suite (ground truth) |

## Git / workflow helpers

| Script | Purpose |
|---|---|
| `push.sh` | Dual-remote push: `moon` (code) + `moon-docs` (.planning/ via subtree) |

## Conventions for new scripts

1. **One purpose per script.** If you find yourself writing `-v2`, `-final`,
   `-debug`, or `-simple` suffixes, you're making throwaways — keep them in
   `/tmp` or delete after the bench session.
2. **Name describes what, not when.** `bench-cold-tier.sh` is good;
   `bench-final-3tier.sh` is not.
3. **Top-of-file docblock** explaining what the script measures and what
   the canonical exit codes mean.
4. **Linux-only assumption is fine** — wrap with `orb run -m moon-dev` from
   macOS.
5. **Don't commit shell scripts that just `cargo build`** — call into the
   `cargo bench` infrastructure instead.
