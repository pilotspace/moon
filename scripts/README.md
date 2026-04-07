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

## Pre-existing canonicals (not modified by feat/disk-offload)

| Script | Purpose |
|---|---|
| `bench-compare.sh` | Single-shard Moon vs Redis throughput comparison |
| `bench-production.sh` | Production-like benchmark with realistic pipeline depth |
| `bench-resources.sh` | CPU / memory profile during a long run |
| `bench-scaling.sh` | Multi-shard scaling curves |
| `bench-server-mode.sh` | Server bootstrap helper for the bench-* family |
| `bench-vector.sh` / `bench-vector-production.sh` / `bench-vector-vs-competitors.sh` | Vector benchmarks (pre-PR canonicals) |
| `bench-mixed-workload.py` / `bench-mixed-1k-compact.py` | Mixed-workload generators |
| `bench-vs-competitors.py` | KV head-to-head driver |
| `profile.sh` / `profile-vector.sh` | perf record + flamegraph helpers |
| `test-commands.sh` | Command-coverage smoke test |
| `test-consistency.sh` | Redis-vs-Moon consistency suite (ground truth) |
| `push.sh` | Helper for the GCloud workflow |

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
