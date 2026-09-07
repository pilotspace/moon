# Memory Baseline Fixture

**File:** `memory-baseline.json`

## Capture Details

- **Date:** 2026-09-08
- **Host:** GCE `moon-bench-x86` (`c3-standard-8`), Ubuntu 24.04.4 LTS,
  kernel `6.17.0-1022-gcp`, `x86_64` -- matches `runs-on: ubuntu-latest` in
  `ci.yml` (Linux/x86_64; exact kernel build need not match, only OS/arch,
  per `check_baseline_provenance`). Idle host, load average 0.00 at capture.
- **Toolchain:** `rustc 1.94.1` (`dtolnay/rust-toolchain@1.94.1`, same pin as CI)
- **Build:** `cargo build --no-default-features --features runtime-tokio,jemalloc,graph,text-index`
  -- the exact `Build moon` step in `ci.yml`'s `memory-steady-state` job,
  debug profile (unoptimized)
- **Env:** `MOON_NO_URING=1` (matches the job's env block)
- **Shards:** 1
- **Superseded a macOS aarch64 / debug baseline** (captured 2026-04-27, no
  `platform` field) that predated moon#764's provenance guard and could
  never legitimately compare against the `ubuntu-latest` runner.

## Workload

| Category | Count | Details |
|----------|-------|---------|
| String keys | 1,000,000 | via redis-benchmark (random keys) |
| Vector docs | 10,000 | 16-dim FLOAT32, HNSW index |
| Graph nodes | 100 | GRAPH.ADDNODE with :N label |
| Steady-state wait | 60s | After all data loaded |

## 7 Subsystem Kinds

1. `dashtable` - DashTable structural overhead + entry data
2. `hnsw` - Mutable vector segments (brute-force buffer)
3. `csr` - Graph CSR storage + MemGraph SlotMap
4. `wal` - WAL writer buffers (0 when --appendonly no)
5. `sealed` - Immutable vector segments (0 without FT.COMPACT)
6. `replication_backlog` - Replication backlog ring buffer (0 when no replicas)
7. `allocator_overhead` - max(0, RSS - sum(other 6))

## Regeneration

To regenerate the baseline after an intentional memory change:

```bash
bash scripts/bench-memory-steady-state.sh --write-baseline tests/fixtures/memory-baseline.json
```

**Commit convention:** The commit subject MUST include `[memory-baseline-update]` followed by the reason:

```
chore(190-04): [memory-baseline-update] reason: added per-entry metadata field
```

## Platform provenance (required)

A memory baseline is only comparable to a snapshot captured on the **same OS and
architecture**. RSS reporting, allocator behaviour and struct padding all differ
across platforms, so a cross-platform delta measures the runner, not the code.

Every snapshot therefore records:

```json
"platform": { "os": "Linux", "arch": "x86_64", "profile": "debug" }
```

`check_baseline_provenance` in the script **refuses to compare** (exit 2) when the
baseline carries no provenance, or when its platform differs from the machine
measuring now. Exit 2 means "the gate cannot run", which is distinct from exit 1,
"a kind regressed".

> This fixture now carries `"platform": {"os": "Linux", "arch": "x86_64", ...}`,
> captured on `moon-bench-x86` (see Capture Details above) to match the
> `ubuntu-latest` runner `ci.yml` actually uses.

## Per-kind noise floor (moon#764 follow-up)

`hnsw` and `allocator_overhead` are not measured directly. `hnsw`'s
Prometheus value tracks a growable mutable buffer whose realized jemalloc
size class depends on concurrent insertion ordering -- the MEMORY DOCTOR
estimate for the same kind is bit-identical run over run; only the real
allocation isn't. `allocator_overhead` is `max(0, RSS - sum(other 6))`, a
residual that inherits every other kind's noise plus RSS's own page-level
jitter.

Measured on `moon-bench-x86` (see Capture Details), 10+ back-to-back real
runs of this exact workload with **zero code changes between runs**:
`dashtable`/`rss`/`csr` held under 2% every time; `hnsw` swung
-13.55%..+15.68% and `allocator_overhead` swung -18.98%..+9.15% -- purely
from run-to-run noise. A flat +/-5% would fail this gate on these two kinds
roughly every other real run, which trains reviewers to re-run without
reading the failure -- a different route to the same "nobody trusts this
gate" outcome moon#764 was filed over.

`compare_snapshot`'s `kind_threshold()` therefore applies a wider floor to
just these two kinds -- `hnsw`: +/-20%, `allocator_overhead`: +/-25% --
comfortably above the measured noise ceiling (~16% / ~19%) so a real
regression several times the noise floor is still caught. `dashtable`,
`csr`, `rss`, `wal`, `sealed`, and `replication_backlog` are unaffected and
stay at the requested `--threshold` (5% by default). Verified: a real
mutated-baseline run with `hnsw` regressed 100% (well past the 20% floor)
still failed the gate (`FAIL: hnsw delta=72.89%`); the self-test's +6%
`dashtable` injection is still caught every time. Full transcripts:
`tmp/perf-campaign/FIX-764.md`.

## CI Gate

The `memory-steady-state` job in `.github/workflows/ci.yml` compares every PR's
memory profile against this baseline with a +/-5% per-kind tolerance. If any kind
exceeds the threshold, the job fails with the offending kind and delta percentage.

### History: the gate was vacuous (moon#764)

CI invoked the script as `--self-test --skip-build`, and `--self-test` used to
`exit 0` immediately after its injection check -- **before** the
committed-baseline comparison. The self-test compares the freshly captured
snapshot against *itself*, so it can only ever prove the comparison machinery
works; it can never detect a real regression. The result: the job passed for
months without once reading this file.

`--self-test` is now a *phase*. It runs the injection check, then falls through to
the real comparison.
