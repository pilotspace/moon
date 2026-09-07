# Memory Baseline Fixture

**File:** `memory-baseline.json`

## Capture Details

**Current file: a placeholder, not a valid baseline.** It is the original
macOS aarch64 / debug snapshot (captured 2026-04-27, no `platform` field at
all), restored deliberately -- see "Status" below.

- **Date:** 2026-04-27 (original capture; see Status for why this file is
  back to this content)
- **Host:** macOS aarch64 (Apple Silicon)
- **Features:** `runtime-tokio,jemalloc,graph,text-index`
- **Build:** debug profile (unoptimized)
- **Shards:** 1

### Status: awaiting a real ubuntu-latest capture (moon#764 follow-up)

A GCE `moon-bench-x86` (`c3-standard-8`, Ubuntu 24.04.4 LTS, x86_64) capture
briefly replaced this file. It was **reverted** after the committed version
ran on the actual `ci.yml` `memory-steady-state` job
(`runs-on: ubuntu-latest`) and produced:

```
=== MEMORY REGRESSION DETECTED ===
  FAIL: rss delta=30.51% (measured=146817024, baseline=112492544)
  FAIL: allocator_overhead delta=243.22% (measured=48943405, baseline=14260002, threshold=+/-25%)
=== 2 kind(s) exceeded +/-5% threshold ===
```

`dashtable` (unaffected by machine class) came in at -0.14% on that same
run -- proof the comparison logic itself is fine; the baseline's *origin*
was not. A GCE `c3-standard-8` and GitHub's hosted `ubuntu-latest` are both
`Linux`/`x86_64`, so `check_baseline_provenance`'s (then) os/arch-only check
waved the mismatch through. Committing that number as "the" baseline would
have meant every future PR compared against a machine nobody's code runs on
-- exactly the class of problem moon#764 was filed to close, recurring one
layer down. **A baseline is only valid if it was captured by this workflow,
on this runner label** -- see "Regeneration" below for how, and "Platform
provenance" for what now stops a repeat of this from landing silently. The
GCE numbers, and the incident above, are kept as the *method* record in
`tmp/perf-campaign/FIX-764.md` -- not as the committed baseline.

Until a genuine `ubuntu-latest` capture replaces this file,
`check_baseline_provenance` refuses to compare (exit 2, "no platform
provenance") -- the honest state: no valid baseline exists yet, not a new
breakage.

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

**On the runner the gate actually runs on** (`ci.yml`'s `memory-steady-state`
job, `runs-on: ubuntu-latest`) -- dispatch the workflow with the
`capture_memory_baseline` input:

```bash
gh workflow run ci.yml --ref <branch> -f capture_memory_baseline=true
```

That run skips the normal self-test/compare step, runs `--write-baseline
tests/fixtures/memory-baseline.json --skip-build` instead, and (per "Always
uploaded" below) uploads the result as the `memory-steady-state-snapshot`
artifact. **Nothing is committed automatically** -- download the artifact
and commit its contents as `tests/fixtures/memory-baseline.json` yourself.

Locally (macOS or any dev box) for iterating on the *script*, not for
producing the committed baseline:

```bash
bash scripts/bench-memory-steady-state.sh --write-baseline tests/fixtures/memory-baseline.json
```

A baseline captured this way, or on any machine that is not the actual CI
runner, will (correctly) make the gate refuse to compare -- see "Status"
above for what happens when that rule is skipped.

**Commit convention:** The commit subject MUST include `[memory-baseline-update]` followed by the reason:

```
chore(190-04): [memory-baseline-update] reason: added per-entry metadata field
```

## Always uploaded: the measured snapshot (moon#764 follow-up)

Every invocation of `bench-memory-steady-state.sh` -- self-test, plain
compare, or `--write-baseline` -- writes what it captured to
`/tmp/moon-memory-snapshot.json` unconditionally, and the `ci.yml` job
uploads that path as the `memory-steady-state-snapshot` artifact on every
run (`if: always()`), pass or fail. Before this, a reviewer's only way to
see what a run measured was to read failure text out of the job log; the
JSON is now always one click away.

## Platform provenance (required)

A memory baseline is only comparable to a snapshot captured on a **matching
machine** -- not merely the same OS and architecture. RSS reporting,
allocator behaviour, core count and available RAM all differ across
platforms *and across machines that happen to share an OS/arch*, so a
cross-machine delta measures the runner, not the code (see "Status" above
for exactly this happening: os/arch matched, the machine didn't, and the
gate reported a 243% "regression" that was pure noise).

Every snapshot therefore records:

```json
"platform": {
  "os": "Linux", "arch": "x86_64", "profile": "debug",
  "cpu_model": "<from /proc/cpuinfo or sysctl>",
  "cpu_count": 4,
  "mem_total_kb": 16000000,
  "runner_environment": "github-hosted",
  "runner_name": "<GitHub Actions RUNNER_NAME, or unknown outside CI>"
}
```

`check_baseline_provenance` in the script enforces this in two tiers, and the
split is a deliberate design decision, not an oversight:

1. **os / arch / cpu_count -- hard gate, exit 2.** Missing, or a mismatch,
   means the gate refuses to compare outright. `cpu_count` is included at
   this tier (not just logged) because it is the field that would have
   caught the GCE-vs-`ubuntu-latest` incident above (8 vCPU vs GitHub's
   documented, fixed vCPU count for the hosted runner label) -- and because
   GitHub *documents and holds constant* the vCPU count per runner label, so
   gating on it does not risk the gate flapping red across legitimate
   `ubuntu-latest` reruns.
2. **cpu_model -- soft, a loud `WARN` log line only, never exit 2.** GitHub
   does **not** document or guarantee the exact CPU SKU behind a hosted
   runner label the way it documents vCPU count; the hosted fleet can
   legitimately rotate silicon under an unchanged label. Hard-failing on
   `cpu_model` would risk trading today's problem (a wrong-machine baseline
   silently accepted) for a new one (a correct baseline silently rejected
   forever because the fleet changed CPU generation) -- a different route to
   "nobody trusts this gate."

`runner_environment`/`runner_name` are recorded and logged on every
comparison (so a human reading a failure sees at a glance which runner
produced which number) but are **not** gate criteria: `RUNNER_NAME` is a
fresh random string on every single hosted run by design, so comparing it
would make the gate permanently exit 2 the moment it started passing at
all.

**Answering "would a future baseline captured on a differently-specced
hosted runner be caught or silently accepted?"**: caught, if the difference
shows up in `cpu_count` (a GitHub-documented, stable-per-label property) --
silently accepted with a loud warning, not a hard failure, if the only
difference is `cpu_model` on an otherwise-identical `cpu_count`/os/arch. The
residual risk this leaves is explicit, not hidden: a hosted-runner CPU
generation bump on the same vCPU count could still shift RSS by some amount
this gate won't hard-block on. `mem_total_kb` is recorded but not currently
gated or warned on either, for the same reason as `cpu_model`.

A baseline that predates this check (has `platform.os`/`.arch` but no
`cpu_count`) is treated the same as one with no `platform` block at all --
exit 2, "regenerate on the runner this gate actually runs on" -- rather than
silently trusting a weaker, os/arch-only match.

## Per-kind noise floor (moon#764 follow-up)

`hnsw` and `allocator_overhead` are not measured directly. `hnsw`'s
Prometheus value tracks a growable mutable buffer whose realized jemalloc
size class depends on concurrent insertion ordering -- the MEMORY DOCTOR
estimate for the same kind is bit-identical run over run; only the real
allocation isn't. `allocator_overhead` is `max(0, RSS - sum(other 6))`, a
residual that inherits every other kind's noise plus RSS's own page-level
jitter.

Measured on GCE `moon-bench-x86` (`c3-standard-8`, see "Status" above and
`tmp/perf-campaign/FIX-764.md` for the full method record -- this noise
finding stands independent of that host's numbers no longer being the
committed baseline), 10+ back-to-back real runs of this exact workload with
**zero code changes between runs**:
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
