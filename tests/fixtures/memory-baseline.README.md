# Memory Baseline Fixture

**File:** `memory-baseline.json`

## Capture Details

**Current file:** a genuine hosted `ubuntu-latest` capture, taken by `ci.yml`
itself with the harness exactly as it stands in this branch, and chosen from
nine such samples because it leaves the most headroom on both sides of the
tolerance window -- see "Choosing which sample to commit".

- **Date:** 2026-09-08
- **Source:** `ci.yml` run
  [34169276001](https://github.com/pilotspace/moon/actions/runs/34169276001),
  `workflow_dispatch` with `capture_memory_baseline=true`, committed verbatim
  from the `memory-steady-state-snapshot` artifact (not hand-edited; no field
  was adjusted to make a comparison pass)
- **Host:** GitHub-hosted `ubuntu-latest`, Linux/x86_64, `cpu_count=4`,
  16 GB, AMD EPYC 7763
- **Features:** `runtime-tokio,jemalloc,graph,text-index`
- **Build:** debug profile (unoptimized)
- **Shards:** 1
- **RSS:** 127,889,408

### The measurement contract (read before changing the harness)

A baseline is a measurement of **moon under a specific harness**, not of
moon. Everything `start_server()` and `populate_workload()` decide is part of
the contract, and changing any of it invalidates the committed baseline just
as surely as changing `src/`:

- the server flags (`--shards`, `--disk-offload`, `--appendonly`,
  **`--memory-arenas-cap`**)
- the feature set the binary is built with, and the build profile
- the workload constants (`NUM_STRINGS`, `NUM_VECTORS`, `VEC_DIM`,
  `NUM_GRAPH_NODES`, `STEADY_STATE_WAIT`)

This is not hypothetical. It is the second thing that went wrong on this
fixture -- see below.

### Incident 2: the harness changed what it measures (moon#764)

Once the `exit 0` was removed and the gate compared for the first time, it
reported:

```
=== MEMORY REGRESSION DETECTED ===
  FAIL: rss delta=-14.02% (measured=124432384, baseline=144723968)
  FAIL: allocator_overhead delta=-43.34% (measured=26408931, baseline=46612622, threshold=+/-25%)
```

Both failing kinds moved **down**, `dashtable` was flat at `-0.06%`, and the
log carried a `cpu_model differs` warning (baseline EPYC 9V74, measured EPYC
7763). Three things resolved it, none of them the CPU:

1. **`allocator_overhead` is not a second opinion.**
   `src/command/server_admin.rs` computes it as
   `rss.saturating_sub(tracked_sum)`. It is a residual: when RSS moves and
   the tracked kinds do not, it moves with RSS by construction. Two FAIL
   lines, one measurement.
2. **The branch changed zero files under `src/`.** moon's code was
   byte-identical between capture and comparison, so "moon got leaner" was
   not an available explanation.
3. **The CPU model was a red herring, and a same-silicon control proves
   it.** A capture on an EPYC **9V74** -- the very model the old baseline
   came from -- measured **127,610,880** against that baseline's
   **144,723,968**: `-11.82%` on identical silicon. Nine post-change hosted
   samples of the identical source tree, across **four** CPU SKUs the
   `ubuntu-latest` pool hands out (three AMD and, as of this writing, an
   Intel):

   | run | CPU | RSS | dashtable | vs this baseline |
   |---|---|---|---|---|
   | 34192309710 | Xeon Platinum 8370C | 119,943,168 | 96,614,352 | -6.21% |
   | 34191449626 | EPYC 7763    | 122,003,456 | 96,589,950 | -4.60% |
   | 34191154173 | EPYC 7763    | 123,351,040 | 96,691,380 | -3.55% |
   | 34157392545 | EPYC 7763    | 124,432,384 | 96,598,476 | -2.70% |
   | 34191132296 | EPYC 6973P-C | 124,641,280 | 96,600,975 | -2.54% |
   | 34190674369 | EPYC 9V74    | 125,308,928 | 96,612,735 | -2.02% |
   | 34190858545 | EPYC 9V74    | 127,610,880 | 96,698,436 | -0.22% |
   | 34169276001 | EPYC 7763    | 127,889,408 | 96,661,392 |  0.00% (this file) |
   | 34190676729 | EPYC 7763    | 129,495,040 | 96,616,116 | +1.26% |

   The two extreme AMD samples are the **same SKU** (EPYC 7763: 122,003,456
   and 129,495,040, 6.1% apart), so silicon does not explain the spread --
   though the single Intel sample being the lowest is a reason the gate
   *warns* on `cpu_model` rather than ignoring it. `dashtable` across all
   nine spans **0.11%**, which is why its flatness is the diagnostic: the
   workload and moon's tracked allocation are identical every run, so
   anything moving in RSS is outside them.

### Choosing which sample to commit

Nine samples of one source tree span **7.96%** min-to-max. Which sample
becomes the baseline therefore decides how much of the tolerance window is
left over, and getting it wrong is not theoretical: the first re-capture
(127,610,880) was measured against a **symmetric** +/-5% band, which left
0.63% under the observed minimum -- and the very next run came in at
`-4.39%`. Green, one ordinary sample from a false SHRANK.

Against the asymmetric window the gate now uses (grow `+5%`, shrink `-10%`
-- see `RSS_SHRINK_FLOOR` in the script for why), headroom by candidate over
the same nine samples:

| candidate baseline | headroom above observed max (grow +5%) | headroom below observed min (shrink -10%) |
|---|---|---|
| 122,003,456 | **-1.07%** (already red) | +8.46% |
| 125,308,928 | +1.61% | +5.98% |
| 127,610,880 | +3.47% | +4.25% |
| **127,889,408** | **+3.70%** | **+4.04%** |
| 129,495,040 | +5.00% | +2.83% |

This file is the balance point: ~3.7% of margin on the side that catches
regressions and ~4.0% on the side that catches staleness. Nothing here is
fitted -- the thresholds are fixed by the script, the numbers are unedited
artifacts, and the only choice being made is *where in the measured
distribution to stand*.

If the gate ever starts flaking, the fix is more samples and a re-centre on
them -- move the baseline, not the threshold. Every point added to the
**growth** threshold is a real regression the gate stops catching, and a
gate that catches nothing is what moon#764 was.

The actual cause: `start_server()` gained `--memory-arenas-cap 2` (jemalloc
8 arenas -> 2) one commit **after** the baseline was captured. Six fewer
arenas' metadata and dirty-page cache is roughly the whole 20 MB, and it
lands entirely outside the tracked kinds -- exactly the observed shape. The
gate was comparing two different measurement configurations, correctly
refused to call it a pass, and this file is the re-capture under the current
one.

`compare_snapshot` now says which direction a failure went, and a
shrink-only failure prints `=== BASELINE NO LONGER DESCRIBES THIS BUILD ===`
with this section's two candidate causes, so the next person does not have
to re-derive the above.

**One thing this does NOT claim.** `--memory-arenas-cap 2` was added on the
theory that it would also *tighten* run-to-run variance. It has not been
shown to: 7.96% min-to-max over the nine samples above versus 5.50% over the
three pre-change ones. The flag is kept for the effect that *is*
demonstrated -- a ~12% lower absolute RSS on identical silicon -- and the
RSS **growth** threshold is not relaxed on the strength of it.

### Incident 1: a baseline from a machine nobody's code runs on (moon#764)

A GCE `moon-bench-x86` (`c3-standard-8`, Ubuntu 24.04.4 LTS, x86_64) capture
briefly stood in for a hosted one. On the actual `memory-steady-state` job it
produced:

```
=== MEMORY REGRESSION DETECTED ===
  FAIL: rss delta=30.51% (measured=146817024, baseline=112492544)
  FAIL: allocator_overhead delta=243.22% (measured=48943405, baseline=14260002, threshold=+/-25%)
```

`dashtable` (unaffected by machine class) came in at -0.14% on that same
run -- proof the comparison logic itself was fine; the baseline's *origin*
was not. A GCE `c3-standard-8` and GitHub's hosted `ubuntu-latest` are both
`Linux`/`x86_64`, so `check_baseline_provenance`'s (then) os/arch-only check
waved the mismatch through; it now also gates on `cpu_count`. **A baseline
is only valid if it was captured by this workflow, on this runner label** --
see "Regeneration" below. The GCE numbers are kept as the *method* record in
`tmp/perf-campaign/FIX-764.md`, not as a baseline.

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
memory profile against this baseline with a +/-5% per-kind tolerance (wider
floors for `hnsw` / `allocator_overhead` -- see "Per-kind noise floor"). If any
kind falls outside its band the job fails, naming the kind, the delta, and
**which direction it moved**.

Exit codes: `0` in tolerance, `1` a kind is out of band, `2` the gate cannot
legitimately run (bad/mismatched baseline provenance, or a workload that did
not populate).

### Direction matters: GREW vs SHRANK

Both fail. They need different fixes, so they print different headlines:

- `=== MEMORY REGRESSION DETECTED ===` -- something grew. Fix the code.
- `=== BASELINE NO LONGER DESCRIBES THIS BUILD ===` -- everything out of band
  shrank. Fix the *baseline*, after working out why: a real improvement, a
  harness change (see "The measurement contract"), or a workload that did not
  fully run. A shrink is never auto-accepted -- less data measured is less
  memory measured, and printing PASS for that is moon#764's vacuous gate in a
  different costume.

`rss` -- and only `rss` -- judges the two directions at different thresholds:
**grow at +5%, shrink at -10%** (`RSS_SHRINK_FLOOR`). It is the one figure
here that is not an accounting number: ~78% tracked kinds plus a residual of
arena metadata, dirty pages, stacks and text that no counter owns, and that
residual moves 7.96% run-to-run on hosted runners (see the table above) while
`dashtable` moves 0.11%. The asymmetry buys the noise slack **only on the
side that can never be a regression**. The threshold that catches memory
regressions is 5% and is not relaxed; a shrink still has to clear -10% to
pass, and moon#764's own -11.82%..-14.02% step does not.

### Two checks that do not depend on this file

Every comparison in this gate is *relative* to this baseline, which leaves one
shared blind spot: if a capture and a measurement are both of an empty server,
they agree at 0% and stay green forever.

- **`check_workload_ran()`** asserts absolute byte floors on
  `dashtable` / `hnsw` / `csr` (~half of what the fixed workload produces)
  before anything is compared **or written as a baseline**, so an empty
  snapshot can never become the reference.
- **`tests/memory_gate_compare_selftest.sh`** sources the gate -- which has a
  lib-only `BASH_SOURCE` guard for exactly this -- and drives
  `compare_snapshot` / `kind_threshold` / `check_workload_ran` against
  synthetic snapshots: 13 checks, no server, no build, ~1s. It runs in the
  `memory-steady-state` job *before* the build, and it asserts the gate's
  ability to go **red**: growth caught, unexplained shrink caught, empty
  server caught. If it is ever green while the gate is vacuous, that file is
  the bug.

### History: the gate was vacuous (moon#764)

CI invoked the script as `--self-test --skip-build`, and `--self-test` used to
`exit 0` immediately after its injection check -- **before** the
committed-baseline comparison. The self-test compares the freshly captured
snapshot against *itself*, so it can only ever prove the comparison machinery
works; it can never detect a real regression. The result: the job passed for
months without once reading this file.

`--self-test` is now a *phase*. It runs the injection check, then falls through to
the real comparison.
