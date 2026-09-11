# Benchmark data — 2026-09-11

Re-measurement of BENCHMARK.md §2's eight command families against merged main
after ten PRs landed, two of them on the measured hot path (#938, #929). The
question was whether either regressed anything; the answer is no, and the
session also settled [moon#923](https://github.com/pilotspace/moon/issues/923).

Everything here recomputes without a re-run: each CSV carries its own
provenance header (binary sha256, Redis build, CPU, kernel, parameters, UTC
date).

## Hosts

| | `moon-bench-arm` | `moon-bench-x86` |
|---|---|---|
| machine | GCE t2a-standard-8 | GCE c3-standard-8 |
| CPU | Neoverse-N1, 8 cores | Xeon Platinum 8481C @ 2.70 GHz, 8 cores |
| kernel | `Linux 7.0.0-1011-gcp` | `Linux 7.0.0-1011-gcp` |
| Redis control | 7.0.15 (jemalloc-5.3.0) | 7.0.15 (jemalloc-5.3.0) |
| cargo | 1.98.0 | 1.94.1 |

Both legs built with `RUSTFLAGS="-C target-cpu=native"`, run at `--shards 1`,
`--appendonly no`, `--disk-offload disable`.

**Absolute ops/s do not compare across architectures** (different cargo, different
CPU) and do not compare across dates (this kernel is `7.0.0-1011-gcp`, not the
6.17 of the originally published §2 tables). Every comparison below is
same-host, same-session.

## Binaries

| label | commit | ARM sha256 | x86 sha256 |
|---|---|---|---|
| HEAD | `5bf716a9` | `8a9ca343d48a8e6253a73f759c52955c6a4c67493d630b2bcccb9e029fe11039` | `08ebc35c2c274772289c3f862bde4313f3a51e761dcb8d9c07ba25c4ae30d89d` |
| baseline | `ab91a23e` (built as `c095f86d`) | `6403691af6c23cd9bfd2e1a14158e4cbb068f0be0a76f147316bfbe15dd1de66` | `57d8882b1345e8306f4792eb815cc947de98db96c45be11c26aa63ea2ca8245a` |
| bisect | `ae6cd003` | `efeda244925d38af9ce2a0f8d1e5a29ce94d9f2302a92f6f290c0358a8eb4d19` | — |
| bisect | `435ff2d8` | `baa07490ac976bb9c4640d9ed972ffa3e66a65f41f2dbebb581a2722efa577f1` | — |
| bisect | `75ad520c` | `b559c24b7f15105b995311b17725e505154b9ae785d341df217885b69403272c` | — |
| bisect | `1a7bd83f` | `91bc2d6ed1b5023c6049b0dcca8ac012667653a59e64a01ca3108529c53280bb` | — |

`c095f86d` and `ab91a23e` have the **same tree** (`git rev-parse <c>^{tree}` is
`f459d8c946e3f1d8df3c10a935d1afbf11579344` for both) — the first is the branch
commit, the second its squashed landing on main. The saved binaries are
byte-identical to the ones measured on 2026-09-10 (their shas match that run's
CSV headers), so the baseline is continuous with the previous report.

## Files

### Throughput matrix — `matrix-<commit>-<arch>-p<pass>.csv`

`scripts/bench-ab-matrix.sh --reps 5`, eight families × three depths, Redis
restarted and re-measured every rep, leg order alternating within each pass.
Two passes per binary per host, run **A,B,B,A** so a monotone session drift
cannot land on one binary. 240 rows each, zero `NA`.

Reduce moon-vs-Redis with `scripts/bench-ab-report.py <csv> 5`; reduce
moon-vs-moon across binaries with the new `scripts/bench-ab-delta.py`.

### Bisect — `bisect-<commit>-arm-p<pass>.csv`

Four points spanning `ae6cd003..1a7bd83f` (12 commits), two passes each, the
second in reverse commit order. Pooled n=10 per point. Windows:

| window | commits | isolates |
|---|---|---|
| W1 | `ae6cd003` → `435ff2d8` | the first 8 (#859, #854, #862, #864, #866, #871, #874, #872) |
| W2 | `435ff2d8` → `75ad520c` | **#861 alone** — `box RedisValue`'s fat variants |
| W3 | `75ad520c` → `1a7bd83f` | the last 3 (#873, #860, #876) |

### Container memory — `containers-<commit>-<arch>.csv`

`scripts/bench-ab-memory-containers.sh`, new this run: §3 measured **string
values only**, so the container encoding campaign was measured nowhere. 200,000
keys of exactly 4 elements, two figures per type — `untouched` and `touched`,
the latter after **one** secondary write per key using the very mutator each PR
taught to mutate in place (HINCRBY #920, LSET/SREM #921, ZINCRBY #922).

### Zset read encoding — `zsetread-<commit|redis>-<arch>.csv`

`scripts/bench-zset-read-encoding.sh`, new this run. Answers whether a *read*
destroys a small zset's encoding, which a write-only throughput matrix cannot
see. Sweeps three dispatch paths — `plain` (read-only path, the negative
control), `multi` (MULTI/EXEC) and `eval` (Lua) — and records both
`OBJECT ENCODING` and `/proc` RSS, so neither instrument is trusted alone.

The `plain` leg is clean on every binary; that is the point. The defect was
only ever reachable from the **mutable** dispatch path, so a probe that issued
a bare `ZSCORE` would have run clean against a buggy binary and proved nothing.

### `bench-load-encodings-arm.txt` — superseded, kept as the record of a wrong probe

`OBJECT ENCODING` under a load of *seed + `SADD p=64` only*. **Do not read this
as the harness's regime.** It omits `SPOP`, which runs immediately after `SADD`
at every depth and flattens the set it touches, so it reports listpack where the
real leg has hashtable. It is kept because it is what produced a published and
then retracted claim; `perf-861/` has the correct measurement.

### `perf-861/` — mechanism of the #861 regression

- `pop-tally-75ad520c-arm.txt` / `pop-tally-5bf716a9-arm.txt` — encoding
  **population** (200 keys) at each family's own p=64 measurement point, after
  replaying the harness leg in full. This is the authoritative regime record.
  The regime oscillates per key, so one key cannot characterise it.
- `regime-leg-replay-arm.txt` — the per-step walk that first showed `SPOP`
  flattening a set mid-leg.
- `sadd-*.txt` / `spop-*.txt` — `perf report` symbol shares for `435ff2d8` vs
  `75ad520c`, two reps each, from binaries rebuilt with the repo's
  `release-with-debug` profile. Sampling is the `cpu-clock` **software** event —
  this VM exposes no PMU, so `cycles`/`instructions` are unsupported and no IPC
  or cache-miss claim can be made from these.
- `perf-861-rps-arm.txt` — the throughput each profiled run measured, so the
  profile can be tied to a regression that actually reproduced (SADD -4.3%).

### `encodings-arm.txt`

### `encodings-arm.txt`

`OBJECT ENCODING` for the exact shapes the container harness loads, on both
engines — recorded because two of the four container rows are **not**
like-for-like encoding comparisons (see the caveat in BENCHMARK.md §3).

## Reproduce

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
./scripts/bench-ab-matrix.sh --moon-bin ./target/release/moon --reps 5 > matrix.csv
python3 scripts/bench-ab-report.py matrix.csv 5
python3 scripts/bench-ab-delta.py --head new.csv --base old.csv

./scripts/bench-ab-memory-containers.sh --moon-bin ./target/release/moon --keys 200000 --reps 3
./scripts/bench-zset-read-encoding.sh   --moon-bin ./target/release/moon --keys 200000
```

Linux only. io_uring, O_DIRECT and the spin governor are all
`cfg(target_os = "linux")`; a macOS run proves nothing and must not be published.
