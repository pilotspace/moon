# Raw benchmark data — 2026-09-08

Backs every table in [`BENCHMARK.md`](../../../../BENCHMARK.md). moon `ae6cd003`
(v0.8.9, tree `e65f49ee`) vs Redis 7.0.15, GCE `c3-standard-8` (x86_64) and
`t2a-standard-8` (aarch64). Binary sha256, Redis version, CPU model and kernel
are in each matrix CSV's `#` header.

**The CSV headers record `moon: unknown`** — the harness derived the version by
running `moon --version`, and moon has no such flag, so the field is useless.
The binary sha256 is real, and this is the mapping, without which the whole
v0.8.7 A/B would rest on nothing but the order the files were written:

| sha256 (first 16) | commit | arch |
|---|---|---|
| `5e897accdefa2dd1` | `ae6cd003` (v0.8.9, main) | x86_64 |
| `63f018ce3179115f` | `d63ffcd8` (v0.8.7) | x86_64 |
| `efeda244925d38af` | `ae6cd003` (v0.8.9, main) | aarch64 |
| `6e62d252ea22a406` | `d63ffcd8` (v0.8.7) | aarch64 |

Both binaries per arch were built on the same host, same toolchain, same
`RUSTFLAGS="-C target-cpu=native"`, fat LTO.

| file | what |
|---|---|
| `matrix-x86.csv` / `matrix-arm.csv` | throughput, moon `ae6cd003`, 8 families x 3 depths x 5 interleaved reps x 2 engines |
| `matrix087-x86.csv` / `matrix087-arm.csv` | the same matrix against moon `d63ffcd8` (v0.8.7), for the A/B in §2 |
| `mem-x86.csv` / `mem-arm.csv` | per-key and idle RSS at 8/64/256/1024 B, 3 reps, fresh server per point |
| `mem-fine-x86.csv` / `mem-fine-arm.csv` | 16/32/48/128 B, 2 reps — measured to test the archived "11-51% worse at 32 B" claim |

Recompute any throughput table with:

```bash
python3 scripts/bench-ab-report.py matrix-x86.csv 5
```

The trailing `5` is the expected repetition count. Without it the script falls
back to the `reps:` field in the CSV header; with neither, it cannot tell a
truncated file from a complete one and says so.

## Two provenance notes, so the numbers are not read as more than they are

**The memory CSVs were produced by an earlier revision of the harness than the
one now in `scripts/`.** They have no `order` column because that revision
alternated engines per *repetition*; the shipped script alternates per
`(rep, value_size)` and records the order, after review pointed out that per-rep
alternation lets a drift over the run land on one engine. Re-running the shipped
script will therefore produce a differently-shaped CSV than the ones here.
RSS with a fresh server per point is far less drift-prone than throughput, and
the collected data argues the ordering did not matter here — the three
repetitions agree to within ~1 B/key, and the two architectures agree to within
3 B/key at every shared size — but the CSVs were produced by the older ordering
and should be re-collected with the current script before anyone leans harder
on them.

**The v0.8.7 legs are a sequential A/B, not an interleaved one.** Each version
was measured against its own freshly-restarted Redis, and Redis drifted a median
-0.4% (x86) / +0.3% (ARM) between the two runs, so the comparison holds — but it
is the weaker design, and it is only adequate because the control was checked
rather than assumed.
