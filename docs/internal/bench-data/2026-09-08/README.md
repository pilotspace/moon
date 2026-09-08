# Raw benchmark data — 2026-09-08

Backs every table in [`BENCHMARK.md`](../../../../BENCHMARK.md). moon `ae6cd003`
(v0.8.9, tree `e65f49ee`) vs Redis 7.0.15, GCE `c3-standard-8` (x86_64) and
`t2a-standard-8` (aarch64). Binary sha256, Redis version, CPU model and kernel
are in each matrix CSV's `#` header.

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

## Two provenance notes, so the numbers are not read as more than they are

**The memory CSVs predate the harness's `order` column.** They were collected
with `bench-ab-memory.sh` alternating engines per *repetition*; the script now
alternates per `(rep, value_size)` and records the order, after review pointed
out that per-rep alternation lets a drift over the run land on one engine.
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
