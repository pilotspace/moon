# Benchmark data — 2026-09-12

One measurement: **how much moon's generic command path costs over its inline
path, for identical handler work.**

## Why this needed measuring

BENCHMARK.md §2 records that moon's inline path (GET, plain SET) is ~0.56-0.58x
of Redis's per-command cost while its generic path is 1.32-1.65x, and that the
excess is a near-constant ~1.0 µs/op across four unrelated container types. That
strongly suggests the cost is *the path*, but every number behind it compares
**different commands** — so the gap could equally have been the datatypes. The
~60% of it attributed to the frame-loop preamble was explicitly **unattributed
by perf**, because fat LTO folds the loop into one `{{closure}}` symbol.

Nothing should be built on an inference that load-bearing. This isolates it.

## Method

`can_inline_reads` is a conjunction whose first term is `conn.acl_skip_allowed()`
(`server/conn/handler_monoio/mod.rs:1468`). A connection authenticated as a
**non-`unrestricted`** ACL user therefore runs *the same `GET` handler* through
the full generic preamble. `unrestricted` requires `allowed_commands` to be
`AllAllowed` (`acl/table.rs` `recompute_unrestricted`), so `+@all -debug` breaks
it while leaving GET fully permitted.

Same command, same keyspace, same host, same session, legs interleaved — **only
the dispatch path differs**.

`scripts/bench-path-tax.sh` + `scripts/bench-path-tax-report.py`.

## Result — ARM (`moon-bench-arm`, Neoverse-N1, kernel 7.0.0-1011-gcp)

| leg | p=8 rps | p=64 rps | C (µs/op) | B (µs/batch) |
|---|---:|---:|---:|---:|
| inline | 501,253 | 1,273,372 | **0.613** | 11.06 |
| generic | 325,203 | 584,352 | **1.516** | 12.47 |

> **Path tax = 0.904 µs/op**, 2.48x the inline per-command cost.

Worst within-leg CV 2.0%; the tax clears that floor by **30x**. The per-batch
term also grows (+1.41 µs/batch), which the `C`/`B` split keeps separate.

### What it means for the five losing families

Required cut to reach parity with Redis at p=64 (ARM `C` values, BENCHMARK.md §2):

| family | must cut | tax covers it | margin |
|---|---:|:---:|---:|
| INCR | 0.568 | yes | +0.336 |
| SADD | 0.583 | yes | +0.321 |
| LPUSH | 0.587 | yes | +0.317 |
| HSET | 0.797 | yes | **+0.107** |

HSET's margin is thin enough that it likely needs handler-level work too.

## Caveats — read before quoting this

- **Upper bound, not a figure.** The generic leg also pays the ACL check the
  inline leg skips. That confound is one-directional: it can only *overstate*
  the path tax.
- **`C`/`B` is a two-point solve** — exactly determined, no residual, a
  decomposition under an assumed model rather than a fit. Read 0.904 as a band.
- **ARM only.** Not re-measured on x86.
- Absolute ops/s are not comparable across dates or architectures.

## The instrument, and how it failed first

`moon_dispatch_path_total{path="local_inline"}` has exactly one production call
site (`handler_monoio/mod.rs:1618`). The harness fails closed unless the counter
advances on **every** inline leg and stays **exactly frozen** on every generic
leg — recorded per row as `inline_delta`. In this run the inline legs advanced by
exactly the request count (400,000 at p=8, 1,500,032 at p=64) and the generic
legs by exactly 0.

That guard exists because the first attempt at this experiment failed silently:
`redis-benchmark` takes `-a`, not `--pass` (that is `redis-cli`), so the generic
legs produced **empty** output — and a frozen counter "confirmed" the path while
nothing had run at all. An empty-vs-empty comparison passes while proving
nothing. The committed harness refuses an empty leg outright.

## Files

- `path-tax-get-arm.csv` — the run, one row per leg × depth, with the
  `inline_delta` instrument check on every row.

## Reproduce

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
./scripts/bench-path-tax.sh --moon-bin ./target/release/moon --reps 3 > tax.csv
python3 scripts/bench-path-tax-report.py tax.csv
```

Linux only. A macOS run exercises neither io_uring nor the Linux-only paths and
must not be published.
