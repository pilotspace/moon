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

## Results — ARM (`moon-bench-arm`, Neoverse-N1, kernel 7.0.0-1011-gcp)

All four runs use the **same binary** (`sha256:8a9ca343d48a8e62`), host, kernel
and Redis build as the HEAD matrix in `../2026-09-11/`, so every number here is
directly comparable with the per-family figures there.

### 1. The raw tax, two commands

| command | inline C | generic C | tax | clears floor by |
|---|---:|---:|---:|---:|
| `GET` | 0.613 | 1.516 | **0.904** | 30.4x |
| `SET` | 0.573 | 1.396 | **0.823** | 31.6x |

The GET figure is 0.081 µs/op higher, and there is a named mechanism for it: the
generic read path runs a **GET-only** cold-tier peek
(`handler_monoio/mod.rs:3674`, gated on `eq_ignore_ascii_case(b"GET")`) that
takes the database's *exclusive* guard and performs a second full DashTable probe
(`db.is_hot`). None of INCR/SADD/LPUSH/HSET/ZADD executes it. Treat the 0.081 as
weak on its own — it is a difference of two numbers each carrying ~0.03 µs of
floor — but it has the right sign and a mechanism.

### 2. The ACL confound, measured rather than assumed

The generic leg above authenticates as a restricted user, so it also pays the ACL
check (`acl/table.rs:428` does `to_ascii_lowercase()` — a **heap allocation per
command** — plus a HashSet probe, reached only when the user is not
`unrestricted`). The first version of this README called that "a one-directional
confound" and left it unbounded. It is measurable: **HSET is inline-ineligible
for both users**, so running it as unrestricted vs restricted puts *both* legs on
the generic path and the difference is the ACL check alone.

| command | reps | unrestricted C | restricted C | ACL cost | clears floor by |
|---|---:|---:|---:|---:|---:|
| `HSET` | 3 | 2.050 | 2.196 | 0.147 | 1.8x |
| `HSET` | 5 | 2.015 | 2.212 | **0.197** | 2.2x |
| `LPUSH` | 5 | — | — | *0.200* | **inside floor (8.3%)** |

The LPUSH run **does not clear its own noise floor** and is reported only as
corroboration of magnitude, not as a result — the reporter suppresses it. Take
the ACL check as **≈0.20 µs/op, ±0.09**. It is the loosest term in the chain and
the one worth re-measuring first if any conclusion turns on it.

### 3. What actually transfers to the write families

> **Transferable write-path tax = 0.823 − 0.197 = `0.626 µs/op`** (±0.09,
> dominated by the ACL term).

Against the required cuts re-solved from the HEAD matrix
(`../2026-09-11/matrix-5bf716a9-arm-p{1,2}.csv`):

| family | cut p1 | cut p2 | 0.626 covers it | margin |
|---|---:|---:|:---:|---:|
| INCR | 0.586 | 0.558 | **marginally** | +0.04 … +0.07 |
| LPUSH | 0.671 | 0.702 | **no** | −0.05 … −0.08 |
| HSET | 0.842 | 0.848 | **no** | −0.22 |
| ZADD | 0.944 | 0.984 | **no** | −0.32 |
| SADD | 1.049 | 1.058 | **no** | −0.43 |

**This reverses the conclusion this directory was created to support.** Removing
the entire generic-path preamble would bring only INCR to parity, and only within
the uncertainty of the ACL term. Every other family needs handler-side work, and
SADD needs roughly two-thirds as much again on top of a perfect path.

### Correction history

The first version of this README compared the 0.904 µs GET tax against a
required-cut table taken from `../2026-09-08/matrix-arm.csv`, whose binary is
`sha256:efeda244925d38af` — **`ae6cd003`, twelve commits behind HEAD and before
#861**. Those cuts reproduce from that file to three decimals, so it is
definitively the source. Two errors followed from it:

1. **A stale comparison.** At HEAD, SADD's cut is 1.05 (not 0.583) and ZADD's is
   0.96. #861 landed after that dataset and is attributed −6.4% on SADD.
2. **An overstated tax.** 0.904 was compared against write families although it
   contains GET-only work and an unmeasured ACL check. Both are now sized.

The tax measurement itself was never wrong. The inference drawn from it was.
Recorded here rather than rewritten away, per this file's convention.

## Caveats — read before quoting this

- **`C`/`B` is a two-point solve** — exactly determined, no residual: a
  decomposition under an assumed model, not a fit. Read every `C` as a band.
- **The ACL term is the weak link** (clears its floor by 2.2x; the LPUSH
  corroboration does not clear its own at all). 0.626 inherits that ±0.09.
- **The subtraction assumes the ACL check costs the same for SET as for HSET.**
  It is a command-name lowercase plus a HashSet probe, so this is reasonable, but
  it is an assumption — SET could not be measured directly because it *is*
  inline-eligible, which is the whole mechanism being used.
- **ARM only.** `moon-bench-x86` is TERMINATED. Trap 3 (PERF-08) records this
  repo measuring a probe-count change at +11% aarch64 and −17% x86 — the two
  architectures have already disagreed *in sign* on a change of this class.
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

Every row of every CSV carries the `inline_delta` instrument check.

- `path-tax-get-arm.csv` — GET, inline vs generic (the original run).
- `path-tax-set-arm.csv` — SET, inline vs generic; drops the GET-only cold peek.
- `acl-cost-hset-arm.csv` — HSET unrestricted vs restricted, 3 reps.
- `acl-cost-hset-arm-n5.csv` — the same at 5 reps; **this is the ACL figure used**.
- `acl-cost-lpush-arm-n5.csv` — LPUSH corroboration; **inside its own noise
  floor**, kept as the record of a measurement that did not resolve.

## Reproduce

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
# the tax for an inline-eligible command
./scripts/bench-path-tax.sh --moon-bin ./target/release/moon --reps 3 \
  --mode tax --command 'set key:__rand_int__ xxxxxxxx' > tax.csv

# the ACL check alone: both legs generic, because HSET is inline-ineligible
./scripts/bench-path-tax.sh --moon-bin ./target/release/moon --reps 5 \
  --mode aclcost --command 'hset h:__rand_int__ f xxxxxxxx' > acl.csv

python3 scripts/bench-path-tax-report.py tax.csv
python3 scripts/bench-path-tax-report.py acl.csv
```

Linux only. A macOS run exercises neither io_uring nor the Linux-only paths and
must not be published.
