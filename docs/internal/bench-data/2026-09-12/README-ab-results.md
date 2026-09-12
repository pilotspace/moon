# A/B results — moon#942 handler-side changes (ARM, 2026-09-12)

Each change measured **separately** against the same base so the effect is
attributed, not inferred. Base = `d3b8a206` (main), binary `sha256:8a9ca343…`.
Harness: `scripts/bench-ab-matrix.sh --reps 5`, compared with
`scripts/bench-ab-delta.py`, which carries Redis as the same-session control and
refuses to normalise any row whose control CV exceeds 5%.

## 1. Wake-guard hoist (`908609c1`) — **no measured effect**

Gating `handler_monoio`'s second exclusive db guard on `producer_family` so
INCR/SADD/HSET stop acquiring it to discover there is nothing to wake.

**21 of 24 cells are ties.** Three fall outside their floor, and two of those are
mechanistically impossible:

- `GET p=8 −2.9%` — GET never reaches the write tail.
- `LPUSH p=64 +2.3%` — LPUSH *is* a list producer, so it still takes the guard.

Since two impossible cells moved by the same magnitude as the one plausible cell
(`INCR p=64 +2.9%`), none of the three is attributable. **Reported as a tie.**

That is the expected answer: an uncontended `parking_lot` write guard is ~20-40 ns
against a ~3.2 µs/op budget, i.e. ~1% — below this instrument's floor. The change
is kept because it is correct, removes a real needless acquisition, can only help
as contention rises (which `--shards 1` cannot show), and because
`tests/wakeup_local_write_gate.rs` now pins moon#595's routing-dependent
behaviour at 1 **and** 4 shards. No throughput claim is made for it.

## 2. listpack `encode_entry` + ZADD/ZINCRBY one-walk (`0e725a5b`) — **real**

`encode_entry` went from **4 heap allocations per string entry to 0** (counting
allocator, control asserted in the same test), and ZADD/ZINCRBY now update a
listpack zset in one walk instead of two (seek counter 1 → 0, byte-equality
oracle against the old shape). Part A also subsumes audit candidate #8
(same-width in-place write) — a splice cannot take a three-part borrowed entry
without materialising it — so this A/B measures #2 and #8 together.

### The result is mechanism-correlated, which is the strongest part of it

| family | writes listpack entries? | p=64 |
|---|:---:|---|
| ZADD | yes | **+6.5%** |
| LPUSH | yes | **+6.0%** |
| HSET | yes | **+4.7%** |
| GET | no (negative control) | tie |
| INCR | no | tie |
| SADD | no — hashtable at this size | tie |
| SPOP | no | tie |

Every family that moved writes listpack entries; every family that did not,
does not. GET is a tie at all three depths.

### Effect on the required cut

| family | cut before | cut after | Δ | covered by the 0.626 µs/op transferable path tax? |
|---|---:|---:|---:|:---|
| **LPUSH** | 0.722 | **0.575** | −0.147 | **yes — newly covered** |
| ZADD | 0.961 | **0.735** | −0.226 | no, short by 0.11 (was 0.32) |
| HSET | 0.861 | **0.765** | −0.096 | no, short by 0.14 (was 0.22) |
| INCR | 0.602 | 0.621 | tie | marginal, unchanged |
| SADD | 1.063 | 1.088 | tie | no, short by 0.46 |

**Caveat on ZADD:** its `C` is solved from a p=8 leg whose worst CV is 6.8-7.4%,
above the 5% limit at which the delta tool declines to normalise. ZADD's clean
evidence is the p=64 row (+6.5%, control CV 1.6%). Read −0.226 as directionally
solid and the least precise figure in the table.

## 3. SADD moon#944 + routing pre-pass (`b838e8ba`) — correctness verified, perf not yet measured

The #944 reply fix was verified independently of the agent that wrote it, with
the orchestrator's own probe against a redis oracle: all four rows match,
including the two that previously diverged (`reply=22, added=22`), with both
controls still passing. The #795 byte-transparency vectors (`007`, `+7`, `-0`,
`00`, `0000000000012345`) round-trip identically, so the new routing predicate
did not reintroduce that corruption class.

The pre-pass change is **not yet A/B'd**. Note the implementing agent's own
correction to the audit: `all_integers` uses `.all()`, which short-circuits, so
at the benchmark's single-member shape the pre-pass is ONE call rather than a
full argv walk — the expected win is smaller than the audit implied.

## 4. INCR in place (`c0e17c48`) — real

Three DashTable probes and a full entry rebuild collapse to one in-place mutation.
All eleven side effects of the old `get`+`set` pair were enumerated and decided
per item; 21 injected defects, 21 caught.

`INCR p=64 +16.5% raw / +14.1% ratio` (floor 1.5%, control CV 1.4%). Required cut
**0.602 -> 0.358**. INCR p=8 +3.5%; p=1 tie. SET/GET ties.

## 5. Accessor probe collapse (`9ea2d7ec`) — real, and it does NOT invert across arches

SADD's end-to-end probe count 7 -> 4 (hashtable) and 4 -> 2 (listpack); every
`get_or_create*` hit 3 -> 2; `get_promoted` 4 -> 2.

**This is the change class PERF-08 measured at +11% aarch64 / -17% x86 — opposite
signs — so it was priced on BOTH architectures before any claim.** It gains on
both:

| family, p=64 | ARM | x86 |
|---|---:|---:|
| SPOP | +9.6% | +8.2% |
| SADD | +6.6% | +7.2% |
| LPUSH | +7.8% | +4.7% |
| HSET | +5.1% | +4.5% |
| ZADD | +2.9% | +3.0% |
| **INCR** | **tie** | **tie** |

INCR is a tie on both, independently — the mechanism check. On this branch INCR
still routes through `Database::get`/`set`, not the collapsed accessors, so it
*should not* move, and does not. Every family that uses them gained on both
arches; the one that does not, did not, on both.

## 6. ALL FIVE COMBINED (`ad6b97cc`) — the number that is actually true

The five changes above were each measured in isolation, and isolated effects need
not add. This is the merged tree measured against the same base.

| family, p=64 | ARM | x86 | moon:redis ratio, ARM |
|---|---:|---:|---|
| INCR | **+20.3%** | +15.4% | 0.628x -> **0.733x** |
| LPUSH | **+15.7%** | +10.7% | 0.700x -> **0.800x** |
| SADD | **+14.7%** | +10.9% | 0.658x -> **0.747x** |
| SPOP | **+14.4%** | +9.6% | 0.797x -> **0.893x** |
| HSET | **+12.6%** | +10.6% | 0.614x -> **0.692x** |
| ZADD | **+10.5%** | +14.8% | 0.693x -> **0.764x** |

Double digits on every family on ARM, and on every family but SPOP on x86.

### Required cut, start to finish (ARM)

Against the measured **0.626 µs/op** transferable dispatch-path tax:

| family | at campaign start | final | path tax covers it now? |
|---|---:|---:|:---|
| **SPOP** | 0.759 | **0.286** | **yes** |
| **INCR** | 0.602 | **0.340** | **yes** |
| **LPUSH** | 0.722 | **0.405** | **yes** |
| **HSET** | 0.861 | **0.592** | **yes** |
| SADD | 1.063 | **0.685** | short by 0.06 |
| ZADD | 0.961 | **0.705** | short by 0.08 |

Four families now sit inside the path budget. SADD — which began at nearly double
it and which nothing but the probe collapse could move — and ZADD are each within
about a tenth of a microsecond.

**ZADD's figure remains the least precise in the table:** its p=8 control CV is
9.2%, above the limit at which the delta tool declines to normalise. Its clean
evidence is p=64 (+10.5% ARM, +14.8% x86, control CV 1.2-2.3%).

## Standing conclusion

Handler work did what path work alone could not, and the campaign's opening
premise — that the dispatch-path tax covered every family's gap — was wrong twice
over (a cut table twelve commits stale, and a tax inflated by GET-only work plus
the instrument's own ACL check). Both errors were corrected by measurement, not
argument; see the correction history in `README.md`.

What is now true, measured on both architectures:

- **Every one of the five target families is double-digit faster at p=64 on ARM**,
  and on x86 too except SPOP at +9.6%.
- **SPOP, INCR, LPUSH and HSET** have required cuts inside the 0.626 µs/op the
  dispatch path can supply. Finishing them is now path work, tracked in #942.
- **SADD and ZADD** are 0.06 and 0.08 µs/op outside it.
- The **wake-guard hoist measured as a tie** and is recorded as one. Its three
  non-tie cells were disqualified by mechanism, not by taste.

### What would falsify any of this

Every A/B here is `--shards 1`, `--reps 5`, one host per architecture, against
redis 7.0.15. A re-run on a different host, a different Redis build, or at a
different shard count could move these. Rows whose Redis control exceeded 5% CV
are marked and were never normalised. `C`/`B` is a two-point solve — exactly
determined, no residual — so read every cut as a band, not a figure.

## Files

- `ab-wakeguard-base-d3b8a206-arm.csv` — the shared ARM base matrix.
- `ab-wakeguard-hoist-908609c1-arm.csv` — wake-guard hoist (tie).
- `ab-listpack-0e725a5b-arm.csv` — listpack + one-walk.
- `ab-incr-c0e17c48-arm.csv` — INCR in place.
- `ab-probes-9ea2d7ec-{arm,x86}.csv` — accessor probe collapse, both arches.
- `ab-base-d3b8a206-x86.csv` — the x86 base matrix.
- `ab-combined-ad6b97cc-{arm,x86}.csv` — all five changes merged, both arches.
