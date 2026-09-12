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

## Standing conclusion

Handler work is doing what the path work could not. After change 2:

- **LPUSH** is reachable by the dispatch path alone.
- **HSET** and **ZADD** are within 0.11-0.14 µs/op of it.
- **SADD** is untouched at 1.088 and remains the outlier. Its 7 accessor probes
  per op (vs Redis's 1) are the only candidate large enough to close it — and
  that is precisely the change class PERF-08 measured at **+11% aarch64 /
  −17% x86**, so it must be priced on both architectures. `moon-bench-x86` is
  TERMINATED; nothing about it ships on ARM-only evidence.
- **INCR** is unchanged and still marginal.

## Files

- `ab-wakeguard-base-d3b8a206-arm.csv` — the shared base matrix.
- `ab-wakeguard-hoist-908609c1-arm.csv` — wake-guard hoist.
- `ab-listpack-0e725a5b-arm.csv` — listpack + one-walk.
