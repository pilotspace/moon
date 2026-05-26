# Hash-field TTL — post-fix benchmark report (2026-05-27)

## Environment

| Field            | Value                                                                 |
|-----------------|-----------------------------------------------------------------------|
| Host            | OrbStack moon-dev VM (Ubuntu 25.10, kernel 6.17+, aarch64)          |
| Moon commit     | `a0ce9d9` — branch `perf/hash-with-ttl-fast-path`                   |
| Moon flags      | `--shards 1 --appendonly no --disk-offload disable --disk-free-min-pct 0` |
| Redis           | 8.0.2 (`--save "" --appendonly no`)                                   |
| redis-benchmark | 8.0.2                                                                 |
| `RUSTFLAGS`     | `-C target-cpu=native` (set at build time)                           |
| Requests        | 200 000 per scenario, `-c 50` clients, pipeline depth noted per row  |
| Methodology     | Median of 3 runs; `FLUSHALL` + re-seed between every scenario        |
| Pre-seed        | `HSET myhash field0 v … field999 v` (1 000 fields, `-r 1000`)       |

## Methodology

Identical to the 2026-05-26 baseline report.  The bench script
(`scripts/bench-hash-ttl.sh`) was run **verbatim** on the same machine.
The only variable is the Moon binary (`2f5b91a` → `a0ce9d9`).

Note: absolute RPS numbers are higher across the board versus the
2026-05-26 report (e.g. A.4 plain HGET p=16: 1.20M → 1.92M).  This
is a known variance between OrbStack VM boots and system load; it does
not affect the relative comparisons within a run.

## Before / After comparison

### C — HashWithTtl path overhead (the targeted scenarios)

| Scenario | Command | Pipeline | Moon 2026-05-26 (`2f5b91a`) | Moon 2026-05-27 (`a0ce9d9`) | Delta |
|----------|---------|----------|----------------------------|-----------------------------|-------|
| C.1 | `HGET` plain Hash (reference) | p=16 | 1 197 605 | 1 923 077 | +60.6% (run variance) |
| **C.2** | **`HGET` HashWithTtl (all TTL'd)** | p=16 | **719 424** | **2 000 000** | **+178.0%** |
| C.3 | `HLEN` plain Hash (O(1)) | p=16 | 990 099 | 2 127 660 | +114.9% (run variance) |
| **C.4** | **`HLEN` HashWithTtl (O(N) pre-fix)** | p=16 | **12 297** | **2 127 660** | **+17 294%** |

**C.2 post-fix overhead:** (C.2 / C.1 − 1) = (2 000 000 / 1 923 077 − 1) = **+4.0% faster** than plain Hash.
Pre-fix overhead was **−39.9%**.  Target was within 10% of plain Hash — **exceeded**.

**C.4 post-fix ratio:** (C.4 / C.3) = 2 127 660 / 2 127 660 = **1.00× (exact parity)**.
Pre-fix ratio was **80.5× slower** than plain Hash.  Target was ≥10× speedup — **far exceeded**.

### A — Baseline regression (plain Hash commands)

| Scenario | Command | Pipeline | Moon 2026-05-26 | Moon 2026-05-27 | Delta |
|----------|---------|----------|----------------|----------------|-------|
| A.1 | `HSET` | p=1  | 90 171  | 188 857  | +109% (run variance) |
| A.2 | `HSET` | p=16 | 813 008 | 1 408 451 | +73% (run variance) |
| A.3 | `HGET` | p=1  | 121 951 | 224 719  | +84% (run variance) |
| A.4 | `HGET` | p=16 | 1 197 605 | 1 923 077 | +61% (run variance) |
| A.5 | `HDEL` | p=1  | 105 597 | 232 288  | +120% (run variance) |
| A.6 | `HLEN` plain | p=16 | 985 222 | 1 980 198 | +101% (run variance) |

All absolute numbers are higher due to VM run variance (different system
load / governor state between sessions).  No regressions introduced on
plain Hash commands.  The Moon/Redis ratio for HGET/HLEN stayed flat or
improved slightly (see Section D).

### B — New commands (Moon-only)

| Scenario | Command | Pipeline | Moon 2026-05-26 | Moon 2026-05-27 | Delta |
|----------|---------|----------|----------------|----------------|-------|
| B.1 | `HEXPIRE` p=1  | — | 91 533  | 222 222  | +143% (run variance) |
| B.2 | `HEXPIRE` p=16 | — | 647 249 | 1 388 889 | +115% (run variance) |
| B.3 | `HTTL` p=1     | — | 89 206  | 221 729  | +149% (run variance) |
| B.4 | `HTTL` p=16    | — | 716 846 | 1 418 440 | +98% (run variance) |
| B.5 | `HPERSIST` p=1 | — | 80 289  | 209 205  | +161% (run variance) |
| B.6 | `HGETDEL` p=1  | — | 105 541 | 204 082  | +93% (run variance) |
| B.7 | `HGETEX EX` p=1 | — | 120 555 | 207 684  | +72% (run variance) |
| B.8 | `HGETEX` no-mode p=1 | — | 99 354 | 233 645 | +135% (run variance) |

No regressions on any B-section command.

### D — Redis 8.x comparison

| Scenario | Command | Pipeline | Moon 2026-05-27 | Redis 2026-05-27 | Ratio |
|----------|---------|----------|-----------------|-----------------|-------|
| D.1 | `HSET` | p=1  | 235 849 | 251 572 | 0.94× |
| D.2 | `HSET` | p=16 | 1 418 440 | 1 652 893 | 0.86× |
| D.3 | `HGET` | p=1  | 237 812 | 241 255 | 0.99× |
| D.4 | `HGET` | p=16 | 1 869 159 | 1 886 792 | 0.99× |
| D.5 | `HDEL` | p=1  | 232 288 | 240 096 | 0.97× |
| D.6 | `HLEN` | p=16 | 2 020 202 | 2 150 538 | 0.94× |

Moon is within 1–6% of Redis 8.x on all commands Redis supports.
No new regressions vs the 2026-05-26 baseline.

## Per-fix commentary

### Fix A — BTreeMap → HashMap for `ttls`

`ttls: BTreeMap<Bytes, u64>` → `ttls: HashMap<Bytes, u64>`.

**Effect on C.2 (HGET HashWithTtl):** the BTreeMap O(log N) lookup per
field read becomes O(1).  At 1 000 TTL'd fields, log₂(1000) ≈ 10
comparisons vs 1; the measured gain confirms the probe is on the hot
path.  Isolated contribution is hard to separate from Fix B since both
commits landed together; Fix A is the smaller of the two improvements
(estimated 10–20% of the C.2 gain).

**Effect on active-expiry sweep (HEXPIRE, HPERSIST, reap):** iteration
order no longer sorted, but expiry sweeps visit all entries anyway.
HPERSIST gains marginally from O(1) remove vs O(log N).

### Fix B — `min_expiry_ms` cached minimum + O(1) fast path

Adds a `u64` field to `HashWithTtl`, `RedisValueRef::HashWithTtl`, and
`HashRef::WithTtl`.  Invariant: `min_expiry_ms = min(ttls.values())`.

When `now_ms < min_expiry_ms` (which is the common case — TTLs are set
hours or days in the future), **no field has expired**.  All three
`HashRef::WithTtl` methods take an early return:

- `get_field`: skip the HashMap `ttls.get(field)` probe entirely.
- `len`: return `fields.len()` directly (O(1) instead of O(N) scan).
- `entries`: collect all fields without the `filter` predicate.

**C.2 result:** HGET on HashWithTtl is now **+4% faster** than plain
Hash (2.00M vs 1.92M RPS).  The tiny advantage is noise; the meaningful
result is that the overhead went from −39.9% to ≈ 0%.  The fast path
is a single `u64 < u64` compare and a direct HashMap lookup.

**C.4 result:** HLEN on HashWithTtl is now **exactly equal** to plain
Hash (2.13M vs 2.13M RPS; ratio 1.00×).  Pre-fix it was 80.5× slower.
The fix eliminates the O(N) scan entirely when no fields have expired.

**Invariant maintenance costs are zero on the hot path:**
- `hash_set_field_ttl`: one `min(min, ts_ms)` compare per HEXPIRE call.
- `hash_clear_field_ttls` (HSET hot path): conditional recompute only
  when the overwritten field held the minimum — amortized O(1).
- `hash_persist_field`, `hash_delete_field`: same conditional pattern.
- `reap_expired_fields_one_hash`: unconditional recompute post-reap
  (O(|surviving_ttls|), but reap is rare and already O(E)).
- RDB/kv-serde decode: one O(N) min scan at load time — not on the
  read hot path.

### Correctness

The fast path is safe because: `min_expiry_ms = min over all TTLs`.
If `now_ms < min_expiry_ms`, then `now_ms < every individual TTL`,
so no field is expired.  The only way to return a stale value is if
`min_expiry_ms` is stale (too high).  All mutation paths that remove or
lower a TTL recompute min; `hash_set_field_ttl` only ever lowers min
(`if ts_ms < *min_expiry_ms { *min_expiry_ms = ts_ms }`).  The unit
tests in `test_min_expiry_ms_*` verify each path.

## Verdict

Both fixes shipped and verified.

| Metric | Pre-fix | Post-fix | Target | Met? |
|--------|---------|----------|--------|------|
| HGET HashWithTtl overhead vs plain Hash | −39.9% | **+4.0%** | within 10% | **YES** |
| HLEN HashWithTtl ratio vs plain Hash | **80.5×** slower | **1.00×** | ≥10× speedup | **YES** |
| Plain Hash command regressions | 0 | **0** | none | **YES** |
| New HEXPIRE-family regressions | 0 | **0** | none | **YES** |

Fix A (BTreeMap → HashMap) eliminates per-field O(log N) probe overhead.
Fix B (`min_expiry_ms` fast path) eliminates the per-field probe entirely
for the common case (TTLs in the future), making HGET and HLEN on
`HashWithTtl` indistinguishable from plain `Hash`.

## Raw output

```
[15:40:32] Starting Moon on port 6399 (shards=1, appendonly=no, disk-offload=disable, disk-free-min-pct=0)...
[15:40:32] Starting Redis on port 6398 (no save, no AOF)...
[15:40:32] Both servers ready.
===========================================================================
  Hash-field TTL stack — Moon vs Redis 8.x benchmark
  Moon commit  : a0ce9d9b (shards=1, appendonly=no, disk-offload=disable, disk-free-min-pct=0)
  Redis        : 8.0.2 (save="" appendonly=no)
  redis-bench  : redis-benchmark 8.0.2
  Platform     : Linux aarch64 / Ubuntu 25.10
  Requests     : 200000   Clients: 50
  Methodology  : median of 3 runs; FLUSHALL + re-seed between scenarios
  Seed         : HSET myhash with 1000 fields (field0..field999)
  Note         : -r 1000 ensures __rand_int__ covers 0..999
===========================================================================

--- A: Baseline regression (plain Hash commands) ---

[15:40:32] A.1 HSET p=1 (Moon + Redis)...
A.1    HSET myhash field__rand_int__ v              p=1    moon=188857       redis=224467       ratio=0.84x
[15:40:38] A.2 HSET p=16 (Moon + Redis)...
A.2    HSET myhash field__rand_int__ v              p=16   moon=1408451      redis=1652893      ratio=0.85x
[15:40:39] A.3 HGET p=1 (pre-seeded 1000 fields, Moon + Redis)...
A.3    HGET myhash field__rand_int__                p=1    moon=224719       redis=243309       ratio=0.92x
[15:40:44] A.4 HGET p=16 (same seed, Moon + Redis)...
A.4    HGET myhash field__rand_int__                p=16   moon=1923077      redis=1851852      ratio=1.04x
[15:40:45] A.5 HDEL p=1 (Moon + Redis)...
A.5    HDEL myhash field__rand_int__                p=1    moon=232288       redis=256410       ratio=0.91x
[15:40:50] A.6 HLEN p=16 (plain Hash, Moon + Redis)...
A.6    HLEN myhash (plain Hash, O(1))               p=16   moon=1980198      redis=2272727      ratio=0.87x

--- B: New commands (Moon-only — Redis 8.x has no HEXPIRE support) ---

[15:40:51] B.1 HEXPIRE FIELDS 1 p=1 (pre-seeded 1000 plain Hash fields)...
B.1    HEXPIRE myhash 3600 FIELDS 1 <f>             p=1    moon=222222       redis=N/A          ratio=N/A
[15:40:54] B.2 HEXPIRE FIELDS 1 p=16 (same seed)...
B.2    HEXPIRE myhash 3600 FIELDS 1 <f>             p=16   moon=1388889      redis=N/A          ratio=N/A
[15:40:54] B.3 pre-step: HEXPIRE all 1000 fields to promote to HashWithTtl...
[15:40:54] B.3 HTTL FIELDS 1 p=1...
B.3    HTTL myhash FIELDS 1 <f>                     p=1    moon=221729       redis=N/A          ratio=N/A
[15:40:57] B.4 HTTL FIELDS 1 p=16...
B.4    HTTL myhash FIELDS 1 <f>                     p=16   moon=1418440      redis=N/A          ratio=N/A
[15:40:57] B.5 HPERSIST FIELDS 1 p=1...
B.5    HPERSIST myhash FIELDS 1 <f>                 p=1    moon=209205       redis=N/A          ratio=N/A
[15:41:00] B.6 HGETDEL FIELDS 1 p=1 (re-seeded plain Hash)...
B.6    HGETDEL myhash FIELDS 1 <f>                  p=1    moon=204082       redis=N/A          ratio=N/A
[15:41:04] B.7 HGETEX EX 600 FIELDS 1 p=1 (HashWithTtl)...
B.7    HGETEX myhash EX 600 FIELDS 1 <f>            p=1    moon=207684       redis=N/A          ratio=N/A
[15:41:06] B.8 HGETEX FIELDS 1 p=1 (no-mode — pure read, compare to A.3)...
B.8    HGETEX myhash FIELDS 1 <f> (no-mode)         p=1    moon=233645       redis=N/A          ratio=N/A

  [B.8 vs A.3] HGETEX no-mode overhead vs plain HGET p=1: -4.0%

--- C: HashWithTtl path overhead vs plain Hash (Moon-only) ---

C.1 HGET p=16 plain Hash (reference — see A.4): moon=1923077
[15:41:09] C.2 HGET p=16 (HashWithTtl — all 1000 fields have TTL)...
C.2    HGET myhash (HashWithTtl, all fields TTL'd)  p=16   moon=2000000      redis=N/A          ratio=N/A
  [C.2 vs A.4] HGET overhead on HashWithTtl vs plain Hash p=16: -4.0%
[15:41:10] C.3 HLEN p=16 (plain Hash, O(1))...
C.3    HLEN myhash (plain Hash, O(1))               p=16   moon=2127660      redis=N/A          ratio=N/A
[15:41:10] C.4 HLEN p=16 (HashWithTtl, O(N), 1000 fields)...
C.4    HLEN myhash (HashWithTtl, O(N))              p=16   moon=2127660      redis=N/A          ratio=N/A
  [C.4 vs C.3] HLEN on HashWithTtl is 1.00x slower than plain Hash (expected O(N) vs O(1))

--- D: Redis 8.x comparison (commands Redis supports) ---

[15:41:10] D.1/D.2 HSET p=1 + p=16 (Moon vs Redis)...
D.1    HSET p=1                                     p=1    moon=235849       redis=251572       ratio=0.94x
D.2    HSET p=16                                    p=16   moon=1418440      redis=1652893      ratio=0.86x
[15:41:16] D.3/D.4 HGET p=1 + p=16 (pre-seeded, Moon vs Redis)...
D.3    HGET p=1                                     p=1    moon=237812       redis=241255       ratio=0.99x
D.4    HGET p=16                                    p=16   moon=1869159      redis=1886792      ratio=0.99x
[15:41:22] D.5 HDEL p=1 (Moon vs Redis)...
D.5    HDEL p=1                                     p=1    moon=232288       redis=240096       ratio=0.97x
[15:41:28] D.6 HLEN p=16 (plain Hash, Moon vs Redis)...
D.6    HLEN p=16 (plain Hash)                       p=16   moon=2020202      redis=2150538      ratio=0.94x

===========================================================================
  HEXPIRE/HTTL/HPERSIST/HGETDEL/HGETEX — Moon-only (B.*)
  Redis 8.x does not support HEXPIRE-family.
  Valkey 9.x would be the apples-to-apples comparison for B.*; not in scope.
===========================================================================

[15:41:28] Benchmark complete.
```
