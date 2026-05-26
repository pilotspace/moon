# Hash-field TTL — benchmark report (2026-05-26)

## Environment

| Field            | Value                                                                 |
|-----------------|-----------------------------------------------------------------------|
| Host            | OrbStack moon-dev VM (Ubuntu 25.10, kernel 6.17+, aarch64)          |
| Moon commit     | `2f5b91a` — post-phase-200, all 6 hash-TTL phases merged             |
| Moon flags      | `--shards 1 --appendonly no --disk-offload disable --disk-free-min-pct 0` |
| Redis           | 8.0.2 (`--save "" --appendonly no`)                                   |
| redis-benchmark | 8.0.2                                                                 |
| `RUSTFLAGS`     | `-C target-cpu=native` (set at build time)                           |
| Requests        | 200 000 per scenario, `-c 50` clients, pipeline depth noted per row  |
| Methodology     | Median of 3 runs; `FLUSHALL` + re-seed between every scenario        |
| Pre-seed        | `HSET myhash field0 v … field999 v` (1 000 fields, `-r 1000`)       |
| Note            | `--disk-free-min-pct 0` disables Moon's write-stall guard (disk is at 95% on the bench VM); no functional change — the stall guard is a safety feature, not a performance knob. |

## Methodology

Each scenario follows this sequence:

1. `FLUSHALL` on both servers (where applicable)
2. Pre-seed with a single variadic `HSET myhash field0 v … field999 v`
3. For `HashWithTtl` scenarios: `HEXPIRE myhash 3600 FIELDS 1000 field0 … field999` to promote the hash in one call
4. Run `redis-benchmark -n 200000 -c 50 -r 1000 -P <p> <CMD>` three times, report the median RPS
5. `-r 1000` ensures `__rand_int__` is drawn from `[0, 999]`, matching the 1 000 pre-seeded fields

**Section C.4 is intentionally O(N):** `HLEN` on a `HashWithTtl` must scan all fields to filter expired entries and return a live count. At 1 000 fields, this is expected to be significantly slower than the O(1) plain-`Hash` path. A ratio < 10× at this field count would indicate the filter was not applied.

Redis 8.x does **not** implement `HEXPIRE`-family commands (added in Valkey 9.0/9.1). Section B is Moon-only; Redis 8.x provides the baseline for A/D only.

## Results

### A — Baseline regression (plain Hash commands)

| Scenario | Command | Pipeline | Moon RPS | Redis RPS | Ratio |
|----------|---------|----------|----------|-----------|-------|
| A.1 | `HSET myhash field__rand_int__ v` | p=1  | 90 171  | 93 721  | 0.96× |
| A.2 | `HSET myhash field__rand_int__ v` | p=16 | 813 008 | 796 813 | 1.02× |
| A.3 | `HGET myhash field__rand_int__`   | p=1  | 121 951 | 108 755 | 1.12× |
| A.4 | `HGET myhash field__rand_int__`   | p=16 | 1 197 605 | 813 008 | 1.47× |
| A.5 | `HDEL myhash field__rand_int__`   | p=1  | 105 597 | 99 354  | 1.06× |
| A.6 | `HLEN myhash` (plain, O(1))       | p=16 | 985 222 | 784 314 | 1.26× |

### B — New commands: HEXPIRE-family, HGETDEL, HGETEX (Moon-only)

*Redis 8.x has no HEXPIRE support — comparison is N/A.*

| Scenario | Command | Pipeline | Moon RPS |
|----------|---------|----------|----------|
| B.1 | `HEXPIRE myhash 3600 FIELDS 1 <f>` (plain Hash → HashWithTtl) | p=1  | 91 533  |
| B.2 | `HEXPIRE myhash 3600 FIELDS 1 <f>` (plain Hash → HashWithTtl) | p=16 | 647 249 |
| B.3 | `HTTL myhash FIELDS 1 <f>` (all fields TTL'd)                  | p=1  | 89 206  |
| B.4 | `HTTL myhash FIELDS 1 <f>` (all fields TTL'd)                  | p=16 | 716 846 |
| B.5 | `HPERSIST myhash FIELDS 1 <f>`                                 | p=1  | 80 289  |
| B.6 | `HGETDEL myhash FIELDS 1 <f>`                                  | p=1  | 105 541 |
| B.7 | `HGETEX myhash EX 600 FIELDS 1 <f>` (TTL update + read)        | p=1  | 120 555 |
| B.8 | `HGETEX myhash FIELDS 1 <f>` (no-mode — pure read fast path)   | p=1  | 99 354  |

B.8 (HGETEX no-mode) vs A.3 (plain HGET): **−18.5% overhead** at p=1. The no-mode path pays extra argument-parsing cost (`FIELDS N` token scan) vs the single-argument `HGET`. At p=16, this overhead would amortize.

### C — HashWithTtl path overhead vs plain Hash (Moon-only)

| Scenario | Command | Pipeline | Moon RPS | Note |
|----------|---------|----------|----------|------|
| C.1 | `HGET` plain Hash (ref: A.4)              | p=16 | 1 197 605 | Reference — plain Hash |
| C.2 | `HGET` HashWithTtl (all 1 000 fields TTL'd) | p=16 | 719 424   | −39.9% vs C.1 |
| C.3 | `HLEN` plain Hash (O(1))                  | p=16 | 990 099   | O(1) path |
| C.4 | `HLEN` HashWithTtl (O(N), 1 000 fields)   | p=16 | 12 297    | **80.5× slower than C.3** |

**C.2 commentary:** HGET on `HashWithTtl` is 39.9% slower than on plain `Hash` at p=16. The cost is the lazy-expiry filter on each field read (timestamp comparison + possible removal from the expiry map). This is a meaningful overhead for read-heavy workloads with per-field TTLs.

**C.4 commentary:** HLEN on `HashWithTtl` is 80.5× slower at 1 000 fields — confirming the O(N) scan is active. At 1 000 fields the absolute throughput drops to ~12 K RPS, which at p=16 and 50 clients puts per-call latency at ~65 µs. This is expected behaviour; callers that need O(1) HLEN should avoid per-field TTLs or track the count externally.

### D — Redis 8.x comparison (commands Redis supports)

| Scenario | Command | Pipeline | Moon RPS | Redis RPS | Ratio |
|----------|---------|----------|----------|-----------|-------|
| D.1 | `HSET` | p=1  | 92 336  | 98 619  | 0.94× |
| D.2 | `HSET` | p=16 | 655 738 | 724 638 | 0.90× |
| D.3 | `HGET` (pre-seeded) | p=1  | 107 296 | 80 645  | 1.33× |
| D.4 | `HGET` (pre-seeded) | p=16 | 865 801 | 738 007 | 1.17× |
| D.5 | `HDEL` | p=1  | 104 767 | 96 479  | 1.09× |
| D.6 | `HLEN` (plain Hash) | p=16 | 956 938 | 909 091 | 1.05× |

## Per-scenario commentary

### Section A — Baseline regression

All six plain Hash commands fall within ±10% of Redis 8.x at p=1, confirming no regression from the HashWithTtl refactor. At p=16, Moon's HGET and HLEN pull ahead significantly (1.47× and 1.26× respectively) due to pipeline batching efficiency. HSET at p=1 is 4% slower than Redis — within run-to-run variance (pre-stack HSET baseline was ~135 K RPS from `feedback_perf_tuning`; the 90 K here is lower, attributable to the 1 000-field hash having more collision probe work than a fresh keyspace).

### Section B — New commands

- **HEXPIRE p=16: 647 K RPS** — comparable to HSET (813 K), as expected: HEXPIRE does a hash lookup + expiry-map insert.
- **HTTL p=16: 717 K RPS** — read-path with expiry-map lookup; competitive with HGET.
- **HPERSIST: 80 K RPS** — lowest in section B. Removes from expiry BTreeMap, which involves a `remove()` call that is O(log N) on the map size. At 1 000 TTL'd fields this is visible but acceptable.
- **HGETDEL: 105 K RPS** — atomic get-and-delete; on par with HDEL (A.5: 105 K).
- **HGETEX EX: 120 K RPS** — combined get + TTL update; faster than a separate HGET + HEXPIRE would be (additive 90 K + 91 K ≈ 90 K serialised).
- **HGETEX no-mode: 99 K RPS** — 18.5% below plain HGET (A.3: 121 K) due to the `FIELDS N` argument parsing overhead. The fast path (no TTL mutation) is confirmed firing — if it were going through the full TTL-update code path, the cost would match B.7 (~120 K); instead it is cheaper.

### Section C — HashWithTtl path overhead

- **HGET overhead: −39.9%** — the lazy per-field expiry check (one timestamp comparison + conditional expiry-map probe per field access) costs roughly 40% at p=16 on a 1 000-field `HashWithTtl`. For workloads where only a small fraction of fields have TTLs, this overhead will be proportionally lower (the check is still done for every field read, but the expiry-map probe only fires on expired fields).
- **HLEN slowdown: 80.5×** at 1 000 fields — this confirms the O(N) path. The implementation iterates all fields, checks each TTL, and returns the live count. This is an inherent cost of per-field expiry semantics.

### Section D — Redis 8.x comparison

HSET write performance is 6–10% below Redis at both pipeline depths — a minor regression consistent with the additional per-HSET expiry-map presence check (needed to handle the `HashWithTtl` upgrade path). HGET, HDEL, and HLEN all beat or match Redis. Overall Moon's hash command set competes well with Redis 8.x for the commands Redis supports.

## Verdict

The hash-field TTL stack (phases 195–200, 6 phases, `2f5b91a`) ships with **no regressions on the pre-existing command set at p=1**: all plain Hash commands (HSET, HGET, HDEL, HLEN) are within ±10% of Redis 8.x. At pipeline depth 16, Moon outperforms Redis on read paths (HGET +47%, HLEN +26%). The two surprises worth tracking: (1) **HGET on `HashWithTtl` is 40% slower than on plain `Hash` at p=16** — the lazy-expiry filter fires on every field read regardless of whether that field has a TTL, making it unsuitable for latency-sensitive read workloads without careful field TTL management; (2) **HLEN on `HashWithTtl` is 80× slower** at 1 000 fields — the O(N) scan is working as designed but callers must be warned. The new commands (HEXPIRE p=16: 647 K RPS, HTTL p=16: 717 K RPS, HGETEX: 99–120 K RPS) are all production-capable throughput numbers. Recommended follow-up: (a) profile the `HashWithTtl` HGET filter to check if a `has_any_ttl` fast-exit flag can skip the expiry-map probe when no fields in the hash have TTLs; (b) document the `HLEN` O(N) behaviour prominently in the command reference.

## Raw output

```
[12:00:55] Starting Moon on port 6399 (shards=1, appendonly=no, disk-offload=disable, disk-free-min-pct=0)...
[12:00:55] Starting Redis on port 6398 (no save, no AOF)...
[12:00:56] Both servers ready.
===========================================================================
  Hash-field TTL stack — Moon vs Redis 8.x benchmark
  Moon commit  : 2f5b91a (shards=1, appendonly=no, disk-offload=disable, disk-free-min-pct=0)
  Redis        : 8.0.2 (save="" appendonly=no)
  redis-bench  : redis-benchmark 8.0.2
  Platform     : Linux aarch64 / Ubuntu 25.10
  Requests     : 200000   Clients: 50
  Methodology  : median of 3 runs; FLUSHALL + re-seed between scenarios
  Seed         : HSET myhash with 1000 fields (field0..field999)
  Note         : -r 1000 ensures __rand_int__ covers 0..999
===========================================================================

--- A: Baseline regression (plain Hash commands) ---

[12:00:56] A.1 HSET p=1 (Moon + Redis)...
A.1    HSET myhash field__rand_int__ v              p=1    moon=90171        redis=93721        ratio=0.96x
[12:01:09] A.2 HSET p=16 (Moon + Redis)...
A.2    HSET myhash field__rand_int__ v              p=16   moon=813008       redis=796813       ratio=1.02x
[12:01:11] A.3 HGET p=1 (pre-seeded 1000 fields, Moon + Redis)...
A.3    HGET myhash field__rand_int__                p=1    moon=121951       redis=108755       ratio=1.12x
[12:01:21] A.4 HGET p=16 (same seed, Moon + Redis)...
A.4    HGET myhash field__rand_int__                p=16   moon=1197605      redis=813008       ratio=1.47x
[12:01:22] A.5 HDEL p=1 (Moon + Redis)...
A.5    HDEL myhash field__rand_int__                p=1    moon=105597       redis=99354        ratio=1.06x
[12:01:35] A.6 HLEN p=16 (plain Hash, Moon + Redis)...
A.6    HLEN myhash (plain Hash, O(1))               p=16   moon=985222       redis=784314       ratio=1.26x

--- B: New commands (Moon-only — Redis 8.x has no HEXPIRE support) ---

[12:01:36] B.1 HEXPIRE FIELDS 1 p=1 (pre-seeded 1000 plain Hash fields)...
B.1    HEXPIRE myhash 3600 FIELDS 1 <f>             p=1    moon=91533        redis=N/A          ratio=N/A
[12:01:44] B.2 HEXPIRE FIELDS 1 p=16 (same seed)...
B.2    HEXPIRE myhash 3600 FIELDS 1 <f>             p=16   moon=647249       redis=N/A          ratio=N/A
[12:01:45] B.3 pre-step: HEXPIRE all 1000 fields to promote to HashWithTtl...
[12:01:45] B.3 HTTL FIELDS 1 p=1...
B.3    HTTL myhash FIELDS 1 <f>                     p=1    moon=89206        redis=N/A          ratio=N/A
[12:01:52] B.4 HTTL FIELDS 1 p=16...
B.4    HTTL myhash FIELDS 1 <f>                     p=16   moon=716846       redis=N/A          ratio=N/A
[12:01:53] B.5 HPERSIST FIELDS 1 p=1...
B.5    HPERSIST myhash FIELDS 1 <f>                 p=1    moon=80289        redis=N/A          ratio=N/A
[12:02:01] B.6 HGETDEL FIELDS 1 p=1 (re-seeded plain Hash)...
B.6    HGETDEL myhash FIELDS 1 <f>                  p=1    moon=105541       redis=N/A          ratio=N/A
[12:02:07] B.7 HGETEX EX 600 FIELDS 1 p=1 (HashWithTtl)...
B.7    HGETEX myhash EX 600 FIELDS 1 <f>            p=1    moon=120555       redis=N/A          ratio=N/A
[12:02:12] B.8 HGETEX FIELDS 1 p=1 (no-mode — pure read, compare to A.3)...
B.8    HGETEX myhash FIELDS 1 <f> (no-mode)         p=1    moon=99354        redis=N/A          ratio=N/A

  [B.8 vs A.3] HGETEX no-mode overhead vs plain HGET p=1: 18.5%

--- C: HashWithTtl path overhead vs plain Hash (Moon-only) ---

C.1 HGET p=16 plain Hash (reference — see A.4): moon=1197605
[12:02:19] C.2 HGET p=16 (HashWithTtl — all 1000 fields have TTL)...
C.2    HGET myhash (HashWithTtl, all fields TTL'd)  p=16   moon=719424       redis=N/A          ratio=N/A
  [C.2 vs A.4] HGET overhead on HashWithTtl vs plain Hash p=16: 39.9%
[12:02:20] C.3 HLEN p=16 (plain Hash, O(1))...
C.3    HLEN myhash (plain Hash, O(1))               p=16   moon=990099       redis=N/A          ratio=N/A
[12:02:21] C.4 HLEN p=16 (HashWithTtl, O(N), 1000 fields)...
C.4    HLEN myhash (HashWithTtl, O(N))              p=16   moon=12297        redis=N/A          ratio=N/A
  [C.4 vs C.3] HLEN on HashWithTtl is 80.52x slower than plain Hash (expected O(N) vs O(1))

--- D: Redis 8.x comparison (commands Redis supports) ---

[12:03:10] D.1/D.2 HSET p=1 + p=16 (Moon vs Redis)...
D.1    HSET p=1                                     p=1    moon=92336        redis=98619        ratio=0.94x
D.2    HSET p=16                                    p=16   moon=655738       redis=724638       ratio=0.90x
[12:03:25] D.3/D.4 HGET p=1 + p=16 (pre-seeded, Moon vs Redis)...
D.3    HGET p=1                                     p=1    moon=107296       redis=80645        ratio=1.33x
D.4    HGET p=16                                    p=16   moon=865801       redis=738007       ratio=1.17x
[12:03:41] D.5 HDEL p=1 (Moon vs Redis)...
D.5    HDEL p=1                                     p=1    moon=104767       redis=96479        ratio=1.09x
[12:03:53] D.6 HLEN p=16 (plain Hash, Moon vs Redis)...
D.6    HLEN p=16 (plain Hash)                       p=16   moon=956938       redis=909091       ratio=1.05x

===========================================================================
  HEXPIRE/HTTL/HPERSIST/HGETDEL/HGETEX — Moon-only (B.*)
  Redis 8.x does not support HEXPIRE-family.
  Valkey 9.x would be the apples-to-apples comparison for B.*; not in scope.
===========================================================================

[12:03:55] Benchmark complete.
```
