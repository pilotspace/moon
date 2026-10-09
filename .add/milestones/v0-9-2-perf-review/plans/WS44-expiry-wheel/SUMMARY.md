# WS44 SUMMARY (moon#1298, evaluate only)

Branch `w2/ws44-expiry-wheel` in `/home/user/wt/lane-c`, base `28feb33`. All prototype commits are default OFF (`MOON_EXPIRY_WHEEL=1` turns the wheel on). Nothing was pushed or merged.

**Verdict: NOT ADOPTED per the gate.** The memory win is large. SET … EX is faster. The drain rate, which the plan names as a gate metric, regresses. Close #1298 as "not adopted". A conditional path is under Follow-ups.

## Per-issue verdict

| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| #1298 | NOT ADOPTED. Prototype stays on the branch, default OFF, droppable. | `9c98987` wheel plus the design note in its body; `082eb92` boxed `Many` bucket; `4f0c1b2` / `b339241` / `6a9273e` / `cd30f5a` resolve cost work, a bench and a callgrind probe; `f7b1fc0` real-server volatile-ttl test; `89c9d84` clippy allow; `2705dbd` bench tooling | memory −29% / −34% of a TTL key, drain −13% to −25% (tables below) | see below |

The unit tests ran on the lib suite with the wheel OFF and ON (see Gates). They cover the wheel against the sorted set under random ops, the volatile-ttl head, active expiry, TXN-held keys, ghost references, rebuild after bulk load, clear (FLUSHDB), and the SWAR masks against a naive walk.

## Measurements

Method: a `--features jemalloc-stats` build, `--shards 1`, a fresh server per row, 1M keys, `SET k:%07d` / `user:session:token:%010d` with an 8-byte value. The figure is allocator_allocated delta per key. Reps were identical to 0.1 B, so this is noise-free.

`used_memory` does NOT see the index (145.0 B and 165.0 B per key with or without TTL), so INFO `used_memory` cannot show this win.

| case | no TTL | sorted set | wheel | index B/key set → wheel | saving |
|---|---|---|---|---|---|
| dense (EX 3600), short key | 107.6 | 173.9 | 124.0 | 66.3 → 16.4 | −49.9 B (−28.7% of the TTL key) |
| dense, key >23 B | 138.8 | 237.9 | 156.1 | 99.1 → 17.3 | −81.8 B (−34.4%) |
| sparse (random TTL, 1–30 days), short | 107.6 | 160.0 | 155.3 | 52.4 → 47.7 | −4.7 B (−2.9%) |
| sparse, key >23 B | 138.8 | 224.0 | 187.3 | 85.2 → 48.5 | −36.7 B (−16.4%) |

- The first version, with an unboxed `BTreeSet` in the bucket enum, regressed sparse short keys to 179.1 B (+12%). Boxing the `Many` bucket fixed that.
- The wheel is a single level, so sparse long-lived TTLs get little benefit. A hierarchical wheel with coarse far levels and a cascade would be needed, and it is not built.

**SET … EX 3600, `-c 50 -r 1000000`, `--shards 1`, monoio release-fast.** The box was shared (load 3.2–5.3), 3 interleaved reps, final code. Server CPU is from `/proc`, which is more robust on a shared box than client rps.

| pipeline | wheel OFF | wheel ON |
|---|---|---|
| P16 rps | 263K / 234K / 241K | 302K / 257K / 260K (+8% to +15%, 3 of 3 reps) |
| P16 server µs/op | 3.62 / 3.95 / 4.03 | 3.20 / 2.90 / 3.67 (−9% to −27%) |
| P1 rps | 66.7K / 48.3K / 63.6K | 64.1K / 74.1K / 64.4K (noise, no signal) |

**Drain (moon#1288 harness `tests/active_expiry_backlog_drain_1288.rs`, 200K keys, monoio), 4 interleaved reps, final code:**

| wheel | keys/s per rep | mean |
|---|---|---|
| OFF | 348K / 348K / 320K / 384K | 350K (matches the 300–370K in the issue) |
| ON | 241K / 257K / 290K / 256K | 261K (**−25%**) |

- A python harness at 200K dense deadlines gave server CPU per key 1.02 µs OFF vs 1.33 µs ON (+30%, rate −13%).
- At 1M spread deadlines, v1 CPU per key was 1.54 µs vs 1.65 µs (+7%).

**In-process micro-bench** (release-fast, min of 5 interleaved reps, 1M keys, shared box so ±15%). Ratios of wheel to sorted set, short / long keys:

| cell | short | long |
|---|---|---|
| insert, dense | 0.78× | 0.76× |
| retarget (EXPIRE) | 0.75× | 0.67× |
| drain through the real sweep | 1.17× | 1.21× |
| volatile-ttl nearest + remove | 1.15× | 1.00× |

**callgrind, 100K due keys, instructions per key** (deterministic):

| cell | sorted set | wheel v1 | wheel final |
|---|---|---|---|
| drain, short | 957 | 2520 | 1690 |
| drain, long | 1279 | 2952 | 2122 |
| nearest + remove | 1136 | 2562 | 1731 |

Simulated LL misses per key were 4.2 for the sorted set vs 3.6 for the wheel. The drain gap is therefore compute and branchy resolution, not cache misses.

**Where the drain cost comes from.** The wheel stores a hash, not a key. Every drained or evicted key needs a resolve: SWAR control-byte masks, then `iter_occupied().nth`, then an xxh64 verify, then a key clone, then the usual `remove_if` probe. Safe code cannot fuse the resolve and the remove. `Segment` exposes no safe slot access, and a hash-addressed remove needs one new `unsafe` block, which CLAUDE.md and UNSAFE_POLICY forbid without approval.

## READY TO BENCH (quiet window)

The box had other lanes building for all of the above. Binaries:
- `/home/user/wt/bin/ws44-v4-monoio` and `/home/user/wt/bin/ws44-v4-tokio` are the final code, built with release-fast.
- `/home/user/wt/bin/ws44-wheel2-stats-monoio` is the jemalloc-stats build, from commit `082eb92`. Later commits do not change memory.
- `/home/user/wt/bin/ws44-base-stats-monoio` is the pristine base.

Scripts are in the repo at `.add/milestones/v0-9-2-perf-review/plans/WS44-expiry-wheel/bench/`:
```
B=/home/user/wt/bin/ws44-v4-monoio; S=.add/milestones/v0-9-2-perf-review/plans/WS44-expiry-wheel/bench
# memory, per case and switch value (use the jemalloc-stats binary)
for c in ttl ttl_long ttl_sparse ttl_sparse_long notl; do for w in 0 1; do python3 $S/memtest.py /home/user/wt/bin/ws44-wheel2-stats-monoio 7640 $c 1000000 MOON_EXPIRY_WHEEL=$w; done; done
# SET EX at P1 and P16, with server CPU per op
$S/ab_set2.sh 7643 5 2000000 1000000 off=$B:0 on=$B:1
# drain, 5 interleaved reps
for i in 1 2 3 4 5; do for w in 0 1; do MOON_EXPIRY_WHEEL=$w MOON_BIN=$B MOON_DISK_FREE_MIN_PCT=0 cargo test --test active_expiry_backlog_drain_1288 -- --include-ignored --nocapture 2>&1 | grep drained; done; done
```
Not measured: OFF-vs-pristine-base on a plain build. The only OFF-path change is an enum branch per index op. Bench `ws44-base-stats-monoio` against the stats build of `082eb92` with the switch OFF to confirm.

## Cross-ownership edits

- `src/storage/db/mod.rs`, `accessors.rs`, `bulk_load.rs`: the field type changed, plus `set_expiry_wheel` and `expiry_wheel_enabled`.
- `Database::peek_nearest_expiry` is now `&mut self`. Its only non-test caller, `find_victim_volatile_ttl`, already held `&mut`.
- `src/storage/dashtable/segment/mod.rs`: one line (`mod tag_masks;`). `tag_masks.rs` is new, safe code only.
- No new `unsafe`. No edits to CHANGELOG, README, TEAM-RULES, CLAUDE.md, `.add/state.json` or other plan dirs.

## Risks / things to re-check at integration

- **If dropping the prototype:** drop `9c98987`, `082eb92`, `4f0c1b2`, `b339241`, `6a9273e`, `cd30f5a`, `89c9d84` and `2705dbd`. Optional: keep `f7b1fc0` (`tests/expiry_wheel_volatile_ttl_1298.rs`). It is a generic real-server volatile-ttl nearest-first test and passes with the wheel OFF. Its wheel variant sets the env var, which is ignored without the code.
- **Ties:** same-millisecond deadlines order by hash, not key bytes. Nothing observable depends on it.
- **Collisions:** two keys with identical (ms, 56-bit hash) would share one reference, with probability ≈ 2⁻⁵⁴ per pair. The worst case is one key left unindexed, which lazy expiry covers.
- **Sparse TTLs:** the wheel is only a ~3% win for sparse short keys.
- **Wheel path unreachable by default:** with the switch OFF the wheel path is never taken.

## Gates

| gate | result |
|---|---|
| `cargo fmt --check` | OK |
| `cargo clippy --all-targets -- -D warnings`, default features | exit 0 |
| same, `--no-default-features --features runtime-tokio,jemalloc` | exit 0 |
| `cargo test --release --lib -- storage::db storage::eviction server::expiration storage::dashtable`, default features | 438 passed, switch OFF and ON |
| same, tokio feature set | 439 passed, OFF and ON |

Integration suites were run on both the monoio and tokio v4 binaries, with the switch OFF and ON, as `cargo test --test <name> -- --include-ignored --test-threads 1` with `MOON_BIN` pinned. The suites:
- `active_expiry_backlog_drain_1288`, `info_expired_keys_1286`, `expired_keys_parity_1286`
- `tracking_expiry_invalidation_1013`, `eviction_reason_del_run_budget_1294`, `cold_tier_observability`
- `expiry_wheel_volatile_ttl_1298`
- `txn_isolation_1299`, `review_r1_txn_isolation_1299`
- `replication_swapdb`, `replication_ttl_semantics`

Results:
- Every suite is green on monoio, OFF and ON.
- On tokio, only the known no-PSYNC replica tests fail, identically OFF and ON: `replication_swapdb` ×3 and `replication_ttl_semantics` ×2.
- One timing flake: `expired_keys_parity_1286::a_write_over_an_expired_key_counts_it_4_shards` failed once on tokio with the wheel OFF (a 30 ms PX / 32 ms sleep race under load). It then passed 3 of 3 reruns.

Not run: `cargo check --manifest-path fuzz/Cargo.toml --all-targets`. A grep of `fuzz/` for the touched methods found no callers.

## Follow-ups

- **Conditional path:** a hash-addressed fused remove in the DashTable would drop the resolve and the second probe. It needs one `unsafe` block and explicit approval. Estimate, not measured: drain at parity with the sorted set, giving the full −29% / −34% memory and the SET EX gain. Re-bench in a quiet window first.
- **Sparse long-TTL workloads** need coarse far levels and a cascade.
- **A cheap observability add:** an INFO field for the expiry-index bytes. `used_memory` hides it.

## Self-evaluation (0–1)

| axis | score | note |
|---|---|---|
| Completeness | 0.92 | memory, throughput, drain and volatile-ttl all measured, gates run. Server-level numbers are marked READY TO BENCH and the plain-build OFF-vs-base comparison is not done. |
| Clarity | 0.92 | |
| Practicality | 0.90 | the verdict follows the gate, and the conditional path is stated with its blocker. |
| Optimization | 0.85 | the drain gap is not closed. It dropped from 2520 to 1690 instructions per key, and the rest needs approval for unsafe code. |
| Edge cases | 0.90 | collisions, sparse, ghosts, TXN-held, clock-back, ties and tiny buckets covered. Cold TTL is covered only through `cold_tier_observability` because cold keys are never indexed. |
| Self-evaluation | 0.90 | |
