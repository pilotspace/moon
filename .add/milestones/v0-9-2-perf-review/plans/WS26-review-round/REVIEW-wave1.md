# Wave-1 adversarial review: `ce65400..f7f1d96` (WS27, WS28, WS29)

Reviewer worktree `/home/user/wt/review5`, branch `review/wave1`. All results come from a
**4-vCPU Linux container, not the merge bar**. The box is shared with the orchestrator's gate.
Binaries: `/home/user/wt/bin/w1-{monoio,tokio}` (f7f1d96) and `base2-ce65400-{monoio,tokio}`.
Oracle: redis-server 7.0.15.

**Verdict: the goal (nothing above NIT) is not met.**
- 5 MAJOR findings:
  - 2 are defects in this wave's own fixes: a regression (F1) and an incomplete fix (F2).
  - 3 are pre-existing TXN atomicity/isolation holes that the WS27 durability claim does not cover (F3–F5). F5 becomes durable because of WS27.
- No BLOCKING finding.

## Verdict table

| commit / area | verdict | severity |
|---|---|---|
| `9d0a742` SSCAN position cursor (moon#1287) | **Regression**: skips members that are present for the whole scan when a `*STORE` rewrites the set (F1) | **MAJOR** |
| `d0a48cd` no-AOF write-gate tiering (moon#1290) | **Incomplete**: a routed `EVAL` at `--shards 4` still plain-drops acknowledged keys (F2) | **MAJOR** |
| `8a08d46` / `cc46445` TXN.ABORT KV compensation (moon#1285) | Correct under an AOF (fold, rewrite, replica full-sync, TTL, field TTL, SWAPDB/FLUSHDB all match live state after restart). Uncovered quadrants: no-AOF snapshot (F3), crash mid-TXN (F4), lost update made durable (F5) | **MAJOR** ×3 (pre-existing) |
| `8c50ec6` graph rollback WAL + 3 new records | Three records are forward-only, so an older replica or a downgrade cannot read them (documented; needs a maintainer decision) | MINOR |
| `1a52851` / `1ac8c5e` vector/text rebuild on RESTORE | Wired on all three connection paths, SPSC and replica apply; guard dropped before the hook | OK |
| `53b849f` fresh AOF generation head before manifest (moon#1293) | Crash windows and dir fsyncs verified by reading the code. Suite green: both runtimes × s1/s4 | OK (+ documented tokio s1 torn-head residual: MINOR) |
| `06f5f57` graves across a smaller `--databases` / failed commit (moon#1291) | Trailer is keyed by file id, so no collision. Suite green on both runtimes × s1/s4. Unit tests go red with the fix reverted | OK |
| `94296bb` 1024-entry / 1 MiB durable batch | Maintainer decision. Hot path unaffected (A/B below) | OK |
| `0cb0879` one reason-DEL bound per eviction run (moon#1294) | Every connection gate carries it. The monoio integration merge carries both `with_manifest` and `&mut aof_budget`. The per-write bound in a pipeline remains (measured) | MINOR (documented residual) |
| `73f37e0` … `89f7f4a` adaptive active expiry (moon#1288) | Correct: replica gate, AOF headroom, latch. 60× faster drain, p99 +0.2 ms during a drain | OK |
| CI wiring (`cd7a976`, `6255ee9`) | The WS29 real-server guards `eviction_reason_del_run_budget_1294` and `active_expiry_backlog_drain_1288` are `#[ignore]`d and no workflow runs them | MINOR |
| Rules: hot-path allocation, unsafe, locks, unwrap, file caps | No new unsafe, no `std::sync` locks, no library unwrap/expect. `scripting/bridge.rs` crosses 1500 lines (1492→1502). `event_loop.rs` grew +53 past the cap | NIT |

## Findings

### F1 MAJOR (regression, moon#1287): SSCAN skips members across a same-membership `*STORE` rewrite

**Mechanism.** `sscan_positions` (`src/command/set/set_read.rs:535`) pages the `IndexSet` by position. It is sound only while nothing moves a member *up*. The `*STORE` family breaks that: it replaces the destination with a fresh set in the algebra's iteration order.
- `union` collects into a `std::collections::HashSet` (`set_algebra.rs:174`), so its order is random.
- `intersect` walks the smallest input (`set_algebra.rs:146`).

A `SUNIONSTORE s s …` (bulk add) or `SINTERSTORE s filter s` (bulk filter) between two SSCAN calls reshuffles every position.

redis keeps the guarantee here, because its cursor is a hash-bucket index. moon did too before #1287: its cursor was a rank in a sorted snapshot, and the same members give the same ranks.

**Test:** `src/command/set/sscan_rewrite_review_tests.rs` (commit `78287fa`). Result: 100 of 2000 members present for the whole scan were never returned, after both SUNIONSTORE and SINTERSTORE.
- Red on f7f1d96.
- Green with only `set_read.rs` reverted to ce65400.

```
cargo test --lib sscan_rewrite_review
```

**Fix direction:** stamp the set with a rebuild generation and encode it in the cursor. On a mismatch, fall back to the old sorted-rank page. Alternatively, keep the old order when a `*STORE` destination is also a source.

### F2 MAJOR (incomplete fix, moon#1290): a routed EVAL still plain-drops no-AOF eviction victims

**Mechanism.**
- The script bridge (`src/scripting/bridge.rs:258`) borrows the manifest through `manifest_cell::with_manifest`. That answers `None` while the event loop holds the cell.
- The loop holds the cell for the whole SPSC drain (`event_loop.rs:1377/1529/2345`: `&mut shard_manifest.borrow_mut()` passed to `drain_spsc_shared`).
- An `EVAL` on another shard's key runs inside that drain (`ShardMessage::Execute` + `script_acl`). Its eviction gate therefore takes the plain-drop path.

WS28 SUMMARY residual 5 names this path but says it was "not seen in the repro". Here it happens on every run.

Measured with 16,000 × 600 B writes, 8 MB `allkeys-lru`, `--appendonly no --disk-offload enable`, `--shards 4`:

| binary | SET | EVAL SET | MSET / MULTI |
|---|---|---|---|
| f7f1d96 monoio | evicted 0, DBSIZE 16000 | **evicted 1793–2127, DBSIZE 13873–14207** | evicted 0 |
| f7f1d96 tokio | evicted 0 | **evicted 1398–2187** | evicted 0 |
| ce65400 monoio | — | evicted 4057 | — |

At `--shards 1` (the script runs on the connection's own shard) nothing is lost.

**Test:** `tests/review_w1_routed_eval_tiering_1290.rs` (commit `4dc40fe`).
- Red on w1-monoio and w1-tokio.
- Green control: `MOON_TEST_COLD_DEL_SHARDS=1`.

```
MOON_DISK_FREE_MIN_PCT=0 MOON_BIN=/home/user/wt/bin/w1-monoio cargo test --test review_w1_routed_eval_tiering_1290 -- --include-ignored --test-threads 1 --nocapture
```

**Fix direction:** pass the drain's `shard_manifest.as_mut()` into the routed script's `LuaEvictionCtx`, the way `spsc_eviction_gate` already receives it.

### F3 MAJOR (pre-existing, uncovered moon#1285 quadrant): without an AOF, a mid-TXN snapshot resurrects an aborted TXN

**Scenario.** `SET k original`, then:
1. `TXN BEGIN`, `SET k aborted`, `SET new inserted`.
2. `BGSAVE` from another connection; it completes.
3. `TXN ABORT` answers `+OK`, and a live `GET k` answers `original`.
4. `kill -9`, then restart: `GET k` answers `aborted`, `GET new` answers `inserted`.

**Mechanism.** The snapshot holds the uncommitted writes. WS27's `snapshot::txn_abort_tests` asserts this by design (point-in-time, needed for the AOF fold). With `--appendonly no` the compensating records have no log, and the snapshot is the durability authority. Autosave (`--save`) hits the same window.

**Test:** `tests/review_w1_txn_abort_no_aof_snapshot_1285.rs::an_abort_after_a_mid_txn_snapshot_survives_a_restart_without_an_aof` (commit `3029f72`). Red on f7f1d96 monoio, f7f1d96 tokio and ce65400 monoio.

### F4 MAJOR (pre-existing, design): kill -9 inside a TXN keeps its uncommitted writes

**Scenario.** AOF on, `appendfsync always`: `SET k original`, `TXN BEGIN`, `SET k uncommitted`, `SET new uncommitted`, `kill -9`. After recovery, `GET k` answers `uncommitted`.

**Mechanism.** TXN writes reach the AOF as they run, with no transaction markers, and recovery rolls nothing back. A crash is an implicit abort that is never applied.

**Test:** same file, `a_crash_inside_a_txn_does_not_keep_its_uncommitted_writes` (commit `42ecaa6`). Red on f7f1d96 monoio/tokio and ce65400 tokio.

### F5 MAJOR (pre-existing live; durable since WS27): TXN.ABORT overwrites another client's acknowledged write

**Scenario.**
1. `SET k orig`.
2. Client A: `TXN BEGIN`, `SET k txn`.
3. Client B: `SET k other-client` answers `+OK` (no write-write conflict with A's intent).
4. A: `TXN ABORT`. Now `GET k` answers `orig`.

**Before and after WS27.** On ce65400, the restart replayed B's SET and brought it back, and replicas kept it. Since WS27, the absolute `RESTORE … REPLACE` makes the lost update durable and replicated.

**Siblings (script `t_swapdb_txn.py`, both runtimes; live and restart agree):**
- **FLUSHDB** by another client while A's TXN is open: the abort resurrects A's keys.
- **SWAPDB 0 1**: the abort restores into slot 0, which clobbers the key that moved there from db 1. The aborted value then survives in db 1.

**Test:** same file, `an_abort_does_not_overwrite_another_clients_acknowledged_write` (commit `00463f5`). Results:
- f7f1d96 monoio/tokio: live `orig`, restart `orig`.
- ce65400 monoio: live `orig`, restart `other-client`.

**Needs a maintainer decision:** write-write conflict detection on keys under a TXN intent, versus documented first-writer-loses semantics.

### MINOR

1. **Per-command reason-DEL bound (moon#1294 residual 4, measured).** Setup: `--shards 1`, AOF held by `MOON_TEST_AOF_FSYNC_STALL_MS`, one pipelined burst of 16 × 64 KB evicting SETs. Result: the pipeline took 7.16 s, 323 victims were evicted, and all 323 reason-DELs were dropped. The max PING gap on another connection was 1.05 s. N=1 costs nothing. The SPSC `MultiExecute` gate mints one bound per sub-command too (`spsc_handler.rs:1287`).
2. **CI gap.** `tests/eviction_reason_del_run_budget_1294.rs` and `tests/active_expiry_backlog_drain_1288.rs` are `#[ignore]`d and absent from every workflow and from `ci-local.sh`, while WS27 and WS28 wired theirs. Note that the tests I added are `#[ignore]` as well.
3. **Graph WAL records** `GRAPH.DELPROP`, `GRAPH.UNDELETENODE` and `GRAPH.UNDELETEEDGE` are new record semantics. An older replica or a downgraded binary cannot parse them. This is a compatibility decision (storage-durability escalation rule).
4. **Tokio `--shards 1` legacy file.** A torn `MOON.COLDCUT` head is never re-seeded, because it is only written to an empty file. This is WS28 residual 1: a boot-time window on one quadrant.
5. **Refused compensation discarded.** The dirty-`TXN COMMIT` rollback and the disconnect abort discard an AOF refusal of the compensating records (`let _ =`; logged only). The master's disk then lacks records its replicas already got.

### NIT

- `scripting/bridge.rs` crosses the 1500-line cap (1492→1502). `shard/event_loop.rs` grew +53 over the cap.
- Every TXN.ABORT restore of a hash with field TTLs logs the misleading WARN "Redis-compat RDB drops per-field TTLs" (`redis_rdb.rs:332`, via `dump_payload::encode`). The `HPEXPIREAT` records do restore the TTLs.
- SSCAN on an intset returns members in lexicographic order (`1 10 2 3`); redis returns numeric order. `SSCAN s -1` answers `ERR invalid cursor`; redis 7.0.15 accepts it. Both are unchanged from ce65400.
- `set_read.rs` `sscan_positions` uses `member.clone()` (a `Bytes` refcount bump) in `src/command`. This is the pre-existing pattern and does not allocate.

## Verified OK (what I tried)

- **TXN.ABORT with AOF, real server** (`t_aof_txn.py`). Covered monoio s1/s4 and tokio s1, with no rewrite, a `BGREWRITEAOF` mid-TXN, and one after the abort. Every case was right after kill -9:
  - key TTL;
  - a per-field hash TTL (`HPEXPIRETIME` exact);
  - DEL of a list;
  - insert;
  - a key whose original deadline passed before the restart (`RESTORE ABSTTL` of a past deadline: absent).
- **Replica full-sync during an open TXN, then abort:** the replica converges, field TTL included.
- **MOVE/COPY DB inside a TXN:** refused, so the per-record db cannot mis-restore.
- **`manifest_cell`:** no event-loop `borrow_mut` spans an `.await`, and `with_manifest` is synchronous. I found no re-entrant `borrow_mut` panic path; the only held-cell caller is F2.
- **Tokio `tiering_no_aof_write_gate_1290`:** s1 and s4 green, 0 evicted.
- **Monoio and tokio at s1 and s4:** MULTI, MSET and EVAL at s1 lose 0.
- **WS28 suites on both runtimes × s1/s4, all green:** `crash_aof_init_generation_1293` and `cold_graves_reduced_databases_1291`.
- **Revert checks:** each of these goes red with only its fix reverted and green on f7f1d96:
  - F9 (`commit_failure_tests`);
  - N7 (`aof_routing_tests`);
  - the moon#1288 fast cycle (`active_expiry_fast_tests` ×3);
  - the move-capture (`txn_abort_tests::abort_after_the_epoch_instant_keeps_the_image_point_in_time`).
- **Hot-path A/B:** 3 interleaved reps, `--shards 1`, `redis-benchmark -t set,get -n 1M -P 16 -c 50 -r 100000`, base vs w1.
  - SET: base 1.29 / 1.10 / 1.30 M rps, w1 1.31 / 1.58 / 1.37 M rps.
  - GET: base 1.48 / 1.62 / 1.55 M rps, w1 1.52 / 1.70 / 1.56 M rps.
  - Within noise: no regression.
- **Adaptive expiry A/B:** 3 reps, 600K keys `PX 4000`, SIGSTOP 4.5 s.
  - Drain time: base 70.4–74.8 s, w1 1.16–2.20 s.
  - GET p99 during the drain: base 0.26–0.33 ms, w1 0.50–0.52 ms.
  - Max latency is noise-dominated: a no-backlog control shows 10–21 ms on both.

## Not checked

- Graph rollback edge cases beyond reading the code: CSR-resident nodes, cross-epoch DROP/CREATE, multi-shard graph legs. I did not run the fuzz target (no cargo-fuzz here).
- An MQ durable stream DEL'd inside a TXN and then aborted (the MQ WAL plane after a RESTORE).
- Power-loss (not SIGKILL) fsync ordering. I verified it by reading only.
- The macOS, Windows and MSRV legs. My new test files pass `cargo fmt` and `cargo clippy --lib --tests --test review_w1_* -- -D warnings` (clean, default features only).
- The F8 analogue where the orphan sweep unlinks an unattached db's files (pre-existing "reduced `--databases`" problem).
- `redis-cli` wire-byte comparisons beyond the SSCAN edge table.

## Tests added (branch `review/wave1`)

| commit | test | red | green |
|---|---|---|---|
| `78287fa` | `src/command/set/sscan_rewrite_review_tests.rs` (2) | f7f1d96 | ce65400 `set_read.rs` |
| `4dc40fe` | `tests/review_w1_routed_eval_tiering_1290.rs` | f7f1d96 monoio + tokio, s4 | s1 control |
| `3029f72` | `review_w1_txn_abort_no_aof_snapshot_1285.rs::an_abort_after_a_mid_txn_snapshot…` | f7f1d96 ×2, ce65400 | — (pre-existing) |
| `42ecaa6` | same file, `a_crash_inside_a_txn…` | f7f1d96 ×2, ce65400 tokio | — (pre-existing) |
| `00463f5` | same file, `an_abort_does_not_overwrite…` | f7f1d96 ×2, ce65400 | — (pre-existing live) |

## Self-evaluation (0–1)

Completeness 0.9 · Clarity 0.9 · Practicality 0.92 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9

Every hunt item was addressed. The gaps listed under "Not checked" cannot be closed in this container: there is no cargo-fuzz, no Mac or Windows host, and no power-loss rig.
