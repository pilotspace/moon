# WS30-wave1-review-fixes SUMMARY: wave-1 review F1, F2 and the MINOR/NIT follow-ups

Worktree `/home/user/wt/ws30`, branch `perf/ws30`, based on f7f1d96 plus the two review test commits. There are 6 commits, none pushed. Every result below comes from the **4-vCPU Linux container, not the merge bar**. The oracle is redis 7.0.15.

Binaries are `/home/user/wt/bin/ws30-monoio` and `/home/user/wt/bin/ws30-tokio` (release-fast, final tree). I checked each copy with `strings | grep 'NO client was told'`, a string only item 4 adds: 1 hit in each.

**Verdict: all six items are done. F1 and F2 went red → green on both runtimes, and the related suites and gates are green.** While fixing F2 I found two more leaks of the same kind (routed FCALL and a routed MULTI body holding a script). Both are fixed and tested.

## Verdict table
| item | verdict | commit | evidence |
|---|---|---|---|
| F1 SSCAN rewrite (moon#1287) + 2 NITs | FIXED | `fc24d21` fix(set) | See "F1" below |
| F2 routed script plain-drop (moon#1290) | FIXED, plus the FCALL and MULTI siblings | `a66bc7d` fix(scripting) | See "F2" below |
| MINOR 5: AOF refusal of an abort ignored | FIXED | `ddc9f21` fix(txn) | New pool unit test: red with the fix reverted, green with it |
| NIT: false "RDB drops per-field TTLs" warning | FIXED | `e41d92c` fix(txn) | New kv_compensation unit test captures WARN output: red with the fix reverted, green with it |
| NIT: `bridge.rs` over 1500 lines | FIXED | `68d3ac4` refactor(scripting) | `bridge/{mod,eviction_ctx,txn_capture,script_state,redis_call}.rs`, largest 640 lines; no behaviour change |
| MINOR 2: CI gap | FIXED | `ebd0209` ci | 1294, 1288 and the routed-tiering suite now run on both runtimes |

## F1: design choice and trade-offs
New file: `src/command/set/sscan_cursor.rs`.

**How a rewrite is detected.** I used option (a), a per-set generation stamp, but it costs **0 extra bytes per set**:
- Every `IndexSet` owns its own `RandomState`, and std seeds each new one differently. So `hasher().hash_one(PROBE)` identifies the set instance.
- Every rebuild creates a new hasher: SUNIONSTORE, SINTERSTORE, SDIFFSTORE, RENAME, COPY REPLACE, RESTORE, a cold decode, a restart.
- `insert` and `swap_remove` keep it, and `clone()` keeps both the hasher and the order.
- The cursor carries a 30-bit tag of it: `tag<<32 | pos`.
- No command has to remember to bump a counter.

**What happens on a mismatch.** On a mismatch, a foreign cursor (`-1`), or a cold-tier value, the scan switches to **hash mode**:
- The cursor becomes `1<<62 | T`, meaning "members whose 61-bit fixed-seed xxh64 is below T are still to visit".
- A member's hash depends only on its bytes, so this is correct under any mutation, including further rewrites.
- It always terminates, even if the set is rebuilt between every call (restarting the scan on each rewrite would livelock).
- Each hash-mode call is O(N), so the page size is `max(COUNT, ceil(N/256))`, at most about 256 calls. Members that share a hash at a page boundary are never split.

**The common case** is unchanged: O(COUNT) per page, one extra SipHash of a u64 per page, and every cursor stays below 2^63.

**Why not (b) or (c).** Making the *STORE commands preserve order does not cover RENAME, COPY, RESTORE, restarts or the cold tier. A redis-style bucket cursor is impossible because `IndexSet` exposes no buckets.

**NITs, checked against redis 7.0.15 first:**
- An intset now answers in numeric order (`-5 1 2 3 10 100`).
- The cursor now follows redis's strtoul rules: `-1`, `+0`, an empty cursor and `007` are accepted; `" 1"`, `"1 "`, `+`, `-`, `0x1` and overflow are refused. This is `scan_options::parse_scan_cursor`, used by SSCAN only.

**Red → green:**
- `sscan_rewrite_review_tests` on f7f1d96: 100 of 2000 members skipped after SUNIONSTORE and after SINTERSTORE. Now green.
- New tests in `sscan_tests`:
  - SINTERSTORE, SDIFFSTORE, RENAME and COPY REPLACE mid-scan, mixed with random SREM/SPOP/SADD;
  - a set rebuilt between every call (all members returned in ≤258 calls);
  - forced hash ties;
  - the cold path;
  - intset order and the `-1` cursor.
- The WS29 property test stays green.
- `test-commands.sh --category set`, with `MOON_BIN` pinned and `MOON_DISK_FREE_MIN_PCT=0`: ws30 24/24; w1 22/24 (the two new NIT rows fail).
- `test-consistency.sh`: ws30 has 76 failures, the same baseline WS29 reported, and none are new. w1 has 80, including the 3 new SSCAN rows (rewrite, intset order, `-1`).

**Performance.** Full SSCAN of 1M members, `--shards 1`, redis-py, runs interleaved:
- WS29's 2.1–2.7 s matches COUNT 100.
  - 3 reps: w1 1.86/2.10/2.20 s, ws30 2.01/2.46/2.57 s, redis 3.03/3.05/3.25 s.
  - A tighter 8-rep A/B gives medians of **w1 2.29 s vs ws30 2.37 s (+3.5%, ranges overlap)**, within 10% of WS29.
- COUNT 10: w1 10.99–11.10 s, ws30 10.82–11.27 s, redis 11.05–11.65 s.
- With one `SUNIONSTORE s s` after page 1:
  - w1 returned 999,990 of 1M members (skipped 10);
  - ws30 returned all 1M in 257 calls, 8.46–9.07 s;
  - redis 11.2–11.5 s.

## F2: mechanism and audit of paths that run while the drain holds the manifest
- **Fix.** `manifest_cell::lend(slot, f)` moves the drain's `&mut` manifest into a thread-local for the duration of the script, then moves it back, also when the script unwinds. There is no unsafe code and no allocation (the move is a memcpy). `with_manifest` falls back to that thread-local when the cell is held.
- **Where it is lent** (`spsc_handler`): routed EVAL/EVALSHA, routed FCALL/FCALL_RO, and a routed MULTI body (`TxnExecute`) that holds a script.
- **Paths that already had the manifest:** the SPSC gate (Execute and MultiExecute legs) and the cross-db COPY gate are handed it directly.
- **Paths that cannot evict inside the drain:**
  - plain commands in a routed MULTI body run no per-command gate in the drain;
  - the FUNCTION fan-out only registers functions.
- **Loop borrows outside the drain** (eviction tick, snapshot finalize, spill drain and shutdown, orphan sweep, autovacuum) pass the manifest directly and run no script.
- **Connection gates** run on connection tasks, never inside a loop borrow.

**Red → green.** 16,000 × 600 B, 8 MB allkeys-lru, `--shards 4`, `tests/review_w1_routed_eval_tiering_1290.rs`, to which I added FCALL and MULTI+EVAL cases:

| binary | EVAL evicted | FCALL evicted | MULTI+EVAL evicted | result |
|---|---|---|---|---|
| w1 monoio | 1889 | 1900 | 1662 | all red |
| w1 tokio | 2200 | 1608 | 1557 | all red |
| ws30 monoio / tokio | 0, DBSIZE 16000/16000 | 0 | 0 | green, `--shards 4` and 1 |

## Related suites (both runtimes, ws30 binaries, all green)
- `tiering_no_aof_write_gate_1290` at s4 and s1: 2/2 each.
- `review_r3_lua_eviction_aof`: 1/1.
- `eviction_reason_del_run_budget_1294`: 1/1, about 8 s.
- `active_expiry_backlog_drain_1288`: 1/1, about 4 s.
- `txn_abort_durability_1285`: 9/9.

## Gates (before the last commit)
- `cargo fmt --check`: OK.
- `cargo clippy --all-targets -D warnings`: clean on both feature sets.
- Fuzz crate check: builds; its `unused doc comment` warnings are in fuzz targets I did not touch.
- `audit-unsafe.sh`: 244/244.
- Filtered lib tests (set, scripting, shard, transaction, scan_options, aof pool, dump_payload, server::conn): tokio 926/926, monoio 992/992.
  - The first monoio run had 1 failure, `reclaim_death_budget_tests::a_systematic_reclaim_bug…`, while the consistency suite was running on the box. That code is untouched. It passed 3/3 alone and the full filtered rerun was 992/992. It still needs an A/B on the base commit before calling it pre-existing.

## Residual risks
1. **F1 blind spot:** a COPY of the set, then writes to both copies, then the copy moved back onto the scanned key mid-scan. The two layouts share one tag and may have diverged.
2. **F1 tag collision:** a 2^-30 chance per rewrite that happens during a scan.
3. **F1 hash-mode cost:** each hash-mode call is O(N) on the shard, about 35 ms at 1M members and about 10× that at 10M. It only happens after a rewrite that itself cost O(N).
4. **F1 very large sets:** a set with ≥2^32 members always scans in hash mode.
5. HSCAN (moon#1171) could reuse the hash-threshold cursor. SCAN, HSCAN and ZSCAN still refuse a `-1` cursor.
6. **Item 4 log level:** an explicit TXN.ABORT refusal logs at WARN; the two cases no client hears about log at ERROR (≥ WARN, and the existing ERROR log was kept for them).
7. The CI wiring has not been run on the hosted runners; the hosted workflow was not dispatched.
8. F3–F5 were out of scope.

## Files outside this workstream's scope
- **Command / script layer:** `src/command/{set/*, scan_options.rs, hash/hash_read.rs (doc only)}`, `src/scripting/bridge/*`.
- **Shard:** `src/shard/{manifest_cell.rs, spsc_handler.rs, persistence_tick.rs (test include only)}`.
- **Persistence:** `src/persistence/{aof/pool.rs, dump_payload.rs, redis_rdb.rs}`.
- **Connection / transaction:** `src/server/conn/{txn_abort.rs, handler_monoio/{mod,txn}.rs, handler_sharded/{mod,txn}.rs}`, `src/transaction/kv_compensation.rs`.
- **Scripts, tests and CI:** `scripts/test-{commands,consistency}.sh`, `tests/review_w1_routed_eval_tiering_1290.rs`, `.github/workflows/integration-tests.yml`.

## CHANGELOG bullets
- **Fixed:** SSCAN no longer skips members that are present for the whole scan when the set is rebuilt mid-scan (SUNIONSTORE, SINTERSTORE, SDIFFSTORE, RENAME, COPY REPLACE, RESTORE, cold tier). A rebuild is detected at zero bytes per set, and the scan falls back to a hash-ordered cursor that always terminates. An intset now answers in numeric order, and `SSCAN key -1` is accepted as in redis (moon#1287).
- **Fixed:** at `--shards ≥ 2` without an AOF, a script routed to another shard (EVAL, FCALL, or a script inside MULTI) no longer drops its eviction victims; it tiers them to disk. Before, 1.5–2.2K of 16K acknowledged keys were lost (moon#1290).
- **Fixed:** an AOF refusal of a TXN rollback's compensating records is now always counted and logged. A record refused by a dead AOF writer now counts in `aof_backpressure_dropped` (moon#1285).
- **Fixed:** a TXN.ABORT that restores a hash with field TTLs no longer logs a false "RDB drops per-field TTLs" warning (moon#1285).
- **CI:** `eviction_reason_del_run_budget_1294`, `active_expiry_backlog_drain_1288` and `review_w1_routed_eval_tiering_1290` now run in `integration-tests.yml` on monoio and tokio (moon#1294, moon#1288, moon#1290).

## Self-evaluation (0–1)
Completeness 0.93 · Clarity 0.91 · Practicality 0.92 · Optimization 0.90 · Edge cases 0.91 · Self-evaluation 0.90
- Edge cases sits at 0.91 rather than higher because of residual risks 1–3; each is documented and bounded, and closing risk 1 would mean re-seeding the hasher on COPY.
- Optimization sits at 0.90 because a hash-mode call stalls the shard for O(N) after a rewrite. That is inherent to any correct fallback that adds no per-member memory.
