# Wave 2b — handoff for a new session

Written 2026-10-04. Read this first; it replaces the session scratchpad, which a new session cannot see.

## 1. Where things stand

| item | state |
|---|---|
| Wave 2a | PR **pilotspace/moon#1316** — **MERGED 2026-10-04** (squash, `9ffa7f5` on main, tree = `fa3f751`). The hosted `ci.yml` dispatch (Windows/MSRV/memory) returned 403 from the agent: the maintainer must run it (`gh workflow run ci.yml --ref claude/gifted-mendel-e9wiz5`). |
| Wave 2b | Branch **`w2/int-2b`** (pushed) @ `9d85dbc` = fa3f751 + WS42/43/45/46 + review rounds R2b 1–4 + docs. Code identical to `fe6fb20` (later commits are CHANGELOG/README only). |
| Wave 2b gate | `fe6fb20`, Linux container, not merge bar: fmt, clippy ×2, fuzz check OK; lib monoio 7150/0, tokio 6206/0; 216 integration suites (both runtimes) — only the known reds in §4; loom fsync_agent 5/5, aof_lane 17/17; epoll (`MOON_NO_URING=1`) aof_everysec_kill9 10/10, aof_shard_write 12/12, aof_fsync_stall 4/4. Full status: `bench/gateR8-status.txt`. |
| Wave 2b bench | Quiet-box A/B 2a (`fa3f751`) vs 2b (`fe6fb20`): `bench/summary-*`. Wins and two regressions, see §3. |
| Open fix lanes | **`w2/r2b5-reclaim-oom`** (moon#1297 −OOM regression) and **`w2/r2b5-coalesce`** (moon#1322 pipeline cost), both branched from `9d85dbc`, started 2026-10-04. If they are not pushed when you start, they were lost with the old container: redo them from §3. |
| All lane branches | `w2/*` on origin (backup, no PRs). `w2/2b-docs` = the docs commits already on int-2b. |

## 2. What is left to ship wave 2b (in order)

1. **Finish the two R2b5 fixes** (§3). Each needs: a red→green regression test, the bench cell re-run vs `fa3f751` and `fe6fb20`, gates on both runtimes. Write `plans/R2b5-*/SUMMARY.md`.
2. **Integrate** onto `w2/int-2b` by cherry-pick (issue order), push.
3. **Re-gate** the integrated tree with `tooling/gate.sh` (resumable; see §5). Expect only the §4 reds.
4. **Re-bench** cells 3 and 5 (and cell 1 tokio everysec as a sanity check) with `tooling/`; replace the regression paragraphs in the Performance section below.
5. **CHANGELOG**: update the moon#1297 and moon#1322 entries (add the OOM fix / coalescing and the measured costs). Keep the Keep-a-Changelog style; no internal names (R2b, WS4x, lanes).
6. **#1316 was squash-merged on 2026-10-04** as `9ffa7f5` on `main`; its tree is byte-identical to `fa3f751` (`git diff fa3f751 origin/main` is empty). So replay wave 2b with `git rebase --onto origin/main fa3f751 w2/int-2b` (or cherry-pick `fa3f751..w2/int-2b`) — do NOT merge, which would re-add the wave-2a commits. The designated branch `claude/gifted-mendel-e9wiz5` still points at the old `fa3f751`; resetting it to `main` needs a force push, which the agent's permission mode refused — the maintainer either allows that reset or names another branch for the wave-2b PR. Open the **wave 2b PR** with `.github/pull_request_template.md` (Summary / Checklist / Performance Impact / Notes), body ending with the Claude Code footer. Then dispatch the hosted `ci.yml` (maintainer, if 403).

No more adversarial review rounds were requested by the maintainer for wave 2b ("integrate, gate, ship"); the two R2b5 fixes still need their own red→green evidence.

## 3. The two regressions found by the benchmark (fix now)

**moon#1297 no-AOF cold block reclaim (WS43) — `-OOM` under steady writes.** `redis-benchmark -t set -r 50000 -d 600 -c 16 -P 16 -n 400000`, `--maxmemory 8mb --maxmemory-policy allkeys-lru`, disk offload on, no AOF, `--save "3600 100000000"`, `--shards 4`:
- 2b answers `-OOM command not allowed when used memory > 'maxmemory'` in 3/5 monoio and 5/5 tokio runs; 2a 0/10; 2b with `MOON_TEST_COLD_RECLAIM_HOLD_FILE` (no compaction starts) 0/10. `bench/oom.csv`.
- Completed monoio runs −22% rps (171K → 131K), shard CPU/op +21%. Callgrind Ir/SET: 2a 45,832 · 2b held 45,811 · 2b 51,079 — all of it the reclaim; 73% of the extra on spill threads competing with 4 shards on 4 vCPUs. `bench/cg-summary.txt`, `bench/thread-ir-summary.txt`.
- Probable mechanism (source, not instrumented): `PendingCompaction.bytes` (`src/storage/tiered/cold_reclaim.rs` ~205) is charged to the memory ledger until a later snapshot adopts the compaction; ~180–200 pending at s4 (cap `NO_AOF_MAX_PENDING_PER_DB = 64` per db per shard, `cold_reclaim/no_aof.rs` ~133). Under a burst at the maxmemory edge that unevictable charge tips eviction into `-OOM`. The WS43 bench used 100K requests, which ends before the reclaim starts — that is why WS43 missed it.
- Fix candidates: don't start no-AOF compactions while used_memory ≥ maxmemory (or on a tick that evicted); keep pending-compaction bytes out of the eviction target; scale the pending cap to maxmemory; rate-limit spill-thread compaction work. Acceptance: 0 `-OOM` in ≥5 reps both runtimes, rps/CPU inside ±6% of 2a, reclaim still reclaims, cold-tier suites green.

**moon#1322 cross-shard barriers — pipelined spanning writes under `appendfsync always`.** s4, spanning MSET (10 keys, c50) vs 2a: P16 −70% monoio (107K → 32K rps) / −60% tokio (66K → 26K); P1 −16% / −9%; single MSET p50 +17% / +14%. Same-shard and everysec unchanged/better. 2a was only faster because it skipped the remote fsync (the bug). Cause (consistent with evidence, not profiled): each spanning write in a pipeline awaits its own barrier set before the next command runs. Fix: coalesce one barrier set per pipeline batch (accumulate written shards, pay once before the batch's replies flush; every mid-batch flush path pays first). Acceptance: P16 near 2a, `tooling/sc3.py` 0 violations (always / after-always / boot; tokio + monoio epoll), `cross_shard_write_barrier_1322` and `loom_aof_lane` green.

## 4. Known reds (red before wave 2b too — not regressions)

- tokio has no master-side PSYNC → "replica link never came up": `replication_streaming`, `replication_multishard`, `replication_hardening`, `replication_planes` (tokio), `replica_past_deadline_1286`, `replication_ttl_semantics`, `replica_blocking_wake_1096`, `stream_group_block_log_1104`, `stream_group_plain_log_1130`, the `a_replica_*` tests of `txn_crash_atomicity_1300` (6), `scripts_in_multi_894`, `perf_ws12_bgsave_split::a_resync_mid_bgsave…`. Run tokio replica suites with `MOON_TEST_NO_MASTER_PSYNC=1` where supported.
- tokio has no graph engine: `crash_recovery_graph_durability`.
- `aof_fold_exactly_once_455::…toplevel` (both runtimes); tokio `cold_tier_aof_double_apply_902`.
- monoio `replication_planes::eviction_parity_hash_disk_offload_shards{1,4}`.
- Timing/statistical, may flake under load: monoio `parked_idle_parity`, lib `vector::store::bg_compact_tests::test_bg_compact_pool_parallelism`, the LFU 0.15%-tail test, the unlink_hold ratio test.

## 5. How to work in this environment (lessons that cost hours)

- **The cloud container restarts when the session idles** and kills every background process and agent. Keep long work resumable: `tooling/gate.sh` records each step/suite in a status file and skips what is done; run it with Bash `run_in_background` (max 2 h per call — just relaunch) and re-arm a `send_later` check-in. Agents: resume with SendMessage, telling them which commits/files are on disk.
- One shared `CARGO_TARGET_DIR` (`/home/user/wt/target-c`, `CARGO_INCREMENTAL=0`) across worktrees: artifacts alias. Copy a binary out in the same command that built it and verify with `strings` markers; pin `MOON_BIN` (and `MOON_BIN_MONOIO`/`MOON_BIN_TOKIO` for runtime-pair suites — they now fail, not pass, when unset).
- Concurrent builds invalidate each other's test binaries (20 min/suite). Run gates and benches with nothing else building. Build all suites first (`cargo test --no-run --test a --test b …`), then run them.
- Many suites use **fixed ports** — never run the same suite twice concurrently (a "failure" may be a collision).
- Every real server needs `MOON_DISK_FREE_MIN_PCT=0` (the disk is ~75% full). Watch `df`; delete old binaries in `/home/user/wt/bin` and finished reviewers' data dirs.
- `pkill -f <pattern>` kills your own shell if the pattern is in the command line — kill by PID.
- Loom: `cargo rustc --release --test <t> -- --cfg loom` in a separate target dir, or compile the test standalone with `rustc --cfg loom` (`loom_aof_lane` does not link moon).
- Bench on this box: ±6% between identical binaries; ≥5 interleaved reps on fresh servers; a delta inside the spread is "no change".
- Redis 7.2.7 oracle: build from source into the scratchpad (`make -C redis-7.2`), used for parity byte-compares.

## 6. Next comprehensive plan (after wave 2b ships)

Prioritised backlog; issue numbers are pilotspace/moon:
1. **#1325** MULTI/EXEC crash atomicity — wrap EXEC bodies in `MOON.TXN BEGIN/END` (maintainer decision: follow-up after 2b).
2. **#1314** AOF write-error latch keeps acking under everysec; **#1309** `-MISCONF` refusal — the remaining durability holes.
3. **#1320** TXN log-id reuse after master restart + partial resync (regenerate repl id when replay rolled back a block).
4. **#1318 residuals** — crash inside the post-sync rewrite window; fold taken while a master TXN block is open; embedded replica cannot rewrite.
5. **#1324** monoio SCAN omits spilled keys (silent runtime divergence).
6. **#1326** `--migrate-aof-*` follow-ups (cold-tier source, per-shard → other shard count, memory); **#1321** honour `--appendfilename` in recovery; **#1327** `--recovery-target-*` parsed but ignored.
7. **#1323** admin console gateway writes skip the fsync barrier.
8. **#1307** graph-plane TXN isolation; **#1308** REPLICAOF with an open TXN; **#1310** ACL/XADD parity; **#1311** TXN hold over-refusal; **#1312** CLIENT KILL residual; **#1313** replica own-clock expiry; **#1315** tokio GET doesn't promote cold keys; **#1317** replica full sync drops field TTLs; **#1319** multi-shard BGSAVE not point-in-time; **#1306** ACL category expansion.
9. Tokio master-side PSYNC (would turn most §4 reds green).
10. Re-run the wave-2b perf table on a quiet hosted/GCE Linux host (all numbers so far are relative evidence from one shared container).

Suggested shape: one wave per theme (durability holes 1–4; replication/runtime parity 5, 8, 9; tooling 6–7), each with the same loop — lanes in worktrees → integrate → resumable gate → adversarial review until nothing above NIT → quiet-box bench → PR.

## 7. Wave-2b Performance Impact (draft for the PR body; replace the two regression paragraphs after R2b5)

See `bench/summary-c1.md` … `summary-c4c5.txt`. Highlights (2a `fa3f751` → 2b `fe6fb20`, 4-vCPU container, ≥5–7 interleaved reps):
- tokio everysec s1: p1 c1 **+18%**, p1 c50 **+20%**, p16 c1 **+13%**, p16 c50 **+65%**; s4 p16 c50 **+33%**. monoio cells inside the ±6% spread.
- Option 1A (`MOON_AOF_SHARD_WRITE`): tokio s1 P16 **+59%** (CPU/op 5.83 → 2.14 µs), epoll s1 P16 **+37%**; io_uring inside spread (CPU/op −19% at P16); `MOON_AOF_SHARD_WRITE=0` matches 2a in every leg.
- COW streaming (5M-field hash, HSET during BGSAVE): longest PING stall 731–843 ms → **27–30 ms**, RSS spike +836 MB → **+3–4 MB**.
- No-AOF SET CPU/op: monoio 860 → 885 ns, tokio 2310 → 2330 ns (inside spread).
- Regressions: §3 (to be replaced with post-fix numbers).
