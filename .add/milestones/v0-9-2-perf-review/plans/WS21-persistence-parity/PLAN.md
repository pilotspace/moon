# WS21-persistence-parity — PLAN (part 5, base: main @ 4a96cd5f, which holds parts 1–4)
personas: `.add/personas/storage-durability-engineer.md` (lead) · `.add/personas/ci-test-integrity-engineer.md`

Snapshot and AOF parity gaps left after part 4. Read first: `plans/WS16-snapshot-capture/{NOTES,SUMMARY}.md` (the COW epoch, pre-image capture, `capture_write_pre_image`) and `plans/WS19-cold-durability/{NOTES,SUMMARY}.md` (moon#1232 save rules, the SHUTDOWN wait with its 20 s deadline, SAVE_IN_PROGRESS).

## Issues (in this order)
1. **moon#1271 (BUG, race).** A per-shard AOF writer that starts late runs `AofManifest::load` → `cleanup_orphans`, which walks EVERY `shard-N/` dir and deletes other shards' in-progress `moon.aof.<seq+1>.base.rdb.tmp`, aborting the rewrite.
   - Fix direction: split the load — a no-cleanup variant for the writer-task startup loads and the rewrite-dispatch load; keep the cleanup in main.rs's single recovery load, which runs before any writer or rewrite exists. (Or skip `seq + 1` files while a rewrite is in progress — argue the choice.)
   - Test: a per-shard rewrite dispatched while one shard's writer is held in its manifest wait loop (test hook); it commits, where today it aborts with the rename error. Evidence of the live failure: `/home/user/wt/handoff/issue1271-server.err`.
2. **moon#1257 (BUG, point-in-time gap).** The eviction paths in `src/storage/eviction.rs` remove keys with `db.remove` and capture no pre-image, so a key evicted mid-save is missing from the snapshot (1,438 of 1,501 in the unit proof). A spill eviction whose write then fails can lose the key (trace, confidence 0.7).
   - Fix direction: capture the pre-image before each eviction removal; thread the victim's db index through `EvictionRun` so `snapshot_cow::capture_write_pre_image` runs on the owning shard. The no-save cost must stay one thread-local `bool` load. Prove or disprove the spill-failure path with an injected write failure, and fix it if real.
   - Tests: arm an epoch, evict under `allkeys-random` to half the budget, and the file equals the epoch-start keyspace; plus the spill-failure path. This also unblocks moon#1185 — note in NOTES what #1185 still needs.
3. **moon#1267 (BUG, parity).** With `--appendonly no` and no save rules, `persistence_dir` is never set: an existing snapshot is ignored at boot and BGSAVE/SAVE/SHUTDOWN SAVE are refused (`SNAPSHOT_DIR_ABSENT`). Redis loads `dump.rdb` and allows manual saves whatever the save rules.
   - Keep the "no per-tick WAL/fsync writer when `appendonly no`" property: only the AOF writer needs `appendonly yes`. Check that disk-offload-only and replica configs keep booting as today.
   - Tests (real server, `--shards 1` and 4, both runtimes): write + BGSAVE with `--save "3600 1"`, restart with `--save ""` → keys present; with `--save ""`, BGSAVE succeeds and a restart reloads the file.
4. **moon#1263 (BUG, data loss on stop).** SIGTERM/SIGINT with save rules and `appendonly no` exits without a final save. Redis's `prepareForShutdown` saves (waiting for a running save first).
   - Route SIGTERM/SIGINT through the same path as a plain `SHUTDOWN` when save rules are set: wait for a running save (WS19's one 20 s deadline), save every shard, exit only after it is durable; a failed save logs and exits non-zero. `SHUTDOWN NOSAVE` unchanged. Both runtimes.
   - Tests: SET, SIGTERM, restart → key present, `--shards 1` and 4; SIGTERM during a running BGSAVE waits, then saves once.
5. **moon#1264 (BUG, parity).** (a) `SHUTDOWN ABORT` always answers "No shutdown in progress"; while a SHUTDOWN is waiting or saving it must cancel it (`+OK`, and the waiting client gets `-ERR Errors trying to SHUTDOWN. Check logs.`). (b) FLUSHALL with save rules must save the empty dataset synchronously before replying, as redis's `flushallCommand` does (or a documented equivalent).
   - Tests: SHUTDOWN during a held save + SHUTDOWN ABORT from a second client (server stays up, replies match redis 7.0.15); FLUSHALL with `--save "3600 1"`, kill -9, restart → empty. All three dispatch paths for both commands.
6. **Test nit (CodeRabbit on PR #1268).** `tests/perf_ws19_script_oom.rs` L282-288: the EVAL DEL may run after db1 is back under quota, so it does not prove the bypass. Re-fill db1 over quota and assert a small SET is refused before the EVAL DEL. Commit as `test(scripting): …`.

## Owned files
`src/storage/eviction.rs` (+ `src/storage/eviction/**`), `src/persistence/{snapshot_cow.rs,snapshot.rs,auto_save.rs}`, `src/persistence/aof_manifest/**`, `src/persistence/aof/writer_task.rs` (+ `writer_task/**`), `src/main.rs` (persistence wiring, signal handling), `src/command/persistence.rs`, the FLUSHALL paths, `tests/perf_ws19_script_oom.rs`, new tests `tests/perf_ws21_*.rs`.
WS20 runs in parallel and owns `src/command/keyspace/move_cmd.rs`, `src/storage/db/**`, `src/storage/tiered/**`, `src/shard/timers.rs`, the SWAPDB regions, and the expired-entry skip sites in `src/persistence/{rdb.rs,replay.rs}`. A necessary edit there is an isolated commit listed under "Cross-ownership edits".
`src/storage/eviction.rs` (4089 lines) and `src/main.rs` (2614) are already over the 1500-line cap: do not grow them; put new logic in new modules.
Not in scope: moon#1266 (everysec write-before-reply) is a design decision held for the maintainer.
