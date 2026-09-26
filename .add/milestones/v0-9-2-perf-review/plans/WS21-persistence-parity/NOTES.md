# WS21-persistence-parity — working notes

Branch `perf/ws21-persistence-parity`, base main `4a96cd5f` + plan `b230d011`.
Personas: storage-durability-engineer (lead), ci-test-integrity-engineer.
Oracle: redis-server 7.0.15 (on PATH). Target: private `/home/user/wt/target-gate`.

SUMMARY.md: the harness refused this agent's write of it; its full content is in the WS21 final
report for the orchestrator to commit (TEAM-RULES §6).

Binaries used for red runs:
- `/home/user/wt/bin/main-4a96cd5f-rel` — release build of the base (monoio default).
- `/home/user/wt/bin/baseline-ae21476` — pre-wave main (release).
- `ws21-1271-red-debug-monoio`, `ws21-1264a-red-debug-monoio` — this tree with ONLY that fix
  reverted (test hooks kept), where the base has no hook to drive the test; deleted after use,
  as were the green debug builds (`ws21-debug-{monoio,tokio}`) and the release-fast build.

## ORIENT — which paths each issue touches

| issue | producer | consumer | configuration that exposes it |
|---|---|---|---|
| moon#1271 | per-shard AOF writer startup load, rewrite dispatch load, size monitor | the per-shard rewrite's `seq + 1` files | `--appendonly yes --shards >= 2`, both runtimes (PerShard layout) |
| moon#1257 | `storage::eviction` victim removal | the BGSAVE file (RDB-only restore), moon#1185's fold | any `maxmemory` + evicting policy while a save runs |
| moon#1267 | main.rs `persistence_dir` gate | boot snapshot load, BGSAVE / SHUTDOWN SAVE | `--appendonly no` with `--save` OMITTED (see below) |
| moon#1263 | SIGTERM / SIGINT handlers | the final snapshot | `--save <rules> --appendonly no` |
| moon#1264 | SHUTDOWN ABORT parse; FLUSHALL | a waiting SHUTDOWN; the on-disk snapshot after FLUSHALL | `--save <rules>` |

## moon#1271 — a late writer's manifest load deletes a rewrite in flight

### Mechanism verified (4a96cd5f)
- `AofManifest::load` ended with `cleanup_orphans()` (aof_manifest/mod.rs L383, byte-identical
  on ae21476). For PerShard it walks every `shard-N/` and deletes every `moon.aof.*` file whose
  sequence is not the committed seq — `.base.rdb`, `.incr.aof`, `.rdb.tmp`, `.tmp`.
- Callers: main.rs L870 (boot, `appendonly yes`, BEFORE the writers are spawned at L937+),
  main.rs L1394 / L1605 (recovery, writers exist but wait for the manifest; no rewrite can exist
  yet: no listener, no monitor), the writer startup wait loops (writer_task.rs L470 TopLevel,
  L1193 tokio per-shard, L1654 monoio per-shard), `pool.rs` per-shard rewrite dispatch (L1404),
  `auto_rewrite::measure_base_size_at` (boot, `init`), `migrate_aof` (offline tool, 3 loads).
- The writers' loads run after main.rs creates the manifest, and a writer thread sleeps 50 ms per
  wait iteration. The issue log: shards 0/2/3 loaded at +42 ms, shard 1 at +45 ms — AFTER the
  BGREWRITEAOF dispatched at +43 ms.
- **Worse interleaving, found here:** when the late load lands after the other shards'
  `advance_shard_staged` renamed their `seq + 1` base and created the `seq + 1` incr, it deletes
  those; the late shard then folds and the coordinator COMMITS `seq 2`. The manifest names files
  that are gone. Next boot: `AOF shard-0 base RDB not found … (incr empty, treating as fresh
  init)` for three of four shards — the red run restarted to DBSIZE 35 of 200. Silent data loss,
  not the "aborts safely" the issue assessed.

### Fix design
- Split the load (the issue's first option): `load` is a pure read; the sweep moved verbatim to
  `aof_manifest/orphans.rs` behind `load_and_sweep_orphans`, called ONLY from main.rs L870.
  Every other caller — including ones I do not own (`pool.rs`, `auto_rewrite.rs`,
  `migrate_aof.rs`) and any future one — gets the safe behaviour without an edit.
- Rejected: "skip `seq + 1` while `AOF_REWRITE_IN_PROGRESS`". The check races the rewrite's own
  start (flag read false, dispatch, temp file created, sweep's `read_dir` sees it), and the flag
  is process-global while the rewrite seams use per-test flags.
- What the runtime sweeps used to clean: an aborted rewrite's `seq + 1` generation. The seq does
  not advance on abort, so the next attempt writes the same names: `File::create` truncates the
  temp base and the new incr, `rename` replaces the base. At most one stale generation per shard
  until the next boot's sweep.
- Test hook `MOON_TEST_AOF_WRITER_HOLD=<shard>:<file>` (writer_task/test_hooks.rs): held writer
  does not start while the file exists; read once (OnceLock), one cached check per writer start.

### Risks
- The boot sweep still deletes uncommitted `seq + 1` files; it runs before any writer exists
  (main.rs L870 precedes the writer spawn), so nothing can be in flight.
- TopLevel (`--shards 1`, monoio): `advance` prunes post-commit itself; unaffected.

## moon#1257 — eviction victims take no pre-image

### Mechanism verified (4a96cd5f)
- Six non-test `db.remove` sites in `storage/eviction.rs`: `evict_batch_durable` (drop before
  expiry; spilled entries after the durable write), `evict_one_async_spill` (drop before expiry;
  after the queue; the expiry race), `evict_one_with_spill` (plain drop). None captures. All
  eviction callers (`evict_to_budget`'s sinks, `timers::run_eviction`, the memory-pressure
  cascade, `db_quota`) reach one of the six.
- `capture_write_pre_image(db, slot, key)` needs the shard SLOT. `Database::db_index` is the slot:
  `shard/mod.rs` L168 stamps it, `db_plane::swap_contents` swaps it back after the contents
  (#1242), `rdb.rs` L452 carries it into `*live = temp`, `Database::clear` keeps the struct.

### Fix design
- `storage/eviction/victim.rs::remove(db, key)`: capture under `db.db_index`, then `db.remove`.
  The six sites call it. Reading the slot from the victim's own database instead of threading
  it through `EvictionRun` (the plan's direction) covers every caller with no edit outside my
  files and makes a wrong index impossible at the call site. The SWAPDB test pins the slot
  mapping (evict from a swapped slot, file still exact).
- No save: `capture_write_pre_image` returns after one thread-local `bool` load.

### Spill-failure trace (issue, confidence 0.7): proven at the file level, not a restart loss today
- Injected failure: the shard dir is a regular file, so `spill_thread::flush_buffer` fails every
  write; the test mirrors `persistence_tick::apply_completion_vec`'s failure branch
  (`spill_inflight_clear` + `set(rehydrate(..))`) after the walk passed. Red: 725 of the spilled
  keys missing from the file.
- Reachability on restart (reworded in the review round, F9): the async sink is chosen from the
  RUNTIME `appendonly` (`EvictionSink::AsyncSpill`; `appendonly no` routes to
  `evict_batch_durable`, which keeps the hot value on a failed write). When the server STARTED
  with `appendonly yes`, the AOF (manifest authority on monoio and multi-shard tokio; legacy
  `appendonly.aof` replayed after the snapshot on tokio `--shards 1`) holds the key's write, so
  the gap was a file that is not a point-in-time image, not a key lost at restart. But
  `CONFIG SET appendonly yes` on a server started with `no` flips the sink with no AOF writer
  behind it: there, before this fix, a failed async spill during a save WAS a restart-loss path
  (the key missing from the snapshot, and no AOF to hold it). The capture closes it in every
  configuration: the victim's pre-image is taken before it leaves the table.

### Risks
- During a save, evicted values stay resident as pre-images until the walk writes their range
  (INFO `current_cow_size`; not `used_memory`, so they cannot trigger more eviction). Bounded by
  the epoch-start dataset, like redis's fork COW.
- Spilled victims are now in the file AND the cold tier (same value); recovery already resolves
  a hot shadow over a cold entry (reads hot, the sweep reclaims the cold slot).

### What moon#1185 (incremental COW AOF fold) still needs after this
1. The TXN.ABORT undo decision (`transaction::abort::abort_cross_store_txn` restores with
   `db.set` / `db.remove` outside dispatch) — unchanged, needs a maintainer decision.
2. The fold itself: WS12's consumer-registry design on top of the per-slot map and frozen tables
   (WS16 NOTES), with the fold consumer serialized through `FoldImageSink`.
3. The spill-in-flight plane: a victim spilled after the fold cut is now in the hot base (its
   pre-image), so the fold's exactly-once argument holds for it; the fold must still decide how a
   cold copy above its watermark and the pre-image in the base resolve (hot wins, same value).

## moon#1267 — no save rules: the snapshot is ignored at boot, manual saves refused

### Mechanism verified (4a96cd5f)
- main.rs L1070: `persistence_dir = Some(dir)` iff `appendonly yes || config.save.is_some()`.
  **`--save ""` is `Some("")`**: that spelling already worked (probe on `main-4a96cd5f-rel`:
  `--save ""` BGSAVE → `shard-0.rrdshard` written; `--save` omitted → "background save
  unavailable"). The real gap is `--save` OMITTED — the default. The issue/plan wording
  ("`--save ""`") is refined here; the tests cover both spellings.
- Boot: recovery ran only `if persistence_dir.is_some() || disk_offload_base.is_some()`. With the
  default `--disk-offload enable` the offload branch already recovered a snapshot at boot; with
  `--disk-offload disable` and no `--save` nothing loaded.
- Manual saves: `SNAPSHOT_DIR_ABSENT = persistence_dir.is_none()` refused BGSAVE / SHUTDOWN SAVE up
  front — also in the default (offload-enabled) configuration. The shard-side path
  (`persistence_tick::check_auto_save_trigger`) had no directory either.
- The per-tick WAL writer is gated on `appendonly` in the event loop (`appendonly_enabled`), not
  on `persistence_dir`, so the old comment's rationale no longer held. `persistence_dir` does
  still switch on other things for a `Some`: vector index persistence (`vector_persist_dir_for`),
  graph save at shutdown, the maintenance-schedule file, graph/temporal/workspace/MQ WAL replay,
  the dir-lost latch.

### Fix design
- Narrow: keep `persistence_dir` exactly as is (so none of the above changes for an in-memory
  server), and add a SNAPSHOT directory that is always `--dir`: `command::persistence::
  set_snapshot_dir` (main.rs), `snapshot_dir_or(persistence_dir)` in the shard's snapshot trigger
  (one cross-ownership line in persistence_tick.rs), no more `SNAPSHOT_DIR_ABSENT` from main.rs,
  and `restore_from_persistence` always runs (dir: persistence dir, else `--dir`).
- Rejected: `persistence_dir` always `Some` — it would also turn on vector/graph persistence,
  the dir-lost write refusal and the WAL replays for every in-memory server, unmeasured.
- Boot of a `--save`-omitted server is now identical to a `--save ""` server's.

### Risks
- A `--save`-omitted server now replays what `restore_from_persistence_v2` finds in `--dir`: the
  snapshot, a legacy flat `appendonly.aof` if present, else the WAL v3 last-resort fallback. That
  is what a `--save ""` / `--save <rules>` server has always done; with `appendonly no` no WAL is
  written, so a `wal-v3/` there is left over from an earlier `appendonly yes` run. Pre-existing
  for the `--save` configurations, now shared by the default one. (redis ignores the AOF when
  `appendonly no`.)
- A replica booted with a local snapshot loads it before its full sync replaces it (checked by
  hand: "stale" → "fresh", link up) — as redis loads `dump.rdb`.
- `SAVE` in the sharded server is still "not supported, use BGSAVE" (pre-existing, unchanged).

## moon#1263 — SIGTERM / SIGINT exit without a final save

### Mechanism verified (4a96cd5f)
- main.rs: SIGTERM is blocked in every thread and consumed by the `sigterm-handler` thread
  (`sigwait`), SIGINT by `ctrlc`'s handler; both only `cancel()` the shared token. No save.

### redis 7.0.15, measured
- `save "3600 1"`, SET, SIGTERM: "Saving the final RDB snapshot before exiting", exit.
- Same with `dump.rdb` squatted by a directory: "Error moving temp DB file … Is a directory",
  "Error trying to save the DB, can't exit.", "Errors trying to shut down the server. Check the
  logs for more information." — **the server keeps running** (PING answers). The issue's "exit
  non-zero, as redis does" is contradicted; this fix follows redis. The plan's "a failed save logs
  and exits non-zero" is therefore NOT implemented, deliberately.

### Fix design
- `command/persistence/signal.rs`: main.rs arms it after every shard thread is spawned (trigger,
  shard count, save points, token). A signal with save points starts a `shutdown-save` std thread
  running `persistence::shutdown_save` (the plain SHUTDOWN path: wait for a running save, save,
  one 20 s deadline) through a noop-waker `block_on` (its sleeps are `std::thread::sleep`, so it
  never returns Pending); the token is cancelled only after the save succeeded. The signal
  thread never blocks. The sigterm thread keeps waiting (a failed save leaves the server up and a
  later SIGTERM retries). Second SIGINT during the save: `exit(1)` (redis "You insist").
- Before `arm` (still booting): cancel at once, as before — a save then would overwrite a good
  snapshot with a partial dataset.

### Risks
- Writes acknowledged while the final save runs are not in it (the cooperative snapshot's epoch
  starts at each shard's next tick). Pre-existing for `SHUTDOWN` (WS19) and now shared by the
  signal path; redis pauses writes during a shutdown (`PAUSE_DURING_SHUTDOWN`). A follow-up needs
  a pause purpose that composes with `CLIENT PAUSE` (moon's `client_pause` has one global state).
- `shutdown-on-sigterm` / `shutdown-on-sigint` (redis's `force`/`nosave` overrides) do not exist
  in moon.

## moon#1264 (a) — SHUTDOWN ABORT

### Mechanism verified (4a96cd5f)
- `parse_shutdown_args` returned `Err("-ERR No shutdown in progress")` for any ABORT; the waiting
  SHUTDOWN (moon#1232) could not be cancelled. The reply also lacked redis's period, and `ABORT
  NOSAVE` answered it instead of a syntax error.

### redis 7.0.15, measured (master waiting for a SIGSTOPped replica)
`SHUTDOWN ABORT` → `+OK`; the waiting client → `-ERR Errors trying to SHUTDOWN. Check logs.`; a
second ABORT → `-ERR No shutdown in progress.`; `ABORT` + any modifier, `SAVE NOSAVE`, unknown
word → `-ERR syntax error`. Encoded as `shutdown_abort_oracle_redis_agrees` (ignored, green).

### Fix design
- `command/persistence/shutdown_abort.rs`: `Pending` guard (count + the abort generation it
  started under, read BEFORE the count goes up) held by `shutdown_save_within` for its whole run —
  every SHUTDOWN with a save and every signal save. ABORT with a shutdown pending bumps the
  generation (+OK); the waiting loop sees it at its next 5 ms poll and answers the redis error.
- ABORT is answered inside `parse_shutdown_args` as the `Err` reply all three SHUTDOWN intercepts
  (monoio, tokio, legacy single) already send back while the server stays up: no handler edit.
  SHUTDOWN reaches `command::dispatch` only inside MULTI ("not allowed", unchanged);
  `dispatch_read` and the inline path never see it.

### Risks
- A save the cancelled shutdown already started runs to completion as an ordinary BGSAVE (a
  cooperative snapshot cannot be stopped mid-shard).

## moon#1264 (b) — FLUSHALL with save points saves synchronously

### Mechanism verified (4a96cd5f)
- FLUSHALL cleared the keyspace, counted the change and replied; the previous snapshot stayed
  until a rule fired (red: 200 of 200 keys back after a SIGKILL right after the reply, every
  shape, both base binaries).
- redis 7.0.15, measured: FLUSHALL with save points rewrites `dump.rdb` at once (mtime moves),
  also from a script and in MULTI; `rdb_changes_since_last_save` is 0 after it.

### Fix design
- `persistence::save_after_flushall`: save points → wait for a running save (the FLUSHALL aborted
  it, moon#1228), run a sharded save and wait (SHUTDOWN's deadline, the connection's own runtime
  timer `runtime::TimerImpl::sleep`); failure is logged and FLUSHALL still answers +OK (redis).
- It must start after EVERY shard flushed (a shard arming the save before its own flush aborts
  it), so it hangs off the originator's completion of the fan-out: plain FLUSHALL (both
  runtimes), EXEC with a FLUSHALL (local + routed, both runtimes), `finish_script_flush` (every
  script entry point, routed ones included). `server/conn/flush_save.rs` keeps each site one line;
  the over-cap files (handler_monoio/mod.rs, handler_sharded/mod.rs, shared.rs) did not grow.
- Not covered, documented: a replica applying the master's FLUSHALL (redis replicas with save
  points do save), the admin console's flush, AOF replay (must never save).

## PLAN item 6 — perf_ws19_script_oom nit
- `an_allow_oom_function_does_not_write_past_a_db_quota` ran the EVAL DEL right after the
  allow-oom FCALL DEL, which can bring db 1 back under quota. Now: re-fill until a SET is refused,
  assert a small SET is refused, then EVAL DEL.
- Mutation proof: EVAL (Compat mode) given the non-command-aware quota check and no shrink-only
  bypass → the old test passed 3/3, the new one fails "an EVAL DEL over the quota".

## Cross-ownership edits made (each in its own commit)
- `src/persistence/aof/auto_rewrite.rs` — doc line (a378418e).
- `src/shard/persistence_tick.rs` — `snapshot_dir_or` fallback, net 0 lines (4516a9aa).
- `scripts/test-commands.sh` — two SHUTDOWN ABORT parity rows (e36731ce).
- `tests/perf_ws15_bgsave_status.rs` — the no-save refusal test now asserts moon#1267's
  behaviour (e86342b1).
- `tests/shutdown_integration.rs` — root-proof failure injection (e572a741).
- Owned per the plan's "FLUSHALL paths": handler_monoio/{mod,write}.rs, handler_sharded/{mod,
  write}.rs, shared.rs (`finish_script_flush`), server/conn/{mod,flush_save}.rs.

## Measurements (this 4-vCPU container, relative evidence only)

Binaries: A = `main-4a96cd5f-rel` (`release`: fat LTO, cgu 1), B = `ws21-final-rf` (`release-fast`
of de8bf26e: thin LTO, cgu 16; one release build, deleted after). Order A B B A A B, fresh server
per run, `--appendonly no --disk-offload disable`.

**FLUSHALL round trip** (1,000 keys refilled untimed before each; 300 FLUSHALLs per run; p50 / p99 ms):

| config | A runs | B runs |
|---|---|---|
| s1, `--save "3600 1000000"` | 0.115/1.72, 0.130/0.64, 0.073/0.46 | 18.9/25.8, 18.7/24.9, 18.6/30.9 |
| s4, `--save "3600 1000000"` | 0.145/4.05, 0.125/4.61, 0.129/5.14 | 24.2/29.2, 24.3/25.9, 24.3/26.0 |
| s4, no save points | 0.150/3.78, 0.129/4.33, 0.122/4.24 | 0.134/3.79, 0.132/5.00, 0.138/4.06 |

- The added cost is exactly one moon BGSAVE of an empty dataset: a bare BGSAVE → done measures
  p50 17.0 ms on BOTH A and B (100 saves each). `MOON_IDLE_PARK=0` does not change it (18.5 ms), so
  it is not the idle park; it is the cooperative save's pickup/walk/stream/fsync/report cycle plus
  the 5 ms poll. redis 7.0.15 on the same box: FLUSHALL with save points p50 3.2 ms (p99 ~10 ms),
  without 0.065 ms — but redis's synchronous `rdbSave` blocks every client; moon's blocks only the
  connection that sent the FLUSHALL. Without save points nothing changed.
- Follow-up (not done here): a shard-notified save pickup, or a direct empty-snapshot write for
  FLUSHALL, would bring the ~17 ms floor (shared by SHUTDOWN and BGSAVE) toward redis's.

**Eviction, no save running** (`--maxmemory 32mb allkeys-random`, `redis-benchmark -t set -n 400000
-r 4000000 -d 100 -P 16 -c 50`, ~258k evictions per run, SET rps):
- Same-profile A/B (debug builds, `ws20-red-4a96cd5f-debug-monoio` vs `ws21-debug-monoio`), s1, 6
  pairs: A 183.2k 186.7k 176.9k 177.1k 174.2k 170.9k (median 177.0k); B 182.4k 190.0k 185.4k 185.4k
  182.0k 188.1k (median 185.4k). No regression.
- Release A vs release-fast B (different LTO profiles, so not a clean A/B): s1 medians A 864k vs B
  826k over 9 pairs; s4 medians A 494k vs B 519k over 3 pairs. Opposite signs across shard counts,
  within the profile difference; the code change is one thread-local `bool` load plus a field read
  per victim (~258k per run).

**SIGTERM exit** (`--shards 4`, ~126k keys × 100 B, kill -TERM → exit; 3 reps each):
- A with save points (no final save): 4–5 ms. B with save points: 69–75 ms (the final save). B with
  no save points: 4–6 ms (unchanged). All exit 0.

## Gates at de8bf26e (the last code commit; NOTES/SUMMARY follow)
- `cargo fmt --check`, `audit-unsafe` (0 missing SAFETY, no new `unsafe`), `audit-unwrap`
  (baseline 0), `audit-test-tempdirs`, `audit-encoding-limits`: clean.
- `cargo clippy --all-targets -- -D warnings` (monoio): clean. `cargo clippy --no-default-features
  --features runtime-tokio,jemalloc -- -D warnings`: clean, also with `--all-targets`.
  `cargo check --all-targets --no-default-features --features runtime-tokio,jemalloc`: clean.
- Lib, monoio, FULL: 6,723 passed / 0 failed / 15 ignored. Lib, tokio, filtered to
  `storage::eviction persistence:: command::persistence server::conn shard::persistence_tick
  scripting::`: 1,247 passed / 0 failed / 1 ignored.
- Integration, monoio (debug build of this tree, provenance checked by a string only 8d0f0eae has):
  perf_ws21_aof_writer_start 1/1, perf_ws21_eviction_bgsave 2/2,
  perf_ws21_snapshot_without_save_rules 9/9, perf_ws21_signal_save 6/6, perf_ws21_shutdown_abort 3/3
  (+ the redis oracle, `--ignored`: green), perf_ws21_flushall_save 8/8, perf_ws19_script_oom 3/3
  (+ its oracle), perf_ws15_bgsave_status 3/3, shutdown_integration `--ignored` 4/4; unchanged
  suites re-run: sigterm_shutdown 7/7, shutdown_drain 4/4, perf_ws19_save_rules 14/14 (+1 ignored),
  perf_ws12_bgsave_split 5/5, perf_ws16_bgsave_capture 8/8, keyspace_event_db_index 4/4,
  lua_flush_685 7/7, cmd_flush_dbsize_debug_memory 11/11.
- Integration, tokio (`MOON_BIN=/home/user/wt/bin/ws21-debug-tokio`, listener line without
  "monoio"): all eight suites above green (the same counts); sigterm_shutdown 7/7, shutdown_drain
  4/4, perf_ws16_bgsave_capture 8/8, keyspace_event_db_index 4/4, lua_flush_685 7/7,
  cmd_flush_dbsize_debug_memory 11/11, perf_ws12_bgsave_split 4/5 and perf_ws19_save_rules 12/14:
  the three failures are the full-resync / replica cases that need a monoio master (monoio-built
  test binaries do not gate them); they fail identically on the base tokio binary
  `ws20-red-4a96cd5f-debug-tokio`, so they are not regressions (WS16/WS19 SUMMARY note the same).
- `scripts/test-commands.sh --category persistence` vs redis-server 7.0.15: 3/3 (1/3 on 4a96cd5f).

## Self-evaluation (0–1: Completeness · Clarity · Practicality · Optimization · Edge cases · Self-evaluation)
- **moon#1271:** 0.95 · 0.95 · 0.95 · 0.95 · 0.92 · 0.92. Found and proved the worse interleaving
  (committed manifest naming deleted files, DBSIZE 35/200 after restart). The e2e red cannot run on
  the base binaries (no hook); the unit red and the byte-identical load/sweep code stand in.
- **moon#1257:** 0.93 · 0.9 · 0.95 · 0.92 · 0.92 · 0.9. All six removal sites, every caller, SWAPDB
  slot mapping, the spill-failure trace proven at file level and its restart reachability argued
  from the sink routing. The release A/B is cross-profile; the same-profile debug A/B shows none.
- **moon#1267:** 0.92 · 0.92 · 0.95 · 0.95 · 0.9 · 0.9. Corrected the premise (`--save ""` worked;
  `--save` omitted did not, in both offload modes). Residual: sharded `SAVE` is unsupported in every
  configuration (not a save-rules gap) — follow-up; the stale-WAL replay risk is shared with the
  `--save` configurations.
- **moon#1263:** 0.92 · 0.93 · 0.92 · 0.9 · 0.9 · 0.92. Deliberately follows measured redis (stay up on
  a failed save) instead of the plan's "exit non-zero". Residual: writes during the final save.
- **moon#1264 (a):** 0.95 · 0.95 · 0.95 · 0.95 · 0.92 · 0.92 (replies byte-checked against redis).
- **moon#1264 (b):** 0.9 · 0.92 · 0.9 · 0.9 · 0.9 · 0.9. Plain / EXEC / script on both runtimes;
  replica apply and the admin console not covered (listed); cost measured (one BGSAVE, ~17–24 ms).
- **Item 6:** 1.0 · 0.95 · 1.0 · 1.0 · 0.95 · 0.95 (mutation-proved).

## Review round (REVIEW-WS21: MERGE-AFTER-FIXES) — branch `perf/ws21-review-fixes` from `d49a2a98`

Every item has a red → green test; one issue per commit. Binaries were built from this worktree
and pinned with `MOON_BIN` (`ws21fix-debug`, `ws21fix-debug-tokio`; `ws21fix-relfast` for M1).

### F1 (moon#1257): eviction during a save moved, not cloned
- Before: `victim::remove` captured by `capture_key` (a deep `entry.clone()` on the shard thread),
  then removed the key and lazily freed the original. That is two copies of a 1M-field hash, and
  the clone took 198–255 ms.
- Now: remove first, then `snapshot_cow::capture_removed(slot, key, entry)`
  (`snapshot_cow/capture.rs`). If the epoch still needs the key's epoch-start state and has none
  yet (first capture wins), the removed `Entry` itself becomes the pre-image (`Removed::Held`),
  and no lazy-free item is queued for it. Otherwise it returns `Removed::Dispose(entry)`, and the
  caller frees it as before.
- A held pre-image is released off-thread by `frozen::dispose` when the walk writes it
  (`advance_segment_inner`), when a later capture is discarded (`capture_cow`), and on `abort`
  and on `snapshot_cow::clear` (disarm). Dropping a 1M-field hash inline there would move the
  stall rather than remove it.
- Red: `a_large_victim_is_moved_into_its_pre_image_not_cloned` fails with the old
  capture-then-remove (1 clone; it would also have had 1 lazy-free item). Green, as are
  `a_victim_captured_earlier_is_freed_as_usual`, all 7 `eviction_capture_tests`, and
  `perf_ws21_eviction_bgsave` on both runtimes.
- M1: see SUMMARY. The worst PING gap is 22–51 ms at 1M fields (main 28–37) and 99 ms at 3M
  (main 49); it was 198–255 ms and 459 ms. RSS is flat across the eviction. `current_cow_size` is
  152 MB, which is the held epoch-start value; a fork-based redis keeps those pages too.
- Residual: `capture_cow` walks the moved value once to size it for `current_cow_size`
  (`pre_image_bytes` → `estimate_memory`), on the next drain tick. When that walk lands in the
  same gap as the removal's own ledger walk, the gap is ~45 ms instead of ~23 ms. The follow-up
  is to carry the removal's credited size (`used_memory` delta) into the pre-image. It was left
  out because it changes the `PENDING` element type and `capture_cow`, and `snapshot.rs` is at
  its line cap.

### F2 (moon#1264 a): SHUTDOWN ABORT vs commit is one decision
- Before, abort and commit were separate atomics, and `aborted()` was checked only before each
  5 ms poll. An ABORT landing in the poll where the save completed answered `+OK` for a shutdown
  that then exited.
- Now `shutdown_abort::Pending` lives under a `parking_lot::Mutex<Window>`. `commit()` (the
  save is on disk) and `abort()` (it takes every shutdown still pending) are decided under that
  one lock, so only the winner replies. An ABORT after the commit answers "No shutdown in
  progress."
- The reviewer's proof became `an_abort_that_answered_ok_stops_the_shutdown_whose_save_just_completed`,
  red without `commit()`. `review_ws21_abort_race` (copied and run) is green.

### F3 (moon#1267): `appendonly no` boots from the snapshot only
- `Shard::restore_from_persistence` makes one decision, `KvSources::for_boot`, from
  `kv_authority_elsewhere` and the shard's `runtime_config.appendonly`. This touched no caller
  signature: main.rs, embedded.rs, and the 7 test callers of `recover_shard_v3_with_fallback`
  are unchanged.
- `SnapshotOnly` loads the snapshot. It skips:
  - the v2 legacy `appendonly.aof` and the WAL v3 last-resort replay;
  - the v3 Phase 4 `Command` records (the set `Elsewhere` already skips);
  - Phase 4b.
- It keeps the manifest, cold index, warm segments, FPI, `last_lsn`, CLOG and the cold
  reconcile.
- Why: with `appendonly no` nothing writes a KV log (no AOF writer, and "WAL skipped
  (appendonly=no)"), so a log on disk predates the snapshot. redis ignores the AOF then.
- Scope note (orchestrator): the decision point is `restore_from_persistence` / `_v2`. The v3
  gate in `recover_shard_v3_pitr` takes the same enum because the default `--disk-offload
  enable` reproduces the bug through Phase 4 / 4b: `stale_wal_offload_enabled` and
  `legacy_aof_offload_enabled_*` were red. `rdb::load` and `shard_snapshot_load` are called
  exactly as before. recovery.rs stayed at 2499 lines.
- Red on the pre-F3 binary:
  - legacy aof: 50/50 keys reverted, with offload off, with offload off plus `--save` rules,
    and with offload on;
  - WAL v3 from a `--wal-kv-log on` run: 37/50 with offload off and with offload on.
- Green now. The appendonly-no cold crash suites stay green (`disk_offload_no_aof`,
  `cold_multidb`, `orphan_sweep_readiness`, `--ignored`).
- `cold_index_rebuild_silent_drops_875` fails 4/4, identically on `main-4a96cd5f-rel`. It runs
  as root, which ignores `chmod 000`, so this is environmental.

### F4 (moon#1263): stall, not deadline; a signal's stop stays armed
- Options weighed:
  - (a) a longer fixed deadline: any number is a dataset size, so it is wrong again later;
  - (b) exit once a running save completes, without our own save: that loses writes since
    the save began;
  - (c) no bound at all: a wedged shard would then hang SHUTDOWN forever;
  - (d) a stall bound, which is what was implemented.
- The design has two parts:
  - `SAVE_PROGRESS` moves on every shard walk advance (`snapshot_cow::note_progress`), save
    start and shard finish.
  - SHUTDOWN and FLUSHALL (`Patience::UntilStalled(20 s)`) wait as long as the save progresses,
    and fail after 20 s with no progress, keeping the server up (as for a failed save). A signal
    (`Patience::Forever`) never gives up: every 20 s without progress it logs redis's two
    shutdown error lines plus "The stop stays armed", and it exits when the save is on disk.
    It defers to other saves without a limit; SHUTDOWN still stops after 3.
- Why a signal differs from SHUTDOWN: SHUTDOWN has a client to answer, and it can retry. A
  signal's only outcomes are "exit when saved" or "silently keep running". redis has no
  deadline on either; SHUTDOWN's stall bound is the one concession to a wedged shard.
- Before, one 20 s deadline covered everything: a save of more than about 4 GB failed SHUTDOWN
  and dropped the SIGTERM.
- The old WS19 virtual-clock tests were rewritten for the stall rule (`save_wait_tests.rs`, 4).
- The reviewer's `review_ws21_sigterm_deadline` was run with its port changed to 7521, which is
  in my range. It is red on the pre-F4 binary ("gave up after 20.017 s … still running 15 s
  later") and green on the fix. It is kept as `a_sigterm_outlasting_the_stall_limit_stays_armed`.

### moon#1274: AOF writers drained before exit
- Before:
  - `broadcast_shutdown` used `try_send` and ran BEFORE the shards stopped;
  - the writers' token was `cancel_token.child_token()`, which in `runtime::cancel` is the SAME
    token, so the tokio writers left at the SIGTERM;
  - only the shards were joined.
- Now, in main.rs:
  - the shards are joined first;
  - then `aof::writer_stop::stop_writers` sends `Shutdown` via `send_deadline` (behind
    everything queued) and waits `is_finished`, re-sending every 500 ms because a rewrite
    overflow drain can swallow it;
  - after that it joins the writers.
- The writers have their own token, cancelled only after 60 s (logged). The tokio selects are
  `biased`. The legacy non-sharded listener keeps try_send semantics (deadline = now).
- `perf_ws21_aof_drain` (7 tests: SIGTERM, SIGINT and SHUTDOWN at s1 and s4, plus a held-writer
  SIGTERM) is red on the pre-fix monoio binary: 300/300 lost at s1 for SIGTERM and SIGINT, and
  the held case exited within 300 ms. It is green 3/3 on monoio and 3/3 on tokio.
- Residual: `embedded.rs` keeps its own order (cancel, join the shards, drop the pool, join the
  writer). Its writer token is still a child of `cancel`. With `biased` it now writes what is
  queued before leaving, but a shard still producing after the cancel can race it.

### F5, F6
- F5: `perf_ws21_shutdown_abort` polls `SHUTDOWN ABORT` until it answers `+OK`. Before the
  shutdown is pending, ABORT answers "No shutdown in progress." and cancels nothing, which
  `an_abort_with_no_shutdown_pending_cancels_nothing_later` pins. The server must stay alive
  throughout; the bound is 30 s. The "still alive after N ms" checks stay; F7 judged them
  acceptable.
- F6: the held-writer rewrite still runs under everysec. The server is SIGKILLed right after the
  commit, and the post-commit keys are written by a restart under `--appendfsync always`
  before the second SIGKILL.

### F10: the runtime save config
- `persistence::save_points_now(&RwLock<RuntimeConfig>)` is used by:
  - FLUSHALL, where `save_after_flushall` / `save_after_txn_flushes` now take a bool;
  - bare SHUTDOWN on both runtimes;
  - signals, where `signal::arm` keeps the live config and decides when the signal lands.
- The lock is read briefly and never held across an await.
- Residuals, both documented:
  - a replica applying FLUSHALL does not save, because it applies per shard with no all-flushed
    point to save from;
  - the auto-save timer still uses the startup rules.
- Red on the pre-F10 binary: the FLUSHALL still saved after `CONFIG SET save ""`, and SHUTDOWN
  saved the late key. Green on both runtimes.

### Snapshot start/finish/abort paths touched (for WS20's `snapshot_hold` hooks)
- No new shard-level snapshot start, finish or abort. Every save these paths run starts through
  `bgsave_start_sharded` (src/command/persistence.rs:232). It reaches the shards by the watch
  trigger, into the existing `persistence_tick::handle_pending_snapshot` /
  `check_auto_save_trigger`, whose WS20 hooks cover it. `finalize_snapshot_*` is unchanged.
- Restructured, all process-side waiters in src/command/persistence/save_wait.rs:
  - `shutdown_save` :146;
  - `shutdown_save_until_done` :169;
  - `save_and_wait` :232;
  - `save_after_flushall` :331.
  A waiter that gives up on a stall leaves the shard snapshot running as an ordinary BGSAVE; it
  does not abort it.
- Touched inside the epoch, with no lifecycle change:
  - `SnapshotState::abort` (src/persistence/snapshot.rs:597) disposes the pre-images;
  - `capture_cow` (:545);
  - `advance_segment_inner` (:884);
  - `snapshot_cow::clear` (src/persistence/snapshot_cow.rs:403, via `disarm` :322);
  - `note_progress` (:309) bumps `SAVE_PROGRESS`.

### Review-round gates (at `a14827df`, the last code commit)
- `cargo fmt --check`; the audits (unsafe, unwrap, test-tempdirs incl. self-test,
  encoding-limits incl. self-test): clean.
- `cargo clippy --all-targets -D warnings` on monoio and on tokio: clean. The tokio
  `check --all-targets` is clean.
- Lib tests (`command::persistence`, `persistence::{snapshot, snapshot_cow, recovery, aof}`,
  `storage::eviction`, `storage::db::lazy_free`, `shard::tests`): monoio 410/0, tokio 413/0.
- Integration suites, green on BOTH runtimes:
  - `perf_ws21_aof_drain` 7;
  - `perf_ws21_aof_writer_start` 1;
  - `perf_ws21_eviction_bgsave` 2;
  - `perf_ws21_flushall_save` 10;
  - `perf_ws21_shutdown_abort` 3 (+1 ignored);
  - `perf_ws21_signal_save` 7;
  - `perf_ws21_snapshot_without_save_rules` 9;
  - `perf_ws21_stale_log_boot` 5.
- Also green:
  - monoio: `perf_ws19_save_rules` 14, `perf_ws12_bgsave_split` 5, `perf_ws15_bgsave_status` 3,
    `sigterm_shutdown` 7, `aof_auto_rewrite` / `aof_toplevel_multishard_refusal` `--ignored` 5+2;
  - both runtimes: `shutdown_integration --ignored` 4.

## Round 3 (REVIEW-FINAL-P5A) — branch `perf/ws21-round3` from `4fd00213`

### A1: a rewrite at shutdown
- **Mechanism, confirmed.** The fold helper pushes `AofFold { reply_tx }` into shard N's SPSC ring and polls `reply_rx`. After the shards stop, the ring outlives them because the pool holds the producer. `reply_tx` sits in it undropped, so `try_recv` returns `Empty` forever and the other writers wait in `await_outcome`. Before, `stop_writers` gave up at 60 s and main exited 0.
- **Fix.** `aof/rewrite/fold_reply.rs::request_fold_snapshot` is now the one fold request used by `do_rewrite_per_shard`, `do_rewrite_sharded` and `rewrite_aof_sharded_sync`.
  - A stopped shard drops its `HeapCons`, which releases the ring's read hold (ringbuf 0.5, `Frozen::drop` → `hold_read(false)`), so `prod.read_is_held()` turns false.
  - The push refuses a ring nobody reads.
  - The wait ends once the reader is gone, after one more `try_recv` for a reply sent just before the exit.
  - The fold then errors. `ShardDoneGuard` marks the rewrite failed, the terminal writer publishes `old_seq`, and the parked writers roll back to the old incr, where `finish_framed` drains their channels and spill buffers.
  - A `Shutdown` swallowed by those drains is re-sent every 500 ms.
- **Why not a global "shards stopped" flag.** The read hold is exact per shard. It also covers a fold pushed after the stop, and the embedded server and in-process tests share nothing global.
- **`stop_writers`.** It takes its bound, cancels the writers' token when the bound runs out, and grants a 2 s grace. It returns `Err(names)` for the writers still running, and main turns that into a non-zero exit.
- **Measured red (build of 4fd00213):** 60.01 s for BGREWRITEAOF + immediate SIGTERM (s1, s4, tokio s4); late writer 59.5 s with 227/300 lost and exit 0. **Green:** under 1 s, 0 lost.

### A2: the second SIGINT
- `signal::insist` sets `INSISTED` and cancels the server. The normal exit then runs: shards stop and `stop_writers` drains. `hurry = signal::insisted` cuts the bound to `HURRY_BOUND` (2 s) plus the grace, and main exits 1.
- The token cancel also releases a writer held by `MOON_TEST_AOF_WRITER_HOLD`; its hold loop checks the token. This is how proof 2c drains all 300 records within about 2 s.
- A SIGINT while a stop is already draining also insists.
- **Red:** 84/300 lost on monoio, 300/300 on tokio. **Green:** 0 lost, status 1.

### A3: progress after the walk
- `snapshot_stream::run_helper` reports every chunk it writes. `publish` reports the footer, the fsync, the rename and the directory fsync.
- The test counter is thread-local (`take_local_save_progress_for_test`), because the global counter is shared with parallel tests.
- **Residual:** one fsync longer than 20 s is still a stall to SHUTDOWN / FLUSHALL. redis bounds its final fsync with `rdb-save-incremental-fsync` (4 MB); that is the follow-up if it ever matters.

### A4: finalize error
- `finalize_snapshot_error` calls `abort()` before dropping the state. `abort` is idempotent: it logs once and its overflow drain is empty the second time.
- The test counts `Discard::Value` sent to `moon-snapdrop` (a new test counter). The first red probe was invalid because rustfmt had re-wrapped the line, so the removal missed. The redone probe gave 0 → 1.

### A5 and A6
- `KvSources::note_unreplayed_logs` WARNs once, from shard 0, for `appendonly.aof` and `appendonlydir/moon.aof.manifest` under `--appendonly no`, and names the remedy.
- Docs: `docs/configuration.md` (Persistence), and in `docs/production-guide.md` the sections "Switching …" and "Graceful shutdown and stop timeouts" (systemd, Docker and Kubernetes timeouts: the dataset's save time plus 60 s). `packaging/moon.service` has a commented `TimeoutStopSec`.

### A7
- `broadcast_shutdown` first sends `try_send` to every writer, then `send_deadline` only to the full ones. Red: writer 1 had no `Shutdown` after 300 ms behind a full writer 0. Green: immediate.

### Files over the cap
None grew:
- rewrite.rs 2026 → 1907;
- pool.rs 3195 → 3195;
- main.rs 2608 → 2608;
- persistence_tick.rs 3532 → 3532;
- recovery.rs 2499 → 2499.

### Round-3 gates (at `767ae796`)
- fmt and the audits (unsafe, unwrap, test-tempdirs plus self-test, encoding-limits plus self-test): clean.
- clippy `--all-targets -D warnings` on monoio and tokio: clean. The first clippy run caught a needless borrow in rewrite.rs, fixed before the commit. The tokio `check --all-targets` is clean.
- Lib tests (`command::persistence`, `persistence::{aof, snapshot, snapshot_cow, snapshot_stream, recovery}`, `shard::{persistence_tick, tests}`, `storage::eviction`): monoio 455/0, tokio 458/0.
- Green on both runtimes (debug builds of `767ae796`):
  - `perf_ws21_aof_drain` 11;
  - `perf_ws21_signal_save` 7;
  - `perf_ws21_shutdown_abort` 3 (+1 ignored);
  - `perf_ws21_flushall_save` 10;
  - `perf_ws21_stale_log_boot` 6;
  - `perf_ws21_aof_writer_start` 1;
  - reviewer proofs `rvf_p5a_rewrite_at_shutdown` 1 and `rvf_p5a_stuck_final_save` 3;
  - `rvf_p5a_embedded_stop` 1 (tokio).
- Also green on monoio: `aof_auto_rewrite` 5, `aof_toplevel_multishard_refusal` 2 and `crash_matrix_per_shard_bgrewriteaof` 2 (all `--ignored`), and `sigterm_shutdown` 7 (both runtimes).
