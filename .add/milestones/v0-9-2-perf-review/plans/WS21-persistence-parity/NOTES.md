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
- Reachability on restart: the async sink runs only under `--appendonly yes`
  (`EvictionSink::AsyncSpill` routes `appendonly no` to `evict_batch_durable`, which keeps the
  hot value on a failed write). With `appendonly yes` the AOF (manifest authority on monoio and
  multi-shard tokio; legacy `appendonly.aof` replayed after the snapshot on tokio `--shards 1`)
  holds the key's write. So the gap was a file that is not a point-in-time image — visible to a
  restore from the RRDSHARD file alone and to moon#1185's fold — not a key lost at restart.

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
