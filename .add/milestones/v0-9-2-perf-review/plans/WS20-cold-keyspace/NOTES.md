# WS20-cold-keyspace — working notes

Branch `perf/ws20-cold-keyspace`, base main `4a96cd5f` + plan `b230d011`.
Personas: storage-durability-engineer (lead), ci-test-integrity-engineer.
Ports 7500–7519. Target `/home/user/wt/target`.

Red references:
- `/home/user/wt/bin/baseline-ae21476` (pre-wave main, monoio release-fast).
- `/home/user/wt/bin/ws20-red-4a96cd5f-debug-monoio`: a debug build of this
  branch at `b230d011` (= `4a96cd5f` code; `b230d011` adds plan docs only),
  built from this worktree (its `moon.d` lists this tree's paths).

## moon#1254 — MOVE of a spilled or in-flight key deletes it

### Mechanism (read in code, `4a96cd5f`)
- `move_cmd::move_core_uncounted` starts with `src.remove(key)`.
- `Database::remove` (`storage/db/kv_ops.rs`) = `remove_cold_only`
  (`spill_inflight_forget` + `ColdIndex::remove`) then `remove_hot`, and
  returns only the HOT entry. For a cold-only or in-flight key it drops the
  cold index entry / retires the in-flight record and returns `None`.
- `move_core` answers `:0` ("not moved"), so no connection path logs it: the
  key is gone from both databases in RAM with no record. With an AOF the dead
  slot enters the moon#1215 ledger and the next fold writes a `DEL` for it
  (the key is not alive at T) — the loss becomes durable. Before moon#1215 a
  restart brought it back from its spill file.
- `copy_core` reads the source with `src.get()`, which promotes a cold or
  in-flight key (`lookup` → `promote_cold_if_present`), so COPY … DB n copies
  a cold key correctly. But a cold key whose bytes cannot be read raises the
  moon#875 cold-fault flag and `get` answers `None`: `copy_core` then answers
  `:0` (a silent "no such key"), and because MOVE/COPY are intercepted before
  `dispatch`, `cold_fault_gate` never consumes the flag — the NEXT command on
  that database is answered `-IOERR` instead.
- Every caller reaches the cores: monoio/sharded connection intercepts,
  `shard/spsc_two_db.rs` (routed), both MULTI executors and the Lua bridge
  (`TwoDbOp::apply`), `replication/apply.rs`, `persistence/replay.rs`,
  `handler_single.rs`. None pre-checks existence, so a fix inside the cores
  covers them all (the same compile-time-coverage argument WS16 used for
  `capture_two_db`).

### Fix design
- `move_core_uncounted`: (1) collision first — `dst.exists(key)` (hot, in-flight
  and cold-alive, no disk I/O) answers `:0` without touching the source, so a
  refused MOVE never promotes; (2) `src.promote_cold_if_present(key, now)` —
  the in-flight plane first, then the cold file, the same path `get` uses; a
  cold fault → `-IOERR` (moon#875/#1225 rule) with the index entry kept; (3)
  `src.remove(key)` now finds the hot entry and moves it.
- `copy_core`: a `None` from `src.get` with a pending cold fault → `-IOERR`.
- WS16's `capture_two_db` stays the first statement of both cores; WS19's
  counting wrapper (`move_core` mutes, then counts one change on `:1`) is
  untouched — the promotion is inside the mute, and a promotion is no change
  anyway (`promote_*` mute themselves).

### Durability of the promotion step
- MOVE `:1` is logged; replay runs the same core, which promotes from the
  cold plane through the gated `cold_lookup_location` (files `< w` or
  authorized by their marker) — so a replayed MOVE of a key cold at the
  generation start reads its file, exactly like a replayed APPEND.
- A MOVE that is refused AFTER promotion cannot happen any more (the
  collision check is first). A MOVE that fails with `-IOERR` promotes
  nothing.

### Risks
- One blocking `pread` on the shard thread for a MOVE of a cold key (the
  MULTI/Lua paths already do this for every cold read; MOVE of a cold key is
  rare).
- Pre-existing, NOT changed: MOVE of a hot key whose TTL has passed moves the
  expired entry and answers `:1` (redis answers `:0`). Out of scope; noted.

### Evidence (red → green)
- lib `move_cold_tests` red on `4a96cd5f`: MOVE of a spilled key
  `(reply, value in dst, in src) = (Integer(0), None, false)` (gone from both);
  in-flight the same; unreadable → `Integer(0)`; COPY … DB unreadable →
  `Integer(0)`. Green after (7/7).
- real server `crash_recovery_cold_del_rewrite::moved_and_copied_*`
  (`--ignored`): `main-4a96cd5f-rel` s1 86/200 and 69/200 not `:1`
  (norw / rw), s4 82 and 95; `baseline-ae21476` s1 85. Green on
  `ws20-1254-debug-monoio` at s1 and s4 (both cases).

## moon#1236 — a cold key overwritten hot WITH a TTL comes back OLD

### Mechanism (read in code + reproduced)
- A blind write to a cold-only key goes through `Database::set`'s `Inserted`
  arm, which deliberately leaves the cold entry standing (the replay-time
  ambiguity documented there). Live reads are right (hot wins; the active
  expiry's `remove_lazily` reaps hot AND cold once the TTL passes; the orphan
  sweep drops the shadow of a hot key).
- An AOF rewrite at T with the key alive puts `k = new, PXAT D` in the base.
  Its old slot stays in a listed file below the cut (it backs live
  neighbours). The fold's moon#1215 head does not DEL it (alive at T).
- Restart after D: `rdb::load` / `rdb::load_from_bytes` SKIP an entry whose
  TTL has passed (`rdb.rs` ~403 / ~1042). The cold index was rebuilt from
  the manifest first, the head's `MOON.COLDCUT` authorizes the file, and
  `k` reads its OLD value.
- Reproduced (scratch `probe1236.py`, 200 probes, 8 MB, `PX 9000`, restart
  after TTL + 1.5 s): `baseline-ae21476` with BGREWRITEAOF: s1 47/100 even
  probes back with the OLD value, s4 43/100; without a rewrite: 0/100 at s1
  and s4, kill -9 and clean SHUTDOWN (the replayed `SET … PXAT <past>`
  inserts an expired hot copy; the gated end-of-replay resolution is hot-wins,
  so the shadow is dropped). The issue's "100/100 without a rewrite" row did
  not reproduce here; the no-rewrite cases are kept as guards.

### Options
- (a) The overwrite retires the shadow durably (ledger). Not enough: the
  fold judges the key ALIVE at T (its TTL has not passed), so it writes no
  head DEL, and the base entry is the only record of the overwrite — which
  the loader then skips. The ledger cannot help a key that dies AFTER the
  fold.
- (b) Every recovery path that skips an expired entry tombstones the key's
  cold shadow. Needs the cold wiring during `rdb::load` (the callers detach
  it: `shard_replay::take_cold_wiring`, WS21's file, and `replay_aof`), and
  it is WRONG for a slot in a file minted after the cut: `k` alive at T,
  `PERSIST k` after T, spilled into F7 >= w with its marker — tombstoning at
  load would drop F7's live copy; the cut `w` is not known until the head
  replays.
- (c) chosen: **load an AOF base's expired entries instead of skipping
  them** — redis's own rule (`rdbLoad` never expires keys of an AOF preamble,
  `RDBFLAGS_AOF_PREAMBLE`: "the log of operations in the incr AOF is assumed
  to work in the exact keyspace state"). The key is then hot (expired, so
  every read hides it) AND cold at replay close, and the existing gated
  hot-wins resolution (`finish_replay_cold_reconcile`, moon#965) drops the
  shadow — the same state machine every non-expired base key already goes
  through, so no new mechanism. A post-cut slot is handled exactly as today:
  its marker drops the (expired) hot copy and the slot wins, which is the
  live history. The active expiry then reaps the hot copy (a `DEL` record
  in the new incr, as redis propagates).

### Every recovery path, read
- per-shard / TopLevel multi-part base: `shard_replay` → `rdb::load` → fixed
  here; gated (COLDCUT head) → hot-wins close.
- tokio `--shards 1` flat file: `replay_aof` → `rdb::load_from_bytes` → fixed;
  the flat-file fold writes the COLDCUT head inline → gated.
- RESP replay of an expired write (`SET … PXAT <past>`): not skipped today
  (inserted hot-expired) → hot-wins close. Unchanged.
- `migrate_aof` (one-time legacy migration): same loader; the per-shard
  bases it re-saves filter expired entries at write time. Harmless.
- `listener.rs` legacy `dump.rdb` load: same loader, not reachable from
  `main` (only `run_sharded` is). Would now reap instead of skip.
- **Review round: fixed (F6, `16cd41c7` + `95621f76`, see "Review round"
  below).** As first shipped — NOT fixed, named: the no-AOF quadrant. `--appendonly no --save` boots from
  the per-shard snapshot (`snapshot.rs` ~1428 skips expired entries, WS21's
  file) and attaches the cold index afterwards (`recovery.rs`), with no
  replay-close resolution. A cold key overwritten with a TTL, snapshotted,
  then restarted after the TTL can come back OLD there. A fix belongs with
  the snapshot loader (keep + reap, or tombstone only pre-snapshot slots,
  which needs the spill counter at the snapshot instant).
  MEASURED on this branch's fix binary (`ws20-1236-debug-monoio`, scratch
  `probe1236_noaof.py`, s1): phase 1 `--appendonly yes` spills the probes and
  stops; phase 2 `--appendonly no --save`: `SET` the even probes `PX 12000`,
  BGSAVE (ok), kill -9, restart after the TTL → **40/100 even probes back
  with the OLD value** (every even probe that was cold; the odd probes that
  were hot in phase 1 are not reloaded at all under `--appendonly no`, by
  configuration). Not changed here: `shard_snapshot_load` also serves the
  non-offload snapshot boot, where keeping expired entries would be a
  redis-parity change (redis skips expired keys loading a dump), so the fix
  is a decision for the snapshot owner (WS21), not a one-line edit.

### Risks
- An AOF base with many keys whose TTL passed during the downtime loads them
  all into RAM until the active expiry reaps them (redis does the same).
  `DBSIZE` counts them until reaped (redis too).
- A downgrade to an older binary is unaffected (loader-only change, no
  format change).

### Evidence (red → green)
- lib `storage::tiered::cold_del_rewrite_tests` (moon#1236 section, production
  fold + production recovery): `a_cold_key_overwritten_with_a_ttl_does_not_
  come_back_old_after_a_rewrite` red on `4a96cd5f` `left: Some("v1")`;
  `…_stays_gone_across_a_second_rewrite` red the same;
  `a_slot_spilled_after_the_rewrite_wins_over_an_expired_base_entry` red
  `Some("v1")` vs `Some("new")` (found while writing it: the PERSIST replayed
  after the fold promoted the OLD value from the older copy before the
  post-cut marker could cut to the new slot). `rdb_expired_load_tests` red
  with only the skip restored. Green after.
- real server `cold_keys_overwritten_with_a_ttl_*`: rewrite cases red on
  `main-4a96cd5f-rel` (s1 55/46, s4 89/84 even probes back OLD) and
  `baseline-ae21476` (s1 75); no-rewrite cases green on both (guards). All
  four green on `ws20-1236-debug-monoio` at s1 and s4.

## moon#1237 — SWAPDB with cold keys

### Mechanism (read in code + reproduced)
- `SWAPDB a b` swaps whole `Database`s (`db_plane::swap_contents`: cold
  index, ledger, in-flight records, replay gate) but a spill file's manifest
  `db_index` stays the slot it was spilled from. Recovery attaches each file
  to its TAG (`rebuild_from_manifest_per_db`) BEFORE the log replays.
- (issue) After a rewrite the base is written per current slot and the
  SWAPDB record is gone: the swapped database's cold keys come back in the
  OLD db, and the moon#1215 head DELs (emitted in the current slot) miss the
  copies rebuilt in the old one. Reproduced: `main-4a96cd5f-rel` s1 178 and
  s4 121 probe states wrong (odd probes in db 0, deleted evens back).
- (found by WS20, a second defect) Without any rewrite, `SWAPDB 0 1` of two
  EMPTY dbs followed by spills in db 1: the rebuild attaches those files (tag
  1) to slot 1, and the replayed SWAPDB then moves them to slot 0 — every
  post-swap spill is duplicated/resurrected in db 0 (deleted keys too).
  Reproduced: base binaries s1 86 and s4 142 wrong; the scratch probe on
  `ae21476` 46 live + 52 deleted probes in db 0.

### Options
- **Re-tag at the swap** (rewrite the manifest `db_index` of both dbs'
  files). Not crash-consistent with the AOF's `SWAPDB` record without a new
  record or manifest field: re-tag durable + record durable → the rebuild is
  already swapped and the replayed record swaps it back; re-tag durable +
  record lost (crash between them) → cold swapped, hot not; either order
  leaves a torn window, and recovery cannot tell which case it is in. The
  format-neutral alternative (each database carries a permanent "cold tag",
  the fold writes its base in tag order and ends its head with SWAPDB
  records) needs the fold writer (`fold_stream`, `rewrite`), the spill
  tagging and completion routing (`eviction.rs` — WS21, `persistence_tick`)
  and still cannot cover the `--appendonly no` snapshot (no records). Filed
  as the follow-up design.
- **Chosen: refuse while either db has a cold footprint** + **replay moves
  only the cold entries that existed at that point of the log**.

### Live rule (`storage::db::swapdb_cold_refusal`)
- Refused (`-ERR SWAPDB is not allowed while either database has keys in
  the disk-offload cold tier`) when db a or db b, on ANY shard, has a live
  cold entry, a rebuild's older copy, a dead slot (moon#1215 ledger), a
  zero-ref file queued or held (moon#1231), a compaction in progress, an
  in-flight spill, or a spill retired in flight whose completion has not
  arrived (moon#1253) — `Database::has_cold_footprint` /
  `ColdIndex::has_footprint`. Each of those is either on disk under the
  db's tag or about to be. With the rule, a database that is swapped owns no
  spill file, so a file's tag always equals the slot of its owner: the
  rewrite's per-slot base and the snapshot are consistent with the rebuild.
- Checked on the connection's shard before anything is logged: local dbs
  through the owner guards, other shards through the L4 read plane
  (`try_read`, never parking; bounded attempts; `-ERR … try again` if an
  owner held a db for all of them — nothing swapped).
- Paths: both sharded connection handlers (`try_handle_swapdb`),
  `handler_single` (embedded server). MULTI/TXN already refuse SWAPDB.
- Not atomic across shards: a shard that spills a key of a/b between the
  check and its own swap still swaps (the others did); logged at `error`
  (`note_swap_with_cold_footprint`, SPSC arm and the local leg). Window: one
  SPSC hop at s>1; at s1 only while the AOF enqueue awaits backpressure.
  Named residual.
- Replica apply (`replication/apply.rs`) must apply the master's SWAPDB and
  cannot refuse; a replica with its own cold tier and its own AOF rewrite
  keeps the pre-fix exposure. Named residual (not changed).
- Behaviour change (redis parity): redis never refuses SWAPDB. moon already
  refuses it inside MULTI and during BGREWRITEAOF; this adds a third
  moon-specific refusal. With `--disk-offload enable` (the default) a db
  that ever spilled refuses SWAPDB until its cold data is gone AND its files
  are reclaimed (after a FLUSHDB: the next committed rewrite + orphan sweep
  with an AOF, or the next sweep without).

### Replay rule (`storage::db::swap_replayed`, replay.rs's SWAPDB intercept)
- Before swapping, each db's cold index is split: entries of files that
  "existed" at this point of the log stay with the database and MOVE; the
  rest (spilled later, still hidden) stay in the SLOT. Existed = the gate
  authorizes the file (below `MOON.COLDCUT`, or its `MOON.SPILLED` replayed);
  in a generation with no cut (tokio `--shards 1` before its first rewrite)
  = its marker replayed (new per-db `ReplayMarkers::files`). A pre-#902 log
  (no cut, no markers) moves nothing (was: everything) — ambiguous there,
  documented.
- For logs written with the live rule, no file of a or b exists at a
  replayed SWAPDB, so nothing moves and the replay is exact. For OLDER logs
  (swaps of cold data by pre-fix binaries) the split is still right: files
  spilled before the swap move, files spilled after stay.
- Split keeps accounting exact (`ColdIndex::split_off_files`: entries, file
  references, older copies, ledger slots, queued unlinks; a move queues no
  unlink). A key whose entry and older copies land on different sides gets
  its newest local copy promoted to entry; merging back
  (`ColdIndex::merge_newer`) keeps a key present on both sides as one entry
  (newest `recency_key`) with the rest as older copies.
- Upgrade residual: data a PRE-fix binary swapped (files tagged with the old
  slot) stays exposed to a rewrite by the new binary until those files are
  reclaimed; nothing re-tags them.

### Evidence (red → green)
- lib `storage::db::cold_replay_gate::swapdb_cold::tests` (production
  recovery, real spill files tagged per db): with only the replay fix
  reverted, 4 red, e.g. `a_key_spilled_after_a_replayed_swapdb_stays_in_its_
  db` `left: (Some("v2"), Some("v2")) right: (None, Some("v2"))` (gated and
  ungated). Green after (11 tests incl. the footprint predicate, split/merge
  accounting and the live refusal on a real shard slice).
- real server `swapdb_of_cold_keys_*` / `keys_spilled_after_a_swapdb_*`:
  red on `main-4a96cd5f-rel` — cold+swap+rewrite s1 178 / s4 121 wrong,
  swap-then-spill without rewrite s1 86 / s4 142 wrong; the other two green
  (guards). All four green on `ws20-1237-debug-monoio` at s1 and s4 (SWAPDB
  of the cold dbs answered the new error; the swap of empty dbs succeeded).

## moon#1260 — `--appendonly no`: inherited cold keys lost after read + sweep + kill -9

### Mechanism (read in code + reproduced)
- `shard::timers::run_cold_orphan_sweep` builds the moon#1231 `FoldView`
  only from an AOF pool; with `--appendonly no` it passes none, and
  `UnlinkHold::admit` without a view unlinks every zero-ref file at once.
- Without an AOF the durable state is the shard's last snapshot (hot keys
  only) + every listed spill file. A key cold at the last save has ONE
  durable copy: its slot. A GET promotes it (no log), its file goes
  zero-ref once its neighbours leave, the sweep unlinks it, and a kill -9
  loses a key unchanged since the save.
- Only files the no-AOF process inherited are exposed in the reviewer's case
  (the connection gate plain-drops without an AOF), but the tick's
  memory-pressure cascade DOES spill without an AOF (`evict_batch_durable`),
  so a no-AOF process can create files too.
- Reproduced (real server, `crash_recovery_cold_del_rewrite::no_aof_*`, the
  reviewer's `r4_no_aof_promoted_…` shape at `MOON_TEST_COLD_DEL_SHARDS`):
  `main-4a96cd5f-rel` s1 179 of 179 readable probes lost, s4 147 of 147;
  the no-sweep control 0 lost.

### Options
- BGSAVE includes cold keys (read from the spill files): the snapshot would
  then load the whole cold tier into RAM at boot (a cold tier is typically
  larger than RAM), and `snapshot.rs` is WS21's.
- **Chosen: the hold, with a snapshot playing the fold's part**
  (`storage::tiered::snapshot_hold`, same `UnlinkHold` rule):
  - view epoch = this shard's snapshot starts + finishes, so the hold's
    bound is raised to the spill counter at every start and finish, and on
    the first view (every file present at boot — the inherited ones — is
    held);
  - a held file is stamped with the epoch at the decision; released once a
    snapshot that STARTED after the stamp has FINISHED successfully
    (`committed_floor` = its start epoch; a failed one releases nothing);
  - while a snapshot runs, every zero-ref file is held (`FoldView::hold_all`,
    new): the snapshot is incremental, so a key evicted into a new file
    before its segment was written and promoted back after it is in neither
    the image nor any file once that file goes.
  - hooks: `note_snapshot_started` where the shard creates its
    `SnapshotState` (BGSAVE / SHUTDOWN SAVE and auto-save paths),
    `note_snapshot_finished(ok)` in `finalize_snapshot_success/_error`
    (`shard::persistence_tick`, cross-ownership one-liners). Per shard thread
    (thread-local): the hooks and the sweep run on the shard's own thread on
    both runtimes; per-shard success is the right signal because each shard
    loads its own snapshot file at boot.
  - ~~Only when snapshots can be written (`SNAPSHOT_DIR_ABSENT` false:
    `--save` given).~~ **Stale (REVIEW-WS20 F2):** since moon#1267 main.rs
    never sets `SNAPSHOT_DIR_ABSENT`, so the hold applies with `--appendonly
    no` with or without `--save`; the dead gate and its test are gone
    (`95812d99`). Without save points only a manual `BGSAVE` or `SHUTDOWN
    SAVE` releases held files.

### Crash windows
| window | durable state | recovery | ok? |
|---|---|---|---|
| promote, sweep holds F, crash | last snapshot + F listed | key from F | yes (the fix) |
| promote, snapshot S starts after it, succeeds, sweep releases F, crash | S has the key hot | key from S | yes |
| snapshot S running when F went zero-ref, S succeeds | stamp = S's start, not < floor → held | key from F | yes |
| S fails | floor unchanged → held | key from F | yes |
| file minted and zero-ref while S runs | held (`hold_all`) until a later snapshot | key from F | yes |
| file minted after the last snapshot's finish, zero-ref, no snapshot running | below the bound only after the next start → unlinked now | key was written or spilled after the last save: outside the snapshot's RPO | as before |
| crash after unlink, before the tombstone commit | listed, missing (`files_missing`) | nothing to load | as before |

### Costs and limits
- Disk: a zero-ref file below the bound stays until the next successful
  snapshot + sweep (the durability cost, not a leak). With `--save` rules
  that rarely fire, only a manual BGSAVE releases it; nothing triggers a
  save for held-file pressure (the AOF path has one, WS19 item 4) —
  `auto_save.rs` is WS21's. Without an AOF new files are rare (the pressure
  cascade only), so the held set is bounded by the cold tier at boot.
- ~~Deleted keys in a held file come back after a crash before the next
  snapshot.~~ **Wrong as shipped (REVIEW-WS20 F1, BLOCKING):** a file
  emptied by DEL/FLUSHALL was stamped at the next ORPHAN SWEEP, so a
  snapshot taken in between (the FLUSHALL save, a BGSAVE) released nothing,
  and the deleted keys came back after a crash until a SECOND snapshot.
  Fixed in the review round (`c6380c47`, `ccb6129e`): the snapshot-start
  hook holds every queued zero-ref file with the pre-start epoch, and the
  shard sweeps right after a successful snapshot. Now: a file emptied
  before snapshot S starts is released as soon as S succeeds; one emptied
  while S runs waits for the next successful snapshot. Keys deleted from a
  file that still backs live keys are a different, pre-existing gap: the
  no-AOF cold plane records no removals (review-round finding N2).
- `INFO cold_files_pending_unlink` counts held files; a db with held files
  refuses SWAPDB (moon#1237's footprint) until they are released.

### Evidence (red → green)
- lib `shard::timers::promote_sweep_tests::without_an_aof_a_promoted_key_
  keeps_its_file_until_a_later_snapshot` (production commands + sweep): red
  with only the no-AOF view reverted ("file 5 is the promoted keys' only
  durable copy: it must be held, not unlinked"); green. WS19's
  `without_an_aof_writer_nothing_is_held` became
  `without_an_aof_or_a_snapshot_directory_nothing_is_held` (thread-local
  override of `SNAPSHOT_DIR_ABSENT`, which is process-global). Unit:
  `storage::tiered::snapshot_hold::tests` (2).
- real server `no_aof_promoted_cold_keys_survive_the_orphan_sweep_and_crash`
  red on `main-4a96cd5f-rel` (s1 179/179 lost, s4 147/147); green on
  `ws20-1260-debug-monoio` at s1 and s4; the no-sweep control green on both.

## Self-evaluation per issue (0–1: Completeness · Clarity · Practicality · Optimization · Edge cases · Self-evaluation)

- **moon#1254** — 0.95 · 0.95 · 0.95 · 0.95 · 0.9 · 0.95. Fix inside the
  two cores covers every caller by construction; collision checked first so
  a refused MOVE has no side effect; IOERR on a cold fault for both. Edge
  left (named, pre-existing): MOVE of a hot-but-expired key.
- **moon#1236** — 0.85 · 0.95 · 0.95 · 0.95 · 0.9 · 0.9. Every AOF
  recovery path covered (per-shard / TopLevel base, flat-file preamble,
  RESP replay unchanged and pinned), found and pinned a third shape (PERSIST
  after the fold). Completeness is below 0.9 because the `--appendonly no`
  snapshot quadrant is measured (40/100) and NOT fixed: its loader is
  WS21's `snapshot.rs`, shared with the plain snapshot boot where the same
  change is a redis-parity decision — it cannot reach 0.9 here without that
  owner's decision.
- **moon#1237** — 0.9 · 0.9 · 0.85 · 0.9 · 0.9 · 0.9. Both the reported
  defect and a second, worse one (no rewrite needed) fixed and pinned on
  both layouts; the replay rule is exact for old and new logs in gated
  generations. Practicality is 0.85: refusing SWAPDB of cold data is a real
  functional restriction under the default `--disk-offload enable`; the
  format-neutral alternative that avoids it is designed in NOTES but needs
  the fold writer, the spill tagging and completion routing (WS21 / other
  owners) and cannot cover the no-AOF snapshot. Residuals (replica apply,
  pre-fix tags, the check-to-swap race) are named and logged.
- **moon#1260** — 0.95 · 0.95 · 0.9 · 0.95 · 0.9 · 0.95. Reuses the
  moon#1231 hold unchanged with a snapshot as the fold; in-progress
  snapshots handled (`hold_all`); failure releases nothing; no-dir config
  unchanged. Practicality 0.9: held files wait for the next successful save
  and nothing triggers one for held-file pressure (`auto_save.rs`, WS21).

## Final verification (code at `da0716b4`)

### Real-server crash suite, full file (`crash_recovery_cold_del_rewrite
--ignored --test-threads 1`, 23 cases = the 11 WS15/WS19 cases + the 12 WS20
cases), debug binaries of `da0716b4` pinned by MOON_BIN (provenance: startup
line `(1 shards, monoio)` / `(1 shards)`, the tokio one contains the new
SWAPDB error string):

| runtime | --shards 1 | --shards 4 |
|---|---|---|
| monoio | 23/23 | 23/23 |
| tokio | 21/23 — the two `keys_spilled_after_a_swapdb_*` fail, see NEW FINDING | 23/23 |

Red on the tokio debug build of `4a96cd5f` at s1: MOVE 77 of 200 not `:1`;
TTL overwrite + rewrite 66 even probes back OLD; cold + SWAPDB + rewrite 97
probe states wrong. The no-AOF case is NOT red on tokio s1 (0 lost: that
layout's reads did not empty the files in this scenario), red on tokio s4
(14 of 163 lost), green after the fix on both.

### NEW FINDING (pre-existing; filed as moon#1275, FIXED in the review round by `b3b029d3`): tokio `--shards 1` + disk offload loses the whole AOF after any SWAPDB and a kill -9
- Probe (scratch `probe_tokio_swap_wal.py`, no maxmemory, nothing spilled):
  `SET a 1; SWAPDB 0 1; SET b 2 (db0); SET c 3 (db1)`, kill -9, restart →
  every key absent, on the tokio build of `4a96cd5f` AND on this branch; the
  same without the SWAPDB → all present; monoio s1 → correct.
- Mechanism (read): `coordinate_swapdb`'s local leg writes the SWAPDB record
  to WAL v3 unconditionally (`try_wal_append_required`), unlike the SPSC arm
  (`if wal_kv_log`). The tokio `--shards 1` boot (legacy flat AOF, no
  manifest) runs `recover_shard_v3` Phase 4: the WAL's one SWAPDB counts as
  KV history (`ReplayRoute::Keyspace`), so Phase 4b treats the WAL as the KV
  authority and never replays `appendonly.aof` (boot log: "WAL v3 replay
  complete (cmds=1)", no AOF replay line).
- It is why this suite's two SWAPDB-then-spill cases fail on tokio s1 (the
  SWAPDB of empty dbs is legal and logged). Owners: `shard/coordinator.rs`
  (the WAL leg), `persistence/recovery.rs` (Phase 4b), `event_loop.rs` (the
  drain). Recommend a new P0 issue.

### Gates
- `cargo fmt --check`; `scripts/audit-unsafe.sh` (244 blocks, 0 missing
  SAFETY — no new `unsafe`); `scripts/audit-unwrap.sh` (0, baseline 0);
  `audit-test-tempdirs.sh`, `audit-encoding-limits.sh`: clean.
- `cargo clippy --all-targets -- -D warnings` (monoio, re-run after touching
  a file so every target re-checked): clean.
- `cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D
  warnings` and `cargo check --all-targets --no-default-features --features
  runtime-tokio,jemalloc`: clean (they found the `handler_single` type error
  fixed in `da0716b4`).
- lib, monoio: full `cargo test --lib` 6739 passed / 0 failed / 15 ignored;
  filtered `storage:: persistence:: shard:: command:: scripting:: server::
  replication::` 4273 / 0.
- lib, tokio, same filter: 4046 passed / 0 failed; the binary lists this
  branch's 24 new lib tests.

## Review round (REVIEW-WS20: MERGE-AFTER-FIXES, F1 BLOCKING)

Branch `perf/ws20-review-fixes` from `86cd20e8` (the merge `db2e1f65` + the
orchestrator's SUMMARY commit). Red binaries: debug builds of `86cd20e8`
(`ws20r-red-86cd20e8-{monoio,tokio}`), `main-4a96cd5f-rel`. Green: debug
builds of each fix commit, and `ws20r-final-{monoio,tokio}` at `a4307d0e`.

### F1 (BLOCKING) — the no-AOF hold released a file emptied before a snapshot only at the SECOND one
- Mechanism (confirmed): `UnlinkHold::admit` stamps a held file with the
  view epoch at the SWEEP's decision. A file emptied by DEL/FLUSHALL
  reaches the hold at the next sweep; the FLUSHALL save or a BGSAVE in
  between already excludes its keys, but its start epoch is not above the
  stamp, so it released nothing. Until a second snapshot started after that
  sweep (never, with save rules and no further writes), a kill -9 rebuilt
  the flushed keys from the files.
- Fix: `ColdIndex::hold_queued_before_snapshot(stamp)` — at snapshot START
  (`timers::note_snapshot_started`, called from both start sites in
  `persistence_tick`), every db's queued zero-ref files enter the hold
  stamped with the epoch BEFORE the start (`UnlinkHold::hold_stamped`); a
  file missing at the boot rebuild keeps its fast path. After
  `note_snapshot_finished(true)` the event loop runs
  `timers::sweep_after_snapshot` (both runtimes), so those files go at
  once. No-op with an AOF.
- Red → green: unit `review_ws20_a_file_emptied_before_a_snapshot_is_
  released_by_it` (the reviewer's proof, permanent) and timers
  `without_an_aof_a_file_emptied_before_a_snapshot_is_released_by_it`; real
  server `crash_recovery_cold_no_aof::no_aof_flushed_*` (FLUSHALL with save
  rules; FLUSHALL + BGSAVE pipelined on one connection so the save starts
  before a sweep — a separate BGSAVE after a 1.1 s LASTSAVE wait let a sweep
  run first and passed on the red binary): probes back out of 200 on
  `86cd20e8` (FLUSHALL save / BGSAVE) — monoio s1 141 / 119, s4 184 / 188;
  tokio s1 187 / 171, s4 167 / 146; 0 after, on all four.
- tokio s1 needed `drop_phase1_aof`: see finding N1.

### F2 — the hold applies without `--save` too
Since moon#1267 `SNAPSHOT_DIR_ABSENT` is never set: the `None` branch and
`without_an_aof_or_a_snapshot_directory_nothing_is_held` modelled a config
that no longer exists. Removed (`95812d99`); `snapshot_fold_view` returns a
view unconditionally; docs say "with or without save points";
`no_aof_no_save_promoted_*` pins it (red on main: files unlinked).

### F3 → moon#1278 — FIXED
The replica apply refuses a `SWAPDB` whose databases have a cold footprint
on its shard (`swapdb_cold::replica_swap_needs_full_resync`) and returns
`ApplyOutcome::FullResync`; both replica loops zero the offset and drop the
link, so the reconnect sends `PSYNC ? -1`; `load_snapshot` clears the
replica's cold tier (`Database::clear` → `clear_all`) and loads the
master's post-swap image. Cost: one full transfer per such swap.
Replication is single-shard (`replica_supported`), and a master must run
monoio. Red → green: `perf_ws20_review::replica_swapdb::moon_1278_*`
(unit twin in `swapdb_cold_tests`): `(db0 keys, db0 probes, db1 keys, db1
probes, full resync)` = `(4407, 165, 11848, 35, false)` on `86cd20e8`
monoio, `(5376, 92, 16200, 200, false)` for a `86cd20e8` tokio replica under
a monoio master (`MOON_REPL_MASTER_BIN`); `(0, 0, 16200, 200, true)` after,
both.

### F4 — check-to-swap race, in-flight variant: DOCUMENTED, residual
`note_swap_with_cold_footprint` docs name both variants: (a) a spill that
completes in the window keeps the pre-swap tag; (b) a spill in flight at
the swap completes into the other Database as a ghost, the swapped-away
in-flight record stays (memory; that db refuses SWAPDB until a restart),
and a kill -9 before a fold leaves a phantom copy in the old db
(`finish_replay_cold_reconcile` keeps the unmarked slot). Not reproduced
(reviewer: 0 in 3,914 racing swaps). Design for the fix: a per-shard
"swap pending (a, b)" prepare, broadcast before the check, under which
eviction skips victims of `a`/`b` and the check waits for their in-flight
spills; cleared after the swap (or the refusal). A cross-shard protocol
step (coordinator + eviction, WS21's `eviction.rs`), so left out.

### F5 → moon#1277 — FIXED, with a design deviation
- Reproduced: 4/4 keys back with PTTL -1 at s1/s4 on both runtimes (redis
  0/4).
- Prescribed: suppress lazy expiry while loading. Rejected: redis is exact
  there only because it logs an expiry's `DEL` BEFORE the command that
  observed it. moon defers that `DEL` to the active-expiry tick (moon#542),
  so a write that saw a just-expired key live (an `INCR` right after a
  rate-limit window, `SET … NX` on a lock that lapsed) is logged with no
  `DEL` ahead of it; suppression would replay it onto the OLD value.
- Chosen: `persistence::replay::clock` pins the expiry-judgment clock of a
  replay to the newest mtime of the log being replayed, capped at the wall
  clock; `DispatchReplayEngine::replay_command` sets every db's cached clock
  to it for the record and hands the databases back on the wall clock
  after it. Every record was written no later than that mtime, so a key
  alive at the log's last write is alive for every record, and one expired
  before it is judged expired exactly as before: never worse than the wall
  clock. Pinned at `aof::replay_aof` (flat file), `replay_multi_part` and
  `replay_per_shard` (the incr files; a base judges nothing), and the Phase
  4 WAL pass (newest `*.wal`).
- Red → green: lib `replay::clock` (red with the pin disabled: `s` = `x`
  TTL 0, `n` = `1` TTL 0); real server `perf_ws20_review::moon_1277_*` at
  s1/s4 with and without a BGREWRITEAOF (control: a persistent key must
  replay), red 4/4 on `86cd20e8` both runtimes, green after; WAL-only
  recovery (`--wal-kv-log on`, AOF removed) at s4: cross-shard keys back
  persistent on `86cd20e8`, none after.
- Residuals: an mtime that lies later than the log (a copied file) errs to
  the wall clock (the old behaviour); the replica's live apply judges on the
  wall clock (a replica lagging across a deadline can still build a fresh
  key from a master RMW; redis never expires keys for master-stream
  commands); `replay_ordered_merge` is unpinned (no production emitter).

### F6 — #1236's no-AOF quadrant: FIXED
`snapshot::shard_snapshot_load_noting_expired` names each `(db, key)` the
loader skipped as expired (`shard_snapshot_load` keeps its signature, so
`restore_from_persistence_v2` is untouched); Phase 3 of
`recover_shard_v3_pitr` hands them, after the cold attach, to
`snapshot_hold::drop_cold_shadows_of_expired_image_keys`, a no-op with an
AOF (the log replays afterwards and a later marker may authorize a newer
slot). It drops a pre-snapshot copy the image superseded or a post-snapshot
spill (outside the no-AOF RPO). Red → green:
`crash_recovery_cold_no_aof::no_aof_cold_keys_overwritten_with_a_ttl_*`
(new): even probes back OLD on `86cd20e8` monoio s1 77, s4 95, tokio s1
84, s4 93; 0 after on all four; unit twin in `snapshot_hold`. Limit: once a
LATER snapshot no longer names the key, a crash brings the slot back if the
file still backs other keys — finding N2, the same as a plain `DEL`.

### F7 — the #1236 boot cost: documented (SUMMARY / CHANGELOG)
Reviewer's numbers: 200k keys expired in the base → DBSIZE 199k and 49 MB
right after boot, reaped in ~95 s, +5.29 MB of DEL records in the AOF, and
with `--maxmemory 32mb noeviction` writes answer `-OOM` until reaped.

### F8, F9 — test flakes
- F8 (`6f6ed4c4`): `rdb_expired_load_tests` TTL 30 ms → 1 s, waiting on the
  clock. With a 50 ms delay injected before the save: old red (left 1,
  right 2), new green.
- F9 (`1eb5b28e`): the no-AOF promote case waits until the sweep has
  DECIDED on every emptied file (`cold_files_pending_unlink` == files on
  disk, or none left) instead of `sleep(6)` — still red on
  `main-4a96cd5f-rel` (121 → 0, every file unlinked); the WS20 AOF cases run
  `--appendfsync always` (the 2 s pause left only lets spills complete); the
  TTL cases' "setup < 20 s" assertion is gone (a TTL that passes live is
  reaped with its DEL logged: absent either way).
- The crash suite had grown to 1629 lines: split (`ad3edc6d`) into
  `crash_recovery_cold_del_rewrite.rs` (AOF), `crash_recovery_cold_no_aof.rs`
  and the shared `crash_recovery_cold_support/`; `crash-matrix.yml` runs
  the new suite at s4 and s1 too.

### F10, F11 — SWAPDB refusal text and the bounded spin
`ERR SWAPDB is not allowed while either database has keys or unreclaimed
spill files in the disk-offload cold tier; after deleting or reading back
its cold keys, run BGREWRITEAOF (appendonly yes) or BGSAVE (appendonly no)
and retry` (unit-pinned). The foreign check spins up to 20,000 `yield_now`
on the connection's shard thread (~7.5 ms) and answers `ERR SWAPDB could
not check the disk-offload cold tier of every shard, try again`; a second
in-process server reads the first one's registry (embedded tests only).
Both documented in code and `docs/production-guide.md`.

### moon#1275 — FIXED (`b3b029d3`)
`ShardDatabases::publish_wal_kv_log` / `wal_kv_log(shard)`: the SPSC drain
publishes the per-shard `--wal-kv-log` decision, and `coordinate_swapdb`'s
local leg writes its WAL record only when it is on. Audit: every other WAL
v3 write outside the SPSC arms is a graph, workspace or transaction record.
Red → green: `perf_ws20_review::moon_1275_*` (tokio s1 `86cd20e8`: every key
lost) and the two tokio s1 `keys_spilled_after_a_swapdb_*` crash cases (101
and 98 probe states wrong → green).

### New findings (pre-existing; for filing)
- **N1 — tokio `--shards 1 --appendonly no` replays a leftover
  `appendonly.aof` at every boot** (redis ignores the file then).
  `probe_tokio_stale_aof.py`: `SET a; SET b` under `--appendonly yes`,
  clean stop, restart with `--appendonly no --save ""` (the keys load from
  the stale AOF), FLUSHALL + BGSAVE, kill -9, restart → both keys back on
  tokio s1 (replayed again: nothing logs the FLUSHALL); monoio: 0. It also masked the F1 cases on that layout (DBSIZE
  16200 with the fix), hence `drop_phase1_aof` in the no-AOF suite.
- **N2 — without an AOF, cold-key removals are not durable.** A snapshot
  holds the hot keyspace only and the cold plane records no removals, so
  `DEL` of a cold key whose file still backs other keys, then BGSAVE, then
  kill -9 brings it back: 46/100 on `main-4a96cd5f-rel`, 36/100 on this
  branch (`probe_noaof_del.py`). Needs a durable no-AOF tombstone set (e.g.
  the snapshot carrying each db's dead cold keys, applied after the cold
  attach — the F6 hook is the place).
- Over-cap files grown by wiring only: `recovery.rs` 2499 → 2506 (#1277 one
  line, F6 six), `event_loop.rs` 3279 → 3296 (F1's post-snapshot sweep,
  both runtimes), `shared_databases.rs` 2728 → 2734 (#1275's field; its
  accessors live in `shared_databases/wal_kv_log.rs`, `67230735`),
  `replication/apply.rs` 1898 → 1903, `coordinator.rs` 4487 → 4491,
  `spsc_handler.rs` 4981 → 4983, `aof/mod.rs` 1989 → 1990; `snapshot.rs`
  stays 1499.

### Gates (final code `67230735`)
fmt, audit-unsafe (no new `unsafe`), audit-unwrap, audit-test-tempdirs,
audit-encoding-limits, clippy `--all-targets -D warnings` on both
runtimes, tokio `check --all-targets`: clean. Lib (storage persistence
shard replication server command scripting): monoio 4294/0, tokio 4067/0.
`perf_ws20_review`: monoio 7/7, tokio 6/6 (+ tokio replica under a monoio
master). `crash_recovery_cold_del_rewrite --ignored` 21/21 and
`crash_recovery_cold_no_aof --ignored` 6/6 at s1 and s4 on both runtimes
(binaries of `a4307d0e`; `67230735` is an accessor move, re-verified by
the lib and review suites and the tokio s1 SWAPDB crash cases). Replication
(`--ignored`) and AOF/TTL replay regression suites green; the one tokio
failure (`cold_tier_aof_double_apply_902::writes_to_a_cold_key_after_a_
rewrite_survive_kill9`, `#[ignore]`) is red on `86cd20e8` too — it reads a
manifest the flat-file layout does not have.
